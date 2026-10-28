// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real successful byte replies withheld by a transparent authenticated relay.
use super::{fault_gates, lifetime, native};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc, time::Duration};

async fn start(
    path: &Path,
    app: Arc<dyn Application>,
    name: &str,
    bucket: &cellar::bucket::Bucket,
) -> Result<(Node, String)> {
    let bucket = bucket.clone();
    let (node, address) = native::start_named(path, app, name, move |config, services, _| {
        config.runtime.handler_budget = Duration::from_secs(10);
        config.actor.core.operation_deadline_ms = Some(30_000);
        config.actor.lease_ttl_ms = 5_000;
        config.startup.recovery_retry_backoff = Duration::from_secs(5);
        config.cell_limits.filesystem.max_host_handles = 4;
        config.cell_limits.filesystem.max_transfer_bytes = 4;
        services.fleet = Some(NodeFleet {
            lease_bucket: bucket.clone(),
            replica_storage: ReplicaStorage::new(
                bucket.store.clone(),
                ReplicaStorageOptions {
                    prefix: bucket.prefix.clone(),
                    ..Default::default()
                },
            )
            .expect("IO replica options"),
            bucket,
            replication: ReplicationOptions {
                flush_interval: Duration::ZERO,
                durability_timeout: Duration::from_secs(30),
                compaction: None,
                ..Default::default()
            },
            log_transport: Arc::new(SignedPeerTransport::from_clients(
                services.peer_http.clone(),
                services.peer_http.clone(),
                services.peer_auth.clone(),
            )),
            log: LogOptions {
                record_publish_timeout: Duration::from_secs(30),
                ..Default::default()
            },
            fleet_durability: true,
        });
    })
    .await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !node.is_ready().await? {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok((node, address))
}

async fn get(client: &reqwest::Client, address: &str, route: &str) -> Result<Value> {
    let response = client.get(format!("{address}{route}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(status.is_success(), "IO {route}: {status} {text}");
    Ok(serde_json::from_str(&text)?)
}

async fn observe(
    client: &reqwest::Client,
    address: &str,
    predicate: impl Fn(&Value) -> bool,
) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let value = get(client, address, "/control/snapshot").await?;
            ensure!(value["failure"].is_null(), "IO guest failed: {value}");
            if predicate(&value) {
                return Ok::<_, anyhow::Error>(value);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?
}

async fn ready_state(client: &reqwest::Client, address: &str) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let response = client.get(format!("{address}/state")).send().await?;
            if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                // Retry only this read/proof route, never the interrupted byte
                // operation or the reconciliation transaction.
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let status = response.status();
            let text = response.text().await?;
            ensure!(status.is_success(), "IO readback: {status} {text}");
            return Ok::<_, anyhow::Error>(serde_json::from_str(&text)?);
        }
    })
    .await
    .context("IO readback deadline")?
}

async fn relay(path: &Path, predicate: impl Fn(&Value) -> bool) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if path.exists() {
                let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
                if predicate(&value) {
                    return Ok::<_, anyhow::Error>(value);
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("relay observation timeout")?
}

fn verify(value: &Value, expected: &[u8]) -> Result<()> {
    ensure!(STANDARD.decode(value["data_base64"].as_str().context("IO bytes")?)? == expected);
    ensure!(value["sha256"] == format!("{:x}", Sha256::digest(expected)) && value["seed"] == 7);
    Ok(())
}

async fn stop(node: Node, mode: ShutdownMode) -> Result<Value> {
    let outcome = node.shutdown(mode).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected IO shutdown: {outcome:?}");
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.pending_application_events == 0
            && report.pending_dispatches == 0
            && report.log_quiesced
            && report.local_durability_closed
            && !report.drain_timed_out
            && !report.deadline_elapsed
            && (mode != ShutdownMode::Preserve || report.clean_reload_prepared),
        "incomplete IO shutdown: {report:?}"
    );
    Ok(
        json!({"outcome":"stopped","mode":format!("{mode:?}"),"complete":true,
        "ingress_drained":report.ingress_drained,"application_work_drained":report.application_work_drained,"cells_drained":report.cells_drained,
        "pending_application_events":report.pending_application_events,"pending_dispatches":report.pending_dispatches,
        "log_quiesced":report.log_quiesced,"local_durability_closed":report.local_durability_closed,
        "clean_reload_prepared":report.clean_reload_prepared,"drain_timed_out":report.drain_timed_out,"deadline_elapsed":report.deadline_elapsed}),
    )
}

async fn successors(
    path: &Path,
    app: Arc<dyn Application>,
    bucket: &cellar::bucket::Bucket,
) -> Result<Vec<(Node, String)>> {
    let mut result = Vec::new();
    for name in ["io-cold-b", "io-cold-c", "io-cold-a"] {
        result.push(start(&path.join(name), app.clone(), name, bucket).await?);
    }
    Ok(result)
}

pub fn exercise(
    app: Arc<dyn Application>,
    path: &Path,
    report: &Path,
    release: &Path,
    operation: &str,
    fence: bool,
    handoff: bool,
) -> Result<Value> {
    let (app, cancellation) = lifetime::observe(app);
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()?.block_on(async {
        let bucket = cellar::bucket::Bucket::open_dev(&path.join("fleet.sqlite"))?;
        let mut nodes = Vec::new();
        for name in ["io-b", "io-c", "io-a"] { let (node, address) = start(&path.join(name), app.clone(), name, &bucket).await?; nodes.push((name, Some(node), address)); }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
        let address = nodes.last().context("IO controller")?.2.clone();
        let seeded = get(&client, &address, "/seed").await?;
        let seed: Vec<u8> = (0..32).collect(); verify(&seeded, &seed)?;
        let target = format!("{address}/cancel-start"); let caller = client.clone();
        let mut held = tokio::spawn(async move { caller.get(target).send().await });
        let cut = relay(report, |value| !value["held"].is_null()).await?;
        let helper = operation.ends_with("_all") || operation.ends_with("_exact");
        ensure!(cut["held"]["ordinal"] == if helper {2} else {1} && cut["released"] == false);
        if matches!(operation, "read" | "pread" | "read_exact") {
            let offset = if helper {7} else {3};
            ensure!(cut["held"]["value"]["count"] == 4 && STANDARD.decode(cut["held"]["value"]["data_base64"].as_str().context("held read")?)? == seed[offset..offset+4]);
        } else { ensure!(cut["held"]["value"] == 4); }
        // A separate invocation proves the actual bytes while the original
        // caller is still missing its reply. This never repeats the byte call.
        let proven = get(&client, &address, "/prove").await?;
        let mut expected = seed.clone();
        if matches!(operation, "write" | "pwrite" | "write_all") {
            let count = if helper {8} else {4};
            for i in 0..count { expected[3+i] = 128+i as u8; }
        }
        verify(&proven, &expected)?;
        ensure!(proven["stat"]["inode"] == seeded["stat"]["inode"] && proven["receipt"] == seeded["receipt"] && proven["reconciled"].is_null());
        let writes = if matches!(operation, "write" | "pwrite" | "write_all") { if helper {2} else {1} } else {0};
        ensure!(proven["stat"]["version"].as_str().context("proven version")?.parse::<u64>()?
            == seeded["stat"]["version"].as_str().context("seed version")?.parse::<u64>()? + writes,
            "byte call count differs from content version progress");
        if writes == 0 { ensure!(proven["stat"] == seeded["stat"], "read changed persistent metadata"); }
        ensure!(tokio::time::timeout(Duration::from_millis(150), &mut held).await.is_err(), "lost byte reply acknowledged");
        let snapshot = observe(&client, &address, |value| !value["owner"].is_null()).await?;
        let owner = snapshot["owner"].as_str().context("IO owner")?;
        let index = nodes.iter().position(|value| value.0 == owner).context("IO owner node")?;
        let controller = nodes.iter().find(|value| value.0 != owner).context("remaining IO controller")?.2.clone();
        let mut outcomes = Vec::new();
        if fence {
            let outcome = fault_gates::force_lease_field(&bucket, nodes[index].1.take().context("IO owner")?, owner, "addr", "http://127.0.0.1:1").await?;
            ensure!(outcome == NodeOutcome::Fenced { code:3, reason:FenceReason::NodeLeaseMismatch });
            outcomes.push(json!({"outcome":"fenced","complete":false,"reason":"NodeLeaseMismatch","code":3}));
        }
        match held.await? { Ok(response) => ensure!(response.status().is_server_error()), Err(error) => ensure!(fence, "deadline lost HTTP failure: {error}") }
        cancellation.wait().await?;
        let cancelled = observe(&client, &controller, |value| !value["failed"].is_null()).await?;
        ensure!(cancelled["failed"]["code"] == "ContextExpired" && cancelled["failed"]["outcome"] == "unknown"
            && cancelled["failed"]["operation"] == format!("filesystem.{operation}") && cancelled["failed"]["completed_bytes"] == if helper {4} else {0});
        relay(report, |value| value["cancellations"].as_u64().is_some_and(|n| n > 0)).await?;
        std::fs::write(release, b"release original frame")?;
        let delivered = relay(report, |value| value["released"] == true).await?;
        ensure!(delivered["held"] == cut["held"]);
        get(&client, &controller, "/control/release").await?;
        let retired = observe(&client, &controller, |value| value["refused"] == 9).await?;
        ensure!(retired["attempts"] == 1 && retired["failed"] == cancelled["failed"]);
        if fence { tokio::time::sleep(Duration::from_secs(5)).await; }
        let ready = ready_state(&client, &controller).await?;
        verify(&ready, &expected)?;
        ensure!(ready == proven, "IO proven state changed before reconciliation");
        let warm = get(&client, &controller, "/reconcile").await?; verify(&warm, &expected)?;
        ensure!(warm["stat"] == proven["stat"] && warm["receipt"] == json!({"sha256":proven["sha256"],"phase":"reconciled"}) && warm["reconciled"]["failure"] == cancelled["failed"]);
        let mut cold = if handoff { successors(path, app.clone(), &bucket).await? } else { Vec::new() };
        for (name, node, _) in nodes.into_iter().rev() { if let Some(node) = node { outcomes.push(stop(node, if handoff {ShutdownMode::Handoff} else {ShutdownMode::Preserve}).await.with_context(|| format!("IO predecessor {name}"))?); } }
        if !handoff { tokio::time::sleep(Duration::from_secs(5)).await; cold = successors(path, app.clone(), &bucket).await?; }
        let cold_address = &cold.last().context("IO cold controller")?.1;
        let restored = ready_state(&client, cold_address).await?;
        ensure!(restored == warm, "IO cold state differs from reconciled proven state");
        let checked = get(&client, cold_address, "/retired").await?;
        ensure!(checked["refused"] == 18 && checked["state"] == warm && checked["owner"].as_str().is_some_and(|s| s.starts_with("io-cold-")));
        let post = get(&client, cold_address, "/post").await?; verify(&post, &seed)?;
        ensure!(post["stat"]["inode"] == warm["stat"]["inode"] && post["stat"]["version"].as_str().context("post version")?.parse::<u64>()? > warm["stat"]["version"].as_str().context("warm version")?.parse::<u64>()?);
        for (node, _) in cold.into_iter().rev() { outcomes.push(stop(node, ShutdownMode::Preserve).await?); }
        ensure!(outcomes.len() == 6 && outcomes.iter().filter(|value| value["complete"] == false).count() == usize::from(fence));
        Ok(json!({"format":1,"termination":if fence {"node_lease_mismatch"} else {"handler_deadline"},"shutdown":if handoff {"Handoff"} else {"Preserve"},
            "scope":"one real successful Node IPC byte reply withheld, authenticated loopback peers and supplied development bucket",
            "relay":delivered,"seed":seeded,"proven_before_cancellation":proven,"cancelled":cancelled["failed"],"retired_continuation":retired,"warm_recovery":warm,"cold_recovery":restored,"successor_refusals":checked["refused"],"post_write":post,"native_outcomes":outcomes}))
    })
}
