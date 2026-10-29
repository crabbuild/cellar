// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real native proof, revocation and recovery around either JavaScript backend.
use super::{
    fault_gates::{self, Gate, Peer, Release, Store},
    lifetime, native,
};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

pub fn payload(generation: u8) -> Vec<u8> {
    let mut bytes = vec![generation; 3 * 65536 + 17];
    bytes[65535] = generation + 8;
    bytes[131072] = generation + 16;
    bytes
}

async fn start(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: cellar::bucket::Bucket,
    peers: Arc<Gate>,
    data: Arc<Gate>,
) -> Result<(Node, String)> {
    let (node, url) = native::start_named(path, application, name, move |config, services, _| {
        config.runtime.handler_budget = Duration::from_secs(10);
        config.actor.core.operation_deadline_ms = Some(30_000);
        config.actor.lease_ttl_ms = 5_000;
        config.startup.recovery_retry_backoff = Duration::from_secs(5);
        config.maintenance.fleet_sample_period = Duration::from_millis(50);
        config.cell_limits.filesystem.max_host_handles = 4;
        let store = Arc::new(Store {
            inner: bucket.store.clone(),
            gate: data,
        });
        let replica_storage = ReplicaStorage::new(
            store.clone(),
            ReplicaStorageOptions {
                prefix: bucket.prefix.clone(),
                ..Default::default()
            },
        )
        .expect("proof replica options");
        let lease_bucket = bucket.clone();
        let mut bucket = bucket;
        bucket.store = store.clone();
        bucket.cas_store = store;
        services.fleet = Some(NodeFleet {
            lease_bucket,
            bucket,
            replica_storage,
            replication: ReplicationOptions {
                flush_interval: Duration::ZERO,
                durability_timeout: Duration::from_secs(30),
                compaction: None,
                ..Default::default()
            },
            log_transport: Arc::new(Peer {
                inner: Arc::new(SignedPeerTransport::from_clients(
                    services.peer_http.clone(),
                    services.peer_http.clone(),
                    services.peer_auth.clone(),
                )),
                gate: peers,
            }),
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
    Ok((node, url))
}

async fn get(client: &reqwest::Client, address: &str, route: &str) -> Result<Value> {
    let response = client.get(format!("{address}{route}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(
        status.is_success(),
        "proof route {route}: {status} {}",
        text.chars().take(4096).collect::<String>()
    );
    Ok(serde_json::from_str(&text)?)
}

async fn observe(
    client: &reqwest::Client,
    address: &str,
    predicate: impl Fn(&Value) -> bool,
) -> Result<Value> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            last = get(client, address, "/control/snapshot").await?;
            ensure!(
                last["failure"].is_null(),
                "JavaScript proof fixture failed: {last}"
            );
            if predicate(&last) {
                return Ok::<_, anyhow::Error>(last.clone());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("JavaScript proof observation timed out: {last}"))?
}

async fn state(client: &reqwest::Client, address: &str) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let response = client.get(format!("{address}/state")).send().await?;
            if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                // This fixed route only reads and proves its selected readback.
                // Never repeat seed, pending or post-recovery mutations.
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let status = response.status();
            let text = response.text().await?;
            ensure!(
                status.is_success(),
                "proof state: {status} {}",
                text.chars().take(4096).collect::<String>()
            );
            return Ok::<_, anyhow::Error>(serde_json::from_str(&text)?);
        }
    })
    .await
    .context("proof state readback deadline")?
}

fn verify(value: &Value, captured: &Value) -> Result<()> {
    let generation = value["fs_generation"]
        .as_u64()
        .context("business generation")?;
    ensure!((1..=3).contains(&generation));
    ensure!(value["seed"] == 7 && value["late"].is_null() && value["late-raw"].is_null());
    if generation != 3 {
        ensure!(
            value["pending"]
                == if generation == 2 {
                    json!(2)
                } else {
                    Value::Null
                }
        );
    }
    let expected = payload(generation as u8);
    let actual = STANDARD.decode(value["data_base64"].as_str().context("file bytes")?)?;
    ensure!(
        actual == expected,
        "recovered file differs from business generation {generation}"
    );
    let hash = format!("{:x}", Sha256::digest(&actual));
    let expected_receipt = json!({"generation":generation,"sha256":hash,"size":actual.len()});
    ensure!(value["filesystem_receipt"] == expected_receipt);
    for field in ["generation", "sha256", "size"] {
        ensure!(value["filesystem"][field] == expected_receipt[field]);
    }
    let key = match generation {
        1 => "seed",
        2 => "pending",
        _ => "post",
    };
    for field in ["inode", "version"] {
        ensure!(
            value["filesystem"][field] == captured[key][field],
            "file {field} differs from captured {key}"
        );
        ensure!(
            value["filesystem"][field]
                .as_str()
                .context("identity")?
                .parse::<u64>()?
                > 0
        );
    }
    Ok(())
}

fn summary(mut value: Value) -> Value {
    value
        .as_object_mut()
        .expect("verified proof state")
        .remove("data_base64");
    value
}

async fn stop(node: Node, mode: ShutdownMode) -> Result<Value> {
    let outcome = node.shutdown(mode).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected proof shutdown: {outcome:?}");
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
        "incomplete proof shutdown: {report:?}"
    );
    Ok(
        json!({"outcome":"stopped","mode":format!("{mode:?}"),"complete":true,
        "ingress_drained":report.ingress_drained,"application_work_drained":report.application_work_drained,
        "cells_drained":report.cells_drained,"pending_application_events":report.pending_application_events,
        "pending_dispatches":report.pending_dispatches,"log_quiesced":report.log_quiesced,
        "local_durability_closed":report.local_durability_closed,"clean_reload_prepared":report.clean_reload_prepared,
        "drain_timed_out":report.drain_timed_out,"deadline_elapsed":report.deadline_elapsed}),
    )
}

async fn successors(
    path: &Path,
    application: Arc<dyn Application>,
    bucket: &cellar::bucket::Bucket,
    peers: &Arc<Gate>,
    data: &Arc<Gate>,
) -> Result<Vec<(Node, String)>> {
    let mut nodes = Vec::new();
    for name in [
        "javascript-proof-cold-b",
        "javascript-proof-cold-c",
        "javascript-proof-cold-a",
    ] {
        nodes.push(
            start(
                &path.join(name),
                application.clone(),
                name,
                bucket.clone(),
                peers.clone(),
                data.clone(),
            )
            .await?,
        );
    }
    Ok(nodes)
}

pub fn exercise(
    application: Arc<super::JavaScriptApplication>,
    barrier: &str,
    fence: bool,
    handoff: bool,
) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let (application, cancellation) = lifetime::observe(application);
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()?.block_on(async {
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        let peers = Arc::new(Gate::default()); let data = Arc::new(Gate::default());
        let _release = Release(peers.clone(), data.clone());
        let mut nodes = Vec::new();
        for name in ["javascript-proof-b", "javascript-proof-c", "javascript-proof-a"] {
            let (node, address) = start(&directory.path().join(name), application.clone(), name, bucket.clone(), peers.clone(), data.clone()).await?;
            nodes.push((name, Some(node), address));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
        let seed_address = nodes.last().context("seed node")?.2.clone();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let seeded = get(&client, &seed_address, "/seed").await;
                if seeded.is_err() {
                    let snapshot = get(&client, &seed_address, "/control/snapshot").await;
                    return seeded.with_context(|| format!("seed fixture state: {snapshot:?}")).map(|_| ());
                }
                ensure!(seeded? == true);
                if peers.members.lock().unwrap().len() >= 2 { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await??;
        let captured = get(&client, &seed_address, "/control/snapshot").await?;
        let owner = captured["owner"].as_str().context("actual proof owner")?;
        let index = nodes.iter().position(|node| node.0 == owner).context("actual native owner")?;
        let control_address = nodes.iter().find(|node| node.0 != owner).context("remaining controller")?.2.clone();
        let before_peer = peers.entered.load(Ordering::Acquire); let before_data = data.entered.load(Ordering::Acquire);
        peers.hold(); data.hold_and_drain().await;
        ensure!(peers.is_held() && data.is_held());
        let caller = client.clone(); let target = format!("{}/{barrier}/cancel-start", nodes[index].2);
        let mut held = tokio::spawn(async move { caller.get(target).send().await });
        observe(&client, &control_address, |value| value["committed"] == true
            && (if barrier == "output" { value["returned"] == true && value["lateEntered"] == true } else { value["barrierEntered"] == true })
            && peers.entered.load(Ordering::Acquire) > before_peer && data.entered.load(Ordering::Acquire) > before_data).await?;
        ensure!(tokio::time::timeout(Duration::from_millis(150), &mut held).await.is_err(), "withheld proof acknowledged before revocation");
        let mut outcomes = Vec::new();
        if fence {
            let node = nodes[index].1.take().context("native owner")?;
            let outcome = fault_gates::force_lease_field(&bucket, node, owner, "addr", "http://127.0.0.1:1").await?;
            ensure!(outcome == NodeOutcome::Fenced { code: 3, reason: FenceReason::NodeLeaseMismatch }, "actual proof fence: {outcome:?}");
            outcomes.push(json!({"outcome":"fenced","reason":"NodeLeaseMismatch","code":3,"complete":false}));
        }
        let response = held.await?;
        match response {
            Ok(response) => ensure!(response.status().is_server_error(), "unproven effect returned success"),
            Err(error) => ensure!(fence, "deadline lost its HTTP error response: {error}"),
        }
        cancellation.wait().await?;
        if barrier != "output" {
            let cancelled = observe(&client, &control_address, |value| !value["cancelled"].is_null()).await?;
            let operation = if barrier == "sync" { "sync" } else if barrier == "fdatasync" { "fdatasync" } else { "fsync" };
            let failure = &cancelled["cancelled"];
            let retired = failure["code"] == "ContextExpired" && failure["errno"].is_null();
            let native_proof = failure["code"] == "Storage" && failure["errno"] == "EIO";
            ensure!((retired || native_proof) && failure["outcome"] == "unknown"
                && failure["operation"] == format!("filesystem.{operation}") && failure["completed_bytes"] == 0,
                "interrupted proof lost its native outcome: {failure}");
        }
        peers.open(); data.open();
        ensure!(get(&client, &control_address, "/control/release").await? == true);
        let retired = observe(&client, &control_address, |value| value["lateDone"] == true).await?;
        ensure!(retired["attempts"] == 1 && retired["refused"] == 51);
        if fence { tokio::time::sleep(Duration::from_secs(5)).await; }
        let warm = state(&client, &control_address).await?;
        verify(&warm, &retired)?;
        let mut cold = if handoff { successors(directory.path(), application.clone(), &bucket, &peers, &data).await? } else { Vec::new() };
        for (name, node, _) in nodes.into_iter().rev() {
            if let Some(node) = node { outcomes.push(stop(node, if handoff { ShutdownMode::Handoff } else { ShutdownMode::Preserve }).await.with_context(|| format!("predecessor {name}"))?); }
        }
        if !handoff {
            tokio::time::sleep(Duration::from_secs(5)).await;
            cold = successors(directory.path(), application.clone(), &bucket, &peers, &data).await?;
        }
        let address = &cold.last().context("cold controller")?.1;
        let restored = state(&client, address).await?;
        verify(&restored, &retired)?;
        ensure!(restored == warm, "cold file/state/receipt changed its proven readback");
        let checked = get(&client, address, "/retired").await?;
        ensure!(checked["refused"] == 102 && checked["owner"].as_str().is_some_and(|owner| owner.starts_with("javascript-proof-cold-")));
        ensure!(checked["state"] == restored, "stale wrappers changed successor state");
        let written = get(&client, address, "/post").await?;
        let after = get(&client, address, "/control/snapshot").await?;
        verify(&written, &after)?;
        ensure!(written["fs_generation"] == 3 && written["filesystem"]["inode"] == restored["filesystem"]["inode"]
            && written["filesystem"]["version"].as_str().context("new version")?.parse::<u64>()?
                > restored["filesystem"]["version"].as_str().context("old version")?.parse::<u64>()?
            && written["pending"] == restored["pending"] && after["attempts"] == 1);
        for (node, _) in cold.into_iter().rev() { outcomes.push(stop(node, ShutdownMode::Preserve).await?); }
        ensure!(outcomes.len() == 6 && outcomes.iter().filter(|value| value["complete"] == false).count() == usize::from(fence));
        Ok(json!({"format":1,"barrier":barrier,"termination":if fence {"node_lease_mismatch"} else {"handler_deadline"},
            "shutdown":if handoff {"Handoff with ready successors"} else {"Preserve and original lease expiry"},
            "scope":"supplied development bucket and authenticated loopback peer transport",
            "expired_continuation":retired,"warm_recovery":summary(warm),"cold_recovery":summary(restored),
            "successor_refusals":checked["refused"],"post_recovery_write":summary(written),"native_outcomes":outcomes,
            "peer_entries_after_hold":peers.entered.load(Ordering::Acquire)-before_peer,
            "bucket_entries_after_hold":data.entered.load(Ordering::Acquire)-before_data,"retained_wrappers":true,"host_slots_reused":4}))
    })
}
