// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the actual HTTP -> native cell -> LTX -> output gate -> restore
//! path. The local object store exercises conditional object APIs; it is not S3.
#![allow(clippy::disallowed_methods)]
use super::*;
use anyhow::ensure;
use cellar::bucket::Bucket;
use cellar::sdk::*;
use serde_json::{json, Value};
use std::{path::Path, time::Duration};

async fn start(
    path: &Path,
    name: &str,
    bucket: Bucket,
    core: tokio::runtime::Handle,
    policy: &str,
) -> Result<Node> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let auth = Arc::new(PeerAuth::new([11; 32], name)?);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident: 16,
            max_cached_routes: 0,
            max_activations: 8,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Release,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(5_000),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 2_000,
        placement_weight: 1,
        generation: signer.public_key_hex(),
        resume_generation: None,
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name.into(),
            advertise: internal.local_addr()?.to_string(),
            region: "test".into(),
        },
        actor,
        path.into(),
    );
    config.maintenance.rebalance = false;
    config.shutdown = ShutdownPolicy {
        total_ms: 10_000,
        token_wait_ms: 0,
        no_progress_ms: 8_000,
    };
    let services = NodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: tokio::runtime::Handle::current(),
        core_executor: core,
        listener,
        internal_listener: internal,
        peer_http: http.clone(),
        peer_auth: auth.clone(),
        probe_signer: signer,
        fleet: Some(NodeFleet {
            lease_bucket: bucket.clone(),
            replica_storage: ReplicaStorage::new(
                bucket.store.clone(),
                ReplicaStorageOptions::default(),
            )?,
            bucket,
            replication: ReplicationOptions {
                flush_interval: Duration::ZERO,
                compaction: None,
                ..Default::default()
            },
            log_transport: Arc::new(SignedPeerTransport::from_clients(http.clone(), http, auth)),
            log: LogOptions::default(),
            fleet_durability: policy == "fleet",
        }),
        containers: None,
        metrics: None,
    };
    let name: Arc<str> = name.into();
    let options = baseline_workload::Options {
        secret: "baseline-test-bearer".into(),
        run_id: "aaaaaaaaaaaaaaaaaaaaaaaa".into(),
        policy: policy.into(),
    };
    let applications = CompiledApplications::default().register("perf-cells", "v1", move || {
        baseline_workload::application(name.clone(), options.clone())
    })?;
    let node = Node::start(config, services, deployment::graph(), applications).await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !node.is_ready().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(node)
}

async fn stop(node: Node) -> Result<()> {
    let outcome = node.run_until(async { ShutdownMode::Handoff }).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected outcome {outcome:?}")
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.log_quiesced
            && report.local_durability_closed
            && !report.drain_timed_out
            && !report.deadline_elapsed,
        "incomplete shutdown {report:?}"
    );
    Ok(())
}

async fn send(
    client: &reqwest::Client,
    node: &Node,
    change: Option<Value>,
    sequence: u64,
    retry: i64,
) -> Result<(u16, Value)> {
    let url = format!("http://{}/cells/0", node.info().public_address);
    let builder = if let Some(change) = change {
        client
            .post(url)
            .header(
                "x-command-id",
                format!("11111111111111111111111111111111:{sequence}"),
            )
            .header("x-retry-until-ms", retry.to_string())
            .json(&change)
    } else {
        client.get(url).header("x-read-consistency", "owner")
    };
    let response = builder.bearer_auth("baseline-test-bearer").send().await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    let value = serde_json::from_str(&body).unwrap_or_else(|_| json!({"error":body}));
    Ok((status, value))
}

async fn exercise(core: tokio::runtime::Handle, policy: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
    let node = start(
        &directory.path().join("first"),
        "first",
        bucket.clone(),
        core.clone(),
        policy,
    )
    .await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let retry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64
        + 300_000;
    let seed = json!({"kind":"initialize","database_bytes":4096});
    let (code, seeded) = send(&client, &node, Some(seed.clone()), 1, retry).await?;
    ensure!(
        code == 200 && seeded["state"]["revision"] == 0,
        "seed {code}: {seeded}"
    );
    let (_, replay) = send(&client, &node, Some(seed), 1, retry).await?;
    ensure!(seeded == replay, "seed retry did not return exact receipt");
    let change = json!({"kind":"set","expected_revision":0,"value":100,"payload":"first"});
    let (code, committed) = send(&client, &node, Some(change.clone()), 2, retry).await?;
    ensure!(
        code == 200 && committed["state"]["left"] == 100 && committed["state"]["revision"] == 1,
        "change {code}: {committed}"
    );
    let (_, replay) = send(&client, &node, Some(change.clone()), 2, retry).await?;
    ensure!(committed == replay, "duplicate mutation changed receipt");
    let conflict = json!({"kind":"set","expected_revision":1,"value":10,"payload":"conflict"});
    ensure!(
        send(&client, &node, Some(conflict.clone()), 2, retry)
            .await?
            .0
            != 200,
        "conflicting identity accepted"
    );
    ensure!(
        send(&client, &node, Some(conflict.clone()), 4, retry)
            .await?
            .0
            != 200,
        "sequence gap accepted"
    );
    ensure!(
        send(&client, &node, Some(conflict.clone()), 3, retry - 600_000)
            .await?
            .0
            != 200,
        "expired identity accepted"
    );
    ensure!(
        send(&client, &node, Some(conflict), 4097, retry).await?.0 != 200,
        "receipt bound exceeded"
    );
    let (_, observed) = send(&client, &node, None, 0, retry).await?;
    ensure!(
        observed["state"] == committed["state"] && observed["sequence"] == 2,
        "refusal mutated state"
    );
    let cas = json!({"kind":"transfer","expected_revision":0,"value":3,"payload":"not-applied"});
    let (code, rejected) = send(&client, &node, Some(cas.clone()), 3, retry).await?;
    ensure!(
        code == 200 && rejected["changed"] == false && rejected["state"] == committed["state"],
        "conditional result lost"
    );
    stop(node).await?;
    // New owner and an empty directory: receipts and payload state must be
    // recovered through the production bucket/LTX restore path.
    let recovered = start(
        &directory.path().join("second"),
        "second",
        bucket,
        core,
        policy,
    )
    .await?;
    let (code, replay) = send(&client, &recovered, Some(change), 2, retry).await?;
    ensure!(
        code == 200 && replay == committed,
        "restored receipt differs: {code} {replay}"
    );
    let (_, replay) = send(&client, &recovered, Some(cas), 3, retry).await?;
    ensure!(replay == rejected, "restored no-op receipt differs");
    let (code, next) = send(
        &client,
        &recovered,
        Some(json!({"kind":"transfer","expected_revision":1,"value":3,"payload":"after-restore"})),
        4,
        retry,
    )
    .await?;
    ensure!(
        code == 200 && next["state"]["left"] == 97 && next["state"]["revision"] == 2,
        "post-restore transition failed: {code} {next}"
    );
    stop(recovered).await
}

#[test]
fn bucket_receipts_and_reference_state_restore_through_native_output_gate() -> Result<()> {
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    host.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(90),
            exercise(core.handle().clone(), "bucket"),
        )
        .await
    })??;
    Ok(())
}

#[test]
fn fleet_fallback_retains_the_same_application_receipt_contract() -> Result<()> {
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    host.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(90),
            exercise(core.handle().clone(), "fleet"),
        )
        .await
    })??;
    Ok(())
}
