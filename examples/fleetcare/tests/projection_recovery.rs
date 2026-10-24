// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise real Queue/DLQ consumers and SQL bindings through the public SDK.
//! Executor construction and socket clients belong to the caller test host.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use cellar::bucket::Bucket;
use cellar::sdk::*;
use fleetcare::{config::*, deployment};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

struct Executor(Option<tokio::runtime::Runtime>);

impl Executor {
    fn new() -> Result<Self> {
        Ok(Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?,
        )))
    }

    fn handle(&self) -> tokio::runtime::Handle {
        self.0.as_ref().unwrap().handle().clone()
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.0.take().unwrap().shutdown_background();
    }
}

#[test]
fn dead_lettered_projection_recovers_while_poison_stays_quarantined() -> Result<()> {
    let host = Executor::new()?;
    let core = Executor::new()?;
    let directory = tempfile::tempdir()?;
    host.handle().block_on(async {
        let app_config = Arc::new(AppConfig {
            profile: Profile::Smoke, node: "projection-test".into(),
            token: "private-test-credential".into(), asynchronous: true,
            simulator: None, container_image: None, fence_image: None,
            offline_ms: 300_000, reservation_ms: 60_000,
        });
        let graph = deployment::graph(&app_config);
        let bucket = Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
        let signer = Arc::new(PeerProbeSigner::generate());
        let auth = Arc::new(PeerAuth::new([19; 32], "projection-test")?);
        let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut config = NodeConfig::new(ActorIdentity {
            node: "projection-test".into(), advertise: internal_listener.local_addr()?.to_string(),
            region: "test".into(),
        }, ActorOptions {
            core: ActorPolicy { max_resident: 32, max_cached_routes: 0,
                max_activations: 8, max_evictions: 4, max_releases: 4, max_outbound_websockets: 8,
                require_node_lease: true, peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                ownership_on_evict: OwnershipOnEvict::Sticky,
                operation_deadline_ms: Some(5_000), owner_log_recovery_backoff_ms: 50,
                owner_log_recovery_attempts: 3, alarm_resident_ms: 100,
                idle_evict_ms: None, pressure: Default::default() },
            generation: signer.public_key_hex(), lease_ttl_ms: 5_000,
            placement_weight: 1, resume_generation: None,
        }, directory.path().join("node"));
        config.maintenance.rebalance = false;
        config.generation_max_age = Duration::ZERO;
        config.shutdown = ShutdownPolicy { total_ms: 10_000, token_wait_ms: 0, no_progress_ms: 5_000 };
        let replica_storage = ReplicaStorage::new(bucket.store.clone(), Default::default())?;
        let services = NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: host.handle(), core_executor: core.handle(), listener, internal_listener,
            peer_http: client.clone(), peer_auth: auth.clone(), probe_signer: signer,
            fleet: Some(NodeFleet {
                lease_bucket: bucket.clone(), bucket, replica_storage,
                replication: ReplicationOptions { flush_interval: Duration::ZERO, compaction: None, ..Default::default() },
                log_transport: Arc::new(SignedPeerTransport::from_clients(client.clone(), client.clone(), auth)),
                log: LogOptions::default(), fleet_durability: false,
            }), containers: None, metrics: None,
        };
        let applications = CompiledApplications::default().register(SCRIPT, VERSION, move || {
            fleetcare::application(app_config.clone())
        })?;
        let node = Node::start(config, services, graph, applications).await?;
        let url = format!("http://{}", node.info().public_address);
        let result = async {
            for partition in 0..3 {
                call(&client, &url, &format!("/system/sql/0/{partition}"), Some(&json!({}))).await?;
            }
            call(&client, &url, "/system/dlq/0/0", Some(&json!({
                "effect_id":"recoverable-projection", "kind":"projection", "state":{"version":0},
            }))).await?;
            let started = std::time::Instant::now();
            loop {
                let mut applied = false;
                for partition in 0..3 {
                    let rows = call(&client, &url, &format!("/system/sql/0/{partition}?after=seed-dlq-"), None).await?;
                    applied |= rows["rows"].as_array().unwrap().iter().any(|row| {
                        row["source_scope"] == "seed-dlq-0" && row["version"] == 0
                    });
                    let rows = call(&client, &url, &format!("/system/sql/0/{partition}?quarantine=1"), None).await?;
                    ensure!(!rows["rows"].as_array().unwrap().iter().any(|row| row["effect_id"] == "recoverable-projection"),
                        "valid dead-lettered projection was quarantined");
                }
                if applied { break; }
                ensure!(started.elapsed() < Duration::from_secs(5), "dead-lettered projection did not recover");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            call(&client, &url, "/system/dlq/0/0", Some(&json!({"effect_id":"permanent-poison","kind":"poison"}))).await?;
            call(&client, &url, "/system/dlq/0/0", Some(&json!({
                "effect_id":"malformed-projection", "kind":"projection", "state":{"version":9},
            }))).await?;
            let started = std::time::Instant::now();
            loop {
                let mut poison = false;
                let mut malformed = false;
                for partition in 0..3 {
                    let rows = call(&client, &url, &format!("/system/sql/0/{partition}?quarantine=1"), None).await?;
                    poison |= rows["rows"].as_array().unwrap().iter().any(|row| row["effect_id"] == "permanent-poison");
                    malformed |= rows["rows"].as_array().unwrap().iter().any(|row| row["effect_id"] == "malformed-projection");
                }
                if poison && malformed { break; }
                ensure!(started.elapsed() < Duration::from_secs(5), "poison or malformed projection was not quarantined");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok::<_, anyhow::Error>(())
        }.await;
        let outcome = node.shutdown(ShutdownMode::Preserve).await?;
        ensure!(matches!(outcome, NodeOutcome::Stopped(ref report) if report.cells_drained && !report.drain_timed_out),
                "test node did not drain: {outcome:?}");
        result
    })
}

async fn call(
    client: &reqwest::Client,
    url: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<Value> {
    let mut request = match body {
        Some(body) => client.post(format!("{url}{path}")).json(body),
        None => client.get(format!("{url}{path}")),
    };
    request = request.bearer_auth("private-test-credential");
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(status.is_success(), "{path}: {status}: {text}");
    Ok(serde_json::from_str(&text)?)
}
