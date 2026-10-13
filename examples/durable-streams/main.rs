//! Standalone bucket-backed stream example with caller-owned executors.
// Filesystem bootstrap is a process-boundary effect before Node admission.
#![allow(clippy::disallowed_methods)]

mod application;

use anyhow::{ensure, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::*;
use std::{path::PathBuf, sync::Arc, time::Duration};

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![application::definition()],
        native: None,
        script_name: application::SCRIPT.into(),
        version: "v1".into(),
        prefix: "stream-example/v1".into(),
        options: WorkerConfigOptions {
            stream_bindings: vec![application::binding(false), application::binding(true)],
            src: String::new(),
            script_name: application::SCRIPT.into(),
            do_classes: vec!["Order".into(), "Projection".into()],
            bindings: vec![
                ("ORDERS".into(), "Order".into()),
                ("PROJECTIONS".into(), "Projection".into()),
            ],
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            blob_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}

fn main() -> Result<()> {
    let data = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "examples/.cellar/durable-streams".into()),
    );
    let port = std::env::args()
        .nth(2)
        .map(|value| value.parse::<u16>())
        .transpose()?
        .unwrap_or(9876);
    std::fs::create_dir_all(&data)?;
    // The application owns these executors and keeps them alive through the node's join.
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    host.block_on(async {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        let public_address = listener.local_addr()?;
        let bucket = cellar::bucket::Bucket::open_dev(&data.join("fleet.sqlite"))?;
        let bootstrap = ExecutionDomain::new(host.handle().clone());
        let key = bootstrap
            .run(cellar::peer_auth::load_or_create(&bucket))
            .await??;
        bootstrap.shutdown().await;
        let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let signer = Arc::new(PeerProbeSigner::generate());
        let mut config = NodeConfig::new(
            ActorIdentity {
                node: application::SCRIPT.into(),
                advertise: internal_listener.local_addr()?.to_string(),
                region: "local".into(),
            },
            ActorOptions {
                core: ActorPolicy {
                    max_resident: 128,
                    max_cached_routes: 256,
                    max_activations: 16,
                    max_evictions: 4,
                    max_releases: 8,
                    max_outbound_websockets: 16,
                    ownership_on_evict: OwnershipOnEvict::Sticky,
                    require_node_lease: true,
                    peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                    operation_deadline_ms: Some(10000),
                    owner_log_recovery_backoff_ms: 1000,
                    owner_log_recovery_attempts: 10,
                    alarm_resident_ms: 1000,
                    idle_evict_ms: None,
                    pressure: Default::default(),
                },
                lease_ttl_ms: 30000,
                placement_weight: 1,
                generation: signer.public_key_hex(),
                resume_generation: None,
            },
            data.join("node"),
        );
        config.runtime.max_stateless_requests = Some(128);
        config.runtime.max_cell_requests = 16;
        config.runtime.handler_budget = Duration::from_secs(10);
        config.maintenance.rebalance = false;
        let peer_http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .build()?;
        let peer_auth = Arc::new(PeerAuth::new(key, application::SCRIPT)?);
        let fleet = NodeFleet {
            replica_storage: ReplicaStorage::new(
                bucket.store.clone(),
                ReplicaStorageOptions {
                    prefix: bucket.prefix.clone(),
                    ..Default::default()
                },
            )?,
            lease_bucket: bucket.clone(),
            bucket,
            replication: ReplicationOptions {
                flush_interval: Duration::ZERO,
                compaction: None,
                ..Default::default()
            },
            log_transport: Arc::new(SignedPeerTransport::from_clients(
                peer_http.clone(),
                peer_http.clone(),
                peer_auth.clone(),
            )),
            log: LogOptions::default(),
            fleet_durability: true,
        };
        let services = NodeServices {
            http_client: None,
            host_executor: host.handle().clone(),
            core_executor: core.handle().clone(),
            listener,
            internal_listener,
            peer_http,
            peer_auth,
            probe_signer: signer,
            fleet: Some(fleet),
            containers: None,
            metrics: None,
        };
        let applications = application::applications()?;
        #[cfg(unix)]
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let node = Node::start(config, services, graph(), applications).await?;
        cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
            "{}",
            serde_json::json!({"ready":format!("http://{public_address}")})
        ))?;
        let outcome = node
            .run_until(async {
                #[cfg(unix)]
                cellar::asyncrt::select! {
                    _ = terminate.recv() => {},
                    _ = tokio::signal::ctrl_c() => {}
                }
                #[cfg(not(unix))]
                let _ = tokio::signal::ctrl_c().await;
                ShutdownMode::Preserve
            })
            .await?;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("node failed: {outcome:?}");
        };
        ensure!(
            report.ingress_drained
                && report.application_work_drained
                && report.cells_drained
                && report.pending_dispatches == 0
                && !report.drain_timed_out,
            "incomplete shutdown: {report:?}"
        );
        cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
            "{}",
            serde_json::json!({
                "outcome": "stopped",
                "ingress_drained": report.ingress_drained,
                "application_work_drained": report.application_work_drained,
                "cells_drained": report.cells_drained,
            })
        ))?;
        Ok(())
    })
}
