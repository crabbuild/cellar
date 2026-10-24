// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(dead_code, clippy::disallowed_methods)]
#[path = "../../../cellar/tests/support/html.rs"]
mod fixture;

use anyhow::{ensure, Result};
use axum::{routing::get, Router};
use cellar::sdk::*;
use cellar_axum::{ApplicationDefinition, CellarService};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::oneshot;

pub fn graph() -> DeploymentGraph {
    let mut graph = fixture::graph("adapter", "", vec!["Counter".into()]);
    graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
    graph
}

pub struct Host {
    pub node: Node,
    pub ingress: Option<IngressHandle>,
    pub url: String,
    pub internal_url: String,
    server: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    stop_server: Option<oneshot::Sender<()>>,
}

pub async fn start(
    path: &Path,
    external: bool,
    mount: &str,
    definition: impl Fn() -> Result<ApplicationDefinition> + Send + Sync + 'static,
) -> Result<Host> {
    start_with(path, external, mount, "adapter", None, definition).await
}

pub async fn start_with(
    path: &Path,
    external: bool,
    mount: &str,
    name: &str,
    bucket: Option<cellar::bucket::Bucket>,
    definition: impl Fn() -> Result<ApplicationDefinition> + Send + Sync + 'static,
) -> Result<Host> {
    start_configured(path, external, mount, name, bucket, definition, |_| {}).await
}

pub async fn start_configured(
    path: &Path,
    external: bool,
    mount: &str,
    name: &str,
    bucket: Option<cellar::bucket::Bucket>,
    definition: impl Fn() -> Result<ApplicationDefinition> + Send + Sync + 'static,
    configure_graph: impl FnOnce(&mut DeploymentGraph),
) -> Result<Host> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let public_address = listener.local_addr()?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_address = internal_listener.local_addr()?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident: 16,
            max_cached_routes: 0,
            max_activations: 8,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(5_000),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 1_000,
        placement_weight: 1,
        generation: signer.public_key_hex(),
        resume_generation: None,
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name.into(),
            region: "test".into(),
            advertise: internal_address.to_string(),
        },
        actor,
        path.to_path_buf(),
    );
    config.maintenance.rebalance = false;
    config.shutdown = ShutdownPolicy {
        total_ms: 5_000,
        token_wait_ms: 0,
        no_progress_ms: 2_000,
    };
    let mut graph = graph();
    configure_graph(&mut graph);
    let applications = CompiledApplications::default()
        .register("adapter", "test", move || definition()?.instantiate())?;
    let peer_auth = Arc::new(PeerAuth::new([11; 32], name)?);
    let peer_http = reqwest::Client::new();
    let fleet = bucket
        .map(|bucket| -> Result<NodeFleet> {
            Ok(NodeFleet {
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
                    durability_timeout: Duration::from_secs(2),
                    compaction: None,
                    ..Default::default()
                },
                log_transport: Arc::new(SignedPeerTransport::from_clients(
                    peer_http.clone(),
                    peer_http.clone(),
                    peer_auth.clone(),
                )),
                log: LogOptions {
                    record_publish_timeout: Duration::from_secs(3),
                    ..Default::default()
                },
                fleet_durability: false,
            })
        })
        .transpose()?;
    let executor = tokio::runtime::Handle::current();
    let (node, ingress, server, stop_server) = if external {
        let (node, ingress) = Node::start_external(
            config,
            ExternalNodeServices {
                wall_clock: None,
                http_client: None,
                host_executor: executor.clone(),
                core_executor: executor,
                public_address,
                ingress_capacity: 128,
                internal_listener,
                peer_http,
                peer_auth,
                probe_signer: signer,
                fleet,
                containers: None,
                metrics: None,
            },
            graph,
            applications,
        )
        .await?;
        let service = CellarService::new(ingress.clone());
        let app = if mount.is_empty() {
            Router::new().fallback_service(service)
        } else {
            Router::new().nest_service(mount, service)
        };
        let app = app.route("/outside", get(|| async { "ordinary Axum" }));
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        });
        (node, Some(ingress), Some(server), Some(stop))
    } else {
        ensure!(mount.is_empty(), "native fixture uses canonical root paths");
        let node = Node::start(
            config,
            NodeServices {
                wall_clock: None,
                http_client: None,
                host_executor: executor.clone(),
                core_executor: executor,
                listener,
                internal_listener,
                peer_http,
                peer_auth,
                probe_signer: signer,
                fleet,
                containers: None,
                metrics: None,
            },
            graph,
            applications,
        )
        .await?;
        (node, None, None, None)
    };
    Ok(Host {
        node,
        ingress,
        url: format!("http://{public_address}{mount}"),
        internal_url: format!("http://{internal_address}"),
        server,
        stop_server,
    })
}

impl Host {
    pub async fn cancel(self) -> Result<NodeOutcome> {
        if let Some(ingress) = self.ingress {
            ingress.close_admission();
        }
        if let Some(stop) = self.stop_server {
            let _ = stop.send(());
        }
        let outcome = self.node.cancel().await?;
        if let Some(server) = self.server {
            server.abort();
            let _ = server.await;
        }
        Ok(outcome)
    }

    pub async fn stop(self) -> Result<ShutdownReport> {
        self.stop_mode(ShutdownMode::Preserve).await
    }

    pub async fn stop_mode(self, mode: ShutdownMode) -> Result<ShutdownReport> {
        let report = self.shutdown_report(mode).await?;
        ensure!(
            report.ingress_drained
                && report.application_work_drained
                && report.cells_drained
                && report.pending_dispatches == 0
                && !report.drain_timed_out,
            "incomplete shutdown {report:?}"
        );
        Ok(report)
    }

    pub async fn shutdown_report(self, mode: ShutdownMode) -> Result<ShutdownReport> {
        if let Some(ingress) = self.ingress {
            ingress.close_admission();
        }
        if let Some(stop) = self.stop_server {
            let _ = stop.send(());
        }
        let outcome = self.node.shutdown(mode).await?;
        if let Some(server) = self.server {
            tokio::time::timeout(Duration::from_secs(8), server).await???;
        }
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected node outcome {outcome:?}");
        };
        Ok(report)
    }
}

pub fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?)
}
