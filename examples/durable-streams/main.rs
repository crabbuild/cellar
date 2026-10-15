//! Standalone bucket-backed stream example with caller-owned executors.
// Filesystem bootstrap is a process-boundary effect before Node admission.
#![allow(clippy::disallowed_methods)]

mod application;

use anyhow::{ensure, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::*;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

fn graph(faults: bool, stream_rows: Option<usize>) -> DeploymentGraph {
    let mut definition = application::definition();
    if faults {
        definition.max_records = stream_rows.unwrap_or(1) as i64;
    }
    DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![definition],
        native: None,
        script_name: application::SCRIPT.into(),
        version: "v1".into(),
        prefix: "stream-example/v1".into(),
        options: WorkerConfigOptions {
            stream_bindings: vec![
                application::binding(false).into(),
                application::binding(true).into(),
            ],
            src: String::new(),
            script_name: application::SCRIPT.into(),
            do_classes: if faults {
                vec!["Order".into(), "Projection".into(), "Backlog".into()]
            } else {
                vec!["Order".into(), "Projection".into()]
            },
            bindings: if faults {
                vec![
                    ("ORDERS".into(), "Order".into()),
                    ("PROJECTIONS".into(), "Projection".into()),
                    ("BACKLOG".into(), "Backlog".into()),
                ]
            } else {
                vec![
                    ("ORDERS".into(), "Order".into()),
                    ("PROJECTIONS".into(), "Projection".into()),
                ]
            },
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
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetFixture {
    node: String,
    bucket: FixtureBucket,
    peer: SocketAddr,
    advertise: SocketAddr,
    declare: bool,
    fleet_durability: bool,
    outbox_rows: Option<usize>,
    stream_rows: Option<usize>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum FixtureBucket {
    Local(PathBuf),
    Network(NetworkBucket),
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NetworkBucket {
    endpoint: String,
    name: String,
}

impl FixtureBucket {
    fn open(&self) -> Result<cellar::bucket::Bucket> {
        match self {
            Self::Local(path) => cellar::bucket::Bucket::open_dev(path),
            Self::Network(config) => {
                use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
                let endpoint = reqwest::Url::parse(&config.endpoint)?;
                ensure!(
                    endpoint.scheme() == "http"
                        && endpoint.host_str() == Some("127.0.0.1")
                        && endpoint.port().is_some()
                        && endpoint.username().is_empty()
                        && endpoint.password().is_none()
                        && endpoint.path() == "/"
                        && endpoint.query().is_none()
                        && endpoint.fragment().is_none(),
                    "network fixtures require a loopback HTTP object store"
                );
                ensure!(
                    !config.name.is_empty() && config.name.len() <= 63,
                    "invalid fixture bucket name"
                );
                // Only this opt-in application fixture captures credentials. The
                // framework receives explicit clients, including no-retry CAS.
                let builder = AmazonS3Builder::new()
                    .with_bucket_name(&config.name)
                    .with_region("us-east-1")
                    .with_endpoint(&config.endpoint)
                    .with_allow_http(true)
                    .with_virtual_hosted_style_request(false)
                    .with_access_key_id(std::env::var("CELLAR_STREAM_FIXTURE_ACCESS")?)
                    .with_secret_access_key(std::env::var("CELLAR_STREAM_FIXTURE_SECRET")?)
                    .with_conditional_put(S3ConditionalPut::ETagMatch);
                let store = Arc::new(builder.clone().build()?);
                let conditional = Arc::new(
                    builder
                        .with_retry(object_store::RetryConfig {
                            max_retries: 0,
                            retry_timeout: Duration::from_secs(30),
                            ..Default::default()
                        })
                        .build()?,
                );
                cellar::bucket::Bucket::from_clients(
                    store.clone(),
                    conditional,
                    store,
                    cellar::bucket::StorageBackend::S3,
                    config.name.clone(),
                    String::new(),
                )
            }
        }
    }
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let fixture =
        if args.get(2).is_some_and(|arg| arg == "--fleet-fixture") {
            ensure!(
                args.len() == 4,
                "--fleet-fixture requires a configuration file"
            );
            let path = PathBuf::from(&args[3]);
            ensure!(
                std::fs::metadata(&path)?.len() <= 4096,
                "fleet fixture configuration exceeds limit"
            );
            let fixture: FleetFixture = serde_json::from_slice(&std::fs::read(path)?)?;
            ensure!(
                !fixture.node.is_empty()
                    && fixture.node.len() <= 128
                    && fixture.peer.ip().is_loopback()
                    && fixture.advertise.ip().is_loopback(),
                "fleet fixtures require a bounded node identity and loopback peers"
            );
            Some(fixture)
        } else {
            ensure!(args.len()<=3 && args.get(2).is_none_or(|arg|arg=="--fault-harness"),
            "usage: cellar-example-streams [DATA] [PORT] [--fault-harness | --fleet-fixture FILE]");
            None
        };
    let faults = args.get(2).is_some();
    if faults && std::env::var_os("CELLAR_STREAM_FIXTURE_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .with_env_filter("off,cellar::daemon::event_resources=debug")
            .try_init()
            .map_err(|error| anyhow::anyhow!("fixture tracing: {error}"))?;
    }
    let node_name = fixture
        .as_ref()
        .map_or(application::SCRIPT, |fixture| fixture.node.as_str());
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
        let bucket = match &fixture {
            Some(fixture) => fixture.bucket.open()?,
            None => cellar::bucket::Bucket::open_dev(&data.join("fleet.sqlite"))?,
        };
        let bootstrap = ExecutionDomain::new(host.handle().clone());
        let key = bootstrap
            .run(cellar::peer_auth::load_or_create(&bucket))
            .await??;
        bootstrap.shutdown().await;
        let peer = fixture
            .as_ref()
            .map(|fixture| fixture.peer)
            .unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
        let internal_listener = tokio::net::TcpListener::bind(peer).await?;
        let signer = Arc::new(PeerProbeSigner::generate());
        let mut config = NodeConfig::new(
            ActorIdentity {
                node: node_name.into(),
                advertise: fixture
                    .as_ref()
                    .map(|fixture| fixture.advertise)
                    .unwrap_or(internal_listener.local_addr()?)
                    .to_string(),
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
        if let Some(rows) = fixture.as_ref().and_then(|fixture| fixture.outbox_rows) {
            ensure!(
                (1..=10000).contains(&rows),
                "fixture outbox row limit must be between 1 and 10000"
            );
            config.cell_limits.events.max_outbox_rows = rows;
        }
        config.runtime.max_cell_requests = 16;
        config.runtime.handler_budget = Duration::from_secs(10);
        config.maintenance.rebalance = false;
        let peer_http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(5))
            .build()?;
        let peer_auth = Arc::new(PeerAuth::new(key, node_name)?);
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
            fleet_durability: fixture
                .as_ref()
                .is_none_or(|fixture| fixture.fleet_durability),
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
        let applications = application::applications(
            faults.then(|| Arc::new(application::FaultControls::for_node(node_name))),
        )?;
        #[cfg(unix)]
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let stream_rows = fixture.as_ref().and_then(|fixture| fixture.stream_rows);
        ensure!(
            stream_rows.is_none_or(|rows| (1..=256).contains(&rows)),
            "fixture stream rows must be between 1 and 256"
        );
        let mut deployment = graph(faults, stream_rows);
        if fixture.as_ref().is_some_and(|fixture| !fixture.declare) {
            deployment.primary.event_streams.clear();
        }
        let node = Node::start(config, services, deployment, applications).await?;
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
