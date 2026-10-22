//! Isolated one-process-per-container native multi-Raft measurement node.
#![allow(clippy::disallowed_methods)]
#[path = "../../examples/raft-counter/counter.rs"]
mod counter;
use anyhow::{ensure, Context, Result};
use cellar::bucket::{Bucket, StorageBackend};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::{consensus::*, *};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn graph(script: &str) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![],
        native: None,
        script_name: script.into(),
        version: "example".into(),
        prefix: "raft-counter".into(),
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: script.into(),
            do_classes: vec![],
            bindings: vec![],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            stream_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: script.into(),
            modules: vec![],
            compat: Compat::default(),
        },
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        cron_revision: None,
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .init();
    let i: u8 = std::env::var("RAFT_NODE")?.parse()?;
    let total: usize = std::env::var("RAFT_CELLS")?.parse()?;
    let tick_ms: u64 = std::env::var("RAFT_TICK_MS")
        .unwrap_or_else(|_| "100".into())
        .parse()?;
    ensure!(
        (1..=3).contains(&i) && (1..=4096).contains(&total),
        "invalid node/cell count"
    );
    let queue_items: usize = std::env::var("RAFT_QUEUE_ITEMS")
        .unwrap_or_else(|_| "256".into())
        .parse()?;
    let primary_only = std::env::var("RAFT_PRIMARY_ONLY").as_deref() == Ok("1");
    let root = PathBuf::from("/data");
    let secret = std::fs::read_to_string("/config/token")?.trim().to_owned();
    let tls = RaftTls::new(
        vec![std::fs::read("/config/ca.der")?.into()],
        vec![std::fs::read("/config/server.der")?.into()],
        rustls::pki_types::PrivateKeyDer::try_from(std::fs::read("/config/key.der")?)
            .map_err(|e| anyhow::anyhow!(e))?,
    )?;
    let builder = AmazonS3Builder::new()
        .with_bucket_name("raft-perf")
        .with_region("us-east-1")
        .with_endpoint("http://rustfs:9000")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY")?)
        .with_conditional_put(S3ConditionalPut::ETagMatch);
    let ordinary = Arc::new(builder.clone().build()?);
    let conditional = Arc::new(
        builder
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()?,
    );
    let bucket = Bucket::from_clients(
        ordinary.clone(),
        conditional,
        ordinary,
        StorageBackend::S3,
        "raft-perf".into(),
        String::new(),
    )?;
    let store = bucket.raft_store();
    let namespace = NamespaceId::new([2; 16])?;
    let cluster = ClusterId::new([1; 16])?;
    let label = format!("raft-node{i}");
    let class = counter::definition()?;
    let api = counter::CounterApi::new(secret.clone(), namespace, &class)?;
    let application = counter::application(api.clone(), class)?;
    let public = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    let internal = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let config = NodeConfig::new(
        ActorIdentity {
            node: label.clone(),
            advertise: format!("172.29.81.{}:8081", i + 10),
            region: "local-demo".into(),
        },
        ActorOptions {
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
                operation_deadline_ms: Some(5000),
                owner_log_recovery_backoff_ms: 100,
                owner_log_recovery_attempts: 3,
                alarm_resident_ms: 100,
                idle_evict_ms: None,
                pressure: Default::default(),
            },
            lease_ttl_ms: 5000,
            placement_weight: 1,
            generation: signer.public_key_hex(),
            resume_generation: None,
        },
        root.join("node"),
    );
    let app = application.clone();
    let node = Node::start(
        config,
        NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: tokio::runtime::Handle::current(),
            core_executor: tokio::runtime::Handle::current(),
            listener: public,
            internal_listener: internal,
            peer_http: reqwest::Client::builder().no_proxy().build()?,
            peer_auth: Arc::new(PeerAuth::new([19; 32], &label)?),
            probe_signer: signer,
            fleet: None,
            containers: None,
            metrics: None,
        },
        graph(&label),
        CompiledApplications::default().register(&label, "example", move || Ok(app.clone()))?,
    )
    .await?;
    let mut peers = Vec::new();
    for n in 1..=3u8 {
        let address = |port| format!("172.29.81.{}:{port}", n + 10).parse();
        peers.push(RaftPeerNode {
            node: NodeId::new([n; 16])?,
            disk: DiskLineage::new([n + 10; 16])?,
            process: ProcessGeneration::new(
                std::fs::read(format!("/config/process{n}"))?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("process identity length"))?,
            )?,
            label: format!("raft-node{n}"),
            server_name: "localhost".into(),
            control_address: address(9100)?,
            data_address: address(9101)?,
            query_address: address(9102)?,
        });
    }
    // Fresh process identity on every restart; peers authenticate physical routes.
    // The runner persists the peer directory per run and replaces all processes together.
    let mut config = NamespaceConfig::new(
        cluster,
        namespace,
        NamespaceMode::Raft,
        counter::definition()?.schema_digest(),
    );
    config.voters = VoterPolicy::ThreeVoters;
    let cells = (0..total)
        .map(|c| -> Result<_> {
            let mut group = [5; 16];
            group[..8].copy_from_slice(&(c as u64 + 1).to_be_bytes());
            Ok(RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: config.clone(),
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: GroupScope {
                        cluster,
                        namespace,
                        cell: format!("Counter:{c}"),
                        group: GroupId::new(group)?,
                        incarnation: IncarnationId::new([4; 16])?,
                    },
                    voters: peers
                        .iter()
                        .enumerate()
                        .map(|(p, peer)| {
                            Ok(InitialMember {
                                node: peer.node,
                                member: MemberId::new(p as u64 + 1)?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    learners: vec![],
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let log_dir = node.info().data_dir.join("raft");
    let mode = if log_dir.exists() {
        LogOpenMode::Resume
    } else {
        LogOpenMode::Create
    };
    let peer = &peers[i as usize - 1];
    let mut host = RaftHostOptions {
        max_groups: total,
        max_requests: 256,
        ..Default::default()
    };
    host.close_report.max_groups = total;
    let service = node
        .start_raft_cells(
            RaftCellsConfig {
                query_origins: vec![],
                infrastructure: NodeRaftConfig {
                    log: ReplicaLogOptions::new(log_dir, peer.node, peer.disk, peer.process, mode),
                    consensus: RaftRuntimeOptions {
                        tick_interval_ms: Some(tick_ms),
                        max_requests: queue_items,
                        max_events: queue_items,
                        ..Default::default()
                    },
                    transport: TransportOptions {
                        queue_items,
                        peer_control_bytes: 16 * 1024 * 1024,
                        node_control_bytes: 32 * 1024 * 1024,
                        ..Default::default()
                    },
                },
                catalog: CatalogOptions {
                    prefix: "perf/multi-raft".into(),
                    ..Default::default()
                },
                cells,
                peers,
                host,
            },
            RaftCellsServices {
                listeners: NodeRaftListeners {
                    control: tokio::net::TcpListener::bind("0.0.0.0:9100").await?,
                    data: tokio::net::TcpListener::bind("0.0.0.0:9101").await?,
                    query: tokio::net::TcpListener::bind("0.0.0.0:9102").await?,
                },
                tls,
                store,
                application,
                versions: VersionKeys::new(
                    VersionSigningKey::new([29; 32], u64::MAX)?,
                    vec![],
                    Duration::from_secs(86400),
                )?,
            },
        )
        .await?;
    *api.cells
        .lock()
        .map_err(|_| anyhow::anyhow!("poisoned binding"))? = Some(service.clone());
    let admin = service.clone();
    let router = counter::router(api).route("/admin/{id}", axum::routing::any(
        move |axum::extract::Path(id): axum::extract::Path<usize>, request: axum::extract::Request| {
            let service = admin.clone(); let secret = secret.clone();
            async move {
                let authorized = request.headers().get("authorization").and_then(|v| v.to_str().ok()) == Some(format!("Bearer {secret}").as_str());
                let transfer = request.method() == axum::http::Method::POST;
                drop(request);
                let result = async {
                    ensure!(authorized, "unauthorized");
                    ensure!(id < total, "invalid cell");
                    let cell = service.cell(namespace, &format!("Counter:{id}"))?;
                    let status = cell.status().await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
                    if transfer && status.command_ready {
                        let target = MemberId::new(if primary_only { 1 } else { (id % 3 + 1) as u64 })?;
                        if status.leader != Some(target) {
                            cell.transfer_leadership(LeadershipTransfer {target,
                                expected_term: status.term, expected_revision: status.membership_revision}, Duration::from_secs(5))
                                .await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
                        }
                    }
                    Ok::<_,anyhow::Error>(serde_json::json!({"cell":id, "leader":status.leader.map(MemberId::get),
                        "ready": status.command_ready, "role":format!("{:?}",status.role),
                        "term":status.term.get(), "committed":status.committed.get(), "applied":status.applied.index.get(), "archived":status.archived.get()}))
                }.await;
                match result { Ok(v) => (axum::http::StatusCode::OK, v.to_string()),
                    Err(e) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()) }
            }
        }));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8082").await?;
    cellar::note!(
        "raft_perf_ready node={} groups={} tick_ms={} queue_items={} durability=quorum archive=rustfs",
        i,
        total,
        tick_ms,
        queue_items
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal");
            cellar::asyncrt::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        })
        .await?;
    node.shutdown(ShutdownMode::Preserve)
        .await
        .context("node shutdown")?;
    let report = service
        .close()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    cellar::note!("raft_perf_closed {:?}", report);
    Ok(())
}
