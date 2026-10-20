//! Three local nodes, two Raft cells, native HTTP and Axum over the same SDK.
//! Usage: cellar-raft-counter DATA_DIR CA.der CERT.der KEY.der TOKEN_FILE
#![allow(clippy::disallowed_methods)]
mod counter;
use anyhow::{ensure, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::{consensus::*, *};
use std::{path::PathBuf, sync::Arc, time::Duration};

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

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() == 5,
        "usage: cellar-raft-counter DATA_DIR CA.der CERT.der KEY.der TOKEN_FILE"
    );
    let root = PathBuf::from(&args[0]);
    std::fs::create_dir_all(root.join("catalog"))?;
    let tls = RaftTls::new(
        vec![std::fs::read(&args[1])?.into()],
        vec![std::fs::read(&args[2])?.into()],
        rustls::pki_types::PrivateKeyDer::try_from(std::fs::read(&args[3])?)
            .map_err(|error| anyhow::anyhow!(error))?,
    )?;
    let secret = std::fs::read_to_string(&args[4])?.trim().to_owned();
    let store = cellar::bucket::Bucket::open_dev(&root.join("catalog.sqlite"))?.store;
    let namespace = NamespaceId::new([2; 16])?;
    let cluster = ClusterId::new([1; 16])?;
    let mut nodes = Vec::new();
    let mut services = Vec::new();
    let mut servers = Vec::new();
    let (stop, _) = tokio::sync::watch::channel(false);
    // Join every successfully started owner even if a later listener/service
    // fails. The caller's executor stays alive through this cleanup.
    let serving = async {
    let mut apis = Vec::new();
    let mut applications = Vec::new();
    let mut listeners = Vec::new();
    let mut peers = Vec::new();
    for i in 0..3_u8 {
        let label = format!("raft-counter-{}", i + 1);
        let class = counter::definition()?;
        let api = counter::CounterApi::new(secret.clone(), namespace, &class)?;
        let application = counter::application(api.clone(), class)?;
        let public =
            tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 9901 + u16::from(i)))
                .await?;
        let internal = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let signer = Arc::new(PeerProbeSigner::generate());
        let config = NodeConfig::new(
            ActorIdentity {
                node: label.clone(),
                advertise: internal.local_addr()?.to_string(),
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
            root.join(&label),
        );
        let app = application.clone();
        nodes.push(
            Node::start(
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
                CompiledApplications::default()
                    .register(&label, "example", move || Ok(app.clone()))?,
            )
            .await?,
        );
        let control = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let data = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let query = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut process = [0; 16];
        getrandom::fill(&mut process)?;
        peers.push(RaftPeerNode {
            node: NodeId::new([i + 1; 16])?,
            disk: DiskLineage::new([i + 10; 16])?,
            process: ProcessGeneration::new(process)?,
            label,
            server_name: "localhost".into(),
            control_address: control.local_addr()?,
            data_address: data.local_addr()?,
                query_address: query.local_addr()?,
        });
        listeners.push(NodeRaftListeners { control, data, query });
        apis.push(api);
        applications.push(application);
    }
    let mut config = NamespaceConfig::new(
        cluster,
        namespace,
        NamespaceMode::Raft,
        counter::definition()?.schema_digest(),
    );
    config.voters = VoterPolicy::ThreeVoters;
    let registrations: Vec<_> = ["one", "two"]
        .iter()
        .enumerate()
        .map(|(i, name)| RaftCellRegistration {
            placement: cellar::sdk::consensus::RaftCellPlacement::Replica,
            namespace: config.clone(),
            class: "Counter".into(),
            birth: BirthProposal {
                scope: GroupScope {
                    cluster,
                    namespace,
                    cell: format!("Counter:{name}"),
                    group: GroupId::new([i as u8 + 5; 16]).unwrap(),
                    incarnation: IncarnationId::new([4; 16]).unwrap(),
                },
                voters: peers
                    .iter()
                    .enumerate()
                    .map(|(i, peer)| InitialMember {
                        node: peer.node,
                        member: MemberId::new(i as u64 + 1).unwrap(),
                    })
                    .collect(),
                learners: vec![],
            },
        })
        .collect();
    for (i, listeners) in listeners.into_iter().enumerate() {
        let log_dir = nodes[i].info().data_dir.join("raft");
        let mode = if log_dir.exists() {
            LogOpenMode::Resume
        } else {
            LogOpenMode::Create
        };
        let service = nodes[i]
            .start_raft_cells(
                RaftCellsConfig {
            query_origins: vec![],
                    infrastructure: NodeRaftConfig {
                        log: ReplicaLogOptions::new(
                            log_dir,
                            peers[i].node,
                            peers[i].disk,
                            peers[i].process,
                            mode,
                        ),
                        consensus: RaftRuntimeOptions {
                            tick_interval_ms: Some(50),
                            ..Default::default()
                        },
                        transport: TransportOptions::default(),
                    },
                    catalog: CatalogOptions {
                        prefix: "demo/raft-counter".into(),
                        ..Default::default()
                    },
                    cells: registrations.clone(),
                    peers: peers.clone(),
                    host: RaftHostOptions::default(),
                },
                RaftCellsServices {
                    listeners,
                    tls: tls.clone(),
                    store: store.clone(),
                    application: applications[i].clone(),
                    // Development-only key, stable across the restart drill.
                    versions: VersionKeys::new(VersionSigningKey::new([29; 32], u64::MAX)?, vec![], Duration::from_secs(86400))?,
                },
            )
            .await?;
        *apis[i]
            .cells
            .lock()
            .map_err(|_| anyhow::anyhow!("counter binding poisoned"))? = Some(service.clone());
        services.push(service);
        let listener =
            tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 9911 + i as u16)).await?;
        let router = counter::router(apis[i].clone());
        let mut stopping = stop.subscribe();
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = stopping.changed().await;
                })
                .await
        }));
    }
    cellar::note!("Raft counter: native ports 9901–9903; Axum ports 9911–9913; cells one/two. Ctrl-C joins all nodes. Three replicas share one machine; this is a development example.");
    tokio::signal::ctrl_c().await?;
        Ok::<(), anyhow::Error>(())
    }.await;
    let _ = stop.send(true);
    let mut failed = false;
    for server in servers {
        failed |= !matches!(server.await, Ok(Ok(())));
    }
    for service in services {
        failed |= !matches!(service.close().await, Ok(report) if report.unfinished.is_empty());
    }
    for node in nodes {
        failed |= node.shutdown(ShutdownMode::Preserve).await.is_err();
    }
    serving?;
    ensure!(
        !failed,
        "incomplete shutdown: retain disks and retry original command IDs"
    );
    Ok(())
}
