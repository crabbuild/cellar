// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real SDK node ownership; no unmanaged controller or per-cell log fallback.
use super::*;
use cellar::{
    engine_api::{Compat, WorkerConfigOptions},
    fleet::LoadedDeployment,
};
use tokio::net::TcpListener;

pub(super) async fn node(
    config: &configuration::Configuration,
    path: PathBuf,
    application: Arc<dyn Application>,
) -> Result<Node> {
    let public = TcpListener::bind("127.0.0.1:0").await?;
    let internal = TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let mut options = NodeConfig::new(
        ActorIdentity {
            node: config.label.clone(),
            advertise: internal.local_addr()?.to_string(),
            region: "qualification".into(),
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
                operation_deadline_ms: Some(3000),
                owner_log_recovery_backoff_ms: 50,
                owner_log_recovery_attempts: 3,
                alarm_resident_ms: 100,
                idle_evict_ms: None,
                pressure: Default::default(),
            },
            lease_ttl_ms: 1000,
            placement_weight: 1,
            generation: signer.public_key_hex(),
            resume_generation: None,
        },
        path,
    );
    options.maintenance.rebalance = false;
    let applications =
        CompiledApplications::default()
            .register(&config.label, "managed", move || Ok(application.clone()))?;
    Node::start(
        options,
        NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: tokio::runtime::Handle::current(),
            core_executor: tokio::runtime::Handle::current(),
            listener: public,
            internal_listener: internal,
            peer_http: reqwest::Client::builder().no_proxy().build()?,
            peer_auth: Arc::new(PeerAuth::new([11; 32], &config.label)?),
            probe_signer: signer,
            fleet: None,
            containers: None,
            metrics: None,
        },
        graph(&config.label),
        applications,
    )
    .await
}

pub(super) async fn cells(
    node: &Node,
    config: &configuration::Configuration,
    bucket: &cellar::bucket::Bucket,
    catalog: &FleetCatalog,
    application: Arc<dyn Application>,
    retained: bool,
) -> Result<RaftCells> {
    let snapshot = catalog.read(TIMEOUT).await?;
    let authorization = catalog
        .authorize_host(&snapshot, NodeId::new(config.node)?, TIMEOUT)
        .await?;
    let mut namespace = NamespaceConfig::new(
        ClusterId::new(id(&config.connection.cluster_id)?)?,
        NamespaceId::new(id(&config.namespace().namespace_id)?)?,
        NamespaceMode::Raft,
        digest(&config.namespace().schema_digest)?,
    );
    namespace.image_storage = config.namespace().image_storage;
    let namespace = catalog.bind_namespace(namespace)?;
    let declaration = RaftNamespaceRegistration {
        namespace: namespace.clone(),
        class: "Counter".into(),
    };
    let listeners = NodeRaftListeners {
        control: TcpListener::bind((config.bind, config.peer_ports[0])).await?,
        data: TcpListener::bind((config.bind, config.peer_ports[1])).await?,
        query: TcpListener::bind((config.bind, config.peer_ports[2])).await?,
    };
    let peer = config.peer()?;
    let mode = if retained {
        LogOpenMode::Resume
    } else {
        LogOpenMode::Create
    };
    let mut log = ReplicaLogOptions::new(
        node.info().data_dir.join("raft"),
        peer.node,
        peer.disk,
        peer.process,
        mode,
    );
    log.max_groups = 2;
    let images = 256 * 1024 * 1024;
    let mut host = RaftHostOptions {
        max_groups: 2,
        max_input_bytes: 1024,
        ..Default::default()
    };
    host.images.max_bytes = images;
    host.execution.max_image_bytes = images / 8;
    host.node_profile = Some(RaftNodeProfile {
        pool: "qualification".into(),
        region: "local".into(),
        zone: "node".into(),
        group_capacity: 2,
        byte_capacity: images * 2,
        bytes_per_group: images,
        leader_capacity: 2,
        draining: false,
    });
    host.node_directory = Some(Default::default());
    host.managed = Some(RaftManagedHostOptions {
        authorization,
        namespaces: vec![declaration],
    });
    host.extents = config.rooted().then(Default::default);
    let tls = RaftTls::new(
        vec![std::fs::read(&config.ca)?.into()],
        vec![std::fs::read(&config.certificate)?.into()],
        rustls::pki_types::PrivateKeyDer::try_from(std::fs::read(&config.key)?)
            .map_err(|error| anyhow::anyhow!(error))?,
    )?;
    node.start_raft_cells(
        RaftCellsConfig {
            infrastructure: NodeRaftConfig {
                log,
                consensus: RaftRuntimeOptions {
                    max_groups: 2,
                    ..Default::default()
                },
                transport: Default::default(),
            },
            catalog: CatalogOptions {
                prefix: config.namespace().catalog_prefix.clone(),
                ..Default::default()
            },
            namespaces: vec![RaftNamespaceConfig {
                namespace,
                class: "Counter".into(),
                initialize: None,
            }],
            groups: vec![],
            peers: vec![peer],
            query_origins: vec![],
            command_origins: vec![],
            host,
            shared: configuration::limits(),
        },
        RaftCellsServices {
            recovery: None,
            listeners,
            tls,
            store: config.store(bucket),
            application,
            versions: VersionKeys::new(
                VersionSigningKey::new([31; 32], u64::MAX)?,
                vec![],
                Duration::from_secs(86400),
            )?,
        },
    )
    .await
}

fn graph(label: &str) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![],
        native: None,
        script_name: label.into(),
        version: "managed".into(),
        prefix: "managed-voter/".into(),
        options: WorkerConfigOptions {
            stream_bindings: vec![],
            src: String::new(),
            script_name: label.into(),
            do_classes: vec![],
            bindings: vec![],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: label.into(),
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
