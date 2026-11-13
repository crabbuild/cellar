// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed namespace capability and actual node startup fixture.

use super::*;
use object_store::list::*;

pub struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "managed discovery never lists".into(),
        })
    }
}

#[derive(Clone)]
pub struct Fixture {
    pub domain: Arc<ExecutionDomain>,
    pub store: Arc<fault::Store>,
    pub catalog: FleetCatalog,
    pub declaration: RaftNamespaceRegistration,
    pub additional_namespaces: Vec<RaftNamespaceRegistration>,
    pub application: Arc<dyn Application>,
    /// One deployed application declaration, independent of each voter label.
    pub configuration: Option<cellar::engine_api::WorkerConfigOptions>,
    pub external_http: Option<Arc<http::Owner>>,
    /// Captured physical I/O provider for the initial node owner.
    pub filesystem: Arc<dyn cellar_ltx::FileSystem>,
    pub effects: Option<RaftEffectOptions>,
    pub sql_workers: usize,
    pub max_pending_transactions: usize,
    pub max_client_identities: usize,
    pub max_input_bytes: usize,
    pub payloads: Option<CellGroupPayloadLimits>,
    pub image_bytes: u64,
    /// Logical admission only; physical attestations and engine/image limits stay unchanged.
    pub logical_byte_capacity: Option<u64>,
    /// Declared fixture labels only; these do not prove independent hosts.
    pub placement_domains: Option<std::collections::BTreeMap<NodeId, (String, String)>>,
    pub spec: FleetNodeSpec,
}

impl Fixture {
    pub fn version_keys() -> Result<VersionKeys> {
        Ok(VersionKeys::new(
            VersionSigningKey::new([31; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(86400),
        )?)
    }

    pub async fn new(rooted: bool) -> Result<Self> {
        Box::pin(Self::with_class(
            rooted,
            RaftClass::new("Counter", 1, [81; 32])?,
        ))
        .await
    }

    pub async fn with_class(rooted: bool, class: RaftClass) -> Result<Self> {
        Box::pin(Self::with_store(
            rooted,
            class,
            Arc::new(fault::Store::default()),
        ))
        .await
    }

    pub async fn with_store(
        rooted: bool,
        class: RaftClass,
        store: Arc<fault::Store>,
    ) -> Result<Self> {
        let name = class.name().to_owned();
        let application: Arc<dyn Application> =
            Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
        Box::pin(Self::with_application(rooted, &name, application, store)).await
    }

    pub async fn with_application(
        rooted: bool,
        name: &str,
        application: Arc<dyn Application>,
        store: Arc<fault::Store>,
    ) -> Result<Self> {
        Box::pin(Self::with_namespaces(rooted, &[name], application, store)).await
    }

    pub async fn with_namespaces(
        rooted: bool,
        names: &[&str],
        application: Arc<dyn Application>,
        store: Arc<fault::Store>,
    ) -> Result<Self> {
        ensure!(!names.is_empty());
        let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
        let catalog = Self::open_catalog(&domain, store.clone(), ClusterId::new([1; 16])?)?;
        catalog.initialize(TIMEOUT).await?;
        let namespace_catalog = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "managed/".into(),
                ..Default::default()
            },
        )?;
        let mut declarations = Vec::new();
        for (index, name) in names.iter().enumerate() {
            let class = application.resolve_raft_class(name)?;
            let namespace = NamespaceId::new(
                [if index == 0 {
                    2
                } else {
                    180 + u8::try_from(index)?
                }; 16],
            )?;
            let cut = catalog.read(TIMEOUT).await?;
            let identity = request(
                if index == 0 {
                    1
                } else {
                    200 + u8::try_from(index)?
                },
                cut.revision(),
            );
            let mut config = NamespaceConfig::new(
                ClusterId::new([1; 16])?,
                namespace,
                NamespaceMode::Raft,
                class.schema_digest(),
            );
            if rooted {
                config.image_storage = RaftImageStorage::Extents;
            }
            declarations.push(
                catalog
                    .provision_managed_namespace(
                        &identity,
                        &namespace_catalog,
                        config,
                        &class,
                        DirectoryLimits::default(),
                        TIMEOUT,
                    )
                    .await?,
            );
        }
        let declaration = declarations[0].clone();
        let spec = FleetNodeSpec {
            node: NodeId::new([4; 16])?,
            disk: DiskLineage::new([14; 16])?,
            physical_host: IncarnationId::new([90; 16])?,
            label: "shared-managed".into(),
            server_name: "localhost".into(),
            capability_digest: RaftManagedHostOptions::capability_digest(&declarations)?,
            storage_instance: None,
        };
        let cut = catalog.read(TIMEOUT).await?;
        catalog
            .register_node(&cut, &request(2, cut.revision()), &spec, TIMEOUT)
            .await?;
        Ok(Self {
            domain,
            store,
            catalog,
            declaration,
            additional_namespaces: declarations.into_iter().skip(1).collect(),
            application,
            configuration: None,
            external_http: None,
            filesystem: Arc::new(cellar_ltx::DirectFileSystem),
            effects: None,
            sql_workers: 2,
            max_pending_transactions: 32,
            max_client_identities: 1024,
            max_input_bytes: 1024,
            payloads: None,
            image_bytes: 256 * 1024 * 1024,
            logical_byte_capacity: None,
            placement_domains: None,
            spec,
        })
    }

    pub async fn start_node(&self, directory: &Path) -> Result<Node> {
        Box::pin(start_configured_node(
            directory,
            self.application.clone(),
            &self.spec.label,
            self.filesystem.clone(),
            self.configuration.clone(),
            self.external_http.clone(),
        ))
        .await
    }

    pub fn open_catalog(
        domain: &ExecutionDomain,
        store: Arc<fault::Store>,
        cluster: ClusterId,
    ) -> Result<FleetCatalog> {
        let bucket = Bucket::from_clients(
            store.clone(),
            store,
            Arc::new(NoListing),
            StorageBackend::S3,
            "managed-component".into(),
            "tenant/".into(),
        )?;
        Ok(FleetCatalog::new(
            domain,
            bucket,
            cluster,
            IncarnationId::new([80; 16])?,
            "hosts/",
        )?)
    }

    pub async fn config(
        &self,
        node: &Node,
        mode: LogOpenMode,
        generation: u8,
    ) -> Result<(RaftCellsConfig, RaftCellsServices)> {
        let cut = self
            .catalog
            .read(TIMEOUT)
            .await
            .context("read the Fleet head for host authorization")?;
        let authorization = self
            .catalog
            .authorize_host(&cut, self.spec.node, TIMEOUT)
            .await
            .context("authorize the host against the Fleet head")?;
        let listeners = NodeRaftListeners {
            control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        };
        let process = ProcessGeneration::new([generation; 16])?;
        let peer = RaftPeerNode {
            node: self.spec.node,
            disk: self.spec.disk,
            process,
            label: self.spec.label.clone(),
            server_name: "localhost".into(),
            control_address: listeners.control.local_addr()?,
            data_address: listeners.data.local_addr()?,
            query_address: listeners.query.local_addr()?,
        };
        let mut host = RaftHostOptions {
            max_groups: 2,
            max_input_bytes: self.max_input_bytes,
            effects: self.effects,
            ..Default::default()
        };
        // Keep two group reservations plus the attested existing usage inside
        // the fixture's unchanged 1-GiB platform budget. Tests can explicitly
        // restore the oversized image profile to qualify its capacity refusal.
        host.images.max_bytes = self.image_bytes;
        // Snapshot/current/private/apply/incoming ownership admits eight copies.
        host.execution.max_image_bytes = self.image_bytes / 8;
        host.execution.max_pending_transactions = self.max_pending_transactions;
        host.execution.max_client_identities = self.max_client_identities;
        let (region, zone) = self
            .placement_domains
            .as_ref()
            .and_then(|domains| domains.get(&self.spec.node))
            .cloned()
            .unwrap_or_else(|| ("local".into(), "node".into()));
        host.node_profile = Some(RaftNodeProfile {
            pool: "test".into(),
            region,
            zone,
            group_capacity: 2,
            byte_capacity: self
                .logical_byte_capacity
                .unwrap_or(host.images.max_bytes * 2),
            bytes_per_group: host.images.max_bytes,
            leader_capacity: 2,
            draining: false,
        });
        host.node_directory = Some(RaftNodeDirectoryOptions::default());
        host.managed = Some(RaftManagedHostOptions {
            authorization,
            namespaces: std::iter::once(self.declaration.clone())
                .chain(self.additional_namespaces.iter().cloned())
                .collect(),
        });
        if self.declaration.namespace.image_storage == RaftImageStorage::Extents {
            host.extents = Some(Default::default());
        }
        let mut log = ReplicaLogOptions::new(
            node.info().data_dir.join("raft"),
            peer.node,
            peer.disk,
            process,
            mode,
        );
        log.max_groups = 2;
        let mut shared = limits();
        shared.payloads = self.payloads;
        shared.namespaces.max_namespaces = 1 + self.additional_namespaces.len();
        shared.sql.workers = self.sql_workers;
        shared.dispatch.max_input_bytes = self.max_input_bytes;
        Ok((
            RaftCellsConfig {
                infrastructure: NodeRaftConfig {
                    log,
                    consensus: RaftRuntimeOptions {
                        max_groups: 2,
                        ..Default::default()
                    },
                    transport: TransportOptions::default(),
                },
                catalog: CatalogOptions {
                    prefix: "managed/".into(),
                    ..Default::default()
                },
                namespaces: std::iter::once(&self.declaration)
                    .chain(self.additional_namespaces.iter())
                    .map(|declaration| RaftNamespaceConfig {
                        namespace: declaration.namespace.clone(),
                        class: declaration.class.clone(),
                        initialize: None,
                    })
                    .collect(),
                groups: vec![],
                peers: vec![peer],
                query_origins: vec![],
                command_origins: vec![],
                host,
                shared,
            },
            RaftCellsServices {
                recovery: None,
                listeners,
                tls: RaftTls::new(
                    vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
                        .to_vec()
                        .into()],
                    vec![include_bytes!("../../transport/tcp/fixtures/server.der")
                        .to_vec()
                        .into()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(
                        include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
                    )
                    .into(),
                )?,
                store: self.store.clone(),
                application: self.application.clone(),
                versions: Self::version_keys()?,
            },
        ))
    }
}

pub fn request(id: u8, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [id; 16],
        operation: [100; 16],
        principal: "managed-test".into(),
        expected_revision: revision,
    }
}

pub fn limits() -> CellGroupNodeLimits {
    CellGroupNodeLimits {
        payloads: None,
        namespaces: CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 16 * 1024 * 1024,
        },
        controller: CellGroupControllerLimits {
            max_groups: 2,
            max_parallel_apply: 2,
            retry_interval_ms: 5,
            max_commands: 2,
            max_command_bytes: 64 * 1024,
            checkpoints: None,
        },
        sql: CellSqlLimits {
            workers: 2,
            queue_per_worker: 4,
            max_pending_cells: 8,
            group_pending_cells: 8,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
        provisioning: CellGroupProvisionLimits {
            max_preparations: 1,
            max_preparation_bytes: 512 * 1024 * 1024,
            max_parallel_preparations: 1,
            max_waiters: 2,
            peer_revision: 1,
        },
        directory: DirectoryLimits::default(),
        dispatch: CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
        manifest: ManifestLimits {
            max_cells: 32,
            max_depth: 16,
        },
        entries: Default::default(),
        manifest_pages: 1024,
        manifest_bytes: 16 * 1024 * 1024,
        retained_roots: 16,
        max_effect_rows: 100,
        checkpoints: None,
        archive: None,
    }
}

pub async fn start_node(
    directory: &Path,
    application: Arc<dyn Application>,
    label: &str,
) -> Result<Node> {
    Box::pin(start_node_with_filesystem(
        directory,
        application,
        label,
        Arc::new(cellar_ltx::DirectFileSystem),
    ))
    .await
}

pub async fn start_node_with_filesystem(
    directory: &Path,
    application: Arc<dyn Application>,
    label: &str,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<Node> {
    Box::pin(start_configured_node(
        directory,
        application,
        label,
        filesystem,
        None,
        None,
    ))
    .await
}

async fn start_configured_node(
    directory: &Path,
    application: Arc<dyn Application>,
    label: &str,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
    options: Option<cellar::engine_api::WorkerConfigOptions>,
    external_http: Option<Arc<http::Owner>>,
) -> Result<Node> {
    let classes = vec![];
    let shutdown = ShutdownPolicy::default();
    let bucket: Option<Bucket> = None;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let probe_signer = Arc::new(PeerProbeSigner::generate());
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: label.into(),
            advertise: internal_listener.local_addr()?.to_string(),
            region: "test".into(),
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
            generation: probe_signer.public_key_hex(),
            resume_generation: None,
        },
        directory.into(),
    );
    config.shutdown = shutdown;
    let peer_http = reqwest::Client::builder().no_proxy().build()?;
    let peer_auth = Arc::new(PeerAuth::new([11; 32], label)?);
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
            })
        })
        .transpose()?;
    let services = NodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: tokio::runtime::Handle::current(),
        core_executor: tokio::runtime::Handle::current(),
        listener,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer,
        fleet,
        containers: None,
        metrics: None,
    };
    let script = options
        .as_ref()
        .map_or(label, |options| &options.script_name);
    let mut graph = graph::graph(script, "", classes);
    if let Some(options) = options {
        graph.primary.options = options;
    }
    let compiled = CompiledApplications::default().register(
        &graph.primary.script_name,
        "test",
        move || Ok(application.clone()),
    )?;
    if let Some(http) = external_http {
        let public_address = services.listener.local_addr()?;
        let (node, ingress) = Node::start_external(
            config,
            ExternalNodeServices {
                wall_clock: services.wall_clock,
                http_client: services.http_client,
                host_executor: services.host_executor,
                core_executor: services.core_executor,
                public_address,
                ingress_capacity: 16,
                internal_listener: services.internal_listener,
                peer_http: services.peer_http,
                peer_auth: services.peer_auth,
                probe_signer: services.probe_signer,
                fleet: services.fleet,
                containers: services.containers,
                metrics: services.metrics,
            },
            graph,
            compiled,
        )
        .await?;
        http.serve(services.listener, ingress);
        Ok(node)
    } else {
        Node::start_with_filesystem(config, services, graph, compiled, filesystem).await
    }
}
