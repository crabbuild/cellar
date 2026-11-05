// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed boot, cold capacity and retained-group qualification over embedded Nodes.
//! Deployment bounds use fixture attestations; the full lifecycle gate is separate.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::{consensus::*, *};
use object_store::list::*;
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
#[path = "support/cell_group_managed_bootstrap.rs"]
mod bootstrap;
#[path = "support/raft_catalog_fault.rs"]
#[allow(dead_code)]
mod fault;
#[path = "support/html.rs"]
#[allow(dead_code)]
mod graph;
#[path = "support/cell_group_managed_registry.rs"]
mod registry;
#[path = "support/cell_group_rustfs.rs"]
#[allow(dead_code)]
mod rustfs;

const TIMEOUT: Duration = Duration::from_secs(5);

struct NoListing;

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

struct Fixture {
    domain: Arc<ExecutionDomain>,
    store: Arc<fault::Store>,
    catalog: FleetCatalog,
    declaration: RaftNamespaceRegistration,
    application: Arc<dyn Application>,
    spec: FleetNodeSpec,
}

impl Fixture {
    async fn new(rooted: bool) -> Result<Self> {
        Box::pin(Self::with_class(
            rooted,
            RaftClass::new("Counter", 1, [81; 32])?,
        ))
        .await
    }

    async fn with_class(rooted: bool, class: RaftClass) -> Result<Self> {
        Box::pin(Self::with_store(
            rooted,
            class,
            Arc::new(fault::Store::default()),
        ))
        .await
    }

    async fn with_store(rooted: bool, class: RaftClass, store: Arc<fault::Store>) -> Result<Self> {
        let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
        let catalog = Self::open_catalog(&domain, store.clone(), ClusterId::new([1; 16])?)?;
        catalog.initialize(TIMEOUT).await?;
        let schema = class.schema_digest();
        let mut config = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            schema,
        );
        config.image_storage = if rooted {
            RaftImageStorage::Extents
        } else {
            RaftImageStorage::Materialized
        };
        let namespace_catalog = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "managed/".into(),
                ..Default::default()
            },
        )?;
        let cut = catalog.read(TIMEOUT).await?;
        let declaration = catalog
            .provision_managed_namespace(
                &request(1, cut.revision()),
                &namespace_catalog,
                config,
                &class,
                DirectoryLimits::default(),
                TIMEOUT,
            )
            .await?;
        let application: Arc<dyn Application> =
            Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
        let spec = FleetNodeSpec {
            node: NodeId::new([4; 16])?,
            disk: DiskLineage::new([14; 16])?,
            physical_host: IncarnationId::new([90; 16])?,
            label: "shared-managed".into(),
            server_name: "localhost".into(),
            capability_digest: RaftManagedHostOptions::capability_digest(std::slice::from_ref(
                &declaration,
            ))?,
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
            application,
            spec,
        })
    }

    fn open_catalog(
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

    async fn config(
        &self,
        node: &Node,
        mode: LogOpenMode,
        generation: u8,
    ) -> Result<(RaftCellsConfig, RaftCellsServices)> {
        let cut = self.catalog.read(TIMEOUT).await?;
        let authorization = self
            .catalog
            .authorize_host(&cut, self.spec.node, TIMEOUT)
            .await?;
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
            max_input_bytes: 1024,
            ..Default::default()
        };
        host.node_profile = Some(RaftNodeProfile {
            pool: "test".into(),
            region: "local".into(),
            zone: "node".into(),
            group_capacity: 2,
            byte_capacity: host.images.max_bytes * 2,
            bytes_per_group: host.images.max_bytes,
            leader_capacity: 2,
            draining: false,
        });
        host.node_directory = Some(RaftNodeDirectoryOptions::default());
        host.managed = Some(RaftManagedHostOptions {
            authorization,
            namespaces: vec![self.declaration.clone()],
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
                namespaces: vec![RaftNamespaceConfig {
                    namespace: self.declaration.namespace.clone(),
                    class: "Counter".into(),
                    initialize: None,
                }],
                groups: vec![],
                peers: vec![peer],
                query_origins: vec![],
                command_origins: vec![],
                host,
                shared: limits(),
            },
            RaftCellsServices {
                recovery: None,
                listeners,
                tls: RaftTls::new(
                    vec![include_bytes!("../transport/tcp/fixtures/ca.der")
                        .to_vec()
                        .into()],
                    vec![include_bytes!("../transport/tcp/fixtures/server.der")
                        .to_vec()
                        .into()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(
                        include_bytes!("../transport/tcp/fixtures/key.der").to_vec(),
                    )
                    .into(),
                )?,
                store: self.store.clone(),
                application: self.application.clone(),
                versions: VersionKeys::new(
                    VersionSigningKey::new([31; 32], u64::MAX)?,
                    vec![],
                    Duration::from_secs(86400),
                )?,
            },
        ))
    }
}

fn request(id: u8, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [id; 16],
        operation: [100; 16],
        principal: "managed-test".into(),
        expected_revision: revision,
    }
}

async fn stopped(node: Node, service: &RaftCells) -> Result<()> {
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = service.close().await?;
    ensure!(
        report.groups_total == 0
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_boot_retained_restart_and_exact_stop_owner() -> Result<()> {
    for rooted in [false, true] {
        let f = Fixture::new(rooted).await?;
        let root = tempfile::tempdir()?;
        let mut previous: Option<FleetBootBinding> = None;
        for generation in [24, 25] {
            let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
            let (config, services) = f
                .config(
                    &node,
                    if previous.is_none() {
                        LogOpenMode::Create
                    } else {
                        LogOpenMode::Resume
                    },
                    generation,
                )
                .await?;
            let host = node.start_raft_cells(config, services).await?;
            let view = host.observe_node(TIMEOUT).await?;
            let boot = view.fleet_boot.context("synced boot")?;
            ensure!(view.node.groups == 0 && view.declared_namespaces == 1);
            ensure!(boot.process == [generation; 16] && boot.node == f.spec.node.bytes());
            if let Some(old) = &previous {
                ensure!(
                    boot.engine == old.engine
                        && boot.epoch > old.epoch
                        && boot.boot != old.boot
                        && boot.join_key != old.join_key
                );
            }
            let mut foreign = boot.clone();
            foreign.process = [99; 16];
            let request: FleetStopRequest = serde_json::from_value(
                serde_json::json!({"selector":{"purpose":"maintenance","boot":foreign,"epoch":boot.epoch+1,"operation":vec![41;16],"finalization":vec![42;16],"request":vec![43;16]},"action":"stop"}),
            )?;
            ensure!(
                matches!(
                    node.request_fleet_stop(request, TIMEOUT).await,
                    Err(FleetCatalogError::Conflict)
                ),
                "Node must install the exact managed stop reporter"
            );
            stopped(node, &host).await?;
            if previous.is_none() {
                let blank = tempfile::tempdir()?;
                let candidate =
                    start_node(blank.path(), f.application.clone(), &f.spec.label).await?;
                let (config, services) = f.config(&candidate, LogOpenMode::Create, 26).await?;
                let before = f.catalog.read(TIMEOUT).await?.revision();
                ensure!(
                    candidate.start_raft_cells(config, services).await.is_err(),
                    "recreated managed engine accepted the pinned disk identity"
                );
                ensure!(
                    f.catalog.read(TIMEOUT).await?.revision() == before,
                    "invalid engine consumed a rooted boot"
                );
                candidate.shutdown(ShutdownMode::Preserve).await?;
            }
            previous = Some(boot);
        }
        f.domain.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_declaration_mismatch_refuses_before_boot_claim() -> Result<()> {
    let f = Fixture::new(false).await?;
    let root = tempfile::tempdir()?;
    let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
    let (mut config, services) = f.config(&node, LogOpenMode::Create, 24).await?;
    config.namespaces[0].namespace.schema_digest = [82; 32];
    let before = f.catalog.read(TIMEOUT).await?.revision();
    ensure!(node.start_raft_cells(config, services).await.is_err());
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == before);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_directory_shutdown_joins_entered_root_refresh() -> Result<()> {
    let f = Fixture::new(false).await?;
    let root = tempfile::tempdir()?;
    let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
    let (config, services) = f.config(&node, LogOpenMode::Create, 24).await?;
    let host = node.start_raft_cells(config, services).await?;
    // No application traffic drives discovery. A rooted node-table change must
    // be installed by the independently timed directory reader.
    let mut peer = f.spec.clone();
    peer.node = NodeId::new([5; 16])?;
    peer.disk = DiskLineage::new([15; 16])?;
    peer.physical_host = IncarnationId::new([91; 16])?;
    peer.label = "managed-other".into();
    let cut = f.catalog.read(TIMEOUT).await?;
    let cut = f
        .catalog
        .register_node(&cut, &request(3, cut.revision()), &peer, TIMEOUT)
        .await?
        .snapshot;
    tokio::time::timeout(TIMEOUT, async {
        while host
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_none_or(|revision| revision < cut.revision())
        {
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    f.store.fleet_read_hang.store(true, Ordering::Release);
    f.store.hold_fleet_reads.store(true, Ordering::Release);
    tokio::time::timeout(TIMEOUT, async {
        while f.store.active_fleet_reads.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let mut closing = Box::pin(node.shutdown(ShutdownMode::Preserve));
    ensure!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err(),
        "shutdown dropped entered directory work"
    );
    ensure!(f.store.active_fleet_reads.load(Ordering::Acquire) > 0);
    f.store.hold_fleet_reads.store(false, Ordering::Release);
    f.store.fleet_read_hang.store(false, Ordering::Release);
    f.store.fleet_read_resume.notify_waiters();
    tokio::time::timeout(TIMEOUT, closing).await??;
    ensure!(f.store.active_fleet_reads.load(Ordering::Acquire) == 0);
    ensure!(host.close().await?.groups_total == 0);
    f.domain.shutdown().await;
    Ok(())
}

fn limits() -> CellGroupNodeLimits {
    CellGroupNodeLimits {
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

async fn start_node(
    directory: &Path,
    application: Arc<dyn Application>,
    label: &str,
) -> Result<Node> {
    let classes = vec![];
    let filesystem = Arc::new(cellar_ltx::DirectFileSystem);
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
    let compiled =
        CompiledApplications::default().register(label, "test", move || Ok(application.clone()))?;
    Node::start_with_filesystem(
        config,
        services,
        graph::graph(label, "", classes),
        compiled,
        filesystem,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_directory_preserves_explicit_advertised_endpoints() -> Result<()> {
    let f = Fixture::new(false).await?;
    let root = tempfile::tempdir()?;
    let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
    let (mut config, mut services) = f.config(&node, LogOpenMode::Create, 24).await?;
    let listeners = NodeRaftListeners {
        control: tokio::net::TcpListener::bind("0.0.0.0:0").await?,
        data: tokio::net::TcpListener::bind("0.0.0.0:0").await?,
        query: tokio::net::TcpListener::bind("0.0.0.0:0").await?,
    };
    let advertised = |bound: std::net::SocketAddr| {
        std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), bound.port())
    };
    let peer = &mut config.peers[0];
    peer.control_address = advertised(listeners.control.local_addr()?);
    peer.data_address = advertised(listeners.data.local_addr()?);
    peer.query_address = advertised(listeners.query.local_addr()?);
    ensure!(peer.control_address != listeners.control.local_addr()?);
    services.listeners = listeners;
    let host = node.start_raft_cells(config, services).await?;
    ensure!(host.observe_node(TIMEOUT).await?.fleet_boot.is_some());
    stopped(node, &host).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_owner_mismatch_refuses_before_boot_claim() -> Result<()> {
    for wrong_node in [true, false] {
        let f = Fixture::new(false).await?;
        let root = tempfile::tempdir()?;
        let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
        let (mut config, services) = f.config(&node, LogOpenMode::Create, 24).await?;
        if wrong_node {
            let other = NodeId::new([5; 16])?;
            let disk = DiskLineage::new([15; 16])?;
            config.infrastructure.log.node = other;
            config.infrastructure.log.disk = disk;
            config.peers[0].node = other;
            config.peers[0].disk = disk;
        } else {
            config.peers[0].server_name = "other.example".into();
        }
        let before = f.catalog.read(TIMEOUT).await?.revision();
        ensure!(node.start_raft_cells(config, services).await.is_err());
        ensure!(
            f.catalog.read(TIMEOUT).await?.revision() == before,
            "invalid managed owner consumed a rooted boot (wrong_node={wrong_node})"
        );
        node.shutdown(ShutdownMode::Preserve).await?;
        f.domain.shutdown().await;
    }
    Ok(())
}
