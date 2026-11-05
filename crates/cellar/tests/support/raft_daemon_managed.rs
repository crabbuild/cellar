// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cold RF3 startup on the daemon's public SDK boundary with fixture capacity.
use super::*;
use cellar::application::consensus::verification::cell_groups::RangeDirectory;
use cellar::bucket::{Bucket, StaticCredentials};
use std::io::Write;

const TIMEOUT: Duration = Duration::from_secs(5);
const PRINCIPAL: &str = "daemon-process-fixture";
const STANDBY_READY: &str = "first-group-ready";
const ARCHIVE_INTENT: &str = "archive-intent";

pub(super) const RUSTFS_ENDPOINT: &str = "CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT";
pub(super) const RUSTFS_ENV: [&str; 6] = [
    RUSTFS_ENDPOINT,
    "CELLAR_CELL_GROUPS_RUSTFS_BUCKET",
    "CELLAR_CELL_GROUPS_RUSTFS_PREFIX",
    "CELLAR_RAFT_PROCESS_IMAGE_MODE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
];

fn open_bucket(root: &Path) -> Result<Bucket> {
    if std::env::var_os(RUSTFS_ENDPOINT).is_none() {
        return Bucket::open_dev(&root.join("catalog.sqlite"));
    }
    let endpoint = std::env::var(RUSTFS_ENDPOINT)?;
    ensure!(
        endpoint.starts_with("http://127.0.0.1:"),
        "expected a loopback RustFS endpoint"
    );
    let bucket = std::env::var("CELLAR_CELL_GROUPS_RUSTFS_BUCKET")?;
    let prefix = std::env::var("CELLAR_CELL_GROUPS_RUSTFS_PREFIX")?;
    ensure!(
        !bucket.is_empty() && !prefix.is_empty(),
        "isolated RustFS bucket and prefix required"
    );
    Bucket::open(
        &format!("s3://{bucket}/{prefix}"),
        Some(&endpoint),
        "us-east-1",
        Some(StaticCredentials {
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
            session_token: None,
        }),
        None,
    )
}

fn request(id: u8, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [id; 16],
        operation: [70; 16],
        principal: PRINCIPAL.into(),
        expected_revision: revision,
    }
}

fn node(index: u8) -> Result<NodeId> {
    Ok(NodeId::new([index + 3; 16])?)
}

fn history() -> Result<GroupHistory> {
    configuration::group(ClusterId::new([1; 16])?, NamespaceId::new([2; 16])?)
}

struct Operator {
    domain: Arc<ExecutionDomain>,
    bucket: Bucket,
    fleet: FleetCatalog,
    adapter: Catalog,
    namespace: NamespaceConfig,
}

impl Operator {
    fn open(domain: Arc<ExecutionDomain>, root: &Path) -> Result<Self> {
        let history = history()?;
        let bucket = open_bucket(root)?;
        let fleet = FleetCatalog::new(
            &domain,
            bucket.clone(),
            history.cluster,
            IncarnationId::new([81; 16])?,
            "daemon-control/",
        )?;
        let adapter = Catalog::new(bucket.raft_store(), catalog_options())?;
        let mut namespace = NamespaceConfig::new(
            history.cluster,
            history.namespace,
            NamespaceMode::Raft,
            counter::definition()?.schema_digest(),
        );
        if std::env::var_os(RUSTFS_ENDPOINT).is_some() {
            namespace.image_storage =
                match std::env::var("CELLAR_RAFT_PROCESS_IMAGE_MODE")?.as_str() {
                    "materialized" => RaftImageStorage::Materialized,
                    "extents" => RaftImageStorage::Extents,
                    _ => anyhow::bail!("invalid RustFS process image mode"),
                };
        }
        Ok(Self {
            domain,
            bucket,
            fleet,
            adapter,
            namespace,
        })
    }

    fn declaration(&self) -> Result<RaftNamespaceRegistration> {
        Ok(RaftNamespaceRegistration {
            namespace: self.fleet.bind_namespace(self.namespace.clone())?,
            class: "Counter".into(),
        })
    }

    async fn archive(&self) -> Result<RaftManagedArchiveProvisioning> {
        let limits = configuration::limits();
        Ok(self
            .fleet
            .open_managed_archive_provisioning(
                &self.adapter,
                &self.declaration()?,
                limits.directory,
                limits.archive.context("managed archive options")?.io,
                TIMEOUT,
            )
            .await?)
    }
}

fn catalog_options() -> CatalogOptions {
    CatalogOptions {
        prefix: "daemon-managed/".into(),
        ..Default::default()
    }
}

pub(super) async fn initialize(root: &Path) -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = domain
        .run(async {
            let operator = Operator::open(domain.clone(), root)?;
            let snapshot = operator.fleet.initialize(TIMEOUT).await?;
            let declaration = operator
                .fleet
                .provision_managed_namespace(
                    &request(10, snapshot.revision()),
                    &operator.adapter,
                    operator.namespace.clone(),
                    &counter::definition()?,
                    configuration::limits().directory,
                    TIMEOUT,
                )
                .await?;
            let capability_digest = RaftManagedHostOptions::capability_digest(&[declaration])?;
            for index in 0..3 {
                let snapshot = operator.fleet.read(TIMEOUT).await?;
                operator
                    .fleet
                    .register_node(
                        &snapshot,
                        &request(11 + index, snapshot.revision()),
                        &FleetNodeSpec {
                            node: node(index)?,
                            disk: DiskLineage::new([index + 4; 16])?,
                            physical_host: IncarnationId::new([index + 90; 16])?,
                            label: format!("raft-cli-cluster-{index}"),
                            server_name: "localhost".into(),
                            capability_digest,
                            storage_instance: None,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}

pub(super) fn child(root: PathBuf, mode: &str) -> Result<()> {
    let (retained, index) = match mode.strip_prefix("cluster-resume-") {
        Some(index) => (true, index.parse::<u8>()?),
        None => (
            false,
            mode.strip_prefix("cluster-")
                .context("invalid managed daemon mode")?
                .parse::<u8>()?,
        ),
    };
    ensure!(index < 3, "invalid daemon fixture member");
    let history = history()?;
    let api = counter::CounterApi::new(
        SECRET.into(),
        history.cluster,
        history.namespace,
        &counter::definition()?,
    )?;
    let serving = api.clone();
    // The test operator stays owned for the whole process, including shutdown
    // and all catalog work entered by the daemon.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let domain = Arc::new(ExecutionDomain::new(runtime.handle().clone()));
    let operator = Arc::new(Operator::open(domain.clone(), &root)?);
    let boot_operator = operator.clone();
    let result = Host::local(graph::graph("raft-cli", "", vec![]))
        .register("raft-cli", "test", move || {
            counter::application(serving.clone(), counter::definition()?)
        })?
        .raft_cells_with_startup(
            "raft-cli",
            move |node| boot_operator.prepare(node, index, retained),
            move |service| async move {
                ready(&operator, &service, &root, index, retained).await?;
                ready_counter(&service, history, MemberId::new(u64::from(index) + 1)?).await?;
                *api.cells
                    .lock()
                    .map_err(|_| anyhow::anyhow!("binding poisoned"))? = Some(service);
                Ok(())
            },
        )?
        .run_until(std::future::pending());
    runtime.block_on(domain.shutdown());
    result
}

impl Operator {
    async fn prepare(
        self: Arc<Self>,
        native: NativeRaftNode,
        index: u8,
        retained: bool,
    ) -> Result<NativeRaftCells> {
        let declaration = self.declaration()?;
        let id = node(index)?;
        let disk = DiskLineage::new([index + 4; 16])?;
        let snapshot = self.fleet.read(TIMEOUT).await?;
        let authorization = self.fleet.authorize_host(&snapshot, id, TIMEOUT).await?;
        let listeners = NodeRaftListeners {
            control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        };
        let mut process = [0; 16];
        process[..4].copy_from_slice(&std::process::id().to_be_bytes());
        let process = ProcessGeneration::new(process)?;
        let peers = vec![RaftPeerNode {
            node: id,
            disk,
            process,
            label: native.node,
            server_name: "localhost".into(),
            control_address: listeners.control.local_addr()?,
            data_address: listeners.data.local_addr()?,
            query_address: listeners.query.local_addr()?,
        }];
        let shared = configuration::limits();
        let mut log = ReplicaLogOptions::new(
            native.data_dir.join("raft"),
            id,
            disk,
            process,
            if retained {
                LogOpenMode::Resume
            } else {
                LogOpenMode::Create
            },
        );
        log.max_groups = shared.controller.max_groups;
        let mut host = RaftHostOptions {
            max_groups: shared.controller.max_groups,
            max_input_bytes: shared.dispatch.max_input_bytes,
            node_directory: Some(RaftNodeDirectoryOptions::default()),
            managed: Some(RaftManagedHostOptions {
                authorization,
                namespaces: vec![declaration.clone()],
            }),
            ..Default::default()
        };
        if declaration.namespace.image_storage == RaftImageStorage::Extents {
            host.extents = Some(Default::default());
        }
        host.node_profile = Some(RaftNodeProfile {
            pool: "daemon-fixture".into(),
            region: "local".into(),
            zone: format!("process-{index}"),
            group_capacity: u32::try_from(host.max_groups)?,
            byte_capacity: host.images.max_bytes,
            bytes_per_group: host.images.max_bytes,
            leader_capacity: u32::try_from(host.max_groups)?,
            draining: false,
        });
        Ok(NativeRaftCells {
            config: RaftCellsConfig {
                infrastructure: NodeRaftConfig {
                    log,
                    consensus: RaftRuntimeOptions {
                        tick_interval_ms: Some(25),
                        max_groups: shared.controller.max_groups,
                        ..Default::default()
                    },
                    transport: TransportOptions::default(),
                },
                catalog: catalog_options(),
                namespaces: vec![RaftNamespaceConfig {
                    namespace: declaration.namespace,
                    class: declaration.class,
                    initialize: None,
                }],
                groups: if retained {
                    vec![RaftGroupAssignment {
                        history: history()?,
                        placement: RaftGroupPlacement::Replica,
                    }]
                } else {
                    vec![]
                },
                peers,
                query_origins: vec![],
                command_origins: vec![],
                host,
                shared,
            },
            listeners,
            tls: fixture_tls()?,
            store: self.bucket.raft_store(),
            versions: fixture_versions()?,
        })
    }
}

async fn ready(
    operator: &Operator,
    service: &RaftCells,
    root: &Path,
    index: u8,
    retained: bool,
) -> Result<()> {
    let history = history()?;
    if !retained {
        // This marker is only a process rendezvous. Inspect rooted boot and
        // capacity independently before reserving or attaching a voter.
        ensure!(service.observe_node(TIMEOUT).await?.node.groups == 0);
        persist(root, &format!("standby-{index}"), b"entered")?;
        if index == 0 {
            provision_first_group(operator, service, root, history).await?;
        }
        let deadline = Instant::now() + Duration::from_secs(20);
        while !root.join(STANDBY_READY).exists() {
            ensure!(
                Instant::now() < deadline,
                "operator did not confirm group and archive"
            );
            cellar::asyncrt::sleep(Duration::from_millis(20)).await;
        }
    }
    operator
        .archive()
        .await?
        .verify_archive(history, &std::fs::read(root.join(ARCHIVE_INTENT))?, TIMEOUT)
        .await?;
    if !retained {
        let cut = operator.fleet.read(TIMEOUT).await?;
        for index in 0..3 {
            inspect_current(service, node(index)?, cut.revision()).await?;
        }
        let authorization = operator
            .fleet
            .authorize_group(&cut, &history, TIMEOUT)
            .await?;
        service
            .activate_managed_initial_group(authorization, TIMEOUT)
            .await?;
    }
    Ok(())
}

fn persist(root: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let mut file = File::create_new(root.join(name))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    File::open(root)?.sync_all()?;
    Ok(())
}

async fn provision_first_group(
    operator: &Operator,
    service: &RaftCells,
    root: &Path,
    history: GroupHistory,
) -> Result<()> {
    let declaration = operator.declaration()?;
    let limits = configuration::limits().directory;
    let standby = operator
        .fleet
        .observe_managed_standby(&operator.adapter, &declaration, limits, TIMEOUT)
        .await?;
    let selected = operator
        .domain
        .run(
            operator
                .adapter
                .existing_namespace(declaration.namespace.clone()),
        )
        .await??;
    let directory = RangeDirectory::new(selected, limits)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while (1..3).any(|index| !root.join(format!("standby-{index}")).exists()) {
        ensure!(
            Instant::now() < deadline,
            "voters did not publish standby boots"
        );
        cellar::asyncrt::sleep(Duration::from_millis(20)).await;
    }
    for index in 0..3 {
        admit_capacity(operator, service, &directory, index).await?;
    }
    let cut = operator.fleet.read(TIMEOUT).await?;
    let reservation = request(30, cut.revision());
    let publication = ProposalId::new([32; 16])?;
    let group = CellGroupBirthSpec {
        history,
        range: CellRange::all(),
        epoch: RouteEpoch::new(1)?,
        creation: ProposalId::new([33; 16])?,
        voters: (0..3)
            .map(|index| {
                Ok(InitialMember {
                    node: node(index)?,
                    member: MemberId::new(u64::from(index) + 1)?,
                })
            })
            .collect::<Result<_>>()?,
        learners: vec![],
    };
    persist(
        root,
        "first-group-reservation",
        &serde_json::to_vec(&reservation)?,
    )?;
    let prepared = operator
        .fleet
        .reserve_managed_first_group(
            &operator.adapter,
            &declaration,
            RaftManagedFirstGroupRequest {
                reservation,
                standby,
                publication,
                group,
            },
            limits,
            TIMEOUT,
        )
        .await?;
    let cut = operator.fleet.read(TIMEOUT).await?;
    let confirmation = request(31, cut.revision());
    persist(
        root,
        "first-group-confirmation",
        &serde_json::to_vec(&confirmation)?,
    )?;
    operator
        .fleet
        .publish_managed_first_group(&prepared, &confirmation, TIMEOUT)
        .await?;
    let archive = operator.archive().await?;
    let prepared = archive.prepare_archive(history, TIMEOUT).await?;
    persist(root, ARCHIVE_INTENT, prepared.encoded_intent())?;
    prepared.commit(&operator.domain).await?;
    archive
        .verify_archive(history, &std::fs::read(root.join(ARCHIVE_INTENT))?, TIMEOUT)
        .await?;
    persist(root, STANDBY_READY, b"confirmed")?;
    Ok(())
}

async fn inspect_current(
    service: &RaftCells,
    node: NodeId,
    revision: u64,
) -> Result<RaftHostObservation> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "daemon has not installed fleet revision {revision}"
        );
        if let Ok(observed) = service.inspect_peer_node(node, revision, remaining).await {
            return Ok(observed.view);
        }
        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
    }
}

async fn admit_capacity(
    operator: &Operator,
    service: &RaftCells,
    directory: &RangeDirectory,
    index: u8,
) -> Result<()> {
    let node = node(index)?;
    let cut = operator.fleet.read(TIMEOUT).await?;
    let observed = inspect_current(service, node, cut.revision()).await?;
    let attestation = FleetCapacityAttestation {
        boot: observed.fleet_boot.context("capacity boot")?,
        profile_revision: 1,
        profile_digest: fleet_capacity_profile_digest(&observed.node.profile)?,
        cpu_millis: 1000,
        memory_bytes: 3 * 1024 * 1024 * 1024,
        durable_bytes: observed.node.profile.byte_capacity.max(1024 * 1024 * 1024),
        engine_slots: 2,
        memory_swap_disabled: true,
        used_memory_bytes: 64 * 1024 * 1024,
        used_durable_bytes: 1024 * 1024,
        evidence_digest: [71; 32],
        evidence_reference: "fixture-only/daemon-process".into(),
    };
    for (offset, action, state) in [
        (0, FleetCapacityAction::Qualify, FleetNodeState::Ready),
        (1, FleetCapacityAction::Activate, FleetNodeState::Active),
    ] {
        let cut = operator.fleet.read(TIMEOUT).await?;
        inspect_current(service, node, cut.revision()).await?;
        let admitted = operator
            .fleet
            .admit_node_capacity(
                &cut,
                &FleetCapacityRequest {
                    identity: request(20 + index * 2 + offset, cut.revision()),
                    attestation: attestation.clone(),
                    action,
                },
                service,
                std::slice::from_ref(directory),
                TIMEOUT,
            )
            .await?;
        ensure!(admitted.state() == state);
    }
    Ok(())
}
