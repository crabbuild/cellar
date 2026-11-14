// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cold RF3 startup on the daemon's public SDK boundary with fixture capacity.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupArchiveTailLimits, RangeDirectory,
};
use cellar::bucket::{Bucket, StaticCredentials};
use cellar::sdk::fleet_operations::*;
use std::io::Write;
#[path = "raft_first_group_operator_cli.rs"]
mod cli;
use cli::{first_group_plan, OperatorCli, OWNER};
#[path = "raft_daemon_cron.rs"]
pub(super) mod cron;
#[path = "raft_daemon_maintenance.rs"]
pub(super) mod maintenance;
#[path = "raft_daemon_repair.rs"]
pub(super) mod repair;
#[path = "raft_daemon_standby.rs"]
pub(super) mod standby;

const TIMEOUT: Duration = Duration::from_secs(5);
const PRINCIPAL: &str = "daemon-process-fixture";

pub(super) const RUSTFS_ENDPOINT: &str = "CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT";
const RUSTFS_ENV: [&str; 6] = [
    RUSTFS_ENDPOINT,
    "CELLAR_CELL_GROUPS_RUSTFS_BUCKET",
    "CELLAR_CELL_GROUPS_RUSTFS_PREFIX",
    "CELLAR_RAFT_PROCESS_IMAGE_MODE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
];

const NODE_PREFIX: &str = "/daemon-runtime";

fn rustfs_location(suffix: &str) -> Result<Option<(String, String)>> {
    if std::env::var_os(RUSTFS_ENDPOINT).is_none() {
        return Ok(None);
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
    Ok(Some((format!("s3://{bucket}/{prefix}{suffix}"), endpoint)))
}

fn open_bucket(root: &Path, local: &str, suffix: &str) -> Result<Bucket> {
    let Some((bucket, endpoint)) = rustfs_location(suffix)? else {
        return Bucket::open_dev(&root.join(local));
    };
    Bucket::open(
        &bucket,
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

pub(super) fn open_deployment_store(root: &Path) -> Result<Bucket> {
    open_bucket(root, "fleet.sqlite", NODE_PREFIX)
}

pub(super) fn configure_deployment(cmd: &mut Command, root: &Path) -> Result<()> {
    if let Some((bucket, endpoint)) = rustfs_location(NODE_PREFIX)? {
        cmd.env("CELLAR_TEST_BUCKET", bucket)
            .env("S3_ENDPOINT", endpoint)
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true");
        for key in RUSTFS_ENV {
            cmd.env(
                key,
                std::env::var_os(key).with_context(|| format!("missing {key}"))?,
            );
        }
    } else {
        cmd.env("CELLAR_TEST_BUCKET", "cellar-dev")
            .env("CELLAR_INTERNAL_DEV_STORE", root.join("fleet.sqlite"));
    }
    Ok(())
}

pub(super) async fn verify_deployment_leases(root: &Path, voters: usize) -> Result<()> {
    let bucket = open_deployment_store(root)?;
    let leases = cellar::ownership_store::BucketOwnership::new(
        bucket.clone(),
        bucket,
        String::new(),
        String::new(),
    );
    for index in 0..voters {
        let label = format!("raft-cli-cluster-{index}");
        let lease = leases
            .read_node_lease(&label)
            .await?
            .context("missing RustFS voter lease")?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        ensure!(lease.node == label && u128::from(lease.expires_ms) > now);
    }
    Ok(())
}

fn request(id: u8, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [id; 16],
        operation: [70; 16],
        principal: PRINCIPAL.into(),
        expected_revision: revision,
    }
}

pub(super) fn node(index: u8) -> Result<NodeId> {
    Ok(NodeId::new([index + 3; 16])?)
}

pub(super) const DOCKER_CATALOG: &str = "managed-fleet-catalog";

fn node_spec(index: u8, capability_digest: [u8; 32]) -> Result<FleetNodeSpec> {
    Ok(FleetNodeSpec {
        node: node(index)?,
        disk: DiskLineage::new([index + 4; 16])?,
        physical_host: IncarnationId::new([if docker::enabled() { 90 } else { index + 90 }; 16])?,
        label: format!("raft-cli-cluster-{index}"),
        server_name: "localhost".into(),
        capability_digest,
        storage_instance: None,
    })
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
    class: String,
}

impl Operator {
    fn open(domain: Arc<ExecutionDomain>, root: &Path) -> Result<Self> {
        Self::open_for_class(domain, root, &counter::definition()?)
    }

    fn open_for_class(
        domain: Arc<ExecutionDomain>,
        root: &Path,
        class: &RaftClass,
    ) -> Result<Self> {
        let history = history()?;
        let bucket = open_bucket(root, "catalog.sqlite", "")?;
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
            class.schema_digest(),
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
            class: class.name().into(),
        })
    }

    fn declaration(&self) -> Result<RaftNamespaceRegistration> {
        Ok(RaftNamespaceRegistration {
            namespace: self.fleet.bind_namespace(self.namespace.clone())?,
            class: self.class.clone(),
        })
    }
}

fn catalog_options() -> CatalogOptions {
    CatalogOptions {
        prefix: "daemon-managed/".into(),
        ..Default::default()
    }
}

async fn provision_call(root: &Path, plan: &Value) -> Result<std::process::Output> {
    provision_call_for(root, plan, "provision-sql").await
}

async fn provision_call_for(root: &Path, plan: &Value, mode: &str) -> Result<std::process::Output> {
    let path = root.join("sql-namespace.json");
    std::fs::write(&path, serde_json::to_vec(plan)?)?;
    let bucket = std::env::var("CELLAR_CELL_GROUPS_RUSTFS_BUCKET")?;
    let prefix = std::env::var("CELLAR_CELL_GROUPS_RUSTFS_PREFIX")?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .env_clear()
        .env("CELLAR_RAFT_CLI_CHILD", mode)
        .env("CELLAR_RAFT_CLI_ROOT", root)
        .arg("--provision-sql-namespace")
        .arg(path)
        .arg("--bucket")
        .arg(format!("s3://{bucket}/{prefix}"))
        .arg("--endpoint")
        .arg(std::env::var(RUSTFS_ENDPOINT)?)
        .arg("--region")
        .arg("us-east-1")
        .kill_on_drop(true);
    for name in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] {
        command.env(
            name,
            std::env::var_os(name).with_context(|| format!("missing {name}"))?,
        );
    }
    Ok(tokio::time::timeout(Duration::from_secs(30), command.output()).await??)
}

async fn provision_from_compiled_cli(
    root: &Path,
    operator: &Operator,
    identity: &FleetRequestIdentity,
) -> Result<RaftNamespaceRegistration> {
    let id = hex::encode(operator.fleet.catalog_id()?);
    let namespace = hex::encode(operator.namespace.namespace.bytes());
    let mut plan = serde_json::json!({
        "catalog_id": id,
        "cluster_id": hex::encode(operator.namespace.cluster.bytes()),
        "incarnation_id": hex::encode([81; 16]),
        "coordination_prefix": "daemon-control/",
        "catalog_prefix": "daemon-managed/",
        "namespace_id": namespace,
        "script": "raft-cli",
        "version": "test",
        "class": "Counter",
        "principal": identity.principal,
        "request_id": hex::encode(identity.id),
        "operation_id": hex::encode(identity.operation),
        "expected_revision": identity.expected_revision,
    });
    let image_storage = match operator.namespace.image_storage {
        RaftImageStorage::Materialized => "materialized",
        RaftImageStorage::Extents => {
            plan["image_storage"] = "extents".into();
            "extents"
        }
    };
    for (field, value) in [
        ("catalog_id", hex::encode([77; 32])),
        ("class", "Absent".into()),
        ("image_storage", "unknown".into()),
    ] {
        let mut wrong = plan.clone();
        wrong[field] = value.into();
        let refused = provision_call(root, &wrong).await?;
        ensure!(
            !refused.status.success(),
            "invalid SQL namespace plan was admitted"
        );
        ensure!(operator.fleet.read(TIMEOUT).await?.revision() == identity.expected_revision);
    }
    let admitted = provision_call(root, &plan).await?;
    ensure!(
        admitted.status.success(),
        "compiled SQL namespace command failed"
    );
    let receipt: Value = serde_json::from_slice(&admitted.stdout)?;
    ensure!(
        receipt["catalog_id"] == id
            && receipt["namespace_id"] == namespace
            && receipt["class"] == "Counter"
            && receipt["image_storage"] == image_storage
            && receipt["schema_digest"] == hex::encode(operator.namespace.schema_digest)
            && receipt["request_id"] == plan["request_id"]
            && receipt["operation_id"] == plan["operation_id"]
    );
    ensure!(operator.fleet.read(TIMEOUT).await?.revision() == identity.expected_revision + 1);
    let replay = provision_call(root, &plan).await?;
    ensure!(replay.status.success() && replay.stdout == admitted.stdout);
    let mut changed = plan;
    changed["image_storage"] = match operator.namespace.image_storage {
        RaftImageStorage::Materialized => "extents",
        RaftImageStorage::Extents => "materialized",
    }
    .into();
    ensure!(!provision_call(root, &changed).await?.status.success());
    ensure!(operator.fleet.read(TIMEOUT).await?.revision() == identity.expected_revision + 1);
    operator.declaration()
}

pub(super) async fn initialize(root: &Path) -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = domain
        .run(async {
            let operator = Operator::open(domain.clone(), root)?;
            let snapshot = if docker::enabled() {
                operator
                    .fleet
                    .initialize_with_replica_separation(FleetReplicaSeparation::Nodes, TIMEOUT)
                    .await?
            } else {
                operator.fleet.initialize(TIMEOUT).await?
            };
            if docker::enabled() {
                persist(
                    root,
                    DOCKER_CATALOG,
                    hex::encode(operator.fleet.catalog_id()?).as_bytes(),
                )?;
            }
            let identity = request(10, snapshot.revision());
            let declaration = if std::env::var_os(RUSTFS_ENDPOINT).is_some() {
                provision_from_compiled_cli(root, &operator, &identity).await?
            } else {
                let class = counter::definition()?;
                operator
                    .fleet
                    .provision_managed_sql_namespace(
                        &identity,
                        &operator.adapter,
                        operator.namespace.namespace,
                        &class,
                        configuration::limits().directory,
                        TIMEOUT,
                    )
                    .await?
            };
            let capability_digest = RaftManagedHostOptions::capability_digest(&[declaration])?;
            for index in 0..3 {
                let snapshot = operator.fleet.read(TIMEOUT).await?;
                operator
                    .fleet
                    .register_node(
                        &snapshot,
                        &request(11 + index, snapshot.revision()),
                        &node_spec(index, capability_digest)?,
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

async fn open_archive_verifier(
    operator: &Operator,
    root: &Path,
) -> Result<FleetGroupArchiveVerifier> {
    let limits = configuration::limits();
    let checkpoints = limits.checkpoints.context("repair replay checkpoints")?;
    let tails = CellGroupArchiveTailLimits::default();
    archive_verifier::open(
        &operator.domain,
        root,
        operator.namespace.image_storage == RaftImageStorage::Extents,
        tails,
        limits,
        checkpoints,
    )
    .await
}

pub(super) fn retained_index(mode: &str) -> Option<&str> {
    mode.strip_prefix("cluster-resume-")
        .or_else(|| mode.strip_prefix("cluster-maintenance-"))
}

pub(super) fn child(root: PathBuf, mode: &str) -> Result<()> {
    let returning = mode.starts_with("cluster-maintenance-");
    let (retained, index) = match retained_index(mode) {
        Some(index) => (true, index.parse::<u8>()?),
        None => (
            false,
            mode.strip_prefix("cluster-")
                .context("invalid managed daemon mode")?
                .parse::<u8>()?,
        ),
    };
    ensure!(index <= standby::INDEX, "invalid daemon fixture member");
    let spare = index == standby::INDEX;
    ensure!(!spare || !retained, "standby boot must use a new disk");
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
    let axum_runtime = runtime.handle().clone();
    let (axum_stop, mut axum_stopping) = tokio::sync::watch::channel(false);
    let (axum_started, axum_task) = tokio::sync::oneshot::channel();
    let host = Host::local(graph::graph("raft-cli", "", vec![])).register(
        "raft-cli",
        "test",
        move || counter::application(serving.clone(), counter::definition()?),
    )?;
    let store = Arc::new(archive::Gate::new(
        operator.bucket.raft_store(),
        &root,
        index,
    ));
    let prepare = move |node| boot_operator.prepare(node, index, retained, returning, store);
    let fixture_operator = operator.clone();
    let (maintenance_started, maintenance_task) = tokio::sync::oneshot::channel();
    let (repair_started, repair_task) = tokio::sync::oneshot::channel();
    let repair_root = root.clone();
    let repair_stop = axum_stopping.clone();
    let return_root = root.clone();
    let admin_root = root.join(format!("park-admin-{index}"));
    let bind = move |service: RaftCells, discovered: GroupHistory| async move {
        ensure!(discovered == history, "unexpected managed namespace group");
        // A repair standby has already proved promotion in the current quorum.
        // The original voters and catalog cut apply only to first-group birth.
        if !retained && !spare {
            let cut = operator.fleet.read(TIMEOUT).await?;
            for index in 0..3 {
                inspect_current(&service, node(index)?, cut.revision()).await?;
            }
        }
        ready_counter(&service, history, MemberId::new(u64::from(index) + 1)?).await?;
        let group = service.group(history)?;
        let declaration = operator.declaration()?;
        let selected = operator
            .domain
            .run(operator.adapter.existing_namespace(declaration.namespace))
            .await??;
        let directory = RangeDirectory::new(selected, configuration::limits().directory)?;
        if index == repair::WORKER && std::env::var_os(repair::ENABLED).is_some() {
            let task = axum_runtime.spawn(Box::pin(repair::run_worker(
                operator.clone(),
                service.clone(),
                repair_root,
                directory.clone(),
                repair_stop,
            )));
            repair_started.send(task).map_err(|task| {
                task.abort();
                anyhow::anyhow!("repair worker join owner dropped during startup")
            })?;
        }
        let control =
            RaftFleetControl::new(&operator.domain, operator.fleet.clone(), vec![directory])?
                .with_node_observer(service.clone());
        std::fs::create_dir_all(&admin_root)?;
        if index == 0 {
            let worker = control
                .start_maintenance_service(None, Default::default())
                .await?;
            maintenance_started
                .send(worker)
                .map_err(|_| anyhow::anyhow!("maintenance join owner dropped"))?;
        }
        let admin = OperatorCli::router(
            &operator.domain,
            operator.bucket.clone(),
            control,
            &admin_root,
            PRINCIPAL,
        )?;
        *api.cells
            .lock()
            .map_err(|_| anyhow::anyhow!("binding poisoned"))? = Some(service);
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let router = recovery::observe_voter(counter::router(api), group.clone()).merge(admin);
        let router = maintenance::with_handoff(router, group);
        let task = axum_runtime.spawn(async move {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            cellar::note!("cellar axum listening on {address}");
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = axum_stopping.changed().await;
                })
                .await
        });
        axum_started.send(task).map_err(|task| {
            task.abort();
            anyhow::anyhow!("Axum join owner dropped during startup")
        })?;
        Ok(())
    };
    let host = if spare {
        host.raft_cells_with_startup("raft-cli", prepare, move |service| async move {
            standby::wait_for_attachment(&service, &return_root).await?;
            bind(service, history).await
        })?
    } else if returning {
        host.raft_cells_with_startup("raft-cli", prepare, move |service| async move {
            maintenance::wait_for_resume(&service, &return_root, index).await?;
            service.start_managed_first_group(history, TIMEOUT).await?;
            bind(service, history).await
        })?
    } else if !retained && index == 0 {
        // The fixture's first process also runs the independent operator.
        host.raft_cells_with_startup("raft-cli", prepare, move |service| async move {
            ensure!(service.observe_node(TIMEOUT).await?.node.groups == 0);
            provision_first_group(&fixture_operator, &service, &root, history).await?;
            let discovered = service
                .start_managed_namespace_first_group(history.cluster, history.namespace, TIMEOUT)
                .await?;
            bind(service, discovered).await
        })?
    } else {
        host.managed_sql_namespace(
            "raft-cli",
            (history.cluster, history.namespace),
            if retained {
                TIMEOUT
            } else {
                Duration::from_secs(20)
            },
            prepare,
            bind,
        )?
    };
    let result = host.run_until(std::future::pending());
    let _ = axum_stop.send(true);
    let axum_result = runtime.block_on(async {
        let joined = match axum_task.await {
            Ok(task) => task
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result.map_err(Into::into)),
            Err(_) => Ok(()),
        };
        if let Ok(worker) = maintenance_task.await {
            worker.shutdown().await;
        }
        let repaired = match repair_task.await {
            Ok(worker) => worker
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result),
            Err(_) => Ok(()),
        };
        domain.shutdown().await;
        joined.and(repaired)
    });
    result.and(axum_result)
}

impl Operator {
    async fn prepare(
        self: Arc<Self>,
        native: NativeRaftNode,
        index: u8,
        retained: bool,
        returning: bool,
        store: Arc<dyn object_store::ObjectStore>,
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
        if std::env::var_os(docker::VOTER).is_some() {
            getrandom::fill(&mut process)
                .map_err(|error| anyhow::anyhow!("process generation: {error}"))?;
        } else {
            process[..4].copy_from_slice(&std::process::id().to_be_bytes());
        }
        let process = if returning {
            let consent = self
                .fleet
                .restart_operator_return_request(
                    &snapshot,
                    ProposalId::new(maintenance::OPERATION)?,
                    TIMEOUT,
                )
                .await?
                .context("rooted daemon maintenance return")?;
            ensure!(
                consent.node == id,
                "maintenance consent belongs to another node"
            );
            consent.next_process
        } else {
            ProcessGeneration::new(process)?
        };
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
        let byte_capacity = host.images.max_bytes
            + if index == standby::INDEX {
                repair::STANDBY_DURABLE_HEADROOM
            } else {
                0
            };
        host.node_profile = Some(RaftNodeProfile {
            pool: "daemon-fixture".into(),
            region: "local".into(),
            zone: format!("process-{index}"),
            group_capacity: u32::try_from(host.max_groups)?,
            byte_capacity,
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
                groups: vec![],
                peers,
                query_origins: vec![],
                command_origins: vec![],
                host,
                shared,
            },
            listeners,
            tls: fixture_tls()?,
            store,
            versions: fixture_versions()?,
        })
    }
}

pub(super) async fn verify_park_wake(
    root: &Path,
    client: &reqwest::Client,
    addresses: &[ManagedIngress],
    receipt: &Value,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    let (leader, term, _) = recovery::observe_leader(client, addresses, None, 1, receipt).await?;
    let history = history()?;
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let operator = Operator::open(domain.clone(), root)?;
    let catalog = hex::encode(operator.fleet.catalog_id()?);
    drop(operator);
    domain.shutdown().await;
    let cli = OperatorCli::connect(addresses[leader].axum.clone(), catalog.clone());
    let namespace = hex::encode(history.namespace.bytes());
    let request = RaftFleetParkRequest {
        catalog_id: catalog,
        namespace_id: namespace.clone(),
        group: RaftFleetTransitionGroup {
            group_id: hex::encode(history.group.bytes()),
            incarnation_id: hex::encode(history.incarnation.bytes()),
        },
        operation_id: hex::encode([119; 16]),
        request_id: hex::encode([120; 16]),
        settlement_request_id: hex::encode([121; 16]),
        expected_revision: cli.revision().await?,
    };
    let plan = root.join("managed-park.json");
    persist(root, "managed-park.json", &serde_json::to_vec(&request)?)?;
    let plan = plan.to_str().context("park plan path")?;
    let send = async |verb: &str| {
        cli.call_with_timeout(
            &["group", verb, &namespace, "--plan", plan],
            OWNER,
            Duration::from_secs(32),
        )
        .await?
        .success()
    };
    let refused = cli
        .call(&["group", "park", &namespace, "--plan", plan], cli::READER)
        .await?;
    ensure!(refused.code == 4, "reader admitted park: {}", refused.value);
    ensure!(
        send("park-status").await?.is_null(),
        "park exists before admission"
    );

    let parked: RaftFleetParkStatus = serde_json::from_value(send("park").await?)?;
    ensure!(
        parked.phase == RaftFleetParkPhase::Delivered,
        "park delivery did not settle"
    );
    let observed: RaftFleetParkStatus = serde_json::from_value(send("park-status").await?)?;
    let replayed: RaftFleetParkStatus = serde_json::from_value(send("park").await?)?;
    ensure!(
        observed == parked && replayed == parked,
        "park replay changed the rooted decision"
    );
    for address in addresses {
        let status: Value = client
            .get(format!("{}{}", address.axum, recovery::OBSERVATION))
            .bearer_auth(SECRET)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            status["ready"] == false && status["term"] == term,
            "status or replay woke a parked voter: {status}"
        );
    }
    ledger.record(serde_json::json!({
        "phase": "managed_park_delivered", "term": term,
        "operation": parked.operation_id, "generation": parked.generation,
        "members": addresses.len(),
    }))?;
    let wake: RaftFleetWakeStatus = serde_json::from_value(send("wake").await?)?;
    ensure!(
        wake.park == parked && wake.applied_term > term && wake.applied_index > parked.generation,
        "wake lacks a fresh applied quorum barrier: {wake:?}"
    );
    ledger.record(serde_json::json!({
        "phase": "managed_park_wake", "operation": wake.park.operation_id,
        "generation": wake.park.generation, "term": wake.applied_term, "index": wake.applied_index,
    }))?;
    for ingress in Ingress::ALL {
        let address = ingress.endpoint(&addresses[leader]);
        let read: Value = client
            .get(format!("{address}/counters/one"))
            .bearer_auth(SECRET)
            .header("x-read-consistency", "linearizable")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            read["value"] == receipt["value"]
                && read["group"] == receipt["group"]
                && read["group_incarnation"] == receipt["group_incarnation"]
                && read["incarnation"] == receipt["incarnation"],
            "wake read changed cell state or history: {read}"
        );
        ledger.record(serde_json::json!({
            "phase": "managed_park_read", "operation": wake.park.operation_id,
            "ingress": ingress.label(), "receipt": read,
        }))?;
    }
    Ok(())
}

pub(super) async fn wait_for_cold_boot(
    root: &Path,
    index: u8,
    process: &mut Process,
) -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = domain
        .run(async {
            let operator = Operator::open(domain.clone(), root)?;
            let node = node(index)?;
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let remaining = deadline
                    .saturating_duration_since(Instant::now())
                    .min(TIMEOUT);
                ensure!(
                    process.child.try_wait()?.is_none() && !remaining.is_zero(),
                    "managed voter {index} did not root its cold boot\n{}",
                    process.logs()?
                );
                let cut = operator.fleet.read(remaining).await?;
                let remaining = deadline
                    .saturating_duration_since(Instant::now())
                    .min(TIMEOUT);
                let boot = operator.fleet.authorize_host(&cut, node, remaining).await?;
                if boot.storage_instance().is_some() {
                    return Ok(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
    domain.shutdown().await;
    result?
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
    let selected = operator
        .domain
        .run(
            operator
                .adapter
                .existing_namespace(declaration.namespace.clone()),
        )
        .await??;
    let directory = RangeDirectory::new(selected, limits)?;
    let cut = operator.fleet.read(TIMEOUT).await?;
    cellar::asyncrt::timeout(Duration::from_secs(15), async {
        for index in 1..3 {
            let observed = inspect_current(service, node(index)?, cut.revision()).await?;
            ensure!(
                observed.node.groups == 0,
                "standby already attached a group"
            );
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let control = RaftFleetControl::new(&operator.domain, operator.fleet.clone(), vec![directory])?
        .with_node_observer(service.clone());
    let cli = OperatorCli::start(
        &operator.domain,
        operator.bucket.clone(),
        control,
        root,
        PRINCIPAL,
    )
    .await?;
    let result = provision_group_metadata(&cli, service, root, history).await;
    cli.close().await?;
    result
}

async fn provision_group_metadata(
    cli: &OperatorCli,
    service: &RaftCells,
    root: &Path,
    history: GroupHistory,
) -> Result<()> {
    for index in 0..3 {
        admit_capacity(cli, service, root, index).await?;
    }
    let namespace = hex::encode(history.namespace.bytes());
    let observed: RaftFleetFirstGroupStandby = serde_json::from_value(
        cli.call(&["first-group", "standby", &namespace], OWNER)
            .await?
            .success()?,
    )?;
    let mut standby = [0; 32];
    hex::decode_to_slice(observed.standby, &mut standby)?;
    let plan = first_group_plan(
        &cli.catalog,
        RaftManagedFirstGroupRequest {
            reservation: request(30, cli.revision().await?),
            standby,
            publication: ProposalId::new([32; 16])?,
            group: CellGroupBirthSpec {
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
            },
        },
    )?;
    let file = "first-group-setup.json";
    let plan_path = root.join(file);
    let path = plan_path.to_str().context("UTF-8 first group setup path")?;
    let setup = serde_json::json!({
        "reservation": plan,
        "publication": {
            "request_id": hex::encode([31; 16]),
            "expected_revision": plan.expected_revision + 1,
        },
        "archive": {
            "request_id": hex::encode([34; 16]),
            "expected_revision": plan.expected_revision + 2,
        },
    });
    persist(root, file, &serde_json::to_vec(&setup)?)?;
    let applied = cli
        .call(&["first-group", "apply", &namespace, "--plan", path], OWNER)
        .await?
        .success()?;
    ensure!(
        applied["reservation"]["stage"] == "reserved"
            && applied["publication"]["stage"] == "published"
            && applied["archive"]["stage"] == "archive_initialized"
    );
    Ok(())
}

async fn inspect_current(
    service: &RaftCells,
    node: NodeId,
    revision: u64,
) -> Result<RaftHostObservation> {
    let deadline = Instant::now() + TIMEOUT;
    let mut last_error = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "daemon could not inspect {node:?} at fleet revision {revision}: {last_error:?}"
        );
        match service.inspect_peer_node(node, revision, remaining).await {
            Ok(observed) => return Ok(observed.view),
            Err(error) => last_error = Some(error),
        }
        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
    }
}

async fn admit_capacity(
    cli: &OperatorCli,
    service: &RaftCells,
    root: &Path,
    index: u8,
) -> Result<()> {
    let node = node(index)?;
    let observed = inspect_current(service, node, cli.revision().await?).await?;
    ensure!(
        observed.node.groups == 0,
        "capacity must observe an empty standby"
    );
    let attestation = capacity_attestation(observed)?;
    let filename = format!("capacity-{index}.json");
    persist(root, &filename, &serde_json::to_vec(&attestation)?)?;
    let path = root.join(filename);
    let path = path.to_str().context("UTF-8 capacity attestation path")?;
    let node_hex = hex::encode(node.bytes());
    for (offset, verb, state) in [
        (0, "qualify", FleetNodeState::Ready),
        (1, "activate", FleetNodeState::Active),
    ] {
        let revision = cli.revision().await?;
        inspect_current(service, node, revision).await?;
        let admission: RaftFleetNodeAcceptance = serde_json::from_value(
            cli.call(
                &[
                    "nodes",
                    verb,
                    &node_hex,
                    "--attestation",
                    path,
                    "--operation-id",
                    &hex::encode([20 + index * 2 + offset; 16]),
                    "--expected-revision",
                    &revision.to_string(),
                ],
                OWNER,
            )
            .await?
            .success()?,
        )?;
        ensure!(admission.admission.admitted_state == state);
    }
    Ok(())
}

fn capacity_attestation(observed: RaftHostObservation) -> Result<FleetCapacityAttestation> {
    Ok(FleetCapacityAttestation {
        boot: observed.fleet_boot.context("capacity boot")?,
        profile_revision: 1,
        profile_digest: fleet_capacity_profile_digest(&observed.node.profile)?,
        cpu_millis: 1000,
        memory_bytes: 3 * 1024 * 1024 * 1024,
        durable_bytes: observed.node.profile.byte_capacity.max(1024 * 1024 * 1024),
        engine_slots: observed.node.profile.group_capacity.max(2),
        memory_swap_disabled: true,
        used_memory_bytes: 64 * 1024 * 1024,
        used_durable_bytes: 1024 * 1024,
        evidence_digest: [71; 32],
        evidence_reference: "fixture-only/daemon-process".into(),
    })
}
