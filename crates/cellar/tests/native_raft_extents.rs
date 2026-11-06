// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public node ownership across cells, closed registrations and joined restart.
#![allow(clippy::disallowed_methods)]
#[path = "support/cell_group_disaster_public_node.rs"]
#[allow(dead_code)]
mod public_node;

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, *};
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use std::{
    collections::BTreeMap,
    io::Write as _,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(5);

struct Group {
    namespace: NamespaceConfig,
    birth: CellGroupBirthSpec,
    cell: CellHistory,
    operation: ProposalId,
}

impl Group {
    fn new(schema: [u8; 32], index: u8) -> Result<Self> {
        let cluster = ClusterId::new([1; 16])?;
        let namespace_id = NamespaceId::new([2 + index; 16])?;
        let mut namespace =
            NamespaceConfig::new(cluster, namespace_id, NamespaceMode::Raft, schema);
        namespace.voters = VoterPolicy::SingleVoterDevelopment;
        namespace.image_storage = RaftImageStorage::Extents;
        Ok(Self {
            namespace,
            birth: CellGroupBirthSpec {
                history: GroupHistory {
                    cluster,
                    namespace: namespace_id,
                    group: GroupId::new([index; 16])?,
                    incarnation: IncarnationId::new([3; 16])?,
                },
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)?,
                creation: ProposalId::new([12 + index; 16])?,
                voters: vec![InitialMember {
                    member: MemberId::new(1)?,
                    node: NodeId::new([1; 16])?,
                }],
                learners: vec![],
            },
            cell: CellHistory {
                cluster,
                namespace: namespace_id,
                cell: CellId::new(format!("Counter:{index}"))?,
                incarnation: IncarnationId::new([4; 16])?,
            },
            operation: ProposalId::new([22 + index; 16])?,
        })
    }

    fn config(&self, initialize: bool) -> RaftNamespaceConfig {
        RaftNamespaceConfig {
            namespace: self.namespace.clone(),
            class: "Counter".into(),
            initialize: initialize.then(|| RaftNamespaceInitialization {
                operation: self.operation,
                groups: vec![self.birth.clone()],
            }),
        }
    }

    async fn provision(
        &self,
        root: &Path,
        store: Arc<dyn object_store::ObjectStore>,
        domain: &ExecutionDomain,
    ) -> Result<()> {
        let provision = RaftNamespaceProvisioning::open(
            domain,
            store,
            CatalogOptions::default(),
            self.config(true),
            DirectoryLimits::default(),
            CellGroupArchiveLimits::default(),
        )
        .await?;
        let archive = provision
            .prepare_archive(domain, self.birth.history)
            .await?;
        let mut journal = std::fs::File::create_new(root.join(self.cell.cell.as_str()))?;
        journal.write_all(archive.encoded_intent())?;
        journal.sync_all()?;
        std::fs::File::open(root)?.sync_all()?;
        archive.commit(domain).await?;
        Ok(())
    }

    async fn ready(&self, service: &RaftCells) -> Result<RaftCellHandle> {
        let group = service.group(self.birth.history)?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if group
                    .status()
                    .await
                    .is_ok_and(|status| status.ready && status.leader == Some(1))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        Ok(service.cell(self.cell.clone())?)
    }
}

async fn start(
    node: &Node,
    application: Arc<dyn Application>,
    store: Arc<dyn object_store::ObjectStore>,
    groups: &[Group],
    attached: usize,
    generation: u8,
    mut host: RaftHostOptions,
) -> Result<RaftCells> {
    let listeners = NodeRaftListeners {
        control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
    };
    let peer = RaftPeerNode {
        node: NodeId::new([1; 16])?,
        disk: DiskLineage::new([5; 16])?,
        process: ProcessGeneration::new([generation; 16])?,
        label: "extent-native".into(),
        server_name: "localhost".into(),
        control_address: listeners.control.local_addr()?,
        data_address: listeners.data.local_addr()?,
        query_address: listeners.query.local_addr()?,
    };
    let mut log = ReplicaLogOptions::new(
        node.info().data_dir.join("raft"),
        peer.node,
        peer.disk,
        peer.process,
        if generation == 1 {
            LogOpenMode::Create
        } else {
            LogOpenMode::Resume
        },
    );
    log.max_groups = 2;
    let mut shared = public_node::limits();
    shared.namespaces.max_namespaces = 2;
    if host.extents.is_some() {
        shared.controller.checkpoints = Some(CellGroupCheckpointPolicy {
            interval_entries: 1,
            max_parallel: 1,
        });
        shared.checkpoints = Some(CellGroupCheckpointLimits {
            max_sources: 2,
            max_retained_bytes: 32 * 1024 * 1024,
            max_cells: 32,
            max_transfer_bytes: 32 * 1024 * 1024,
            max_chunk_bytes: 4096,
        });
        shared.archive = Some(CellGroupNodeArchiveOptions {
            io: Default::default(),
            uploads: Default::default(),
            tails: Default::default(),
        });
    }
    host.max_input_bytes = shared.dispatch.max_input_bytes;
    node.start_raft_cells(
        RaftCellsConfig {
            infrastructure: NodeRaftConfig {
                log,
                consensus: RaftRuntimeOptions {
                    max_groups: 2,
                    tick_interval_ms: Some(20),
                    ..Default::default()
                },
                transport: TransportOptions::default(),
            },
            catalog: CatalogOptions::default(),
            namespaces: groups
                .iter()
                .map(|group| group.config(host.extents.is_none()))
                .collect(),
            groups: groups
                .iter()
                .take(attached)
                .map(|group| RaftGroupAssignment {
                    history: group.birth.history,
                    placement: RaftGroupPlacement::Replica,
                })
                .collect(),
            peers: vec![peer],
            query_origins: vec![],
            command_origins: vec![],
            host,
            shared,
        },
        RaftCellsServices {
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
            store,
            application,
            versions: VersionKeys::new(
                VersionSigningKey::new([9; 32], u64::MAX)?,
                vec![],
                Duration::from_secs(60),
            )?,
            recovery: None,
        },
    )
    .await
}

fn counter(callbacks: Arc<Callbacks>) -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [21; 32])?
        .initializer([24; 32], |_, sql| {
            sql.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)",
            )?;
            Ok(())
        })?
        .command("Increment", [22; 32], move |context, input| {
            callbacks.record(context.identity());
            let amount = i64::from_be_bytes(input.try_into()?);
            context
                .sql()
                .execute("UPDATE counter SET value=value+?1", [amount])?;
            let value: i64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [23; 32], |context, _| {
            let value: i64 =
                context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?)
}

fn application(class: RaftClass) -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
    ))
}

fn host(max_roots: usize) -> RaftHostOptions {
    RaftHostOptions {
        max_groups: 2,
        poll_ms: 10,
        extents: Some(RaftExtentOptions {
            inventory: RaftExtentLimits {
                max_roots,
                max_root_extents: 256,
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[derive(Default)]
struct Callbacks {
    by_cell: Mutex<BTreeMap<String, usize>>,
    refused: AtomicUsize,
}

impl Callbacks {
    fn record(&self, scope: &cellar_logic::cell_group::CellHistory) {
        *self
            .by_cell
            .lock()
            .unwrap()
            .entry(scope.cell.as_str().to_owned())
            .or_default() += 1;
    }

    fn count(&self, scope: &CellHistory) -> usize {
        self.by_cell
            .lock()
            .unwrap()
            .get(scope.cell.as_str())
            .copied()
            .unwrap_or(0)
    }

    fn total(&self) -> usize {
        self.by_cell.lock().unwrap().values().sum()
    }
}

async fn write(
    cell: &RaftCellHandle,
    contract: &OperationContract,
    sequence: u64,
    amount: i64,
    callbacks: &Callbacks,
) -> Result<CommandReceipt> {
    let retry_until_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    let mut options = CommandOptions::new(
        CommandId::new(ClientId::new([7; 16])?, sequence)?,
        retry_until_ms,
    );
    options.durability = Durability::S3;
    let deadline = tokio::time::Instant::now() + options.timeout;
    loop {
        options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        ensure!(
            !options.timeout.is_zero(),
            "extent command admission deadline"
        );
        let before = callbacks.count(cell.history());
        let result = cell
            .command(contract, amount.to_be_bytes().to_vec(), options.clone())
            .await;
        let executed = callbacks.count(cell.history()) - before;
        ensure!(
            executed <= 1,
            "one command attempt executed its callback more than once"
        );
        match result {
            Err(error)
                if matches!(error.cause.refusal(), Some(CommandFailure::Overloaded))
                    || matches!(
                        *error.cause,
                        CellDispatchError::Runtime(RuntimeError::Overloaded)
                    ) =>
            {
                // Capture may exhaust physical roots after the private SQL
                // callback. This definite pre-proposal refusal discards that
                // execution; retrying the same identity may run it again.
                callbacks.refused.fetch_add(executed, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            result => {
                return result
                    .map_err(anyhow::Error::from)
                    .with_context(|| format!("extent write sequence={sequence}, amount={amount}"))
            }
        }
    }
}

async fn close(node: Node, service: RaftCells) -> Result<()> {
    let closed = service.close().await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    ensure!(
        service.resource_usage().serving_images.extents.is_none(),
        "retained SDK handle owns extent storage after close"
    );
    ensure!(closed?.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
    shutdown?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extent_activation_requires_physical_admission_before_catalog_mutation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let class = counter(Arc::new(Callbacks::default()))?;
    let group = Group::new(class.schema_digest(), 1)?;
    let app = application(class)?;
    let node = public_node::node_named(root.path(), app.clone(), "extent-native").await?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let result = start(
        &node,
        app,
        store.clone(),
        &[group],
        1,
        1,
        RaftHostOptions {
            max_groups: 2,
            ..Default::default()
        },
    )
    .await;
    node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        result.is_err(),
        "extent namespace activated without physical admission"
    );
    ensure!(
        store.list(None).try_collect::<Vec<_>>().await?.is_empty(),
        "failed preflight wrote catalog state"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn node_extent_gc_preserves_closed_groups_and_releases_the_shared_inventory_on_shutdown(
) -> Result<()> {
    exercise(12).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn extent_gc_retries_preserve_exact_committed_callbacks_under_root_pressure() -> Result<()> {
    exercise(8).await
}

async fn exercise(max_roots: usize) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar=debug")
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir()?;
    let calls = Arc::new(Callbacks::default());
    let class = counter(calls.clone())?;
    let groups = [
        Group::new(class.schema_digest(), 1)?,
        Group::new(class.schema_digest(), 2)?,
    ];
    let increment = class.contract("Increment").context("Increment contract")?;
    let get = class.contract("Get").context("Get contract")?;
    let app = application(class)?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    for group in &groups {
        group.provision(root.path(), store.clone(), &domain).await?;
    }
    domain.shutdown().await;
    let node = public_node::node_named(root.path(), app.clone(), "extent-native").await?;
    let service = start(
        &node,
        app.clone(),
        store.clone(),
        &groups,
        2,
        1,
        host(max_roots),
    )
    .await?;
    let result = async {
        let a = groups[0].ready(&service).await?;
        let b = groups[1].ready(&service).await?;
        for (cell, id) in [(&a, 1), (&b, 2)] {
            cell.enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([32 + id; 16])?,
                    durability: Durability::S3,
                },
                TIMEOUT,
            )
            .await?;
        }
        let mut retained = None;
        for sequence in 1..=16 {
            let (left, right) = futures_util::future::join(
                write(&a, &increment, sequence, 1, &calls),
                write(&b, &increment, sequence, 3, &calls),
            )
            .await;
            ensure!(left?.result == i64::try_from(sequence)?.to_be_bytes());
            let right = right?;
            ensure!(right.result == (3 * i64::try_from(sequence)?).to_be_bytes());
            retained = Some(right);
        }
        let usage = service
            .resource_usage()
            .serving_images
            .extents
            .context("missing shared node inventory")?;
        ensure!(!usage.quarantined && usage.roots <= max_roots && usage.workspaces <= 2);
        let text = service.resource_usage().prometheus();
        for inventory in ["serving", "archive_verifier"] {
            for metric in [
                "bytes",
                "capacity_bytes",
                "objects",
                "roots",
                "workspace_bytes",
                "quarantined",
            ] {
                ensure!(
                    text.lines().any(|line| line.starts_with(&format!(
                        "cellar_cell_groups_extent_{metric}{{kind=\"{inventory}\"}} "
                    ))),
                    "missing {inventory} extent {metric} metric"
                );
            }
            ensure!(text.contains(&format!(
                "cellar_cell_groups_extents_sample_available{{kind=\"{inventory}\"}} 1\n"
            )));
        }
        for group in &groups {
            let observed = service.group(group.birth.history)?.observe(TIMEOUT).await?;
            ensure!(observed.archive_required && observed.checkpoint.index.get() > 0);
        }
        retained.context("missing retained receipt")
    }
    .await;
    let closed = close(node, service).await;
    let retained = result?;
    closed?;

    // Reattach only A. The node inventory also owns B's retained current
    // and checkpoint roots while the B SQL owner is absent.
    let node = public_node::node_named(root.path(), app.clone(), "extent-native").await?;
    let service = start(
        &node,
        app.clone(),
        store.clone(),
        &groups,
        1,
        2,
        host(max_roots),
    )
    .await?;
    let result = async {
        let a = groups[0].ready(&service).await?;
        let b = service.group(groups[1].birth.history)?;
        ensure!(
            b.status().await.is_err(),
            "retained group gained a runtime owner"
        );
        ensure!(service.resource_usage().retained_manifest_cells == Some(2));
        for sequence in 17..=32 {
            ensure!(
                write(&a, &increment, sequence, 1, &calls).await?.result
                    == i64::try_from(sequence)?.to_be_bytes()
            );
        }
        let collected = service
            .resource_usage()
            .collection
            .context("node collector")?;
        ensure!(
            collected.files_reclaimed > 0 && collected.failures == 0,
            "collector did not reclaim files while B was unattached: {collected:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = close(node, service).await;
    result?;
    closed?;

    let node = public_node::node_named(root.path(), app.clone(), "extent-native").await?;
    let service = start(&node, app, store, &groups, 2, 3, host(max_roots)).await?;
    let result = async {
        let b = groups[1].ready(&service).await?;
        let historical = b
            .query(
                ProposalId::new([40; 16])?,
                &get,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(retained.version_token.clone()),
                    ..Default::default()
                },
            )
            .await?;
        ensure!(historical.result == 48_i64.to_be_bytes());
        let before = calls.total();
        let duplicate = write(&b, &increment, 16, 3, &calls).await?;
        ensure!(duplicate.id == retained.id && duplicate.result == retained.result);
        ensure!(
            calls.total() == before,
            "restored receipt reran the callback"
        );
        ensure!(write(&b, &increment, 17, 3, &calls).await?.result == 51_i64.to_be_bytes());
        let refused = calls.refused.load(Ordering::SeqCst);
        ensure!(
            calls.total().checked_sub(refused) == Some(49),
            "expected 49 committed callbacks; total={}, pre-proposal refusals={refused}",
            calls.total()
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = close(node, service).await;
    result?;
    closed
}
