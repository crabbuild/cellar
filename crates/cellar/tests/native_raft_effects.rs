// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native committed business features: three-voter alarm/outbox groups and an
//! explicit single-voter development fixture for immutable Blob closure.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_alarm_native.rs"]
mod alarms;
#[path = "support/raft_binding_gate.rs"]
mod binding_gate;
#[path = "support/raft_catalog_fault.rs"]
#[allow(dead_code)]
mod faults;
#[path = "support/raft_outbox_loopback.rs"]
mod loopback;
#[path = "support/raft_outbox_native.rs"]
mod outbox;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::consensus::verification::{Catalog, CellArchive};
use cellar::sdk::{consensus::*, Application, Node, Output, Registry, ShutdownMode};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn outbox_never_speculative() -> Result<()> {
    outbox::never_speculative().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn effect_leadership_race() -> Result<()> {
    outbox::leadership_race().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delivery_progress_replicated() -> Result<()> {
    outbox::replicated_progress().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_binding_gate() -> Result<()> {
    binding_gate::native_gate().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alarm_no_traffic_recovery() -> Result<()> {
    alarms::no_traffic_recovery().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_alarm_private_intent_is_not_scheduled() -> Result<()> {
    alarms::private_intent().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_alarm_rearm_cancel_and_public_system_refusal() -> Result<()> {
    alarms::rearm_cancel_and_system_refusal().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_alarm_requires_scheduler_before_catalog_activation() -> Result<()> {
    alarms::disabled_scheduler_preflight().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_alarm_node_shutdown_joins_owned_timer() -> Result<()> {
    alarms::node_shutdown_joins_timer().await
}

struct Definition {
    application: Arc<dyn Application>,
    schema: [u8; 32],
    store: OperationContract,
    read: OperationContract,
}

fn definition(limits: Option<RaftBlobLimits>, calls: Arc<AtomicUsize>) -> Result<Definition> {
    let mut class = RaftClass::new("Artifacts", 1, [45; 32])?;
    if let Some(limits) = limits {
        class = class.immutable_blobs(limits)?;
    }
    class = class
        .command("Store", [46; 32], move |context, input| {
            calls.fetch_add(1, Ordering::SeqCst);
            let reference = context.put_blob(input)?;
            ensure!(context.blob(reference)? == input);
            if input.starts_with(b"abort:") {
                anyhow::bail!("business transaction refused");
            }
            Ok(reference.encode().to_vec())
        })?
        .query("Read", [47; 32], |context, input| {
            context.blob(RaftBlobReference::decode(input)?)
        })?;
    let schema = class.schema_digest();
    let store = class.contract("Store").context("Store")?;
    let read = class.contract("Read").context("Read")?;
    let application =
        Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
    Ok(Definition {
        application,
        schema,
        store,
        read,
    })
}

struct Native {
    root: tempfile::TempDir,
    node: Node,
    service: RaftCells,
    definition: Definition,
    registration: RaftCellRegistration,
    store: Arc<faults::Store>,
}

impl Native {
    async fn start(
        limits: Option<RaftBlobLimits>,
        store: Arc<faults::Store>,
        calls: Arc<AtomicUsize>,
    ) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let definition = definition(limits, calls)?;
        let mut namespace = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            definition.schema,
        );
        namespace.voters = VoterPolicy::SingleVoterDevelopment;
        let registration = RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace,
            class: "Artifacts".into(),
            birth: BirthProposal {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: "Artifacts:one".into(),
                    group: GroupId::new([8; 16])?,
                    incarnation: IncarnationId::new([9; 16])?,
                },
                voters: vec![InitialMember {
                    member: MemberId::new(1)?,
                    node: NodeId::new([1; 16])?,
                }],
                learners: vec![],
            },
        };
        let node = public_node::node(root.path(), definition.application.clone()).await?;
        let service = match public_node::cells(
            &node,
            definition.application.clone(),
            store.clone(),
            registration.clone(),
            false,
        )
        .await
        {
            Ok(service) => service,
            Err(error) => {
                node.shutdown(ShutdownMode::Preserve).await?;
                return Err(error);
            }
        };
        let result = Self {
            root,
            node,
            service,
            definition,
            registration,
            store,
        };
        public_node::ready(&result.cell()?).await?;
        Ok(result)
    }

    fn cell(&self) -> Result<RaftCellHandle> {
        Ok(self.service.cell(
            self.registration.namespace.namespace,
            &self.registration.birth.scope.cell,
        )?)
    }

    async fn write(
        &self,
        sequence: u64,
        bytes: &[u8],
        durability: Durability,
        timeout: Duration,
    ) -> Result<CommandReceipt, CommandFailure> {
        let mut options =
            public_node::options(sequence).map_err(|_| CommandFailure::Unavailable)?;
        options.durability = durability;
        options.timeout = timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            let result = self
                .cell()
                .map_err(|_| CommandFailure::Unavailable)?
                .command(
                    self.definition.store.clone(),
                    bytes.to_vec(),
                    options.clone(),
                )
                .await;
            if !matches!(result, Err(CommandFailure::Overloaded))
                || tokio::time::Instant::now() >= deadline
            {
                return result;
            }
            tokio::time::sleep(
                Duration::from_millis(5)
                    .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
            )
            .await;
        }
    }

    async fn archive(&self) -> Result<CellArchive> {
        let namespace = Catalog::new(
            self.store.clone(),
            CatalogOptions {
                prefix: "host/test".into(),
                ..Default::default()
            },
        )?
        .existing_namespace(self.registration.namespace.clone())
        .await?;
        let birth = namespace
            .birth(&self.registration.birth.scope.cell)
            .await?
            .context("birth")?;
        Ok(namespace.cell_archive(
            &birth,
            cellar_ltx::LtxHost::default(),
            ArchiveLimits::default(),
        )?)
    }

    async fn close(self) -> Result<tempfile::TempDir> {
        ensure!(self.service.close().await?.unfinished.is_empty());
        self.node.shutdown(ShutdownMode::Preserve).await?;
        Ok(self.root)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_archive_dependency() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let native = Native::start(
        Some(RaftBlobLimits::default()),
        store.clone(),
        calls.clone(),
    )
    .await?;
    let first = native
        .write(
            1,
            b"first immutable bytes",
            Durability::S3,
            Duration::from_secs(5),
        )
        .await?;
    ensure!(first.durability == Durability::S3);
    let archive = native.archive().await?;
    let first_set = archive.recovery_set().await?.context("first archive")?;
    ensure!(first_set.dependencies().count() == 1);

    store.dependency_unavailable.store(true, Ordering::Release);
    let second = native
        .write(
            2,
            b"second immutable bytes",
            Durability::Quorum,
            Duration::from_secs(5),
        )
        .await?;
    let refused = native
        .write(
            2,
            b"second immutable bytes",
            Durability::S3,
            Duration::from_millis(200),
        )
        .await;
    ensure!(
        matches!(&refused, Err(CommandFailure::ArchiveUnavailable { command } | CommandFailure::Ambiguous { command }) if *command == second.id),
        "S3 wait did not refuse the missing dependency: {refused:?}"
    );
    ensure!(
        archive
            .recovery_set()
            .await?
            .context("old archive disappeared")?
            .position()
            .index
            < second.version.position.index
    );
    let read = native
        .cell()?
        .query(
            native.definition.read.clone(),
            second.result.clone(),
            QueryOptions {
                consistency: QueryConsistency::AtLeast(second.version_token.clone()),
                ..Default::default()
            },
        )
        .await?;
    ensure!(read.result == b"second immutable bytes");
    store.dependency_unavailable.store(false, Ordering::Release);
    let durable = native
        .write(
            2,
            b"second immutable bytes",
            Durability::S3,
            Duration::from_secs(5),
        )
        .await?;
    ensure!(
        durable.durability == Durability::S3
            && durable.result == second.result
            && calls.load(Ordering::SeqCst) == 2
    );
    let selected = archive
        .recovery_set()
        .await?
        .context("second archive")?
        .reference();
    drop(native.close().await?); // Remove every node-owned file before restore.

    let set = archive.recovery_set_at(&selected).await?;
    let reference = RaftBlobReference::decode(&durable.result)?;
    ensure!(
        set.dependencies().count() == 2
            && set
                .dependencies()
                .any(|(digest, body)| digest == reference.digest()
                    && body == b"second immutable bytes")
    );
    let restored = tempfile::tempdir()?;
    let images = cellar_ltx::generation::GenerationStore::open(
        restored.path().join("images"),
        Arc::new(cellar_ltx::DirectFileSystem),
        cellar_ltx::generation::GenerationLimits::default(),
    )?;
    let pin = set.restore(&images, cellar_ltx::generation::ImageId::new([55; 16])?)?;
    let query = cellar::storage::open_immutable_generation(
        pin,
        cellar::storage::ImmutableQueryOptions {
            services: cellar_ltx::generation::ReadServices {
                now_unix_millis: Arc::new(|| 0),
                fill_random: Arc::new(|bytes| bytes.fill(17)),
            },
            sqlite_vec: false,
        },
    )?;
    let bytes: Vec<u8> = query.connection().query_row(
        "SELECT body FROM __sys_raft_blobs WHERE digest=?1",
        [RaftBlobReference::decode(&durable.result)?
            .digest()
            .as_slice()],
        |row| row.get(0),
    )?;
    ensure!(bytes == b"second immutable bytes");

    let dependency = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|o| o.location.as_ref().contains("/dependencies/"))
        .context("dependency object")?;
    store.delete(&dependency.location).await?;
    ensure!(
        archive.recovery_set_at(&selected).await.is_err(),
        "missing selected dependency was ignored"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_private_rollback_and_duplicate_content() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    let native = Native::start(
        Some(RaftBlobLimits::default()),
        store,
        Arc::new(AtomicUsize::new(0)),
    )
    .await?;
    ensure!(native
        .write(
            1,
            b"abort:private",
            Durability::Quorum,
            Duration::from_secs(5)
        )
        .await
        .is_err());
    let one = native
        .write(1, b"committed", Durability::S3, Duration::from_secs(5))
        .await?;
    let two = native
        .write(2, b"committed", Durability::S3, Duration::from_secs(5))
        .await?;
    ensure!(one.result == two.result);
    let archive = native.archive().await?;
    drop(native.close().await?);
    let set = archive.recovery_set().await?.context("archive")?;
    ensure!(
        set.dependencies().count() == 1 && set.dependencies().next().unwrap().1 == b"committed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blob_capability_and_archive_capacity_gate() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let plain = definition(None, calls.clone())?;
    ensure!(plain.schema != definition(Some(RaftBlobLimits::default()), calls.clone())?.schema);
    let native = Native::start(None, Arc::new(faults::Store::default()), calls.clone()).await?;
    ensure!(matches!(
        native
            .write(
                1,
                b"not enabled",
                Durability::Quorum,
                Duration::from_secs(5)
            )
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    drop(native.close().await?);
    let store = Arc::new(faults::Store::default());
    let outcome = Native::start(
        Some(RaftBlobLimits {
            max_objects: 129,
            ..Default::default()
        }),
        store.clone(),
        calls,
    )
    .await;
    ensure!(
        outcome.is_err(),
        "capacity incompatible class was activated"
    );
    ensure!(
        store.list(None).try_collect::<Vec<_>>().await?.is_empty(),
        "preflight failure created catalog objects"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_delivery_authentication_and_receipt_replay() -> Result<()> {
    outbox::authentication_and_replay().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_alarm_and_outbox_share_internal_client() -> Result<()> {
    outbox::shared_alarm_client().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_outbox_self_delivery_definition_identity() -> Result<()> {
    loopback::self_delivery().await
}
