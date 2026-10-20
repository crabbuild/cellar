//! Archive storage/restore qualification through real engines and physical LTX.
//! Direct durable-marker fixtures qualify archive storage; the Native fixture
//! separately exercises actual Node/SDK acknowledgement and outage boundaries.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_cluster.rs"]
#[allow(dead_code)]
mod cluster;
#[path = "support/raft_catalog_fault.rs"]
mod faults;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod images;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::application::consensus::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate};
use cellar::sdk::consensus::*;
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_logic::NamespaceMode;
use cellar_ltx::generation::{GenerationLimits, GenerationPin, GenerationStore};
use cellar_ltx::{DirectFileSystem, LtxHost};
use futures_util::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use protobuf::Message as _;
use raft::{
    prelude::{ConfChangeSingle, ConfChangeType, ConfChangeV2, Entry, EntryType},
    Storage,
};
use std::sync::{atomic::Ordering, Arc};

struct Fixture {
    _root: tempfile::TempDir,
    domain: ExecutionDomain,
    log: ReplicaLog,
    group: GroupLog,
    images: GenerationStore,
    store: Arc<faults::Store>,
    archive: CellArchive,
    verifier: ArchiveVerifier,
    namespace: NamespaceCatalog,
    birth: GroupBirth,
    cut: Option<CommittedArchiveCut>,
    pin: Option<GenerationPin>,
    bytes: Vec<u8>,
    txid: u8,
}

impl Fixture {
    async fn new(limits: ArchiveLimits) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(faults::Store::default());
        let catalog = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "archive-test".into(),
                ..Default::default()
            },
        )?;
        let mut config = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            [9; 32],
        );
        config.voters = VoterPolicy::SingleVoterDevelopment;
        let namespace = catalog.open_namespace(config).await?;
        let birth = namespace
            .resolve_birth(BirthProposal {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: "Counter:7".into(),
                    group: GroupId::new([7; 16])?,
                    incarnation: IncarnationId::new([3; 16])?,
                },
                voters: vec![InitialMember {
                    member: MemberId::new(1)?,
                    node: NodeId::new([5; 16])?,
                }],
                learners: vec![],
            })
            .await?;
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.path().join("raft"),
                    NodeId::new([5; 16])?,
                    DiskLineage::new([6; 16])?,
                    ProcessGeneration::new([7; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let group = log
            .create_group(GroupBootstrap {
                scope: birth.scope().clone(),
                member: MemberId::new(1)?,
                birth_digest: birth.digest(),
                voters: vec![MemberId::new(1)?],
                learners: vec![],
            })
            .await?;
        let generations = GenerationStore::open(
            root.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let archive = namespace.cell_archive(&birth, LtxHost::default(), limits)?;
        let mut verifier_limits = GenerationLimits::default();
        verifier_limits.apply.decode.max_encoded_bytes = 64 * 1024 * 1024;
        verifier_limits.apply.max_total_encoded_bytes = 64 * 1024 * 1024;
        let verifier = ArchiveVerifier::new(
            GenerationStore::open(
                root.path().join("archive-verifier"),
                Arc::new(DirectFileSystem),
                verifier_limits,
            )?,
            images::id(92),
        )?;
        Ok(Self {
            _root: root,
            domain,
            log,
            group,
            images: generations,
            store,
            archive,
            verifier,
            namespace,
            birth,
            cut: None,
            pin: None,
            bytes: Vec::new(),
            txid: 0,
        })
    }

    fn payload(&self, cut: &images::Cut) -> Result<Vec<u8>> {
        let position = |state: cellar_ltx::apply::ImageState| LtxImage {
            epoch: state.epoch,
            txid: state.txid.0,
            checksum: state.checksum,
            image_digest: state.digest,
        };
        Ok(CellUpdate {
            scope: self.group.identity().scope.clone(),
            application_digest: [9; 32],
            proposal: ProposalId::new([self.txid + 1; 16])?,
            base: position(cut.base),
            result: position(cut.result),
            files: vec![LtxUpdate {
                min_txid: cut.base.txid.0 + 1,
                max_txid: cut.result.txid.0,
                page_count: cut.pages.len() as u64,
                decoded_bytes: cut.pages.len() as u64 * 512,
                bytes: cut.bytes.clone(),
            }],
        }
        .encode(EnvelopeLimits::default())?)
    }

    async fn commit(
        &mut self,
        value: i64,
        dependencies: Vec<ArchiveDependency>,
    ) -> Result<CommittedArchiveCut> {
        self.commit_payload(value, dependencies, false).await
    }

    async fn commit_payload(
        &mut self,
        value: i64,
        dependencies: Vec<ArchiveDependency>,
        corrupt: bool,
    ) -> Result<CommittedArchiveCut> {
        let next = if self.txid == 0 {
            images::image(
                None,
                &format!("CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES({value})"),
            )?
        } else {
            images::image(
                Some(&self.bytes),
                &format!("UPDATE counter SET value={value}"),
            )?
        };
        let cut = images::Cut::new(
            &self.bytes,
            &next,
            u64::from(self.txid),
            true,
            self.txid.is_multiple_of(2),
        );
        let pin =
            self.images
                .prepare(images::id(self.txid + 1), self.pin.as_ref(), &cut.update())?;
        let index = self.group.applied()?.position.index.get() + 1;
        let mut write = GroupWrite::new(&self.group);
        let mut payload = self.payload(&cut)?;
        if corrupt {
            // The mechanical storage fixture can lie about applied bytes. A
            // real ImageApplier refuses this entry; archive replay must also
            // refuse it before advancing the independently verified head.
            let mut update = CellUpdate::decode(&payload, EnvelopeLimits::default())?;
            let bytes = &mut update.files[0].bytes;
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
            payload = update.encode(EnvelopeLimits::default())?;
        }
        write.entries.push(Entry {
            index,
            term: 1,
            data: payload.into(),
            ..Default::default()
        });
        let mut hard = self.group.initial_state()?.hard_state;
        hard.commit = index;
        write.hard_state = Some(hard);
        let state = pin.state();
        write.applied = Some(DurableApply {
            position: LogPosition {
                index: LogIndex::new(index),
                term: Term::new(1),
            },
            image: Some(CommittedImage {
                name: format!("{}.sqlite", hex::encode(pin.id().bytes())),
                digest: state.digest,
                length: state.length,
                ltx_epoch: state.epoch,
                txid: state.txid.0,
                checksum: state.checksum,
                page_size: state.page_size,
            }),
        });
        self.log
            .persist(OperationId::new(index), vec![write])
            .await?;
        let published = ImageApplier::recover(
            self.group.clone(),
            self.images.clone(),
            [9; 32],
            EnvelopeLimits::default(),
        )?
        .current()
        .ok_or_else(|| anyhow::anyhow!("missing published image"))?;
        let archive_cut = CommittedArchiveCut::capture(
            &self.group,
            published,
            dependencies,
            ArchiveLimits::default(),
        )?;
        if let Some(old) = self.pin.take() {
            self.images.retire(old.id())?;
        }
        self.pin = Some(pin);
        self.bytes = next;
        self.txid += 1;
        self.cut = Some(archive_cut.clone());
        self.images.reclaim(8)?;
        Ok(archive_cut)
    }

    async fn publish(&self, cut: CommittedArchiveCut) -> Result<ArchiveRecoverySet, ArchiveError> {
        self.archive.publish(cut, &self.verifier).await
    }

    async fn close(self) -> Result<()> {
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }

    async fn protocol(&self, learner: bool) -> Result<CommittedArchiveCut> {
        let mut marker = self.group.applied()?;
        let index = marker.position.index.get() + 1;
        marker.position.index = LogIndex::new(index);
        let mut write = GroupWrite::new(&self.group);
        let mut entry = Entry {
            index,
            term: 1,
            ..Default::default()
        };
        let mut hard = self.group.initial_state()?.hard_state;
        hard.commit = index;
        write.hard_state = Some(hard);
        write.applied = Some(marker);
        if learner {
            let mut change = ConfChangeV2::default();
            change.mut_changes().push(ConfChangeSingle {
                node_id: 2,
                change_type: ConfChangeType::AddLearnerNode,
                ..Default::default()
            });
            entry.set_entry_type(EntryType::EntryConfChangeV2);
            entry.data = change.write_to_bytes()?.into();
            let mut conf = self.group.initial_state()?.conf_state;
            conf.learners.push(2);
            write.conf_state = Some(conf);
        }
        write.entries.push(entry);
        self.log
            .persist(OperationId::new(index), vec![write])
            .await?;
        let image = ImageApplier::recover(
            self.group.clone(),
            self.images.clone(),
            [9; 32],
            EnvelopeLimits::default(),
        )?
        .current()
        .ok_or_else(|| anyhow::anyhow!("missing published image"))?;
        Ok(CommittedArchiveCut::capture(
            &self.group,
            image,
            vec![],
            ArchiveLimits::default(),
        )?)
    }
}

fn restored(set: &ArchiveRecoverySet, expected: i64) -> Result<Vec<u8>> {
    let root = tempfile::tempdir()?;
    let images = GenerationStore::open(
        root.path().join("restored"),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    let pin = set.restore(&images, images::id(91))?;
    let bytes = images::read(&pin)?;
    let query = images::query(pin)?;
    ensure!(
        query
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?
            == expected
    );
    Ok(bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_archive_restores_snapshot_and_physical_tail() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture
        .commit(
            7,
            vec![ArchiveDependency(b"immutable referenced object".to_vec())],
        )
        .await?;
    let set = fixture.publish(first).await?;
    ensure!(set.tail_entries() == 0);
    ensure!(
        set.dependencies().next().map(|(_, bytes)| bytes)
            == Some(b"immutable referenced object".as_slice())
    );
    ensure!(restored(&set, 7)? == fixture.bytes);
    let second = fixture.commit(19, vec![]).await?;
    let set = fixture.publish(second).await?;
    ensure!(set.tail_entries() == 1);
    ensure!(set.position().index == LogIndex::new(3));
    let expected = fixture.bytes.clone();
    let archive = fixture.archive.clone();
    fixture.close().await?;
    // The producer and its temporary directory have been dropped. Recovery
    // below has only the object store and the selected namespace/birth binding.
    let set = archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing archive head"))?;
    ensure!(restored(&set, 19)? == expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_never_speculative() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    let speculative = images::image(Some(&fixture.bytes), "UPDATE counter SET value=99")?;
    let change = images::Cut::new(&fixture.bytes, &speculative, 1, true, true);
    let mut write = GroupWrite::new(&fixture.group);
    write.entries.push(Entry {
        index: 3,
        term: 1,
        data: fixture.payload(&change)?.into(),
        ..Default::default()
    });
    fixture
        .log
        .persist(OperationId::new(3), vec![write])
        .await?;
    let set = fixture.publish(first).await?;
    ensure!(set.position().index == LogIndex::new(2));
    ensure!(set.tail_entries() == 0);
    restored(&set, 7)?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_head_monotonic() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    let second = fixture.commit(12, vec![]).await?;
    let (a, b) = tokio::join!(
        fixture.publish(first.clone()),
        fixture.publish(second.clone())
    );
    a?;
    b?;
    let set = fixture.publish(first).await?;
    ensure!(set.position() == second.position());
    restored(&set, 12)?;
    let (a, b) = tokio::join!(fixture.publish(second.clone()), fixture.publish(second));
    ensure!(a?.position() == b?.position());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_reconciles_lost_put_response() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    fixture.store.fail_after.store(true, Ordering::Release);
    let set = fixture.publish(first.clone()).await?;
    restored(&set, 7)?;
    let second = fixture.commit(13, vec![]).await?;
    fixture
        .store
        .fail_confirmation
        .store(true, Ordering::Release);
    ensure!(fixture.publish(second.clone()).await.is_err());
    let head = fixture
        .archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing prior head"))?;
    ensure!(head.position() == first.position());
    restored(&fixture.publish(second).await?, 13)?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_verifies_every_dependency_and_object() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let cut = fixture
        .commit(
            7,
            vec![ArchiveDependency(b"required immutable bytes".to_vec())],
        )
        .await?;
    fixture.publish(cut).await?;
    let objects = fixture.store.list(None).try_collect::<Vec<_>>().await?;
    let dependency = objects
        .iter()
        .find(|o| o.location.as_ref().contains("/dependencies/"))
        .ok_or_else(|| anyhow::anyhow!("missing dependency"))?;
    fixture.store.inner.delete(&dependency.location).await?;
    ensure!(fixture.archive.recovery_set().await.is_err());
    fixture
        .store
        .inner
        .put(
            &dependency.location,
            b"required immutable bytes".to_vec().into(),
        )
        .await?;
    ensure!(fixture.archive.recovery_set().await?.is_some());
    let snapshot = objects
        .iter()
        .find(|o| o.location.as_ref().contains("/snapshots/"))
        .ok_or_else(|| anyhow::anyhow!("missing snapshot"))?;
    fixture
        .store
        .inner
        .put(&snapshot.location, b"corrupt replacement".to_vec().into())
        .await?;
    ensure!(fixture.archive.recovery_set().await.is_err());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_cut_pins_retired_image_during_outage() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    fixture.store.fail_before.store(true, Ordering::Release);
    ensure!(fixture.publish(first.clone()).await.is_err());
    fixture.commit(23, vec![]).await?;
    fixture.images.reclaim(8)?;
    // The first image was retired and is no longer the current engine marker.
    // Its immutable captured receipt still pins the bytes needed for archiving.
    restored(&fixture.publish(first).await?, 7)?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_tail_limit_establishes_new_snapshot() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits {
        max_tails: 1,
        ..Default::default()
    })
    .await?;
    let first = fixture.commit(7, vec![]).await?;
    fixture.publish(first).await?;
    let second = fixture.commit(8, vec![]).await?;
    ensure!(fixture.publish(second).await?.tail_entries() == 1);
    let third = fixture.commit(9, vec![]).await?;
    let set = fixture.publish(third).await?;
    ensure!(set.tail_entries() == 0);
    restored(&set, 9)?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_refuses_foreign_birth_and_noncanonical_head() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    let another = Catalog::new(
        fixture.store.clone(),
        CatalogOptions {
            prefix: "archive-test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(fixture.namespace.config().clone())
    .await?;
    ensure!(another
        .cell_archive(&fixture.birth, LtxHost::default(), ArchiveLimits::default())
        .is_err());
    fixture.publish(first).await?;
    let head = fixture
        .store
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|o| o.location.filename() == Some("head"))
        .ok_or_else(|| anyhow::anyhow!("missing head"))?;
    let mut bytes = fixture
        .store
        .inner
        .get(&head.location)
        .await?
        .bytes()
        .await?
        .to_vec();
    bytes.push(0);
    fixture
        .store
        .inner
        .put(&head.location, bytes.into())
        .await?;
    ensure!(fixture.archive.recovery_set().await.is_err());
    ensure!(fixture
        .store
        .inner
        .get(&ObjectPath::from("head"))
        .await
        .is_err());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_noop_tail_and_exact_configuration_checkpoint() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    fixture.publish(first).await?;
    let noop = fixture.protocol(false).await?;
    let set = fixture.publish(noop).await?;
    ensure!(set.tail_entries() == 1 && set.position().index == LogIndex::new(3));
    restored(&set, 7)?;
    let changed = fixture.protocol(true).await?;
    let set = fixture.publish(changed).await?;
    ensure!(set.tail_entries() == 0 && set.position().index == LogIndex::new(4));
    ensure!(set.configuration()?.learners == vec![2]);
    restored(&set, 7)?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_capture_refuses_unrelated_newer_marker() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    fixture.commit(7, vec![]).await?;
    let old = ImageApplier::recover(
        fixture.group.clone(),
        fixture.images.clone(),
        [9; 32],
        EnvelopeLimits::default(),
    )?
    .current()
    .ok_or_else(|| anyhow::anyhow!("missing old image"))?;
    fixture.commit(21, vec![]).await?;
    ensure!(
        CommittedArchiveCut::capture(&fixture.group, old, vec![], ArchiveLimits::default())
            .is_err()
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_head_lost_response_and_confirmation() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    fixture.store.fail_head_after.store(true, Ordering::Release);
    restored(&fixture.publish(first).await?, 7)?;
    let second = fixture.commit(15, vec![]).await?;
    fixture
        .store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    ensure!(fixture.publish(second.clone()).await.is_err());
    // Losing the confirmation does not imply rollback: the exact head read
    // recovers the already-published cut, and repeating publication is safe.
    let set = fixture
        .archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing committed head"))?;
    ensure!(set.position() == second.position());
    restored(&fixture.publish(second).await?, 15)?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_replay_failure_never_publishes_head() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    fixture.publish(first.clone()).await?;
    let second = fixture.commit_payload(25, vec![], true).await?;
    ensure!(fixture.publish(second).await.is_err());
    let set = fixture
        .archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing prior head"))?;
    ensure!(set.position() == first.position());
    restored(&set, 7)?;
    // Partial verification images cannot be reused after a failed replay.
    ensure!(fixture.publish(first).await.is_err());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_refuses_different_engine_birth() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    fixture.commit(7, vec![]).await?;
    let log = fixture
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                fixture._root.path().join("wrong-birth"),
                NodeId::new([5; 16])?,
                DiskLineage::new([8; 16])?,
                ProcessGeneration::new([9; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let group = log
        .create_group(GroupBootstrap {
            scope: fixture.group.identity().scope.clone(),
            member: MemberId::new(1)?,
            birth_digest: [100; 32],
            voters: vec![MemberId::new(1)?],
            learners: vec![],
        })
        .await?;
    let initial = images::Cut::new(&[], &fixture.bytes, 0, true, true);
    let mut write = GroupWrite::new(&group);
    write.entries.push(Entry {
        index: 2,
        term: 1,
        data: fixture.payload(&initial)?.into(),
        ..Default::default()
    });
    let mut hard = group.initial_state()?.hard_state;
    hard.commit = 2;
    write.hard_state = Some(hard);
    write.applied = Some(fixture.group.applied()?);
    log.persist(OperationId::new(2), vec![write]).await?;
    let image = ImageApplier::recover(
        group.clone(),
        fixture.images.clone(),
        [9; 32],
        EnvelopeLimits::default(),
    )?
    .current()
    .ok_or_else(|| anyhow::anyhow!("missing image"))?;
    let cut = CommittedArchiveCut::capture(&group, image, vec![], ArchiveLimits::default())?;
    ensure!(matches!(
        fixture.publish(cut).await,
        Err(ArchiveError::Invalid(_))
    ));
    ensure!(fixture.archive.recovery_set().await?.is_none());
    log.close().await?;
    fixture.close().await
}

struct Native {
    _entered: tokio::sync::mpsc::UnboundedReceiver<()>,
    root: tempfile::TempDir,
    node: cellar::sdk::Node,
    service: RaftCells,
    definition: public_node::Counter,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    store: Arc<faults::Store>,
    registration: RaftCellRegistration,
    options: RaftHostOptions,
}

impl Native {
    async fn start(
        store: Arc<faults::Store>,
        options: RaftHostOptions,
        minimum: Durability,
    ) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (gate, entered) = public_node::Gate::new();
        gate.release();
        let definition = public_node::counter(calls.clone(), gate)?;
        let mut namespace = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            definition.schema,
        );
        namespace.voters = VoterPolicy::SingleVoterDevelopment;
        namespace.minimum_durability = minimum;
        let registration = RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace,
            class: "Counter".into(),
            birth: BirthProposal {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: "Counter:one".into(),
                    group: GroupId::new([5; 16])?,
                    incarnation: IncarnationId::new([6; 16])?,
                },
                voters: vec![InitialMember {
                    member: MemberId::new(1)?,
                    node: NodeId::new([1; 16])?,
                }],
                learners: vec![],
            },
        };
        let node = public_node::node(root.path(), definition.application.clone()).await?;
        let service = public_node::cells_with_host(
            &node,
            definition.application.clone(),
            store.clone(),
            registration.clone(),
            false,
            options.clone(),
        )
        .await?;
        let native = Self {
            _entered: entered,
            root,
            node,
            service,
            definition,
            calls,
            store,
            registration,
            options,
        };
        public_node::ready(&native.cell()?).await?;
        Ok(native)
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
        durability: Durability,
        timeout: std::time::Duration,
    ) -> Result<CommandReceipt, CommandFailure> {
        let mut options =
            public_node::options(sequence).map_err(|_| CommandFailure::Unavailable)?;
        options.durability = durability;
        options.timeout = timeout;
        self.cell()
            .map_err(|_| CommandFailure::Unavailable)?
            .command(
                self.definition.increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                options,
            )
            .await
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
            .ok_or_else(|| anyhow::anyhow!("missing birth"))?;
        Ok(namespace.cell_archive(&birth, LtxHost::default(), self.options.archive.limits)?)
    }

    async fn retry(&self, sequence: u64, durability: Durability) -> Result<CommandReceipt> {
        Ok(
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    match self
                        .write(sequence, durability, std::time::Duration::from_secs(5))
                        .await
                    {
                        Err(CommandFailure::Overloaded) => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await
                        }
                        result => return result,
                    }
                }
            })
            .await??,
        )
    }

    async fn restart(mut self) -> Result<Self> {
        self.service.close().await?;
        self.node
            .shutdown(cellar::sdk::ShutdownMode::Preserve)
            .await?;
        self.node =
            public_node::node(self.root.path(), self.definition.application.clone()).await?;
        self.service = public_node::cells_with_host(
            &self.node,
            self.definition.application.clone(),
            self.store.clone(),
            self.registration.clone(),
            true,
            self.options.clone(),
        )
        .await?;
        public_node::ready(&self.cell()?).await?;
        Ok(self)
    }

    async fn close(self) -> Result<()> {
        self.service.close().await?;
        self.node
            .shutdown(cellar::sdk::ShutdownMode::Preserve)
            .await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_ack_complete_restore_set() -> Result<()> {
    let native = Native::start(
        Arc::new(faults::Store::default()),
        RaftHostOptions::default(),
        Durability::S3,
    )
    .await?;
    let first = native.retry(1, Durability::Quorum).await?;
    ensure!(
        first.durability == Durability::S3,
        "namespace minimum was weakened"
    );
    let second = native.retry(2, Durability::S3).await?;
    ensure!(second.result == 2_i64.to_be_bytes() && second.durability == Durability::S3);
    let archive = native.archive().await?;
    native.close().await?;
    let set = archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("S3 success has no recovery set"))?;
    ensure!(set.position().index >= second.version.position.index);
    ensure!(
        set.tail_entries() > 0,
        "second command did not exercise physical tail replay"
    );
    let bytes = restored(&set, 2)?;
    let restored_root = tempfile::tempdir()?;
    let path = restored_root.path().join("receipts.sqlite");
    std::fs::write(&path, bytes)?;
    let database =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut statement = database.prepare(
        "SELECT sequence,result,minimum_durability FROM __sys_raft_receipts ORDER BY sequence",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        rows == vec![
            (first.id.sequence().to_be_bytes().to_vec(), first.result, 1),
            (
                second.id.sequence().to_be_bytes().to_vec(),
                second.result,
                1
            )
        ],
        "S3 recovery lost an acknowledged command receipt"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_s3_outage() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let mut options = RaftHostOptions::default();
    options.archive.max_unarchived_entries = 2;
    let native = Native::start(store.clone(), options, Durability::Quorum).await?;
    native.retry(1, Durability::Quorum).await?;
    native.retry(2, Durability::Quorum).await?;
    ensure!(matches!(
        native
            .write(3, Durability::Quorum, std::time::Duration::from_secs(2))
            .await,
        Err(CommandFailure::Overloaded)
    ));
    ensure!(
        native.calls.load(Ordering::SeqCst) == 2,
        "backpressure ran business callback"
    );
    let repeated = native.retry(1, Durability::Quorum).await?;
    ensure!(repeated.result == 1_i64.to_be_bytes());
    ensure!(
        native.calls.load(Ordering::SeqCst) == 2,
        "full backlog blocked duplicate lookup or reran work"
    );
    store.archive_unavailable.store(false, Ordering::Release);
    let third = native.retry(3, Durability::S3).await?;
    ensure!(third.result == 3_i64.to_be_bytes());
    native.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_timeout_ambiguous() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let native = Native::start(
        store.clone(),
        RaftHostOptions::default(),
        Durability::Quorum,
    )
    .await?;
    let options = public_node::options(1)?;
    let result = native
        .write(1, Durability::S3, std::time::Duration::from_millis(500))
        .await;
    ensure!(
        matches!(result, Err(CommandFailure::Ambiguous { command }) if command == options.id),
        "post-commit timeout was reported as rollback: {result:?}"
    );
    ensure!(native.calls.load(Ordering::SeqCst) == 1);
    let value = native
        .cell()?
        .query(
            native.definition.get.clone(),
            vec![],
            QueryOptions::default(),
        )
        .await?;
    ensure!(
        value.result == 1_i64.to_be_bytes(),
        "timed-out command was not committed"
    );
    store.archive_unavailable.store(false, Ordering::Release);
    let result = native.retry(1, Durability::S3).await?;
    ensure!(result.durability == Durability::S3 && result.result == 1_i64.to_be_bytes());
    let weaker = native.retry(1, Durability::Quorum).await?;
    ensure!(
        weaker.durability == Durability::S3,
        "stored S3 minimum was weakened"
    );
    ensure!(native.calls.load(Ordering::SeqCst) == 1);
    native.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_strengthens_durability() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let native = Native::start(
        store.clone(),
        RaftHostOptions::default(),
        Durability::Quorum,
    )
    .await?;
    let original = native.retry(1, Durability::Quorum).await?;
    let cell = native.cell()?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    let mut stronger = Box::pin(cell.command(
        native.definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        options,
    ));
    ensure!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut stronger)
            .await
            .is_err(),
        "stronger retry used only a Quorum proof"
    );
    ensure!(native.calls.load(Ordering::SeqCst) == 1);
    store.archive_unavailable.store(false, Ordering::Release);
    let repeated = tokio::time::timeout(std::time::Duration::from_secs(10), stronger).await??;
    ensure!(
        repeated.version == original.version
            && repeated.result == original.result
            && repeated.durability == Durability::S3
    );
    ensure!(native.calls.load(Ordering::SeqCst) == 1);
    let native = native.restart().await?;
    let repeated = native.retry(1, Durability::S3).await?;
    ensure!(repeated.version == original.version && repeated.durability == Durability::S3);
    ensure!(native.calls.load(Ordering::SeqCst) == 1);
    native.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_preserves_only_copy() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let mut options = RaftHostOptions::default();
    options.archive.max_unarchived_entries = 2;
    let native = Native::start(store.clone(), options, Durability::Quorum).await?;
    native.retry(1, Durability::Quorum).await?;
    native.retry(2, Durability::Quorum).await?;
    ensure!(matches!(
        native
            .write(3, Durability::Quorum, std::time::Duration::from_secs(2))
            .await,
        Err(CommandFailure::Overloaded)
    ));
    let native = native.restart().await?;
    ensure!(
        native
            .cell()?
            .query(
                native.definition.get.clone(),
                vec![],
                QueryOptions::default()
            )
            .await?
            .result
            == 2_i64.to_be_bytes()
    );
    store.archive_unavailable.store(false, Ordering::Release);
    let repeated = native.retry(2, Durability::S3).await?;
    let archive = native.archive().await?;
    native.close().await?;
    let set = archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing retained history"))?;
    ensure!(set.position().index >= repeated.version.position.index);
    restored(&set, 2)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_shutdown_joins_hung_store_without_acknowledging() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_hang.store(true, Ordering::Release);
    let native = Native::start(
        store.clone(),
        RaftHostOptions::default(),
        Durability::Quorum,
    )
    .await?;
    let cell = native.cell()?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    let pending = cell.command(
        native.definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        options,
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.archive_attempts.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), native.close()).await??;
    ensure!(matches!(
        pending.await,
        Err(CommandFailure::Ambiguous { .. })
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_commands_reclaim_generations_with_small_disk_admission() -> Result<()> {
    let mut options = RaftHostOptions::default();
    options.images.max_generations = 3;
    options.enable_snapshots = false;
    let native = Native::start(Arc::new(faults::Store::default()), options, Durability::S3).await?;
    for sequence in 1..=40 {
        let receipt = native.retry(sequence, Durability::S3).await?;
        ensure!(receipt.result == (sequence as i64).to_be_bytes());
    }
    ensure!(native.calls.load(Ordering::SeqCst) == 40);
    let native = native.restart().await?;
    ensure!(
        native
            .cell()?
            .query(
                native.definition.get.clone(),
                vec![],
                QueryOptions::default()
            )
            .await?
            .result
            == 40_i64.to_be_bytes()
    );
    native.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn s3_three_voter_failover_restores_only_committed_commands() -> Result<()> {
    let mut definitions = Vec::new();
    let mut entered = Vec::new();
    for _ in 0..3 {
        let (gate, receiver) = public_node::Gate::new();
        gate.release();
        entered.push(receiver);
        definitions.push(public_node::counter(
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            gate,
        )?);
    }
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "Counter:rf3".into(),
        group: GroupId::new([17; 16])?,
        incarnation: IncarnationId::new([18; 16])?,
    };
    let namespace = NamespaceConfig::new(
        scope.cluster,
        scope.namespace,
        NamespaceMode::Raft,
        definitions[0].schema,
    );
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: namespace.clone(),
        class: "Counter".into(),
        birth: BirthProposal {
            scope: scope.clone(),
            voters: (1..=3)
                .map(|n| {
                    Ok(InitialMember {
                        member: MemberId::new(n)?,
                        node: NodeId::new([n as u8; 16])?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            learners: vec![],
        },
    };
    let mut host = RaftHostOptions::default();
    host.images.max_generations = 3;
    host.enable_snapshots = false;
    let cluster =
        cluster::Cluster::start_with_host(&definitions, vec![registration], 20, host).await?;
    let handles = cluster
        .services
        .iter()
        .map(|s| s.cell(scope.namespace, &scope.cell))
        .collect::<Result<Vec<_>, _>>()?;

    async fn leader(handles: &[RaftCellHandle], excluded: Option<usize>) -> Result<usize> {
        Ok(
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    for (i, cell) in handles.iter().enumerate() {
                        if excluded == Some(i) {
                            continue;
                        }
                        if cell.status().await.is_ok_and(|status| {
                            status.command_ready
                                && status.leader.is_some_and(|id| id.get() == i as u64 + 1)
                        }) {
                            return i;
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await?,
        )
    }
    let original = leader(&handles, None).await?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    handles[original]
        .command(
            definitions[0].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    let selected = Catalog::new(
        cluster.store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(namespace)
    .await?;
    let birth = selected
        .birth(&scope.cell)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing birth"))?;
    let archive = selected.cell_archive(&birth, LtxHost::default(), ArchiveLimits::default())?;
    cluster.isolate(original, false).await?;
    let mut options = public_node::options(2)?;
    options.durability = Durability::S3;
    options.timeout = std::time::Duration::from_millis(500);
    ensure!(
        handles[original]
            .command(
                definitions[0].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                options
            )
            .await
            .is_err(),
        "isolated replica used S3 in place of Raft quorum"
    );
    let set = archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing first archive"))?;
    restored(&set, 1)?;
    let replacement = leader(&handles, Some(original)).await?;
    let mut options = public_node::options(2)?;
    options.durability = Durability::S3;
    let result = handles[replacement]
        .command(
            definitions[0].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    ensure!(result.result == 2_i64.to_be_bytes() && result.durability == Durability::S3);
    cluster.heal();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let mut caught_up = true;
            for (i, cell) in handles.iter().enumerate() {
                caught_up &= cell
                    .status()
                    .await
                    .is_ok_and(|status| status.applied.index >= result.version.position.index)
                    && cluster.nodes[i]
                        .services()
                        .application_work()
                        .pending_events
                        == 0;
            }
            if caught_up {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    cluster.close().await?;
    let set = archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing final archive"))?;
    ensure!(set.position().index >= result.version.position.index);
    restored(&set, 2)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_io_deadline_retries_without_reexecuting() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_hang.store(true, Ordering::Release);
    let mut options = RaftHostOptions::default();
    options.archive.limits.io_timeout_ms = 50;
    options.archive.retry_ms = 10;
    let native = Native::start(store.clone(), options, Durability::Quorum).await?;
    ensure!(matches!(
        native
            .write(1, Durability::S3, std::time::Duration::from_millis(500))
            .await,
        Err(CommandFailure::Ambiguous { .. })
    ));
    ensure!(
        store.archive_attempts.load(Ordering::Acquire) >= 2,
        "hung I/O did not release its bounded attempt"
    );
    store.archive_hang.store(false, Ordering::Release);
    ensure!(native.retry(1, Durability::S3).await?.durability == Durability::S3);
    ensure!(native.calls.load(Ordering::SeqCst) == 1);
    native.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_pin_pressure_defers_committed_apply_without_corruption() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_hang.store(true, Ordering::Release);
    let mut options = RaftHostOptions::default();
    options.images.max_generations = 3;
    options.enable_snapshots = false;
    options.archive.limits.io_timeout_ms = 60_000;
    let native = Native::start(store.clone(), options, Durability::Quorum).await?;
    native.retry(1, Durability::Quorum).await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.archive_attempts.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    native.retry(2, Durability::Quorum).await?;
    let cell = native.cell()?;
    let mut third = Box::pin(cell.command(
        native.definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        public_node::options(3)?,
    ));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if cell
                .status()
                .await
                .is_ok_and(|status| status.committed > status.applied.index)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    ensure!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut third)
            .await
            .is_err(),
        "image capacity was treated as corruption or acknowledged before apply"
    );
    store.archive_hang.store(false, Ordering::Release);
    store.archive_resume.notify_waiters();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), third).await??;
    ensure!(result.result == 3_i64.to_be_bytes());
    ensure!(native.calls.load(Ordering::SeqCst) == 3);
    native.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bucket_archive_store_preserves_prefix_and_conditional_writes() -> Result<()> {
    let fixture = Fixture::new(ArchiveLimits::default()).await?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async move {
            let mut fixture = fixture;
            let target = tempfile::tempdir()?;
            let mut bucket =
                cellar::bucket::Bucket::open_dev(&target.path().join("bucket.sqlite"))?;
            bucket.prefix = "tenant/archive/".into();
            let raw = bucket.cas_store.clone();
            let catalog = Catalog::new(
                bucket.raft_store(),
                CatalogOptions {
                    prefix: "archive-test".into(),
                    ..Default::default()
                },
            )?;
            let namespace = catalog
                .open_namespace(fixture.namespace.config().clone())
                .await?;
            let birth = namespace
                .resolve_birth(BirthProposal {
                    scope: fixture.birth.scope().clone(),
                    voters: fixture.birth.voters().to_vec(),
                    learners: fixture.birth.learners().to_vec(),
                })
                .await?;
            ensure!(birth.digest() == fixture.birth.digest());
            let archive =
                namespace.cell_archive(&birth, LtxHost::default(), ArchiveLimits::default())?;
            let first = fixture.commit(7, vec![]).await?;
            archive.publish(first, &fixture.verifier).await?;
            let second = fixture.commit(9, vec![]).await?;
            let set = archive.publish(second, &fixture.verifier).await?;
            ensure!(set.tail_entries() == 1);
            restored(&set, 9)?;
            let objects = raw.list(None).try_collect::<Vec<_>>().await?;
            ensure!(
                !objects.is_empty()
                    && objects
                        .iter()
                        .all(|object| object.location.as_ref().starts_with("tenant/archive/"))
            );
            ensure!(objects
                .iter()
                .any(|object| object.location.filename() == Some("head")));
            fixture.close().await
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_close_releases_workspace_before_returning() -> Result<()> {
    let native = Native::start(
        Arc::new(faults::Store::default()),
        RaftHostOptions::default(),
        Durability::S3,
    )
    .await?;
    native.retry(1, Durability::S3).await?;
    let workspace = native
        .node
        .info()
        .data_dir
        .join("raft-cells")
        .join("archive-verifier");
    native.service.close().await?;
    let reopened = GenerationStore::open(
        workspace,
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    ensure!(reopened.usage()?.generations == 0);
    drop(reopened);
    native
        .node
        .shutdown(cellar::sdk::ShutdownMode::Preserve)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unpolled_s3_waiter_cannot_receive_success_after_deadline() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let native = Native::start(
        store.clone(),
        RaftHostOptions::default(),
        Durability::Quorum,
    )
    .await?;
    let cell = native.cell()?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    options.timeout = std::time::Duration::from_millis(300);
    let id = options.id;
    let pending = cell.command(
        native.definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        options,
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.archive_attempts.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    store.archive_unavailable.store(false, Ordering::Release);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if cell
                .status()
                .await
                .is_ok_and(|status| status.archived >= status.committed)
                && native.node.services().application_work().pending_events == 0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    ensure!(matches!(pending.await, Err(CommandFailure::Ambiguous { command }) if command == id));
    ensure!(native.retry(1, Durability::S3).await?.durability == Durability::S3);
    ensure!(native.calls.load(Ordering::SeqCst) == 1);
    native.close().await
}
