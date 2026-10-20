//! Archive storage/restore qualification through real engines and physical LTX.
//! Direct durable-marker fixtures do not qualify host acknowledgement gates.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_catalog_fault.rs"]
mod faults;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod images;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::application::consensus::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate};
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
