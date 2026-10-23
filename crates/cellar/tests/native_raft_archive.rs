//! Archive storage/restore qualification through real engines and physical LTX.
//! Direct durable-marker fixtures qualify archive storage; the Native fixture
//! separately exercises actual Node/SDK acknowledgement and outage boundaries.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_cluster.rs"]
#[allow(dead_code)]
mod cluster;
#[path = "support/raft_archive_download.rs"]
mod download;
#[path = "support/raft_catalog_fault.rs"]
mod faults;
#[path = "support/raft_ltx_fault.rs"]
#[allow(dead_code)]
mod image_fault;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod images;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;

use anyhow::{ensure, Context as _, Result};
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
        Self::new_group(limits, Arc::new(faults::Store::default()), 7).await
    }

    async fn new_group(limits: ArchiveLimits, store: Arc<faults::Store>, id: u8) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
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
                    cell: format!("Counter:{id}"),
                    group: GroupId::new([id; 16])?,
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
        self.commit_bytes(next, dependencies, |update| {
            if corrupt {
                let bytes = &mut update.files[0].bytes;
                let last = bytes.len() - 1;
                bytes[last] ^= 0xff;
            }
        })
        .await
    }

    async fn commit_bytes(
        &mut self,
        next: Vec<u8>,
        dependencies: Vec<ArchiveDependency>,
        mutate: impl FnOnce(&mut CellUpdate),
    ) -> Result<CommittedArchiveCut> {
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
        // The mechanical storage fixture can lie about applied bytes. A real
        // ImageApplier refuses such entries; archive replay must also refuse
        // them before advancing the independently verified head.
        let mut update = CellUpdate::decode(&self.payload(&cut)?, EnvelopeLimits::default())?;
        mutate(&mut update);
        let payload = update.encode(EnvelopeLimits::default())?;
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
            let mut conf = self.group.initial_state()?.conf_state;
            let before = conf.clone();
            conf.learners.push(2);
            let membership = self.group.membership()?;
            membership.bind_change(
                &mut change,
                &[JoiningMember {
                    member: MemberId::new(2)?,
                    node: NodeId::new([2; 16])?,
                    disk: DiskLineage::new([12; 16])?,
                }],
            )?;
            entry.data = change.write_to_bytes()?.into();
            write.membership =
                Some(membership.applied(&change, &before, &conf, LogIndex::new(index))?);
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
async fn archive_replay_seals_one_generation_for_a_bounded_tail_batch() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits {
        // Exercise replay batching with a retained twenty-entry tail.
        max_tail_image_ratio: 128,
        ..Default::default()
    })
    .await?;
    let first = fixture.commit(1, vec![]).await?;
    fixture.publish(first).await?;
    for value in 2..=20 {
        fixture.commit(value, vec![]).await?;
    }
    let last = fixture.commit(21, vec![]).await?;
    let set = fixture.publish(last).await?;
    ensure!(set.tail_entries() == 20);
    for (max_files, expected_seals) in [(64, 1), (3, 7), (1, 21)] {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("replay");
        let faults = image_fault::Faults::new(directory.clone(), None);
        let mut limits = GenerationLimits::default();
        limits.apply.max_files = max_files;
        let store = GenerationStore::open(
            directory,
            Arc::new(image_fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            limits,
        )?;
        faults.events.lock().unwrap().clear();
        let pin = set.restore(&store, images::id(91))?;
        ensure!(images::read(&pin)? == fixture.bytes);
        let events = faults.events.lock().unwrap().clone();
        for operation in ["image-sync", "rename"] {
            assert_eq!(
                events.iter().filter(|&&event| event == operation).count(),
                expected_seals,
                "archive replay {operation} count with {max_files} files per batch"
            );
        }
    }
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_replay_batches_preserve_shrink_then_growth_capacity() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let initial = images::image(
        None,
        "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1);\
         CREATE TABLE payload(value BLOB); INSERT INTO payload VALUES(zeroblob(32768))",
    )?;
    let initial_length = initial.len() as u64;
    let first = fixture.commit_bytes(initial, vec![], |_| {}).await?;
    fixture.publish(first).await?;
    fixture.commit(2, vec![]).await?;
    let shrunk = images::image(Some(&fixture.bytes), "DELETE FROM payload; VACUUM")?;
    let shrunk_length = shrunk.len() as u64;
    fixture.commit_bytes(shrunk, vec![], |_| {}).await?;
    let grown = images::image(
        Some(&fixture.bytes),
        "INSERT INTO payload VALUES(zeroblob(36864))",
    )?;
    let capacity = 3 * initial_length;
    ensure!(shrunk_length + 2 * grown.len() as u64 <= capacity);
    ensure!(initial_length + 2 * grown.len() as u64 > capacity);
    let last = fixture.commit_bytes(grown, vec![], |_| {}).await?;
    let set = fixture.publish(last).await?;
    let root = tempfile::tempdir()?;
    let mut limits = GenerationLimits {
        max_generations: 2,
        max_bytes: capacity,
        ..Default::default()
    };
    limits.apply.max_files = 2;
    let store = GenerationStore::open(
        root.path().join("replay"),
        Arc::new(DirectFileSystem),
        limits,
    )?;
    let pin = set.restore(&store, images::id(91))?;
    ensure!(images::read(&pin)? == fixture.bytes);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_replay_refuses_an_overwritten_intermediate_image_digest() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(1, vec![]).await?;
    fixture.publish(first.clone()).await?;
    let middle = images::image(Some(&fixture.bytes), "UPDATE counter SET value=2")?;
    let mut false_digest = [0; 32];
    fixture
        .commit_bytes(middle, vec![], |update| {
            update.result.image_digest[0] ^= 1;
            false_digest = update.result.image_digest;
        })
        .await?;
    let final_image = images::image(Some(&fixture.bytes), "UPDATE counter SET value=3")?;
    let last = fixture
        .commit_bytes(final_image, vec![], |update| {
            update.base.image_digest = false_digest;
        })
        .await?;
    ensure!(matches!(
        fixture.publish(last).await,
        Err(ArchiveError::Images(
            cellar_ltx::generation::GenerationError::Apply(
                cellar_ltx::apply::ApplyError::ResultDigest
            )
        ))
    ));
    let set = fixture
        .archive
        .recovery_set()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing prior head"))?;
    ensure!(set.position() == first.position());
    restored(&set, 1)?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_reference_stays_exact_after_head_advance() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    ensure!(fixture.archive.recovery_metadata().await?.is_none());
    let first = fixture
        .commit(7, vec![ArchiveDependency(b"retained dependency".to_vec())])
        .await?;
    let set = fixture.publish(first).await?;
    let reference = set.reference();
    let reads = fixture.store.archive_payload_reads.load(Ordering::Acquire);
    fixture
        .store
        .refuse_archive_payload_reads
        .store(true, Ordering::Release);
    let metadata = fixture
        .archive
        .recovery_metadata()
        .await?
        .context("missing metadata")?;
    assert_eq!(metadata.reference(), reference);
    assert_eq!(metadata.scope(), fixture.birth.scope());
    assert_eq!(metadata.position(), set.position());
    assert_eq!(metadata.schema_digest(), [9; 32]);
    assert_eq!(metadata.configuration()?, set.configuration()?);
    assert_eq!(metadata.membership(), set.membership());
    assert_eq!(
        reads,
        fixture.store.archive_payload_reads.load(Ordering::Acquire),
        "metadata downloaded image/tail/dependency payloads"
    );
    fixture
        .store
        .refuse_archive_payload_reads
        .store(false, Ordering::Release);
    let second = fixture.commit(19, vec![]).await?;
    let newer = fixture.publish(second).await?;
    ensure!(newer.reference() != reference && newer.position().index > set.position().index);
    let encoded = serde_json::to_vec(&reference)?;
    let selected: ArchiveReference = serde_json::from_slice(&encoded)?;
    let old = fixture.archive.recovery_set_at(&selected).await?;
    assert_eq!(old.reference(), reference);
    assert_eq!(old.position(), set.position());
    restored(&old, 7)?;
    restored(
        &fixture.archive.recovery_set().await?.context("head lost")?,
        19,
    )?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_reference_refuses_substitution_and_missing_material() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    let set = fixture.publish(first).await?;
    let reference = set.reference();
    let mut altered = serde_json::to_value(&reference)?;
    altered["kind"] = serde_json::json!("Snapshot");
    let altered: ArchiveReference = serde_json::from_value(altered)?;
    ensure!(matches!(
        fixture.archive.recovery_set_at(&altered).await,
        Err(ArchiveError::Corrupt("archive head domain"))
    ));
    let mut altered = serde_json::to_value(&reference)?;
    altered["length"] = serde_json::json!(ArchiveLimits::default().max_manifest_bytes + 1);
    let altered: ArchiveReference = serde_json::from_value(altered)?;
    ensure!(matches!(
        fixture.archive.recovery_set_at(&altered).await,
        Err(ArchiveError::Limit)
    ));
    let mut altered = serde_json::to_value(&reference)?;
    altered["range"]["last"]["index"] = serde_json::json!(99);
    let altered: ArchiveReference = serde_json::from_value(altered)?;
    ensure!(
        fixture.archive.recovery_set_at(&altered).await.is_err(),
        "bad reference fell back to a current head"
    );
    let objects = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?;
    let snapshot = objects
        .into_iter()
        .find(|object| object.location.as_ref().contains("/snapshots/"))
        .context("missing snapshot object")?;
    fixture.store.inner.delete(&snapshot.location).await?;
    ensure!(
        fixture.archive.recovery_metadata().await?.is_some(),
        "metadata implicitly claimed payload availability"
    );
    ensure!(matches!(
        fixture.archive.recovery_set_at(&reference).await,
        Err(ArchiveError::Corrupt("missing archive object"))
    ));
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_tail_replay_seals_one_private_image() -> Result<()> {
    // Exercise replay partitioning independently of size-triggered rotation.
    let mut fixture = Fixture::new(ArchiveLimits {
        max_tail_image_ratio: 128,
        ..Default::default()
    })
    .await?;
    let first = fixture.commit(1, vec![]).await?;
    fixture.publish(first).await?;
    let mut cut = None;
    for value in 2..=33 {
        cut = Some(fixture.commit(value, vec![]).await?);
    }
    let set = fixture.publish(cut.context("missing tail")?).await?;
    ensure!(set.tail_entries() == 32);
    let directory = tempfile::tempdir()?;
    let faults = image_fault::Faults::new(directory.path().join("images"), None);
    let images = GenerationStore::open(
        faults.directory.clone(),
        Arc::new(image_fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits::default(),
    )?;
    faults.events.lock().unwrap().clear();
    let started = std::time::Instant::now();
    let restored = set.restore(&images, images::id(93))?;
    let elapsed = started.elapsed();
    ensure!(
        restored.state()
            == fixture
                .pin
                .as_ref()
                .context("missing committed image")?
                .state()
    );
    let bytes = std::fs::read(
        directory
            .path()
            .join("images")
            .join(format!("{}.sqlite", hex::encode(restored.id().bytes()))),
    )?;
    ensure!(bytes == fixture.bytes, "restore changed physical bytes");
    let events = faults.events.lock().unwrap().clone();
    let syncs = events
        .iter()
        .filter(|event| **event == "image-sync")
        .count();
    // Intermediate cuts remain private, but their exact physical checksums and
    // digests must still be checked before the final immutable image is sealed.
    ensure!(
        syncs == 1,
        "32-entry archive replay sealed {syncs} images in {elapsed:?}: {events:?}"
    );
    ensure!(
        events == ["image-sync", "rename", "directory-sync"],
        "unexpected publication work: {events:?}"
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_tail_replay_partitions_at_destination_limits() -> Result<()> {
    // Exercise replay partitioning independently of size-triggered rotation.
    let mut fixture = Fixture::new(ArchiveLimits {
        max_tail_image_ratio: 128,
        ..Default::default()
    })
    .await?;
    let first = fixture.commit(1, vec![]).await?;
    fixture.publish(first).await?;
    let mut cut = None;
    for value in 2..=66 {
        cut = Some(fixture.commit(value, vec![]).await?);
    }
    let set = fixture.publish(cut.context("missing tail")?).await?;
    ensure!(set.tail_entries() == 65);
    for max_files in [64, 7, 1] {
        let directory = tempfile::tempdir()?;
        let faults = image_fault::Faults::new(directory.path().join("images"), None);
        let mut limits = GenerationLimits::default();
        limits.apply.max_files = max_files;
        let images = GenerationStore::open(
            faults.directory.clone(),
            Arc::new(image_fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            limits,
        )?;
        faults.events.lock().unwrap().clear();
        let restored = set.restore(&images, images::id(94))?;
        ensure!(
            restored.state()
                == fixture
                    .pin
                    .as_ref()
                    .context("missing committed image")?
                    .state()
        );
        ensure!(images::read(&restored)? == fixture.bytes);
        let events = faults.events.lock().unwrap().clone();
        let syncs = events
            .iter()
            .filter(|event| **event == "image-sync")
            .count();
        ensure!(
            syncs == 66usize.div_ceil(max_files),
            "wrong partition count: {syncs}"
        );
        ensure!(
            images.usage()?.generations == 1,
            "retired batches were not reclaimed"
        );
    }
    fixture.close().await
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
async fn archive_extension_reads_retained_payloads_once() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits {
        // Keep the history needed to count retained-payload reads.
        max_tail_image_ratio: 128,
        ..Default::default()
    })
    .await?;
    let first = fixture
        .commit(7, vec![ArchiveDependency(b"retained dependency".to_vec())])
        .await?;
    fixture.publish(first).await?;
    for value in 8..=10 {
        let cut = fixture.commit(value, vec![]).await?;
        fixture.publish(cut).await?;
    }
    let cut = fixture.commit(11, vec![]).await?;
    fixture
        .store
        .archive_payload_reads
        .store(0, Ordering::Release);
    let set = fixture.publish(cut).await?;
    ensure!(set.tail_entries() == 4);
    // The new tail is read back after PUT. Complete final verification then
    // reads one snapshot, four tails and the retained dependency exactly once.
    // Reading the old set for planning alone adds five redundant object reads.
    let reads = fixture.store.archive_payload_reads.load(Ordering::Acquire);
    ensure!(reads == 7, "archive extension used {reads} payload reads");
    ensure!(restored(&set, 11)? == fixture.bytes);
    let selected = fixture
        .archive
        .recovery_set()
        .await?
        .context("archive head")?;
    ensure!(selected.reference() == set.reference());
    ensure!(restored(&selected, 11)? == fixture.bytes);
    let cut = fixture
        .commit(12, vec![ArchiveDependency(b"retained dependency".to_vec())])
        .await?;
    fixture
        .store
        .archive_payload_reads
        .store(0, Ordering::Release);
    let set = fixture.publish(cut).await?;
    let reads = fixture.store.archive_payload_reads.load(Ordering::Acquire);
    ensure!(
        reads == 8,
        "reused dependency added redundant payload reads: {reads}"
    );
    ensure!(set.tail_entries() == 5);
    ensure!(restored(&set, 12)? == fixture.bytes);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_publication_rechecks_prior_payloads_before_head_success() -> Result<()> {
    for operation in [
        "covered",
        "extend",
        "extend-with-dependency",
        "rotate",
        "rotate-by-size",
    ] {
        let mut fixture = Fixture::new(ArchiveLimits {
            max_tails: if operation == "rotate" { 1 } else { 32 },
            max_tail_image_ratio: if operation == "rotate-by-size" {
                2
            } else {
                128
            },
            ..Default::default()
        })
        .await?;
        let first = fixture
            .commit(
                7,
                vec![ArchiveDependency(b"required prior object".to_vec())],
            )
            .await?;
        fixture.publish(first).await?;
        let second = fixture.commit(8, vec![]).await?;
        let previous = fixture.publish(second.clone()).await?;
        ensure!(previous.tail_entries() == 1);
        let cut = if operation == "covered" {
            second
        } else {
            let dependencies = if operation == "extend-with-dependency" {
                vec![ArchiveDependency(b"required prior object".to_vec())]
            } else {
                vec![]
            };
            fixture.commit(9, dependencies).await?
        };
        let objects = fixture.store.list(None).try_collect::<Vec<_>>().await?;
        let head = objects
            .iter()
            .find(|o| o.location.filename() == Some("head"))
            .context("archive head")?;
        let head_bytes = fixture
            .store
            .inner
            .get(&head.location)
            .await?
            .bytes()
            .await?;
        for kind in ["/snapshots/", "/tails/", "/dependencies/"] {
            let object = objects
                .iter()
                .find(|o| o.location.as_ref().contains(kind))
                .context("prior recovery payload")?;
            let bytes = fixture
                .store
                .inner
                .get(&object.location)
                .await?
                .bytes()
                .await?;
            for missing in [true, false] {
                if missing {
                    fixture.store.inner.delete(&object.location).await?;
                } else {
                    let mut corrupt = bytes.to_vec();
                    corrupt[0] ^= 0xff;
                    fixture
                        .store
                        .inner
                        .put(&object.location, corrupt.into())
                        .await?;
                }
                ensure!(
                    matches!(
                        fixture.publish(cut.clone()).await,
                        Err(ArchiveError::Corrupt(_))
                    ),
                    "{operation} accepted invalid prior {kind} (missing={missing})"
                );
                ensure!(
                    fixture
                        .store
                        .inner
                        .get(&head.location)
                        .await?
                        .bytes()
                        .await?
                        == head_bytes,
                    "{operation} advanced the head with invalid prior {kind}"
                );
                fixture
                    .store
                    .inner
                    .put(&object.location, bytes.clone().into())
                    .await?;
            }
        }
        let set = fixture.publish(cut).await?;
        ensure!(
            set.tail_entries()
                == match operation {
                    "covered" => 1,
                    "extend" | "extend-with-dependency" => 2,
                    _ => 0,
                }
        );
        ensure!(restored(&set, if operation == "covered" { 8 } else { 9 })? == fixture.bytes);
        fixture.close().await?;
    }
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
async fn archive_size_rotation_reduces_repeated_payload_reads() -> Result<()> {
    let mut reads = Vec::new();
    for ratio in [2, 128] {
        let mut fixture = Fixture::new(ArchiveLimits {
            max_tail_image_ratio: ratio,
            ..Default::default()
        })
        .await?;
        for value in 1..=20 {
            let cut = fixture.commit(value, vec![]).await?;
            let set = fixture.publish(cut).await?;
            ensure!(restored(&set, value)? == fixture.bytes);
        }
        let count = fixture.store.archive_payload_reads.load(Ordering::Acquire);
        eprintln!("archive tail/image ratio={ratio}, payload reads={count}");
        reads.push(count);
        fixture.close().await?;
    }
    ensure!(
        reads[0] < reads[1],
        "size-based rotation did not reduce repeated payload reads: {reads:?}"
    );
    Ok(())
}

#[test]
fn archive_tail_image_ratio_has_explicit_bounds() {
    for ratio in [0, 1, 2, 128, 129, u32::MAX] {
        let result = ArchiveLimits {
            max_tail_image_ratio: ratio,
            ..Default::default()
        }
        .validate();
        if (1..=128).contains(&ratio) {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(ArchiveError::Limit)));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_small_updates_rotate_before_tail_count_limit() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let mut rotations = 0;
    for value in 7..=22 {
        let cut = fixture.commit(value, vec![]).await?;
        let set = fixture.publish(cut).await?;
        ensure!(restored(&set, value)? == fixture.bytes);
        if value > 7 && set.tail_entries() == 0 {
            rotations += 1;
        }
    }
    ensure!(
        rotations > 0,
        "small images accumulated fifteen tails without a size-based snapshot"
    );
    let expected = fixture.bytes.clone();
    let archive = fixture.archive.clone();
    fixture.close().await?;
    let set = archive.recovery_set().await?.context("archive head")?;
    ensure!(restored(&set, 22)? == expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_large_image_keeps_short_efficient_tails() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let next = images::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7);\
         CREATE TABLE padding(bytes BLOB);INSERT INTO padding VALUES(zeroblob(65536))",
    )?;
    let cut = fixture.commit_bytes(next, vec![], |_| {}).await?;
    fixture.publish(cut).await?;
    for value in 8..=18 {
        let cut = fixture.commit(value, vec![]).await?;
        let set = fixture.publish(cut).await?;
        ensure!(set.tail_entries() == (value - 7) as usize);
        ensure!(restored(&set, value)? == fixture.bytes);
    }
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
async fn archive_refuses_member_binding_birth_mismatch() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let first = fixture.commit(7, vec![]).await?;
    fixture.publish(first).await?;
    let alternate = fixture
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                fixture._root.path().join("alternate"),
                NodeId::new([5; 16])?,
                DiskLineage::new([6; 16])?,
                ProcessGeneration::new([99; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    // Deliberately use the trusted mechanical API to supply an inconsistent
    // birth ID set with the same history digest. Ordinary catalog bootstrap
    // cannot create this state; the archive must still validate its producer.
    let group = alternate
        .create_group(GroupBootstrap {
            scope: fixture.birth.scope().clone(),
            member: MemberId::new(1)?,
            birth_digest: fixture.birth.digest(),
            voters: vec![MemberId::new(1)?],
            learners: vec![MemberId::new(2)?],
        })
        .await?;
    let mut write = GroupWrite::new(&group);
    write.entries = fixture
        .group
        .entries(2, 3, None, raft::GetEntriesContext::empty(false))?;
    write.applied = Some(fixture.group.applied()?);
    let mut hard = group.initial_state()?.hard_state;
    hard.commit = 2;
    write.hard_state = Some(hard);
    alternate.persist(OperationId::new(1), vec![write]).await?;
    let before = group.initial_state()?.conf_state;
    let mut after = before.clone();
    after.learners.clear();
    let state = group.membership()?;
    let mut change = ConfChangeV2::default();
    change.mut_changes().push(ConfChangeSingle {
        node_id: 2,
        change_type: ConfChangeType::RemoveNode,
        ..Default::default()
    });
    state.bind_change(&mut change, &[])?;
    let mut write = GroupWrite::new(&group);
    write.entries.push(Entry {
        index: 3,
        term: 1,
        entry_type: EntryType::EntryConfChangeV2,
        data: change.write_to_bytes()?.into(),
        ..Default::default()
    });
    write.membership = Some(state.applied(&change, &before, &after, LogIndex::new(3))?);
    write.conf_state = Some(after.clone());
    let mut applied = group.applied()?;
    applied.position.index = LogIndex::new(3);
    write.applied = Some(applied);
    let mut hard = group.initial_state()?.hard_state;
    hard.commit = 3;
    write.hard_state = Some(hard);
    alternate.persist(OperationId::new(2), vec![write]).await?;
    assert_eq!(
        after,
        fixture.group.initial_state()?.conf_state,
        "numeric configuration alone cannot detect the mismatched birth"
    );
    let image = ImageApplier::recover(
        fixture.group.clone(),
        fixture.images.clone(),
        [9; 32],
        EnvelopeLimits::default(),
    )?
    .current()
    .context("missing image")?;
    let cut = CommittedArchiveCut::capture(&group, image, vec![], ArchiveLimits::default())?;
    assert!(
        matches!(fixture.publish(cut).await, Err(ArchiveError::Invalid(_))),
        "archive advertised bindings from another birth ID set"
    );
    let set = fixture
        .archive
        .recovery_set()
        .await?
        .context("original archive disappeared")?;
    assert_eq!(set.position().index, LogIndex::new(2));
    restored(&set, 7)?;
    alternate.close().await?;
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
    assert_eq!(set.membership(), fixture.group.membership()?);
    let joined = set.membership().joined(MemberId::new(2)?)?.unwrap();
    assert_eq!(joined.0.node, NodeId::new([2; 16])?);
    assert_eq!(joined.0.disk, DiskLineage::new([12; 16])?);
    assert_eq!(joined.1, LogIndex::new(4));
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
    entered: tokio::sync::mpsc::UnboundedReceiver<()>,
    gate: Arc<public_node::Gate>,
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
        Self::start_many(store, options, minimum, 1).await
    }

    async fn start_many(
        store: Arc<faults::Store>,
        options: RaftHostOptions,
        minimum: Durability,
        count: u8,
    ) -> Result<Self> {
        Self::start_gated(store, options, minimum, count, false).await
    }

    async fn start_gated(
        store: Arc<faults::Store>,
        options: RaftHostOptions,
        minimum: Durability,
        count: u8,
        hold: bool,
    ) -> Result<Self> {
        Self::start_with_filesystem(
            store,
            options,
            minimum,
            count,
            hold,
            Arc::new(DirectFileSystem),
        )
        .await
    }

    async fn start_with_filesystem(
        store: Arc<faults::Store>,
        options: RaftHostOptions,
        minimum: Durability,
        count: u8,
        hold: bool,
        filesystem: Arc<dyn cellar_ltx::FileSystem>,
    ) -> Result<Self> {
        ensure!((1..=32).contains(&count));
        let root = tempfile::tempdir()?;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (gate, entered) = public_node::Gate::new();
        if !hold {
            gate.release();
        }
        let definition = public_node::counter(calls.clone(), gate.clone())?;
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
        let node = public_node::node_declared_with_filesystem(
            root.path(),
            definition.application.clone(),
            "raft-host",
            vec![],
            filesystem,
        )
        .await?;
        let mut registrations = vec![registration.clone()];
        for id in 1..count {
            let mut extra = registration.clone();
            extra.birth.scope.cell = format!("Counter:extra-{id}");
            extra.birth.scope.group = GroupId::new([5 + id; 16])?;
            registrations.push(extra);
        }
        let service = public_node::cells_many(
            &node,
            definition.application.clone(),
            store.clone(),
            registrations.clone(),
            false,
            options.clone(),
            TransportOptions::default(),
        )
        .await?;
        let native = Self {
            entered,
            gate,
            root,
            node,
            service,
            definition,
            calls,
            store,
            registration,
            options,
        };
        for registration in registrations {
            public_node::ready(&native.service.cell(
                registration.namespace.namespace,
                &registration.birth.scope.cell,
            )?)
            .await?;
        }
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
async fn archive_refusal_preserves_private_workspace_and_duplicate_receipts() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let trace = image_fault::Faults::new(std::path::PathBuf::from("unused"), None);
    let mut options = RaftHostOptions::default();
    options.archive.max_unarchived_entries = 2;
    let native = Native::start_with_filesystem(
        store.clone(),
        options,
        Durability::Quorum,
        1,
        false,
        Arc::new(image_fault::FaultFileSystem {
            faults: trace.clone(),
        }),
    )
    .await?;
    let result = async {
        native.retry(1, Durability::Quorum).await?;
        native.retry(2, Durability::Quorum).await?;
        let before = trace.private_image_copies.load(Ordering::Acquire);
        for _ in 0..3 {
            ensure!(matches!(
                native
                    .write(3, Durability::Quorum, std::time::Duration::from_secs(2))
                    .await,
                Err(CommandFailure::Overloaded)
            ));
        }
        let metrics = native.service.performance();
        let count = |name| {
            metrics
                .counters
                .iter()
                .find(|counter| counter.name == name)
                .unwrap()
                .value
        };
        ensure!(count("archive_admission_retained_entries_refused") >= 3);
        ensure!(count("archive_admission_retained_bytes_refused") == 0);
        let duplicate = native.retry(1, Durability::Quorum).await?;
        ensure!(duplicate.result == 1_i64.to_be_bytes());
        ensure!(native.calls.load(Ordering::SeqCst) == 2);
        ensure!(
            trace.private_image_copies.load(Ordering::Acquire) == before,
            "known admission refusal rebuilt the private image"
        );
        store.archive_unavailable.store(false, Ordering::Release);
        let third = native.retry(3, Durability::S3).await?;
        ensure!(third.result == 3_i64.to_be_bytes());
        ensure!(native.calls.load(Ordering::SeqCst) == 3);
        ensure!(trace.private_image_copies.load(Ordering::Acquire) == before);
        ensure!(
            trace.private_ltx_creates.load(Ordering::Acquire) == 0,
            "reused private workspace wrote disposable capture files"
        );

        // A real SQL mutation followed by a callback error remains dirty.
        // Overflow changes SQLite's value to REAL, then the i64 result read
        // fails. The transaction must roll back and the next command rebuild.
        let mut overflow_options = public_node::options(1)?;
        overflow_options.id = CommandId::new(ClientId::new([8; 16])?, 1)?;
        let overflow = native
            .cell()?
            .command(
                native.definition.increment.clone(),
                i64::MAX.to_be_bytes().to_vec(),
                overflow_options,
            )
            .await;
        ensure!(matches!(overflow, Err(CommandFailure::Unavailable)));
        ensure!(native.calls.load(Ordering::SeqCst) == 4);
        let fourth = native.retry(4, Durability::S3).await?;
        ensure!(fourth.result == 4_i64.to_be_bytes());
        ensure!(native.calls.load(Ordering::SeqCst) == 5);
        ensure!(
            trace.private_image_copies.load(Ordering::Acquire) > before,
            "callback mutation failure reused uncertain private state"
        );
        Ok(())
    }
    .await;
    store.archive_unavailable.store(false, Ordering::Release);
    result.and(native.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_command_reservation_waits_for_prepared_size() -> Result<()> {
    check_unknown_reservation(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_archive_reservation_waiter_never_executes() -> Result<()> {
    check_unknown_reservation(true).await
}

async fn check_unknown_reservation(cancel_waiter: bool) -> Result<()> {
    use std::time::Duration;
    let mut options = RaftHostOptions::default();
    options.archive.max_unarchived_bytes = options.execution.max_image_bytes
        + options.execution.envelope.max_payload_bytes as u64
        + options.execution.envelope.max_header_bytes as u64
        + 1024
        + 256 * 1024;
    let mut native = Native::start_gated(
        Arc::new(faults::Store::default()),
        options,
        Durability::Quorum,
        2,
        true,
    )
    .await?;
    let mut tasks = Vec::new();
    let result = async {
        let cells = [
            native.cell()?,
            native
                .service
                .cell(native.registration.namespace.namespace, "Counter:extra-1")?,
        ];
        for cell in &cells {
            cell.command(
                native.definition.increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::archived_options(1)?,
            )
            .await?;
            public_node::ready(cell).await?;
        }
        tasks.push(tokio::spawn(cells[0].command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )));
        tokio::time::timeout(Duration::from_secs(5), native.entered.recv())
            .await?
            .context("first callback did not enter")?;
        tasks.push(tokio::spawn(cells[1].command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if native.service.performance().counters.iter().any(|counter| {
                    counter.name == "archive_admission_reserved_bytes_deferred" && counter.value > 0
                }) {
                    break;
                }
                ensure!(
                    !tasks[1].is_finished(),
                    "reservation waiter exited before deferral"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        ensure!(
            native.calls.load(Ordering::SeqCst) == 3,
            "second callback ran without a worst-case reservation"
        );
        ensure!(
            !tasks[1].is_finished(),
            "transient reservation pressure refused the queued command"
        );
        if cancel_waiter {
            let task = tasks.pop().unwrap();
            task.abort();
            ensure!(task.await.is_err_and(|error| error.is_cancelled()));
            // Cancellation must release this cell while the other producer
            // still owns its unknown-size reservation.
            public_node::ready(&cells[1]).await?;
            ensure!(native.calls.load(Ordering::SeqCst) == 3);
        }
        native.gate.release();
        for task in tasks.drain(..) {
            let receipt = tokio::time::timeout(Duration::from_secs(5), task).await???;
            ensure!(receipt.result == 2_i64.to_be_bytes());
        }
        if cancel_waiter {
            let value = cells[1]
                .query(
                    native.definition.get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(value.result == 1_i64.to_be_bytes());
            let receipt = cells[1]
                .command(
                    native.definition.increment.clone(),
                    1_i64.to_be_bytes().to_vec(),
                    public_node::options(2)?,
                )
                .await?;
            ensure!(receipt.result == 2_i64.to_be_bytes());
        }
        ensure!(native.calls.load(Ordering::SeqCst) == 4);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    native.gate.release();
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    let cleanup = native.close().await;
    result.and(cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_small_commands_release_worst_case_archive_reservations() -> Result<()> {
    // Retain the real 256-MiB backlog cap and 64-MiB image/16-MiB envelope
    // limits. Eight small prepared commands fit; eight worst-case ones do not.
    check_prepared_archive_reservations(RaftHostOptions::default(), 8, 8).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_commands_keep_actual_archive_byte_charge() -> Result<()> {
    let mut options = RaftHostOptions::default();
    // Enough room for one unknown callback and the initial protocol entries,
    // but not another unknown callback plus the first committed counter image.
    options.archive.max_unarchived_bytes = options.execution.max_image_bytes
        + options.execution.envelope.max_payload_bytes as u64
        + options.execution.envelope.max_header_bytes as u64
        + 1024
        + 16 * 1024;
    check_prepared_archive_reservations(options, 2, 1).await
}

async fn check_prepared_archive_reservations(
    host: RaftHostOptions,
    count: u8,
    admitted: usize,
) -> Result<()> {
    use std::time::Duration;
    let store = Arc::new(faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let native = Native::start_many(store.clone(), host, Durability::Quorum, count)
        .await
        .context("start native groups for archive admission")?;
    let mut tasks = Vec::new();
    let result = async {
        for id in 0..usize::from(count) {
            let name = if id == 0 {
                "Counter:one".into()
            } else {
                format!("Counter:extra-{id}")
            };
            let cell = native
                .service
                .cell(native.registration.namespace.namespace, &name)?;
            let mut options = public_node::options(1)?;
            options.durability = Durability::S3;
            if id >= admitted {
                options.timeout = Duration::from_secs(2);
                let refused = cell
                    .command(
                        native.definition.increment.clone(),
                        1_i64.to_be_bytes().to_vec(),
                        options,
                    )
                    .await;
                ensure!(
                    matches!(refused, Err(CommandFailure::Overloaded)),
                    "retained archive bytes were not charged: {refused:?}"
                );
                continue;
            }
            options.timeout = Duration::from_secs(30);
            tasks.push(tokio::spawn(cell.command(
                native.definition.increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                options,
            )));
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    ensure!(
                        !tasks.last().unwrap().is_finished(),
                        "small command {id} refused before archive recovery"
                    );
                    let applied = match cell.status().await {
                        Ok(status) => status.applied.index.get() > 2,
                        Err(CommandFailure::Overloaded) => false,
                        Err(error) => {
                            return Err(anyhow::Error::new(error).context("observe applied command"))
                        }
                    };
                    if native.calls.load(Ordering::SeqCst) == id + 1 && applied {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .context("wait for small command to apply")??;
        }
        ensure!(native.calls.load(Ordering::SeqCst) == admitted);
        if admitted < usize::from(count) {
            let metrics = native.service.performance();
            ensure!(metrics.counters.iter().any(|counter| counter.name
                == "archive_admission_retained_bytes_refused"
                && counter.value > 0));
        }
        store.archive_unavailable.store(false, Ordering::Release);
        for task in tasks.drain(..) {
            let receipt = tokio::time::timeout(Duration::from_secs(10), task).await???;
            ensure!(receipt.result == 1_i64.to_be_bytes());
        }
        Ok(())
    }
    .await;
    store.archive_unavailable.store(false, Ordering::Release);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    let cleanup = native
        .close()
        .await
        .context("close archive admission groups");
    result.and(cleanup)
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
    let retry_timeout = public_node::options(1)?.timeout;
    // Archive capture can own SQL when the Quorum receipt reaches its caller.
    // Retry only the fixture's explicit Overloaded admission refusal, while one
    // original deadline bounds the entire stronger retry, including admission.
    let mut stronger = Box::pin(async {
        tokio::time::timeout(retry_timeout, native.retry(1, Durability::S3)).await?
    });
    let observed = tokio::time::timeout(std::time::Duration::from_millis(200), &mut stronger).await;
    ensure!(
        observed.is_err(),
        "stronger retry completed during archive outage: {observed:?}"
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
async fn independent_cell_archives_while_another_publication_is_stalled() -> Result<()> {
    use std::time::Duration;
    let store = Arc::new(faults::Store::default());
    let native = Native::start_many(
        store.clone(),
        RaftHostOptions::default(),
        Durability::Quorum,
        2,
    )
    .await?;
    store.payload_hang.store(true, Ordering::Release);
    store.hold_next_payload.store(true, Ordering::Release);
    let first = native.cell()?;
    let blocked = first.command(
        native.definition.increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        public_node::archived_options(1)?,
    );
    let result = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !store.payload_waiting.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("first archive did not enter its payload gate")?;
        let second = native
            .service
            .cell(native.registration.namespace.namespace, "Counter:extra-1")?;
        let receipt = tokio::time::timeout(
            Duration::from_secs(2),
            second.command(
                native.definition.increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::archived_options(1)?,
            ),
        )
        .await
        .context("independent archive waited behind stalled publication")??;
        ensure!(receipt.result == 1_i64.to_be_bytes());
        let first_status = archive_status(&first).await?;
        ensure!(first_status.archived < first_status.committed);
        let second_status = archive_status(&second).await?;
        ensure!(second_status.archived >= second_status.committed);
        ensure!(store.payload_waiting.load(Ordering::Acquire));
        ensure!(native.calls.load(Ordering::Acquire) == 2);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // Release and join both real SDK owners even when the progress assertion fails.
    store.release_payload();
    let first_result = tokio::time::timeout(Duration::from_secs(5), blocked).await;
    let cleanup = native.close().await;
    result?;
    ensure!(first_result??.result == 1_i64.to_be_bytes());
    cleanup
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_producer_excludes_its_cell_until_captured_cut_finishes() -> Result<()> {
    use std::time::Duration;
    let store = Arc::new(faults::Store::default());
    let native = Native::start(
        store.clone(),
        RaftHostOptions::default(),
        Durability::Quorum,
    )
    .await?;
    store.hold_archive_payloads.store(true, Ordering::Release);
    let result = async {
        let cell = native.cell()?;
        let first = native
            .write(1, Durability::Quorum, Duration::from_secs(5))
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.active_payload_reads.load(Ordering::Acquire) != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        let mut command = public_node::options(2)?;
        let second = cell
            .command(
                native.definition.increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                command.clone(),
            )
            .await?;
        ensure!(first.result == 1_i64.to_be_bytes() && second.result == 2_i64.to_be_bytes());
        let value = cell
            .query(
                native.definition.get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(value.result == 2_i64.to_be_bytes());
        ensure!(native.service.resource_usage().archive_capture.items == 1);
        ensure!(store.active_payload_reads.load(Ordering::Acquire) == 1);
        ensure!(store.peak_payload_reads.load(Ordering::Acquire) == 1);
        command.durability = Durability::S3;
        let archived = cell.command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            command,
        );
        store.hold_archive_payloads.store(false, Ordering::Release);
        store.payload_window_resume.notify_waiters();
        ensure!(archived.await?.result == 2_i64.to_be_bytes());
        ensure!(native.calls.load(Ordering::Acquire) == 2);
        let set = native
            .archive()
            .await?
            .recovery_set()
            .await?
            .context("missing archive")?;
        restored(&set, 2)?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    store.hold_archive_payloads.store(false, Ordering::Release);
    store.payload_window_resume.notify_waiters();
    let cleanup = native.close().await;
    result.and(cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_producer_limits_hold_until_joined_shutdown() -> Result<()> {
    for max_producers in [1, 2, 4] {
        check_archive_producer_shutdown(max_producers).await?;
    }
    Ok(())
}

async fn archive_status(handle: &RaftCellHandle) -> Result<CellStatus> {
    use std::time::Duration;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match handle.status().await {
                Ok(status) => return Ok(status),
                // A read-only observation can collide with the host's existing
                // status poll for a command waiting on S3. Keep one deadline.
                Err(CommandFailure::Overloaded) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await
    .context("observe archive progress while a command waits for S3")?
}

async fn check_archive_producer_shutdown(max_producers: usize) -> Result<()> {
    use std::time::Duration;
    let store = Arc::new(faults::Store::default());
    let mut options = RaftHostOptions::default();
    options.archive.max_producers = max_producers;
    // This case counts held physical reads one-for-one with producer slots.
    // Bundle coalescing has separate coverage and can serve two cuts with one
    // object read, so it must not change this fixture's occupancy observation.
    options.archive.max_batch_groups = 1;
    let capture_bytes = options.archive.limits.max_capture_bytes;
    let count = max_producers + 1;
    let native =
        Native::start_many(store.clone(), options, Durability::Quorum, count as u8).await?;
    store.hold_archive_payloads.store(true, Ordering::Release);
    let mut handles = vec![native.cell()?];
    for id in 1..count {
        handles.push(native.service.cell(
            native.registration.namespace.namespace,
            &format!("Counter:extra-{id}"),
        )?);
    }
    // Startup has already committed its leader barrier. Observing index > 1
    // can therefore finish before these commands commit, making Unavailable
    // on shutdown legitimate. Require progress beyond each pre-command cut.
    let mut before_commands = Vec::new();
    for handle in &handles {
        before_commands.push(archive_status(handle).await?.committed);
    }
    let mut pending = Vec::new();
    for handle in &handles {
        let mut command = public_node::archived_options(1)?;
        command.timeout = Duration::from_secs(15);
        pending.push(handle.command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            command,
        ));
    }
    let observed = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut committed = 0;
                for (handle, before) in handles.iter().zip(&before_commands) {
                    let status = archive_status(handle).await?;
                    if status.committed > *before && status.applied.index >= status.committed {
                        committed += 1;
                    }
                    ensure!(status.archived < status.committed);
                }
                if committed == count
                    && store.active_payload_reads.load(Ordering::Acquire) == max_producers
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!(
            "archive producer window did not fill: limit={max_producers}, calls={}, reads={}, slots={}",
            native.calls.load(Ordering::Acquire),
            store.active_payload_reads.load(Ordering::Acquire),
            native.service.resource_usage().archive_capture.items,
        ))??;
        ensure!(store.peak_payload_reads.load(Ordering::Acquire) == max_producers);
        let capture = native.service.resource_usage().archive_capture;
        ensure!(capture.items == max_producers && capture.max_items == max_producers);
        ensure!(capture.bytes == capture_bytes * max_producers);
        ensure!(capture.max_bytes == capture.bytes);
        ensure!(native.calls.load(Ordering::Acquire) == count);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // Closing must cancel every held read without relying on this test's gates.
    let closed = tokio::time::timeout(Duration::from_secs(5), native.service.close()).await;
    let reads_after_close = store.active_payload_reads.load(Ordering::Acquire);
    let capture_after_close = native.service.resource_usage().archive_capture;
    store.hold_archive_payloads.store(false, Ordering::Release);
    store.payload_window_resume.notify_waiters();
    let mut outcomes = Vec::new();
    for command in pending {
        outcomes.push(tokio::time::timeout(Duration::from_secs(5), command).await);
    }
    let cleanup = native.close().await;
    observed?;
    closed??;
    ensure!(reads_after_close == 0);
    ensure!(capture_after_close.bytes == 0 && capture_after_close.items == 0);
    for outcome in outcomes {
        let outcome = outcome?;
        let kind = outcome.as_ref().err().map(|error| {
            format!("{error:?}")
                .split(" {")
                .next()
                .unwrap_or("unknown")
                .to_owned()
        });
        ensure!(
            matches!(outcome, Err(CommandFailure::Ambiguous { .. })),
            "committed S3 waiter at producer limit {max_producers} returned {kind:?}"
        );
    }
    cleanup
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
async fn exact_leader_preview_publishes_under_archive_pin_pressure() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    store.archive_hang.store(true, Ordering::Release);
    let mut options = RaftHostOptions::default();
    options.images.max_generations = 3;
    options.enable_snapshots = false;
    options.archive.limits.io_timeout_ms = 60_000;
    let native = Native::start(store.clone(), options, Durability::Quorum)
        .await
        .context("start archive pressure fixture")?;
    native
        .retry(1, Durability::Quorum)
        .await
        .context("commit first pressure image")?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.archive_attempts.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .context("archive I/O did not start")?;
    native
        .retry(2, Durability::Quorum)
        .await
        .context("commit second pressure image")?;
    let cell = native.cell()?;
    let request = public_node::options(3)?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        cell.command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            request.clone(),
        ),
    )
    .await
    .context("sealed preview did not publish while the archive pin was held")??;
    ensure!(result.result == 3_i64.to_be_bytes());
    ensure!(cell.status().await?.applied.index >= result.version.position.index);
    ensure!(store.archive_hang.load(Ordering::Acquire));
    ensure!(native.calls.load(Ordering::SeqCst) == 3);
    // Three retained generations cover the old archive pin, current image and
    // next preview. Applying the exact preview needs no fourth generation.
    store.archive_hang.store(false, Ordering::Release);
    store.archive_resume.notify_waiters();
    let mut archived = request;
    archived.durability = Durability::S3;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        cell.command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            archived,
        ),
    )
    .await
    .context("archive did not catch up after releasing its old pin")??;
    ensure!(result.result == 3_i64.to_be_bytes() && result.durability == Durability::S3);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_bundle_partial_head_failure_keeps_proofs_independent() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    let mut a = Fixture::new_group(ArchiveLimits::default(), store.clone(), 7).await?;
    let mut b = Fixture::new_group(ArchiveLimits::default(), store.clone(), 8).await?;
    let cuts = [a.commit(17, vec![]).await?, b.commit(29, vec![]).await?];
    let archives = [a.archive.clone(), b.archive.clone()];
    let scopes = [a.birth.scope().clone(), b.birth.scope().clone()];
    store.fail_head_before.store(true, Ordering::Release);
    let results = CellArchive::publish_batch(
        archives.iter().cloned().zip(cuts.iter().cloned()).collect(),
        &a.verifier,
    )
    .await?;
    ensure!(results.len() == 2 && results.iter().filter(|result| result.is_ok()).count() == 1);
    ensure!(!store.fail_head_before.load(Ordering::Acquire));
    for (index, result) in results.into_iter().enumerate() {
        let selected = archives[index].recovery_set().await?;
        match result {
            Ok(set) => {
                ensure!(set.scope() == &scopes[index] && set.position() == cuts[index].position());
                ensure!(selected.is_some(), "successful member has no own head");
                restored(&set, [17, 29][index])?;
            }
            Err(_) => {
                ensure!(selected.is_none(), "failed member borrowed the other head");
                restored(
                    &archives[index]
                        .publish(cuts[index].clone(), &a.verifier)
                        .await?,
                    [17, 29][index],
                )?;
            }
        }
    }
    let objects = store.inner.list(None).try_collect::<Vec<_>>().await?;
    ensure!(
        objects
            .iter()
            .filter(|object| object.location.as_ref().contains("/archives/bundles/"))
            .count()
            == 1
    );
    // Reject invalid batches before any member can publish.
    let before = objects.len();
    ensure!(CellArchive::publish_batch(
        vec![
            (archives[0].clone(), cuts[0].clone()),
            (archives[0].clone(), cuts[0].clone())
        ],
        &a.verifier
    )
    .await
    .is_err());
    let mut foreign = Fixture::new(ArchiveLimits::default()).await?;
    let foreign_cut = foreign.commit(31, vec![]).await?;
    ensure!(CellArchive::publish_batch(
        vec![
            (archives[0].clone(), cuts[0].clone()),
            (foreign.archive.clone(), foreign_cut)
        ],
        &a.verifier
    )
    .await
    .is_err());
    ensure!(store.inner.list(None).try_collect::<Vec<_>>().await?.len() == before);
    ensure!(foreign.archive.recovery_set().await?.is_none());
    foreign.close().await?;
    a.close().await?;
    b.close().await
}
