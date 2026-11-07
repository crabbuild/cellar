// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Archive storage/restore qualification through real engines and physical LTX.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_archive_download.rs"]
mod download;
#[path = "support/raft_empty_archive.rs"]
mod empty_archive;
#[path = "support/raft_catalog_fault.rs"]
pub mod faults;
#[path = "support/raft_ltx_fault.rs"]
#[allow(dead_code)]
mod image_fault;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod images;
#[path = "support/raft_archive_publication.rs"]
mod publication;
#[path = "support/raft_archive_publication_origin.rs"]
mod publication_origin;
#[path = "support/raft_archive_replay_budget.rs"]
mod replay_budget;
#[path = "support/raft_archive_replay_memo.rs"]
mod replay_memo;
#[path = "support/raft_archive_snapshot_upload.rs"]
mod snapshot_upload;

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
            commitment: state.digest.into(),
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
                commitment: state.digest.into(),
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
        assert_eq!(
            events
                .iter()
                .filter(|&&event| event == "image-sync")
                .count(),
            expected_seals,
            "archive replay image-sync count with {max_files} files per batch"
        );
        assert!(
            !events.contains(&"rename"),
            "exclusive image files need no rename"
        );
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
    let mut false_digest = cellar::sdk::consensus::ImageCommitment::Sha256([0; 32]);
    fixture
        .commit_bytes(middle, vec![], |update| {
            let cellar::sdk::consensus::ImageCommitment::Sha256(digest) =
                &mut update.result.commitment
            else {
                panic!("materialized fixture returned a root commitment");
            };
            digest[0] ^= 1;
            false_digest = update.result.commitment;
        })
        .await?;
    let final_image = images::image(Some(&fixture.bytes), "UPDATE counter SET value=3")?;
    let last = fixture
        .commit_bytes(final_image, vec![], |update| {
            update.base.commitment = false_digest;
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
        events == ["image-sync", "directory-sync"],
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
            .find(|o| {
                o.location.filename() == Some("head")
                    && o.location
                        .as_ref()
                        .starts_with(fixture.namespace.data_prefix())
            })
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
async fn archive_snapshot_parts_verify_every_object_and_preserve_exact_head() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits {
        max_capture_bytes: 16 * 1024 * 1024,
        ..Default::default()
    })
    .await?;
    let next = images::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7);\
         CREATE TABLE padding(bytes BLOB);INSERT INTO padding VALUES(randomblob(9437184))",
    )?;
    let cut = fixture.commit_bytes(next, vec![], |_| {}).await?;
    fixture.store.fail_after.store(true, Ordering::Release);
    let set = fixture.publish(cut.clone()).await?;
    ensure!(!fixture.store.fail_after.load(Ordering::Acquire));
    let reference = set.reference();
    let parts = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|object| object.location.as_ref().contains("/snapshot-parts/"))
        .collect::<Vec<_>>();
    ensure!(
        parts.len() >= 3,
        "the fixture must exercise multiple incompressible parts"
    );
    ensure!(parts.iter().all(|part| part.size <= 4 * 1024 * 1024));
    ensure!(restored(&set, 7)? == fixture.bytes);
    for (mode, part) in parts.iter().take(3).enumerate() {
        let original = fixture
            .store
            .inner
            .get(&part.location)
            .await?
            .bytes()
            .await?;
        match mode {
            0 => fixture.store.inner.delete(&part.location).await?,
            1 => {
                let mut corrupt = original.to_vec();
                let at = corrupt.len() / 2;
                corrupt[at] ^= 1;
                fixture
                    .store
                    .inner
                    .put(&part.location, corrupt.into())
                    .await?;
            }
            _ => {
                fixture
                    .store
                    .inner
                    .put(&part.location, original.slice(..original.len() - 1).into())
                    .await?;
            }
        }
        ensure!(fixture.archive.recovery_set_at(&reference).await.is_err());
        ensure!(fixture.publish(cut.clone()).await.is_err());
        ensure!(
            fixture
                .archive
                .recovery_metadata()
                .await?
                .context("exact head")?
                .reference()
                == reference
        );
        fixture
            .store
            .inner
            .put(&part.location, original.into())
            .await?;
    }
    let recovered = fixture.archive.recovery_set_at(&reference).await?;
    ensure!(restored(&recovered, 7)? == fixture.bytes);
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
        .find(|o| {
            o.location.filename() == Some("head")
                && o.location
                    .as_ref()
                    .starts_with(fixture.namespace.data_prefix())
        })
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
async fn archive_data_and_learner_change_share_the_existing_snapshot() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let result = async {
        let first = fixture.commit(7, vec![]).await?;
        fixture.publish(first).await?;
        // Repair admission may follow an ordinary durable command before the
        // archive publisher captures either entry. Both transitions must remain
        // replayable from the existing base rather than force a new full image.
        fixture.commit(8, vec![]).await?;
        let changed = fixture.protocol(true).await?;
        let set = fixture.publish(changed).await?;
        ensure!(
            set.tail_entries() == 2,
            "configuration change rotated the full snapshot"
        );
        ensure!(set.position().index == LogIndex::new(4));
        ensure!(set.configuration()?.learners == vec![2]);
        ensure!(set.membership() == fixture.group.membership()?);
        let joined = set
            .membership()
            .joined(MemberId::new(2)?)?
            .context("missing learner")?;
        ensure!(joined.0.node == NodeId::new([2; 16])?);
        ensure!(joined.0.disk == DiskLineage::new([12; 16])?);
        ensure!(joined.1 == LogIndex::new(4));
        restored(&set, 8)?;
        Ok(())
    }
    .await;
    let closed = fixture.close().await;
    result.and(closed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_noop_tail_and_exact_configuration_checkpoint() -> Result<()> {
    // An explicit tail-count limit still requires an exact checkpoint after
    // the no-op tail, including its committed learner's physical binding.
    let mut fixture = Fixture::new(ArchiveLimits {
        max_tails: 1,
        ..Default::default()
    })
    .await?;
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
                .any(|object| object.location.filename() == Some("head")
                    && object
                        .location
                        .as_ref()
                        .starts_with(&format!("tenant/archive/{}", namespace.data_prefix()))));
            fixture.close().await
        })
        .await;
    domain.shutdown().await;
    result?
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
