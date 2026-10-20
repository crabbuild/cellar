//! Physical snapshot components over real SQLite/LTX and Raft Engine. These
//! fixtures do not qualify automatic member repair or snapshot RPC scheduling.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_ltx_fault.rs"]
#[allow(dead_code)]
mod image_fault;
#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;
#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod log_fault;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::application::consensus::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_ltx::generation::{GenerationPin, GenerationStore};
use cellar_ltx::{DirectFileSystem, FileSystem};
use protobuf::Message as _;
use raft::{prelude::Entry, Storage};
use std::{path::Path, sync::Arc};

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:7".into(),
        group: GroupId::new([7; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

fn marker(pin: &GenerationPin) -> CommittedImage {
    let state = pin.state();
    CommittedImage {
        name: format!("{}.sqlite", hex::encode(pin.id().bytes())),
        digest: state.digest,
        length: state.length,
        ltx_epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        page_size: state.page_size,
    }
}

struct Fixture {
    domain: ExecutionDomain,
    log: ReplicaLog,
    group: GroupLog,
    images: GenerationStore,
    bytes: Vec<u8>,
    txid: u8,
}

impl Fixture {
    async fn empty_configuration_cut(&self) -> Result<()> {
        let mut conf = self.group.initial_state()?.conf_state;
        conf.learners = vec![4];
        let change = raft::prelude::ConfChangeV2 {
            changes: vec![raft::prelude::ConfChangeSingle {
                node_id: 4,
                change_type: raft::prelude::ConfChangeType::AddLearnerNode,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        let mut write = GroupWrite::new(&self.group);
        write.entries = vec![
            Entry {
                index: 2,
                term: 2,
                ..Default::default()
            },
            Entry {
                index: 3,
                term: 2,
                entry_type: raft::prelude::EntryType::EntryConfChangeV2,
                data: change.write_to_bytes()?.into(),
                ..Default::default()
            },
        ];
        let mut hard = self.group.initial_state()?.hard_state;
        hard.term = 2;
        hard.vote = 1;
        hard.commit = 3;
        write.hard_state = Some(hard);
        write.conf_state = Some(conf);
        write.applied = Some(DurableApply {
            position: LogPosition {
                index: LogIndex::new(3),
                term: Term::new(2),
            },
            image: None,
        });
        self.log.persist(OperationId::new(8), vec![write]).await?;
        Ok(())
    }

    async fn open(root: &Path, member: u8, mode: LogOpenMode) -> Result<Self> {
        Self::with_filesystems(
            root,
            member,
            mode,
            Arc::new(DirectFileSystem),
            Arc::new(DirectFileSystem),
        )
        .await
    }

    async fn with_filesystems(
        root: &Path,
        member: u8,
        mode: LogOpenMode,
        image_fs: Arc<dyn FileSystem>,
        log_fs: Arc<dyn FileSystem>,
    ) -> Result<Self> {
        Self::open_born(root, member, mode, image_fs, log_fs, [9; 32]).await
    }

    async fn open_born(
        root: &Path,
        member: u8,
        mode: LogOpenMode,
        image_fs: Arc<dyn FileSystem>,
        log_fs: Arc<dyn FileSystem>,
        birth_digest: [u8; 32],
    ) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.join("raft"),
                    NodeId::new([member; 16])?,
                    DiskLineage::new([member + 40; 16])?,
                    ProcessGeneration::new([member + 80; 16])?,
                    mode,
                ),
                log_fs,
            ))
            .await??;
        let group = match mode {
            LogOpenMode::Create => {
                log.create_group(GroupBootstrap {
                    scope: scope(),
                    member: MemberId::new(u64::from(member))?,
                    birth_digest,
                    voters: (1..=3).map(|id| MemberId::new(id).unwrap()).collect(),
                    learners: vec![],
                })
                .await?
            }
            LogOpenMode::Resume => log.group(&scope())?,
        };
        let limits = cellar::sdk::consensus::RaftHostOptions::default().images;
        let images = GenerationStore::open(root.join("images"), image_fs, limits)?;
        Ok(Self {
            domain,
            log,
            group,
            images,
            bytes: vec![],
            txid: 0,
        })
    }

    fn applier(&self) -> Result<ImageApplier> {
        Ok(ImageApplier::recover(
            self.group.clone(),
            self.images.clone(),
            [9; 32],
            EnvelopeLimits::default(),
        )?)
    }

    async fn commit(&mut self, value: i64) -> Result<()> {
        let next = images::image(
            (!self.bytes.is_empty()).then_some(self.bytes.as_slice()),
            &if self.txid == 0 {
                format!("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES({value})")
            } else {
                format!("UPDATE counter SET value={value}")
            },
        )?;
        let current = self.applier()?.current();
        let checksums = current
            .as_ref()
            .is_none_or(|image| image.pin().state().checksum != 0);
        let cut = images::Cut::new(&self.bytes, &next, u64::from(self.txid), checksums, true);
        let pin = self.images.prepare(
            images::id(self.txid + 1),
            current.as_ref().map(|image| image.pin()).as_ref(),
            &cut.update(),
        )?;
        let position = LogPosition {
            index: LogIndex::new(self.group.applied()?.position.index.get() + 1),
            term: Term::new(2),
        };
        let payload = self.payload(&cut)?;
        let mut write = GroupWrite::new(&self.group);
        write.entries.push(Entry {
            index: position.index.get(),
            term: position.term.get(),
            data: payload.into(),
            ..Default::default()
        });
        let mut hard = self.group.initial_state()?.hard_state;
        hard.term = 2;
        hard.vote = 1;
        hard.commit = position.index.get();
        write.hard_state = Some(hard);
        write.applied = Some(DurableApply {
            position,
            image: Some(marker(&pin)),
        });
        self.log.persist(OperationId::new(1), vec![write]).await?;
        self.bytes = next;
        self.txid += 1;
        Ok(())
    }

    fn payload(&self, cut: &images::Cut) -> Result<Vec<u8>> {
        let ltx_image = |state: cellar_ltx::apply::ImageState| LtxImage {
            epoch: state.epoch,
            txid: state.txid.0,
            checksum: state.checksum,
            image_digest: state.digest,
        };
        Ok(CellUpdate {
            scope: scope(),
            application_digest: [9; 32],
            proposal: ProposalId::new([self.txid + 1; 16])?,
            base: ltx_image(cut.base),
            result: ltx_image(cut.result),
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

    fn package(&self) -> Result<SnapshotPackage> {
        Ok(SnapshotPackage::capture(
            &self.group,
            self.applier()?.current().unwrap(),
            SnapshotLimits::default(),
        )?)
    }

    async fn install(&self, package: &SnapshotPackage) -> Result<()> {
        let snapshot = package.snapshot();
        let mut write = GroupWrite::new(&self.group);
        let position = LogPosition {
            index: LogIndex::new(snapshot.get_metadata().index),
            term: Term::new(snapshot.get_metadata().term),
        };
        write.snapshot = Some(SnapshotChange::Install {
            snapshot,
            applied: DurableApply {
                position,
                image: package.source_pin().as_ref().map(marker),
            },
        });
        let mut hard = self.group.initial_state()?.hard_state;
        hard.term = hard.term.max(position.term.get());
        hard.commit = position.index.get();
        write.hard_state = Some(hard);
        self.log.persist(OperationId::new(2), vec![write]).await?;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

fn download(package: &SnapshotPackage, target: &Fixture) -> Result<PreparedSnapshot> {
    let mut download = SnapshotDownload::new(
        &target.group,
        [9; 32],
        package.snapshot(),
        SnapshotLimits::default(),
    )?;
    for offset in (0..package.encoded_length()).step_by(257) {
        let bytes = package.chunk(offset, 257.min(package.encoded_length() - offset))?;
        download.append(offset, bytes)?;
    }
    Ok(download.finish(&target.images)?)
}

fn value(pin: GenerationPin) -> Result<i64> {
    let query = images::query(pin)?;
    Ok(query
        .connection()
        .query_row("SELECT value FROM counter", [], |row| row.get(0))?)
}

async fn checkpointed(events: &mut RuntimeEvents) -> Result<ApplyTicket> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match events.recv().await {
                Some(RuntimeEvent {
                    kind: RuntimeEventKind::Checkpointed { ticket },
                    ..
                }) => return Ok(ticket),
                Some(RuntimeEvent {
                    kind: RuntimeEventKind::Failed { error, .. },
                    ..
                }) => anyhow::bail!("checkpoint failed: {error}"),
                Some(_) => (),
                None => anyhow::bail!("checkpoint event stream closed"),
            }
        }
    })
    .await?
}

fn runtime_options() -> RaftRuntimeOptions {
    RaftRuntimeOptions {
        tick_interval_ms: None,
        election_seed: Some([18; 32]),
        ..Default::default()
    }
}

/// Exactly the admitted 64-MiB image, with incompressible overflow pages. The
/// SQLite record header has six bytes and 39 local bytes; each overflow page
/// carries 508 bytes. Three root pages plus the chain fill 131,072 pages.
fn maximum_image() -> Result<(Vec<u8>, usize)> {
    use rand::{RngCore, SeedableRng};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("maximum.sqlite");
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute_batch("PRAGMA page_size=512;PRAGMA journal_mode=MEMORY;PRAGMA synchronous=OFF;PRAGMA max_page_count=131072;CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(1);CREATE TABLE payloads(id INTEGER PRIMARY KEY,value BLOB);")?;
    let length = (131_072 - 3) * 508 + 33;
    let mut payload = vec![0; length];
    rand::rngs::StdRng::seed_from_u64(212).fill_bytes(&mut payload);
    connection.execute("INSERT INTO payloads VALUES(1,?1)", [payload])?;
    assert_eq!(
        connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
        "ok"
    );
    drop(connection);
    let bytes = std::fs::read(path)?;
    assert_eq!(bytes.len(), 64 * 1024 * 1024);
    Ok((bytes, length))
}

#[test]
fn snapshot_limits_require_full_encoded_image_capacity() -> Result<()> {
    let mut limits = SnapshotLimits::default();
    let bound = cellar_ltx::ltx::full_snapshot_bound(limits.max_image_bytes, 512).unwrap();
    ensure!(bound > limits.max_image_bytes as usize);
    limits.max_encoded_bytes = bound - 1;
    ensure!(limits.validate().is_err());
    limits.max_encoded_bytes = bound;
    limits.validate()?;
    ensure!(cellar_ltx::ltx::full_snapshot_bound(u64::MAX, 512).is_none());
    ensure!(cellar_ltx::ltx::full_snapshot_bound(512, 513).is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maximum_image_retained_snapshot_and_archive_roundtrip() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let namespace = Catalog::new(
        store,
        CatalogOptions {
            prefix: "maximum-image".into(),
            ..Default::default()
        },
    )?
    .open_namespace(NamespaceConfig::new(
        scope().cluster,
        scope().namespace,
        cellar_logic::NamespaceMode::Raft,
        [9; 32],
    ))
    .await?;
    let birth = namespace
        .resolve_birth(BirthProposal {
            scope: scope(),
            voters: (1..=3)
                .map(|member| InitialMember {
                    member: MemberId::new(member).unwrap(),
                    node: NodeId::new([member as u8; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        })
        .await?;
    let mut source = Fixture::open_born(
        &root.path().join("source"),
        1,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(DirectFileSystem),
        birth.digest(),
    )
    .await?;
    let target = Fixture::open_born(
        &root.path().join("target"),
        2,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(DirectFileSystem),
        birth.digest(),
    )
    .await?;
    let (bytes, payload_length) = maximum_image()?;
    let cut = images::Cut::new(&[], &bytes, 0, false, true);
    ensure!(
        cut.bytes.len() > 64 * 1024 * 1024,
        "the encoded payload must exercise the former limit"
    );
    assert!(
        cut.bytes.len() <= cellar_ltx::ltx::full_snapshot_bound(bytes.len() as u64, 512).unwrap()
    );
    let pin = source.images.prepare(images::id(1), None, &cut.update())?;
    drop(cut);
    let mut write = GroupWrite::new(&source.group);
    write.entries.push(Entry {
        index: 2,
        term: 2,
        ..Default::default()
    });
    let mut hard = source.group.initial_state()?.hard_state;
    hard.term = 2;
    hard.vote = 1;
    hard.commit = 2;
    write.hard_state = Some(hard);
    // This component fixture selects a real, completely verified SQLite image.
    // The seed is not evidence of a 64-MiB SDK command/envelope admission.
    write.applied = Some(DurableApply {
        position: LogPosition {
            index: LogIndex::new(2),
            term: Term::new(2),
        },
        image: Some(marker(&pin)),
    });
    source.log.persist(OperationId::new(1), vec![write]).await?;
    drop(pin);
    source.bytes = bytes;
    source.txid = 1;
    let package = source.package()?;
    ensure!(package.encoded_length() > source.bytes.len());
    let expected = package.snapshot();
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(package.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    runtime.close().await?;
    drop(package);

    let archive = source
        .domain
        .run(async {
            namespace.cell_archive(
                &birth,
                cellar_ltx::LtxHost::default(),
                ArchiveLimits::default(),
            )
        })
        .await??;
    let verifier = ArchiveVerifier::new(
        GenerationStore::open(
            root.path().join("verifier"),
            Arc::new(DirectFileSystem),
            cellar::sdk::consensus::RaftHostOptions::default().images,
        )?,
        images::id(91),
    )?;
    let published = source.applier()?.current().unwrap();
    let cut =
        CommittedArchiveCut::capture(&source.group, published, vec![], ArchiveLimits::default())?;
    let set = archive.publish(cut, &verifier).await?;
    assert_eq!(set.position().index.get(), 2);
    drop(set);

    source.commit(2).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    let package = SnapshotPackage::retained(
        &source.group,
        &source.images,
        [9; 32],
        &expected,
        SnapshotLimits::default(),
    )?;
    assert_eq!(package.snapshot(), expected);
    let mut download =
        SnapshotDownload::new(&target.group, [9; 32], expected, SnapshotLimits::default())?;
    for offset in (0..package.encoded_length()).step_by(1024 * 1024) {
        download.append(
            offset,
            package.chunk(offset, (1024 * 1024).min(package.encoded_length() - offset))?,
        )?;
    }
    let prepared = download.finish(&target.images)?;
    target.install(&package).await?;
    let mut applier = target.applier()?;
    let image = prepared.publish(&mut applier)?;
    assert_eq!(
        value(image.pin())?,
        1,
        "retained cut must not silently become the newer image"
    );
    assert_eq!(image.pin().state().length, 64 * 1024 * 1024);
    let query = images::query(image.pin())?;
    assert_eq!(
        query
            .connection()
            .query_row("SELECT length(value) FROM payloads", [], |row| row
                .get::<_, usize>(0))?,
        payload_length
    );
    drop(query);
    drop(image);
    drop(applier);
    drop(package);

    let cut = CommittedArchiveCut::capture(
        &source.group,
        source.applier()?.current().unwrap(),
        vec![],
        ArchiveLimits::default(),
    )?;
    let set = archive.publish(cut, &verifier).await?;
    assert_eq!(set.position().index.get(), 3);
    assert_eq!(set.tail_entries(), 1);
    drop(set);
    drop(verifier);
    drop(archive);
    source.close().await?;
    target.close().await?;
    std::fs::remove_dir_all(root.path().join("source"))?;
    std::fs::remove_dir_all(root.path().join("target"))?;

    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let archive = domain
        .run(async {
            namespace.cell_archive(
                &birth,
                cellar_ltx::LtxHost::default(),
                ArchiveLimits::default(),
            )
        })
        .await??;
    let set = archive.recovery_set().await?.unwrap();
    assert_eq!(set.position().index.get(), 3);
    let restored = GenerationStore::open(
        root.path().join("isolated-restore"),
        Arc::new(DirectFileSystem),
        cellar::sdk::consensus::RaftHostOptions::default().images,
    )?;
    let pin = set.restore(&restored, images::id(93))?;
    assert_eq!(value(pin.clone())?, 2);
    assert_eq!(pin.state().length, 64 * 1024 * 1024);
    let query = images::query(pin)?;
    assert_eq!(
        query
            .connection()
            .query_row("SELECT length(value) FROM payloads", [], |row| row
                .get::<_, usize>(0))?,
        payload_length
    );
    drop(query);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_checkpoint_capture_never_substitutes_newer_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let original = source.package()?;
    let expected = original.snapshot();
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(original.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    runtime.close().await?;
    drop(original);
    source.commit(2).await?;
    let retained = SnapshotPackage::retained(
        &source.group,
        &source.images,
        [9; 32],
        &expected,
        SnapshotLimits::default(),
    )?;
    assert_eq!(retained.snapshot(), expected);
    ensure!(SnapshotPackage::retained(
        &source.group,
        &source.images,
        [8; 32],
        &expected,
        SnapshotLimits::default()
    )
    .is_err());
    let next = source.package()?;
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(next.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    drop(source.applier()?);
    source.images.retire(images::id(1))?;
    source.images.reclaim(4096)?;
    ensure!(
        SnapshotPackage::retained(
            &source.group,
            &source.images,
            [9; 32],
            &expected,
            SnapshotLimits::default()
        )
        .is_err(),
        "a new request for the replaced cut must not return a different checkpoint"
    );
    let prepared = download(&retained, &target)?;
    target.install(&retained).await?;
    let mut applier = target.applier()?;
    assert_eq!(
        value(prepared.publish(&mut applier)?.pin())?,
        1,
        "an already captured transfer remains coherent after checkpoint replacement and GC"
    );
    drop(applier);
    drop(retained);
    drop(next);
    runtime.close().await?;
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_empty_checkpoint_has_no_image_or_payload() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    let original =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    let expected = original.snapshot();
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(original.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    drop(original);
    let retained = SnapshotPackage::retained(
        &source.group,
        &source.images,
        [9; 32],
        &expected,
        SnapshotLimits::default(),
    )?;
    assert_eq!(retained.snapshot(), expected);
    assert!(retained.source_pin().is_none());
    assert_eq!(retained.encoded_length(), 0);
    assert_eq!(source.images.usage()?.generations, 0);
    drop(retained);
    runtime.close().await?;
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_configuration_snapshot_installs_then_applies_first_ltx() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 3, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    let package =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    assert!(package.source_pin().is_none());
    assert_eq!(package.encoded_length(), 0);
    let (source_runtime, mut source_events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    source_runtime
        .checkpoint(package.checkpoint_material())
        .await?;
    assert_eq!(
        checkpointed(&mut source_events)
            .await?
            .position()
            .index
            .get(),
        3
    );
    assert_eq!(source.images.usage()?.generations, 0);
    let (runtime, mut events) = target
        .domain
        .run(RaftRuntime::start(target.log.clone(), runtime_options()))
        .await??;
    let mut applier = target.applier()?;
    let mut message = raft::prelude::Message {
        from: 1,
        to: 3,
        term: 2,
        ..Default::default()
    };
    message.set_msg_type(raft::prelude::MessageType::MsgSnapshot);
    message.set_snapshot(package.snapshot());
    runtime.step(&scope(), message).await?;
    let mut prepared = None;
    let mut installed = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !installed && tokio::time::Instant::now() < deadline {
        if let Ok(event) = events.try_recv() {
            match event.kind {
                RuntimeEventKind::Snapshot { ticket, snapshot } => {
                    let image = SnapshotDownload::new(
                        &target.group,
                        [9; 32],
                        snapshot,
                        SnapshotLimits::default(),
                    )?
                    .finish(&target.images)?;
                    let completion = image.completion(&ticket)?;
                    assert!(completion.applied.image.is_none());
                    runtime.complete_snapshot(completion).await?;
                    prepared = Some(image);
                }
                RuntimeEventKind::SnapshotInstalled { .. } => {
                    prepared.take().unwrap().publish_empty(&mut applier)?;
                    installed = true;
                }
                RuntimeEventKind::Failed { error, .. } => {
                    anyhow::bail!("empty snapshot failed: {error}")
                }
                _ => (),
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(installed, "empty snapshot must install");
    assert!(applier.current().is_none());
    assert_eq!(target.images.usage()?.generations, 0);
    assert_eq!(target.group.initial_state()?.conf_state.learners, vec![4]);
    assert_eq!(target.group.applied()?.position.index.get(), 3);

    let bytes = images::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(1)",
    )?;
    let cut = images::Cut::new(&[], &bytes, 0, true, true);
    let mut message = raft::prelude::Message {
        from: 1,
        to: 3,
        term: 2,
        index: 3,
        log_term: 2,
        commit: 4,
        entries: vec![Entry {
            index: 4,
            term: 2,
            data: source.payload(&cut)?.into(),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    message.set_msg_type(raft::prelude::MessageType::MsgAppend);
    runtime.step(&scope(), message).await?;
    let mut applied = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !applied && tokio::time::Instant::now() < deadline {
        if let Ok(event) = events.try_recv() {
            match event.kind {
                RuntimeEventKind::Apply { ticket, entry } => {
                    runtime
                        .complete_apply(applier.prepare(ticket, &entry, images::id(1))?)
                        .await?;
                }
                RuntimeEventKind::Applied { ticket, .. } => {
                    assert_eq!(value(applier.publish(&ticket)?.pin())?, 1);
                    applied = true;
                }
                RuntimeEventKind::Failed { error, .. } => {
                    anyhow::bail!("first image failed: {error}")
                }
                _ => (),
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(applied, "first LTX command after empty install must apply");
    runtime.close().await?;
    source_runtime.close().await?;
    drop(applier);
    drop(prepared);
    drop(package);
    source.close().await?;
    target.close().await?;
    let target = Fixture::open(&root.path().join("target"), 3, LogOpenMode::Resume).await?;
    assert_eq!(value(target.applier()?.current().unwrap().pin())?, 1);
    assert_eq!(target.group.snapshot(0, 1)?.get_metadata().index, 3);
    assert_eq!(target.images.usage()?.generations, 1);
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_snapshot_refuses_data_and_cannot_erase_committed_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    target.commit(9).await?;
    ensure!(
        SnapshotPackage::capture_empty(&target.group, [9; 32], SnapshotLimits::default()).is_err()
    );
    let package =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    let mut download = SnapshotDownload::new(
        &target.group,
        [9; 32],
        package.snapshot(),
        SnapshotLimits::default(),
    )?;
    ensure!(download.append(0, &[1]).is_err());
    let prepared = download.finish(&target.images)?;
    let mut applier = target.applier()?;
    ensure!(prepared.publish_empty(&mut applier).is_err());
    ensure!(target.install(&package).await.is_err());
    assert_eq!(value(applier.current().unwrap().pin())?, 9);
    assert_eq!(target.group.applied()?.position.index.get(), 2);
    drop(applier);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_handoff_survives_newer_apply_late_retirement_and_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let gate = log_fault::Gate::new(None);
    let mut source = Fixture::with_filesystems(
        root.path(),
        1,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(log_fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    let old_state = package.source_pin().unwrap().state();
    source.commit(2).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    drop(package);
    source.images.retire(images::id(1))?;
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    let release = gate.arm(log_fault::HOLD_SYNC);
    runtime.checkpoint(checkpoint).await?;
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified()).await?;
    assert_eq!(source.group.snapshot(0, 1)?.get_metadata().index, 1);
    assert!(
        events.try_recv().is_err(),
        "unsynced checkpoint must not report completion"
    );
    assert_eq!(source.images.reclaim(4096)?.files, 0);
    drop(release);
    let ticket = checkpointed(&mut events).await?;
    assert_eq!(ticket.position().index.get(), 2);
    assert_eq!(source.group.snapshot(0, 1)?.get_metadata().index, 2);
    assert_eq!(source.group.applied()?.position.index.get(), 3);
    // A publisher that captured older metadata can retire after checkpoint sync.
    // The independent reference, not a retained process-local pin, must win.
    source.images.retire(images::id(1))?;
    assert_eq!(source.images.reclaim(4096)?.files, 0);
    let old = source.images.adopt(images::id(1), old_state)?;
    assert_eq!(value(old.clone())?, 1);
    drop(old);
    assert!(
        !runtime.status(&scope()).await?.protocol_ready,
        "a local checkpoint is not leader authority"
    );
    runtime.close().await?;
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    let mut applier = source.applier()?;
    assert_eq!(value(applier.current().unwrap().pin())?, 2);
    assert_eq!(source.images.usage()?.generations, 2);
    let package = source.package()?;
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(package.checkpoint_material()).await?;
    assert_eq!(checkpointed(&mut events).await?.position().index.get(), 3);
    applier.refresh_snapshot()?;
    source.images.retire(images::id(1))?;
    assert_eq!(source.images.reclaim(4096)?.files, 1);
    assert_eq!(source.images.usage()?.generations, 1);
    runtime.close().await?;
    drop(package);
    drop(applier);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_completion_loss_still_retains_durable_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    source.commit(2).await?;
    drop(source.applier()?);
    drop(package);
    let (runtime, events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    // The runtime must settle retention at storage completion, independently of
    // whether its consumer polls or retains the eventual notification.
    runtime.checkpoint(checkpoint).await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while source.group.snapshot(0, 1)?.get_metadata().index != 2 {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "checkpoint did not sync"
        );
        tokio::task::yield_now().await;
    }
    runtime.close().await?;
    drop(events);
    source.images.retire(images::id(1))?;
    source.images.reclaim(4096)?;
    assert_eq!(source.images.usage()?.generations, 2);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_refuses_material_from_reopened_engine() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    source.log.close().await?;
    source.log = source
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("raft"),
                NodeId::new([1; 16])?,
                DiskLineage::new([41; 16])?,
                ProcessGeneration::new([99; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    source.group = source.log.group(&scope())?;
    let (runtime, _events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    ensure!(matches!(
        runtime.checkpoint(checkpoint).await,
        Err(RuntimeError::Invalid(_))
    ));
    assert_eq!(source.group.snapshot(0, 1)?.get_metadata().index, 1);
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 1);
    runtime.close().await?;
    drop(package);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_failed_sync_preserves_prospective_recovery_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let gate = log_fault::Gate::new(None);
    let mut source = Fixture::with_filesystems(
        root.path(),
        1,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(log_fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    source.commit(2).await?;
    drop(source.applier()?);
    drop(package);
    source.images.retire(images::id(1))?;
    let (runtime, _events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    let _release = gate.arm(log_fault::FAIL_SYNC);
    runtime.checkpoint(checkpoint).await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while runtime.status(&scope()).await.is_ok() {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "failed checkpoint must close consensus"
        );
        tokio::task::yield_now().await;
    }
    ensure!(runtime.close().await.is_err());
    ensure!(source.log.close().await.is_err());
    source.domain.shutdown().await;
    // The write may have reached the engine despite its failed sync response.
    // Joining and losing the producer must not make those image bytes reclaimable.
    source.images.retire(images::id(1))?;
    assert_eq!(source.images.reclaim(4096)?.files, 0);
    assert_eq!(source.images.usage()?.generations, 2);
    drop(source);
    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    let index = source.group.snapshot(0, 1)?.get_metadata().index;
    ensure!(index == 1 || index == 2);
    assert_eq!(
        source.images.usage()?.generations,
        if index == 1 { 1 } else { 2 }
    );
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_wrong_history() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    for (field, replacement) in [
        ("birth", serde_json::json!(vec![10; 32])),
        ("schema", serde_json::json!(vec![11; 32])),
        ("index", serde_json::json!(99)),
        ("format", serde_json::json!(2)),
    ] {
        let mut snapshot = package.snapshot();
        let descriptor: serde_json::Value = serde_json::from_slice(&snapshot.data)?;
        let original = String::from_utf8(snapshot.data.to_vec())?;
        snapshot.data = original
            .replacen(
                &format!("\"{field}\":{}", descriptor[field]),
                &format!("\"{field}\":{replacement}"),
                1,
            )
            .into_bytes()
            .into();
        ensure!(
            SnapshotDownload::new(&target.group, [9; 32], snapshot, SnapshotLimits::default())
                .is_err()
        );
    }
    for field in ["cluster", "namespace", "group", "incarnation"] {
        let mut snapshot = package.snapshot();
        let descriptor: serde_json::Value = serde_json::from_slice(&snapshot.data)?;
        let original = String::from_utf8(snapshot.data.to_vec())?;
        snapshot.data = original
            .replacen(
                &format!("\"{field}\":{}", descriptor["scope"][field]),
                &format!("\"{field}\":\"{}\"", hex::encode([22; 16])),
                1,
            )
            .into_bytes()
            .into();
        ensure!(
            SnapshotDownload::new(&target.group, [9; 32], snapshot, SnapshotLimits::default())
                .is_err()
        );
    }
    let mut other = package.snapshot();
    other.mut_metadata().mut_conf_state().voters.push(4);
    ensure!(
        SnapshotDownload::new(&target.group, [9; 32], other, SnapshotLimits::default()).is_err()
    );
    let mut state = SnapshotDownload::new(
        &target.group,
        [9; 32],
        package.snapshot(),
        SnapshotLimits::default(),
    )?;
    let mut bytes = package.chunk(0, package.encoded_length())?.to_vec();
    bytes[0] ^= 0xff;
    state.append(0, &bytes)?;
    ensure!(state.finish(&target.images).is_err());
    assert_eq!(target.group.applied()?.position.index.get(), 1);
    assert_eq!(target.images.usage()?.generations, 0);
    target.commit(999).await?;
    ensure!(
        download(&package, &target).is_err(),
        "a colliding image name cannot replace unrelated bytes"
    );
    assert_eq!(value(target.applier()?.current().unwrap().pin())?, 999);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compacted_boundary_term() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    assert_eq!(source.group.term(2)?, 2);
    assert_eq!(source.group.first_index()?, 3);
    ensure!(source.group.term(1).is_err());
    let mut write = GroupWrite::new(&source.group);
    write.entries.push(Entry {
        index: 3,
        term: 2,
        data: b"uncommitted suffix".to_vec().into(),
        ..Default::default()
    });
    source.log.persist(OperationId::new(4), vec![write]).await?;
    drop(package);
    source.close().await?;
    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    assert_eq!(source.group.term(2)?, 2);
    assert_eq!(source.group.last_index()?, 3);
    assert_eq!(source.group.initial_state()?.hard_state.commit, 2);
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 1);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_old_reader() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    target.commit(1).await?;
    let mut applier = target.applier()?;
    let old = applier.current().unwrap().pin();
    let reader = images::query(old.clone())?;
    let package = source.package()?;
    ensure!(
        download(&package, &target)?.publish(&mut applier).is_err(),
        "sealed bytes alone must not publish"
    );
    assert_eq!(value(applier.current().unwrap().pin())?, 1);
    let prepared = download(&package, &target)?;
    target.install(&package).await?;
    let current = prepared.publish(&mut applier)?;
    target.images.reclaim(4096)?;
    assert_eq!(value(current.pin())?, 2);
    assert_eq!(
        reader
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?,
        1
    );
    assert_eq!(value(old.clone())?, 1);
    drop(reader);
    drop(old);
    target.images.reclaim(4096)?;
    assert_eq!(target.images.usage()?.generations, 1);
    // Repeated installation adopts the same sealed generation and does not
    // consume another name, image slot or business command execution.
    let repeat = download(&package, &target)?;
    target.install(&package).await?;
    assert_eq!(value(repeat.publish(&mut applier)?.pin())?, 2);
    assert_eq!(target.images.usage()?.generations, 1);
    drop(current);
    drop(applier);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_download_bounds_and_incomplete_refusal() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    let limits = SnapshotLimits {
        max_chunk_bytes: 32,
        ..Default::default()
    };
    let mut state = SnapshotDownload::new(&target.group, [9; 32], package.snapshot(), limits)?;
    ensure!(state.append(1, &[0]).is_err());
    ensure!(state.append(0, &[]).is_err());
    ensure!(state.append(0, &[0; 33]).is_err());
    state.append(0, package.chunk(0, 32)?)?;
    ensure!(state.append(0, &[0]).is_err());
    ensure!(state.finish(&target.images).is_err());
    assert_eq!(target.images.usage()?.generations, 0);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reference_survives_newer_head_and_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    drop(package);
    source.commit(3).await?;
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    let mut applier = source.applier()?;
    source.images.reclaim(4096)?;
    assert_eq!(value(applier.current().unwrap().pin())?, 3);
    let old_state = images::state(
        &images::image(
            None,
            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1)",
        )?,
        1,
        true,
    );
    ensure!(source.images.adopt(images::id(1), old_state).is_err());
    assert_eq!(
        source.images.usage()?.generations,
        2,
        "the older durable snapshot must remain retained"
    );
    drop(applier);
    source.images.reclaim(4096)?;
    assert_eq!(
        source.images.usage()?.generations,
        2,
        "durable retention must outlive process-local pins"
    );

    applier = source.applier()?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(4), vec![package.checkpoint()])
        .await?;
    applier.refresh_snapshot()?;
    source.images.reclaim(4096)?;
    assert_eq!(
        source.images.usage()?.generations,
        1,
        "a newer durable snapshot replaces the old reference"
    );
    drop(package);
    drop(applier);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_runtime_installs_physical_image_with_exact_ticket() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    let options = |seed| RaftRuntimeOptions {
        tick_interval_ms: None,
        election_seed: Some([seed; 32]),
        ..Default::default()
    };
    let (leader, mut leader_events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), options(11)))
        .await??;
    let (follower, mut follower_events) = target
        .domain
        .run(RaftRuntime::start(target.log.clone(), options(12)))
        .await??;
    let mut applier = target.applier()?;
    let mut prepared = None;
    let mut installed = false;
    leader.campaign(&scope()).await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !installed && tokio::time::Instant::now() < deadline {
        for (node, events) in [(1, &mut leader_events), (2, &mut follower_events)] {
            while let Ok(event) = events.try_recv() {
                match event.kind {
                    RuntimeEventKind::Message { message, .. } => match message.to {
                        1 => leader.step(&scope(), message).await?,
                        2 => follower.step(&scope(), message).await?,
                        _ => (),
                    },
                    RuntimeEventKind::Snapshot { ticket, snapshot } => {
                        assert_eq!(node, 2);
                        assert_eq!(snapshot, package.snapshot());
                        assert_eq!(target.group.applied()?.position.index.get(), 1);
                        ensure!(applier.current().is_none());
                        let image = download(&package, &target)?;
                        let completion = image.completion(&ticket)?;
                        follower.complete_snapshot(completion.clone()).await?;
                        ensure!(follower.complete_snapshot(completion).await.is_err());
                        prepared = Some(image);
                    }
                    RuntimeEventKind::SnapshotInstalled { ticket, applied } => {
                        assert_eq!(node, 2);
                        assert_eq!(ticket.position(), applied.position);
                        target.images.retire(images::id(2))?;
                        drop(prepared.take().unwrap());
                        assert_eq!(target.images.reclaim(4096)?.files, 0,
                            "durable retention must outlive the transfer owner and delayed retirement");
                        let image = download(&package, &target)?.publish(&mut applier)?;
                        assert_eq!(value(image.pin())?, 2);
                        installed = true;
                    }
                    RuntimeEventKind::Failed { error, .. } => {
                        anyhow::bail!("runtime failed: {error}")
                    }
                    RuntimeEventKind::Apply { .. } => {
                        anyhow::bail!("compacted data must arrive through the snapshot")
                    }
                    _ => (),
                }
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(
        installed,
        "a live majority must install the physical snapshot"
    );
    leader.close().await?;
    follower.close().await?;
    drop(prepared);
    drop(applier);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_committed_replay() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    let bytes = images::image(Some(&source.bytes), "UPDATE counter SET value=2")?;
    let cut = images::Cut::new(&source.bytes, &bytes, 1, true, true);
    let mut write = GroupWrite::new(&source.group);
    write.entries = vec![
        Entry {
            index: 3,
            term: 2,
            data: source.payload(&cut)?.into(),
            ..Default::default()
        },
        Entry {
            index: 4,
            term: 2,
            data: b"uncommitted suffix".to_vec().into(),
            ..Default::default()
        },
    ];
    let mut hard = source.group.initial_state()?.hard_state;
    hard.commit = 3;
    write.hard_state = Some(hard);
    source.log.persist(OperationId::new(4), vec![write]).await?;
    assert_eq!(source.group.applied()?.position.index.get(), 2);
    drop(package);
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    let mut applier = source.applier()?;
    assert_eq!(value(applier.current().unwrap().pin())?, 1);
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(
            source.log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([12; 32]),
                ..Default::default()
            },
        ))
        .await??;
    let mut published = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !published && tokio::time::Instant::now() < deadline {
        while let Ok(event) = events.try_recv() {
            match event.kind {
                RuntimeEventKind::Apply { ticket, entry } => {
                    assert_eq!(entry.index, 3, "the uncommitted suffix must never execute");
                    let package = source.package()?;
                    assert_eq!(
                        tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            runtime.checkpoint(package.checkpoint_material())
                        )
                        .await?,
                        Err(RuntimeError::NotReady),
                        "checkpoint admission must not wait behind the apply it would block"
                    );
                    let completion = applier.prepare(ticket, &entry, images::id(2))?;
                    runtime.complete_apply(completion).await?;
                }
                RuntimeEventKind::Applied { ticket, .. } => {
                    assert_eq!(value(applier.publish(&ticket)?.pin())?, 2);
                    published = true;
                }
                RuntimeEventKind::Failed { error, .. } => anyhow::bail!("replay failed: {error}"),
                _ => (),
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(
        published,
        "the durable committed tail must replay after restart"
    );
    assert_eq!(source.group.last_index()?, 4);
    assert_eq!(source.group.initial_state()?.hard_state.commit, 3);
    assert_eq!(source.group.applied()?.position.index.get(), 3);
    runtime.close().await?;
    drop(applier);
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    assert_eq!(
        source.images.usage()?.generations,
        2,
        "the snapshot and newer applied image both survive"
    );
    assert_eq!(source.group.last_index()?, 4);
    assert_eq!(source.group.initial_state()?.hard_state.commit, 3);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_same_cut_cannot_name_an_unrelated_applied_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    target.commit(1).await?;
    let old = target.group.applied()?.image;
    let package = source.package()?;
    let prepared = download(&package, &target)?;
    let snapshot = package.snapshot();
    let mut write = GroupWrite::new(&target.group);
    write.snapshot = Some(SnapshotChange::Install {
        snapshot,
        applied: DurableApply {
            position: LogPosition {
                index: LogIndex::new(3),
                term: Term::new(2),
            },
            image: old,
        },
    });
    let mut hard = target.group.initial_state()?.hard_state;
    hard.commit = 3;
    write.hard_state = Some(hard);
    // The mechanical engine API deliberately accepts opaque snapshot data.
    // Application recovery must independently refuse its mismatched image.
    target.log.persist(OperationId::new(9), vec![write]).await?;
    ensure!(target.applier().is_err());
    assert_eq!(
        target.images.usage()?.generations,
        2,
        "failed reference validation must not partly reclaim inventory"
    );
    drop(prepared);
    drop(package);
    source.close().await?;
    target.close().await
}

struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_install_crash_matrix() -> Result<()> {
    for cut in [
        "image-before-sync",
        "image-after-rename",
        "image-after-directory-sync",
        "engine-before-sync",
        "engine-after-sync",
        "engine-after-reply",
        "after-publication",
    ] {
        let root = tempfile::tempdir()?;
        let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
        let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
        source.commit(1).await?;
        source.commit(2).await?;
        target.commit(1).await?;
        source.close().await?;
        target.close().await?;
        let signal = root.path().join("child.signal");
        let output = root.path().join("child.log");
        let file = std::fs::File::create(&output)?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "snapshot_process_fixture",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("CELLAR_SNAPSHOT_TEST_ROOT", root.path())
                .env("CELLAR_SNAPSHOT_TEST_CUT", cut)
                .env("CELLAR_SNAPSHOT_TEST_SIGNAL", &signal)
                .stdout(file.try_clone()?)
                .stderr(file)
                .spawn()?,
        );
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while !signal.exists() {
                if let Some(status) = child.0.try_wait()? {
                    anyhow::bail!(
                        "child exited before {cut}: {status}; {}",
                        std::fs::read_to_string(&output)?
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        child.0.kill()?;
        ensure!(!child.0.wait()?.success(), "fixture must be killed");
        drop(child);

        let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Resume).await?;
        let applier = target.applier()?;
        let image = applier.current().unwrap();
        let snapshot_index = target.group.snapshot(0, 1)?.get_metadata().index;
        match image.position().index.get() {
            2 => {
                ensure!(
                    cut.starts_with("image-") || cut == "engine-before-sync",
                    "synced install lost at {cut}"
                );
                assert_eq!(snapshot_index, 1);
                assert_eq!(value(image.pin())?, 1);
                assert_eq!(target.group.initial_state()?.hard_state.commit, 2);
            }
            3 => {
                ensure!(
                    !cut.starts_with("image-"),
                    "an unsubmitted install appeared at {cut}"
                );
                assert_eq!(snapshot_index, 3);
                assert_eq!(value(image.pin())?, 2);
                assert_eq!(target.group.initial_state()?.hard_state.commit, 3);
            }
            index => anyhow::bail!("incoherent install at {cut}: {index}"),
        }
        assert_eq!(
            target.images.usage()?.generations,
            1,
            "unreferenced interrupted images must be reclaimed at {cut}"
        );
        drop(image);
        drop(applier);
        target.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "process-kill fixture, invoked by snapshot_install_crash_matrix"]
async fn snapshot_process_fixture() -> Result<()> {
    use std::sync::atomic::Ordering;
    let root = std::path::PathBuf::from(std::env::var("CELLAR_SNAPSHOT_TEST_ROOT")?);
    let cut = std::env::var("CELLAR_SNAPSHOT_TEST_CUT")?;
    let signal = std::path::PathBuf::from(std::env::var("CELLAR_SNAPSHOT_TEST_SIGNAL")?);
    let source = Fixture::open(&root.join("source"), 1, LogOpenMode::Resume).await?;
    let image_faults = image_fault::Faults::new(root.join("target/images"), Some(signal.clone()));
    let log_gate = log_fault::Gate::new(Some(signal.clone()));
    let target = Fixture::with_filesystems(
        &root.join("target"),
        2,
        LogOpenMode::Resume,
        Arc::new(image_fault::FaultFileSystem {
            faults: image_faults.clone(),
        }),
        Arc::new(log_fault::FaultFileSystem {
            gate: log_gate.clone(),
        }),
    )
    .await?;
    let mut applier = target.applier()?;
    let package = source.package()?;
    let image_mode = match cut.as_str() {
        "image-before-sync" => image_fault::BEFORE_IMAGE_SYNC,
        "image-after-rename" => image_fault::AFTER_RENAME,
        "image-after-directory-sync" => image_fault::AFTER_DIRECTORY_SYNC,
        _ => 0,
    };
    image_faults.mode.store(image_mode, Ordering::Release);
    let prepared = download(&package, &target)?;
    let _release = match cut.as_str() {
        "engine-before-sync" => Some(log_gate.arm(log_fault::CRASH_BEFORE_SYNC)),
        "engine-after-sync" => Some(log_gate.arm(log_fault::CRASH_AFTER_SYNC)),
        _ => None,
    };
    target.install(&package).await?;
    if cut == "after-publication" {
        assert_eq!(value(prepared.publish(&mut applier)?.pin())?, 2);
    }
    let mut file = std::fs::File::create(signal)?;
    std::io::Write::write_all(&mut file, b"reached")?;
    file.sync_all()?;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
