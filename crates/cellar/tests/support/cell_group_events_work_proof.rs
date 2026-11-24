// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the sealed source checkpoint, then cut its real archive publication.
use super::*;
use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileIo,
    HostFileLock, HostMetadata,
};
use sha2::{Digest, Sha256};
use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
};

#[derive(Debug)]
struct Checkpoint {
    intents: u64,
    reserved: bool,
    minimum: u8,
}

#[derive(Clone)]
struct Files {
    first: Arc<Mutex<Option<std::result::Result<Checkpoint, String>>>>,
    store: Arc<catalog_fault::Store>,
    cut: Arc<AtomicBool>,
}

impl Files {
    fn file(&self, path: &Path, file: HostFile) -> HostFile {
        if path.extension().is_some_and(|e| e == "sqlite")
            && path.to_string_lossy().contains("/raft-cells/images/")
            && !path.to_string_lossy().contains("/archive-verifier/")
        {
            HostFile::from_io(SyncedImage {
                file,
                path: path.to_owned(),
                observer: self.clone(),
            })
        } else {
            file
        }
    }

    fn observe(&self, path: &Path) {
        // Generation preparation finishes writing before this handle's file
        // barrier. Another operation's directory sync cannot establish that
        // this image is complete. This supplies no publication authority.
        let mut first = self.first.lock().unwrap();
        if first.is_some() {
            return;
        }
        match self
            .inspect(path)
            .with_context(|| format!("observe completed image {}", path.display()))
        {
            Ok(None) => {}
            Ok(Some(checkpoint)) => {
                *first = Some(Ok(checkpoint));
                if self.cut.load(Ordering::Acquire) {
                    self.store
                        .deny_sandbox_parent_writes
                        .store(true, Ordering::Release);
                }
            }
            Err(error) => *first = Some(Err(format!("{error:#}"))),
        }
    }

    fn inspect(&self, path: &Path) -> Result<Option<Checkpoint>> {
        let mut url =
            reqwest::Url::from_file_path(path).map_err(|_| anyhow::anyhow!("image URI"))?;
        url.query_pairs_mut()
            .append_pair("mode", "ro")
            .append_pair("immutable", "1");
        let db = rusqlite::Connection::open_with_flags(
            url.as_str(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let source: bool = db.query_row(
            "SELECT count(*)=2 FROM sqlite_schema WHERE name IN ('__sys_outbox_lanes','__sys_raft_native_work')",
            [],
            |r| r.get(0),
        )?;
        if !source {
            return Ok(None);
        }
        let outbox: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_raft_native_work WHERE reason='outbox')",
            [],
            |r| r.get(0),
        )?;
        if !outbox {
            return Ok(None);
        }
        let first = 1u64.to_be_bytes();
        let receipt = db.query_row(
            "SELECT count(*),min(minimum_durability) FROM __sys_raft_receipts
             WHERE operation='__sys_raft_native_work' AND rpc_step=?1",
            [first.as_slice()],
            |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Option<u8>>(1)?)),
        )?;
        if receipt.0 == 0 {
            return Ok(None);
        }
        let has_intents: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='__sys_raft_binding_intents')",
            [],
            |r| r.get(0),
        )?;
        let intents = if has_intents {
            db.query_row(
                "SELECT count(*) FROM __sys_raft_binding_intents i JOIN __sys_raft_receipts r
             ON i.parent_client=r.client_id AND i.parent_sequence=r.sequence
             WHERE r.operation='__sys_raft_native_work' AND r.rpc_step=?1 AND i.phase=0",
                [first.as_slice()],
                |r| r.get(0),
            )?
        } else {
            0
        };
        let reserved = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_outbox_lanes WHERE dispatch_generation=1 AND attempts=1 AND retry_at_ms>0)",
            [],
            |r| r.get(0),
        )?;
        Ok(Some(Checkpoint {
            intents,
            reserved,
            minimum: receipt.1.context("work receipt minimum")?,
        }))
    }
}

struct SyncedImage {
    file: HostFile,
    path: PathBuf,
    observer: Files,
}

impl HostFileIo for SyncedImage {
    fn advise_streaming(&mut self) -> io::Result<()> {
        self.file.advise_streaming()
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.file.read_exact_at(offset, len)
    }

    fn read_exact_into_at(&mut self, offset: u64, output: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_into_at(offset, output)
    }

    fn write_all_window(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all_window(bytes)
    }

    fn write_all_at_window(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all_at_window(offset, bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        self.observer.observe(&self.path);
        Ok(())
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.file.file_len()
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all_at(offset, bytes)
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.file.set_len(length)
    }
}

impl FileSystem for Files {
    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        let file = DirectFileSystem.create_read_write(path)?;
        Ok(self.file(path, file))
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open_read_write(path)
    }

    fn clone_read_write(&self, from: &Path, to: &Path) -> io::Result<Option<HostFile>> {
        let file = DirectFileSystem.clone_read_write(from, to)?;
        Ok(file.map(|file| self.file(to, file)))
    }

    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        DirectFileSystem.temporary_file(directory)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        DirectFileSystem.read_dir_iter(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.symlink_metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_new(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(path)
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        DirectFileSystem.remove_dir_batch(path, limit)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }
}

pub fn qualify_observer() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("raft-cells/images");
    DirectFileSystem.create_dir_all(&directory)?;
    let fixture = root.path().join("fixture.db");
    let db = rusqlite::Connection::open(&fixture)?;
    db.execute_batch(
        "CREATE TABLE __sys_outbox_lanes(dispatch_generation,attempts,retry_at_ms);
        INSERT INTO __sys_outbox_lanes VALUES(0,0,0);
        CREATE TABLE __sys_raft_native_work(reason,claimed);
        INSERT INTO __sys_raft_native_work VALUES('outbox',1);
        CREATE TABLE __sys_raft_receipts(operation,rpc_step,minimum_durability);
        INSERT INTO __sys_raft_receipts VALUES('__sys_raft_native_work',x'0000000000000001',0);",
    )?;
    drop(db);
    let bytes = DirectFileSystem.read(&fixture)?;
    let files = Files {
        first: Arc::new(Mutex::new(None)),
        store: Arc::new(catalog_fault::Store::default()),
        cut: Arc::new(AtomicBool::new(false)),
    };
    let path = directory.join("candidate.sqlite");
    let mut image = files.create_read_write(&path)?;
    image.write_all(&bytes[..100])?;
    // Another store operation can sync this shared inventory while this
    // generation is still being written. That must not trigger inspection.
    files.sync_all(&directory)?;
    ensure!(
        files.first.lock().unwrap().is_none(),
        "unrelated directory sync inspected an unfinished image"
    );
    image.write_all_at_window(0, &bytes)?;
    image.sync_all()?;
    let first = files.first.lock().unwrap();
    let checkpoint = first
        .as_ref()
        .context("completed image was not observed")?
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))?;
    ensure!(
        checkpoint.intents == 0 && !checkpoint.reserved && checkpoint.minimum == 0,
        "observer did not inspect its completed image: {checkpoint:?}"
    );
    Ok(())
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    for minimum in [Durability::Quorum, Durability::S3] {
        let store = Arc::new(if real_rustfs {
            catalog_fault::Store::around(rustfs::store(
                if minimum == Durability::S3 {
                    "outbox-proof-s3"
                } else {
                    "outbox-proof-quorum"
                },
                false,
            )?)
        } else {
            catalog_fault::Store::default()
        });
        let files = Arc::new(Files {
            first: Arc::new(Mutex::new(None)),
            store: store.clone(),
            cut: Arc::new(AtomicBool::new(false)),
        });
        let (application, declared) = application()?;
        let definitions = [
            "Counter",
            cellar::deploy::EVENT_STREAM_CLASS,
            cellar::deploy::EVENT_CATALOG_CLASS,
        ]
        .into_iter()
        .map(|name| {
            Ok((
                application.resolve_raft_class(name)?,
                if name == "Counter" {
                    minimum
                } else {
                    Durability::Quorum
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
        let mut fixture =
            Fixture::with_definition_policies(false, definitions, application, store.clone())
                .await?;
        fixture.configuration = Some(declared);
        fixture.event_streams = vec![definition()];
        fixture.sql_workers = 1;
        fixture.max_input_bytes = 384 << 10;
        fixture.cell_limits.events.max_event_bytes = 2048;
        fixture.cell_limits.events.max_batch_bytes = 4096;
        fixture.cell_limits.events.max_outbox_bytes = 64 << 10;
        fixture.effects = Some(RaftEffectOptions {
            callback_timeout_ms: 5000,
            ..Default::default()
        });
        let domain = fixture.domain.clone();
        let mut cluster = Box::pin(ManagedCluster::start_with_filesystems(
            fixture,
            vec![files.clone(); 3],
        ))
        .await?;
        let work = domain
            .run(async {
                named_service::activate_namespace(&cluster, 0, "stream-archive").await?;
                named_service::activate_namespace(&cluster, 1, "catalog-archive").await?;
                for node in &cluster.nodes {
                    node.activate_event_resources().await?;
                }
                let fixture = &cluster.fixtures[0];
                let catalog = verification::Catalog::new(
                    fixture.store.clone(),
                    verification::CatalogOptions {
                        prefix: "managed/".into(),
                        ..Default::default()
                    },
                )?;
                let namespace = catalog
                    .existing_namespace(fixture.declaration.namespace.clone())
                    .await?;
                let group = cluster.history;
                let mut digest = Sha256::new();
                digest.update(b"cellar-cell-group-archive-history-1\0");
                for id in [
                    group.cluster.bytes(),
                    group.namespace.bytes(),
                    group.group.bytes(),
                    group.incarnation.bytes(),
                ] {
                    digest.update(id);
                }
                *store.sandbox_parent_archive_prefix.lock().unwrap() =
                    Some(object_store::path::Path::from(format!(
                        "{}cell-group-archives/cellar-cell-group-archive-checkpoint-1/{}",
                        namespace.data_prefix(),
                        hex::encode(digest.finalize())
                    )));
                let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                let scope = cellar_native::cell_id::Namespace::new(
                    "Counter",
                    format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
                )
                .id_from_name("producer")
                .scope();
                let cell = cluster.hosts[leader].cell(CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(scope)?,
                    incarnation: IncarnationId::new([226; 16])?,
                })?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([227; 16])?,
                        durability: minimum,
                    },
                    TIMEOUT,
                )
                .await?;
                files
                    .cut
                    .store(minimum == Durability::S3, Ordering::Release);
                let retry = u64::try_from(cellar::asyncrt::wall_ms())? + 60_000;
                let options =
                    CommandOptions::new(CommandId::new(ClientId::new([228; 16])?, 1)?, retry);
                let emitted = cell
                    .command(&cell.contract("Emit")?, b"[]".to_vec(), options.clone())
                    .await;
                if let Err(error) = emitted {
                    ensure!(
                        minimum == Durability::S3
                            && matches!(
                                error.cause.as_ref(),
                                CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                            ),
                        "unexpected Emit failure: {error:?}"
                    );
                }
                cellar::asyncrt::timeout(TIMEOUT, async {
                    while files.first.lock().unwrap().is_none() {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
                {
                    let first = files.first.lock().unwrap();
                    let checkpoint = first
                        .as_ref()
                        .unwrap()
                        .as_ref()
                        .map_err(|e| anyhow::anyhow!(e.clone()))?;
                    ensure!(
                        checkpoint.intents == 1
                            && checkpoint.reserved
                            && checkpoint.minimum == u8::from(minimum == Durability::S3),
                        "first outbox proof split claim from intent: {checkpoint:?}"
                    );
                }
                if minimum == Durability::S3 {
                    // Observe the actual destination through its public read API
                    // across the original five-second callback; never query or
                    // mutate the source to prompt a recovery turn.
                    let until = cellar::asyncrt::instant() + TIMEOUT;
                    while cellar::asyncrt::instant() < until {
                        ensure!(
                            read(&cluster, leader).await? == json!([]),
                            "unproved outbox reached destination"
                        );
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                    ensure!(
                        store.denied_sandbox_parent_writes.load(Ordering::Acquire) > 0,
                        "source archive cut was not exercised"
                    );
                    store
                        .deny_sandbox_parent_writes
                        .store(false, Ordering::Release);
                }
                let delivered = cellar::asyncrt::timeout(Duration::from_secs(60), async {
                    loop {
                        let records = read(&cluster, leader).await?;
                        let envelopes: Vec<events::Envelope> =
                            serde_json::from_value(records.clone())?;
                        if envelopes.len() == 1 {
                            ensure!(envelopes[0].key == "order-1", "unexpected event key");
                            return Ok::<_, anyhow::Error>(records);
                        }
                        ensure!(envelopes.is_empty(), "duplicate destination events");
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("outbox did not autonomously recover its source proof")??;
                let replay = cell
                    .command(&cell.contract("Emit")?, b"[]".to_vec(), options)
                    .await?;
                ensure!(replay.result == b"true", "Emit receipt replay");
                let counts = cell
                    .query(
                        ProposalId::new([229; 16])?,
                        &cell.contract("Counts")?,
                        vec![],
                        QueryOptions::default(),
                    )
                    .await?;
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&counts.result)?
                        == json!([1, null]),
                    "source callback repeated"
                );
                ensure!(
                    read(&cluster, leader).await? == delivered,
                    "receipt replay duplicated destination events"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        store
            .deny_sandbox_parent_writes
            .store(false, Ordering::Release);
        let close = managed_fixture::close_group_nodes_with_groups(
            std::mem::take(&mut cluster.nodes),
            &cluster.hosts,
            3,
        )
        .await;
        if work.is_err() || close.is_err() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained outbox proof disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work?;
        close?;
        eprintln!("outbox_work_proof_configuration minimum={minimum:?} materialized=true real_rustfs={real_rustfs} passed");
    }
    Ok(())
}
