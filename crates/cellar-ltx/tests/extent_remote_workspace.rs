// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Writable demand loading without a complete local root. Sources here model
//! retained remote bytes; native publication and activation need separate proofs.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    internal::{
        db::{observe_extents, ExtentObservation},
        CacheLimits, ExtentDownload, ExtentId, ExtentManifest, ExtentSource, ExtentWorkspace,
        Geometry, ImageBinding, WorkspaceOptions, EXTENT_BYTES,
    },
    ltx::{decode_file_bounded, DecodeLimits},
    stream_snapshot::{Error, ScratchLimits, ScratchPool, ScratchUsage, MAX_IMAGE_BYTES},
    CheckpointMode, Db, DirectFileSystem, FileSystem, TXID,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, MAIN_DB};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

fn binding(txid: u64) -> ImageBinding {
    ImageBinding {
        history: [1; 32],
        schema: [2; 32],
        epoch: 3,
        txid,
        checksum: 0,
    }
}

struct Source {
    path: PathBuf,
    locations: Vec<(ExtentId, u64)>,
    requests: Mutex<Vec<ExtentId>>,
    mode: AtomicUsize,
}

impl ExtentSource for Source {
    fn read_into(&self, id: ExtentId, download: &mut ExtentDownload) -> Result<(), Error> {
        self.requests.lock().unwrap().push(id);
        let mode = self.mode.load(Ordering::Acquire);
        if mode == 1 {
            return Err(io::ErrorKind::NotFound.into());
        }
        let position = self
            .locations
            .iter()
            .find(|(candidate, _)| *candidate == id)
            .map(|(_, offset)| *offset)
            .ok_or(io::ErrorKind::NotFound)?;
        let mut file = DirectFileSystem.open(&self.path)?;
        let mut offset = 0;
        while offset < u64::from(id.length()) {
            let length = (u64::from(id.length()) - offset).min(65536) as usize;
            let mut bytes = file.read_exact_at(position + offset, length)?;
            if mode == 2 && offset == 0 {
                bytes[0] ^= 1;
            }
            download.append(offset, &bytes)?;
            offset += length as u64;
            if mode == 3 {
                return Err(io::Error::other("source interrupted after a partial append").into());
            }
        }
        Ok(())
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    source: Arc<Source>,
    manifest: ExtentManifest,
    scratch: ScratchPool,
}

impl Fixture {
    fn new(rows: u32) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.sqlite");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
            CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(7);
            CREATE TABLE receipts(command TEXT PRIMARY KEY, value INTEGER NOT NULL) WITHOUT ROWID;
            CREATE TABLE payload(id INTEGER PRIMARY KEY, value BLOB); BEGIN;",
        )
        .unwrap();
        for row in 1..=rows {
            db.execute(
                "INSERT INTO payload VALUES(?1,?2)",
                rusqlite::params![row, vec![(row % 251) as u8; 1024 * 1024]],
            )
            .unwrap();
        }
        db.execute_batch("COMMIT").unwrap();
        drop(db);
        Self::from_path(directory, path)
    }

    fn from_path(directory: tempfile::TempDir, path: PathBuf) -> Self {
        // Source preparation is separate from destination demand loading. Never
        // put these bytes in a NodeExtentStore or grant a complete-local seal.
        let mut file = DirectFileSystem.open(&path).unwrap();
        let header = file.read_exact_at(0, 100).unwrap();
        let encoded = u16::from_be_bytes(header[16..18].try_into().unwrap());
        let page_size = if encoded == 1 {
            65536
        } else {
            u32::from(encoded)
        };
        let geometry = Geometry::new(page_size, file.file_len().unwrap()).unwrap();
        let locations = (0..geometry.extent_count())
            .map(|index| {
                let offset = u64::from(index) * EXTENT_BYTES;
                let bytes = file
                    .read_exact_at(offset, geometry.extent_length(index).unwrap() as usize)
                    .unwrap();
                (ExtentId::of(&bytes).unwrap(), offset)
            })
            .collect::<Vec<_>>();
        let manifest = ExtentManifest::new(
            binding(1),
            geometry,
            &locations.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        )
        .unwrap();
        Self {
            directory,
            manifest,
            source: Arc::new(Source {
                path,
                locations,
                requests: Mutex::default(),
                mode: AtomicUsize::new(0),
            }),
            scratch: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * EXTENT_BYTES,
                max_files: 2,
            })
            .unwrap(),
        }
    }

    fn workspace(&self, name: &str) -> (ExtentWorkspace, PathBuf) {
        let path = self.directory.path().join(format!("{name}.sqlite"));
        let cache_directory = self.directory.path().join(format!("{name}-cache"));
        std::fs::create_dir(&cache_directory).unwrap();
        let workspace = ExtentWorkspace::from_source(
            self.manifest.clone(),
            self.source.clone(),
            WorkspaceOptions {
                path: path.clone(),
                sqlite_vfs: None,
                filesystem: Arc::new(DirectFileSystem),
                max_image_bytes: MAX_IMAGE_BYTES,
                max_wal_bytes: 64 * 1024 * 1024,
                cache_directory,
                scratch: self.scratch.clone(),
                cache: CacheLimits {
                    max_cached_bytes: EXTENT_BYTES,
                    max_cached_extents: 1,
                },
                retention: None,
            },
        )
        .unwrap();
        (workspace, path)
    }
}

fn connect(workspace: &ExtentWorkspace, path: &Path) -> Connection {
    Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        workspace.vfs_name(),
    )
    .unwrap()
}

fn command(writer: &mut Connection, id: &str) -> i64 {
    let transaction = writer.transaction().unwrap();
    let existing = transaction
        .query_row("SELECT value FROM receipts WHERE command=?1", [id], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .unwrap();
    if let Some(value) = existing {
        return value;
    }
    transaction
        .execute("UPDATE counter SET value=value+13", [])
        .unwrap();
    let value = transaction
        .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
        .unwrap();
    transaction
        .execute(
            "INSERT INTO receipts VALUES(?1,?2)",
            rusqlite::params![id, value],
        )
        .unwrap();
    transaction.commit().unwrap();
    value
}

fn observe(
    db: &Db,
    writer: &Connection,
    previous: Option<&ExtentObservation>,
    txid: u64,
) -> ExtentObservation {
    let pages: u64 = writer
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    let observed =
        observe_extents(db, previous, binding(txid), pages * 4096, 64 * 1024 * 1024).unwrap();
    assert!(observed.work().reused_base);
    assert!(observed.work().image_bytes_read <= 3 * EXTENT_BYTES);
    observed
}

#[test]
fn remote_writer_captures_receipts_incrementally_across_checkpoint_resets() {
    for rows in [16, 256] {
        exercise_writer(&Fixture::new(rows), rows == 16);
    }
}

fn exercise_writer(fixture: &Fixture, full_oracle: bool) {
    let (workspace, path) = fixture.workspace("writer");
    assert!(fixture.source.requests.lock().unwrap().is_empty());
    let mut db =
        Db::open_disposable_with_host(&path, Default::default(), Some(workspace.vfs_name()))
            .unwrap();
    db.min_checkpoint_page_n = u32::MAX;
    db.truncate_page_n = 0;
    db.checkpoint_interval = Duration::ZERO;
    db.require_incremental_capture().unwrap();
    db.disable_disposable_close_checkpoint().unwrap();
    db.seed_continuation(
        TXID(1),
        (fixture.manifest.geometry().length() / 4096) as u32,
    )
    .unwrap();
    let mut writer = connect(&workspace, &path);
    writer
        .execute_batch("PRAGMA synchronous=OFF; PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    let mut previous = observe(&db, &writer, None, 2);
    assert!(
        fixture.source.requests.lock().unwrap().len() <= 3,
        "open/initial observation fetched cold extents"
    );
    for (index, mode) in [
        CheckpointMode::Passive,
        CheckpointMode::Restart,
        CheckpointMode::Passive,
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("command-{index}");
        let expected = 7 + 13 * (index as i64 + 1);
        assert_eq!(command(&mut writer, &id), expected);
        assert_eq!(command(&mut writer, &id), expected);
        assert!(
            fixture.source.requests.lock().unwrap().len() <= 3,
            "command {index} fetched cold extents"
        );
        let captured = db.capture_with_files(8, 1024 * 1024).unwrap();
        assert!(!captured.is_empty());
        assert!(
            fixture.source.requests.lock().unwrap().len() <= 3,
            "capture {index} fetched cold extents"
        );
        let changed = observe(&db, &writer, Some(&previous), 3 + index as u64 * 2);
        db.checkpoint(mode).unwrap();
        assert!(
            fixture.source.requests.lock().unwrap().len() <= 3,
            "checkpoint {index} fetched cold extents"
        );
        previous = observe(&db, &writer, Some(&changed), 4 + index as u64 * 2);
    }
    // Measure before the independent small-image oracle deliberately reads
    // all bytes. Large-image verification preserves the cold dependencies.
    let requests = fixture.source.requests.lock().unwrap().clone();
    assert!(requests.len() <= 3, "unexpected source work: {requests:?}");
    assert!(workspace.stats().unwrap().bytes_faulted <= 3 * EXTENT_BYTES);
    assert!(
        workspace.stats().unwrap().hydrated_extents
            < fixture.manifest.geometry().extent_count() as usize
    );
    for index in 0..fixture.manifest.geometry().extent_count() {
        let id = fixture.manifest.extent(index).unwrap();
        if !requests.contains(&id) {
            assert_eq!(previous.manifest().extent(index), Some(id));
        }
    }
    assert_eq!(
        writer
            .query_row("SELECT count(*) FROM receipts", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        3
    );
    let mut decoded_bytes = 0;
    let mut replay = full_oracle.then(|| std::fs::read(&fixture.source.path).unwrap());
    for txid in 2..=db.pos().unwrap().txid.0 {
        // Include the checkpoint's control cuts, not just command captures.
        let bytes = db.read_ltx_file(0, TXID(txid), TXID(txid)).unwrap();
        let decoded = decode_file_bounded(
            &bytes,
            DecodeLimits {
                max_image_bytes: MAX_IMAGE_BYTES,
                ..Default::default()
            },
        )
        .unwrap();
        decoded_bytes += decoded.decoded_bytes();
        assert_eq!(decoded.header().min_txid, TXID(txid));
        assert_eq!(decoded.header().max_txid, TXID(txid));
        if let Some(replay) = &mut replay {
            replay.resize(decoded.header().commit as usize * 4096, 0);
            for (number, page) in decoded.pages() {
                let offset = (*number as usize - 1) * 4096;
                replay[offset..offset + 4096].copy_from_slice(page);
            }
        }
    }
    assert!(decoded_bytes < EXTENT_BYTES);
    eprintln!("remote writer: image_bytes={} requests={} hydrated_extents={} faulted_bytes={} captured_decoded_bytes={}", fixture.manifest.geometry().length(), requests.len(), workspace.stats().unwrap().hydrated_extents, workspace.stats().unwrap().bytes_faulted, decoded_bytes);
    if let Some(replay) = replay {
        let path = fixture.directory.path().join("independent-replay.sqlite");
        std::fs::write(&path, replay).unwrap();
        let restored =
            Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            restored
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            46
        );
        assert_eq!(
            restored
                .query_row("SELECT count(*) FROM receipts", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            restored
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }
    if full_oracle {
        let bytes = writer.serialize(MAIN_DB).unwrap();
        let expected = ExtentManifest::new(
            previous.manifest().binding(),
            Geometry::new(4096, bytes.len() as u64).unwrap(),
            &bytes
                .chunks(EXTENT_BYTES as usize)
                .map(|part| ExtentId::of(part).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(previous.manifest().root(), expected.root());
        assert_eq!(
            writer
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }
    drop(previous);
    drop(writer);
    db.close().unwrap();
    drop(workspace);
    assert_eq!(fixture.scratch.usage().unwrap(), ScratchUsage::default());
}

#[test]
fn truncate_boundary_refuses_before_fetching_cold_remote_dependencies() {
    let fixture = Fixture::new(16);
    let (workspace, path) = fixture.workspace("truncate");
    let mut db =
        Db::open_disposable_with_host(&path, Default::default(), Some(workspace.vfs_name()))
            .unwrap();
    db.min_checkpoint_page_n = u32::MAX;
    db.truncate_page_n = 0;
    db.checkpoint_interval = Duration::ZERO;
    db.disable_disposable_close_checkpoint().unwrap();
    db.require_incremental_capture().unwrap();
    db.seed_continuation(
        TXID(1),
        (fixture.manifest.geometry().length() / 4096) as u32,
    )
    .unwrap();
    let mut writer = connect(&workspace, &path);
    writer
        .execute_batch("PRAGMA synchronous=OFF; PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    assert_eq!(command(&mut writer, "before-truncate"), 20);
    db.capture_with_files(8, 1024 * 1024).unwrap();
    // Observe the command's touched extents before checkpoint copies its pages
    // to main. Hydrating that same hot set is legitimate checkpoint work.
    drop(observe(&db, &writer, None, 2));
    let requests = fixture.source.requests.lock().unwrap().len();
    assert!(requests <= 3);
    let error = db.checkpoint(CheckpointMode::Truncate).unwrap_err();
    assert!(
        matches!(error, cellar_ltx::Error::CaptureRebuildRequired { reason } if reason == "checkpoint boundary snapshot")
    );
    assert_eq!(fixture.source.requests.lock().unwrap().len(), requests);
    // Refusal can follow physical WAL changes. Drop the entire disposable
    // residency; never retry this command against its uncertain local state.
    drop(writer);
    db.close().unwrap();
    drop(workspace);
    assert_eq!(fixture.scratch.usage().unwrap(), ScratchUsage::default());
}

#[test]
fn missing_corrupt_and_interrupted_sources_refuse_without_hydrating() {
    let fixture = Fixture::new(8);
    for mode in 1..=3 {
        fixture.source.mode.store(mode, Ordering::Release);
        let (workspace, path) = fixture.workspace(&format!("fault-{mode}"));
        let connection = Connection::open_with_flags_and_vfs(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
            workspace.vfs_name(),
        );
        if let Ok(connection) = connection {
            assert!(connection
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
                .is_err());
        }
        assert_eq!(workspace.stats().unwrap().hydrated_extents, 0);
        assert_eq!(workspace.stats().unwrap().bytes_faulted, 0);
        assert_eq!(fixture.scratch.usage().unwrap(), ScratchUsage::default());
        fixture.source.mode.store(0, Ordering::Release);
        let reader = connect(&workspace, &path);
        assert_eq!(
            reader
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            7
        );
    }
}

#[test]
fn open_sqlite_handles_retain_the_source_after_workspace_retirement() {
    let fixture = Fixture::new(16);
    let (workspace, path) = fixture.workspace("retained");
    let reader = connect(&workspace, &path);
    assert_eq!(
        reader
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        7
    );
    let weak = Arc::downgrade(&fixture.source);
    let scratch = fixture.scratch.clone();
    drop(fixture.source);
    drop(workspace);
    // Fault a previously untouched remote dependency after registration and
    // the caller's source owner have gone away.
    assert_eq!(
        reader
            .query_row(
                "SELECT hex(substr(value,1,1)) FROM payload WHERE id=16",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "10"
    );
    assert!(weak.upgrade().is_some());
    drop(reader);
    assert!(weak.upgrade().is_none());
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
}

#[test]
fn empty_remote_base_retains_its_source_without_fetching() {
    let mut fixture = Fixture::new(0);
    fixture.manifest = ExtentManifest::new(binding(0), Geometry::new(0, 0).unwrap(), &[]).unwrap();
    fixture.source.mode.store(1, Ordering::Release);
    let (workspace, path) = fixture.workspace("empty");
    let writer = connect(&workspace, &path);
    writer
        .execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(7)")
        .unwrap();
    assert!(fixture.source.requests.lock().unwrap().is_empty());
    let weak = Arc::downgrade(&fixture.source);
    drop(fixture.source);
    drop(workspace);
    assert!(weak.upgrade().is_some());
    assert_eq!(
        writer
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        7
    );
    drop(writer);
    assert!(weak.upgrade().is_none());
    assert_eq!(fixture.scratch.usage().unwrap(), ScratchUsage::default());
}

#[cfg(unix)]
#[test]
#[ignore = "requires the retained decimal-10-GB campaign fixture and an admitted private work directory"]
fn decimal_ten_gb_remote_writer_fetches_only_the_hot_working_set() {
    use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt};
    let template = PathBuf::from(std::env::var_os("CELLAR_REMOTE_WRITER_TEMPLATE").unwrap());
    let work = PathBuf::from(std::env::var_os("CELLAR_REMOTE_WRITER_WORK").unwrap());
    assert!(template.is_absolute() && work.is_absolute());
    let length = MAX_IMAGE_BYTES / 4096 * 4096;
    assert_eq!(std::fs::metadata(&template).unwrap().len(), length);
    assert!(std::fs::metadata(&template)
        .unwrap()
        .permissions()
        .readonly());
    let name = std::ffi::CString::new(work.as_os_str().as_bytes()).unwrap();
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: the absolute directory is NUL terminated and the output points
    // to valid storage; assume initialization only after statvfs succeeds.
    let stats = unsafe {
        assert_eq!(libc::statvfs(name.as_ptr(), stats.as_mut_ptr()), 0);
        stats.assume_init()
    };
    assert!(
        u128::from(stats.f_bavail) * u128::from(stats.f_frsize)
            >= u128::from(2 * length + 8 * 1024 * 1024 * 1024),
        "source copy, private main and 8 GiB headroom require admission"
    );
    let directory = tempfile::Builder::new()
        .prefix("remote-writer-10gb-")
        .tempdir_in(work)
        .unwrap();
    let source = directory.path().join("source.sqlite");
    std::fs::copy(&template, &source).unwrap();
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
    // Prepare the source outside the demand-loading measurement. Free only
    // the fixture's padding rows to install the hot workload without growing
    // beyond quota. The original retained campaign image stays untouched.
    let connection = Connection::open(&source).unwrap();
    connection.execute_batch("PRAGMA journal_mode=DELETE; DELETE FROM quota_tail; CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(7); CREATE TABLE receipts(command TEXT PRIMARY KEY,value INTEGER NOT NULL) WITHOUT ROWID;").unwrap();
    drop(connection);
    assert_eq!(std::fs::metadata(&source).unwrap().len(), length);
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o400)).unwrap();
    let fixture = Fixture::from_path(directory, source);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exercise_writer(&fixture, false)
    }));
    if let Err(error) = result {
        let retained = fixture.directory.keep();
        eprintln!(
            "failed remote writer data retained at {}",
            retained.display()
        );
        std::panic::resume_unwind(error);
    }
}
