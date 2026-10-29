// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual SQLite writable residency and independent main/WAL observations.
//! These data-plane tests do not qualify public SDK selection or peer recovery.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    internal::{
        db::{observe_extents, ExtentObservation},
        CacheLimits, ExtentId, ExtentManifest, ExtentWorkspace, Geometry, ImageBinding,
        NodeExtentStore, SealedImageRoot, StoreError, WorkspaceOptions, EXTENT_BYTES,
    },
    stream_snapshot::{ScratchLimits, ScratchPool, MAX_IMAGE_BYTES},
    CheckpointMode, Db, DirectFileSystem, FileSystem, TXID,
};
use rusqlite::{ffi, Connection, OpenFlags, MAIN_DB};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
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

fn manifest(bytes: &[u8], page_size: u32, txid: u64) -> ExtentManifest {
    ExtentManifest::new(
        binding(txid),
        Geometry::new(page_size, bytes.len() as u64).unwrap(),
        &bytes
            .chunks(EXTENT_BYTES as usize)
            .map(|chunk| ExtentId::of(chunk).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

struct Fixture {
    directory: tempfile::TempDir,
    base: Arc<SealedImageRoot>,
    store: NodeExtentStore,
    bytes: Vec<u8>,
    page_size: u32,
}

impl Fixture {
    fn new(page_size: u32, rows: u8) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source.sqlite");
        let connection = Connection::open(&source).unwrap();
        connection
            .pragma_update(None, "page_size", page_size)
            .unwrap();
        connection.execute_batch("PRAGMA auto_vacuum=FULL; VACUUM; CREATE TABLE counter(value); INSERT INTO counter VALUES(7); CREATE TABLE payload(id INTEGER PRIMARY KEY, value BLOB); BEGIN;").unwrap();
        for row in 1..=rows {
            connection
                .execute(
                    "INSERT INTO payload VALUES(?1,?2)",
                    rusqlite::params![row, vec![row; 1024 * 1024]],
                )
                .unwrap();
        }
        connection.execute_batch("COMMIT").unwrap();
        drop(connection);
        let bytes = std::fs::read(source).unwrap();
        let manifest = manifest(&bytes, page_size, 1);
        let store = NodeExtentStore::open(
            directory.path().join("extents"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )
        .unwrap();
        for (index, chunk) in bytes.chunks(EXTENT_BYTES as usize).enumerate() {
            store
                .put_extent(manifest.extent(index as u32).unwrap(), |offset, length| {
                    Ok(chunk[offset as usize..offset as usize + length].to_vec())
                })
                .unwrap();
        }
        let base = Arc::new(
            store
                .seal_image_root(manifest, &mut vec![0; EXTENT_BYTES as usize])
                .unwrap(),
        );
        Self {
            directory,
            base,
            store,
            bytes,
            page_size,
        }
    }

    fn options(&self, name: &str, quota: u64) -> WorkspaceOptions {
        let cache = self.directory.path().join(format!("{name}-cache"));
        DirectFileSystem.create_dir_all(&cache).unwrap();
        WorkspaceOptions {
            path: self.directory.path().join(format!("{name}.sqlite")),
            sqlite_vfs: None,
            filesystem: Arc::new(DirectFileSystem),
            max_image_bytes: quota,
            max_wal_bytes: 64 * 1024 * 1024,
            retention: None,
            cache_directory: cache,
            scratch: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * EXTENT_BYTES,
                max_files: 2,
            })
            .unwrap(),
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        }
    }

    fn workspace(&self, name: &str) -> (ExtentWorkspace, PathBuf) {
        let options = self.options(name, MAX_IMAGE_BYTES);
        let path = options.path.clone();
        (
            ExtentWorkspace::new(self.base.clone(), options).unwrap(),
            path,
        )
    }
}

fn connection(workspace: &ExtentWorkspace, path: &Path) -> Connection {
    Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        workspace.vfs_name(),
    )
    .unwrap()
}

fn value(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT value FROM counter", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn hot_open_and_partial_main_writes_retain_a_bounded_extent_workspace() {
    for (page_size, rows) in [(512, 8), (4096, 8), (4096, 40), (65536, 8)] {
        let fixture = Fixture::new(page_size, rows);
        let (workspace, path) = fixture.workspace("hot");
        assert_eq!(workspace.stats().unwrap().bytes_faulted, 0);
        assert_eq!(
            workspace.stats().unwrap().extent_slots as u64,
            MAX_IMAGE_BYTES.div_ceil(EXTENT_BYTES)
        );
        let reader = connection(&workspace, &path);
        assert_eq!(value(&reader), 7);
        assert_eq!(workspace.stats().unwrap().hydrated_extents, 1);
        assert_eq!(workspace.stats().unwrap().bytes_faulted, EXTENT_BYTES);
        assert_eq!(
            std::fs::read(fixture.directory.path().join("source.sqlite")).unwrap(),
            fixture.bytes
        );
        let name = workspace.vfs_name().to_owned();
        drop(workspace);
        // Existing SQLite files retain the retired VFS and exact immutable base.
        assert_eq!(value(&reader), 7);
        assert!(Connection::open_with_flags_and_vfs(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE,
            name.as_str()
        )
        .is_err());
        drop(reader);
    }
}

fn observe(
    db: &Db,
    writer: &Connection,
    previous: Option<&ExtentObservation>,
    txid: u64,
    page_size: u32,
) -> ExtentObservation {
    let pages: u64 = writer
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    let observed = observe_extents(
        db,
        previous,
        binding(txid),
        pages * u64::from(page_size),
        64 * 1024 * 1024,
    )
    .unwrap();
    let expected = manifest(&writer.serialize(MAIN_DB).unwrap(), page_size, txid);
    assert_eq!(
        observed.manifest().encode().unwrap(),
        expected.encode().unwrap()
    );
    assert!(observed.work().reused_base);
    observed
}

#[test]
fn opening_small_commits_and_checkpoint_resets_reuse_the_verified_main_baseline() {
    for page_size in [512, 4096, 65536] {
        // Opening control tables can relocate auto-vacuum pages into a third
        // extent. Keep at least five extents so this performance assertion
        // also requires an actually unchanged subtree across the checkpoint.
        let fixture = Fixture::new(page_size, 16);
        assert!(fixture.base.manifest().geometry().extent_count() >= 5);
        let (workspace, path) = fixture.workspace("writer");
        let mut db =
            Db::open_disposable_with_host(&path, Default::default(), Some(workspace.vfs_name()))
                .unwrap();
        db.min_checkpoint_page_n = u32::MAX;
        db.truncate_page_n = 0;
        db.checkpoint_interval = Duration::ZERO;
        db.seed_continuation(TXID(1), (fixture.bytes.len() / page_size as usize) as u32)
            .unwrap();
        let writer = connection(&workspace, &path);
        writer
            .execute_batch("PRAGMA synchronous=OFF; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        let mut previous = observe(&db, &writer, None, 2, page_size);
        assert!(previous.work().image_bytes_read <= 3 * EXTENT_BYTES);
        for (index, mode) in [
            CheckpointMode::Passive,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ]
        .into_iter()
        .enumerate()
        {
            writer
                .execute_batch("UPDATE counter SET value=value+1")
                .unwrap();
            let changed = observe(
                &db,
                &writer,
                Some(&previous),
                3 + 2 * index as u64,
                page_size,
            );
            assert_eq!(changed.work().image_bytes_read, EXTENT_BYTES);
            db.checkpoint(mode).unwrap();
            writer
                .execute_batch("UPDATE counter SET value=value+1")
                .unwrap();
            previous = observe(
                &db,
                &writer,
                Some(&changed),
                4 + 2 * index as u64,
                page_size,
            );
            assert!(
                previous.work().image_bytes_read < previous.manifest().geometry().length(),
                "page_size={page_size} mode={mode:?} work={:?} geometry={:?} main={:?}",
                previous.work(),
                previous.manifest().geometry(),
                workspace.stats().unwrap()
            );
        }
        // Actual WAL readers keep their own snapshots through later writes.
        let reader = connection(&workspace, &path);
        reader.execute_batch("BEGIN").unwrap();
        let before = value(&reader);
        writer
            .execute_batch("UPDATE counter SET value=value+1")
            .unwrap();
        assert_eq!(value(&reader), before);
        reader.execute_batch("ROLLBACK").unwrap();
        assert_eq!(value(&reader), before + 1);
        drop(reader);
        writer
            .execute_batch("DELETE FROM payload WHERE id>1")
            .unwrap();
        let shrunk = observe(&db, &writer, Some(&previous), 20, page_size);
        db.checkpoint(CheckpointMode::Truncate).unwrap();
        writer
            .execute("INSERT INTO payload VALUES(42,zeroblob(6291456))", [])
            .unwrap();
        drop(observe(&db, &writer, Some(&shrunk), 21, page_size));
        let integrity: String = writer
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        assert_eq!(
            std::fs::read(fixture.directory.path().join("source.sqlite")).unwrap(),
            fixture.bytes
        );
        drop(writer);
        db.close().unwrap();
    }
}

fn main_file(connection: &Connection) -> *mut ffi::sqlite3_file {
    let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
    // SAFETY: this test exclusively owns the live connection; it retains that
    // owner while exercising raw VFS methods, without concurrent pager work.
    let result = unsafe {
        ffi::sqlite3_file_control(
            connection.handle(),
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            (&mut file as *mut *mut ffi::sqlite3_file).cast(),
        )
    };
    assert_eq!(result, ffi::SQLITE_OK);
    assert!(!file.is_null());
    file
}

#[test]
fn raw_partial_writes_truncation_regrowth_and_quota_never_resurrect_base_bytes() {
    let fixture = Fixture::new(4096, 8);
    let options = fixture.options("raw", fixture.bytes.len() as u64);
    let path = options.path.clone();
    let workspace = ExtentWorkspace::new(fixture.base.clone(), options).unwrap();
    let reader = connection(&workspace, &path);
    let file = main_file(&reader);
    let offset = EXTENT_BYTES + 17;
    let patch = [42_u8];
    let mut actual = [0; 64];
    // SAFETY: each SQLite method receives a live file and correctly sized
    // buffers. No SQL is executed after these intentional physical mutations.
    unsafe {
        let methods = &*(*file).pMethods;
        assert_eq!(
            methods.xWrite.unwrap()(file, patch.as_ptr().cast(), 1, offset as i64),
            ffi::SQLITE_OK
        );
        assert_eq!(
            methods.xRead.unwrap()(file, actual.as_mut_ptr().cast(), 64, offset as i64 - 16),
            ffi::SQLITE_OK
        );
        let mut expected = fixture.bytes[offset as usize - 16..offset as usize + 48].to_vec();
        expected[16] = 42;
        assert_eq!(actual.as_slice(), expected);
        let cut = offset + 7;
        assert_eq!(methods.xTruncate.unwrap()(file, cut as i64), ffi::SQLITE_OK);
        assert_eq!(
            methods.xTruncate.unwrap()(file, fixture.bytes.len() as i64),
            ffi::SQLITE_OK
        );
        assert_eq!(
            methods.xRead.unwrap()(file, actual.as_mut_ptr().cast(), 64, cut as i64),
            ffi::SQLITE_OK
        );
        assert_eq!(actual, [0; 64]);
        let sequence = workspace.stats().unwrap().main_sequence;
        assert_eq!(
            methods.xWrite.unwrap()(file, patch.as_ptr().cast(), 1, fixture.bytes.len() as i64),
            ffi::SQLITE_FULL
        );
        assert_eq!(
            methods.xTruncate.unwrap()(file, fixture.bytes.len() as i64 + 1),
            ffi::SQLITE_FULL
        );
        assert_eq!(workspace.stats().unwrap().main_sequence, sequence);
    }
    drop(reader);
    assert!(
        ExtentWorkspace::new(
            fixture.base.clone(),
            fixture.options("raw", MAX_IMAGE_BYTES)
        )
        .is_err(),
        "existing disposable files must never be reused"
    );
    let too_small = fixture.options("too-small", 512);
    let refused_path = too_small.path.clone();
    assert!(ExtentWorkspace::new(fixture.base.clone(), too_small).is_err());
    assert!(!refused_path.exists());
    assert_eq!(fixture.page_size, 4096);
}

#[test]
fn a_corrupt_base_refuses_the_fault_without_caching_or_zero_filling_it() {
    let fixture = Fixture::new(4096, 8);
    let id = fixture.base.manifest().extent(0).unwrap();
    let digest: String = id
        .bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let object = fixture
        .directory
        .path()
        .join("extents")
        .join(format!("e-{:08x}-{digest}", id.length()));
    let original = std::fs::read(&object).unwrap();
    let mut corrupt = original.clone();
    corrupt[65536] ^= 1;
    std::fs::write(&object, corrupt).unwrap();
    let (workspace, path) = fixture.workspace("corrupt");
    if let Ok(reader) = Connection::open_with_flags_and_vfs(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE,
        workspace.vfs_name(),
    ) {
        assert!(reader
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .is_err());
    }
    assert_eq!(workspace.stats().unwrap().bytes_faulted, 0);
    assert!(
        matches!(
            fixture.store.seal_root(fixture.base.manifest().clone()),
            Err(StoreError::Unverified)
        ),
        "failed source verification retained whole-extent admission"
    );
    std::fs::write(&object, original).unwrap();
    let reader = connection(&workspace, &path);
    assert_eq!(value(&reader), 7);
    assert_eq!(workspace.stats().unwrap().bytes_faulted, EXTENT_BYTES);
}

#[cfg(unix)]
#[test]
#[ignore = "isolated file-size-limit child, invoked by failed_main_write_poisoning_is_process_isolated"]
fn workspace_write_failure_child() {
    assert!(std::env::var_os("CELLAR_EXTENT_WORKSPACE_WRITE_CHILD").is_some());
    let fixture = Fixture::new(4096, 8);
    let (workspace, path) = fixture.workspace("write-failure");
    let reader = connection(&workspace, &path);
    let file = main_file(&reader);
    let mut original = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: this ignored test runs only in its own joined child process.
    // Both output and subsequent rlimit inputs are live, correctly sized values.
    let original = unsafe {
        assert_eq!(
            libc::getrlimit(libc::RLIMIT_FSIZE, original.as_mut_ptr()),
            0
        );
        original.assume_init()
    };
    let limited = libc::rlimit {
        rlim_cur: fixture.bytes.len() as libc::rlim_t,
        rlim_max: original.rlim_max,
    };
    let patch = [42_u8; 32];
    // SAFETY: signals and this soft limit affect only this disposable child.
    // Keep the connection and patch live while SQLite performs the real write.
    // A crossing write may partially land; either failure must poison the owner.
    let result = unsafe {
        let signal = libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
        assert_ne!(signal, libc::SIG_ERR);
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &limited), 0);
        let result = ((*(*file).pMethods).xWrite.unwrap())(
            file,
            patch.as_ptr().cast(),
            patch.len() as i32,
            fixture.bytes.len() as i64 - 8,
        );
        assert_eq!(libc::setrlimit(libc::RLIMIT_FSIZE, &original), 0);
        libc::signal(libc::SIGXFSZ, signal);
        result
    };
    assert_ne!(result, ffi::SQLITE_OK);
    assert!(
        workspace.stats().is_err(),
        "failed physical writes cannot retain an observable working cut"
    );
    let mut header = [0; 100];
    // SAFETY: the live file receives the exact output buffer. A poisoned owner
    // must refuse even after restoring process limits; it cannot resume in place.
    let refused =
        unsafe { ((*(*file).pMethods).xRead.unwrap())(file, header.as_mut_ptr().cast(), 100, 0) };
    assert_ne!(refused, ffi::SQLITE_OK);
    drop(reader);
    drop(workspace);
    let (rebuilt, path) = fixture.workspace("rebuilt");
    assert_eq!(value(&connection(&rebuilt, &path)), 7);
}

#[cfg(unix)]
#[test]
fn failed_main_write_poisoning_is_process_isolated() {
    struct ChildOwner(Option<std::process::Child>);

    impl Drop for ChildOwner {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "workspace_write_failure_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("CELLAR_EXTENT_WORKSPACE_WRITE_CHILD", "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut owner = ChildOwner(Some(child));
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if owner.0.as_mut().unwrap().try_wait().unwrap().is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "file-size-limit child did not finish"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = owner.0.take().unwrap().wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "child failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn empty_bootstrap_tracks_sqlite_and_bounds_only_its_own_wal_before_capture() {
    let directory = tempfile::tempdir().unwrap();
    let store = NodeExtentStore::open(
        directory.path().join("extents"),
        Arc::new(DirectFileSystem),
        Default::default(),
    )
    .unwrap();
    let empty = ExtentManifest::new(binding(0), Geometry::new(0, 0).unwrap(), &[]).unwrap();
    let base = Arc::new(
        store
            .seal_image_root(empty, &mut vec![0; EXTENT_BYTES as usize])
            .unwrap(),
    );
    let fixture = Fixture {
        directory,
        base,
        store,
        bytes: Vec::new(),
        page_size: 4096,
    };
    let mut options = fixture.options("empty", 8 * 1024 * 1024);
    options.max_wal_bytes = 256 * 1024;
    let path = options.path.clone();
    let workspace = ExtentWorkspace::new(fixture.base.clone(), options).unwrap();
    let db = Db::open_disposable_with_host(&path, Default::default(), Some(workspace.vfs_name()))
        .unwrap();
    let writer = connection(&workspace, &path);
    writer.execute_batch("PRAGMA cache_size=16; PRAGMA wal_autocheckpoint=0; CREATE TABLE counter(value); INSERT INTO counter VALUES(7); CREATE TABLE big(value);").unwrap();
    let before = observe(&db, &writer, None, 1, 4096);
    assert_eq!(workspace.stats().unwrap().bytes_faulted, 0);
    let error = writer
        .execute_batch("BEGIN; INSERT INTO big VALUES(zeroblob(1048576)); COMMIT;")
        .unwrap_err();
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(error, _) if error.code == rusqlite::ErrorCode::DiskFull)
    );
    if !writer.is_autocommit() {
        writer.execute_batch("ROLLBACK").unwrap();
    }
    let wal = fixture.directory.path().join("empty.sqlite-wal");
    assert!(std::fs::metadata(wal).unwrap().len() <= 256 * 1024);
    assert_eq!(value(&writer), 7);
    let after = observe(&db, &writer, Some(&before), 1, 4096);
    assert_eq!(after.manifest().root(), before.manifest().root());
    // A foreign database opened through this name is still an ordinary file;
    // it must inherit neither the cell's source nor the cell's WAL quota.
    let foreign = fixture.directory.path().join("foreign.sqlite");
    let other = Connection::open_with_flags_and_vfs(
        &foreign,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        workspace.vfs_name(),
    )
    .unwrap();
    other.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE payload(value); INSERT INTO payload VALUES(zeroblob(1048576));").unwrap();
    assert_eq!(
        other
            .query_row("SELECT length(value) FROM payload", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1048576
    );
    drop(other);
    drop(writer);
    db.close().unwrap();
}

#[test]
fn stale_sidecars_refuse_before_creating_a_new_main_file() {
    let fixture = Fixture::new(4096, 8);
    for suffix in ["-wal", "-shm", "-journal"] {
        let options = fixture.options("stale", MAX_IMAGE_BYTES);
        let path = options.path.clone();
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        let sidecar = PathBuf::from(sidecar);
        std::fs::write(&sidecar, b"untrusted previous workspace").unwrap();
        assert!(ExtentWorkspace::new(fixture.base.clone(), options).is_err());
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(&sidecar).unwrap(),
            b"untrusted previous workspace"
        );
        std::fs::remove_file(sidecar).unwrap();
    }
}
