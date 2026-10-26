// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared immutable/private file admission and actual named-file lifetime.
#![allow(clippy::disallowed_methods)]
#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

use bounded_files::BoundedFiles;
use cellar_ltx::{
    internal::{
        CacheLimits, ExtentId, ExtentManifest, ExtentWorkspace, Geometry, ImageBinding,
        NodeExtentLimits, NodeExtentStore, StoreError, WorkspaceBounds, WorkspaceOptions,
        EXTENT_BYTES,
    },
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem, FileSystem,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn bounds() -> WorkspaceBounds {
    WorkspaceBounds {
        image_bytes: 1024 * 1024,
        wal_bytes: 1024 * 1024,
        capture_bytes: 1024 * 1024,
    }
}

fn files(directory: &std::path::Path) -> Arc<BoundedFiles> {
    Arc::new(BoundedFiles {
        directory: directory.to_path_buf(),
        metrics: Arc::default(),
    })
}

#[test]
fn immutable_objects_and_workspaces_share_admission_before_source_or_directory_io() {
    let directory = tempfile::tempdir().unwrap();
    let bounds = bounds();
    let charge = bounds.reservation().unwrap();
    let store = NodeExtentStore::open(
        directory.path().join("store"),
        Arc::new(DirectFileSystem),
        NodeExtentLimits {
            max_bytes: charge,
            max_workspaces: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let workspace = store.create_workspace([1; 32], bounds).unwrap();
    assert_eq!(store.usage().unwrap().workspace_bytes, charge);
    assert!(matches!(
        store.create_workspace([1; 32], bounds),
        Err(StoreError::Busy)
    ));
    assert!(matches!(
        store.create_workspace([2; 32], bounds),
        Err(StoreError::Limit)
    ));
    assert_eq!(
        std::fs::read_dir(directory.path().join("store"))
            .unwrap()
            .count(),
        2
    );
    let calls = AtomicUsize::new(0);
    assert!(matches!(
        store.put_extent(ExtentId::of(b"data").unwrap(), |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(b"data".to_vec())
        }),
        Err(StoreError::Limit)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let held = workspace.clone();
    let path = workspace.directory().to_path_buf();
    drop(workspace);
    assert!(path.exists());
    assert_eq!(store.usage().unwrap().bytes, charge);
    drop(held);
    assert!(!path.exists());
    assert_eq!(store.usage().unwrap().bytes, 0);
    let next = store.create_workspace([2; 32], bounds).unwrap();
    assert!(store.owns_workspace(&next));
    let other = NodeExtentStore::open(
        directory.path().join("other"),
        Arc::new(DirectFileSystem),
        Default::default(),
    )
    .unwrap();
    assert!(!other.owns_workspace(&next));
}

#[test]
fn failed_cleanup_or_deletion_sync_retains_the_whole_charge_until_reopen() {
    for fault in 0..3 {
        let directory = tempfile::tempdir().unwrap();
        let files = files(directory.path());
        let path = directory.path().join("store");
        let store = NodeExtentStore::open(path.clone(), files.clone(), Default::default()).unwrap();
        let workspace = store.create_workspace([1; 32], bounds()).unwrap();
        std::fs::write(workspace.directory().join("working.sqlite"), b"private").unwrap();
        match fault {
            0 => files.metrics.fail_remove.store(true, Ordering::Release),
            1 => files.metrics.fail_dir_sync.store(true, Ordering::Release),
            _ => files
                .metrics
                .fail_dir_remove_after
                .store(true, Ordering::Release),
        }
        drop(workspace);
        let usage = store.usage().unwrap();
        assert!(usage.quarantined);
        assert_eq!(usage.workspace_bytes, bounds().reservation().unwrap());
        assert!(matches!(
            store.create_workspace([2; 32], bounds()),
            Err(StoreError::Quarantined)
        ));
        drop(store);
        let recovered = NodeExtentStore::open(path, files, Default::default()).unwrap();
        assert_eq!(recovered.usage().unwrap().bytes, 0);
        assert_eq!(recovered.usage().unwrap().workspaces, 0);
        assert!(!recovered.usage().unwrap().quarantined);
    }
}

#[test]
fn uncertain_directory_creation_remains_charged_and_restart_discards_it() {
    let directory = tempfile::tempdir().unwrap();
    let files = files(directory.path());
    let path = directory.path().join("store");
    let store = NodeExtentStore::open(path.clone(), files.clone(), Default::default()).unwrap();
    files.metrics.fail_dir_sync.store(true, Ordering::Release);
    assert!(store.create_workspace([1; 32], bounds()).is_err());
    assert!(store.usage().unwrap().quarantined);
    assert_eq!(
        store.usage().unwrap().workspace_bytes,
        bounds().reservation().unwrap()
    );
    drop(store);
    let recovered = NodeExtentStore::open(path, files, Default::default()).unwrap();
    assert_eq!(recovered.usage().unwrap().workspaces, 0);
    assert_eq!(recovered.usage().unwrap().bytes, 0);
}

#[test]
fn open_sqlite_files_keep_the_directory_and_node_lock_after_workspace_drop() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let store = NodeExtentStore::open(path.clone(), Arc::new(DirectFileSystem), Default::default())
        .unwrap();
    let empty = ExtentManifest::new(
        ImageBinding {
            history: [1; 32],
            schema: [2; 32],
            epoch: 1,
            txid: 0,
            checksum: 0,
        },
        Geometry::new(0, 0).unwrap(),
        &[],
    )
    .unwrap();
    let base = Arc::new(
        store
            .seal_image_root(empty, &mut vec![0; EXTENT_BYTES as usize])
            .unwrap(),
    );
    let workspace = store.create_workspace([1; 32], bounds()).unwrap();
    let private = workspace.directory().to_path_buf();
    let database = private.join("working.sqlite");
    let cache = directory.path().join("cache");
    DirectFileSystem.create_dir_all(&cache).unwrap();
    let vfs = ExtentWorkspace::new(
        base,
        WorkspaceOptions {
            path: database.clone(),
            sqlite_vfs: None,
            filesystem: Arc::new(DirectFileSystem),
            max_image_bytes: bounds().image_bytes,
            max_wal_bytes: bounds().wal_bytes,
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
            retention: Some(workspace.retention()),
        },
    )
    .unwrap();
    let db = rusqlite::Connection::open_with_flags_and_vfs(
        &database,
        rusqlite::OpenFlags::default(),
        vfs.vfs_name(),
    )
    .unwrap();
    db.execute_batch(
        "PRAGMA journal_mode=WAL;CREATE TABLE counter(value);INSERT INTO counter VALUES(7)",
    )
    .unwrap();
    drop(workspace);
    drop(vfs);
    assert_eq!(store.usage().unwrap().workspaces, 1);
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        7
    );
    drop(store);
    assert!(
        NodeExtentStore::open(path.clone(), Arc::new(DirectFileSystem), Default::default())
            .is_err()
    );
    drop(db);
    assert!(!private.exists());
    let recovered =
        NodeExtentStore::open(path, Arc::new(DirectFileSystem), Default::default()).unwrap();
    assert_eq!(recovered.usage().unwrap().workspaces, 0);
}

#[test]
fn creation_handle_closes_before_the_last_directory_pin_can_refund_storage() {
    let directory = tempfile::tempdir().unwrap();
    let files = files(directory.path());
    files
        .metrics
        .refuse_dir_remove_with_handles
        .store(true, Ordering::Release);
    let store = NodeExtentStore::open(
        directory.path().join("store"),
        files.clone(),
        Default::default(),
    )
    .unwrap();
    let empty = ExtentManifest::new(
        ImageBinding {
            history: [1; 32],
            schema: [2; 32],
            epoch: 1,
            txid: 0,
            checksum: 0,
        },
        Geometry::new(0, 0).unwrap(),
        &[],
    )
    .unwrap();
    let base = Arc::new(
        store
            .seal_image_root(empty, &mut vec![0; EXTENT_BYTES as usize])
            .unwrap(),
    );
    let workspace = store.create_workspace([1; 32], bounds()).unwrap();
    let path = workspace.directory().to_path_buf();
    let vfs = ExtentWorkspace::new(
        base,
        WorkspaceOptions {
            path: path.join("working.sqlite"),
            sqlite_vfs: None,
            filesystem: files.clone(),
            max_image_bytes: bounds().image_bytes,
            max_wal_bytes: bounds().wal_bytes,
            cache_directory: directory.path().join("cache"),
            scratch: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * EXTENT_BYTES,
                max_files: 2,
            })
            .unwrap(),
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
            retention: Some(workspace.retention()),
        },
    )
    .unwrap();
    // No SQLite connection holds another pin: this directly exercises the
    // creation handle and registration's own drop order.
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 1);
    drop(workspace);
    drop(vfs);
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
    assert!(!path.exists());
    let usage = store.usage().unwrap();
    assert!(!usage.quarantined);
    assert_eq!(usage.workspaces, 0);
}

#[test]
#[ignore = "abrupt private-directory owner exit, invoked by the parent"]
fn workspace_crash_child() {
    let path = std::path::PathBuf::from(std::env::var_os("CELLAR_WORKSPACE_CRASH_PATH").unwrap());
    let store =
        NodeExtentStore::open(path, Arc::new(DirectFileSystem), Default::default()).unwrap();
    store
        .put_extent(ExtentId::of(b"data").unwrap(), |offset, count| {
            Ok(b"data"[offset as usize..offset as usize + count].to_vec())
        })
        .unwrap();
    let workspace = store.create_workspace([1; 32], bounds()).unwrap();
    std::fs::write(
        workspace.directory().join("working.sqlite"),
        b"private crash state",
    )
    .unwrap();
    std::process::exit(77);
}

#[test]
fn process_exit_discards_private_files_without_skipping_immutable_inventory() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "workspace_crash_child",
            "--ignored",
            "--exact",
            "--test-threads=1",
        ])
        .env("CELLAR_WORKSPACE_CRASH_PATH", &path)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(77),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let store = NodeExtentStore::open(path.clone(), Arc::new(DirectFileSystem), Default::default())
        .unwrap();
    let usage = store.usage().unwrap();
    assert_eq!(usage.objects, 1);
    assert_eq!(usage.bytes, 4);
    assert_eq!(usage.workspaces, 0);
    assert_eq!(std::fs::read_dir(path).unwrap().count(), 2);
}

#[cfg(unix)]
#[test]
fn restart_refuses_symlinked_workspace_names_without_removing_the_target() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store");
    let store = NodeExtentStore::open(path.clone(), Arc::new(DirectFileSystem), Default::default())
        .unwrap();
    let workspace = store.create_workspace([1; 32], bounds()).unwrap();
    let private = workspace.directory().to_path_buf();
    drop(workspace);
    drop(store);
    let target = directory.path().join("outside");
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("keep"), b"keep").unwrap();
    std::os::unix::fs::symlink(&target, &private).unwrap();
    assert!(matches!(
        NodeExtentStore::open(path, Arc::new(DirectFileSystem), Default::default()),
        Err(StoreError::Layout)
    ));
    assert_eq!(std::fs::read(target.join("keep")).unwrap(), b"keep");
}
