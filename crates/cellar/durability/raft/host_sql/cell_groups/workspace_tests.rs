// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private SQL directory creation is separate from image persistence and release.

use super::*;
use cellar_logic::consensus::{ClusterId, IncarnationId, NamespaceId};
use cellar_ltx::{
    internal::{CacheLimits, NodeExtentLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock,
    HostMetadata,
};
use std::{io, path::Path, sync::atomic::AtomicUsize};

#[derive(Default)]
struct Files {
    syncs: AtomicUsize,
}

impl FileSystem for Files {
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

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::AcqRel);
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

#[test]
#[allow(clippy::disallowed_methods)]
fn sql_scratch_creation_omits_durability_but_retains_physical_release() -> anyhow::Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained SQL workspace disk: {}", root.path().display());
    let files = Arc::new(Files::default());
    let limits = ExecutionLimits::default();
    let charge = RootPrivateOptions::workspace_bounds(limits)?.reservation()?;
    let images = NodeExtentStore::open(
        root.path().join("store"),
        files.clone(),
        NodeExtentLimits {
            max_bytes: charge,
            max_workspaces: 1,
            ..Default::default()
        },
    )?;
    let options = CellSqlRootOptions {
        host: cellar_ltx::LtxHost::default().with_filesystem(files.clone()),
        sqlite_vfs: None,
        images: images.clone(),
        query_services: cellar_ltx::generation::ReadServices {
            now_unix_millis: Arc::new(|| 0),
            fill_random: Arc::new(|output| output.fill(0)),
        },
        application_digest: [4; 32],
        limits,
        cache_directory: root.path().join("cache"),
        cache_pool: ScratchPool::new(ScratchLimits {
            max_bytes: 2 * EXTENT_BYTES,
            max_files: 2,
        })?,
        cache: CacheLimits {
            max_cached_bytes: EXTENT_BYTES,
            max_cached_extents: 1,
        },
    };
    let cell = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: cellar_logic::cell_group::CellId::new("Counter:scratch")?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let before = files.syncs.load(Ordering::Acquire);
    let prepared = options.clone().prepare(&cell)?;
    let created = files.syncs.load(Ordering::Acquire) - before;
    let path = prepared.workspace.directory().to_path_buf();
    assert!(path.is_dir());
    assert_eq!(images.usage()?.workspace_bytes, charge);
    assert!(matches!(
        options.prepare(&cell),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    let retained = prepared.workspace.retention();
    drop(prepared);
    assert!(path.is_dir());
    assert_eq!(images.usage()?.workspace_bytes, charge);
    assert_eq!(files.syncs.load(Ordering::Acquire) - before, created);
    drop(retained);
    assert!(!path.exists());
    assert_eq!(images.usage()?.workspaces, 0);
    assert_eq!(images.usage()?.workspace_bytes, 0);
    assert_eq!(
        (created, files.syncs.load(Ordering::Acquire) - before),
        (0, 1),
        "disposable SQL workspace added creation durability work; final deletion must still sync"
    );
    Ok(())
}
