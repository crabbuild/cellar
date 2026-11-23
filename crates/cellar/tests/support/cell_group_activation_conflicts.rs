// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe actual private-writer preparation without changing filesystem results.

use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock,
    HostMetadata,
};
use std::{
    io,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

pub(super) struct Files {
    rooted: bool,
    opens: AtomicUsize,
}

impl Files {
    pub(super) fn new(rooted: bool) -> Self {
        Self {
            rooted,
            opens: AtomicUsize::new(0),
        }
    }

    pub(super) fn opens(&self) -> usize {
        self.opens.load(Ordering::Acquire)
    }
}

impl FileSystem for Files {
    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_read_write(path)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open_read_write(path)
    }

    fn clone_read_write(&self, source: &Path, destination: &Path) -> io::Result<Option<HostFile>> {
        DirectFileSystem.clone_read_write(source, destination)
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
        let private = if self.rooted {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("w-"))
        } else {
            path.components().any(|part| part.as_os_str() == "sql")
        };
        if private {
            self.opens.fetch_add(1, Ordering::AcqRel);
        }
        DirectFileSystem.create_dir_all(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }

    fn set_modified_unix_millis(&self, path: &Path, millis: i64) -> io::Result<()> {
        DirectFileSystem.set_modified_unix_millis(path, millis)
    }
}
