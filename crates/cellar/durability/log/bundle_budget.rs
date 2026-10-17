//! Node-local admission for bundle materialization. A reservation follows
//! entered I/O and the derived spool, rather than the caller's future alone.
use super::recovery_rows::Rows;
use anyhow::{Context, Result};
use cellar_ltx::{FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileIo, HostMetadata};
use std::{io, path::Path, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct Budget(Arc<Semaphore>);

impl Default for Budget {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(1)))
    }
}

impl Budget {
    pub async fn acquire(&self) -> Result<Scope> {
        Ok(Scope(Arc::new(
            self.0
                .clone()
                .acquire_owned()
                .await
                .context("bundle scan admission closed")?,
        )))
    }
}

#[derive(Clone)]
pub(super) struct Scope(Arc<OwnedSemaphorePermit>);

impl Scope {
    pub fn filesystem(&self, inner: Arc<dyn FileSystem>) -> Arc<dyn FileSystem> {
        Arc::new(GuardedFs {
            inner,
            scope: self.clone(),
        })
    }

    pub fn hold_rows(&self, rows: &mut Rows) {
        rows.keep_alive(self.0.clone());
    }

    pub fn read_guard(&self) -> crate::bucket::ReadGuard {
        crate::bucket::ReadGuard::new(self.0.clone())
    }

    pub fn file(&self, inner: HostFile) -> HostFile {
        HostFile::from_io(GuardedFile {
            inner,
            _reservation: self.0.clone(),
        })
    }
}

struct GuardedFile {
    inner: HostFile,
    // Close the actual file before returning admission, including after an
    // abandoned caller leaves this handle with an entered blocking worker.
    _reservation: Arc<OwnedSemaphorePermit>,
}

impl HostFileIo for GuardedFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.inner.read_exact_at(offset, length)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.inner.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.inner.file_len()
    }
}

struct GuardedFs {
    inner: Arc<dyn FileSystem>,
    scope: Scope,
}

struct GuardedDirectory {
    inner: HostDirIterator,
    _scope: Scope,
}

impl Iterator for GuardedDirectory {
    type Item = io::Result<HostDirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl FileSystem for GuardedFs {
    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        Ok(self.scope.file(self.inner.temporary_file(directory)?))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.inner.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.inner.read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        Ok(Box::new(GuardedDirectory {
            inner: self.inner.read_dir_iter(path)?,
            _scope: self.scope.clone(),
        }))
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.inner.metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.scope.file(self.inner.create(path)?))
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.scope.file(self.inner.create_new(path)?))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.scope.file(self.inner.open(path)?))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.inner.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.inner.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        self.inner.remove_dir_batch(path, limit)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }

    fn set_modified_unix_millis(&self, path: &Path, millis: i64) -> io::Result<()> {
        self.inner.set_modified_unix_millis(path, millis)
    }
}

#[cfg(test)]
#[path = "bundle_budget_tests.rs"]
mod tests;
