// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit per-snapshot filesystem progress, never a durability certificate.
use super::{FileSystem, HostFile, CHUNK_BYTES};
use cellar_ltx::{HostDirEntry, HostDirIterator, HostFileIo, HostMetadata};
use std::{
    io,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

pub(super) struct ProgressFs {
    pub inner: Arc<dyn FileSystem>,
    pub cancelled: Arc<AtomicBool>,
    pub progress: Arc<AtomicU64>,
}

impl ProgressFs {
    fn done<T>(&self, result: io::Result<T>) -> io::Result<T> {
        let result = result?;
        self.progress.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    fn file(&self, file: io::Result<HostFile>) -> io::Result<HostFile> {
        Ok(HostFile::from_io(ProgressFile {
            inner: self.done(file)?,
            cancelled: self.cancelled.clone(),
            progress: self.progress.clone(),
        }))
    }
}

struct ProgressFile {
    inner: HostFile,
    cancelled: Arc<AtomicBool>,
    progress: Arc<AtomicU64>,
}

impl ProgressFile {
    fn check(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "tail I/O cancelled",
            ));
        }
        Ok(())
    }
}

impl HostFileIo for ProgressFile {
    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(length);
        while bytes.len() < length {
            self.check()?;
            let count = (length - bytes.len()).min(CHUNK_BYTES);
            let chunk = self
                .inner
                .read_exact_at(offset + bytes.len() as u64, count)?;
            self.check()?;
            if chunk.len() != count {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "short tail progress read",
                ));
            }
            bytes.extend_from_slice(&chunk);
            self.progress.fetch_add(1, Ordering::Relaxed);
        }
        Ok(bytes)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        for chunk in bytes.chunks(CHUNK_BYTES) {
            self.check()?;
            self.inner.write_all(chunk)?;
            self.check()?;
            self.progress.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.check()?;
        let length = self.inner.file_len()?;
        self.progress.fetch_add(1, Ordering::Relaxed);
        Ok(length)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.check()?;
        self.inner.sync_all()?;
        self.progress.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl FileSystem for ProgressFs {
    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        self.file(self.inner.temporary_file(directory))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        self.file(self.inner.open(path))
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        self.file(self.inner.create(path))
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        self.file(self.inner.create_new(path))
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        let iterator = self.inner.read_dir_iter(path)?;
        let progress = self.progress.clone();
        Ok(Box::new(iterator.inspect(move |item| {
            if item.is_ok() {
                progress.fetch_add(1, Ordering::Relaxed);
            }
        })))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.done(self.inner.read(path))
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.done(self.inner.read_dir(path))
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.done(self.inner.metadata(path))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.done(self.inner.write(path, bytes))
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.done(self.inner.sync_all(path))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.done(self.inner.rename(from, to))
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.done(self.inner.remove_file(path))
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.done(self.inner.remove_dir_all(path))
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        self.done(self.inner.remove_dir_batch(path, limit))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.done(self.inner.create_dir_all(path))
    }

    fn set_modified_unix_millis(&self, path: &Path, millis: i64) -> io::Result<()> {
        self.done(self.inner.set_modified_unix_millis(path, millis))
    }
}
