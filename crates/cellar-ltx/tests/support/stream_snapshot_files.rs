// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Every codec I/O fails if it attempts a whole-image allocation or write.
use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileIo,
    HostFileLock, HostMetadata,
};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Default)]
pub struct Metrics {
    pub max_read: AtomicUsize,
    pub max_write: AtomicUsize,
    pub active_handles: AtomicUsize,
    pub fail_append: AtomicBool,
    pub fail_sync: AtomicBool,
    pub corrupt_output: AtomicBool,
    pub short_read: AtomicBool,
    pub clone_mode: AtomicUsize,
    pub clone_calls: AtomicUsize,
    /// 0: unsupported; 1: accepted; 2: I/O failure; 3: second file fails;
    /// 4: forward to the native provider.
    pub streaming_mode: AtomicUsize,
    pub streaming_calls: AtomicUsize,
}

struct File {
    inner: HostFile,
    metrics: Arc<Metrics>,
}

impl Drop for File {
    fn drop(&mut self) {
        self.metrics.active_handles.fetch_sub(1, Ordering::AcqRel);
    }
}

impl File {
    fn write_bound(&self, count: usize) -> io::Result<()> {
        self.metrics.max_write.fetch_max(count, Ordering::Relaxed);
        if count > 65536 {
            return Err(io::Error::other("unbounded codec write"));
        }
        Ok(())
    }
}

impl HostFileIo for File {
    fn advise_streaming(&mut self) -> io::Result<()> {
        let call = self.metrics.streaming_calls.fetch_add(1, Ordering::AcqRel) + 1;
        match self.metrics.streaming_mode.load(Ordering::Acquire) {
            1 => Ok(()),
            2 => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected cache-advice failure",
            )),
            3 if call == 2 => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected index-advice failure",
            )),
            3 => Ok(()),
            4 => self.inner.advise_streaming(),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "fixture has no cache advice",
            )),
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.write_bound(bytes.len())?;
        if self.metrics.fail_append.swap(false, Ordering::AcqRel) {
            self.inner.write_all(&bytes[..bytes.len() / 2])?;
            return Err(io::Error::other("uncertain partial append"));
        }
        self.inner.write_all(bytes)
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.write_bound(bytes.len())?;
        if self.metrics.fail_append.swap(false, Ordering::AcqRel) {
            self.inner.write_all_at(offset, &bytes[..bytes.len() / 2])?;
            return Err(io::Error::other("uncertain partial random write"));
        }
        if self.metrics.corrupt_output.swap(false, Ordering::AcqRel) {
            let mut corrupt = bytes.to_vec();
            if let Some(byte) = corrupt.last_mut() {
                *byte ^= 1;
            }
            return self.inner.write_all_at(offset, &corrupt);
        }
        self.inner.write_all_at(offset, bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.metrics.max_read.fetch_max(len, Ordering::Relaxed);
        if len > 65536 {
            return Err(io::Error::other("unbounded codec read"));
        }
        let mut bytes = self.inner.read_exact_at(offset, len)?;
        if self.metrics.short_read.swap(false, Ordering::AcqRel) {
            bytes.pop();
        }
        Ok(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        if self.metrics.fail_sync.swap(false, Ordering::AcqRel) {
            return Err(io::Error::other("uncertain file sync"));
        }
        self.inner.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.inner.file_len()
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
}

pub struct BoundedFiles {
    pub directory: PathBuf,
    pub metrics: Arc<Metrics>,
}

impl BoundedFiles {
    fn wrap(&self, file: HostFile) -> HostFile {
        self.metrics.active_handles.fetch_add(1, Ordering::AcqRel);
        HostFile::from_io(File {
            inner: file,
            metrics: self.metrics.clone(),
        })
    }
}

impl FileSystem for BoundedFiles {
    fn clone_read_write(&self, source: &Path, destination: &Path) -> io::Result<Option<HostFile>> {
        let mode = self.metrics.clone_mode.load(Ordering::Acquire);
        if mode == 0 {
            return Ok(None);
        }
        self.metrics.clone_calls.fetch_add(1, Ordering::AcqRel);
        let mut target = self.create_read_write(destination)?;
        if mode == 2 {
            return Err(io::Error::other("uncertain clone after exclusive creation"));
        }
        let mut source = self.open(source)?;
        let length = source.file_len()?;
        let mut offset = 0;
        while offset < length {
            let n = (length - offset).min(65536) as usize;
            target.write_all_at(offset, &source.read_exact_at(offset, n)?)?;
            offset += n as u64;
        }
        Ok(Some(target))
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        DirectFileSystem.read_dir_iter(path)
    }

    fn temporary_file(&self, _: Option<&Path>) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.temporary_file(Some(&self.directory))?))
    }

    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.create_read_write(path)?))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.open(path)?))
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.create(path)?))
    }

    fn read(&self, _: &Path) -> io::Result<Vec<u8>> {
        Err(io::Error::other("whole-file reads forbidden"))
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn write(&self, _: &Path, _: &[u8]) -> io::Result<()> {
        Err(io::Error::other("whole-file writes forbidden"))
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

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }
}
