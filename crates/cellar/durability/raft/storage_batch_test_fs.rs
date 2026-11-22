// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Counts actual barriers on real files, including the engine's positional I/O.

use cellar_ltx::{
    FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileIo, HostFileLock, HostMetadata,
};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::Notify;

type Hook = Box<dyn FnOnce() -> io::Result<()> + Send>;

pub(super) struct Hold {
    pub entered: Notify,
    released: Mutex<bool>,
    release: Condvar,
}

pub(super) struct Release(Arc<Hold>);

impl Drop for Release {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.release.notify_all();
    }
}

#[derive(Default)]
pub(super) struct Barriers {
    pub files: AtomicUsize,
    pub directories: AtomicUsize,
    pub max_write_bytes: AtomicUsize,
    before: Mutex<Option<Hook>>,
}

impl Barriers {
    pub fn before_next_sync(&self, hook: impl FnOnce() -> io::Result<()> + Send + 'static) {
        let mut slot = self.before.lock().unwrap();
        assert!(slot.is_none(), "unconsumed physical sync schedule");
        *slot = Some(Box::new(hook));
    }

    pub fn hold_next_sync(&self) -> (Arc<Hold>, Release) {
        let hold = Arc::new(Hold {
            entered: Notify::new(),
            released: Mutex::new(false),
            release: Condvar::new(),
        });
        let owned = hold.clone();
        self.before_next_sync(move || {
            owned.entered.notify_one();
            let mut released = owned.released.lock().unwrap();
            while !*released {
                released = owned.release.wait(released).unwrap();
            }
            Ok(())
        });
        (hold.clone(), Release(hold))
    }

    pub fn reset(&self) {
        self.files.store(0, Ordering::SeqCst);
        self.directories.store(0, Ordering::SeqCst);
        self.max_write_bytes.store(0, Ordering::SeqCst);
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.files.load(Ordering::SeqCst),
            self.directories.load(Ordering::SeqCst),
        )
    }
}

pub(super) struct Filesystem {
    pub barriers: Arc<Barriers>,
    pub inner: Arc<dyn FileSystem>,
}

struct File(HostFile, Arc<Barriers>);

impl HostFileIo for File {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.1
            .max_write_bytes
            .fetch_max(bytes.len(), Ordering::SeqCst);
        self.0.write_all(bytes)
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.1
            .max_write_bytes
            .fetch_max(bytes.len(), Ordering::SeqCst);
        self.0.write_all_at(offset, bytes)
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.0.read_exact_at(offset, length)
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.0.file_len()
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.0.set_len(length)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.1.files.fetch_add(1, Ordering::SeqCst);
        let hook = self.1.before.lock().unwrap().take();
        if let Some(hook) = hook {
            hook()?;
        }
        self.0.sync_all()
    }
}

impl Filesystem {
    fn file(&self, file: HostFile) -> HostFile {
        HostFile::from_io(File(file, self.barriers.clone()))
    }
}

impl FileSystem for Filesystem {
    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        Ok(self.file(self.inner.temporary_file(directory)?))
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        self.inner.lock_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.inner.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.inner.read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        self.inner.read_dir_iter(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.inner.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.inner.symlink_metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.file(self.inner.create(path)?))
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.file(self.inner.create_new(path)?))
    }

    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.file(self.inner.create_read_write(path)?))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.file(self.inner.open(path)?))
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.file(self.inner.open_read_write(path)?))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.inner.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.barriers.directories.fetch_add(1, Ordering::SeqCst);
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

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
}
