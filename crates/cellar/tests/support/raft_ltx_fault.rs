//! Faults and trace points on actual image-file operations.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileIo,
    HostFileLock, HostMetadata,
};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const BEFORE_IMAGE_SYNC: u8 = 1;
pub const AFTER_RENAME: u8 = 2;
pub const AFTER_DIRECTORY_SYNC: u8 = 3;
pub const AFTER_MARKER_SYNC: u8 = 4;
pub const FAIL_IMAGE_SYNC: u8 = 5;
pub const FAIL_DIRECTORY_SYNC: u8 = 6;
pub const FAIL_AFTER_FINAL_CREATE: u8 = 7;

pub struct Faults {
    pub directory: PathBuf,
    pub mode: AtomicU8,
    pub fail_reads: AtomicBool,
    pub collision: AtomicBool,
    pub reads: AtomicUsize,
    pub events: Mutex<Vec<&'static str>>,
    pub image_seals: Mutex<Vec<PathBuf>>,
    pub signal: Option<PathBuf>,
}

impl Faults {
    pub fn new(directory: PathBuf, signal: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            directory,
            mode: AtomicU8::new(0),
            fail_reads: AtomicBool::new(false),
            collision: AtomicBool::new(false),
            reads: AtomicUsize::new(0),
            events: Mutex::new(Vec::new()),
            image_seals: Mutex::new(Vec::new()),
            signal,
        })
    }

    pub fn take(&self, mode: u8) -> bool {
        self.mode
            .compare_exchange(mode, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn pause(&self) -> io::Result<()> {
        let signal = self
            .signal
            .as_ref()
            .ok_or_else(|| io::Error::other("missing child signal"))?;
        let mut file = std::fs::File::create(signal)?;
        std::io::Write::write_all(&mut file, b"reached")?;
        file.sync_all()?;
        loop {
            std::thread::park();
        }
    }

    fn event(&self, event: &'static str) {
        let mut events = self.events.lock().unwrap();
        assert!(events.len() < 4096, "fixture trace bound");
        events.push(event);
    }
}

pub struct FaultFileSystem {
    pub faults: Arc<Faults>,
}

struct File {
    inner: HostFile,
    path: PathBuf,
    faults: Arc<Faults>,
}

impl FaultFileSystem {
    fn wrap(&self, path: &Path, inner: HostFile) -> HostFile {
        HostFile::from_io(File {
            inner,
            path: path.into(),
            faults: self.faults.clone(),
        })
    }
}

impl HostFileIo for File {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all_at(offset, bytes)
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.faults.reads.fetch_add(1, Ordering::AcqRel);
        if self.faults.fail_reads.load(Ordering::Acquire) {
            return Err(io::Error::other("injected image read failure"));
        }
        self.inner.read_exact_at(offset, length)
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.inner.set_len(length)
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.inner.file_len()
    }

    fn sync_all(&mut self) -> io::Result<()> {
        if self.path.extension().is_some_and(|p| p == "stage") {
            if self.faults.take(BEFORE_IMAGE_SYNC) {
                self.faults.pause()?;
            }
            if self.faults.take(FAIL_IMAGE_SYNC) {
                return Err(io::Error::other("injected image sync failure"));
            }
            self.inner.sync_all()?;
            self.faults.event("image-sync");
            self.faults
                .image_seals
                .lock()
                .unwrap()
                .push(self.path.clone());
            return Ok(());
        }
        self.inner.sync_all()
    }
}

impl FileSystem for FaultFileSystem {
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

    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.create_read_write(path)?))
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.open_read_write(path)?))
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.create(path)?))
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        if path.extension().is_some_and(|p| p == "sqlite")
            && self.faults.collision.swap(false, Ordering::AcqRel)
        {
            DirectFileSystem.write(path, b"unowned image")?;
        }
        if path.extension().is_some_and(|p| p == "sqlite")
            && self.faults.take(FAIL_AFTER_FINAL_CREATE)
        {
            drop(DirectFileSystem.create_new(path)?);
            return Err(io::Error::other("ambiguous final creation failure"));
        }
        Ok(self.wrap(path, DirectFileSystem.create_new(path)?))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.open(path)?))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        if path == self.faults.directory {
            if self.faults.take(FAIL_DIRECTORY_SYNC) {
                return Err(io::Error::other("injected directory sync failure"));
            }
            DirectFileSystem.sync_all(path)?;
            self.faults.event("directory-sync");
            if self.faults.take(AFTER_DIRECTORY_SYNC) {
                self.faults.pause()?;
            }
            return Ok(());
        }
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)?;
        if to.extension().is_some_and(|p| p == "sqlite") {
            self.faults.event("rename");
            if self.faults.take(AFTER_RENAME) {
                self.faults.pause()?;
            }
        }
        Ok(())
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
}
