// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Faults run through the production filesystem adapter and real local files.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileIo,
    HostFileLock, HostMetadata,
};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::Notify;

pub const OFF: u8 = 0;
pub const HOLD_SYNC: u8 = 1;
pub const FAIL_SYNC: u8 = 2;
pub const FAIL_DIRECTORY_SYNC: u8 = 3;
pub const CRASH_BEFORE_SYNC: u8 = 4;
pub const CRASH_AFTER_SYNC: u8 = 5;
pub const TORN_WRITE: u8 = 6;
pub const CRASH_AFTER_DELETE: u8 = 7;
pub const FAIL_PARENT_SYNC: u8 = 8;
pub const FAIL_SYNC_HOLD_LOCK_DROP: u8 = 9;
pub const HOLD_LOCK_DROP: u8 = 10;
pub const HOLD_IMAGE_READ: u8 = 11;
pub const HOLD_SNAPSHOT_TEMP: u8 = 12;
pub const HOLD_EXTENT_READ: u8 = 13;
pub const FAIL_AFTER_SYNC: u8 = 14;
pub const HOLD_DEPENDENCY_PREFIX: u8 = 15;
pub const HOLD_DEPENDENCY_SYNC: u8 = 16;

pub struct Gate {
    pub mode: AtomicU8,
    pub entered: Notify,
    released: Mutex<bool>,
    release: Condvar,
    signal: Option<std::path::PathBuf>,
    pub sync_path: Mutex<Option<std::path::PathBuf>>,
    pub image_read_paths: Mutex<Vec<std::path::PathBuf>>,
    pub dependency_cut: Mutex<Option<(std::path::PathBuf, u64)>>,
    /// Optional sync selection, installed before arming and cleared on release.
    pub sync_when: Mutex<Option<Arc<dyn Fn() -> bool + Send + Sync>>>,
}

impl Gate {
    pub fn new(signal: Option<std::path::PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            mode: AtomicU8::new(OFF),
            entered: Notify::new(),
            released: Mutex::new(false),
            release: Condvar::new(),
            signal,
            sync_path: Mutex::new(None),
            image_read_paths: Mutex::new(vec![]),
            dependency_cut: Mutex::new(None),
            sync_when: Mutex::new(None),
        })
    }

    pub fn arm(self: &Arc<Self>, mode: u8) -> Release {
        *self.released.lock().unwrap() = false;
        self.mode.store(mode, Ordering::Release);
        Release(self.clone())
    }

    fn pause(&self) -> io::Result<()> {
        if let Some(signal) = &self.signal {
            let mut file = std::fs::File::create(signal)?;
            std::io::Write::write_all(&mut file, b"reached")?;
            file.sync_all()?;
        }
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.release.wait(released).unwrap();
        }
        Ok(())
    }
}

pub struct Release(Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.mode.store(OFF, Ordering::Release);
        self.0.sync_when.lock().unwrap().take();
        *self.0.released.lock().unwrap() = true;
        self.0.release.notify_all();
    }
}

pub struct FaultFileSystem {
    pub gate: Arc<Gate>,
}

impl FaultFileSystem {
    fn wrap(&self, path: &Path, inner: HostFile) -> HostFile {
        HostFile::from_io(FaultFile {
            inner,
            path: path.into(),
            gate: self.gate.clone(),
            log: path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "log"),
            image: path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "images")
                && path.extension().is_some_and(|name| name == "sqlite"),
        })
    }
}

struct FaultFile {
    inner: HostFile,
    path: std::path::PathBuf,
    gate: Arc<Gate>,
    log: bool,
    image: bool,
}

impl FaultFile {
    fn dependency(&self) -> bool {
        self.path == Path::new("snapshot.scratch")
            || self
                .path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "extents")
                && self
                    .path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("e-"))
    }

    fn pause_dependency(&mut self) -> io::Result<()> {
        *self.gate.dependency_cut.lock().unwrap() =
            Some((self.path.clone(), self.inner.file_len()?));
        self.gate.pause()
    }
}

struct TrackedLock {
    _lock: HostFileLock,
    gate: Arc<Gate>,
}

impl Drop for TrackedLock {
    fn drop(&mut self) {
        if self
            .gate
            .mode
            .compare_exchange(HOLD_LOCK_DROP, OFF, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let _ = self.gate.pause();
        }
    }
}

impl HostFileIo for FaultFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if self.dependency()
            && offset == 0
            && bytes.len() > 1
            && self
                .gate
                .mode
                .compare_exchange(
                    HOLD_DEPENDENCY_PREFIX,
                    OFF,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            let prefix = bytes.len() / 2;
            self.inner.write_all_at(offset, &bytes[..prefix])?;
            self.inner.sync_all()?;
            self.pause_dependency()?;
            return self
                .inner
                .write_all_at(offset + prefix as u64, &bytes[prefix..]);
        }
        if self.log
            && self
                .gate
                .mode
                .compare_exchange(TORN_WRITE, OFF, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.inner.write_all_at(offset, &bytes[..bytes.len() / 2])?;
            self.inner.sync_all()?;
            self.gate.pause()?;
            return Err(io::Error::other("injected torn write"));
        }
        self.inner.write_all_at(offset, bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        // An empty selection preserves the generic capture-read fault. A
        // named selection holds only the intended immutable generation read.
        let selected = {
            let paths = self.gate.image_read_paths.lock().unwrap();
            paths.is_empty() || self.image && paths.contains(&self.path)
        };
        let mode = self.gate.mode.load(Ordering::Acquire);
        let extent = mode == HOLD_EXTENT_READ
            && self
                .path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("e-"));
        if !self.log
            && (mode == HOLD_IMAGE_READ && selected || extent)
            && self
                .gate
                .mode
                .compare_exchange(mode, OFF, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.gate.pause()?;
        }
        self.inner.read_exact_at(offset, len)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        if self.dependency()
            && self
                .gate
                .mode
                .compare_exchange(
                    HOLD_DEPENDENCY_SYNC,
                    OFF,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            self.inner.sync_all()?;
            self.pause_dependency()?;
            return Ok(());
        }
        let sync_when = self.gate.sync_when.lock().unwrap().clone();
        if self.log && sync_when.is_none_or(|when| when()) {
            let mode = self.gate.mode.load(Ordering::Acquire);
            let mode = if matches!(
                mode,
                HOLD_SYNC
                    | FAIL_SYNC
                    | CRASH_BEFORE_SYNC
                    | CRASH_AFTER_SYNC
                    | FAIL_AFTER_SYNC
                    | FAIL_SYNC_HOLD_LOCK_DROP
            ) && self
                .gate
                .mode
                .compare_exchange(mode, OFF, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                mode
            } else {
                OFF
            };
            match mode {
                HOLD_SYNC | CRASH_BEFORE_SYNC => self.gate.pause()?,
                FAIL_SYNC => return Err(io::Error::other("injected Raft sync failure")),
                FAIL_AFTER_SYNC => {
                    self.inner.sync_all()?;
                    return Err(io::Error::other("injected failure after durable Raft sync"));
                }
                FAIL_SYNC_HOLD_LOCK_DROP => {
                    self.gate.mode.store(HOLD_LOCK_DROP, Ordering::Release);
                    return Err(io::Error::other(
                        "injected Raft sync failure before held lock release",
                    ));
                }
                CRASH_AFTER_SYNC => {
                    self.inner.sync_all()?;
                    self.gate.pause()?;
                    return Ok(());
                }
                _ => (),
            }
        }
        self.inner.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.inner.file_len()
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.inner.set_len(length)
    }
}

impl FileSystem for FaultFileSystem {
    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        // Hold every encoder until the explicit release. Archive and consensus
        // snapshot encoders can race; consuming a one-shot gate could otherwise
        // leave the serialized SQL encoder untested.
        if directory
            .and_then(Path::file_name)
            .is_some_and(|name| name.to_string_lossy().ends_with(".snapshot-scratch"))
            && self.gate.mode.load(Ordering::Acquire) == HOLD_SNAPSHOT_TEMP
        {
            self.gate.pause()?;
        }
        Ok(self.wrap(
            Path::new("snapshot.scratch"),
            DirectFileSystem.temporary_file(directory)?,
        ))
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
        Ok(self.wrap(path, DirectFileSystem.create(path)?))
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.create_new(path)?))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.open(path)?))
    }

    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.create_read_write(path)?))
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(path, DirectFileSystem.open_read_write(path)?))
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        let lock = DirectFileSystem.lock_file(path)?;
        if path.file_name().is_some_and(|name| name == "CELLAR_LOCK") {
            Ok(Box::new(TrackedLock {
                _lock: lock,
                gate: self.gate.clone(),
            }))
        } else {
            Ok(lock)
        }
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        if self.gate.sync_path.lock().unwrap().as_deref() == Some(path)
            && self
                .gate
                .mode
                .compare_exchange(FAIL_PARENT_SYNC, OFF, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Err(io::Error::other("injected parent directory sync failure"));
        }
        if path.file_name().is_some_and(|name| name == "log")
            && self
                .gate
                .mode
                .compare_exchange(
                    FAIL_DIRECTORY_SYNC,
                    OFF,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            return Err(io::Error::other("injected Raft directory sync failure"));
        }
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)?;
        if path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == "log")
            && self
                .gate
                .mode
                .compare_exchange(CRASH_AFTER_DELETE, OFF, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.gate.pause()?;
        }
        Ok(())
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
