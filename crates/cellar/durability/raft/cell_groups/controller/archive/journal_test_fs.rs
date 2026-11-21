// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Deterministic publication around the actual open of a journal generation.

use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostFile, HostFileIo, HostFileLock, HostMetadata,
};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    BeforeOpen,
    AfterOpen,
}

type Hook = Box<dyn FnOnce() -> io::Result<()> + Send>;

pub(super) struct Filesystem {
    state: PathBuf,
    hook: Mutex<Option<(Phase, Hook)>>,
    reads: Arc<Mutex<Vec<usize>>>,
}

impl Filesystem {
    pub(super) fn new(state: PathBuf) -> Self {
        Self {
            state,
            hook: Mutex::new(None),
            reads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(super) fn arm(&self, phase: Phase, hook: impl FnOnce() -> io::Result<()> + Send + 'static) {
        self.reads.lock().unwrap().clear();
        let mut slot = self.hook.lock().unwrap();
        assert!(slot.is_none(), "unconsumed journal schedule");
        *slot = Some((phase, Box::new(hook)));
    }

    fn take(&self, path: &Path, phase: Phase) -> Option<Hook> {
        let mut hook = self.hook.lock().unwrap();
        if path == self.state && hook.as_ref().is_some_and(|(at, _)| *at == phase) {
            hook.take().map(|(_, hook)| hook)
        } else {
            None
        }
    }

    pub(super) fn reads(&self) -> Vec<usize> {
        self.reads.lock().unwrap().clone()
    }

    /// Publish real bytes using the same sync/rename/directory-sync sequence.
    /// Negative cases deliberately bypass Journal's encoder, not its reader.
    pub(super) fn replace(&self, bytes: &[u8]) -> io::Result<()> {
        let temporary = self.state.with_extension("replacement");
        let mut file = DirectFileSystem.create_new(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        DirectFileSystem.rename(&temporary, &self.state)?;
        DirectFileSystem.sync_all(self.state.parent().unwrap())
    }
}

struct Reader {
    file: HostFile,
    reads: Arc<Mutex<Vec<usize>>>,
}

impl HostFileIo for Reader {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.file.file_len()
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.reads.lock().unwrap().push(length);
        self.file.read_exact_at(offset, length)
    }
}

impl FileSystem for Filesystem {
    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
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
        if let Some(hook) = self.take(path, Phase::BeforeOpen) {
            hook()?;
        }
        let file = DirectFileSystem.open(path)?;
        if let Some(hook) = self.take(path, Phase::AfterOpen) {
            hook()?;
        }
        Ok(if path == self.state {
            HostFile::from_io(Reader {
                file,
                reads: self.reads.clone(),
            })
        } else {
            file
        })
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

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }
}
