// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Completed file operations for the offline query diagnostic. The direct
//! provider retains its real I/O windows, synchronization and clone behavior.

use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileIo,
    HostFileLock, HostMetadata,
};
use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Copy, Default)]
pub struct Counts {
    reads: u64,
    writes: u64,
    temporary_files: u64,
    syncs: u64,
    sync_us: u64,
}

impl Counts {
    pub fn delta(self, before: Self) -> serde_json::Value {
        serde_json::json!({
            "read_bytes": self.reads - before.reads,
            "write_bytes": self.writes - before.writes,
            "temporary_files": self.temporary_files - before.temporary_files,
            "syncs": self.syncs - before.syncs,
            "sync_seconds": (self.sync_us - before.sync_us) as f64 / 1_000_000.0,
        })
    }
}

#[derive(Default)]
pub struct ObservedFiles(Arc<Mutex<Counts>>);

impl ObservedFiles {
    pub fn capture(&self) -> Counts {
        *self.0.lock().unwrap()
    }

    fn wrap(&self, file: HostFile) -> HostFile {
        HostFile::from_io(ObservedFile {
            file,
            counts: self.0.clone(),
        })
    }
}

struct ObservedFile {
    file: HostFile,
    counts: Arc<Mutex<Counts>>,
}

impl HostFileIo for ObservedFile {
    fn advise_streaming(&mut self) -> io::Result<()> {
        self.file.advise_streaming()
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)?;
        self.counts.lock().unwrap().writes += bytes.len() as u64;
        Ok(())
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all_at(offset, bytes)?;
        self.counts.lock().unwrap().writes += bytes.len() as u64;
        Ok(())
    }

    fn write_all_window(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all_window(bytes)?;
        self.counts.lock().unwrap().writes += bytes.len() as u64;
        Ok(())
    }

    fn write_all_at_window(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all_at_window(offset, bytes)?;
        self.counts.lock().unwrap().writes += bytes.len() as u64;
        Ok(())
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let bytes = self.file.read_exact_at(offset, len)?;
        self.counts.lock().unwrap().reads += bytes.len() as u64;
        Ok(bytes)
    }

    fn read_exact_into_at(&mut self, offset: u64, bytes: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_into_at(offset, bytes)?;
        self.counts.lock().unwrap().reads += bytes.len() as u64;
        Ok(())
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.file.file_len()
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        let started = Instant::now();
        self.file.sync_all()?;
        let mut counts = self.counts.lock().unwrap();
        counts.syncs += 1;
        counts.sync_us += started.elapsed().as_micros() as u64;
        Ok(())
    }
}

impl FileSystem for ObservedFiles {
    fn read(&self, p: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(p)
    }

    fn read_dir(&self, p: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(p)
    }

    fn read_dir_iter(&self, p: &Path) -> io::Result<HostDirIterator> {
        DirectFileSystem.read_dir_iter(p)
    }

    fn metadata(&self, p: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(p)
    }

    fn symlink_metadata(&self, p: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.symlink_metadata(p)
    }

    fn create(&self, p: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.create(p)?))
    }

    fn create_new(&self, p: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.create_new(p)?))
    }

    fn open(&self, p: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.open(p)?))
    }

    fn create_read_write(&self, p: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.create_read_write(p)?))
    }

    fn open_read_write(&self, p: &Path) -> io::Result<HostFile> {
        Ok(self.wrap(DirectFileSystem.open_read_write(p)?))
    }

    fn lock_file(&self, p: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(p)
    }

    fn clone_read_write(&self, source: &Path, destination: &Path) -> io::Result<Option<HostFile>> {
        Ok(DirectFileSystem
            .clone_read_write(source, destination)?
            .map(|file| self.wrap(file)))
    }

    fn temporary_file(&self, p: Option<&Path>) -> io::Result<HostFile> {
        let file = DirectFileSystem.temporary_file(p)?;
        self.0.lock().unwrap().temporary_files += 1;
        Ok(self.wrap(file))
    }

    fn write(&self, p: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(p, bytes)
    }

    fn sync_all(&self, p: &Path) -> io::Result<()> {
        let started = Instant::now();
        DirectFileSystem.sync_all(p)?;
        let mut counts = self.0.lock().unwrap();
        counts.syncs += 1;
        counts.sync_us += started.elapsed().as_micros() as u64;
        Ok(())
    }

    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        DirectFileSystem.rename(a, b)
    }

    fn remove_file(&self, p: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(p)
    }

    fn remove_dir_all(&self, p: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(p)
    }

    fn remove_dir_batch(&self, p: &Path, limit: usize) -> io::Result<bool> {
        DirectFileSystem.remove_dir_batch(p, limit)
    }

    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(p)
    }

    fn set_modified_unix_millis(&self, p: &Path, millis: i64) -> io::Result<()> {
        DirectFileSystem.set_modified_unix_millis(p, millis)
    }
}
