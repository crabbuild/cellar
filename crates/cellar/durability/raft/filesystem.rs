// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Every engine file and directory operation uses the supplied node filesystem.

use super::performance::{Counter, Operation, Registry};
use cellar_ltx::{FileSystem, HostFile};
use raft_engine::env::{
    DirectoryEntry, DirectoryLock, DirectoryMetadata, Handle, Permission, WriteExt,
};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) struct EngineFileSystem(pub Arc<dyn FileSystem>, pub Arc<Registry>);

pub(crate) struct EngineHandle {
    performance: Arc<Registry>,
    file: Mutex<HostFile>,
}

impl EngineHandle {
    fn lock(&self) -> io::Result<MutexGuard<'_, HostFile>> {
        self.file
            .lock()
            .map_err(|_| io::Error::other("Raft file handle poisoned"))
    }
}

impl Handle for EngineHandle {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        self.lock()?.set_len(offset as u64)
    }

    fn file_size(&self) -> io::Result<usize> {
        usize::try_from(self.lock()?.file_len()?)
            .map_err(|_| io::Error::other("Raft file is too large"))
    }

    fn sync(&self) -> io::Result<()> {
        let mut file = self.lock()?;
        let timer = self.performance.timer(Operation::EngineFileSync);
        let result = file.sync_all();
        timer.finish(result.is_ok());
        result
    }
}

pub(crate) struct EngineCursor {
    handle: Arc<EngineHandle>,
    offset: u64,
}

impl Read for EngineCursor {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let mut file = self.handle.lock()?;
        let remaining = file.file_len()?.saturating_sub(self.offset);
        let length = remaining.min(buffer.len() as u64) as usize;
        if length == 0 {
            return Ok(0);
        }
        let bytes = file.read_exact_at(self.offset, length)?;
        self.handle
            .performance
            .increment(Counter::EngineReadBytes, length as u64);
        buffer[..length].copy_from_slice(&bytes);
        self.offset += length as u64;
        Ok(length)
    }
}

impl Write for EngineCursor {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("Raft file offset overflow"))?;
        let mut file = self.handle.lock()?;
        self.handle
            .performance
            .increment(Counter::EngineWriteRequestedBytes, bytes.len() as u64);
        file.write_all_at(self.offset, bytes)?;
        self.handle
            .performance
            .increment(Counter::EngineWriteCompletedBytes, bytes.len() as u64);
        self.offset = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for EngineCursor {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.offset = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(delta) => self
                .offset
                .checked_add_signed(delta)
                .ok_or_else(|| io::Error::other("invalid Raft seek"))?,
            SeekFrom::End(delta) => self
                .handle
                .lock()?
                .file_len()?
                .checked_add_signed(delta)
                .ok_or_else(|| io::Error::other("invalid Raft seek"))?,
        };
        Ok(self.offset)
    }
}

impl WriteExt for EngineCursor {
    fn truncate(&mut self, offset: usize) -> io::Result<()> {
        self.handle.truncate(offset)?;
        self.offset = offset as u64;
        Ok(())
    }

    fn allocate(&mut self, offset: usize, size: usize) -> io::Result<()> {
        // Allocation is a hint. Keep the durable file length equal to written
        // bytes: persisted zero-filled tails would require a successful close
        // truncation before AbsoluteConsistency recovery could consume them.
        offset
            .checked_add(size)
            .ok_or_else(|| io::Error::other("Raft allocation overflow"))?;
        Ok(())
    }
}

impl raft_engine::env::FileSystem for EngineFileSystem {
    type Handle = EngineHandle;
    type Reader = EngineCursor;
    type Writer = EngineCursor;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        Ok(EngineHandle {
            performance: self.1.clone(),
            file: Mutex::new(self.0.create_read_write(path.as_ref())?),
        })
    }

    fn open<P: AsRef<Path>>(&self, path: P, permission: Permission) -> io::Result<Self::Handle> {
        let file = match permission {
            Permission::ReadOnly => self.0.open(path.as_ref())?,
            Permission::ReadWrite => self.0.open_read_write(path.as_ref())?,
        };
        Ok(EngineHandle {
            performance: self.1.clone(),
            file: Mutex::new(file),
        })
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        self.0.remove_file(path.as_ref())
    }

    fn rename<P: AsRef<Path>>(&self, source: P, destination: P) -> io::Result<()> {
        self.0.rename(source.as_ref(), destination.as_ref())
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Reader> {
        Ok(EngineCursor { handle, offset: 0 })
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Writer> {
        Ok(EngineCursor { handle, offset: 0 })
    }

    fn directory_metadata(&self, path: &Path) -> io::Result<Option<DirectoryMetadata>> {
        match self.0.metadata(path) {
            Ok(metadata) => Ok(Some(DirectoryMetadata {
                is_dir: metadata.is_dir,
                is_file: metadata.is_file,
            })),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn create_directory(&self, path: &Path) -> io::Result<()> {
        self.0.create_dir_all(path)
    }

    fn read_directory(&self, path: &Path) -> io::Result<Vec<DirectoryEntry>> {
        let entries: io::Result<Vec<_>> = self
            .0
            .read_dir_iter(path)?
            .take(4097)
            .map(|entry| {
                let entry = entry?;
                let metadata = self.0.metadata(&entry.path)?;
                Ok(DirectoryEntry {
                    path: entry.path,
                    is_file: metadata.is_file,
                })
            })
            .collect();
        let entries = entries?;
        if entries.len() > 4096 {
            return Err(io::Error::other(
                "Raft engine directory entry budget exceeded",
            ));
        }
        Ok(entries)
    }

    fn lock_directory(&self, path: &Path) -> io::Result<DirectoryLock> {
        self.0.lock_file(&path.join("LOCK")).map_err(|error| {
            io::Error::new(error.kind(), format!("Raft Engine directory lock: {error}"))
        })
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        let timer = self.1.timer(Operation::EngineDirectorySync);
        let result = self.0.sync_all(path);
        timer.finish(result.is_ok());
        result
    }
}
