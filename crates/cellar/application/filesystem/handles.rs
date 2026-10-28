// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::storage::transaction::Transaction;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};

static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

/// Bounded weak records shared by the execution domain, rather than the
/// process. Closing or expiring a descriptor makes its slot reusable even if
/// application code retains a strong reference to the stale capability.
#[derive(Default)]
pub(crate) struct HostRegistry(Mutex<Vec<Weak<Descriptor>>>);

impl HostRegistry {
    fn register(&self, descriptor: &Arc<Descriptor>, maximum: usize) -> Result<()> {
        let mut records = self.0.lock().unwrap();
        // Compact only at the ceiling: ordinary admission is constant work,
        // and the weak-record vector itself never grows past this bound.
        if records.len() >= maximum {
            records.retain(|record| record.upgrade().is_some_and(|record| record.live()));
        }
        if records.len() >= maximum {
            return Err(Error::new(
                Errno::ENFILE,
                "host filesystem handle limit reached",
            ));
        }
        records.push(Arc::downgrade(descriptor));
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Registry(Mutex<Vec<Weak<Descriptor>>>);

impl Registry {
    pub(crate) fn pins(&self) -> HashSet<u64> {
        Self::scan(&mut self.0.lock().unwrap()).1
    }

    pub(crate) fn observation(&self) -> Result<(usize, HashSet<u64>)> {
        let mut registry = self.0.lock().unwrap();
        if registry.len() > 4096 {
            return Err(Error::new(
                Errno::EDQUOT,
                "filesystem observation handle work exceeds bounds",
            ));
        }
        Ok(Self::scan(&mut registry))
    }

    fn scan(registry: &mut Vec<Weak<Descriptor>>) -> (usize, HashSet<u64>) {
        let mut pinned = HashSet::new();
        let mut handles = 0;
        registry.retain(|record| {
            record.upgrade().is_some_and(|record| {
                if record.live() {
                    handles += 1;
                    pinned.insert(record.description.lock().unwrap().inode);
                    true
                } else {
                    false
                }
            })
        });
        (handles, pinned)
    }

    pub(crate) fn has_live(&self) -> bool {
        !self.pins().is_empty()
    }

    fn allocate(
        &self,
        fs: &Filesystem,
        description: Arc<Mutex<Description>>,
    ) -> Result<Arc<Descriptor>> {
        let io = crate::host_io::installed_context()
            .ok_or_else(|| Error::new(Errno::ESTALE, "no executing filesystem invocation"))?;
        let mut registry = self.0.lock().unwrap();
        registry.retain(|record| record.upgrade().is_some_and(|record| record.live()));
        if registry.len() >= fs.limits.max_handles {
            return Err(Error::new(
                Errno::EMFILE,
                "cell filesystem handle limit reached",
            ));
        }
        let token = NEXT_HANDLE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| Error::new(Errno::EMFILE, "filesystem handle identity exhausted"))?;
        let record = Arc::new(Descriptor {
            token,
            owner: Arc::downgrade(&io),
            event: io.native_event.get().cloned().ok_or_else(|| {
                Error::new(
                    Errno::ESTALE,
                    "filesystem descriptor has no native invocation owner",
                )
            })?,
            transaction: fs.storage.1.as_ref().map(Arc::downgrade),
            closed: AtomicBool::new(false),
            description,
        });
        io.host_services()
            .filesystem_handles()
            .register(&record, fs.limits.max_host_handles)?;
        registry.push(Arc::downgrade(&record));
        Ok(record)
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;

    #[test]
    fn observation_refuses_excess_registry_work_without_changing_pin_cleanup() {
        let registry = Registry(Mutex::new((0..4097).map(|_| Weak::new()).collect()));
        assert_eq!(registry.observation().unwrap_err().errno, Errno::EDQUOT);
        assert!(registry.pins().is_empty());
        assert_eq!(registry.observation().unwrap().0, 0);
    }
}

pub(super) struct Description {
    pub inode: u64,
    pub offset: u64,
    pub readable: bool,
    pub writable: bool,
    pub append: bool,
}

pub(super) struct Descriptor {
    pub token: u64,
    owner: Weak<crate::host_io::IoContext>,
    event: Weak<crate::application::EventState>,
    transaction: Option<Weak<Transaction>>,
    closed: AtomicBool,
    pub description: Arc<Mutex<Description>>,
}

impl Descriptor {
    fn live(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
            && self
                .event
                .upgrade()
                .is_some_and(|event| !event.retired.load(Ordering::Acquire))
            && self
                .owner
                .upgrade()
                .is_some_and(|io| !io.input_gates.lock().unwrap().retired)
            && self
                .transaction
                .as_ref()
                .is_none_or(|tx| tx.upgrade().is_some_and(|tx| tx.check_active().is_ok()))
    }

    pub fn check(&self, fs: &Filesystem) -> Result<()> {
        if !self.live() {
            return Err(Error::new(
                Errno::EBADF,
                "filesystem descriptor is closed or expired",
            ));
        }
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| Error::new(Errno::EBADF, "filesystem invocation has ended"))?;
        if !crate::host_io::installed_context().is_some_and(|io| Arc::ptr_eq(&io, &owner)) {
            return Err(Error::new(
                Errno::ESTALE,
                "descriptor belongs to another invocation",
            ));
        }
        fs.scope()?;
        Ok(())
    }
}

/// Owned descriptor; duplication is explicit and shares the open-description
/// offset. No SQLite pointer or operating system descriptor crosses the API.
pub struct File {
    pub(super) fs: Filesystem,
    pub(super) record: Arc<Descriptor>,
}

impl File {
    pub(super) fn reserve(fs: &Filesystem, inode: u64, options: OpenOptions) -> Result<Self> {
        let description = Arc::new(Mutex::new(Description {
            inode,
            offset: 0,
            readable: options.readable,
            writable: options.writable,
            append: options.append,
        }));
        let record = fs
            .storage
            .0
            .resources
            .filesystem
            .allocate(fs, description)?;
        Ok(Self {
            fs: fs.clone(),
            record,
        })
    }

    pub fn dup(&self) -> Result<Self> {
        operation("filesystem.dup", || {
            self.record.check(&self.fs)?;
            let record = self
                .fs
                .storage
                .0
                .resources
                .filesystem
                .allocate(&self.fs, self.record.description.clone())?;
            Ok(Self {
                fs: self.fs.clone(),
                record,
            })
        })
    }

    pub fn read(&self, bytes: &mut [u8]) -> Result<usize> {
        operation("filesystem.read", || {
            self.record.check(&self.fs)?;
            self.fs.charge(
                0,
                bytes
                    .len()
                    .min(self.fs.limits.max_transfer_bytes)
                    .div_ceil(CHUNK_BYTES)
                    .saturating_add(usize::from(!bytes.is_empty()))
                    .saturating_mul(CHUNK_BYTES),
            )?;
            let mut description = self.record.description.lock().unwrap();
            if !description.readable {
                return Err(Error::new(Errno::EBADF, "descriptor is not readable"));
            }
            let count = self
                .fs
                .read(|db| db.pread(description.inode, description.offset, bytes))?;
            description.offset += count as u64;
            Ok(count)
        })
    }

    pub fn pread(&self, bytes: &mut [u8], offset: u64) -> Result<usize> {
        operation("filesystem.pread", || {
            self.record.check(&self.fs)?;
            self.fs.charge(
                0,
                bytes
                    .len()
                    .min(self.fs.limits.max_transfer_bytes)
                    .div_ceil(CHUNK_BYTES)
                    .saturating_add(usize::from(!bytes.is_empty()))
                    .saturating_mul(CHUNK_BYTES),
            )?;
            let description = self.record.description.lock().unwrap();
            if !description.readable {
                return Err(Error::new(Errno::EBADF, "descriptor is not readable"));
            }
            self.fs
                .read(|db| db.pread(description.inode, offset, bytes))
        })
    }

    pub fn write(&self, bytes: &[u8]) -> Result<usize> {
        operation("filesystem.write", || {
            self.record.check(&self.fs)?;
            self.fs.charge(
                0,
                bytes
                    .len()
                    .min(self.fs.limits.max_transfer_bytes)
                    .div_ceil(CHUNK_BYTES)
                    .saturating_add(usize::from(!bytes.is_empty()))
                    .saturating_mul(CHUNK_BYTES),
            )?;
            let mut description = self.record.description.lock().unwrap();
            if !description.writable {
                return Err(Error::new(Errno::EBADF, "descriptor is not writable"));
            }
            let (count, end) = self.fs.mutate(|db| {
                db.pwrite(
                    description.inode,
                    description.offset,
                    bytes,
                    description.append,
                )
            })?;
            if count > 0 {
                description.offset = end;
            }
            Ok(count)
        })
    }

    pub fn pwrite(&self, bytes: &[u8], offset: u64) -> Result<usize> {
        operation("filesystem.pwrite", || {
            self.record.check(&self.fs)?;
            self.fs.charge(
                0,
                bytes
                    .len()
                    .min(self.fs.limits.max_transfer_bytes)
                    .div_ceil(CHUNK_BYTES)
                    .saturating_add(usize::from(!bytes.is_empty()))
                    .saturating_mul(CHUNK_BYTES),
            )?;
            let description = self.record.description.lock().unwrap();
            if !description.writable {
                return Err(Error::new(Errno::EBADF, "descriptor is not writable"));
            }
            self.fs.mutate(|db| {
                db.pwrite(description.inode, offset, bytes, false)
                    .map(|(count, _)| count)
            })
        })
    }

    /// Looping writes may commit a prefix before an error. Use one bounded
    /// transaction when the complete buffer must commit atomically.
    pub fn write_all(&self, mut bytes: &[u8]) -> Result<()> {
        operation("filesystem.write_all", || {
            self.record.check(&self.fs)?;
            if bytes.len() > self.fs.limits.max_file_helper_bytes {
                return Err(Error::new(
                    Errno::EFBIG,
                    "write_all exceeds helper bound; use repeated bounded writes",
                ));
            }
            let total = bytes.len();
            if bytes.is_empty() {
                self.write(bytes)?;
            }
            while !bytes.is_empty() {
                let count = self.write(bytes).map_err(|error| {
                    error.after_progress(total - bytes.len(), self.fs.storage.1.is_none())
                })?;
                if count == 0 {
                    return Err(Error::new(Errno::EIO, "write made no progress")
                        .after_progress(total - bytes.len(), self.fs.storage.1.is_none()));
                }
                bytes = &bytes[count..];
            }
            Ok(())
        })
    }

    /// Fill the supplied buffer. EOF reports the consumed prefix through the
    /// advanced offset; this looping operation is not an atomic read receipt.
    pub fn read_exact(&self, mut bytes: &mut [u8]) -> Result<()> {
        operation("filesystem.read_exact", move || {
            self.record.check(&self.fs)?;
            if bytes.len() > self.fs.limits.max_file_helper_bytes {
                return Err(Error::new(
                    Errno::EFBIG,
                    "read_exact exceeds helper bound; use repeated bounded reads",
                ));
            }
            let total = bytes.len();
            if bytes.is_empty() {
                self.read(bytes)?;
            }
            while !bytes.is_empty() {
                let count = self
                    .read(bytes)
                    .map_err(|error| error.after_progress(total - bytes.len(), false))?;
                if count == 0 {
                    return Err(
                        Error::new(Errno::EIO, "end of file before read_exact completed")
                            .with_code("UnexpectedEof")
                            .after_progress(total - bytes.len(), false),
                    );
                }
                bytes = &mut bytes[count..];
            }
            Ok(())
        })
    }

    pub fn lseek(&self, seek: Seek) -> Result<u64> {
        operation("filesystem.lseek", || {
            self.record.check(&self.fs)?;
            let mut description = self.record.description.lock().unwrap();
            let offset = match seek {
                Seek::Start(offset) => i128::from(offset),
                Seek::Current(delta) => i128::from(description.offset) + i128::from(delta),
                Seek::End(delta) => {
                    i128::from(self.fs.read(|db| db.stat(description.inode))?.size)
                        + i128::from(delta)
                }
            };
            if offset < 0 {
                return Err(Error::new(
                    Errno::EINVAL,
                    "seek would have a negative offset",
                ));
            }
            if offset > i128::from(i64::MAX) {
                return Err(Error::new(
                    Errno::EOVERFLOW,
                    "seek offset exceeds integer range",
                ));
            }
            description.offset = offset as u64;
            Ok(description.offset)
        })
    }

    pub fn fstat(&self) -> Result<Stat> {
        operation("filesystem.fstat", || {
            self.record.check(&self.fs)?;
            let inode = self.record.description.lock().unwrap().inode;
            self.fs.read(|db| db.stat(inode))
        })
    }

    pub fn ftruncate(&self, size: u64) -> Result<()> {
        operation("filesystem.ftruncate", || {
            self.record.check(&self.fs)?;
            self.fs.charge(0, CHUNK_BYTES)?;
            let description = self.record.description.lock().unwrap();
            if !description.writable {
                return Err(Error::new(Errno::EBADF, "descriptor is not writable"));
            }
            self.fs.mutate(|db| db.truncate(description.inode, size))
        })
    }

    pub fn fchmod(&self, mode: u32) -> Result<()> {
        operation("filesystem.fchmod", || {
            self.record.check(&self.fs)?;
            types::validate_mode(mode)?;
            let inode = self.record.description.lock().unwrap().inode;
            self.fs.mutate(|db| db.metadata(inode, Some(mode), None))
        })
    }

    pub fn close(self) -> Result<()> {
        self.record.closed.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn fsync(&self) -> Result<()> {
        (async {
            self.record.check(&self.fs)?;
            self.fs.sync().await
        })
        .await
        .map_err(|error: Error| error.in_operation("filesystem.fsync"))
    }

    pub async fn fdatasync(&self) -> Result<()> {
        self.fsync()
            .await
            .map_err(|error: Error| error.in_operation("filesystem.fdatasync"))
    }
}

impl Drop for File {
    fn drop(&mut self) {
        self.record.closed.store(true, Ordering::Release);
    }
}

/// An enumeration position belongs to one directory descriptor identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirCookie {
    token: u64,
    position: u64,
}

impl DirCookie {
    /// Opaque transport representation; seekdir validates descriptor affinity.
    pub fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.token.to_be_bytes());
        bytes[8..].copy_from_slice(&self.position.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self {
            token: u64::from_be_bytes(bytes[..8].try_into().expect("eight-byte token")),
            position: u64::from_be_bytes(bytes[8..].try_into().expect("eight-byte position")),
        }
    }
}

pub struct Directory {
    pub(super) file: File,
}

impl Directory {
    pub(super) fn reserve(fs: &Filesystem) -> Result<Self> {
        Ok(Self {
            file: File::reserve(fs, 1, OpenOptions::read_only())?,
        })
    }

    pub(super) fn inode(&self, fs: &Filesystem) -> Result<u64> {
        self.file.record.check(&self.file.fs)?;
        if !Arc::ptr_eq(&self.file.fs.storage.0, &fs.storage.0) {
            return Err(Error::new(
                Errno::EXDEV,
                "directory belongs to another cell",
            ));
        }
        fs.scope()?;
        Ok(self.file.record.description.lock().unwrap().inode)
    }

    pub fn readdir(&self) -> Result<Option<DirEntry>> {
        operation("filesystem.readdir", || {
            self.file.record.check(&self.file.fs)?;
            let mut description = self.file.record.description.lock().unwrap();
            let entry = self.file.fs.read(|db| {
                // rmdir and directory replacement remove the dot entries too.
                // The pinned inode remains available for metadata and cleanup,
                // but rewinding it must not synthesize names after unlink.
                if description.offset < 2 && db.stat(description.inode)?.nlink == 0 {
                    return Ok(None);
                }
                // Descriptor enumeration includes the POSIX dot entries. Real
                // directory cookies are translated by two without changing the
                // persisted namespace or the stateless read_dir helper's cursors.
                match description.offset {
                    0 => Ok(Some((
                        1,
                        DirEntry {
                            name: b".".to_vec(),
                            inode: description.inode,
                            kind: Kind::Directory,
                        },
                    ))),
                    1 => Ok(Some((
                        2,
                        DirEntry {
                            name: b"..".to_vec(),
                            inode: db.parent(description.inode)?,
                            kind: Kind::Directory,
                        },
                    ))),
                    position => Ok(db
                        .readdir(description.inode, position - 2)?
                        .map(|(cookie, entry)| (cookie + 2, entry))),
                }
            })?;
            Ok(entry.map(|(cookie, entry)| {
                description.offset = cookie;
                entry
            }))
        })
    }

    pub fn telldir(&self) -> Result<DirCookie> {
        operation("filesystem.telldir", || {
            self.file.record.check(&self.file.fs)?;
            Ok(DirCookie {
                token: self.file.record.token,
                position: self.file.record.description.lock().unwrap().offset,
            })
        })
    }

    pub fn seekdir(&self, cookie: DirCookie) -> Result<()> {
        operation("filesystem.seekdir", || {
            self.file.record.check(&self.file.fs)?;
            if cookie.token != self.file.record.token || cookie.position > i64::MAX as u64 + 2 {
                return Err(Error::new(
                    Errno::EINVAL,
                    "directory cookie belongs to another descriptor",
                ));
            }
            self.file.record.description.lock().unwrap().offset = cookie.position;
            Ok(())
        })
    }

    pub fn rewinddir(&self) -> Result<()> {
        operation("filesystem.rewinddir", || {
            self.file.record.check(&self.file.fs)?;
            self.file.record.description.lock().unwrap().offset = 0;
            Ok(())
        })
    }

    pub fn closedir(self) -> Result<()> {
        operation("filesystem.closedir", || self.file.close())
    }

    pub fn fstat(&self) -> Result<Stat> {
        operation("filesystem.fstat", || self.file.fstat())
    }

    pub async fn fsync(&self) -> Result<()> {
        self.file
            .fsync()
            .await
            .map_err(|error: Error| error.in_operation("filesystem.fsync"))
    }
}
