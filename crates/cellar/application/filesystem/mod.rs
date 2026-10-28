// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Per-cell filesystem operations over the cell's transactional SQLite state.

mod error;
mod handles;
mod helpers;
mod limits;
pub(crate) mod types;
use error::operation;
pub use error::{Errno, Error, Outcome, Result};
pub use handles::{DirCookie, Directory, File};
pub(crate) use handles::{HostRegistry, Registry};
pub use limits::FilesystemLimits;
pub use types::*;

use crate::application::Storage;
use crate::storage::filesystem as backend;
use std::sync::Arc;

/// A checked view of the current root cell's filesystem. Cloning a view never
/// changes its activation or transaction authority.
#[derive(Clone)]
pub struct Filesystem {
    storage: Storage,
    limits: FilesystemLimits,
    policy: Policy,
}

impl Storage {
    pub fn fs(&self) -> Result<Filesystem> {
        operation("filesystem.acquire", || {
            self.filesystem_scope()?;
            let class = self
                .0
                .scope
                .split_once(':')
                .map_or(self.0.scope.as_str(), |(class, _)| class);
            let policy = self
                .0
                .worker_config
                .native_application
                .as_ref()
                .and_then(|app| app.filesystem_policy(class))
                .ok_or_else(|| {
                    Error::new(Errno::EACCES, "cell class has no filesystem capability")
                        .with_code("CapabilityMissing")
                })?;
            let limits = crate::application::limits::current().filesystem;
            policy.validate()?;
            backend::initialize(&self.0.scope, limits).map_err(Error::from)?;
            Ok(Filesystem {
                storage: self.clone(),
                limits,
                policy,
            })
        })
    }

    pub(crate) fn filesystem_scope(&self) -> Result<&str> {
        let scope = self.scope().map_err(|error| {
            let mut error = Error::from(error);
            // Authority/transaction admission precedes this operation's
            // filesystem backend work. Retain an earlier failure as the cause
            // without claiming an uncertain effect for the refused operation.
            error.outcome = Outcome::NotApplied;
            error
        })?;
        if crate::storage::facet_opening(scope).is_some() {
            return Err(Error::new(
                Errno::EOPNOTSUPP,
                "facets do not support the filesystem primitive",
            ));
        }
        let current = self.0.resources.transaction.lock().unwrap();
        let matches = match (&self.1, &*current) {
            (None, None) => true,
            (Some(owned), Some(current)) => Arc::ptr_eq(owned, current),
            _ => false,
        };
        if !matches {
            return Err(Error::affinity());
        }
        Ok(scope)
    }
}

impl Filesystem {
    fn scope(&self) -> Result<&str> {
        self.storage.filesystem_scope()
    }

    fn read<T>(
        &self,
        callback: impl FnOnce(&backend::Database<'_>) -> anyhow::Result<T>,
    ) -> Result<T> {
        let scope = self.scope()?;
        self.charge(1, 0)?;
        backend::read(scope, self.limits, callback).map_err(Error::from)
    }

    fn mutate<T>(
        &self,
        callback: impl FnOnce(&backend::Database<'_>) -> anyhow::Result<T>,
    ) -> Result<T> {
        let scope = self.scope()?;
        self.require_writable()?;
        self.charge(1, 0)?;
        backend::mutate(scope, self.limits, callback).map_err(Error::from)
    }

    fn require_writable(&self) -> Result<()> {
        if self.policy.read_only {
            return Err(Error::new(
                Errno::EROFS,
                "filesystem class policy is read-only",
            ));
        }
        Ok(())
    }

    fn charge(&self, operations: usize, bytes: usize) -> Result<()> {
        if let Some(transaction) = &self.storage.1 {
            transaction.charge_filesystem(operations, bytes, self.limits)?;
        }
        Ok(())
    }

    fn path(&self, path: impl AsRef<[u8]>) -> Result<types::Path> {
        types::Path::parse(path, self.limits)
    }

    pub fn capabilities(&self) -> Result<Capabilities> {
        operation("filesystem.capabilities", || {
            self.scope()?;
            Ok(Capabilities {
                logical_uid: 1000,
                logical_gid: 1000,
                creation_mask: self.policy.creation_mask,
                read_only: self.policy.read_only,
                regular_files: true,
                directories: true,
                positional_io: true,
                directory_relative: true,
                transactions: true,
                symlinks: false,
                hard_links: false,
                advisory_locks: false,
                atime: false,
                limits: self.limits,
            })
        })
    }

    pub fn open(&self, path: impl AsRef<[u8]>, options: OpenOptions) -> Result<File> {
        operation("filesystem.open", || self.open_from(1, path, options))
    }

    fn open_from(&self, start: u64, path: impl AsRef<[u8]>, options: OpenOptions) -> Result<File> {
        self.scope()?;
        options.validate()?;
        if options.writable {
            self.require_writable()?;
        }
        if options.truncate {
            self.charge(0, CHUNK_BYTES)?;
        }
        let path = self.path(path)?;
        // Admission and descriptor allocation precede any create/truncate.
        let file = File::reserve(self, 1, options)?;
        let inode = if self.policy.read_only {
            self.read(|db| db.open_read_only(start, &path, options))?
        } else {
            self.mutate(|db| db.open(start, &path, options, self.policy.creation_mask))?
        };
        file.record.description.lock().unwrap().inode = inode;
        Ok(file)
    }

    pub fn openat(
        &self,
        dir: &Directory,
        path: impl AsRef<[u8]>,
        options: OpenOptions,
    ) -> Result<File> {
        operation("filesystem.openat", || {
            self.open_from(dir.inode(self)?, path, options)
        })
    }

    pub fn creat(&self, path: impl AsRef<[u8]>, mode: u32) -> Result<File> {
        operation("filesystem.creat", || {
            self.open(
                path,
                OpenOptions::write_only()
                    .create(true)
                    .truncate(true)
                    .mode(mode),
            )
        })
    }

    pub fn stat(&self, path: impl AsRef<[u8]>) -> Result<Stat> {
        operation("filesystem.stat", || self.stat_from(1, path))
    }

    fn stat_from(&self, start: u64, path: impl AsRef<[u8]>) -> Result<Stat> {
        let path = self.path(path)?;
        self.read(|db| db.stat(db.resolve(start, &path)?))
    }

    pub fn lstat(&self, path: impl AsRef<[u8]>) -> Result<Stat> {
        operation("filesystem.lstat", || self.stat(path))
    }

    pub fn fstatat(
        &self,
        dir: &Directory,
        path: impl AsRef<[u8]>,
        _flags: StatFlags,
    ) -> Result<Stat> {
        operation("filesystem.fstatat", || {
            self.stat_from(dir.inode(self)?, path)
        })
    }

    pub fn mkdir(&self, path: impl AsRef<[u8]>, mode: u32) -> Result<()> {
        operation("filesystem.mkdir", || self.mkdir_from(1, path, mode))
    }

    fn mkdir_from(&self, start: u64, path: impl AsRef<[u8]>, mode: u32) -> Result<()> {
        types::validate_mode(mode)?;
        let path = self.path(path)?;
        self.mutate(|db| db.mkdir(start, &path, mode, self.policy.creation_mask))
    }

    pub fn mkdirat(&self, dir: &Directory, path: impl AsRef<[u8]>, mode: u32) -> Result<()> {
        operation("filesystem.mkdirat", || {
            self.mkdir_from(dir.inode(self)?, path, mode)
        })
    }

    pub fn unlink(&self, path: impl AsRef<[u8]>) -> Result<()> {
        operation("filesystem.unlink", || self.unlink_from(1, path, false))
    }

    pub fn rmdir(&self, path: impl AsRef<[u8]>) -> Result<()> {
        operation("filesystem.rmdir", || self.unlink_from(1, path, true))
    }

    fn unlink_from(&self, start: u64, path: impl AsRef<[u8]>, directory: bool) -> Result<()> {
        let path = self.path(path)?;
        self.mutate(|db| db.unlink(start, &path, directory))
    }

    pub fn unlinkat(
        &self,
        dir: &Directory,
        path: impl AsRef<[u8]>,
        flags: UnlinkFlags,
    ) -> Result<()> {
        operation("filesystem.unlinkat", || {
            self.unlink_from(
                dir.inode(self)?,
                path,
                matches!(flags, UnlinkFlags::RemoveDirectory),
            )
        })
    }

    pub fn rename(&self, old: impl AsRef<[u8]>, new: impl AsRef<[u8]>) -> Result<()> {
        operation("filesystem.rename", || self.rename_from(1, old, 1, new))
    }

    fn rename_from(
        &self,
        old_start: u64,
        old: impl AsRef<[u8]>,
        new_start: u64,
        new: impl AsRef<[u8]>,
    ) -> Result<()> {
        let old = self.path(old)?;
        let new = self.path(new)?;
        self.mutate(|db| db.rename(old_start, &old, new_start, &new))
    }

    pub fn renameat(
        &self,
        old_dir: &Directory,
        old: impl AsRef<[u8]>,
        new_dir: &Directory,
        new: impl AsRef<[u8]>,
    ) -> Result<()> {
        operation("filesystem.renameat", || {
            self.rename_from(old_dir.inode(self)?, old, new_dir.inode(self)?, new)
        })
    }

    pub fn truncate(&self, path: impl AsRef<[u8]>, size: u64) -> Result<()> {
        operation("filesystem.truncate", || {
            self.scope()?;
            self.charge(0, CHUNK_BYTES)?;
            let path = self.path(path)?;
            self.mutate(|db| {
                let inode = db.resolve(1, &path)?;
                db.require_permission(inode, 0o200)?;
                db.truncate(inode, size)
            })
        })
    }

    pub fn chmod(&self, path: impl AsRef<[u8]>, mode: u32) -> Result<()> {
        operation("filesystem.chmod", || {
            let path = self.path(path)?;
            self.scope()?;
            types::validate_mode(mode)?;
            self.mutate(|db| db.metadata(db.resolve(1, &path)?, Some(mode), None))
        })
    }

    pub fn utimens(&self, path: impl AsRef<[u8]>, times: Times) -> Result<()> {
        operation("filesystem.utimens", || {
            if !matches!(times.atime, Time::Omit) {
                return Err(Error::new(
                    Errno::EOPNOTSUPP,
                    "access timestamps are untracked",
                ));
            }
            let time = match times.mtime {
                Time::Omit => None,
                Time::Value(time) => Some(time),
                Time::Now => Some(
                    crate::asyncrt::wall_ms()
                        .checked_mul(1_000_000)
                        .ok_or_else(|| Error::new(Errno::EOVERFLOW, "timestamp overflow"))?,
                ),
            };
            let path = self.path(path)?;
            if time.is_none() {
                return self.read(|db| {
                    db.resolve(1, &path)?;
                    Ok(())
                });
            }
            self.mutate(|db| {
                let inode = db.resolve(1, &path)?;
                db.metadata(inode, None, time)?;
                Ok(())
            })
        })
    }

    pub fn opendir(&self, path: impl AsRef<[u8]>) -> Result<Directory> {
        operation("filesystem.opendir", || {
            self.scope()?;
            let path = self.path(path)?;
            let dir = Directory::reserve(self)?;
            let inode = self.read(|db| {
                let inode = db.resolve(1, &path)?;
                let stat = db.stat(inode)?;
                if stat.kind != Kind::Directory {
                    return Err(Error::new(Errno::ENOTDIR, "opendir requires a directory").into());
                }
                db.require_permission(inode, 0o400)?;
                Ok(inode)
            })?;
            dir.file.record.description.lock().unwrap().inode = inode;
            Ok(dir)
        })
    }

    pub fn statvfs(&self) -> Result<StatVfs> {
        operation("filesystem.statvfs", || self.read(|db| db.statvfs()))
    }

    pub async fn sync(&self) -> Result<()> {
        (async {
            self.scope()?;
            if self.storage.1.is_some() {
                return Err(Error::affinity());
            }
            self.storage.sync().await.map_err(Error::from)
        })
        .await
        .map_err(|error: Error| error.in_operation("filesystem.sync"))
    }

    pub fn read_file(&self, path: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        operation("filesystem.read_file", || {
            self.read_file_bounded(path, self.limits.max_file_helper_bytes)
        })
    }

    pub fn write_file(&self, path: impl AsRef<[u8]>, data: &[u8]) -> Result<()> {
        operation("filesystem.write_file", || {
            self.scope()?;
            if data.len() > self.limits.max_file_helper_bytes {
                return Err(Error::new(
                    Errno::EFBIG,
                    "whole-file write limit exceeded; use pwrite",
                ));
            }
            let path = self.path(path)?;
            self.charge(
                0,
                data.len().div_ceil(CHUNK_BYTES).saturating_mul(CHUNK_BYTES),
            )?;
            self.mutate(|db| {
                let inode = db.open(
                    1,
                    &path,
                    OpenOptions::write_only().create(true).truncate(true),
                    self.policy.creation_mask,
                )?;
                let mut offset = 0;
                while offset < data.len() {
                    let (count, _) = db.bounded_content().pwrite(
                        inode,
                        offset as u64,
                        &data[offset..],
                        false,
                    )?;
                    offset += count;
                }
                Ok(())
            })
        })
    }

    /// One bounded owner-side reclamation batch; retained data remains charged
    /// until it is reclaimed. Host maintenance also uses this same backend.
    pub fn collect_garbage(&self) -> Result<usize> {
        operation("filesystem.collect_garbage", || {
            self.scope()?;
            self.charge(0, self.limits.gc_batch_chunks.saturating_mul(CHUNK_BYTES))?;
            let pins = self.storage.0.resources.filesystem.pins();
            self.mutate(|db| db.collect(&pins))
        })
    }
}

pub(crate) const CHUNK_BYTES: usize = 64 * 1024;

/// Host work uses current owner authority, including after application code
/// stops using the API. File capabilities from other invocations remain pinned.
pub(crate) async fn maintain(
    context: crate::application::Context,
    installation: cellar_logic::work::Installation,
) -> anyhow::Result<crate::application::Output> {
    context.check_turn()?;
    let storage = context.storage()?;
    if storage.work_installation(cellar_logic::work::Reason::FilesystemMaintenance)?
        != Some(installation)
    {
        return Ok(crate::application::Output::Done);
    }
    let limits = crate::application::limits::current().filesystem;
    let pinned = storage.0.resources.filesystem.pins();
    let result = storage.tx(|tx| {
        tx.clear_work(installation)?;
        backend::mutate(tx.scope()?, limits, |db| {
            db.collect(&pinned)?;
            Ok(())
        })
    });
    if let Err(error) = result {
        if error
            .downcast_ref::<Error>()
            .is_some_and(|error| error.errno == Errno::EDQUOT)
        {
            storage.schedule_work(
                cellar_logic::work::Reason::FilesystemMaintenance,
                crate::asyncrt::wall_ms().saturating_add(1000).max(0),
            )?;
        } else {
            return Err(error);
        }
    }
    Ok(crate::application::Output::Done)
}
