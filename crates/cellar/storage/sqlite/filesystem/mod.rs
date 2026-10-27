// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Framework-only operations on the isolate-owned connection. Every mutation
//! has a rollback boundary, including inside an application savepoint.

use super::{with, with_application_storage_write, with_storage_batch};
use crate::application::filesystem::{
    Errno, Error, FilesystemLimits, Kind, Stat, StatVfs, CHUNK_BYTES,
};
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

mod content;
mod gc;
mod inspection;
mod namespace;
pub(crate) use inspection::inspect;

pub(crate) struct Database<'a> {
    pub connection: &'a Connection,
    pub limits: FilesystemLimits,
}

pub(crate) fn initialize(scope: &str, limits: FilesystemLimits) -> Result<()> {
    if with(scope, initialized).transpose()?.unwrap_or(false) {
        return Ok(());
    }
    mutate(scope, limits, |db| {
        let work_schema: String = db.connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name='__sys_work'",
            [],
            |row| row.get(0),
        )?;
        if !work_schema.contains("'filesystem-maintenance'") {
            return Err(Error::new(
                Errno::EOPNOTSUPP,
                "development database predates filesystem maintenance format",
            )
            .with_code("UnsupportedFormat")
            .into());
        }
        db.connection.execute_batch(include_str!("schema.sql"))?;
        db.check_quota()
    })
}

fn initialized(connection: &Connection) -> Result<bool> {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='__sys_fs_meta' AND type='table')",
        [],
        |row| row.get(0),
    )?;
    if exists {
        let format: i64 = connection.query_row(
            "SELECT format FROM __sys_fs_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        anyhow::ensure!(format == 1, "unsupported filesystem format");
    }
    Ok(exists)
}

/// Validate an already persisted filesystem before installing its connection as
/// an admitted activation. This does not initialize unused filesystem state.
pub(super) fn validate_existing(connection: &Connection) -> Result<()> {
    if initialized(connection)? {
        let resize_trigger: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='__sys_fs_inode_size'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        anyhow::ensure!(
            resize_trigger
                .as_deref()
                .is_some_and(|sql| sql.contains("logical_bytes-OLD.size+NEW.size")),
            "unsupported filesystem quota accounting format"
        );
        connection.prepare("SELECT id,kind,parent,mode,uid,gid,size,nlink,mtime_ns,ctime_ns,version,next_cookie,allocated_bytes FROM __sys_fs_inodes LIMIT 0")?;
        connection
            .prepare("SELECT inode,chunk_index,data,revision FROM __sys_fs_chunks LIMIT 0")?;
        connection.prepare("SELECT inode,first_chunk,last_chunk,cutoff,cursor FROM __sys_fs_discarded_ranges LIMIT 0")?;
        connection.prepare("SELECT id FROM __sys_fs_inodes INDEXED BY __sys_fs_live_directories WHERE parent=1 AND kind=1 AND nlink>0 ORDER BY id LIMIT 0")?;
        let root:bool=connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_fs_inodes WHERE id=1 AND kind=1 AND parent=1 AND nlink>=2)",[],|row|row.get(0))?;
        anyhow::ensure!(root, "invalid filesystem root inode");
        let work_schema: String = connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name='__sys_work'",
            [],
            |row| row.get(0),
        )?;
        anyhow::ensure!(
            work_schema.contains("'filesystem-maintenance'"),
            "unsupported filesystem maintenance format"
        );
    }
    Ok(())
}

pub(crate) fn read<T>(
    scope: &str,
    limits: FilesystemLimits,
    callback: impl FnOnce(&Database<'_>) -> Result<T>,
) -> Result<T> {
    with(scope, |connection| {
        callback(&Database { connection, limits })
    })
    .unwrap_or_else(|| Err(Error::new(Errno::ESTALE, "cell filesystem is not active").into()))
}

pub(crate) fn mutate<T>(
    scope: &str,
    limits: FilesystemLimits,
    callback: impl FnOnce(&Database<'_>) -> Result<T>,
) -> Result<T> {
    let result = with_application_storage_write(scope, |connection| {
        with_storage_batch(connection, |connection| {
            let db = Database { connection, limits };
            let before = if initialized(connection)? {
                Some(db.statvfs()?)
            } else {
                None
            };
            let result = callback(&db)?;
            db.check_quota_since(before.as_ref())?;
            db.schedule_gc(scope)?;
            Ok(result)
        })
    });
    if result.is_ok() {
        if let Some(deadline) = super::publish_alarm(scope) {
            if deadline >= 0 && crate::host_io::installed_context().is_some() {
                crate::host_gates::spawn_arm_gate(
                    scope,
                    deadline,
                    crate::host_io::installed_context(),
                );
            }
        }
    }
    result
}

impl Database<'_> {
    pub fn stat(&self, inode: u64) -> Result<Stat> {
        self.connection.query_row(
            "SELECT id,kind,mode,size,nlink,mtime_ns,ctime_ns,version,allocated_bytes,uid,gid FROM __sys_fs_inodes WHERE id=?1", [inode], |row| {
                Ok(Stat { inode: row.get(0)?, kind: if row.get::<_, i64>(1)? == 1 { Kind::Directory } else { Kind::File },
                    mode: row.get(2)?, uid: row.get(9)?, gid: row.get(10)?, size: row.get(3)?, nlink: row.get(4)?, atime_ns: 0,
                    mtime_ns: row.get(5)?, ctime_ns: row.get(6)?, version: row.get(7)?, allocated_bytes: row.get(8)? })
            }).optional()?.ok_or_else(|| Error::new(Errno::ENOENT, "inode does not exist").into())
    }

    pub fn statvfs(&self) -> Result<StatVfs> {
        let (logical, allocated, inodes): (u64, u64, u64) = self.connection.query_row(
            "SELECT logical_bytes,allocated_bytes,inode_count FROM __sys_fs_meta WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok(StatVfs {
            logical_bytes: logical,
            allocated_bytes: allocated,
            inodes,
            available_logical_bytes: self.limits.max_logical_bytes.saturating_sub(logical),
            available_allocated_bytes: self.limits.max_allocated_bytes.saturating_sub(allocated),
            available_inodes: self.limits.max_inodes.saturating_sub(inodes),
            chunk_bytes: CHUNK_BYTES,
        })
    }

    pub fn check_quota(&self) -> Result<()> {
        self.check_quota_since(None)
    }

    /// Admit positive deltas before SQLite's arithmetic can promote an
    /// overflowing INTEGER counter to REAL. Zero growth remains legal when
    /// a lowered quota is already below usage, including reclamation.
    pub(super) fn check_growth(&self, logical: u64, allocated: u64, inodes: u64) -> Result<()> {
        if logical == 0 && allocated == 0 && inodes == 0 {
            return Ok(());
        }
        let available = self.statvfs()?;
        if logical > available.available_logical_bytes
            || allocated > available.available_allocated_bytes
            || inodes > available.available_inodes
        {
            return Err(Error::new(Errno::EDQUOT, "cell filesystem quota exceeded").into());
        }
        Ok(())
    }

    fn check_quota_since(&self, before: Option<&StatVfs>) -> Result<()> {
        // The schema creation callback calls this before mutate's common check.
        let stats = self.statvfs()?;
        if (stats.logical_bytes > self.limits.max_logical_bytes
            && before.is_none_or(|before| stats.logical_bytes > before.logical_bytes))
            || (stats.allocated_bytes > self.limits.max_allocated_bytes
                && before.is_none_or(|before| stats.allocated_bytes > before.allocated_bytes))
            || (stats.inodes > self.limits.max_inodes
                && before.is_none_or(|before| stats.inodes > before.inodes))
        {
            return Err(Error::new(Errno::EDQUOT, "cell filesystem quota exceeded").into());
        }
        Ok(())
    }

    pub fn revision(&self) -> Result<u64> {
        let revision: u64 = self.connection.query_row(
            "SELECT revision FROM __sys_fs_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if revision >= i64::MAX as u64 {
            return Err(Error::new(Errno::EOVERFLOW, "filesystem revision exhausted").into());
        }
        self.connection.execute(
            "UPDATE __sys_fs_meta SET revision=revision+1 WHERE singleton=1",
            [],
        )?;
        Ok(revision + 1)
    }

    pub fn metadata(&self, inode: u64, mode: Option<u32>, mtime_ns: Option<i64>) -> Result<()> {
        if mode.is_some_and(|mode| mode & !0o777 != 0) {
            return Err(Error::new(Errno::EOPNOTSUPP, "unsupported mode bits").into());
        }
        self.stat(inode)?;
        self.revision()?;
        self.connection.execute("UPDATE __sys_fs_inodes SET mode=COALESCE(?2,mode),mtime_ns=COALESCE(?3,mtime_ns),ctime_ns=?4 WHERE id=?1",
            params![inode,mode,mtime_ns,now_ns()?])?;
        Ok(())
    }
}

pub(super) fn now_ns() -> Result<i64> {
    crate::asyncrt::wall_ms()
        .checked_mul(1_000_000)
        .ok_or_else(|| Error::new(Errno::EOVERFLOW, "filesystem timestamp overflow").into())
}

/// Clear guards run even when the API was never opened in this activation.
pub(super) fn check_clear(connection: &Connection) -> Result<()> {
    if !initialized(connection)? {
        return Ok(());
    }
    let present: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM __sys_fs_inodes WHERE id<>1) OR EXISTS(SELECT 1 FROM __sys_fs_discarded_ranges)", [], |row| row.get(0))?;
    if present {
        return Err(Error::new(
            Errno::EBUSY,
            "filesystem must be empty before clearing business state",
        )
        .with_code("FilesystemPresent")
        .into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "../filesystem_tests.rs"]
mod tests;
