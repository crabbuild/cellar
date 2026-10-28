// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{now_ns, Database, CHUNK_BYTES};
use crate::application::filesystem::{Errno, Error, Kind};
use anyhow::Result;
use rusqlite::{params, OptionalExtension};

impl Database<'_> {
    /// Whole-file helpers have their own checked byte/work bound. Do not split
    /// them into tiny descriptor transfers that repeatedly rewrite the same
    /// chunk and evade the enclosing transaction's charged chunk-work limit.
    pub fn bounded_content(&self) -> Database<'_> {
        Database {
            connection: self.connection,
            limits: crate::application::filesystem::FilesystemLimits {
                max_transfer_bytes: self.limits.max_file_helper_bytes,
                ..self.limits
            },
        }
    }

    fn regular_size(&self, inode: u64) -> Result<u64> {
        let stat = self.stat(inode)?;
        if stat.kind != Kind::File {
            return Err(Error::new(Errno::EISDIR, "regular file operation on a directory").into());
        }
        Ok(stat.size)
    }

    fn logical_chunk(&self, inode: u64, index: u64) -> Result<Option<Vec<u8>>> {
        // Range metadata stays until bounded GC has visited every old chunk.
        // A rewritten chunk has a newer revision and remains visible.
        Ok(self.connection.query_row("SELECT c.data FROM __sys_fs_chunks c WHERE c.inode=?1 AND c.chunk_index=?2 AND NOT EXISTS(
            SELECT 1 FROM __sys_fs_discarded_ranges r WHERE r.inode=c.inode AND c.chunk_index BETWEEN r.first_chunk AND r.last_chunk AND c.revision<=r.cutoff)",
            params![inode,index], |row| row.get(0)).optional()?)
    }

    pub fn pread(&self, inode: u64, offset: u64, bytes: &mut [u8]) -> Result<usize> {
        if offset > i64::MAX as u64 {
            return Err(Error::new(Errno::EOVERFLOW, "read offset exceeds integer range").into());
        }
        let size = self.regular_size(inode)?;
        let count = bytes
            .len()
            .min(self.limits.max_transfer_bytes)
            .min(size.saturating_sub(offset).min(usize::MAX as u64) as usize);
        let bytes = &mut bytes[..count];
        bytes.fill(0);
        let mut copied = 0;
        while copied < count {
            let position = offset + copied as u64;
            let index = position / CHUNK_BYTES as u64;
            let inside = (position % CHUNK_BYTES as u64) as usize;
            let length = (CHUNK_BYTES - inside).min(count - copied);
            if let Some(chunk) = self.logical_chunk(inode, index)? {
                if chunk.len() != CHUNK_BYTES {
                    return Err(Error::new(Errno::EIO, "invalid filesystem chunk length").into());
                }
                bytes[copied..copied + length].copy_from_slice(&chunk[inside..inside + length]);
            }
            copied += length;
        }
        Ok(count)
    }

    pub fn pwrite(
        &self,
        inode: u64,
        offset: u64,
        bytes: &[u8],
        append: bool,
    ) -> Result<(usize, u64)> {
        let size = self.regular_size(inode)?;
        if offset > i64::MAX as u64 {
            return Err(Error::new(Errno::EOVERFLOW, "write offset exceeds integer range").into());
        }
        let offset = if append { size } else { offset };
        if bytes.is_empty() {
            return Ok((0, offset));
        }
        let count = bytes.len().min(self.limits.max_transfer_bytes);
        let end = offset
            .checked_add(count as u64)
            .filter(|end| *end <= self.limits.max_file_bytes && *end <= i64::MAX as u64)
            .ok_or_else(|| Error::new(Errno::EFBIG, "file size limit exceeded"))?;
        let first = offset / CHUNK_BYTES as u64;
        let last = (end - 1) / CHUNK_BYTES as u64;
        // Count physical rows, including truncated rows awaiting GC: replacing
        // one does not allocate another chunk. The accepted transfer bounds
        // this indexed count independently of total file size.
        let existing: u64 = self.connection.query_row(
            "SELECT count(*) FROM __sys_fs_chunks WHERE inode=?1 AND chunk_index BETWEEN ?2 AND ?3",
            params![inode, first, last],
            |row| row.get(0),
        )?;
        let allocated = (last - first + 1 - existing) * CHUNK_BYTES as u64;
        self.check_growth(end.saturating_sub(size), allocated, 0)?;
        let revision = self.revision()?;
        let mut copied = 0;
        while copied < count {
            let position = offset + copied as u64;
            let index = position / CHUNK_BYTES as u64;
            let inside = (position % CHUNK_BYTES as u64) as usize;
            let length = (CHUNK_BYTES - inside).min(count - copied);
            let mut chunk = if inside == 0 && length == CHUNK_BYTES {
                vec![0; CHUNK_BYTES]
            } else {
                self.logical_chunk(inode, index)?
                    .unwrap_or_else(|| vec![0; CHUNK_BYTES])
            };
            if chunk.len() != CHUNK_BYTES {
                return Err(Error::new(Errno::EIO, "invalid filesystem chunk length").into());
            }
            chunk[inside..inside + length].copy_from_slice(&bytes[copied..copied + length]);
            self.connection.execute("INSERT INTO __sys_fs_chunks VALUES(?1,?2,?3,?4) ON CONFLICT(inode,chunk_index) DO UPDATE SET data=excluded.data,revision=excluded.revision",
                params![inode,index,chunk,revision])?;
            copied += length;
        }
        self.connection.execute("UPDATE __sys_fs_inodes SET size=MAX(size,?2),version=?3,mtime_ns=?4,ctime_ns=?4 WHERE id=?1", params![inode,end,revision,now_ns()?])?;
        Ok((count, end))
    }

    pub fn truncate(&self, inode: u64, size: u64) -> Result<()> {
        let old = self.regular_size(inode)?;
        if size > self.limits.max_file_bytes || size > i64::MAX as u64 {
            return Err(Error::new(Errno::EFBIG, "file size limit exceeded").into());
        }
        self.check_growth(size.saturating_sub(old), 0, 0)?;
        let revision = self.revision()?;
        if size < old {
            let first = size.div_ceil(CHUNK_BYTES as u64);
            let last = (old - 1) / CHUNK_BYTES as u64;
            if first <= last {
                let count: u64 = self.connection.query_row(
                    "SELECT count(*) FROM __sys_fs_discarded_ranges",
                    [],
                    |row| row.get(0),
                )?;
                if count >= self.limits.max_discarded_ranges as u64 {
                    return Err(
                        Error::new(Errno::EDQUOT, "truncate reclamation backlog is full").into(),
                    );
                }
                self.connection.execute("INSERT INTO __sys_fs_discarded_ranges(inode,first_chunk,last_chunk,cutoff,cursor) VALUES(?1,?2,?3,?4,?2)", params![inode,first,last,revision])?;
            }
            let tail = (size % CHUNK_BYTES as u64) as usize;
            if tail != 0 {
                let index = size / CHUNK_BYTES as u64;
                if let Some(mut chunk) = self.logical_chunk(inode, index)? {
                    if chunk.len() != CHUNK_BYTES {
                        return Err(
                            Error::new(Errno::EIO, "invalid filesystem chunk length").into()
                        );
                    }
                    chunk[tail..].fill(0);
                    self.connection.execute("UPDATE __sys_fs_chunks SET data=?3,revision=?4 WHERE inode=?1 AND chunk_index=?2", params![inode,index,chunk,revision])?;
                }
            }
        }
        self.connection.execute(
            "UPDATE __sys_fs_inodes SET size=?2,version=?3,mtime_ns=?4,ctime_ns=?4 WHERE id=?1",
            params![inode, size, revision, now_ns()?],
        )?;
        Ok(())
    }
}
