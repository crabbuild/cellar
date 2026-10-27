// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only aggregate sampling with fixed VM/time bounds, independent of quota.
use super::*;
use crate::filesystem_state::FilesystemUsage;

const MAX_STEPS: u32 = 1_000_000;
const MAX_MS: u64 = 100;

struct Budget<'a>(&'a Connection);

impl Drop for Budget<'_> {
    fn drop(&mut self) {
        self.0
            .progress_handler(0, None::<fn() -> bool>)
            .expect("filesystem observation owns its connection handle");
    }
}

pub(crate) fn inspect(scope: &str) -> Result<Option<FilesystemUsage>> {
    with(scope, |connection| {
        anyhow::ensure!(connection.is_autocommit(), "filesystem observation cannot sample an open transaction");
        let started = crate::asyncrt::mono_ms();
        let mut steps = 0u32;
        connection.progress_handler(1000, Some(move || {
            steps += 1000;
            steps >= MAX_STEPS || crate::asyncrt::mono_ms().saturating_sub(started) >= MAX_MS
        }))?;
        let _budget = Budget(connection);
        if !initialized(connection)? {
            return Ok(None);
        }
        let mut usage = connection.query_row(
            "SELECT revision,logical_bytes,allocated_bytes,inode_count FROM __sys_fs_meta WHERE singleton=1", [],
            |row| Ok(FilesystemUsage { revision: row.get(0)?, logical_bytes: row.get(1)?,
                allocated_bytes: row.get(2)?, inodes: row.get(3)?, ..Default::default() }))?;
        let (inodes, logical, allocated) = connection.query_row(
            "SELECT count(*),coalesce(sum(size),0),coalesce(sum(allocated_bytes),0) FROM __sys_fs_inodes WHERE nlink=0", [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
        usage.unlinked_inodes = inodes;
        usage.unlinked_logical_bytes = logical;
        usage.unlinked_allocated_bytes = allocated;
        usage.discarded_ranges = connection.query_row("SELECT count(*) FROM __sys_fs_discarded_ranges", [], |row| row.get(0))?;
        // Drive from the bounded range table, not every live file/chunk. A
        // rewritten chunk is excluded, and overlapping cutoffs count it once.
        let discarded: u64 = connection.query_row(
            "SELECT count(*) FROM (SELECT c.inode,c.chunk_index FROM __sys_fs_discarded_ranges r
             JOIN __sys_fs_inodes i ON i.id=r.inode AND i.nlink>0
             JOIN __sys_fs_chunks c ON c.inode=r.inode AND c.chunk_index BETWEEN r.first_chunk AND r.last_chunk AND c.revision<=r.cutoff
             GROUP BY c.inode,c.chunk_index)", [], |row| row.get(0))?;
        usage.discarded_allocated_bytes = discarded.checked_mul(CHUNK_BYTES as u64)
            .ok_or_else(|| anyhow::anyhow!("filesystem discarded-byte accounting overflow"))?;
        Ok(Some(usage))
    }).unwrap_or_else(|| Err(Error::new(Errno::ESTALE, "filesystem observation has no active owner").into()))
}
