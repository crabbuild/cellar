// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::Database;
use anyhow::Result;
use rusqlite::{params, OptionalExtension};

impl Database<'_> {
    pub fn pending_gc(&self) -> Result<bool> {
        Ok(self.connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_fs_discarded_ranges) OR EXISTS(SELECT 1 FROM __sys_fs_inodes WHERE nlink=0)",[],|row|row.get(0))?)
    }

    pub(super) fn schedule_gc(&self, scope: &str) -> Result<()> {
        if self.pending_gc()? {
            // Install once. Ordinary writes cannot postpone already scheduled
            // reclamation or invalidate the identity an admitted job captured.
            let deadline = crate::asyncrt::wall_ms().saturating_add(100).max(0);
            self.connection.execute("INSERT INTO __sys_work(scope,reason,generation,at_ms) VALUES(?1,'filesystem-maintenance',1,?2)
                ON CONFLICT(scope,reason) DO UPDATE SET generation=__sys_work.generation+1,at_ms=excluded.at_ms WHERE __sys_work.at_ms IS NULL",params![scope,deadline])?;
        }
        Ok(())
    }

    /// Reclaim a bounded batch. `pinned` contains every currently live inode
    /// capability in this activation, including directory-relative handles.
    /// No capability survives activation replacement, so recovery supplies an
    /// empty pin set and resumes persisted work.
    pub fn collect(&self, pinned: &std::collections::HashSet<u64>) -> Result<usize> {
        let budget = self.limits.gc_batch_chunks;
        let range: Option<(u64,u64,u64,u64,u64)> = self.connection.query_row(
            "SELECT id,inode,last_chunk,cutoff,cursor FROM __sys_fs_discarded_ranges ORDER BY id LIMIT 1", [],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).optional()?;
        if let Some((id, inode, last, cutoff, cursor)) = range {
            let chunks = self.connection.prepare("SELECT chunk_index,revision FROM __sys_fs_chunks WHERE inode=?1 AND chunk_index BETWEEN ?2 AND ?3 ORDER BY chunk_index LIMIT ?4")?
                .query_map(params![inode,cursor,last,budget], |row| Ok((row.get::<_,u64>(0)?,row.get::<_,u64>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for (index, revision) in &chunks {
                if *revision <= cutoff {
                    self.connection.execute(
                        "DELETE FROM __sys_fs_chunks WHERE inode=?1 AND chunk_index=?2",
                        params![inode, index],
                    )?;
                }
            }
            if chunks.len() < budget || chunks.last().is_some_and(|(index, _)| *index == last) {
                self.connection
                    .execute("DELETE FROM __sys_fs_discarded_ranges WHERE id=?1", [id])?;
            } else if let Some((index, _)) = chunks.last() {
                self.connection.execute(
                    "UPDATE __sys_fs_discarded_ranges SET cursor=?2 WHERE id=?1",
                    params![id, index + 1],
                )?;
            }
            return Ok(chunks.len().max(1));
        }
        // An open directory retains its parent chain for directory-relative
        // `..`, including after rmdir of the directory and its ancestors.
        let mut protected = pinned.clone();
        let mut ancestors = std::collections::HashSet::new();
        for inode in pinned {
            if self.stat(*inode)?.kind != crate::application::filesystem::Kind::Directory {
                continue;
            }
            let mut current = *inode;
            while current != 1 {
                if !ancestors.insert(current) {
                    break;
                }
                if ancestors.len() > self.limits.max_namespace_directories {
                    return Err(crate::application::filesystem::Error::new(
                        crate::application::filesystem::Errno::EDQUOT,
                        "directory pin ancestry exceeds bounded maintenance work",
                    )
                    .into());
                }
                current = self.parent(current)?;
                protected.insert(current);
            }
        }
        // At most protected.len() candidates can be blocked. The handle and
        // ancestry ceilings bound the extra scan without starving later orphans.
        let orphans = self
            .connection
            .prepare("SELECT id FROM __sys_fs_inodes WHERE nlink=0 ORDER BY id LIMIT ?1")?
            .query_map([budget.saturating_add(protected.len())], |row| {
                row.get::<_, u64>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut remaining = budget;
        for inode in orphans {
            if protected.contains(&inode) {
                continue;
            }
            let indices=self.connection.prepare("SELECT chunk_index FROM __sys_fs_chunks WHERE inode=?1 ORDER BY chunk_index LIMIT ?2")?
                .query_map(params![inode,remaining],|row|row.get::<_,u64>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let complete = indices.len() < remaining;
            for index in &indices {
                self.connection.execute(
                    "DELETE FROM __sys_fs_chunks WHERE inode=?1 AND chunk_index=?2",
                    params![inode, index],
                )?;
            }
            remaining -= indices.len();
            if complete && remaining > 0 {
                self.connection.execute(
                    "DELETE FROM __sys_fs_inodes WHERE id=?1 AND nlink=0",
                    [inode],
                )?;
                remaining -= 1;
            }
            if remaining == 0 {
                break;
            }
        }
        Ok(budget - remaining)
    }
}
