// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQL tokens belong to one worker-owned command/query, never its public handles.

use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Default)]
pub(super) struct RaftCursors {
    cells: Cells,
    images: RefCell<Vec<CursorImage>>,
}

struct CursorImage {
    database: *mut rusqlite::ffi::sqlite3,
    close: Box<dyn FnOnce() -> anyhow::Result<()>>,
}

impl RaftCursors {
    pub(super) fn install(&self) -> Installed {
        self.cells.install()
    }

    pub(super) fn prune(&self) -> anyhow::Result<()> {
        let _installed = self.install();
        self.cells
            .sql_cursors
            .borrow_mut()
            .retain(|_, cursor| !cursor.cancelled.load(Ordering::Acquire));
        let mut retained = Vec::new();
        let mut result = Ok(());
        for image in self.images.take() {
            if self.uses(image.database) {
                retained.push(image);
            } else {
                result = result.and((image.close)());
            }
        }
        *self.images.borrow_mut() = retained;
        result
    }

    fn uses(&self, database: *mut rusqlite::ffi::sqlite3) -> bool {
        self.cells
            .sql_cursors
            .borrow()
            .values()
            .any(|cursor| cursor.database == database)
    }

    /// The caller transfers the entire physical image owner, including its
    /// connection, VFS, lock and disk reservation. Only read VMs can reach here.
    pub(super) fn retain_image(
        &self,
        database: *mut rusqlite::ffi::sqlite3,
        close: Box<dyn FnOnce() -> anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        if self.uses(database) {
            self.images
                .borrow_mut()
                .push(CursorImage { database, close });
            Ok(())
        } else {
            close()
        }
    }

    pub(super) fn park(&self) {
        let _installed = self.install();
        // A returning old VM must never enter the new connection's cache.
        for cursor in self.cells.sql_cursors.borrow_mut().values_mut() {
            cursor.cache_query = None;
        }
        close_sql_statement_cache(RAFT_STORAGE_SCOPE);
    }

    pub(super) fn publishable(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.parkable()?,
            "cannot publish storage with an unfinished SQL write cursor; consume its rows before output or storage.sync()"
        );
        Ok(())
    }

    pub(super) fn parkable(&self) -> anyhow::Result<bool> {
        self.prune()?;
        Ok(self.cells.sql_cursors.borrow().values().all(|cursor| {
            // SAFETY: this arena owns each live statement and its command
            // still owns the connection. No cursor can step concurrently.
            unsafe { rusqlite::ffi::sqlite3_stmt_readonly(cursor.statement) != 0 }
        }))
    }

    pub(super) fn clear(&self) -> anyhow::Result<()> {
        let _installed = self.install();
        close(RAFT_STORAGE_SCOPE);
        self.prune()
    }
}

impl Drop for RaftCursors {
    fn drop(&mut self) {
        // The frame declares this arena before its connection/image. All raw
        // statements, including idle cache entries, die before that owner.
        let _ = self.clear();
    }
}

pub(crate) fn raft_sql_cursor_lifetime(id: u64) -> Option<Arc<AtomicBool>> {
    sql_cursors(|cursors| {
        cursors
            .borrow()
            .get(&id)
            .map(|cursor| cursor.cancelled.clone())
    })
}
