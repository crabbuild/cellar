// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQL tokens belong to one worker-owned command/query, never its public handles.

use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Default)]
pub(super) struct RaftCursors {
    cells: Cells,
}

impl RaftCursors {
    pub(super) fn install(&self) -> Installed {
        self.cells.install()
    }

    pub(super) fn prune(&self) {
        let _installed = self.install();
        self.cells
            .sql_cursors
            .borrow_mut()
            .retain(|_, cursor| !cursor.cancelled.load(Ordering::Acquire));
    }

    pub(super) fn can_park(&self) -> bool {
        self.prune();
        self.cells.sql_cursors.borrow().is_empty()
    }

    pub(super) fn publishable(&self) -> anyhow::Result<()> {
        self.prune();
        anyhow::ensure!(
            self.cells.sql_cursors.borrow().values().all(|cursor| {
                // SAFETY: this arena owns each live statement and its command
                // still owns the connection. No cursor can step concurrently.
                unsafe { rusqlite::ffi::sqlite3_stmt_readonly(cursor.statement) != 0 }
            }),
            "cannot publish storage with an unfinished SQL write cursor; consume its rows before output or storage.sync()"
        );
        Ok(())
    }

    pub(super) fn clear(&self) {
        let _installed = self.install();
        close(RAFT_STORAGE_SCOPE);
    }
}

impl Drop for RaftCursors {
    fn drop(&mut self) {
        // The frame declares this arena before its connection/image. All raw
        // statements, including idle cache entries, die before that owner.
        self.clear();
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
