// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Namespace persistence epochs. These prove directory entries only, never
//! file data. A publisher independently syncs/verifies every file before seal.
use super::*;
use timing::Phase;

#[derive(Default)]
struct State {
    changed: u64,
    completed: u64,
    syncing: bool,
    failed: bool,
}

#[derive(Default)]
pub(super) struct DirectoryBarrier {
    state: Mutex<State>,
    idle: Condvar,
}

struct Leader<'a> {
    inner: &'a Inner,
    complete: bool,
}

impl Drop for Leader<'_> {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = self
                .inner
                .barrier
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            state.failed = true;
            state.syncing = false;
            drop(state);
            self.inner.quarantine();
            self.inner.barrier.idle.notify_all();
        }
    }
}

impl Inner {
    /// Record after a name is created. Any later directory barrier may cover
    /// it, even while its owner is still syncing file data. Neither observation
    /// alone supplies a complete object or image seal.
    pub(super) fn namespace_changed(&self) -> Result<u64, StoreError> {
        let mut state = self
            .barrier
            .state
            .lock()
            .map_err(|_| StoreError::Quarantined)?;
        if state.failed {
            return Err(StoreError::Quarantined);
        }
        let next = state.changed.checked_add(1).ok_or(StoreError::Limit)?;
        state.changed = next;
        Ok(next)
    }

    pub(super) fn sync_directory(&self) -> Result<(), StoreError> {
        if !self.batch_directory_barriers {
            return self.sync_directory_cleanup();
        }
        self.sync_directory_through(self.namespace_changed()?)
    }

    pub(super) fn sync_directory_through(&self, required: u64) -> Result<(), StoreError> {
        let wait = timing::span(Phase::BarrierWait);
        let mut state = self
            .barrier
            .state
            .lock()
            .map_err(|_| StoreError::Quarantined)?;
        if required == 0 || required > state.changed {
            return Err(StoreError::Layout);
        }
        loop {
            if state.failed {
                return Err(StoreError::Quarantined);
            }
            if state.completed >= required {
                return Ok(());
            }
            if !state.syncing {
                break;
            }
            state = self
                .barrier
                .idle
                .wait(state)
                .map_err(|_| StoreError::Quarantined)?;
        }
        // Only names registered BEFORE this sync starts are covered. Requests
        // with a later epoch must wait for another barrier, never this one.
        let covered = state.changed;
        state.syncing = true;
        drop(state);
        drop(wait);
        let mut leader = Leader {
            inner: self,
            complete: false,
        };
        let result = {
            let _sync = timing::span(Phase::DirectorySync);
            self.filesystem.sync_all(&self.directory)
        };
        let mut state = self
            .barrier
            .state
            .lock()
            .map_err(|_| StoreError::Quarantined)?;
        state.syncing = false;
        match result {
            Ok(()) => state.completed = covered,
            Err(error) => {
                state.failed = true;
                drop(state);
                self.quarantine();
                self.barrier.idle.notify_all();
                leader.complete = true;
                return Err(error.into());
            }
        }
        drop(state);
        self.barrier.idle.notify_all();
        leader.complete = true;
        Ok(())
    }

    pub(super) fn persist_created(&self, epoch: u64) -> Result<(), StoreError> {
        if self.batch_directory_barriers {
            self.sync_directory_through(epoch)
        } else {
            self.sync_directory_cleanup()
        }
    }

    // Cleanup establishes deletion before refund even after a shared barrier
    // failed. It does not reopen admission or restore any publication proof.
    pub(super) fn sync_directory_cleanup(&self) -> Result<(), StoreError> {
        let _sync = timing::span(Phase::DirectorySync);
        Ok(self.filesystem.sync_all(&self.directory)?)
    }

    pub(super) fn sync_file(&self, file: &mut HostFile) -> Result<(), StoreError> {
        let _sync = timing::span(Phase::FileSync);
        Ok(file.sync_all()?)
    }
}
