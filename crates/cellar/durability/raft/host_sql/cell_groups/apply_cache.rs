// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable prepared successors. Every entry retains its actual apply-bank
//! bytes/item permit; selectors and acknowledgement authority stay with Raft.

use super::*;
use crate::raft::cell_groups::PrefetchedApply;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct ApplyCache(Mutex<BTreeMap<GroupHistory, PrefetchedApply>>);

impl ApplyCache {
    pub(super) fn take(
        &self,
        group: GroupHistory,
    ) -> Result<Option<PrefetchedApply>, ExecutionError> {
        Ok(self
            .0
            .lock()
            .map_err(|_| ExecutionError::Invalid("poisoned apply prefetch"))?
            .remove(&group))
    }

    pub(super) fn insert(
        &self,
        group: GroupHistory,
        value: PrefetchedApply,
        active: impl FnOnce() -> Result<bool, ExecutionError>,
    ) -> Result<(), ExecutionError> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| ExecutionError::Invalid("poisoned apply prefetch"))?;
        // Retirement releases its admission lock before taking this lock. The
        // check and insertion share this lock so a retired history cannot gain
        // a late cached pin after its invalidation.
        let previous = if active()? {
            entries.insert(group, value)
        } else {
            Some(value)
        };
        drop(entries);
        drop(previous);
        Ok(())
    }

    pub(super) fn clear(&self) -> usize {
        // These are discardable immutable preparations, never unpublished
        // mutable tasks. ApplyTasks separately keeps admission through join.
        let values = std::mem::take(&mut *self.0.lock().unwrap_or_else(|error| error.into_inner()));
        let count = values.len();
        drop(values);
        count
    }
}
