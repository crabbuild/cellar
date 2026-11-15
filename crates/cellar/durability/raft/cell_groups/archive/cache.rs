// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Idle verification is evictable; active output proofs keep their own custody.

use super::CellGroupArchiveCoverage;
use std::sync::{Arc, Mutex, Weak};

pub(in crate::raft::cell_groups) struct CachedCoverage {
    coverage: Mutex<Option<CellGroupArchiveCoverage>>,
}

impl CachedCoverage {
    pub(in crate::raft::cell_groups) fn new(coverage: CellGroupArchiveCoverage) -> Arc<Self> {
        Arc::new(Self {
            coverage: Mutex::new(Some(coverage)),
        })
    }

    pub(in crate::raft::cell_groups) fn take(&self) -> Option<CellGroupArchiveCoverage> {
        self.coverage
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }
}

/// Each physical bank remembers at most one idle owner, shared across its clones.
/// Admission can evict that cache without borrowing another controller's slot.
#[derive(Clone, Default)]
pub(in crate::raft::cell_groups) struct CacheEviction(Arc<Mutex<Weak<CachedCoverage>>>);

impl CacheEviction {
    pub(in crate::raft::cell_groups) fn remember(&self, cache: &Arc<CachedCoverage>) {
        let previous = {
            let mut current = self.0.lock().unwrap_or_else(|error| error.into_inner());
            std::mem::replace(&mut *current, Arc::downgrade(cache))
        };
        if let Some(previous) = previous.upgrade().filter(|old| !Arc::ptr_eq(old, cache)) {
            // Release physical admission outside both cache locks. Real users
            // retain independent coverage clones and cannot be evicted here.
            drop(previous.take());
        }
    }

    pub(in crate::raft::cell_groups) fn evict(&self) {
        let previous = {
            let mut current = self.0.lock().unwrap_or_else(|error| error.into_inner());
            std::mem::take(&mut *current)
        };
        if let Some(previous) = previous.upgrade() {
            drop(previous.take());
        }
    }
}
