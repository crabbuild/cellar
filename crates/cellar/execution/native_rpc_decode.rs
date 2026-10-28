// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared native codec ownership, independent of transport byte reservations.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Usage {
    pub active_decodes: usize,
    /// Conservative decoder allowances, not measured allocations or process RSS.
    pub reserved_decode_bytes: usize,
}

pub(crate) struct Budget(Arc<Inner>);

struct Inner {
    bytes: Arc<Semaphore>,
    maximum: usize,
    active: AtomicUsize,
}

pub(crate) struct Admission {
    pool: Arc<Inner>,
    _bytes: OwnedSemaphorePermit,
    pub(crate) weight: usize,
}

impl Budget {
    pub(crate) fn new(maximum: usize) -> Self {
        Self(Arc::new(Inner {
            bytes: Arc::new(Semaphore::new(maximum)),
            maximum,
            active: AtomicUsize::new(0),
        }))
    }

    pub(crate) fn reserve(&self, weight: usize) -> Option<Admission> {
        let bytes = self
            .0
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(weight).ok()?)
            .ok()?;
        self.0.active.fetch_add(1, Ordering::SeqCst);
        Some(Admission {
            pool: self.0.clone(),
            _bytes: bytes,
            weight,
        })
    }

    pub(crate) fn usage(&self) -> Usage {
        Usage {
            active_decodes: self.0.active.load(Ordering::SeqCst),
            reserved_decode_bytes: self.0.maximum - self.0.bytes.available_permits(),
        }
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.pool.active.fetch_sub(1, Ordering::SeqCst);
        // The owned semaphore permit refunds even after its execution domain
        // closes. No clock, task or original-thread access is needed here.
    }
}
