//! Byte and item admission retained through queued work and event delivery.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) struct Budget {
    pub wake: tokio::sync::Notify,
    bytes: AtomicUsize,
    items: AtomicUsize,
    max_bytes: usize,
    max_items: usize,
}

impl Budget {
    pub(crate) fn usage(&self) -> (usize, usize) {
        (
            self.bytes.load(Ordering::Acquire),
            self.items.load(Ordering::Acquire),
        )
    }

    pub fn new(max_bytes: usize, max_items: usize) -> Arc<Self> {
        Arc::new(Self {
            wake: tokio::sync::Notify::new(),
            bytes: AtomicUsize::new(0),
            items: AtomicUsize::new(0),
            max_bytes,
            max_items,
        })
    }

    pub fn acquire(self: &Arc<Self>, bytes: usize) -> Option<Permit> {
        self.items
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(1).filter(|n| *n <= self.max_items)
            })
            .ok()?;
        if self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|n| *n <= self.max_bytes)
            })
            .is_err()
        {
            self.items.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Permit {
            owner: self.clone(),
            bytes,
        })
    }
}

pub(crate) struct Permit {
    owner: Arc<Budget>,
    bytes: usize,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.owner.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.owner.items.fetch_sub(1, Ordering::AcqRel);
        self.owner.wake.notify_one();
    }
}
