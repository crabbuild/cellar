// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
    pub(crate) fn capacity(&self) -> (usize, usize) {
        (self.max_bytes, self.max_items)
    }

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

impl Permit {
    /// Extend an admitted operation without consuming another item slot.
    /// Failure leaves its existing reservation unchanged.
    pub(crate) fn grow_to(&mut self, bytes: usize) -> bool {
        let Some(extra) = bytes.checked_sub(self.bytes) else {
            return false;
        };
        if self
            .owner
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(extra).filter(|n| *n <= self.owner.max_bytes)
            })
            .is_err()
        {
            return false;
        }
        self.bytes = bytes;
        true
    }

    /// Refund transient capture capacity while keeping the operation slot and
    /// the retained payload charged. A reservation can never grow here.
    pub(crate) fn shrink_to(&mut self, bytes: usize) -> bool {
        if bytes > self.bytes {
            return false;
        }
        let released = self.bytes - bytes;
        self.bytes = bytes;
        self.owner.bytes.fetch_sub(released, Ordering::AcqRel);
        if released != 0 {
            self.owner.wake.notify_one();
        }
        true
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.owner.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.owner.items.fetch_sub(1, Ordering::AcqRel);
        self.owner.wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grow_preserves_item_and_refuses_without_changing_accounting() {
        let budget = Budget::new(100, 2);
        let mut pin = budget.acquire(10).unwrap();
        let other = budget.acquire(70).unwrap();
        assert!(pin.grow_to(30));
        assert_eq!(budget.usage(), (100, 2));
        assert!(!pin.grow_to(31));
        assert!(!pin.grow_to(29));
        assert_eq!(budget.usage(), (100, 2));
        drop(other);
        assert!(pin.grow_to(100));
        drop(pin);
        assert_eq!(budget.usage(), (0, 0));
    }

    #[test]
    fn shrink_refunds_bytes_but_keeps_the_operation_slot() {
        let budget = Budget::new(100, 2);
        let mut capture = budget.acquire(100).unwrap();
        assert!(budget.acquire(1).is_none());
        assert!(capture.shrink_to(20));
        let next = budget.acquire(80).unwrap();
        assert_eq!(budget.usage(), (100, 2));
        assert!(budget.acquire(0).is_none());
        assert!(!capture.shrink_to(21));
        assert_eq!(budget.usage(), (100, 2));
        drop(capture);
        assert_eq!(budget.usage(), (80, 1));
        drop(next);
        assert_eq!(budget.usage(), (0, 0));
    }
}
