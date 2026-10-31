// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Byte and item admission retained through queued work and event delivery.

use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Default)]
struct Usage {
    bytes: usize,
    items: usize,
}

pub(crate) struct Budget {
    pub wake: tokio::sync::Notify,
    usage: Mutex<Usage>,
    max_bytes: usize,
    max_items: usize,
}

impl Budget {
    pub(crate) fn capacity(&self) -> (usize, usize) {
        (self.max_bytes, self.max_items)
    }

    pub(crate) fn usage(&self) -> (usize, usize) {
        let usage = self.lock();
        (usage.bytes, usage.items)
    }

    fn lock(&self) -> MutexGuard<'_, Usage> {
        self.usage.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn released(&self) {
        // Requests have different byte sizes. A waiter that still cannot fit
        // must not consume another request's only opportunity to make progress.
        self.wake.notify_waiters();
        // Workers also inspect capacity before registering their select arm.
        // Keep a stored signal when release precedes that registration.
        self.wake.notify_one();
    }

    pub fn new(max_bytes: usize, max_items: usize) -> Arc<Self> {
        Arc::new(Self {
            wake: tokio::sync::Notify::new(),
            usage: Mutex::new(Usage::default()),
            max_bytes,
            max_items,
        })
    }

    pub fn acquire(self: &Arc<Self>, bytes: usize) -> Option<Permit> {
        // Reserve both dimensions together. A rejected byte request must never
        // temporarily exhaust the item count seen by a fitting caller.
        let mut usage = self.lock();
        let next_bytes = usage
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.max_bytes)?;
        let next_items = usage
            .items
            .checked_add(1)
            .filter(|n| *n <= self.max_items)?;
        *usage = Usage {
            bytes: next_bytes,
            items: next_items,
        };
        drop(usage);
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
        let mut usage = self.owner.lock();
        let Some(next) = usage
            .bytes
            .checked_add(extra)
            .filter(|n| *n <= self.owner.max_bytes)
        else {
            return false;
        };
        usage.bytes = next;
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
        let mut usage = self.owner.lock();
        usage.bytes -= released;
        self.bytes = bytes;
        drop(usage);
        if released != 0 {
            self.owner.released();
        }
        true
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut usage = self.owner.lock();
        usage.bytes -= self.bytes;
        usage.items -= 1;
        drop(usage);
        self.owner.released();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    fn fitting_waiter_after_partial_release(shrink: bool) {
        let budget = Budget::new(100, 3);
        let mut held = budget.acquire(80).unwrap();
        let mut other = Some(budget.acquire(20).unwrap());
        let mut large = std::pin::pin!(budget.wake.notified());
        large.as_mut().enable();
        let mut small = std::pin::pin!(budget.wake.notified());
        small.as_mut().enable();
        assert!(budget.acquire(40).is_none());
        assert!(budget.acquire(20).is_none());
        if shrink {
            assert!(held.shrink_to(60));
        } else {
            drop(other.take());
        }
        assert_eq!(budget.usage().0, 80);
        // The first registered request still cannot use the released bytes.
        // A fitting request must be woken without another capacity change.
        assert!(large.as_mut().now_or_never().is_some());
        assert!(budget.acquire(40).is_none());
        assert!(
            small.as_mut().now_or_never().is_some(),
            "a non-fitting waiter must not consume the fitting request's only wake"
        );
        let admitted = budget.acquire(20).unwrap();
        assert_eq!(budget.usage().0, 100);
        drop(admitted);
        drop(other);
        drop(held);
        assert_eq!(budget.usage(), (0, 0));
    }

    #[test]
    fn release_wakes_fitting_waiter_behind_a_larger_request() {
        fitting_waiter_after_partial_release(false);
    }

    #[test]
    fn shrink_wakes_fitting_waiter_behind_a_larger_request() {
        fitting_waiter_after_partial_release(true);
    }

    #[test]
    fn late_worker_observes_release_without_prior_registration() {
        let budget = Budget::new(100, 1);
        let held = budget.acquire(100).unwrap();
        drop(held);
        assert!(budget.wake.notified().now_or_never().is_some());
        assert_eq!(budget.usage(), (0, 0));
    }

    #[test]
    fn nonfitting_attempts_do_not_consume_available_item_capacity() {
        let budget = Budget::new(100, 2);
        let held = budget.acquire(80).unwrap();
        let start = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                start.wait();
                for _ in 0..100_000 {
                    assert!(budget.acquire(30).is_none());
                }
            });
            start.wait();
            for _ in 0..100_000 {
                let fitting = budget.acquire(20);
                assert!(
                    fitting.is_some(),
                    "a rejected request must not charge an item against a fitting caller"
                );
                drop(fitting);
            }
            worker.join().unwrap();
        });
        drop(held);
        assert_eq!(budget.usage(), (0, 0));
    }

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
