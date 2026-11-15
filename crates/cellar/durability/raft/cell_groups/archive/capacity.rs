// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A failed physical admission supplies a wake hint, never a reservation.
use crate::raft::budget::{Budget, Permit};
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone)]
pub(in crate::raft::cell_groups) struct Wait {
    bank: Weak<Budget>,
    bytes: usize,
}

impl Wait {
    pub(in crate::raft::cell_groups) async fn released(self) {
        let Some(bank) = self.bank.upgrade() else {
            return;
        };
        loop {
            let mut changed = std::pin::pin!(bank.wake.notified());
            changed.as_mut().enable();
            // Register before inspecting capacity. Release can precede this
            // waiter, and another consumer may have taken its stored signal.
            let (bytes, items) = bank.usage();
            let (max_bytes, max_items) = bank.capacity();
            if items < max_items
                && bytes
                    .checked_add(self.bytes)
                    .is_some_and(|n| n <= max_bytes)
            {
                return;
            }
            changed.await;
        }
    }
}

/// One publication job records at most one actual refusal. Ordinary pool
/// clones have no capture. The weak hint neither owns credit nor pins images.
#[derive(Clone, Default)]
pub(in crate::raft::cell_groups) struct Capture(Option<Arc<Mutex<Option<Wait>>>>);

impl Capture {
    pub(in crate::raft::cell_groups) fn new() -> Self {
        Self(Some(Arc::new(Mutex::new(None))))
    }

    pub(in crate::raft::cell_groups) fn acquire(
        &self,
        bank: &Arc<Budget>,
        bytes: usize,
    ) -> Option<Permit> {
        let permit = bank.acquire(bytes);
        if permit.is_none() {
            if let Some(capture) = &self.0 {
                let (max_bytes, max_items) = bank.capacity();
                if bytes <= max_bytes && max_items != 0 {
                    *capture.lock().unwrap_or_else(|error| error.into_inner()) = Some(Wait {
                        bank: Arc::downgrade(bank),
                        bytes,
                    });
                }
            }
        }
        permit
    }

    pub(in crate::raft::cell_groups) fn take(&self) -> Option<Wait> {
        self.0.as_ref().and_then(|capture| {
            capture
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
        })
    }
}

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;
