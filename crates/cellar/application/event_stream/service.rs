// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ephemeral execution reservations belong to the serving host. Losing these
//! reservations never loses work: SQLite deadlines remain authoritative.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct Service {
    waiters: AtomicUsize,
    dispatches: Mutex<BTreeSet<(String, String)>>,
    pub(crate) metrics: Arc<crate::event_metrics::Registry>,
}

pub(crate) struct Waiter(Arc<Service>);

impl Drop for Waiter {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct Dispatch {
    service: Arc<Service>,
    identity: (String, String),
}

impl Drop for Dispatch {
    fn drop(&mut self) {
        self.service
            .dispatches
            .lock()
            .unwrap()
            .remove(&self.identity);
    }
}

impl Service {
    pub(crate) fn metrics_snapshot(&self) -> crate::event_metrics::EventMetricsSnapshot {
        self.metrics.snapshot(
            self.waiters.load(Ordering::Acquire) as u64,
            self.dispatches.lock().unwrap().len() as u64,
        )
    }

    pub(crate) fn waiter(self: &Arc<Self>, limit: usize) -> Option<Waiter> {
        self.waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                if count < limit {
                    count.checked_add(1)
                } else {
                    None
                }
            })
            .ok()
            .map(|_| Waiter(self.clone()))
    }

    pub(crate) fn dispatch(
        self: &Arc<Self>,
        scope: &str,
        lane: &str,
        limit: usize,
    ) -> Option<Dispatch> {
        let identity = (scope.to_owned(), lane.to_owned());
        let mut dispatches = self.dispatches.lock().unwrap();
        if dispatches.len() >= limit || !dispatches.insert(identity.clone()) {
            return None;
        }
        Some(Dispatch {
            service: self.clone(),
            identity,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_bound_hosts_and_lanes_and_release_on_cancellation() {
        let a = Arc::new(Service::default());
        let b = Arc::new(Service::default());
        let waiter = a.waiter(1).unwrap();
        assert!(a.waiter(1).is_none());
        assert!(b.waiter(1).is_some());
        drop(waiter);
        assert!(a.waiter(1).is_some());
        let dispatch = a.dispatch("source-a", "lane-a", 2).unwrap();
        assert!(a.dispatch("source-a", "lane-a", 2).is_none());
        let other = a.dispatch("source-b", "lane-a", 2).unwrap();
        assert!(a.dispatch("source-c", "lane-c", 2).is_none());
        drop(dispatch);
        assert!(a.dispatch("source-a", "lane-a", 2).is_some());
        drop(other);
    }
}
