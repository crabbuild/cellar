use anyhow::{ensure, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct BridgeReport {
    pub queued_bytes: usize,
    pub peak_queued_bytes: usize,
    /// Bytes reserved on each native bridge transfer; excludes Python heap,
    /// base64/JSON intermediate allocations and native transport copies.
    pub transferred_bytes: usize,
    pub budget_exhaustions: usize,
    pub pending_reservations: usize,
}

pub(crate) struct Budget {
    maximum: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    transferred: AtomicUsize,
    denied: AtomicUsize,
    pending: AtomicUsize,
}

impl Budget {
    pub fn new(maximum: usize) -> Arc<Self> {
        Arc::new(Self {
            maximum,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            transferred: AtomicUsize::new(0),
            denied: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
        })
    }

    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        let admitted = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.maximum)
            });
        if admitted.is_err() {
            self.denied.fetch_add(1, Ordering::Relaxed);
        }
        ensure!(
            admitted.is_ok(),
            "Python aggregate bridge byte budget exhausted"
        );
        self.peak
            .fetch_max(admitted.unwrap() + bytes, Ordering::Relaxed);
        self.transferred.fetch_add(bytes, Ordering::Relaxed);
        self.pending.fetch_add(1, Ordering::Relaxed);
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }

    pub fn report(&self) -> BridgeReport {
        BridgeReport {
            queued_bytes: self.used.load(Ordering::Acquire),
            peak_queued_bytes: self.peak.load(Ordering::Relaxed),
            transferred_bytes: self.transferred.load(Ordering::Relaxed),
            budget_exhaustions: self.denied.load(Ordering::Relaxed),
            pending_reservations: self.pending.load(Ordering::Acquire),
        }
    }
}

pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Release);
        self.budget.pending.fetch_sub(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_bound_aggregate_and_release_on_drop() {
        let budget = Budget::new(10);
        let first = budget.reserve(6).unwrap();
        let second = budget.reserve(4).unwrap();
        assert!(budget.reserve(1).is_err());
        assert!(budget.reserve(usize::MAX).is_err());
        drop(first);
        let third = budget.reserve(6).unwrap();
        assert_eq!(budget.report().queued_bytes, 10);
        drop((second, third));
        let report = budget.report();
        assert_eq!(report.queued_bytes, 0);
        assert_eq!(report.pending_reservations, 0);
        assert_eq!(report.peak_queued_bytes, 10);
        assert_eq!(report.budget_exhaustions, 2);
    }
}
