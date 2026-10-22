use anyhow::{ensure, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct BridgeReport {
    /// Outstanding native payload charges. Raw HTTP bodies retain their
    /// base64-equivalent charge, which is at least their owned byte length.
    pub queued_bytes: usize,
    pub peak_queued_bytes: usize,
    /// Sum of successful byte reservations, including later-abandoned work.
    pub reserved_bytes_total: usize,
    /// Explicit owned UTF-8/body copies from Python: SDK name/payload,
    /// completion metadata and raw response body.
    /// Excludes Python's own codec/UTF-8 caches and native transport copies.
    pub input_copied_bytes: usize,
    /// Explicit owned UTF-8 and body copies into Python: invocation metadata,
    /// raw request bodies and SDK reply payloads.
    pub output_copied_bytes: usize,
    pub budget_exhaustions: usize,
    pub pending_reservations: usize,
    pub operations: super::OperationReport,
}

pub(crate) struct Budget {
    pub metrics: Arc<super::telemetry::Metrics>,
    maximum: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    reserved_total: AtomicUsize,
    input_copied: AtomicUsize,
    output_copied: AtomicUsize,
    denied: AtomicUsize,
    pending: AtomicUsize,
}

impl Budget {
    pub fn new(maximum: usize) -> Arc<Self> {
        Arc::new(Self {
            metrics: Arc::default(),
            maximum,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            reserved_total: AtomicUsize::new(0),
            input_copied: AtomicUsize::new(0),
            output_copied: AtomicUsize::new(0),
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
        self.reserved_total.fetch_add(bytes, Ordering::Relaxed);
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
            reserved_bytes_total: self.reserved_total.load(Ordering::Relaxed),
            input_copied_bytes: self.input_copied.load(Ordering::Relaxed),
            output_copied_bytes: self.output_copied.load(Ordering::Relaxed),
            budget_exhaustions: self.denied.load(Ordering::Relaxed),
            pending_reservations: self.pending.load(Ordering::Acquire),
            operations: self.metrics.report(),
        }
    }

    pub fn copied_input(&self, bytes: usize) {
        self.input_copied.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn copied_output(&self, bytes: usize) {
        self.output_copied.fetch_add(bytes, Ordering::Relaxed);
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
