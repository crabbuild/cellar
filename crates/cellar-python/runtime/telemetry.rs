//! Bounded observations of bridge work; these values never confer authority.
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const BOUNDS: [u64; 8] = [
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
];

/// Fixed, noncumulative latency buckets. The last bucket has no upper bound.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct LatencyReport {
    pub samples: u64,
    pub total_ns: u64,
    pub maximum_ns: u64,
    pub bucket_counts: [u64; 9],
}

impl LatencyReport {
    /// Inclusive nanosecond upper bounds, in bucket order; `None` is infinity.
    pub fn upper_bounds_ns() -> [Option<u64>; 9] {
        std::array::from_fn(|index| BOUNDS.get(index).copied())
    }
}

/// Live fields are independently sampled. Conservation holds after quiescence.
/// Failed or abandoned operations can be fully accounted for without proving
/// that their mutations rolled back or that application work succeeded.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct OperationReport {
    pub admitted: u64,
    pub started: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub delivered: u64,
    pub abandoned_queued: u64,
    pub abandoned_running: u64,
    pub undelivered: u64,
    pub pending: u64,
    pub peak_pending: u64,
    pub queued: u64,
    pub running: u64,
    pub replies_pending: u64,
    /// Admission to the native driver's first poll.
    pub queue_wait: LatencyReport,
    /// Native parse/validation/SDK await time, including dropped futures.
    pub sdk_duration: LatencyReport,
    /// Native SDK completion to managed-loop dequeue, including result encoding.
    pub reply_wait: LatencyReport,
    /// Admission to managed-loop dequeue; excludes Python future consumption.
    pub round_trip: LatencyReport,
}

impl OperationReport {
    /// All admitted work has an observed terminal disposition. Not a durability
    /// proof, and not an indication that abandoned effects were undone.
    pub fn settled(&self) -> bool {
        self.pending == 0
            && self.queued == 0
            && self.running == 0
            && self.replies_pending == 0
            && self.admitted
                == self
                    .delivered
                    .saturating_add(self.abandoned_queued)
                    .saturating_add(self.abandoned_running)
                    .saturating_add(self.undelivered)
            && self.started
                == self
                    .succeeded
                    .saturating_add(self.failed)
                    .saturating_add(self.abandoned_running)
    }
}

#[derive(Default)]
struct Histogram {
    samples: AtomicU64,
    total: AtomicU64,
    maximum: AtomicU64,
    buckets: [AtomicU64; 9],
}

impl Histogram {
    fn observe(&self, duration: Duration) {
        let ns = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.samples.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_add(ns))
            });
        self.maximum.fetch_max(ns, Ordering::Relaxed);
        self.buckets[BOUNDS.partition_point(|bound| *bound < ns)].fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) -> LatencyReport {
        LatencyReport {
            samples: self.samples.load(Ordering::Acquire),
            total_ns: self.total.load(Ordering::Relaxed),
            maximum_ns: self.maximum.load(Ordering::Relaxed),
            bucket_counts: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
        }
    }
}

#[derive(Default)]
pub(crate) struct Metrics {
    admitted: AtomicU64,
    started: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    delivered: AtomicU64,
    abandoned_queued: AtomicU64,
    abandoned_running: AtomicU64,
    undelivered: AtomicU64,
    pending: AtomicU64,
    peak_pending: AtomicU64,
    queued: AtomicU64,
    running: AtomicU64,
    replies: AtomicU64,
    queue_wait: Histogram,
    sdk_duration: Histogram,
    reply_wait: Histogram,
    round_trip: Histogram,
}

impl Metrics {
    /// The origin is captured from the original execution domain before the
    /// invocation enters Python. Sampling its elapsed time on either thread
    /// does not install a domain, acquire a GIL or invoke an SDK operation.
    pub fn admit(self: &Arc<Self>, origin: Instant) -> Operation {
        self.admitted.fetch_add(1, Ordering::Relaxed);
        self.queued.fetch_add(1, Ordering::Relaxed);
        let pending = self.pending.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak_pending.fetch_max(pending, Ordering::Relaxed);
        Operation {
            metrics: self.clone(),
            origin,
            admitted: origin.elapsed(),
            stage: Stage::Queued,
        }
    }

    pub fn report(&self) -> OperationReport {
        OperationReport {
            admitted: self.admitted.load(Ordering::Acquire),
            started: self.started.load(Ordering::Acquire),
            succeeded: self.succeeded.load(Ordering::Acquire),
            failed: self.failed.load(Ordering::Acquire),
            delivered: self.delivered.load(Ordering::Acquire),
            abandoned_queued: self.abandoned_queued.load(Ordering::Acquire),
            abandoned_running: self.abandoned_running.load(Ordering::Acquire),
            undelivered: self.undelivered.load(Ordering::Acquire),
            pending: self.pending.load(Ordering::Acquire),
            peak_pending: self.peak_pending.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Acquire),
            running: self.running.load(Ordering::Acquire),
            replies_pending: self.replies.load(Ordering::Acquire),
            queue_wait: self.queue_wait.report(),
            sdk_duration: self.sdk_duration.report(),
            reply_wait: self.reply_wait.report(),
            round_trip: self.round_trip.report(),
        }
    }
}

enum Stage {
    Queued,
    Running(Duration),
    Reply(Duration),
    Delivered,
}

/// Moves with the queued event, original native future, then reply. Drop is
/// nonblocking and records abandoned work even if Python never resumes.
pub(crate) struct Operation {
    metrics: Arc<Metrics>,
    origin: Instant,
    admitted: Duration,
    stage: Stage,
}

impl Operation {
    pub fn begin(&mut self) {
        let now = self.origin.elapsed();
        self.metrics
            .queue_wait
            .observe(now.saturating_sub(self.admitted));
        self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
        self.metrics.running.fetch_add(1, Ordering::Relaxed);
        self.metrics.started.fetch_add(1, Ordering::Relaxed);
        self.stage = Stage::Running(now);
    }

    pub fn complete(&mut self, succeeded: bool) {
        let now = self.origin.elapsed();
        if let Stage::Running(started) = self.stage {
            self.metrics
                .sdk_duration
                .observe(now.saturating_sub(started));
        }
        self.metrics.running.fetch_sub(1, Ordering::AcqRel);
        self.metrics.replies.fetch_add(1, Ordering::Relaxed);
        if succeeded {
            &self.metrics.succeeded
        } else {
            &self.metrics.failed
        }
        .fetch_add(1, Ordering::Relaxed);
        self.stage = Stage::Reply(now);
    }

    pub fn delivered(mut self) {
        let now = self.origin.elapsed();
        if let Stage::Reply(completed) = self.stage {
            self.metrics
                .reply_wait
                .observe(now.saturating_sub(completed));
        }
        self.metrics
            .round_trip
            .observe(now.saturating_sub(self.admitted));
        self.metrics.replies.fetch_sub(1, Ordering::AcqRel);
        self.metrics.delivered.fetch_add(1, Ordering::Relaxed);
        self.stage = Stage::Delivered;
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        match self.stage {
            Stage::Queued => {
                self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
                self.metrics
                    .abandoned_queued
                    .fetch_add(1, Ordering::Relaxed);
            }
            Stage::Running(started) => {
                self.metrics
                    .sdk_duration
                    .observe(self.origin.elapsed().saturating_sub(started));
                self.metrics.running.fetch_sub(1, Ordering::AcqRel);
                self.metrics
                    .abandoned_running
                    .fetch_add(1, Ordering::Relaxed);
            }
            Stage::Reply(_) => {
                self.metrics.replies.fetch_sub(1, Ordering::AcqRel);
                self.metrics.undelivered.fetch_add(1, Ordering::Relaxed);
            }
            Stage::Delivered => {}
        }
        self.metrics.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[path = "telemetry_tests.rs"]
mod tests;
