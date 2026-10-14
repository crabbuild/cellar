//! Fixed-size, execution-domain observations. These are not a command ledger.

use serde::{Serialize, Serializer};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const BOUNDS_MS: [u64; 12] = [
    1,
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    5_000,
    30_000,
    u64::MAX,
];

#[derive(Clone, Copy)]
pub(crate) enum Counter {
    DispatchStarted,
    DispatchSucceeded,
    DispatchFailed,
    DispatchCancelled,
    AppendCalls,
    AppendedRecords,
    DeduplicatedRecords,
    AckCalls,
    AckSucceeded,
    AckFailed,
    AckCancelled,
    AckReplayed,
    CursorExpired,
    WakeDiscoveryFailed,
    TimingUnavailable,
}

const COUNTERS: [Counter; 15] = [
    Counter::DispatchStarted,
    Counter::DispatchSucceeded,
    Counter::DispatchFailed,
    Counter::DispatchCancelled,
    Counter::AppendCalls,
    Counter::AppendedRecords,
    Counter::DeduplicatedRecords,
    Counter::AckCalls,
    Counter::AckSucceeded,
    Counter::AckFailed,
    Counter::AckCancelled,
    Counter::AckReplayed,
    Counter::CursorExpired,
    Counter::WakeDiscoveryFailed,
    Counter::TimingUnavailable,
];

impl Counter {
    fn name(self) -> &'static str {
        match self {
            Self::DispatchStarted => "cellar_event_dispatch_rpc_attempts_total",
            Self::DispatchSucceeded => "cellar_event_dispatch_rpc_succeeded_total",
            Self::DispatchFailed => "cellar_event_dispatch_rpc_failed_total",
            Self::DispatchCancelled => "cellar_event_dispatch_rpc_cancelled_total",
            Self::AppendCalls => "cellar_event_append_calls_total",
            Self::AppendedRecords => "cellar_event_appended_records_total",
            Self::DeduplicatedRecords => "cellar_event_deduplicated_records_total",
            Self::AckCalls => "cellar_event_ack_calls_total",
            Self::AckSucceeded => "cellar_event_ack_processed_total",
            Self::AckFailed => "cellar_event_ack_failed_total",
            Self::AckCancelled => "cellar_event_ack_cancelled_total",
            Self::AckReplayed => "cellar_event_ack_replayed_total",
            Self::CursorExpired => "cellar_event_cursor_expired_total",
            Self::WakeDiscoveryFailed => "cellar_event_wake_discovery_failed_total",
            Self::TimingUnavailable => "cellar_event_timing_unavailable_total",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Histogram {
    Dispatch,
    SourceToAppend,
    Ack,
}

const HISTOGRAMS: [Histogram; 3] = [
    Histogram::Dispatch,
    Histogram::SourceToAppend,
    Histogram::Ack,
];

impl Histogram {
    fn name(self) -> &'static str {
        match self {
            Self::Dispatch => "cellar_event_dispatch_rpc_duration_seconds",
            Self::SourceToAppend => "cellar_event_source_to_append_seconds",
            Self::Ack => "cellar_event_ack_duration_seconds",
        }
    }
}

fn decimal<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

#[derive(Clone, Debug, Serialize)]
pub struct EventCounter {
    pub name: &'static str,
    #[serde(serialize_with = "decimal")]
    pub value: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventBucket {
    /// None is the positive-infinity bucket; counts are cumulative.
    pub upper_ms: Option<u64>,
    #[serde(serialize_with = "decimal")]
    pub count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventHistogram {
    pub name: &'static str,
    pub buckets: Vec<EventBucket>,
    #[serde(serialize_with = "decimal")]
    pub sum_ms: u64,
    #[serde(serialize_with = "decimal")]
    pub count: u64,
}

/// Host-lifetime operation observations, with fixed metric names and bucket sets.
/// Snapshots are live atomic observations, not a cross-counter transaction.
#[derive(Clone, Debug, Serialize)]
pub struct EventMetricsSnapshot {
    pub counters: Vec<EventCounter>,
    pub histograms: Vec<EventHistogram>,
    #[serde(serialize_with = "decimal")]
    pub active_waiters: u64,
    #[serde(serialize_with = "decimal")]
    pub inflight_dispatches: u64,
}

fn seconds(ms: u64) -> String {
    format!("{}.{:03}", ms / 1_000, ms % 1_000)
}

impl EventMetricsSnapshot {
    /// Prometheus text with no event, cell, stream, subscription or caller labels.
    pub fn prometheus(&self) -> String {
        use std::fmt::Write;
        let mut text = String::new();
        for counter in &self.counters {
            let _ = writeln!(
                text,
                "# TYPE {} counter\n{} {}",
                counter.name, counter.name, counter.value
            );
        }
        for histogram in &self.histograms {
            let _ = writeln!(text, "# TYPE {} histogram", histogram.name);
            for bucket in &histogram.buckets {
                let bound = bucket
                    .upper_ms
                    .map(seconds)
                    .unwrap_or_else(|| "+Inf".into());
                let _ = writeln!(
                    text,
                    "{}_bucket{{le=\"{}\"}} {}",
                    histogram.name, bound, bucket.count
                );
            }
            let _ = writeln!(
                text,
                "{}_sum {}\n{}_count {}",
                histogram.name,
                seconds(histogram.sum_ms),
                histogram.name,
                histogram.count
            );
        }
        for (name, value) in [
            ("cellar_event_active_waiters", self.active_waiters),
            ("cellar_event_inflight_dispatches", self.inflight_dispatches),
        ] {
            let _ = writeln!(text, "# TYPE {name} gauge\n{name} {value}");
        }
        text
    }
}

#[derive(Default)]
struct Samples {
    buckets: [AtomicU64; 12],
    sum_ms: AtomicU64,
}

#[derive(Default)]
pub(crate) struct Registry {
    counters: [AtomicU64; 15],
    histograms: [Samples; 3],
}

fn add(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

impl Registry {
    pub(crate) fn increment(&self, counter: Counter, amount: u64) {
        add(&self.counters[counter as usize], amount);
    }

    pub(crate) fn observe(&self, histogram: Histogram, ms: u64) {
        let samples = &self.histograms[histogram as usize];
        let bucket = BOUNDS_MS
            .iter()
            .position(|bound| ms <= *bound)
            .expect("infinity bucket");
        add(&samples.buckets[bucket], 1);
        add(&samples.sum_ms, ms);
    }

    pub(crate) fn snapshot(
        &self,
        active_waiters: u64,
        inflight_dispatches: u64,
    ) -> EventMetricsSnapshot {
        EventMetricsSnapshot {
            counters: COUNTERS
                .iter()
                .map(|counter| EventCounter {
                    name: counter.name(),
                    value: self.counters[*counter as usize].load(Ordering::Relaxed),
                })
                .collect(),
            histograms: HISTOGRAMS
                .iter()
                .map(|histogram| {
                    let samples = &self.histograms[*histogram as usize];
                    let mut count = 0u64;
                    let buckets = BOUNDS_MS
                        .iter()
                        .zip(&samples.buckets)
                        .map(|(bound, bucket)| {
                            count = count.saturating_add(bucket.load(Ordering::Relaxed));
                            EventBucket {
                                upper_ms: (*bound != u64::MAX).then_some(*bound),
                                count,
                            }
                        })
                        .collect();
                    EventHistogram {
                        name: histogram.name(),
                        buckets,
                        sum_ms: samples.sum_ms.load(Ordering::Relaxed),
                        count,
                    }
                })
                .collect(),
            active_waiters,
            inflight_dispatches,
        }
    }

    pub(crate) fn timer(
        self: &Arc<Self>,
        operation: Histogram,
        clock: Option<crate::asyncrt::DomainToken>,
    ) -> Timer {
        let started = clock.as_ref().and_then(|clock| clock.mono_ms().ok());
        let counter = match operation {
            Histogram::Dispatch => Counter::DispatchStarted,
            Histogram::Ack => Counter::AckCalls,
            Histogram::SourceToAppend => unreachable!("source age is not a host-clock timer"),
        };
        self.increment(counter, 1);
        Timer {
            registry: self.clone(),
            clock,
            started,
            operation,
            outcome: None,
        }
    }
}

pub(crate) struct Timer {
    registry: Arc<Registry>,
    clock: Option<crate::asyncrt::DomainToken>,
    started: Option<u64>,
    operation: Histogram,
    outcome: Option<bool>,
}

impl Timer {
    pub(crate) fn finish(mut self, succeeded: bool) {
        self.outcome = Some(succeeded);
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let counter = match (self.operation, self.outcome) {
            (Histogram::Dispatch, Some(true)) => Counter::DispatchSucceeded,
            (Histogram::Dispatch, Some(false)) => Counter::DispatchFailed,
            (Histogram::Dispatch, None) => Counter::DispatchCancelled,
            (Histogram::Ack, Some(true)) => Counter::AckSucceeded,
            (Histogram::Ack, Some(false)) => Counter::AckFailed,
            (Histogram::Ack, None) => Counter::AckCancelled,
            _ => unreachable!("source age is not a host-clock timer"),
        };
        self.registry.increment(counter, 1);
        if let Some((start, now)) = self
            .started
            .zip(self.clock.as_ref().and_then(|clock| clock.mono_ms().ok()))
        {
            self.registry
                .observe(self.operation, now.saturating_sub(start));
        } else {
            self.registry.increment(Counter::TimingUnavailable, 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_are_bounded_saturating_and_preserve_integer_precision() {
        let registry = Registry::default();
        registry.increment(Counter::AppendCalls, u64::MAX);
        registry.increment(Counter::AppendCalls, 1);
        for ms in [0, 5, 6, 30_001] {
            registry.observe(Histogram::Dispatch, ms);
        }
        let snapshot = registry.snapshot(2, 3);
        assert_eq!(snapshot.counters.len(), 15);
        assert_eq!(snapshot.histograms.len(), 3);
        let dispatch = &snapshot.histograms[0];
        assert_eq!(dispatch.count, 4);
        assert_eq!(dispatch.buckets[0].count, 1);
        assert_eq!(dispatch.buckets[1].count, 2);
        assert_eq!(dispatch.buckets.last().unwrap().count, 4);
        let json = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(json["counters"][4]["value"], u64::MAX.to_string());
        let text = snapshot.prometheus();
        assert!(text.contains("cellar_event_dispatch_rpc_duration_seconds_bucket{le=\"+Inf\"} 4"));
        assert!(text.contains("cellar_event_dispatch_rpc_duration_seconds_sum 30.012"));
        assert!(
            !text.contains("scope=")
                && !text.contains("subscription=")
                && !text.contains("resource=")
        );
    }

    #[tokio::test]
    async fn cancellation_and_clock_observations_retain_their_host_across_domains(
    ) -> anyhow::Result<()> {
        let a = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let b = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let timer = a
            .run(async {
                let services = crate::asyncrt::services();
                services
                    .events()
                    .metrics
                    .timer(Histogram::Dispatch, services.domain_token())
            })
            .await?;
        b.run(async move {
            drop(timer);
        })
        .await?;
        let snapshot = a.services().event_metrics();
        assert_eq!(
            snapshot.counters[Counter::DispatchCancelled as usize].value,
            1
        );
        assert_eq!(snapshot.histograms[Histogram::Dispatch as usize].count, 1);
        assert!(b
            .services()
            .event_metrics()
            .counters
            .iter()
            .all(|counter| counter.value == 0));
        let timer = a
            .run(async {
                let services = crate::asyncrt::services();
                services
                    .events()
                    .metrics
                    .timer(Histogram::Ack, services.domain_token())
            })
            .await?;
        a.shutdown().await;
        drop(timer);
        assert_eq!(
            a.services().event_metrics().counters[Counter::TimingUnavailable as usize].value,
            1
        );
        b.shutdown().await;
        Ok(())
    }
}
