//! Fixed-size observations of Raft work and I/O APIs, never durability proofs.
use serde::{Serialize, Serializer};

pub(crate) fn decimal<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

fn optional_decimal<S: Serializer>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RaftPerformanceCounter {
    pub name: &'static str,
    #[serde(serialize_with = "decimal")]
    pub value: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RaftDurationBucket {
    /// Inclusive microsecond upper bound; None is positive infinity.
    #[serde(serialize_with = "optional_decimal")]
    pub upper_us: Option<u64>,
    #[serde(serialize_with = "decimal")]
    pub count: u64,
}

/// One operation's counters and histogram are sampled under the same short lock.
/// Duration covers all outcomes with an available owner clock. An abandoned
/// timer means unwinding, cancellation or discarded queued work, not rollback.
#[derive(Clone, Debug, Serialize)]
pub struct RaftOperationPerformance {
    pub name: &'static str,
    #[serde(serialize_with = "decimal")]
    pub started: u64,
    #[serde(serialize_with = "decimal")]
    pub succeeded: u64,
    #[serde(serialize_with = "decimal")]
    pub failed: u64,
    #[serde(serialize_with = "decimal")]
    pub abandoned: u64,
    #[serde(serialize_with = "decimal")]
    pub timing_unavailable: u64,
    #[serde(serialize_with = "decimal")]
    pub count: u64,
    #[serde(serialize_with = "decimal")]
    pub sum_us: u64,
    pub buckets: Vec<RaftDurationBucket>,
}

/// Bounded cumulative observations from one host startup or standalone engine open.
/// `source` changes on reopen, even if the configured process identity is reused.
/// Cross-operation samples are not transactional. Unavailable or saturated
/// samples cannot support a measured delta. Histograms give bucket bounds, not
/// exact quantiles. Bytes describe filesystem/object APIs and sockets below TLS,
/// not device writes or network delivery. Controller timings describe attempts.
#[derive(Clone, Debug, Serialize)]
pub struct RaftPerformanceSnapshot {
    pub source: String,
    pub available: bool,
    pub saturated: bool,
    #[serde(serialize_with = "optional_decimal")]
    pub sampled_at_us: Option<u64>,
    pub counters: Vec<RaftPerformanceCounter>,
    pub operations: Vec<RaftOperationPerformance>,
}

fn seconds(us: u64) -> String {
    format!("{}.{:06}", us / 1_000_000, us % 1_000_000)
}

impl RaftPerformanceSnapshot {
    /// Fixed metric vocabulary with no cell, group, peer or source labels.
    pub fn prometheus(&self) -> String {
        use std::fmt::Write;
        let mut text = String::new();
        for (name, value) in [("available", self.available), ("saturated", self.saturated)] {
            let _ = writeln!(
                text,
                "# TYPE cellar_raft_performance_{name} gauge\ncellar_raft_performance_{name} {}",
                u8::from(value)
            );
        }
        for counter in &self.counters {
            let _ = writeln!(
                text,
                "# TYPE cellar_raft_{}_total counter\ncellar_raft_{}_total {}",
                counter.name, counter.name, counter.value
            );
        }
        for operation in &self.operations {
            for (outcome, value) in [
                ("started", operation.started),
                ("succeeded", operation.succeeded),
                ("failed", operation.failed),
                ("abandoned", operation.abandoned),
                ("timing_unavailable", operation.timing_unavailable),
            ] {
                let _ = writeln!(text,"# TYPE cellar_raft_{}_{outcome}_total counter\ncellar_raft_{}_{outcome}_total {value}",operation.name,operation.name);
            }
            let name = format!("cellar_raft_{}_duration_seconds", operation.name);
            let _ = writeln!(text, "# TYPE {name} histogram");
            for bucket in &operation.buckets {
                let bound = bucket
                    .upper_us
                    .map(seconds)
                    .unwrap_or_else(|| "+Inf".into());
                let _ = writeln!(text, "{name}_bucket{{le=\"{bound}\"}} {}", bucket.count);
            }
            let _ = writeln!(
                text,
                "{name}_sum {}\n{name}_count {}",
                seconds(operation.sum_us),
                operation.count
            );
        }
        text
    }
}
