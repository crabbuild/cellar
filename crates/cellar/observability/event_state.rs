// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fixed-cardinality owner samples and checked fleet aggregation.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Gauge {
    Sources,
    OutboxRows,
    OutboxBytes,
    OutboxOldestAgeMs,
    BlockedForbidden,
    BlockedDestinationDeleted,
    BlockedSequenceGap,
    BlockedPayloadTooLarge,
    BlockedUnsupportedProtocol,
    BlockedConfigConflict,
    BlockedProducerSealed,
    Streams,
    RetiredStreams,
    StreamRows,
    StreamBytes,
    StreamEarliestOffset,
    StreamEndOffset,
    ActiveLeases,
    ExpiredSubscriptions,
    ProtectedRecords,
    ProtectedBytes,
    RetentionBlockedBytes,
    Subscriptions,
    SubscriptionLagRecords,
    SubscriptionRetainedLagRecords,
    SubscriptionOldestAgeMs,
    SqliteAllocatedBytes,
    SqliteUsedBytes,
}

impl Gauge {
    pub const ALL: [Self; 28] = [
        Self::Sources,
        Self::OutboxRows,
        Self::OutboxBytes,
        Self::OutboxOldestAgeMs,
        Self::BlockedForbidden,
        Self::BlockedDestinationDeleted,
        Self::BlockedSequenceGap,
        Self::BlockedPayloadTooLarge,
        Self::BlockedUnsupportedProtocol,
        Self::BlockedConfigConflict,
        Self::BlockedProducerSealed,
        Self::Streams,
        Self::RetiredStreams,
        Self::StreamRows,
        Self::StreamBytes,
        Self::StreamEarliestOffset,
        Self::StreamEndOffset,
        Self::ActiveLeases,
        Self::ExpiredSubscriptions,
        Self::ProtectedRecords,
        Self::ProtectedBytes,
        Self::RetentionBlockedBytes,
        Self::Subscriptions,
        Self::SubscriptionLagRecords,
        Self::SubscriptionRetainedLagRecords,
        Self::SubscriptionOldestAgeMs,
        Self::SqliteAllocatedBytes,
        Self::SqliteUsedBytes,
    ];

    fn maximum(self) -> bool {
        matches!(
            self,
            Self::OutboxOldestAgeMs | Self::SubscriptionOldestAgeMs
        )
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Sources => "sources",
            Self::OutboxRows => "outbox_rows",
            Self::OutboxBytes => "outbox_bytes",
            Self::OutboxOldestAgeMs => "outbox_oldest_age_ms",
            Self::BlockedForbidden => "blocked_forbidden_lanes",
            Self::BlockedDestinationDeleted => "blocked_destination_deleted_lanes",
            Self::BlockedSequenceGap => "blocked_sequence_gap_lanes",
            Self::BlockedPayloadTooLarge => "blocked_payload_too_large_lanes",
            Self::BlockedUnsupportedProtocol => "blocked_unsupported_protocol_lanes",
            Self::BlockedConfigConflict => "blocked_config_conflict_lanes",
            Self::BlockedProducerSealed => "blocked_producer_sealed_lanes",
            Self::Streams => "streams",
            Self::RetiredStreams => "retired_streams",
            Self::StreamRows => "stream_rows",
            Self::StreamBytes => "stream_bytes",
            Self::StreamEarliestOffset => "stream_earliest_offset_sum",
            Self::StreamEndOffset => "stream_end_offset_sum",
            Self::ActiveLeases => "active_leases",
            Self::ExpiredSubscriptions => "expired_subscriptions",
            Self::ProtectedRecords => "protected_records",
            Self::ProtectedBytes => "protected_bytes",
            Self::RetentionBlockedBytes => "retention_blocked_bytes",
            Self::Subscriptions => "subscriptions",
            Self::SubscriptionLagRecords => "subscription_lag_records",
            Self::SubscriptionRetainedLagRecords => "subscription_retained_lag_records",
            Self::SubscriptionOldestAgeMs => "subscription_oldest_age_ms",
            Self::SqliteAllocatedBytes => "sqlite_allocated_bytes",
            Self::SqliteUsedBytes => "sqlite_used_bytes",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateSample {
    pub format: u8,
    pub scope: String,
    #[serde(with = "crate::application::event_stream::decimal")]
    pub sampled_ms: i64,
    /// Canonical decimal values keep structured RPC's binary64 codec out of counters.
    pub gauges: BTreeMap<Gauge, String>,
}

impl StateSample {
    pub(crate) fn new(scope: String, sampled_ms: i64) -> Self {
        Self {
            format: 1,
            scope,
            sampled_ms,
            gauges: Gauge::ALL.into_iter().map(|g| (g, "0".into())).collect(),
        }
    }

    pub(crate) fn set(&mut self, gauge: Gauge, value: i64) -> Result<()> {
        ensure!(value >= 0, "negative event state gauge");
        self.gauges.insert(gauge, value.to_string());
        Ok(())
    }

    pub(crate) fn set_wide(&mut self, gauge: Gauge, value: u128) {
        self.gauges.insert(gauge, value.to_string());
    }

    pub fn values(&self, scope: &str) -> Result<BTreeMap<Gauge, u128>> {
        ensure!(
            self.format == 1
                && self.scope == scope
                && cellar_logic::cell::valid_cell_scope(scope)
                && self.sampled_ms >= 0,
            "event state sample identity conflicts"
        );
        ensure!(
            self.gauges.len() == Gauge::ALL.len(),
            "incomplete event state gauge set"
        );
        let values = Gauge::ALL
            .into_iter()
            .map(|g| {
                let raw = self
                    .gauges
                    .get(&g)
                    .ok_or_else(|| anyhow::anyhow!("event state gauge missing"))?;
                let n = raw.parse::<u128>()?;
                ensure!(n.to_string() == *raw, "noncanonical event state gauge");
                Ok((g, n))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        for (g, n) in &values {
            let maximum = if matches!(
                g,
                Gauge::SubscriptionLagRecords | Gauge::SubscriptionRetainedLagRecords
            ) {
                128u128 * i64::MAX as u128
            } else {
                i64::MAX as u128
            };
            ensure!(*n <= maximum, "event state value exceeds durable bounds");
        }
        ensure!(
            values[&Gauge::Sources] <= 1
                && values[&Gauge::Streams] <= 1
                && values[&Gauge::RetiredStreams] <= values[&Gauge::Streams]
                && values[&Gauge::ActiveLeases] <= 128
                && values[&Gauge::Subscriptions] <= 128,
            "event state cardinality exceeds current schema"
        );
        ensure!(
            values[&Gauge::SqliteUsedBytes] <= values[&Gauge::SqliteAllocatedBytes]
                && values[&Gauge::ProtectedBytes] <= values[&Gauge::StreamBytes]
                && values[&Gauge::RetentionBlockedBytes] <= values[&Gauge::ProtectedBytes]
                && values[&Gauge::SubscriptionRetainedLagRecords]
                    <= values[&Gauge::SubscriptionLagRecords],
            "inconsistent event state accounting"
        );
        ensure!(
            values[&Gauge::OutboxBytes] >= values[&Gauge::OutboxRows]
                && (values[&Gauge::OutboxBytes] == 0) == (values[&Gauge::OutboxRows] == 0),
            "outbox state byte/row conflict"
        );
        ensure!(
            values[&Gauge::OutboxOldestAgeMs] <= self.sampled_ms as u128
                && values[&Gauge::SubscriptionOldestAgeMs] <= self.sampled_ms as u128,
            "state sample age exceeds its clock"
        );
        if values[&Gauge::Sources] == 0 {
            for g in [
                Gauge::OutboxRows,
                Gauge::OutboxBytes,
                Gauge::OutboxOldestAgeMs,
                Gauge::BlockedForbidden,
                Gauge::BlockedDestinationDeleted,
                Gauge::BlockedSequenceGap,
                Gauge::BlockedPayloadTooLarge,
                Gauge::BlockedUnsupportedProtocol,
                Gauge::BlockedConfigConflict,
                Gauge::BlockedProducerSealed,
            ] {
                ensure!(values[&g] == 0, "absent source has outbox state");
            }
        }
        if values[&Gauge::Streams] == 1 {
            ensure!(
                values[&Gauge::StreamEarliestOffset] >= 1
                    && values[&Gauge::StreamEndOffset] >= values[&Gauge::StreamEarliestOffset]
                    && values[&Gauge::StreamRows]
                        == values[&Gauge::StreamEndOffset] - values[&Gauge::StreamEarliestOffset]
                    && values[&Gauge::StreamBytes] >= values[&Gauge::StreamRows],
                "stream state offset conflict"
            );
            ensure!(
                values[&Gauge::ProtectedRecords] <= values[&Gauge::StreamRows]
                    && values[&Gauge::ActiveLeases] <= values[&Gauge::Subscriptions]
                    && values[&Gauge::ExpiredSubscriptions] <= values[&Gauge::Subscriptions]
                    && values[&Gauge::SubscriptionRetainedLagRecords]
                        <= values[&Gauge::Subscriptions] * values[&Gauge::StreamRows]
                    && values[&Gauge::SubscriptionLagRecords]
                        <= values[&Gauge::Subscriptions] * (values[&Gauge::StreamEndOffset] - 1),
                "state sample subscription accounting conflicts"
            );
        } else {
            for g in [
                Gauge::StreamRows,
                Gauge::StreamBytes,
                Gauge::StreamEarliestOffset,
                Gauge::StreamEndOffset,
                Gauge::ActiveLeases,
                Gauge::ExpiredSubscriptions,
                Gauge::ProtectedRecords,
                Gauge::ProtectedBytes,
                Gauge::RetentionBlockedBytes,
                Gauge::Subscriptions,
                Gauge::SubscriptionLagRecords,
                Gauge::SubscriptionRetainedLagRecords,
                Gauge::SubscriptionOldestAgeMs,
            ] {
                ensure!(values[&g] == 0, "non-stream sample contains stream state");
            }
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_acceptance_never_partially_changes_fleet_totals() -> Result<()> {
        let mut sample = StateSample::new("Order:one".into(), 100);
        sample.set(Gauge::Sources, 1)?;
        sample.set(Gauge::OutboxRows, 2)?;
        sample.set(Gauge::OutboxBytes, 80)?;
        sample.set(Gauge::OutboxOldestAgeMs, 30)?;
        let mut totals = StateTotals::default();
        totals.add(&sample, "Order:one")?;
        let before = totals.json(false);
        sample.gauges.insert(Gauge::OutboxBytes, "01".into());
        assert!(totals.add(&sample, "Order:one").is_err());
        assert_eq!(totals.json(false), before);
        assert!(totals.add(&sample, "Order:other").is_err());
        sample.gauges.remove(&Gauge::Sources);
        assert!(sample.values("Order:one").is_err());
        Ok(())
    }

    #[test]
    fn gauge_sums_are_exact_above_i64_and_ages_take_maximum() -> Result<()> {
        let mut sample = StateSample::new("Order:one".into(), 100);
        sample.set(Gauge::Sources, 1)?;
        sample.set(Gauge::OutboxRows, 1)?;
        sample.set(Gauge::OutboxBytes, i64::MAX)?;
        sample.set(Gauge::OutboxOldestAgeMs, 30)?;
        let mut totals = StateTotals::default();
        totals.add(&sample, "Order:one")?;
        sample.scope = "Order:two".into();
        sample.set(Gauge::OutboxOldestAgeMs, 10)?;
        totals.add(&sample, "Order:two")?;
        assert_eq!(
            totals.json(true)["gauges"]["outbox_bytes"],
            (2u128 * i64::MAX as u128).to_string()
        );
        assert_eq!(totals.json(true)["gauges"]["outbox_oldest_age_ms"], "30");
        let text = totals.prometheus(false);
        assert!(text.contains("collection_complete 0"));
        assert!(!text.contains("cellar_event_fleet_outbox_bytes"));
        assert!(totals.json(false)["gauges"].is_null());
        assert!(!text.contains("Order:") && !text.contains("{resource="));
        Ok(())
    }
}

#[derive(Default)]
pub struct StateTotals {
    values: BTreeMap<Gauge, u128>,
    pub observed: usize,
    pub failed: usize,
}

impl StateTotals {
    pub fn add(&mut self, sample: &StateSample, scope: &str) -> Result<()> {
        // Validate and calculate the whole addition before replacing any total.
        let values = sample.values(scope)?;
        let mut next = self.values.clone();
        for (g, n) in values {
            let old = next.get(&g).copied().unwrap_or(0);
            next.insert(
                g,
                if g.maximum() {
                    old.max(n)
                } else {
                    old.checked_add(n)
                        .ok_or_else(|| anyhow::anyhow!("fleet gauge overflow"))?
                },
            );
        }
        let observed = self
            .observed
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("fleet sample count overflow"))?;
        self.values = next;
        self.observed = observed;
        Ok(())
    }

    pub fn json(&self, complete: bool) -> serde_json::Value {
        let gauges = Gauge::ALL
            .into_iter()
            .map(|g| {
                (
                    g.name(),
                    self.values.get(&g).copied().unwrap_or(0).to_string(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        serde_json::json!({"format":1,"complete":complete,"observed":self.observed,"unavailable":self.failed,
            "gauges":if complete{Some(&gauges)}else{None},"observed_gauges":gauges})
    }

    pub fn prometheus(&self, complete: bool) -> String {
        use std::fmt::Write;
        let mut text = String::new();
        for (name, value) in [
            ("collection_complete", usize::from(complete)),
            ("observed_roots", self.observed),
            ("unavailable_roots", self.failed),
        ] {
            let _ = writeln!(
                text,
                "# TYPE cellar_event_fleet_{name} gauge\ncellar_event_fleet_{name} {value}"
            );
        }
        if !complete {
            return text;
        }
        for g in Gauge::ALL {
            let name = g.name();
            let n = self.values.get(&g).copied().unwrap_or(0);
            let _ = writeln!(
                text,
                "# TYPE cellar_event_fleet_{name} gauge\ncellar_event_fleet_{name} {n}"
            );
        }
        text
    }
}
