// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Advisory clocks never participate in write acknowledgement or admission.
use super::ArchiveError;
use crate::application::consensus::{ArchiveFailure, ArchiveReadiness, ArchiveStatus};
use cellar_logic::consensus::LogIndex;

#[derive(Default)]
pub(super) struct Observation {
    committed: u64,
    behind_since: Option<u64>,
    success_at: Option<u64>,
    failures: u64,
    failure: Option<ArchiveFailure>,
}

impl Observation {
    pub fn observe(&mut self, committed: LogIndex, archived: LogIndex, now: u64) {
        self.committed = self.committed.max(committed.get());
        if self.committed > archived.get() {
            self.behind_since.get_or_insert(now);
        } else {
            self.behind_since = None;
        }
    }

    pub fn confirmed(&mut self, archived: LogIndex, now: u64) {
        self.success_at = Some(now);
        self.failures = 0;
        self.failure = None;
        if archived.get() >= self.committed {
            self.behind_since = None;
        }
    }

    pub fn failed(&mut self, error: &ArchiveError) {
        self.failures = self.failures.saturating_add(1);
        self.failure = Some(match error {
            ArchiveError::Storage(_) => ArchiveFailure::Storage,
            ArchiveError::Timeout => ArchiveFailure::Timeout,
            ArchiveError::Corrupt(_) | ArchiveError::Conflict | ArchiveError::Invalid(_) => {
                ArchiveFailure::Integrity
            }
            ArchiveError::Limit => ArchiveFailure::Capacity,
            ArchiveError::Unconfirmed => ArchiveFailure::Unconfirmed,
            _ => ArchiveFailure::Unavailable,
        });
    }

    pub fn capture(
        &self,
        committed: LogIndex,
        archived: LogIndex,
        now: u64,
        retained_bytes: u64,
        publishing: bool,
    ) -> ArchiveStatus {
        let lag_entries = committed.get().saturating_sub(archived.get());
        ArchiveStatus {
            readiness: if archived.get() == 0 {
                ArchiveReadiness::Unconfirmed
            } else if lag_entries > 0 {
                ArchiveReadiness::Lagging
            } else {
                ArchiveReadiness::Current
            },
            lag_entries,
            observed_lag_ms: self
                .behind_since
                .filter(|_| lag_entries > 0)
                .and_then(|since| now.checked_sub(since)),
            retained_bytes,
            publishing,
            last_success_age_ms: self.success_at.and_then(|at| now.checked_sub(at)),
            consecutive_failures: self.failures,
            last_failure: self.failure,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backlog_and_failures_do_not_promote_coverage_and_restart_resets_clocks() {
        let mut observed = Observation::default();
        let index = LogIndex::new;
        observed.observe(index(8), LogIndex::new(0), 100);
        observed.failed(&ArchiveError::Timeout);
        let status = observed.capture(index(8), LogIndex::new(0), 130, 4096, true);
        assert_eq!(status.readiness, ArchiveReadiness::Unconfirmed);
        assert_eq!(status.lag_entries, 8);
        assert_eq!(status.observed_lag_ms, Some(30));
        assert_eq!(status.last_success_age_ms, None);
        assert_eq!(status.last_failure, Some(ArchiveFailure::Timeout));
        observed.confirmed(index(5), 140);
        let status = observed.capture(index(8), index(5), 150, 4096, false);
        assert_eq!(status.readiness, ArchiveReadiness::Lagging);
        assert_eq!(status.observed_lag_ms, Some(50));
        assert_eq!(status.consecutive_failures, 0);
        assert_eq!(status.last_failure, None);
        observed.confirmed(index(8), 160);
        let status = observed.capture(index(8), index(8), 180, 0, false);
        assert_eq!(status.readiness, ArchiveReadiness::Current);
        assert_eq!(status.observed_lag_ms, None);
        assert_eq!(status.last_success_age_ms, Some(20));
        observed.observe(index(10), index(8), 200);
        assert_eq!(
            observed
                .capture(index(10), index(8), 201, 0, false)
                .observed_lag_ms,
            Some(1)
        );
        let restarted = Observation::default().capture(index(10), LogIndex::new(0), 0, 0, false);
        assert_eq!(restarted.readiness, ArchiveReadiness::Unconfirmed);
        assert_eq!(restarted.observed_lag_ms, None);
        assert_eq!(restarted.last_success_age_ms, None);
    }
}
