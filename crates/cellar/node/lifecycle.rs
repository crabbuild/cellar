//! Outcomes and shutdown policy for the production node lifecycle.
//!
//! These contracts separate engine supervision from the CLI's signals and
//! process exit. The public node supervisor closes and joins its execution
//! domain before returning. Forced closure does not prove a graceful drain.

pub use crate::actor::ShutdownMode;
pub use cellar_logic::HaltReason as FenceReason;

/// An authority loss reported by the core, with its observed cause. Losing
/// this reporting channel is a supervision failure, not an authority report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceReport {
    pub code: i32,
    pub reason: FenceReason,
}

/// One absolute shutdown budget, including ownership and local durability.
/// Progress can extend the stall window but never the complete budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShutdownPolicy {
    pub total_ms: u64,
    pub token_wait_ms: u64,
    pub no_progress_ms: u64,
}

impl Default for ShutdownPolicy {
    /// Library defaults do not read the environment.
    fn default() -> Self {
        Self {
            total_ms: 40_000,
            token_wait_ms: 30_000,
            no_progress_ms: 25_000,
        }
    }
}

impl ShutdownPolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.total_ms > 0, "shutdown total_ms must be positive");
        anyhow::ensure!(
            self.token_wait_ms <= self.total_ms,
            "shutdown token_wait_ms must not exceed total_ms"
        );
        anyhow::ensure!(
            self.no_progress_ms > 0 && self.no_progress_ms <= self.total_ms,
            "shutdown no_progress_ms must be positive and at most total_ms"
        );
        Ok(())
    }

    /// Explicit process-runner adapter; capture before starting the driver.
    pub fn from_env() -> anyhow::Result<Self> {
        let timing = crate::env_vars::shutdown_timing()?;
        let policy = Self {
            total_ms: timing.total_ms,
            token_wait_ms: timing.drain_token_wait_ms,
            no_progress_ms: timing.drain_no_progress_ms,
        };
        policy.validate()?;
        Ok(policy)
    }
}

/// The proof phases observed by a bounded stop. A deadline or failed phase
/// leaves normal lease-expiry recovery responsible for unfinished cells.
/// These flags do not assert completion of untracked application work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShutdownReport {
    pub mode: ShutdownMode,
    pub ingress_drained: bool,
    /// Complete application event drivers, including post-response waitUntil
    /// and their nested SDK calls, drained before the ownership/durability cut.
    pub application_work_drained: bool,
    /// Event lifetimes still pending when the semantic drain returned. Forced
    /// domain closure can subsequently retire them without a graceful proof.
    pub pending_application_events: usize,
    pub cells_drained: bool,
    pub pending_dispatches: usize,
    pub log_quiesced: bool,
    pub local_durability_closed: bool,
    pub clean_reload_prepared: bool,
    /// Ingress, application-event or cell draining exhausted its stall or
    /// absolute budget.
    pub drain_timed_out: bool,
    /// The complete budget elapsed, independently of a shorter stall timeout.
    pub deadline_elapsed: bool,
}

/// The node's own result, to be supervised by its host application.
/// Fencing and component failures are distinct from a requested bounded stop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeOutcome {
    Stopped(ShutdownReport),
    /// Forced domain closure; no graceful durability proof is claimed.
    Cancelled,
    /// Authority loss observed by the core, including its specific cause.
    Fenced {
        code: i32,
        reason: FenceReason,
    },
    CoreFailed {
        error: String,
    },
    ReplicationFailed {
        error: String,
    },
}

impl NodeOutcome {
    /// Machine-readable CLI observation of the actual driver outcome. This is
    /// deliberately not a graceful-stop assertion: incomplete phases survive.
    pub(crate) fn cli_report(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            Self::Stopped(r) => json!({
                "event": "daemon_node_stopped", "outcome": "stopped",
                "mode": match r.mode { ShutdownMode::Handoff => "handoff", ShutdownMode::Preserve => "preserve" },
                "ingress_drained": r.ingress_drained,
                "application_work_drained": r.application_work_drained,
                "pending_application_events": r.pending_application_events,
                "cells_drained": r.cells_drained, "pending_dispatches": r.pending_dispatches,
                "log_quiesced": r.log_quiesced, "local_durability_closed": r.local_durability_closed,
                "clean_reload_prepared": r.clean_reload_prepared,
                "drain_timed_out": r.drain_timed_out, "deadline_elapsed": r.deadline_elapsed,
            }),
            Self::Fenced { code, reason } => json!({
                "event": "daemon_node_stopped", "outcome": "fenced", "fence_code": code,
                "reason": match reason {
                    FenceReason::NodeLeaseExpired => "node_lease_expired",
                    FenceReason::NodeLeaseMissing => "node_lease_missing",
                    FenceReason::NodeLeaseMismatch => "node_lease_mismatch",
                }
            }),
            Self::Cancelled => json!({"event": "daemon_node_stopped", "outcome": "cancelled"}),
            Self::CoreFailed { .. } => {
                json!({"event": "daemon_node_stopped", "outcome": "core_failed"})
            }
            Self::ReplicationFailed { .. } => {
                json!({"event": "daemon_node_stopped", "outcome": "replication_failed"})
            }
        }
    }

    /// Only the command-line adapter translates outcomes to process exits.
    pub(crate) fn cli_exit_code(&self) -> i32 {
        match self {
            Self::Stopped(_) => 0,
            Self::Fenced { code, .. } => *code,
            Self::Cancelled | Self::CoreFailed { .. } | Self::ReplicationFailed { .. } => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_report_preserves_incomplete_shutdown() {
        let report = NodeOutcome::Stopped(ShutdownReport {
            mode: ShutdownMode::Preserve,
            ingress_drained: true,
            application_work_drained: false,
            pending_application_events: 7,
            cells_drained: false,
            pending_dispatches: 3,
            log_quiesced: false,
            local_durability_closed: false,
            clean_reload_prepared: false,
            drain_timed_out: true,
            deadline_elapsed: true,
        })
        .cli_report();
        assert_eq!(report["mode"], "preserve");
        assert_eq!(report["pending_application_events"], 7);
        assert_eq!(report["pending_dispatches"], 3);
        assert_eq!(report["application_work_drained"], false);
        assert_eq!(report["deadline_elapsed"], true);
    }

    #[test]
    fn cli_report_distinguishes_failure_from_fence() {
        let value = NodeOutcome::ReplicationFailed {
            error: "private detail".into(),
        }
        .cli_report();
        assert_eq!(value["outcome"], "replication_failed");
        assert!(value.get("reason").is_none());
        let value = NodeOutcome::Fenced {
            code: 3,
            reason: FenceReason::NodeLeaseMismatch,
        }
        .cli_report();
        assert_eq!(value["reason"], "node_lease_mismatch");
    }
}
