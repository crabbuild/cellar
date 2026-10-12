//! Outcomes and shutdown policy for the production node lifecycle.
//!
//! These contracts separate engine supervision from the CLI's signals and
//! process exit. The public node supervisor closes and joins its execution
//! domain before returning. Forced closure does not prove a graceful drain.

pub use crate::actor::ShutdownMode;

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
    pub cells_drained: bool,
    pub pending_dispatches: usize,
    pub log_quiesced: bool,
    pub local_durability_closed: bool,
    pub clean_reload_prepared: bool,
    /// Admission or cell draining exhausted its stall or absolute budget.
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
    Fenced {
        code: i32,
    },
    CoreFailed {
        error: String,
    },
    ReplicationFailed {
        error: String,
    },
}

impl NodeOutcome {
    /// Only the command-line adapter translates outcomes to process exits.
    pub(crate) fn cli_exit_code(&self) -> i32 {
        match self {
            Self::Stopped(_) => 0,
            Self::Fenced { code } => *code,
            Self::Cancelled | Self::CoreFailed { .. } | Self::ReplicationFailed { .. } => 3,
        }
    }
}
