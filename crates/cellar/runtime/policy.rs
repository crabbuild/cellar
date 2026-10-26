// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Runtime limits selected by the embedding application.

use std::time::Duration;

/// Immutable request and worker policy for one Cellar host.
/// Configure through `domain.services().configure_runtime(...)` before
/// building a generation. Pools, cell admission, handler drivers, and queue
/// settlement all use the same policy. Limits on stateless work apply per
/// script pool, including pools in draining generations, rather than to the
/// application's entire process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimePolicy {
    /// Maximum workers in one script's stateless pool.
    pub max_stateless_workers: usize,
    /// Maximum admitted stateless events per script pool. Background work
    /// retains its slot after the response. `None` removes this bound.
    pub max_stateless_requests: Option<usize>,
    /// Maximum resident cells sharing one worker.
    pub max_cells_per_worker: usize,
    /// Maximum concurrent fetches and RPC calls admitted to one cell.
    pub max_cell_requests: usize,
    /// Maximum SDK namespace RPCs awaiting a gate, queued, routed, executing or
    /// awaiting reply consumption in this host, across application generations.
    /// Trusted direct host control calls use separate admission.
    pub max_outbound_rpc_requests: usize,
    /// Reserved SDK RPC request/copy/control allowance across this host, computed
    /// from encoded lengths. This does not measure allocator capacity/shared
    /// backing allocations or replace receiving-peer, response, adapter or
    /// process memory limits. Minimum 128 KiB.
    pub max_outbound_rpc_bytes: usize,
    /// How long a saturated stateless pool waits before refusing an event.
    /// Zero means refuse immediately.
    pub admission_wait: Duration,
    /// Time allowed for a handler to answer. Background work and durability
    /// settlement after the answer do not consume this budget.
    pub handler_budget: Duration,
}

impl Default for RuntimePolicy {
    /// Library defaults are bounded and never read the process environment.
    fn default() -> Self {
        Self {
            max_stateless_workers: 4,
            max_stateless_requests: Some(64),
            max_cells_per_worker: 32,
            max_cell_requests: crate::runtime::DEFAULT_MAX_CELL_REQUESTS,
            max_outbound_rpc_requests: 128,
            max_outbound_rpc_bytes: 256 * 1024 * 1024,
            admission_wait: Duration::from_secs(1),
            handler_budget: Duration::from_secs(300),
        }
    }
}

impl RuntimePolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_stateless_workers > 0,
            "max_stateless_workers must be positive"
        );
        anyhow::ensure!(
            self.max_stateless_requests != Some(0),
            "max_stateless_requests must be positive or None"
        );
        anyhow::ensure!(
            self.max_cells_per_worker > 0,
            "max_cells_per_worker must be positive"
        );
        anyhow::ensure!(
            self.max_cell_requests > 0,
            "max_cell_requests must be positive"
        );
        anyhow::ensure!(
            (1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&self.max_outbound_rpc_requests),
            "max_outbound_rpc_requests must fit a positive semaphore capacity"
        );
        anyhow::ensure!(
            (128 * 1024..=(u32::MAX as usize).min(tokio::sync::Semaphore::MAX_PERMITS))
                .contains(&self.max_outbound_rpc_bytes),
            "max_outbound_rpc_bytes must fit at least 128 KiB of RPC request/control capacity"
        );
        anyhow::ensure!(
            !self.handler_budget.is_zero(),
            "handler_budget must be positive"
        );
        Ok(())
    }

    /// Resolve the command-line runner's process settings once. Embedding
    /// applications can opt into this adapter, or supply their own values.
    /// The CLI retains unbounded stateless admission when its request limit
    /// is unset; library defaults provide a finite bound.
    pub fn from_env() -> anyhow::Result<Self> {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let policy = Self {
            max_stateless_workers: crate::env_vars::positive_or(
                "CELLAR_MAX_STATELESS_ISOLATES",
                cores,
            )?,
            max_stateless_requests: crate::env_vars::positive("CELLAR_MAX_REQUESTS")?,
            max_cell_requests: crate::env_vars::positive_or(
                "CELLAR_MAX_CELL_REQUESTS",
                crate::runtime::DEFAULT_MAX_CELL_REQUESTS,
            )?,
            max_outbound_rpc_requests: crate::env_vars::positive_or(
                "CELLAR_MAX_OUTBOUND_RPC_REQUESTS",
                Self::default().max_outbound_rpc_requests,
            )?,
            max_outbound_rpc_bytes: crate::env_vars::positive_or(
                "CELLAR_MAX_OUTBOUND_RPC_BYTES",
                Self::default().max_outbound_rpc_bytes,
            )?,
            admission_wait: Duration::from_millis(crate::env_vars::with_default(
                "CELLAR_ADMISSION_WAIT_MS",
                1000u64,
            )?),
            handler_budget: Duration::from_secs(crate::env_vars::positive_or(
                "CELLAR_HANDLER_BUDGET_S",
                300,
            )?),
            ..Self::default()
        };
        policy.validate()?;
        Ok(policy)
    }

    pub(crate) fn pool_limits(self) -> cellar_logic::isolate::PoolLimits {
        cellar_logic::isolate::PoolLimits {
            // A single hysteresis policy; these thresholds must move together.
            grow_at: 2,
            shrink_under: 1,
            max_stateless: self.max_stateless_workers,
            max_requests: self.max_stateless_requests,
            max_cells: self.max_cells_per_worker,
        }
    }
}
