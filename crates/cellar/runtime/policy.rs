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
    /// Maximum receiving peer RPC requests, including upload and output waits.
    pub max_inbound_rpc_requests: usize,
    /// Shared receiving input/copy/control and published reply payload allowance.
    /// Minimum 128 KiB. It does not measure shared backing allocations or RSS.
    pub max_inbound_rpc_bytes: usize,
    /// Shared conservative native Protobuf decode allowances. Transport buffers,
    /// application-retained values and whole-process RSS have separate ownership.
    pub max_native_rpc_decode_bytes: usize,
    /// Shared host response encoding/collection and retained-copy allowance,
    /// including typed provider output-proof waits. Minimum 64 KiB. This does
    /// not cap application-created graphs, guest heaps or whole-process RSS.
    pub max_rpc_response_bytes: usize,
    /// Conservative per-decode stack allowance. The caller-owned executor must
    /// provide enough thread stack and runtime/application headroom for it.
    pub max_native_rpc_stack_bytes: usize,
    /// Maximum open authenticated receiving RPC tunnels across this host.
    pub max_inbound_rpc_tunnels: usize,
    /// Maximum receiving RPC tunnels owned by one authenticated peer name.
    pub max_rpc_tunnels_per_peer: usize,
    /// Maximum receiving RPC upgrade, idle and post-handler reply-write interval.
    /// An original caller deadline can shorten delivery; handler execution keeps
    /// its separate budget.
    pub rpc_tunnel_idle_timeout: Duration,
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
            max_inbound_rpc_requests: 128,
            max_inbound_rpc_bytes: 256 * 1024 * 1024,
            max_native_rpc_decode_bytes: 256 * 1024 * 1024,
            max_rpc_response_bytes: 256 * 1024 * 1024,
            max_native_rpc_stack_bytes: 1024 * 1024,
            max_inbound_rpc_tunnels: 256,
            max_rpc_tunnels_per_peer: 64,
            rpc_tunnel_idle_timeout: Duration::from_secs(30),
            admission_wait: Duration::from_secs(1),
            handler_budget: Duration::from_secs(300),
        }
    }
}

impl RuntimePolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (64 * 1024..=(u32::MAX as usize).min(tokio::sync::Semaphore::MAX_PERMITS))
                .contains(&self.max_rpc_response_bytes),
            "max_rpc_response_bytes must fit at least 64 KiB of response/control capacity"
        );
        anyhow::ensure!(
            (1..=(u32::MAX as usize).min(tokio::sync::Semaphore::MAX_PERMITS))
                .contains(&self.max_native_rpc_decode_bytes),
            "max_native_rpc_decode_bytes must fit a positive semaphore capacity"
        );
        anyhow::ensure!(
            (1..=u32::MAX as usize).contains(&self.max_native_rpc_stack_bytes),
            "max_native_rpc_stack_bytes must fit 1..=u32::MAX bytes"
        );
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
            [
                self.max_inbound_rpc_requests,
                self.max_inbound_rpc_tunnels,
                self.max_rpc_tunnels_per_peer
            ]
            .into_iter()
            .all(|value| (1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&value)),
            "receiving RPC request and tunnel quotas must fit positive semaphore capacities"
        );
        anyhow::ensure!(
            (128 * 1024..=(u32::MAX as usize).min(tokio::sync::Semaphore::MAX_PERMITS))
                .contains(&self.max_inbound_rpc_bytes),
            "max_inbound_rpc_bytes must fit at least 128 KiB of RPC request/control capacity"
        );
        anyhow::ensure!(
            self.rpc_tunnel_idle_timeout >= Duration::from_millis(1)
                && self.rpc_tunnel_idle_timeout <= Duration::from_millis(u32::MAX.into()),
            "rpc_tunnel_idle_timeout must fit 1..=u32::MAX milliseconds"
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
            max_inbound_rpc_requests: crate::env_vars::positive_or(
                "CELLAR_MAX_INBOUND_RPC_REQUESTS",
                Self::default().max_inbound_rpc_requests,
            )?,
            max_inbound_rpc_bytes: crate::env_vars::positive_or(
                "CELLAR_MAX_INBOUND_RPC_BYTES",
                Self::default().max_inbound_rpc_bytes,
            )?,
            max_native_rpc_decode_bytes: crate::env_vars::positive_or(
                "CELLAR_MAX_NATIVE_RPC_DECODE_BYTES",
                Self::default().max_native_rpc_decode_bytes,
            )?,
            max_rpc_response_bytes: crate::env_vars::positive_or(
                "CELLAR_MAX_RPC_RESPONSE_BYTES",
                Self::default().max_rpc_response_bytes,
            )?,
            max_native_rpc_stack_bytes: crate::env_vars::positive_or(
                "CELLAR_MAX_NATIVE_RPC_STACK_BYTES",
                Self::default().max_native_rpc_stack_bytes,
            )?,
            max_inbound_rpc_tunnels: crate::env_vars::positive_or(
                "CELLAR_MAX_INBOUND_RPC_TUNNELS",
                Self::default().max_inbound_rpc_tunnels,
            )?,
            max_rpc_tunnels_per_peer: crate::env_vars::positive_or(
                "CELLAR_MAX_RPC_TUNNELS_PER_PEER",
                Self::default().max_rpc_tunnels_per_peer,
            )?,
            rpc_tunnel_idle_timeout: Duration::from_millis(crate::env_vars::positive_or(
                "CELLAR_RPC_TUNNEL_IDLE_MS",
                30_000,
            )?),
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

#[cfg(test)]
mod tests {
    use super::RuntimePolicy;

    #[test]
    fn response_pool_capacities_are_validated_before_allocation() {
        for invalid in [0, 64 * 1024 - 1, usize::MAX] {
            assert!(RuntimePolicy {
                max_rpc_response_bytes: invalid,
                ..Default::default()
            }
            .validate()
            .is_err());
        }
        RuntimePolicy {
            max_rpc_response_bytes: 64 * 1024,
            ..Default::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn native_decoder_capacities_are_checked_before_pools_are_built() {
        RuntimePolicy::default().validate().unwrap();
        for invalid in [0, usize::MAX] {
            let policy = RuntimePolicy {
                max_native_rpc_decode_bytes: invalid,
                ..Default::default()
            };
            assert!(policy.validate().is_err());
            // usize::MAX is a representable stack allowance on 32-bit hosts.
            if invalid == 0 || invalid > u32::MAX as usize {
                let policy = RuntimePolicy {
                    max_native_rpc_stack_bytes: invalid,
                    ..Default::default()
                };
                assert!(policy.validate().is_err());
            }
        }
        RuntimePolicy {
            max_native_rpc_decode_bytes: 1,
            max_native_rpc_stack_bytes: 1,
            ..Default::default()
        }
        .validate()
        .unwrap();
    }
}
