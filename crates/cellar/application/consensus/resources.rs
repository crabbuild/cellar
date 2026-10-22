//! Fixed-size observations of actual local admission reservations.

/// These counters are sampled independently, rather than one atomic snapshot.
/// Retained request/result owners can keep reservations after the host closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftBudgetUsage {
    pub bytes: usize,
    pub items: usize,
    pub max_bytes: usize,
    pub max_items: usize,
}

impl RaftBudgetUsage {
    pub(crate) fn capture(budget: &crate::raft::budget::Budget) -> Self {
        let (bytes, items) = budget.usage();
        let (max_bytes, max_items) = budget.capacity();
        Self {
            bytes,
            items,
            max_bytes,
            max_items,
        }
    }
}

/// Reservations for SDK query callbacks, acquired on the physical SQL owner
/// before opening its immutable database and released after that database drops.
/// Cancellation of a caller does not release a running callback's reservation.
/// Archive, seed, checkpoint and effect readers have their separate budgets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftQueryPinUsage {
    pub in_use: usize,
    pub limit: usize,
}

/// Physical SQL owners and the real queued/executing work reservations.
/// A running owner can be idle. These counters do not measure CPU utilization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftSqlUsage {
    pub running_workers: usize,
    pub worker_capacity: usize,
    pub work_reservations: usize,
    pub work_capacity: usize,
    /// Internal SQL admission; application admission can already be draining.
    pub queue_closed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftTransportBanks {
    pub control: RaftBudgetUsage,
    pub data: RaftBudgetUsage,
    pub query: RaftBudgetUsage,
}

/// Actual node-wide banks in each direction, plus independently owned snapshot
/// and metadata reservations. Per-peer banks are not summed into these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftTransportResources {
    pub incoming: RaftTransportBanks,
    pub outgoing: RaftTransportBanks,
    pub snapshot_source: RaftBudgetUsage,
    pub snapshot_receive: RaftBudgetUsage,
    pub seed_metadata: RaftBudgetUsage,
    pub inspection_metadata: RaftBudgetUsage,
    pub tasks: usize,
    pub peers: usize,
    pub member_routes: usize,
    pub router_routes: usize,
    #[serde(serialize_with = "decimal")]
    pub accepted: u64,
    #[serde(serialize_with = "decimal")]
    pub refused: u64,
    #[serde(serialize_with = "decimal")]
    pub failed: u64,
}

fn decimal<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

/// Local host request/output and archive-capture reservations. This is not an
/// aggregate health/quorum proof or the complete transport/image/storage usage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftResourceUsage {
    pub business: RaftBudgetUsage,
    pub control: RaftBudgetUsage,
    pub archive_capture: RaftBudgetUsage,
    pub query_pins: RaftQueryPinUsage,
    /// At most sixteen worker banks are sampled. None means no valid observer.
    pub sql: Option<RaftSqlUsage>,
    /// None means the transport owner has exited or its registry is unavailable.
    /// This must not be interpreted as measured zero usage.
    pub transport: Option<RaftTransportResources>,
}

#[cfg(test)]
#[path = "resources_tests.rs"]
mod tests;
