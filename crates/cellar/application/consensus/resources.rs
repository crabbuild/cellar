//! Fixed-size observations of actual local admission reservations.

/// These counters are sampled independently, rather than one atomic snapshot.
/// Retained request/result owners can keep reservations after the host closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftQueryPinUsage {
    pub in_use: usize,
    pub limit: usize,
}

/// Local host request/output and archive-capture reservations. This is not an
/// aggregate health/quorum proof or the complete transport/image/storage usage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftResourceUsage {
    pub business: RaftBudgetUsage,
    pub control: RaftBudgetUsage,
    pub archive_capture: RaftBudgetUsage,
    pub query_pins: RaftQueryPinUsage,
}
