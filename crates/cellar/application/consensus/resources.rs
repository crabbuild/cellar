// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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

/// Physical reservations in currently open image stores, including stores kept
/// alive by pins. Closed directories may retain durable files; this is not a
/// node-wide disk inventory or permission to reclaim retained voting state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftImageUsage {
    pub stores: usize,
    pub generations: usize,
    #[serde(serialize_with = "decimal")]
    pub bytes: u64,
    pub retained: usize,
    pub busy: usize,
    pub max_generations: usize,
    #[serde(serialize_with = "decimal")]
    pub max_bytes: u64,
}

impl From<cellar_ltx::generation::GenerationResources> for RaftImageUsage {
    fn from(value: cellar_ltx::generation::GenerationResources) -> Self {
        Self {
            stores: value.stores,
            generations: value.generations,
            bytes: value.bytes,
            retained: value.retained,
            busy: value.busy,
            max_generations: value.max_generations,
            max_bytes: value.max_bytes,
        }
    }
}

/// Complete shared node inventory, including durable roots kept after a cell
/// closes and disposable workspaces whose physical owners have not exited.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftExtentUsage {
    #[serde(serialize_with = "decimal")]
    pub bytes: u64,
    #[serde(serialize_with = "decimal")]
    pub max_bytes: u64,
    pub objects: usize,
    pub max_objects: usize,
    pub roots: usize,
    pub max_roots: usize,
    pub root_extents: usize,
    pub max_root_extents: usize,
    #[serde(serialize_with = "decimal")]
    pub workspace_bytes: u64,
    pub workspaces: usize,
    pub max_workspaces: usize,
    pub busy: bool,
    pub quarantined: bool,
}

impl
    From<(
        cellar_ltx::internal::NodeExtentUsage,
        cellar_ltx::internal::NodeExtentLimits,
    )> for RaftExtentUsage
{
    fn from(
        (usage, limits): (
            cellar_ltx::internal::NodeExtentUsage,
            cellar_ltx::internal::NodeExtentLimits,
        ),
    ) -> Self {
        Self {
            bytes: usage.bytes,
            max_bytes: limits.max_bytes,
            objects: usage.objects,
            max_objects: limits.max_objects,
            roots: usage.roots,
            max_roots: limits.max_roots,
            root_extents: usage.root_extents,
            max_root_extents: limits.max_root_extents,
            workspace_bytes: usage.workspace_bytes,
            workspaces: usage.workspaces,
            max_workspaces: limits.max_workspaces,
            busy: usage.busy,
            quarantined: usage.quarantined,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftTransportBanks {
    pub control: RaftBudgetUsage,
    pub data: RaftBudgetUsage,
    pub query: RaftBudgetUsage,
}

/// Physical connection owners, including TCP/TLS handshakes and idle pooled
/// connections. A cancelled waiter cannot release a still-owned connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftConnectionReservations {
    pub control: usize,
    pub data: usize,
    pub query: usize,
}

/// Actual node-wide banks in each direction, plus independently owned snapshot
/// and metadata reservations. Per-peer banks are not summed into these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftTransportResources {
    pub incoming: RaftTransportBanks,
    pub outgoing: RaftTransportBanks,
    pub incoming_connections: RaftConnectionReservations,
    pub outgoing_connections: RaftConnectionReservations,
    /// Actual independent lane capacities. Control includes reserved inspection
    /// slots once enabled. Outgoing is bounded by peer/operation admission.
    pub incoming_connection_limits: RaftConnectionReservations,
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

/// A complete bounded scan of attached histories. These independently timed
/// observations are hints, never quorum, admission or archive recovery proof.
/// Age starts with the first observation, not the last batch or latest scrape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RaftGroupResources {
    pub durable_groups: usize,
    pub attached_groups: usize,
    /// Durable histories without an attached cell have unknown archive coverage.
    pub parked_groups: usize,
    pub leaders: usize,
    pub voters: usize,
    pub learners: usize,
    pub removed: usize,
    pub unknown: usize,
    pub coherent_images: usize,
    pub historical_queries: usize,
    pub recent_source_queries: usize,
    pub failed: usize,
    pub retired: usize,
    pub sql_busy: usize,
    pub pending_commands: usize,
    pub incoming_seeds: usize,
    pub applying: usize,
    pub archive_pending: usize,
    /// Sum and maximum committed-minus-applied positions, in Raft entries.
    #[serde(serialize_with = "decimal")]
    pub apply_lag_entries: u64,
    #[serde(serialize_with = "decimal")]
    pub max_apply_lag_entries: u64,
    /// Locally observed coverage; another publisher may have archived further.
    #[serde(serialize_with = "decimal")]
    pub archive_lag_entries: u64,
    #[serde(serialize_with = "decimal")]
    pub max_archive_lag_entries: u64,
    #[serde(serialize_with = "decimal")]
    pub sample_age_ms: u64,
}

/// Local controller activity and actual reserved metadata. Attempt counters are
/// observations, not proof of successful membership or leadership changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftControllerResources {
    pub enabled: bool,
    pub recovering: bool,
    pub busy: bool,
    pub metadata: RaftBudgetUsage,
    #[serde(serialize_with = "decimal")]
    pub probe_rounds: u64,
    #[serde(serialize_with = "decimal")]
    pub repair_requests: u64,
    #[serde(serialize_with = "decimal")]
    pub activation_requests: u64,
    #[serde(serialize_with = "decimal")]
    pub transfer_attempts: u64,
    #[serde(serialize_with = "decimal")]
    pub completed_repairs: u64,
    #[serde(serialize_with = "decimal")]
    pub sample_age_ms: u64,
}

/// Passive reservations and age-bearing operation observations. No field grants
/// authority, establishes quorum health or inventories closed on-disk histories.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftResourceUsage {
    pub business: RaftBudgetUsage,
    pub control: RaftBudgetUsage,
    pub archive_capture: RaftBudgetUsage,
    pub query_pins: RaftQueryPinUsage,
    /// At most sixteen worker banks are sampled. None means no valid observer.
    pub sql: Option<RaftSqlUsage>,
    /// None means image accounting is unavailable or a store was quarantined.
    pub images: Option<RaftImageUsage>,
    /// None when extent storage is absent, closed or its counters are invalid.
    pub extents: Option<RaftExtentUsage>,
    /// None until a whole scan completes, or if no valid source is available.
    pub groups: Option<RaftGroupResources>,
    /// None if no controller is configured or no valid source was captured.
    pub controller: Option<RaftControllerResources>,
    /// None means the transport owner has exited or its registry is unavailable.
    /// This must not be interpreted as measured zero usage.
    pub transport: Option<RaftTransportResources>,
}

#[cfg(test)]
#[path = "resources_tests.rs"]
mod tests;
