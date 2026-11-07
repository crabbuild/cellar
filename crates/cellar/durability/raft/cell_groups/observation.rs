// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, same-owner physical log metadata for administration and measurement.
use super::CellGroupStatus;
use crate::application::consensus::RaftReplicaRole;
use cellar_logic::{
    cell_group::{CellKey, GroupHistory, GroupRoute, MembershipRevision},
    cell_group_placement::{
        HotCell, PartitionBasis, PartitionLoad, PartitionSample, HOT_WRITE_WINDOW_MS,
    },
    consensus::{Durability, LogIndex, LogPosition, ReplicaIdentity},
};

/// A local sample of one physical group, independent of its cell occupancy.
/// Selected in one Raft-owner turn with a single durable-cache read. It grants
/// no quorum, serving lease, archive coverage or permission to reclaim logs.
#[derive(Clone, Debug)]
pub struct CellGroupObservation {
    pub replica: ReplicaIdentity<GroupHistory>,
    pub status: CellGroupStatus,
    pub role: RaftReplicaRole,
    pub membership_revision: LogIndex,
    /// Last locally persisted entry or checkpoint, including uncommitted data.
    pub persisted: LogIndex,
    /// Installed local checkpoint, which can lag both apply and archive output.
    pub checkpoint: LogPosition,
    /// Entries retained after the checkpoint; this is not a byte/disk estimate.
    pub retained_entries: usize,
    /// Logical cells in the currently applied manifest, including sealed cells.
    pub cells: u32,
    /// Compaction requires independently verified archive coverage on this log.
    pub archive_required: bool,
}

/// Geometry and whole-key neighbors from one selected local manifest.
#[derive(Clone, Debug)]
pub struct CellGroupPartitionCell {
    pub key: CellKey,
    pub next: Option<CellKey>,
    pub has_predecessor: bool,
    pub logical_bytes: u64,
}

/// A lower bound for one writer in a complete one-second owner window.
#[derive(Clone, Debug)]
pub struct CellGroupPartitionWriter {
    pub cell: CellGroupPartitionCell,
    pub started_at_ms: u64,
    pub admitted_writes: u64,
}

/// Cumulative work on this physical owner and its monotonic sample time.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupWorkload {
    pub(super) owner: [u8; 16],
    pub sampled_at_ms: u64,
    /// New Raft proposals for enrollments and mutations. Reusing a proposal
    /// or serving a durable receipt does not increase this counter.
    pub admitted_writes: u64,
    /// Fresh cell leaf installations after durable apply. Metadata-only
    /// entries and received snapshots do not increase this counter.
    pub applied_writes: u64,
    /// Age of the oldest committed entry still in the local apply queue.
    pub queue_age_ms: u64,
}

/// Placement observations from the controller that owns this sample. Supply
/// an actual directory digest and resource readings for the selected route.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupPartitionContext {
    pub directory_digest: [u8; 32],
    pub topology_changed_at_ms: u64,
    pub durability: Durability,
    pub placement_class: [u8; 32],
    pub leader_cpu_bps: u16,
    pub failure_domain_load_bps: u16,
    pub active_and_stable: bool,
}

/// Bounded scheduler input from one local owner. Advisory only.
#[derive(Clone, Debug)]
pub struct CellGroupPartitionObservation {
    pub route: GroupRoute,
    pub membership: MembershipRevision,
    pub status: CellGroupStatus,
    /// Local leadership and membership are stable, including while apply runs.
    pub stable_leader: bool,
    pub workload: CellGroupWorkload,
    pub cells: u32,
    pub logical_bytes: u64,
    pub last_mutation: Option<LogPosition>,
    pub balanced_pivot: Option<CellKey>,
    pub largest_cell: Option<CellGroupPartitionCell>,
    pub hot_writer: Option<CellGroupPartitionWriter>,
    /// Independently sampled archive owner, bound to this exact route.
    pub archive_lag_entries: Option<u64>,
}

impl CellGroupPartitionObservation {
    pub(crate) fn same_workload_owner(&self, earlier: &Self) -> bool {
        self.stable_leader
            && earlier.stable_leader
            && self.route == earlier.route
            && self.membership == earlier.membership
            && self.status.term == earlier.status.term
            && self.workload.owner == earlier.workload.owner
    }

    /// Convert two samples from the same stable local owner into a policy
    /// observation. The context's topology time uses this owner's monotonic
    /// clock. Sample and recommendation carry no authority to change routes.
    pub fn sample_since(
        &self,
        earlier: &Self,
        context: CellGroupPartitionContext,
    ) -> Option<PartitionSample> {
        if !self.same_workload_owner(earlier) {
            return None;
        }
        let elapsed_ms = self
            .workload
            .sampled_at_ms
            .checked_sub(earlier.workload.sampled_at_ms)?;
        let rate = |current: u64, previous| {
            if current == u64::MAX || elapsed_ms == 0 {
                return None;
            }
            let count = current.checked_sub(previous)?;
            u64::try_from(u128::from(count) * 1_000 / u128::from(elapsed_ms)).ok()
        };
        let writes_per_second = rate(
            self.workload.admitted_writes,
            earlier.workload.admitted_writes,
        )?;
        let applies_per_second = rate(
            self.workload.applied_writes,
            earlier.workload.applied_writes,
        )?;
        let hot_writer = self.hot_writer.as_ref().and_then(|writer| {
            // Include only a complete window contained in this interval.
            // Consecutive policy observations never count one window twice.
            let end = writer.started_at_ms.checked_add(HOT_WRITE_WINDOW_MS)?;
            if writer.started_at_ms < earlier.workload.sampled_at_ms
                || end > self.workload.sampled_at_ms
            {
                return None;
            }
            let writes = rate(writer.admitted_writes, 0)?;
            Some(writer.cell.hot(writes.min(writes_per_second)))
        });
        Some(PartitionSample {
            basis: PartitionBasis {
                route: self.route.clone(),
                directory_digest: context.directory_digest,
                membership: self.membership,
                term: self.status.term,
                durability: context.durability,
                placement_class: context.placement_class,
            },
            sampled_at_ms: self.workload.sampled_at_ms,
            topology_changed_at_ms: context.topology_changed_at_ms,
            load: PartitionLoad {
                cells: self.cells,
                logical_bytes: self.logical_bytes,
                writes_per_second,
            },
            applies_per_second,
            queue_age_ms: self.workload.queue_age_ms,
            archive_lag_entries: self.archive_lag_entries,
            leader_cpu_bps: context.leader_cpu_bps,
            failure_domain_load_bps: context.failure_domain_load_bps,
            active_and_stable: context.active_and_stable,
            balanced_pivot: self.balanced_pivot.clone(),
            hot_cell: self.largest_cell.as_ref().map(|cell| cell.hot(0)),
            hot_writer,
        })
    }
}

impl CellGroupPartitionCell {
    fn hot(&self, writes_per_second: u64) -> HotCell {
        HotCell {
            key: self.key.clone(),
            next: self.next.clone(),
            has_predecessor: self.has_predecessor,
            logical_bytes: self.logical_bytes,
            writes_per_second,
        }
    }
}
