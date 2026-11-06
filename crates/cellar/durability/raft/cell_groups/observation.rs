// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, same-owner physical log metadata for administration and measurement.
use super::CellGroupStatus;
use crate::application::consensus::RaftReplicaRole;
use cellar_logic::{
    cell_group::{CellKey, GroupHistory, GroupRoute, MembershipRevision},
    consensus::{LogIndex, LogPosition, ReplicaIdentity},
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

/// Bounded scheduler input from one local owner. Advisory only.
#[derive(Clone, Debug)]
pub struct CellGroupPartitionObservation {
    pub route: GroupRoute,
    pub membership: MembershipRevision,
    pub status: CellGroupStatus,
    pub cells: u32,
    pub logical_bytes: u64,
    pub last_mutation: Option<LogPosition>,
    pub balanced_pivot: Option<CellKey>,
    pub largest_cell: Option<CellGroupPartitionCell>,
    /// Independently sampled archive owner, bound to this exact route.
    pub archive_lag_entries: Option<u64>,
}
