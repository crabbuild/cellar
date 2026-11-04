// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, same-owner physical log metadata for administration and measurement.
use super::CellGroupStatus;
use crate::application::consensus::RaftReplicaRole;
use cellar_logic::{
    cell_group::GroupHistory,
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
