// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only inspection values. None of these observations grants membership,
//! fresh-quorum seeding, read visibility or an acknowledgement proof.
use super::member_state::RepairStatus;
use cellar_logic::consensus::{
    EngineInstanceId, GroupScope, LogIndex, LogPosition, MemberId, NodeIdentity, Term,
};

/// Peer probe outcomes preserve whether a direct connection attempt failed.
/// Local pressure, missing discovery and an unusable response do not establish
/// that the remote node failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InspectionError {
    Unreachable,
    Overloaded,
    Unavailable,
    Refused,
    StalePeer,
}

impl std::fmt::Display for InspectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft inspection: {self:?}")
    }
}

impl std::error::Error for InspectionError {}

/// Explicit placement declarations, independent of the current process address.
/// Storage reservations constrain selection; owning host/engine limits still
/// decide whether actual initialization can be admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftNodeProfile {
    pub pool: String,
    pub region: String,
    pub zone: String,
    pub group_capacity: u32,
    pub byte_capacity: u64,
    pub bytes_per_group: u64,
    pub leader_capacity: u32,
    pub draining: bool,
}

impl RaftNodeProfile {
    pub(crate) fn validate(&self, max_groups: usize, image_reservation: u64) -> anyhow::Result<()> {
        let label = |value: &str| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        };
        anyhow::ensure!(
            label(&self.pool)
                && label(&self.region)
                && label(&self.zone)
                && self.group_capacity > 0
                && self.group_capacity as usize <= max_groups
                && self.leader_capacity > 0
                && self.leader_capacity <= self.group_capacity
                && self.bytes_per_group >= image_reservation
                && self.bytes_per_group > 0
                && self.byte_capacity >= self.bytes_per_group
                && u64::try_from(max_groups)
                    .ok()
                    .and_then(|groups| groups.checked_add(1))
                    .and_then(|groups| self.bytes_per_group.checked_mul(groups))
                    .is_some(),
            "invalid Raft node placement profile"
        );
        Ok(())
    }
}

/// Occupancy is scoped to the exact registered cell history. An existing or
/// retired durable identity cannot be treated as a blank standby.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaOccupancy {
    Standby,
    Initializing,
    Member(MemberId),
    Retired(MemberId),
    Occupied(MemberId),
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeInspection {
    pub identity: NodeIdentity,
    pub storage_instance: EngineInstanceId,
    pub profile: RaftNodeProfile,
    /// Durable groups, with a conservative extra reservation during activation.
    pub groups: u32,
    pub reserved_bytes: u64,
}

/// A complete node-wide scan supplies this value. Missing/old load cannot be
/// replaced with an invented zero when choosing a leadership transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderLoad {
    pub leaders: u32,
    pub capacity: u32,
    pub sample_age_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberInspection {
    pub member: MemberId,
    pub term: Term,
    pub leader: Option<MemberId>,
    pub is_leader: bool,
    pub membership_revision: LogIndex,
    pub member_high_water: u64,
    pub committed: LogIndex,
    /// Durable apply can lead local publication; this field is not visible state.
    pub applied: LogPosition,
    pub published: Option<LogPosition>,
    pub repair: Option<RepairStatus>,
    /// Conservative age at capture since the local status request began.
    /// Receiving adapters also account for the complete request duration.
    pub sample_age_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellInspection {
    pub scope: GroupScope,
    pub schema_digest: [u8; 32],
    pub birth_digest: [u8; 32],
    pub node: NodeInspection,
    pub occupancy: ReplicaOccupancy,
    pub member: Option<MemberInspection>,
    pub leader_load: Option<LeaderLoad>,
}

/// Progress captured with one physical group's complete quorum-selected root.
/// This never substitutes a business cell identity or grants leader authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupInspection {
    pub history: cellar_logic::cell_group::GroupHistory,
    pub schema_digest: [u8; 32],
    pub birth_digest: [u8; 32],
    pub node: NodeInspection,
    pub member: Option<MemberInspection>,
    /// None means no complete node-wide leadership scan was captured.
    pub leader_load: Option<LeaderLoad>,
}

pub(crate) const RESPONSE_RESERVATION: usize = 64 * 1024;
