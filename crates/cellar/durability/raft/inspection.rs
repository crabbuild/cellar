//! Read-only inspection values. None of these observations grants membership,
//! fresh-quorum seeding, read visibility or an acknowledgement proof.
use super::member_state::{GroupMembership, RepairStatus};
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

// The transport uses the validated ledger/configuration pair to encode a strict
// bounded response. The public SDK returns only the observation above.
#[derive(Clone, Debug)]
pub(crate) struct InspectionSnapshot {
    pub view: CellInspection,
    pub membership: Option<GroupMembership>,
    pub configuration: Option<::raft::prelude::ConfState>,
}

pub(crate) const RESPONSE_RESERVATION: usize = 64 * 1024;

impl InspectionSnapshot {
    pub(crate) fn validate(&self) -> Result<(), cellar_logic::consensus::CommandFailure> {
        use cellar_logic::consensus::CommandFailure;
        let invalid = CommandFailure::CorruptState;
        let view = &self.view;
        if view.scope.validate().is_err()
            || view.schema_digest == [0; 32]
            || view.birth_digest == [0; 32]
            || view.node.profile.validate(4096, 1).is_err()
            || view.node.groups > 32_769
            || view
                .node
                .profile
                .bytes_per_group
                .checked_mul(u64::from(view.node.groups))
                != Some(view.node.reserved_bytes)
        {
            return Err(invalid);
        }
        match (
            &view.occupancy,
            &view.member,
            &self.membership,
            &self.configuration,
        ) {
            (
                ReplicaOccupancy::Member(member),
                Some(sample),
                Some(membership),
                Some(configuration),
            ) => {
                membership
                    .validate(configuration, sample.applied.index.get())
                    .map_err(|_| CommandFailure::CorruptState)?;
                if *member != sample.member
                    || view.node.groups == 0
                    || sample.term.get() == 0
                    || sample.committed < sample.applied.index
                    || sample.applied.term > sample.term
                    || sample.membership_revision != membership.revision()
                    || sample.member_high_water != membership.high_water()
                    || sample.repair
                        != membership
                            .repair_status()
                            .map_err(|_| CommandFailure::CorruptState)?
                    || !super::member_state::members(configuration).contains(&member.get())
                    || sample.leader.is_some_and(|leader| {
                        !super::member_state::members(configuration).contains(&leader.get())
                    })
                    || (sample.is_leader && sample.leader != Some(*member))
                    || sample.published.is_some_and(|cut| {
                        cut.index.get() == 0
                            || cut.term.get() == 0
                            || cut.index > sample.applied.index
                            || cut.term > sample.applied.term
                            || (cut.index == sample.applied.index
                                && cut.term != sample.applied.term)
                    })
                {
                    return Err(invalid);
                }
            }
            (ReplicaOccupancy::Member(_), ..) => return Err(invalid),
            (ReplicaOccupancy::Retired(_) | ReplicaOccupancy::Occupied(_), ..)
                if view.node.groups == 0 =>
            {
                return Err(invalid)
            }
            (_, None, None, None) => (),
            _ => return Err(invalid),
        }
        if view.leader_load.as_ref().is_some_and(|load| {
            load.capacity != view.node.profile.leader_capacity || load.leaders > view.node.groups
        }) {
            return Err(invalid);
        }
        Ok(())
    }
}
