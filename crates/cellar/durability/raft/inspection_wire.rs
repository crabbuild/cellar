//! Strict bounded inspection body. Its physical route and payload digest are
//! authenticated separately by the purpose-specific TLS RPC header.
use super::inspection::*;
use super::member_state::GroupMembership;
use super::transport::TransportError;
use super::transport_frame::decimal;
use crate::application::consensus::protocol::WireScope;
use cellar_logic::consensus::{
    DiskLineage, EngineInstanceId, LogIndex, LogPosition, MemberId, NodeId, NodeIdentity,
    ProcessGeneration, Term,
};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_BODY: usize = 64 * 1024;

fn required_nullable<'de, T, D>(decoder: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::<T>::deserialize(decoder)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Profile {
    pool: String,
    region: String,
    zone: String,
    group_capacity: u32,
    byte_capacity: u64,
    bytes_per_group: u64,
    leader_capacity: u32,
    draining: bool,
}

impl From<&RaftNodeProfile> for Profile {
    fn from(value: &RaftNodeProfile) -> Self {
        Self {
            pool: value.pool.clone(),
            region: value.region.clone(),
            zone: value.zone.clone(),
            group_capacity: value.group_capacity,
            byte_capacity: value.byte_capacity,
            bytes_per_group: value.bytes_per_group,
            leader_capacity: value.leader_capacity,
            draining: value.draining,
        }
    }
}

impl From<Profile> for RaftNodeProfile {
    fn from(value: Profile) -> Self {
        Self {
            pool: value.pool,
            region: value.region,
            zone: value.zone,
            group_capacity: value.group_capacity,
            byte_capacity: value.byte_capacity,
            bytes_per_group: value.bytes_per_group,
            leader_capacity: value.leader_capacity,
            draining: value.draining,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    index: String,
    term: String,
}

impl From<LogPosition> for Position {
    fn from(value: LogPosition) -> Self {
        Self {
            index: value.index.get().to_string(),
            term: value.term.get().to_string(),
        }
    }
}

impl Position {
    fn decode(self) -> Result<LogPosition, TransportError> {
        let index = decimal(&self.index)?;
        let term = decimal(&self.term)?;
        if (index == 0) != (term == 0) || index >= u64::MAX - 2 || term >= u64::MAX - 2 {
            return Err(TransportError::Protocol);
        }
        Ok(LogPosition {
            index: LogIndex::new(index),
            term: Term::new(term),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    member: String,
    term: String,
    #[serde(deserialize_with = "required_nullable")]
    leader: Option<String>,
    is_leader: bool,
    revision: String,
    high_water: String,
    committed: String,
    applied: Position,
    #[serde(deserialize_with = "required_nullable")]
    published: Option<Position>,
    sample_age_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    voters: Vec<u64>,
    outgoing: Vec<u64>,
    learners: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
}

impl From<&::raft::prelude::ConfState> for Configuration {
    fn from(value: &::raft::prelude::ConfState) -> Self {
        Self {
            voters: value.voters.clone(),
            outgoing: value.voters_outgoing.clone(),
            learners: value.learners.clone(),
            learners_next: value.learners_next.clone(),
            auto_leave: value.auto_leave,
        }
    }
}

impl Configuration {
    fn decode(self) -> Result<::raft::prelude::ConfState, TransportError> {
        if [
            &self.voters,
            &self.outgoing,
            &self.learners,
            &self.learners_next,
        ]
        .into_iter()
        .any(|ids| ids.len() > 32)
        {
            return Err(TransportError::Limit);
        }
        Ok(::raft::prelude::ConfState {
            voters: self.voters,
            voters_outgoing: self.outgoing,
            learners: self.learners,
            learners_next: self.learners_next,
            auto_leave: self.auto_leave,
            ..Default::default()
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Load {
    leaders: u32,
    capacity: u32,
    sample_age_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    format: u32,
    scope: WireScope,
    schema: [u8; 32],
    birth: [u8; 32],
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    storage_instance: [u8; 16],
    profile: Profile,
    groups: u32,
    reserved_bytes: u64,
    occupancy: u8,
    #[serde(deserialize_with = "required_nullable")]
    member: Option<String>,
    #[serde(deserialize_with = "required_nullable")]
    observation: Option<Member>,
    #[serde(deserialize_with = "required_nullable")]
    membership: Option<GroupMembership>,
    #[serde(deserialize_with = "required_nullable")]
    configuration: Option<Configuration>,
    #[serde(deserialize_with = "required_nullable")]
    leader_load: Option<Load>,
}

pub(crate) fn encode(snapshot: &InspectionSnapshot) -> Result<Vec<u8>, TransportError> {
    snapshot.validate().map_err(|_| TransportError::Protocol)?;
    let view = &snapshot.view;
    let (occupancy, member) = match view.occupancy {
        ReplicaOccupancy::Standby => (0, None),
        ReplicaOccupancy::Initializing => (1, None),
        ReplicaOccupancy::Member(id) => (2, Some(id.get().to_string())),
        ReplicaOccupancy::Retired(id) => (3, Some(id.get().to_string())),
        ReplicaOccupancy::Occupied(id) => (4, Some(id.get().to_string())),
        ReplicaOccupancy::Failed => (5, None),
    };
    let body = Body {
        format: 1,
        scope: WireScope::from(&view.scope),
        schema: view.schema_digest,
        birth: view.birth_digest,
        node: view.node.identity.node.bytes(),
        disk: view.node.identity.disk.bytes(),
        process: view.node.identity.process.bytes(),
        storage_instance: view.node.storage_instance.bytes(),
        profile: Profile::from(&view.node.profile),
        groups: view.node.groups,
        reserved_bytes: view.node.reserved_bytes,
        occupancy,
        member,
        observation: view.member.as_ref().map(|value| Member {
            member: value.member.get().to_string(),
            term: value.term.get().to_string(),
            leader: value.leader.map(|leader| leader.get().to_string()),
            is_leader: value.is_leader,
            revision: value.membership_revision.get().to_string(),
            high_water: value.member_high_water.to_string(),
            committed: value.committed.get().to_string(),
            applied: Position::from(value.applied),
            published: value.published.map(Position::from),
            sample_age_ms: value.sample_age_ms,
        }),
        membership: snapshot.membership.clone(),
        configuration: snapshot.configuration.as_ref().map(Configuration::from),
        leader_load: view.leader_load.as_ref().map(|value| Load {
            leaders: value.leaders,
            capacity: value.capacity,
            sample_age_ms: value.sample_age_ms,
        }),
    };
    let bytes = serde_json::to_vec(&body).map_err(|_| TransportError::Protocol)?;
    if bytes.len() > MAX_BODY {
        return Err(TransportError::Limit);
    }
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Box<InspectionSnapshot>, TransportError> {
    if bytes.is_empty() || bytes.len() > MAX_BODY {
        return Err(TransportError::Limit);
    }
    let body: Body = serde_json::from_slice(bytes).map_err(|_| TransportError::Protocol)?;
    if body.format != 1 {
        return Err(TransportError::Protocol);
    }
    let id = body.member.as_deref().map(member).transpose()?;
    let occupancy = match (body.occupancy, id) {
        (0, None) => ReplicaOccupancy::Standby,
        (1, None) => ReplicaOccupancy::Initializing,
        (2, Some(id)) => ReplicaOccupancy::Member(id),
        (3, Some(id)) => ReplicaOccupancy::Retired(id),
        (4, Some(id)) => ReplicaOccupancy::Occupied(id),
        (5, None) => ReplicaOccupancy::Failed,
        _ => return Err(TransportError::Protocol),
    };
    let membership = body.membership;
    let observation = body
        .observation
        .map(|value| {
            let term = positive(&value.term)?;
            let revision = positive(&value.revision)?;
            let high_water = positive(&value.high_water)?;
            let committed = positive(&value.committed)?;
            Ok::<_, TransportError>(MemberInspection {
                member: member(&value.member)?,
                term: Term::new(term),
                leader: value.leader.as_deref().map(member).transpose()?,
                is_leader: value.is_leader,
                membership_revision: LogIndex::new(revision),
                member_high_water: high_water,
                committed: LogIndex::new(committed),
                applied: value.applied.decode()?,
                published: value.published.map(Position::decode).transpose()?,
                repair: membership
                    .as_ref()
                    .map(|ledger| ledger.repair_status())
                    .transpose()
                    .map_err(|_| TransportError::Protocol)?
                    .flatten(),
                sample_age_ms: value.sample_age_ms,
            })
        })
        .transpose()?;
    let snapshot = Box::new(InspectionSnapshot {
        view: CellInspection {
            scope: body.scope.decode().map_err(|_| TransportError::Identity)?,
            schema_digest: body.schema,
            birth_digest: body.birth,
            node: NodeInspection {
                identity: NodeIdentity {
                    node: NodeId::new(body.node).map_err(|_| TransportError::Identity)?,
                    disk: DiskLineage::new(body.disk).map_err(|_| TransportError::Identity)?,
                    process: ProcessGeneration::new(body.process)
                        .map_err(|_| TransportError::Identity)?,
                },
                storage_instance: EngineInstanceId::new(body.storage_instance)
                    .map_err(|_| TransportError::Identity)?,
                profile: body.profile.into(),
                groups: body.groups,
                reserved_bytes: body.reserved_bytes,
            },
            occupancy,
            member: observation,
            leader_load: body.leader_load.map(|value| LeaderLoad {
                leaders: value.leaders,
                capacity: value.capacity,
                sample_age_ms: value.sample_age_ms,
            }),
        },
        membership,
        configuration: body.configuration.map(Configuration::decode).transpose()?,
        publication: None,
    });
    snapshot.validate().map_err(|_| TransportError::Protocol)?;
    Ok(snapshot)
}

fn positive(value: &str) -> Result<u64, TransportError> {
    let value = decimal(value)?;
    if value == 0 || value >= u64::MAX - 2 {
        return Err(TransportError::Protocol);
    }
    Ok(value)
}

fn member(value: &str) -> Result<MemberId, TransportError> {
    MemberId::new(positive(value)?).map_err(|_| TransportError::Identity)
}

#[cfg(test)]
#[path = "inspection_wire_tests.rs"]
mod tests;
