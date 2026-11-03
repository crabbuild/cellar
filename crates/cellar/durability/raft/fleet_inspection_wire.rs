// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated fresh quorum metadata for a complete physical group. The body
//! never chooses a cell image and cannot be decoded as an ordinary inspection.
use super::cell_groups::{CellGroupQuorumMembership, CellGroupStatus, GroupManifest};
use super::fleet_catalog::FleetGroupAuthorization;
use super::host::{FleetMembershipObservation, FleetPublishedCut, RaftHostObservation};
use super::inspection::{GroupInspection, LeaderLoad, MemberInspection};
use super::member_state::{FleetGroupBinding, GroupMembership};
use super::transport::TransportError;
use super::{inspection_wire, node_inspection_wire};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{EngineInstanceId, LogIndex, LogPosition, MemberId, ReplicaIdentity, Term},
};
use raft::prelude::ConfState;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_BODY: usize = 12 + node_inspection_wire::MAX_BODY + inspection_wire::MAX_BODY;
pub(crate) const RESPONSE_BYTES: usize = super::physical_inventory::RESPONSE_BYTES;
const DOMAIN: &str = "cellar-fleet-group-quorum-1";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct History {
    pub binding: FleetGroupBinding,
    birth: [u8; 32],
    schema: [u8; 32],
}

impl History {
    pub(crate) fn new(authorization: &FleetGroupAuthorization) -> Self {
        let (birth, schema) = authorization.history_digests();
        Self {
            binding: authorization.binding().clone(),
            birth,
            schema,
        }
    }

    pub(crate) fn scope(&self) -> Result<GroupHistory, TransportError> {
        if self.birth == [0; 32] || self.schema == [0; 32] {
            return Err(TransportError::Identity);
        }
        self.binding.scope().map_err(|_| TransportError::Identity)
    }

    pub(crate) fn matches(&self, authorization: &FleetGroupAuthorization) -> bool {
        self == &Self::new(authorization)
    }
}

#[derive(Clone)]
pub(crate) struct GroupSnapshot {
    pub replica: ReplicaIdentity<GroupHistory>,
    pub engine: EngineInstanceId,
    pub root: GroupManifest,
    pub barrier: LogPosition,
    pub membership: GroupMembership,
    pub configuration: ConfState,
    pub status: CellGroupStatus,
    pub load: Option<LeaderLoad>,
}

impl GroupSnapshot {
    pub(crate) fn capture(selected: &CellGroupQuorumMembership) -> Self {
        Self {
            replica: selected.replica().clone(),
            engine: selected.engine(),
            root: selected.root().clone(),
            barrier: selected.barrier(),
            membership: selected.membership().clone(),
            configuration: selected.configuration().clone(),
            status: selected.status().clone(),
            load: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        self.root.check().map_err(|_| TransportError::Protocol)?;
        self.membership
            .validate(&self.configuration, self.root.position().index.get())
            .map_err(|_| TransportError::Protocol)?;
        self.membership
            .validate_scope(&self.replica.scope)
            .map_err(|_| TransportError::Identity)?;
        let fleet = self
            .membership
            .fleet_status()
            .ok_or(TransportError::Identity)?;
        let leader = self
            .status
            .leader
            .and_then(|id| MemberId::new(id).ok())
            .ok_or(TransportError::Protocol)?;
        if self.root.route().history != self.replica.scope
            || !fleet.binding.matches_scope(&self.replica.scope)
            || self.root.membership_revision() != self.membership.revision().get()
            || self.barrier.index.get() == 0
            || self.barrier.term.get() == 0
            || self.barrier.index < self.membership.revision()
            || self.root.position().index < self.barrier.index
            || self.root.position().term != self.barrier.term
            || self.status.term != self.barrier.term
            || self.status.applied != self.root.position()
            || self.status.committed < self.root.position().index
            || !self.membership.permits_group(&self.replica)
            || !(self.configuration.voters.contains(&leader.get())
                || self.configuration.voters_outgoing.contains(&leader.get()))
            || self.status.ready && self.status.leader != Some(self.replica.member.get())
            || !self.root.is_active()
        {
            return Err(TransportError::Identity);
        }
        Ok(())
    }
}

pub(crate) struct LocalObservation {
    pub node: Box<RaftHostObservation>,
    pub snapshot: GroupSnapshot,
}

impl LocalObservation {
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        node_inspection_wire::validate(&self.node)?;
        self.snapshot.validate()?;
        let s = &self.snapshot;
        if s.replica.node != self.node.node.identity.node
            || s.replica.disk != self.node.node.identity.disk
            || s.replica.process != self.node.node.identity.process
            || s.engine != self.node.node.storage_instance
            || s.load.as_ref().is_some_and(|load| {
                load.capacity != self.node.node.profile.leader_capacity
                    || load.leaders > self.node.node.groups
            })
        {
            return Err(TransportError::Identity);
        }
        Ok(())
    }

    pub(crate) fn into_peer(
        self,
        authorization: &FleetGroupAuthorization,
        sample_age_ms: u64,
    ) -> Result<RaftPeerFleetMembershipObservation, TransportError> {
        self.validate()?;
        let s = self.snapshot;
        let progress = GroupInspection {
            history: s.replica.scope,
            schema_digest: s.root.route().schema_digest,
            birth_digest: s.root.route().birth_digest,
            node: self.node.node.clone(),
            member: Some(MemberInspection {
                member: s.replica.member,
                term: s.status.term,
                leader: s
                    .status
                    .leader
                    .map(|id| MemberId::new(id).map_err(|_| TransportError::Protocol))
                    .transpose()?,
                is_leader: s.status.leader == Some(s.replica.member.get()),
                membership_revision: s.membership.revision(),
                member_high_water: s.membership.high_water(),
                committed: s.status.committed,
                applied: s.status.applied,
                published: Some(s.root.position()),
                repair: s
                    .membership
                    .repair_status()
                    .map_err(|_| TransportError::Protocol)?,
                sample_age_ms: 0,
            }),
            leader_load: s.load.clone(),
        };
        let engine = s.engine;
        let membership = FleetMembershipObservation::from_quorum_group(s, authorization)
            .map_err(|_| TransportError::Identity)?;
        let published = FleetPublishedCut::capture_group(&membership, engine);
        let mut observed = RaftPeerFleetMembershipObservation {
            node: *self.node,
            membership,
            progress,
            published,
            sample_age_ms: 0,
        };
        observed
            .record_delivery_age(sample_age_ms)
            .map_err(|_| TransportError::Limit)?;
        Ok(observed)
    }
}

fn nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Option<T>, D::Error> {
    Option::deserialize(decoder)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Body {
    domain: String,
    root: GroupManifest,
    member: String,
    barrier_index: String,
    barrier_term: String,
    committed: String,
    #[serde(deserialize_with = "nullable")]
    leader: Option<String>,
    ready: bool,
    membership: GroupMembership,
    configuration: Vec<u8>,
    #[serde(deserialize_with = "nullable")]
    load: Option<Load>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Load {
    leaders: u32,
    capacity: u32,
    age: u64,
}

pub(crate) fn encode(observation: &LocalObservation) -> Result<Vec<u8>, TransportError> {
    observation.validate()?;
    let node = node_inspection_wire::encode(&observation.node)?;
    let s = &observation.snapshot;
    let group = serde_json::to_vec(&Body {
        domain: DOMAIN.into(),
        root: s.root.clone(),
        member: s.replica.member.get().to_string(),
        barrier_index: s.barrier.index.get().to_string(),
        barrier_term: s.barrier.term.get().to_string(),
        committed: s.status.committed.get().to_string(),
        leader: s.status.leader.map(|id| id.to_string()),
        ready: s.status.ready,
        membership: s.membership.clone(),
        configuration: super::manifest::encode_conf(&s.configuration)
            .map_err(|_| TransportError::Protocol)?,
        load: s.load.as_ref().map(|l| Load {
            leaders: l.leaders,
            capacity: l.capacity,
            age: l.sample_age_ms,
        }),
    })
    .map_err(|_| TransportError::Protocol)?;
    if group.len() > inspection_wire::MAX_BODY {
        return Err(TransportError::Limit);
    }
    let mut bytes = Vec::with_capacity(12 + node.len() + group.len());
    for word in [1, node.len() as u32, group.len() as u32] {
        bytes.extend_from_slice(&word.to_be_bytes());
    }
    bytes.extend_from_slice(&node);
    bytes.extend_from_slice(&group);
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<LocalObservation, TransportError> {
    if bytes.len() < 12 || bytes.len() > MAX_BODY {
        return Err(TransportError::Limit);
    }
    let word = |i| u32::from_be_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
    let (node_len, group_len) = (word(4) as usize, word(8) as usize);
    if word(0) != 1
        || node_len > node_inspection_wire::MAX_BODY
        || group_len > inspection_wire::MAX_BODY
        || 12 + node_len + group_len != bytes.len()
    {
        return Err(TransportError::Protocol);
    }
    let node = node_inspection_wire::decode(&bytes[12..12 + node_len])?;
    let body: Body =
        serde_json::from_slice(&bytes[12 + node_len..]).map_err(|_| TransportError::Protocol)?;
    if body.domain != DOMAIN || body.configuration.len() > 4096 {
        return Err(TransportError::Protocol);
    }
    let positive = |value: &str| {
        let n = super::transport_frame::decimal(value)?;
        if n == 0 || n >= u64::MAX - 2 {
            return Err(TransportError::Protocol);
        }
        Ok(n)
    };
    let barrier = LogPosition {
        index: LogIndex::new(positive(&body.barrier_index)?),
        term: Term::new(positive(&body.barrier_term)?),
    };
    let leader = body.leader.as_deref().map(positive).transpose()?;
    let snapshot = GroupSnapshot {
        replica: ReplicaIdentity {
            scope: body.root.route().history,
            member: MemberId::new(positive(&body.member)?).map_err(|_| TransportError::Identity)?,
            node: node.node.identity.node,
            disk: node.node.identity.disk,
            process: node.node.identity.process,
        },
        engine: node.node.storage_instance,
        status: CellGroupStatus {
            term: barrier.term,
            leader,
            applied: body.root.position(),
            committed: LogIndex::new(positive(&body.committed)?),
            ready: body.ready,
        },
        root: body.root,
        barrier,
        membership: body.membership,
        configuration: super::manifest::decode_conf(&body.configuration)
            .map_err(|_| TransportError::Protocol)?,
        load: body.load.map(|l| LeaderLoad {
            leaders: l.leaders,
            capacity: l.capacity,
            sample_age_ms: l.age,
        }),
    };
    let observation = LocalObservation { node, snapshot };
    observation.validate()?;
    Ok(observation)
}

/// Opaque result from the purpose-specific authenticated safe-ReadIndex lane.
/// Retaining it neither extends freshness nor permits mutation or reclamation.
#[derive(Clone, Debug)]
pub struct RaftPeerFleetMembershipObservation {
    node: RaftHostObservation,
    membership: FleetMembershipObservation,
    progress: GroupInspection,
    published: FleetPublishedCut,
    sample_age_ms: u64,
}

impl RaftPeerFleetMembershipObservation {
    pub(crate) fn record_delivery_age(
        &mut self,
        age: u64,
    ) -> Result<(), super::inspection::InspectionError> {
        use super::inspection::InspectionError;
        let next = self.sample_age_ms.max(age);
        let additional = next - self.sample_age_ms;
        if let Some(member) = &mut self.progress.member {
            member.sample_age_ms = member
                .sample_age_ms
                .checked_add(additional)
                .ok_or(InspectionError::Unavailable)?;
        }
        if let Some(load) = &mut self.progress.leader_load {
            load.sample_age_ms = load
                .sample_age_ms
                .checked_add(additional)
                .ok_or(InspectionError::Unavailable)?;
        }
        self.sample_age_ms = next;
        Ok(())
    }

    pub fn node(&self) -> &RaftHostObservation {
        &self.node
    }

    pub fn membership(&self) -> &FleetMembershipObservation {
        &self.membership
    }

    pub fn progress(&self) -> &GroupInspection {
        &self.progress
    }

    /// Atomic complete-root publication. The zero-cell root is explicit.
    pub fn published_cut(&self) -> &FleetPublishedCut {
        &self.published
    }

    pub fn sample_age_ms(&self) -> u64 {
        self.sample_age_ms
    }
}

#[cfg(test)]
#[path = "fleet_inspection_wire_tests.rs"]
mod tests;
