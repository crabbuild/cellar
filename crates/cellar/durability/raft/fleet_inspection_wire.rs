//! Quorum metadata binds one rooted history to one authenticated node route.
//! The body reuses the bounded node and membership inspection codecs.
use super::fleet_catalog::FleetGroupAuthorization;
use super::host::{FleetMembershipObservation, FleetPublishedCut, RaftHostObservation};
use super::inspection::{CellInspection, InspectionSnapshot};
use super::member_state::FleetGroupBinding;
use super::transport::TransportError;
use super::{inspection_wire, node_inspection_wire};
use cellar_logic::consensus::{GroupScope, LogIndex};
use serde::{Deserialize, Serialize};

#[path = "fleet_cut_wire.rs"]
mod publication;

pub(crate) const MAX_BODY: usize =
    20 + node_inspection_wire::MAX_BODY + inspection_wire::MAX_BODY + publication::MAX_BODY;
pub(crate) const RESPONSE_BYTES: usize = 128 * 1024;

#[cfg(test)]
#[path = "fleet_inspection_wire_tests.rs"]
mod tests;

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

    pub(crate) fn scope(&self) -> Result<GroupScope, TransportError> {
        if self.birth == [0; 32] || self.schema == [0; 32] {
            return Err(TransportError::Identity);
        }
        self.binding.scope().map_err(|_| TransportError::Identity)
    }

    pub(crate) fn matches(&self, authorization: &FleetGroupAuthorization) -> bool {
        self == &Self::new(authorization)
    }
}

pub(crate) struct LocalObservation {
    pub node: Box<RaftHostObservation>,
    pub snapshot: Box<InspectionSnapshot>,
    pub barrier: LogIndex,
}

impl LocalObservation {
    fn validate(&self) -> Result<(), TransportError> {
        node_inspection_wire::validate(&self.node)?;
        self.snapshot
            .validate()
            .map_err(|_| TransportError::Protocol)?;
        let view = &self.snapshot.view;
        if view.node.identity != self.node.node.identity
            || view.node.storage_instance != self.node.node.storage_instance
            || self.barrier.get() == 0
            || self.snapshot.publication.is_none()
            || view
                .member
                .as_ref()
                .is_none_or(|m| m.applied.index < self.barrier)
            || self
                .snapshot
                .membership
                .as_ref()
                .and_then(|m| m.fleet_status())
                .is_none()
        {
            return Err(TransportError::Identity);
        }
        Ok(())
    }

    pub(crate) fn into_peer(
        self,
        authorization: &FleetGroupAuthorization,
        sample_age_ms: u64,
    ) -> Result<super::host::RaftPeerFleetMembershipObservation, TransportError> {
        self.validate()?;
        let membership = FleetMembershipObservation::from_quorum_snapshot(
            &self.snapshot,
            authorization,
            self.barrier,
        )
        .map_err(|_| TransportError::Identity)?;
        let published = FleetPublishedCut::capture(
            &membership,
            self.node.node.storage_instance,
            self.snapshot
                .publication
                .as_ref()
                .ok_or(TransportError::Protocol)?,
        );
        Ok(super::host::RaftPeerFleetMembershipObservation::new(
            *self.node,
            membership,
            self.snapshot.view,
            published,
            sample_age_ms,
        ))
    }
}

pub(crate) fn encode(observation: &LocalObservation) -> Result<Vec<u8>, TransportError> {
    observation.validate()?;
    let node = node_inspection_wire::encode(&observation.node)?;
    let group = inspection_wire::encode(&observation.snapshot)?;
    let mut bytes = Vec::with_capacity(20 + node.len() + group.len());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&(node.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&(group.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&observation.barrier.get().to_be_bytes());
    bytes.extend_from_slice(&node);
    bytes.extend_from_slice(&group);
    bytes.extend_from_slice(&publication::encode(
        observation
            .snapshot
            .publication
            .as_ref()
            .ok_or(TransportError::Protocol)?,
    ));
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<LocalObservation, TransportError> {
    if bytes.len() < 20 || bytes.len() > MAX_BODY {
        return Err(TransportError::Limit);
    }
    // The fixed prefix is checked above before reading any word or lengths.
    let word = |start| {
        u32::from_be_bytes([
            bytes[start],
            bytes[start + 1],
            bytes[start + 2],
            bytes[start + 3],
        ])
    };
    let node = word(4) as usize;
    let group = word(8) as usize;
    if word(0) != 1
        || node > node_inspection_wire::MAX_BODY
        || group > inspection_wire::MAX_BODY
        || 20 + node + group >= bytes.len()
        || bytes.len() - (20 + node + group) > publication::MAX_BODY
    {
        return Err(TransportError::Protocol);
    }
    let mut snapshot = inspection_wire::decode(&bytes[20 + node..20 + node + group])?;
    snapshot.publication = Some(publication::decode(&bytes[20 + node + group..])?);
    let observation = LocalObservation {
        node: node_inspection_wire::decode(&bytes[20..20 + node])?,
        snapshot,
        barrier: LogIndex::new(u64::from_be_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
        ])),
    };
    observation.validate()?;
    Ok(observation)
}

/// Opaque authenticated result. Keeping it does not extend its freshness or
/// confer placement, reservation-release or removal authority.
#[derive(Clone, Debug)]
pub struct RaftPeerFleetMembershipObservation {
    node: RaftHostObservation,
    membership: FleetMembershipObservation,
    progress: CellInspection,
    published: Option<FleetPublishedCut>,
    sample_age_ms: u64,
}

impl RaftPeerFleetMembershipObservation {
    pub(crate) fn record_delivery_age(
        &mut self,
        age: u64,
    ) -> Result<(), super::inspection::InspectionError> {
        use super::inspection::InspectionError;
        self.sample_age_ms = self.sample_age_ms.max(age);
        if let Some(member) = &mut self.progress.member {
            member.sample_age_ms = member
                .sample_age_ms
                .checked_add(self.sample_age_ms)
                .ok_or(InspectionError::Unavailable)?;
        }
        if let Some(load) = &mut self.progress.leader_load {
            load.sample_age_ms = load
                .sample_age_ms
                .checked_add(self.sample_age_ms)
                .ok_or(InspectionError::Unavailable)?;
        }
        Ok(())
    }

    pub(crate) fn new(
        node: RaftHostObservation,
        membership: FleetMembershipObservation,
        progress: CellInspection,
        published: Option<FleetPublishedCut>,
        sample_age_ms: u64,
    ) -> Self {
        Self {
            node,
            membership,
            progress,
            published,
            sample_age_ms,
        }
    }

    pub fn node(&self) -> &RaftHostObservation {
        &self.node
    }

    pub fn membership(&self) -> &FleetMembershipObservation {
        &self.membership
    }

    pub fn progress(&self) -> &CellInspection {
        &self.progress
    }

    /// A fixed data/configuration cut only when publication matched durable
    /// apply at capture. None means publication is pending; an empty cut is an
    /// explicit Some value whose image() is None. Neither supplies archive or
    /// current-health proof.
    pub fn published_cut(&self) -> Option<&FleetPublishedCut> {
        self.published.as_ref()
    }

    /// Whole invocation through SDK delivery, including retained reply time.
    pub fn sample_age_ms(&self) -> u64 {
        self.sample_age_ms
    }
}
