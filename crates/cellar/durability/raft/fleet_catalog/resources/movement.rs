// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One owned quorum observation selects the exact resource-backed permit.
use super::records::Request;
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::host::RaftPeerFleetMembershipObservation;
use crate::raft::member_state::{FleetMembershipPermit, RepairPlan};
use cellar_logic::consensus::{DiskLineage, LogIndex, MemberId, ProposalId};
use serde::Deserialize;
use std::collections::BTreeSet;

/// Explicit membership intent. Capacity and an exact group permit are committed
/// together; this does not supply a complete fleet placement/feasibility plan.
#[derive(Clone, Debug)]
pub struct FleetMovementRequest {
    pub resources: FleetResourceRequest,
    pub source: NodeId,
    pub plan: RepairPlan,
}

/// Historical atomic acceptance. The group still owns permit consumption and
/// target initialization. No resource release or health lease is implied.
pub struct FleetMovementAdmission {
    pub(super) resources: FleetResourceAdmission,
    pub(super) authorization: FleetPermitAuthorization,
}

impl FleetMovementAdmission {
    pub fn resources(&self) -> &FleetResourceAdmission {
        &self.resources
    }

    pub fn authorization(&self) -> FleetPermitAuthorization {
        self.authorization.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Movement {
    source: [u8; 16],
    plan: [u8; 16],
    policy_revision: u64,
    expected_revision: u64,
    obsolete: u64,
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
}

impl Movement {
    pub fn new(request: &FleetMovementRequest) -> Result<Self> {
        let plan = &request.plan;
        let selected = Self {
            source: request.source.bytes(),
            plan: plan.id.bytes(),
            policy_revision: plan.policy_revision,
            expected_revision: plan.expected_revision.get(),
            obsolete: plan.obsolete.get(),
            member: plan.replacement.member.get(),
            node: plan.replacement.node.bytes(),
            disk: plan.replacement.disk.bytes(),
        };
        selected.validate()?;
        Ok(selected)
    }

    pub fn validate(&self) -> Result<()> {
        NodeId::new(self.source).map_err(|_| invalid())?;
        if self.policy_revision == 0
            || self.expected_revision == 0
            || self.expected_revision >= u64::MAX - 2
        {
            return Err(invalid());
        }
        self.plan()?;
        Ok(())
    }

    pub fn source(&self) -> Result<NodeId> {
        NodeId::new(self.source).map_err(|_| invalid())
    }

    pub fn plan(&self) -> Result<RepairPlan> {
        Ok(RepairPlan {
            id: ProposalId::new(self.plan).map_err(|_| invalid())?,
            policy_revision: self.policy_revision,
            expected_revision: LogIndex::new(self.expected_revision),
            obsolete: MemberId::new(self.obsolete).map_err(|_| invalid())?,
            replacement: crate::raft::member_state::JoiningMember {
                member: MemberId::new(self.member).map_err(|_| invalid())?,
                node: NodeId::new(self.node).map_err(|_| invalid())?,
                disk: DiskLineage::new(self.disk).map_err(|_| invalid())?,
            },
        })
    }
}

fn invalid() -> FleetCatalogError {
    FleetCatalogError::Invalid("resource membership intent")
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MovementOwner {
    pub node: [u8; 16],
    #[serde(with = "owner_digest")]
    pub boot: [u8; 32],
    pub member: u64,
}

impl MovementOwner {
    fn new(boot: &BootBinding, member: MemberId) -> Result<Self> {
        Ok(Self {
            node: boot.node,
            boot: Self::digest(boot)?,
            member: member.get(),
        })
    }

    fn digest(boot: &BootBinding) -> Result<[u8; 32]> {
        boot.validate().map_err(|_| invalid())?;
        let mut bytes = b"cellar-fleet-seed-owner-1".to_vec();
        bytes.extend_from_slice(&serde_json::to_vec(boot).map_err(|_| invalid())?);
        Ok(hash(&bytes))
    }

    pub fn matches(&self, boot: &BootBinding) -> Result<bool> {
        Ok(self.node == boot.node && self.boot == Self::digest(boot)?)
    }
}

mod owner_digest {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        digest: &[u8; 32],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(digest))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        decoder: D,
    ) -> std::result::Result<[u8; 32], D::Error> {
        let value = String::deserialize(decoder)?;
        let mut digest = [0; 32];
        hex::decode_to_slice(&value, &mut digest).map_err(serde::de::Error::custom)?;
        if value != hex::encode(digest) {
            return Err(serde::de::Error::custom("noncanonical boot digest"));
        }
        Ok(digest)
    }
}

pub(super) struct MovementProof {
    pub permit: FleetMembershipPermit,
    pub owners: Vec<MovementOwner>,
}

pub(super) struct Source {
    observation: RaftPeerFleetMembershipObservation,
    pub started: u64,
}

impl FleetCatalog {
    /// Own fresh target occupancy and source ReadIndex under one original
    /// deadline. Exact admitted retries are resolved before these prerequisites.
    pub async fn reserve_movement(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetMovementRequest,
        observer: &RaftCells,
        namespaces: &[crate::raft::catalog::NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetMovementAdmission> {
        let mut canonical = Request::new(self, &request.resources)?;
        canonical.movement = Some(Movement::new(request)?);
        canonical.validate(self)?;
        let resources = self
            .reserve_resource_request(snapshot, canonical, observer, namespaces, timeout)
            .await?;
        let permit = resources
            .record
            .permit
            .clone()
            .ok_or(FleetCatalogError::Corrupt("missing movement permit"))?;
        let authorization = FleetPermitAuthorization::admitted(permit, resources.applied);
        Ok(FleetMovementAdmission {
            resources,
            authorization,
        })
    }

    pub(super) async fn observe_resource_source(
        &self,
        snapshot: &FleetSnapshot,
        request: &Request,
        observer: &RaftCells,
        deadline: &super::super::deadline::Deadline<'_>,
    ) -> Result<Option<Source>> {
        let Some(movement) = &request.movement else {
            return Ok(None);
        };
        let scope = self
            .run(deadline.remaining()?, async {
                self.history_inner(snapshot, &request.group)
                    .await?
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("movement scope"))
            })
            .await?;
        let authorization = self
            .authorize_group(snapshot, &scope, deadline.remaining()?)
            .await?;
        let observation = observer
            .observe_peer_fleet_membership(
                NodeId::new(movement.source).map_err(|_| invalid())?,
                authorization,
                snapshot.revision(),
                deadline.remaining()?,
            )
            .await
            .map_err(FleetCatalogError::Observation)?;
        let started = self
            .resource_now()?
            .checked_sub(observation.sample_age_ms())
            .ok_or(FleetCatalogError::Unconfirmed)?;
        self.require_resource_age(started)?;
        Ok(Some(Source {
            observation,
            started,
        }))
    }

    pub(super) async fn resource_permit(
        &self,
        snapshot: &FleetSnapshot,
        request: &Request,
        source: Option<&Source>,
    ) -> Result<Option<MovementProof>> {
        let Some(movement) = &request.movement else {
            return Ok(None);
        };
        let source = source.ok_or(FleetCatalogError::Unconfirmed)?;
        self.require_resource_age(source.started)?;
        let observed = &source.observation;
        let proof = observed.membership();
        let replica = proof.replica();
        let node = observed.node();
        let history = self.history_inner(snapshot, &request.group).await?;
        let source = self.host_from(snapshot, replica.node).await?;
        let enrollment = node
            .current_enrollment
            .as_ref()
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let plan = movement.plan()?;
        let configuration = proof.configuration();
        let voters = configuration
            .voters()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if replica.node.bytes() != movement.source
            || !matches!(
                source.state(),
                FleetNodeState::Active | FleetNodeState::Draining
            )
            || node.fleet_boot.as_ref() != Some(&source.boot_binding()?)
            || node.node.identity.node != replica.node
            || node.node.identity.disk != replica.disk
            || node.node.identity.process != replica.process
            || source.disk() != replica.disk
            || source.storage_instance() != Some(node.node.storage_instance)
            || enrollment.fleet != source.fleet()
            || enrollment.epoch != source.epoch()
            || enrollment.state != source.state()
            || enrollment.physical_host != source.physical_host()
            || enrollment.capability_digest != source.capability_digest()
            || enrollment.revision < snapshot.revision()
            || node
                .fleet_trust_revision
                .is_none_or(|cut| cut < snapshot.revision())
            || proof.status().binding != history.binding
            || proof.birth_digest() != history.birth_digest()
            || proof.schema_digest() != history.schema_digest()
            || proof.membership_revision() != plan.expected_revision
            || voters.len() != 3
            || configuration.voters().len() != 3
            || !voters.contains(&replica.member)
            || !voters.contains(&plan.obsolete)
            || !configuration.outgoing_voters().is_empty()
            || !configuration.learners().is_empty()
            || !configuration.next_learners().is_empty()
            || configuration.auto_leave()
            || plan.replacement.member.get() <= proof.member_high_water()
            || proof
                .repair_status()
                .map_err(|_| invalid())?
                .is_some_and(|s| s.phase.active())
            || plan.replacement.node.bytes() != request.attestation.boot.node
            || plan.replacement.disk.bytes() != request.attestation.boot.disk
        {
            return Err(FleetCatalogError::Conflict);
        }
        let latest = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Admissions.slot()],
                &request.group,
            )
            .await?
            .map(|bytes| {
                FleetMembershipPermit::decode(&bytes)
                    .map_err(|_| FleetCatalogError::Corrupt("movement latest permit"))
            })
            .transpose()?;
        let status = proof.status();
        let settled = match latest.as_ref() {
            None => {
                status.consumed_sequence == 0 && status.permit.is_none() && status.phase.is_none()
            }
            Some(permit) => {
                status.permit.as_ref() == Some(permit)
                    && status.consumed_sequence == permit.sequence()
                    && status.phase.is_some_and(|phase| !phase.active())
            }
        };
        if !settled {
            return Err(FleetCatalogError::Unconfirmed);
        }
        let destination = self.host_from(snapshot, plan.replacement.node).await?;
        let mut nodes = BTreeSet::from([destination.node()]);
        let mut hosts = BTreeSet::from([destination.physical_host()]);
        let mut owners = Vec::with_capacity(3);
        for member in voters {
            let node = match proof.joined_member(member).map_err(|_| invalid())? {
                Some((joined, _)) => joined.node,
                None => NodeId::new(
                    history
                        .voters
                        .iter()
                        .find(|entry| entry.member == member.get())
                        .ok_or(FleetCatalogError::Conflict)?
                        .node,
                )
                .map_err(|_| invalid())?,
            };
            let voter = self.host_from_record(snapshot, node).await?;
            if voter.record.isolation.is_some() {
                if member != plan.obsolete {
                    return Err(FleetCatalogError::Conflict);
                }
                self.confirm_node_isolation(snapshot, &voter.record).await?;
            } else {
                self.host_from(snapshot, node).await?;
            }
            owners.push(MovementOwner::new(&voter.boot_binding()?, member)?);
            if !nodes.insert(node)
                || snapshot.replica_separation() == FleetReplicaSeparation::PhysicalHosts
                    && !hosts.insert(voter.physical_host())
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        let sequence = status
            .consumed_sequence
            .checked_add(1)
            .ok_or(FleetCatalogError::Limit)?;
        Ok(Some(MovementProof {
            owners,
            permit: FleetMembershipPermit::new(
                history.binding.clone(),
                sequence,
                ProposalId::new(request.identity.id).map_err(|_| invalid())?,
                ProposalId::new(request.identity.operation).map_err(|_| invalid())?,
                &plan,
            )
            .map_err(|_| invalid())?,
        }))
    }
}
