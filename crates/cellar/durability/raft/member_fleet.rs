//! Configuration-owned permit consumption. Catalog observations select work;
//! only ordered, committed changes can consume or fence a permit sequence.
use super::*;
use ::raft::prelude::ConfChangeSingle;
use cellar_logic::consensus::{
    ClusterId, GroupId, GroupScope, IncarnationId, NamespaceId, ProposalId,
};
use sha2::{Digest, Sha256};

#[cfg(test)]
#[path = "member_fleet_tests.rs"]
mod tests;

/// Exact fleet and cell-history identity. Encoding this descriptor does not
/// enroll a group; enrollment requires a rooted catalog authorization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetGroupBinding {
    fleet: [u8; 16],
    cluster: [u8; 16],
    namespace: [u8; 16],
    cell: String,
    group: [u8; 16],
    incarnation: [u8; 16],
}

impl FleetGroupBinding {
    pub fn new(fleet: IncarnationId, scope: &GroupScope) -> Result<Self, LogError> {
        scope.validate().map_err(|_| invalid())?;
        Ok(Self {
            fleet: fleet.bytes(),
            cluster: scope.cluster.bytes(),
            namespace: scope.namespace.bytes(),
            cell: scope.cell.clone(),
            group: scope.group.bytes(),
            incarnation: scope.incarnation.bytes(),
        })
    }

    pub fn fleet(&self) -> [u8; 16] {
        self.fleet
    }

    pub fn scope(&self) -> Result<GroupScope, LogError> {
        self.validate()?;
        Ok(GroupScope {
            cluster: ClusterId::new(self.cluster).map_err(|_| invalid())?,
            namespace: NamespaceId::new(self.namespace).map_err(|_| invalid())?,
            cell: self.cell.clone(),
            group: GroupId::new(self.group).map_err(|_| invalid())?,
            incarnation: IncarnationId::new(self.incarnation).map_err(|_| invalid())?,
        })
    }

    pub fn key(&self) -> Result<String, LogError> {
        self.validate()?;
        let bytes = serde_json::to_vec(&(
            self.cluster,
            self.namespace,
            &self.cell,
            self.group,
            self.incarnation,
        ))
        .map_err(|_| invalid())?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }

    pub fn encode(&self) -> Result<Vec<u8>, LogError> {
        self.validate()?;
        encode_record(45, self)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, LogError> {
        let binding: Self = decode_record(45, bytes)?;
        binding.validate()?;
        Ok(binding)
    }

    fn validate(&self) -> Result<(), LogError> {
        IncarnationId::new(self.fleet).map_err(|_| invalid())?;
        ClusterId::new(self.cluster).map_err(|_| invalid())?;
        NamespaceId::new(self.namespace).map_err(|_| invalid())?;
        GroupId::new(self.group).map_err(|_| invalid())?;
        IncarnationId::new(self.incarnation).map_err(|_| invalid())?;
        if !cellar_logic::cell::valid_cell_scope(&self.cell) {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn matches_scope(&self, scope: &GroupScope) -> bool {
        self.cluster == scope.cluster.bytes()
            && self.namespace == scope.namespace.bytes()
            && self.cell == scope.cell
            && self.group == scope.group.bytes()
            && self.incarnation == scope.incarnation.bytes()
    }
}

/// A descriptor selected by the fleet catalog. Constructing this value grants
/// no authority; host mutation APIs require the catalog's opaque authorization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetMembershipPermit {
    binding: FleetGroupBinding,
    sequence: u64,
    id: [u8; 16],
    operation: [u8; 16],
    plan_id: [u8; 16],
    expected_revision: u64,
    plan_digest: [u8; 32],
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
}

impl FleetMembershipPermit {
    pub fn new(
        binding: FleetGroupBinding,
        sequence: u64,
        id: ProposalId,
        operation: ProposalId,
        plan: &RepairPlan,
    ) -> Result<Self, LogError> {
        if plan.policy_revision == 0 {
            return Err(invalid());
        }
        let permit = Self {
            binding,
            sequence,
            id: id.bytes(),
            operation: operation.bytes(),
            plan_id: plan.id.bytes(),
            expected_revision: plan.expected_revision.get(),
            plan_digest: plan_digest(plan),
            member: plan.replacement.member.get(),
            node: plan.replacement.node.bytes(),
            disk: plan.replacement.disk.bytes(),
        };
        permit.validate()?;
        Ok(permit)
    }

    pub fn binding(&self) -> &FleetGroupBinding {
        &self.binding
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn id(&self) -> [u8; 16] {
        self.id
    }

    pub fn operation(&self) -> [u8; 16] {
        self.operation
    }

    pub fn expected_revision(&self) -> LogIndex {
        LogIndex::new(self.expected_revision)
    }

    pub(crate) fn repair_id(&self) -> Result<ProposalId, LogError> {
        ProposalId::new(self.plan_id).map_err(|_| invalid())
    }

    pub fn replacement(&self) -> Result<JoiningMember, LogError> {
        Ok(JoiningMember {
            member: MemberId::new(self.member).map_err(|_| invalid())?,
            node: NodeId::new(self.node).map_err(|_| invalid())?,
            disk: DiskLineage::new(self.disk).map_err(|_| invalid())?,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, LogError> {
        self.validate()?;
        encode_record(46, self)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, LogError> {
        let permit: Self = decode_record(46, bytes)?;
        permit.validate()?;
        Ok(permit)
    }

    pub(crate) fn check_plan(&self, plan: &RepairPlan) -> Result<(), LogError> {
        self.validate()?;
        if self.plan_id != plan.id.bytes()
            || self.expected_revision != plan.expected_revision.get()
            || self.plan_digest != plan_digest(plan)
            || self.replacement()? != plan.replacement
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), LogError> {
        self.binding.validate()?;
        ProposalId::new(self.id).map_err(|_| invalid())?;
        ProposalId::new(self.operation).map_err(|_| invalid())?;
        ProposalId::new(self.plan_id).map_err(|_| invalid())?;
        self.replacement()?;
        if self.sequence == 0
            || self.sequence >= u64::MAX - 2
            || self.expected_revision == 0
            || self.expected_revision >= u64::MAX - 2
            || self.plan_digest == [0; 32]
        {
            return Err(invalid());
        }
        Ok(())
    }
}

fn plan_digest(plan: &RepairPlan) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"cellar-fleet-repair-1");
    digest.update(plan.id.bytes());
    digest.update(plan.policy_revision.to_be_bytes());
    digest.update(plan.expected_revision.get().to_be_bytes());
    digest.update(plan.obsolete.get().to_be_bytes());
    digest.update(plan.replacement.member.get().to_be_bytes());
    digest.update(plan.replacement.node.bytes());
    digest.update(plan.replacement.disk.bytes());
    digest.finalize().into()
}

fn encode_record(kind: u8, value: &impl Serialize) -> Result<Vec<u8>, LogError> {
    crate::raft::catalog_wire::encode(kind, value, 4096).map_err(|_| invalid())
}

fn decode_record<T: for<'de> Deserialize<'de> + Serialize>(
    kind: u8,
    bytes: &[u8],
) -> Result<T, LogError> {
    crate::raft::catalog_wire::decode(kind, bytes, 4096).map_err(|_| invalid())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FleetPermitPhase {
    Seeding,
    Joint,
    Complete,
    Aborted,
    Fenced,
}

impl FleetPermitPhase {
    pub fn active(self) -> bool {
        matches!(self, Self::Seeding | Self::Joint)
    }
}

/// Synced local membership observation. It is not a fresh quorum barrier or
/// permission to release an outstanding catalog reservation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetMembershipStatus {
    pub binding: FleetGroupBinding,
    pub consumed_sequence: u64,
    pub permit: Option<FleetMembershipPermit>,
    pub phase: Option<FleetPermitPhase>,
    pub changed_at: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FleetState {
    binding: FleetGroupBinding,
    enrolled_at: u64,
    sequence: u64,
    #[serde(deserialize_with = "required_nullable")]
    last: Option<Consumed>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Consumed {
    permit: FleetMembershipPermit,
    phase: FleetPermitPhase,
    changed_at: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub(super) enum FleetIntent {
    Enroll { binding: FleetGroupBinding },
    Start { permit: FleetMembershipPermit },
    Continue { permit: FleetMembershipPermit },
    Fence { permit: FleetMembershipPermit },
}

fn same_configuration_change(change: &ConfChangeV2, conf: &ConfState) -> Result<(), LogError> {
    if !conf.voters_outgoing.is_empty()
        || !conf.learners_next.is_empty()
        || conf.auto_leave
        || conf.voters.len() != 3
        || change.get_transition() != ::raft::prelude::ConfChangeTransition::Auto
    {
        return Err(invalid());
    }
    let voter = conf.voters.iter().copied().min().ok_or_else(invalid)?;
    if change.changes.as_ref()
        != [ConfChangeSingle {
            node_id: voter,
            change_type: ConfChangeType::AddNode,
            ..Default::default()
        }]
    {
        return Err(invalid());
    }
    Ok(())
}

fn bind_same_configuration(change: &mut ConfChangeV2, conf: &ConfState) -> Result<(), LogError> {
    let voter = conf.voters.iter().copied().min().ok_or_else(invalid)?;
    *change = ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            node_id: voter,
            change_type: ConfChangeType::AddNode,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    same_configuration_change(change, conf)
}

impl GroupMembership {
    pub fn fleet_status(&self) -> Option<FleetMembershipStatus> {
        self.fleet.as_ref().map(|state| FleetMembershipStatus {
            binding: state.binding.clone(),
            consumed_sequence: state.sequence,
            permit: state.last.as_ref().map(|last| last.permit.clone()),
            phase: state.last.as_ref().map(|last| last.phase),
            changed_at: LogIndex::new(
                state
                    .last
                    .as_ref()
                    .map_or(state.enrolled_at, |last| last.changed_at),
            ),
        })
    }

    pub fn bind_fleet_enrollment(
        &self,
        change: &mut ConfChangeV2,
        conf: &ConfState,
        binding: &FleetGroupBinding,
    ) -> Result<(), LogError> {
        bind_same_configuration(change, conf)?;
        self.bind_intent(
            change,
            &[],
            None,
            Some(FleetIntent::Enroll {
                binding: binding.clone(),
            }),
        )?;
        self.admit(change, conf)
    }

    pub fn bind_fleet_fence(
        &self,
        change: &mut ConfChangeV2,
        conf: &ConfState,
        permit: &FleetMembershipPermit,
    ) -> Result<(), LogError> {
        bind_same_configuration(change, conf)?;
        self.bind_intent(
            change,
            &[],
            None,
            Some(FleetIntent::Fence {
                permit: permit.clone(),
            }),
        )?;
        self.admit(change, conf)
    }

    pub fn bind_managed_repair_start(
        &self,
        change: &mut ConfChangeV2,
        conf: &ConfState,
        plan: &RepairPlan,
        permit: &FleetMembershipPermit,
    ) -> Result<(), LogError> {
        permit.check_plan(plan)?;
        self.bind_repair_start_with(
            change,
            conf,
            plan,
            Some(FleetIntent::Start {
                permit: permit.clone(),
            }),
        )
    }

    pub fn bind_managed_repair_step(
        &self,
        change: &mut ConfChangeV2,
        conf: &ConfState,
        id: ProposalId,
        step: RepairStep,
        permit: &FleetMembershipPermit,
    ) -> Result<(), LogError> {
        self.bind_repair_step_with(
            change,
            conf,
            id,
            step,
            Some(FleetIntent::Continue {
                permit: permit.clone(),
            }),
        )
    }

    pub(crate) fn validate_scope(&self, scope: &GroupScope) -> Result<(), LogError> {
        if self
            .fleet
            .as_ref()
            .is_some_and(|state| !state.binding.matches_scope(scope))
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(crate) fn admit_scoped(
        &self,
        change: &ConfChangeV2,
        conf: &ConfState,
        scope: &GroupScope,
    ) -> Result<(), LogError> {
        self.validate_scope(scope)?;
        let binding = match self.intent(change)?.fleet {
            Some(FleetIntent::Enroll { binding }) => Some(binding),
            Some(
                FleetIntent::Start { permit }
                | FleetIntent::Continue { permit }
                | FleetIntent::Fence { permit },
            ) => Some(permit.binding),
            None => None,
        };
        if binding.is_some_and(|binding| !binding.matches_scope(scope)) {
            return Err(invalid());
        }
        self.admit(change, conf)
    }

    pub(super) fn validate_fleet_change(
        &self,
        intent: &Intent,
        change: &ConfChangeV2,
        conf: &ConfState,
    ) -> Result<(), LogError> {
        let active = self
            .repair_status()?
            .is_some_and(|status| status.phase.active());
        match (&self.fleet, &intent.fleet) {
            (None, None) => Ok(()),
            (None, Some(FleetIntent::Enroll { binding })) => {
                binding.validate()?;
                same_configuration_change(change, conf)?;
                if active
                    || !intent.joins.is_empty()
                    || intent.repair.is_some()
                    || !conf.learners.is_empty()
                {
                    return Err(invalid());
                }
                Ok(())
            }
            (Some(state), Some(action)) => {
                let (permit, continuing) = match action {
                    FleetIntent::Start { permit } | FleetIntent::Fence { permit } => {
                        (permit, false)
                    }
                    FleetIntent::Continue { permit } => (permit, true),
                    _ => return Err(invalid()),
                };
                permit.validate()?;
                if permit.binding != state.binding {
                    return Err(invalid());
                }
                if continuing {
                    if !state
                        .last
                        .as_ref()
                        .is_some_and(|last| last.permit == *permit && last.phase.active())
                    {
                        return Err(invalid());
                    }
                    let status = self.repair_status()?.ok_or_else(invalid)?;
                    permit.check_plan(&status.plan)?;
                    if !matches!(
                        intent.repair,
                        Some(
                            RepairIntent::Promote { .. }
                                | RepairIntent::Finish { .. }
                                | RepairIntent::Abort { .. }
                        )
                    ) {
                        return Err(invalid());
                    }
                } else {
                    if active
                        || state.sequence.checked_add(1) != Some(permit.sequence)
                        || permit.expected_revision != self.revision
                    {
                        return Err(invalid());
                    }
                    match action {
                        FleetIntent::Start { .. } => {
                            let Some(RepairIntent::Start { plan }) = &intent.repair else {
                                return Err(invalid());
                            };
                            permit.check_plan(&plan.public()?)?;
                        }
                        FleetIntent::Fence { .. } => {
                            same_configuration_change(change, conf)?;
                            if intent.repair.is_some() || !intent.joins.is_empty() {
                                return Err(invalid());
                            }
                        }
                        _ => return Err(invalid()),
                    }
                }
                Ok(())
            }
            _ => Err(invalid()),
        }
    }

    pub(super) fn apply_fleet(&mut self, intent: &Intent, index: u64) -> Result<(), LogError> {
        let Some(action) = &intent.fleet else {
            return Ok(());
        };
        if let FleetIntent::Enroll { binding } = action {
            self.fleet = Some(Box::new(FleetState {
                binding: binding.clone(),
                enrolled_at: index,
                sequence: 0,
                last: None,
            }));
            return Ok(());
        }
        let (permit, phase) = match action {
            FleetIntent::Fence { permit } => (permit, FleetPermitPhase::Fenced),
            FleetIntent::Start { permit } | FleetIntent::Continue { permit } => {
                let status = self.repair_status()?.ok_or_else(invalid)?;
                let phase = match status.phase {
                    RepairPhase::Seeding => FleetPermitPhase::Seeding,
                    RepairPhase::Joint => FleetPermitPhase::Joint,
                    RepairPhase::Complete => FleetPermitPhase::Complete,
                    RepairPhase::Aborted => FleetPermitPhase::Aborted,
                };
                (permit, phase)
            }
            _ => return Err(invalid()),
        };
        let state = self.fleet.as_mut().ok_or_else(invalid)?;
        state.sequence = permit.sequence;
        state.last = Some(Consumed {
            permit: permit.clone(),
            phase,
            changed_at: index,
        });
        Ok(())
    }

    pub(super) fn validate_fleet(&self) -> Result<(), LogError> {
        let Some(state) = &self.fleet else {
            return Ok(());
        };
        state.binding.validate()?;
        if state.enrolled_at <= 1
            || state.enrolled_at > self.revision
            || state.sequence >= u64::MAX - 2
        {
            return Err(invalid());
        }
        let Some(last) = &state.last else {
            return if state.sequence == 0
                && !self
                    .repair_status()?
                    .is_some_and(|status| status.phase.active())
            {
                Ok(())
            } else {
                Err(invalid())
            };
        };
        last.permit.validate()?;
        if last.permit.sequence != state.sequence
            || last.permit.binding != state.binding
            || last.changed_at <= state.enrolled_at
            || last.changed_at > self.revision
            || last.permit.expected_revision < state.enrolled_at
            || last.permit.expected_revision >= last.changed_at
        {
            return Err(invalid());
        }
        if last.phase != FleetPermitPhase::Fenced {
            let status = self.repair_status()?.ok_or_else(invalid)?;
            last.permit.check_plan(&status.plan)?;
            let expected = match status.phase {
                RepairPhase::Seeding => FleetPermitPhase::Seeding,
                RepairPhase::Joint => FleetPermitPhase::Joint,
                RepairPhase::Complete => FleetPermitPhase::Complete,
                RepairPhase::Aborted => FleetPermitPhase::Aborted,
            };
            if last.phase != expected || last.changed_at != status.changed_at.get() {
                return Err(invalid());
            }
        } else if self
            .repair_status()?
            .is_some_and(|status| status.phase.active())
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(super) fn fleet_succeeds(&self, old: &Self) -> Result<(), LogError> {
        match (&self.fleet, &old.fleet) {
            (None, Some(_)) => Err(invalid()),
            (Some(next), None) if next.enrolled_at <= old.revision => Err(invalid()),
            (Some(next), Some(prior)) => {
                if next.binding != prior.binding
                    || next.enrolled_at != prior.enrolled_at
                    || next.sequence < prior.sequence
                {
                    return Err(invalid());
                }
                if next.sequence == prior.sequence {
                    match (&next.last, &prior.last) {
                        (Some(next), Some(old)) if next.permit == old.permit => {
                            if next.phase == old.phase {
                                if next != old {
                                    return Err(invalid());
                                }
                            } else if next.changed_at <= old.changed_at
                                || !matches!(
                                    (old.phase, next.phase),
                                    (
                                        FleetPermitPhase::Seeding,
                                        FleetPermitPhase::Joint
                                            | FleetPermitPhase::Aborted
                                            | FleetPermitPhase::Complete
                                    ) | (FleetPermitPhase::Joint, FleetPermitPhase::Complete)
                                )
                            {
                                return Err(invalid());
                            }
                        }
                        (None, None) => (),
                        _ => return Err(invalid()),
                    }
                } else if !next.last.as_ref().is_some_and(|last| {
                    last.changed_at > old.revision && last.permit.expected_revision >= old.revision
                }) {
                    return Err(invalid());
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
