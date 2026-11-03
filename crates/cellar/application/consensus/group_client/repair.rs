// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Whole-group repair controls use exact permits and fresh post-apply quorum.
use super::membership::{CellGroupControlError, Change as MembershipChange};
use super::*;
use crate::raft::{
    completion::RuntimeError,
    fleet_catalog::{FleetGroupAuthorization, FleetPermitAuthorization},
    member_state::{FleetPermitPhase, RepairPhase, RepairPlan, RepairStep},
    transport::{RaftTransport, TransportError},
};
use cellar_logic::consensus::{CommandFailure, LogIndex};

pub(super) enum Change {
    Start {
        group: FleetGroupAuthorization,
        authorization: FleetPermitAuthorization,
        plan: RepairPlan,
    },
    Advance {
        group: FleetGroupAuthorization,
        authorization: FleetPermitAuthorization,
        step: RepairStep,
        expected: LogIndex,
    },
}

impl Change {
    fn authority(&self) -> (&FleetGroupAuthorization, &FleetPermitAuthorization) {
        match self {
            Self::Start {
                group,
                authorization,
                ..
            }
            | Self::Advance {
                group,
                authorization,
                ..
            } => (group, authorization),
        }
    }

    pub(super) fn satisfied(
        &self,
        observed: &CellGroupQuorumMembership,
    ) -> Result<bool, RuntimeError> {
        let (group, authorization) = self.authority();
        let permit = authorization.permit();
        if group.binding() != permit.binding()
            || group.history_digests()
                != (
                    observed.root().route().birth_digest,
                    observed.root().route().schema_digest,
                )
            || !group.matches_replica(observed.replica(), observed.membership())
        {
            return Err(RuntimeError::Invalid(
                "managed group repair authority".into(),
            ));
        }
        let fleet = observed
            .membership()
            .fleet_status()
            .ok_or_else(|| RuntimeError::Invalid("group is not enrolled".into()))?;
        if &fleet.binding != permit.binding() {
            return Err(RuntimeError::Invalid("foreign repair permit".into()));
        }
        if let Self::Start { plan, .. } = self {
            permit.check_plan(plan)?;
        }
        if fleet.consumed_sequence < permit.sequence() {
            return match self {
                Self::Start { .. } => Ok(false),
                Self::Advance { .. } => Err(RuntimeError::StaleCompletion),
            };
        }
        if fleet.consumed_sequence != permit.sequence()
            || fleet.permit.as_ref() != Some(permit)
            || fleet.phase == Some(FleetPermitPhase::Fenced)
        {
            return Err(RuntimeError::StaleCompletion);
        }
        let repair = observed
            .membership()
            .repair_status()?
            .ok_or(RuntimeError::StaleCompletion)?;
        permit.check_plan(&repair.plan)?;
        let wanted = match self {
            Self::Start { plan, .. } => {
                if &repair.plan != plan {
                    return Err(RuntimeError::StaleCompletion);
                }
                RepairPhase::Seeding
            }
            Self::Advance { step, .. } => match step {
                RepairStep::PromoteAndRemove => RepairPhase::Joint,
                RepairStep::FinishJoint => RepairPhase::Complete,
                RepairStep::AbortLearner => RepairPhase::Aborted,
            },
        };
        if repair.phase == RepairPhase::Aborted && wanted != RepairPhase::Aborted
            || wanted == RepairPhase::Aborted
                && repair.phase != RepairPhase::Seeding
                && repair.phase != RepairPhase::Aborted
        {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(repair.phase == wanted
            || wanted == RepairPhase::Seeding
            || wanted == RepairPhase::Joint && repair.phase == RepairPhase::Complete)
    }

    pub(super) fn prepare(
        &self,
        observed: &CellGroupQuorumMembership,
    ) -> Result<CellGroupMembershipChange, RuntimeError> {
        let (group, authorization) = self.authority();
        if !group.matches_scope(&observed.replica().scope)
            || !authorization.matches_scope(&observed.replica().scope)
        {
            return Err(RuntimeError::Invalid("group repair history".into()));
        }
        Ok(match self {
            Self::Start { plan, .. } => CellGroupMembershipChange::managed_repair_start(
                observed.root().route(),
                observed.membership(),
                observed.configuration(),
                plan,
                authorization,
            )?,
            Self::Advance { step, expected, .. } => {
                if observed.membership().revision() != *expected {
                    return Err(RuntimeError::StaleCompletion);
                }
                CellGroupMembershipChange::managed_repair_step(
                    observed.root().route(),
                    observed.membership(),
                    observed.configuration(),
                    authorization.permit().repair_id()?,
                    *step,
                    authorization,
                )?
            }
        })
    }

    pub(super) fn check_placement(
        &self,
        transport: &RaftTransport,
        observed: &CellGroupQuorumMembership,
    ) -> Result<(), RuntimeError> {
        if let Self::Start {
            group,
            authorization,
            plan,
        } = self
        {
            let nodes = group.placement_nodes(observed)?;
            transport
                .check_fleet_placement(
                    &observed.replica().scope,
                    &nodes,
                    &plan.replacement,
                    authorization,
                )
                .map_err(|error| match error {
                    TransportError::Overloaded => RuntimeError::Overloaded,
                    TransportError::StalePeer | TransportError::Refused => RuntimeError::NotReady,
                    TransportError::Runtime(error) => error,
                    TransportError::Closed => RuntimeError::Closed,
                    _ => RuntimeError::Invalid("managed repair placement".into()),
                })?;
        }
        Ok(())
    }
}

fn admission_error(error: CommandFailure) -> RuntimeError {
    match error {
        CommandFailure::Overloaded => RuntimeError::Overloaded,
        CommandFailure::VersionUnavailable => RuntimeError::DeadlineExceeded,
        CommandFailure::UnsupportedCapability => {
            RuntimeError::Invalid("group repair callback context".into())
        }
        _ => RuntimeError::Closed,
    }
}

impl RaftGroupHandle {
    /// Start the exact catalog-selected learner repair. Admission is followed by
    /// actual durable apply and a fresh quorum observation of the same permit.
    pub fn begin_managed_repair(
        &self,
        group: FleetGroupAuthorization,
        authorization: FleetPermitAuthorization,
        plan: RepairPlan,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, CellGroupControlError>> + Send + 'static
    {
        self.repair_change(
            Change::Start {
                group,
                authorization,
                plan,
            },
            timeout,
        )
    }

    /// Advance or abort the consumed plan at this observed revision. Promotion
    /// still requires actual RawNode progress; a route or seed receipt is insufficient.
    pub fn advance_managed_repair(
        &self,
        group: FleetGroupAuthorization,
        authorization: FleetPermitAuthorization,
        step: RepairStep,
        expected: LogIndex,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, CellGroupControlError>> + Send + 'static
    {
        self.repair_change(
            Change::Advance {
                group,
                authorization,
                step,
                expected,
            },
            timeout,
        )
    }

    fn repair_change(
        &self,
        change: Change,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, CellGroupControlError>> + Send + 'static
    {
        let admitted = self
            .owner
            .metadata_admission(timeout, 64 * 1024)
            .map_err(admission_error);
        let owner = self.clone();
        async move {
            let (deadline, _memory) = admitted.map_err(CellGroupControlError::Refused)?;
            let (group, authorization) = change.authority();
            if !group.matches_scope(&owner.history) || !authorization.matches_scope(&owner.history)
            {
                return Err(CellGroupControlError::Refused(RuntimeError::Invalid(
                    "group repair history".into(),
                )));
            }
            let result = owner
                .change_membership(MembershipChange::Repair(change), deadline)
                .await?;
            if owner.owner.is_closed() {
                return Err(CellGroupControlError::Unconfirmed(RuntimeError::Closed));
            }
            if owner
                .owner
                .handle
                .domain
                .mono_ms()
                .map_err(|_| CellGroupControlError::Unconfirmed(RuntimeError::Closed))?
                >= deadline
            {
                return Err(CellGroupControlError::Unconfirmed(
                    RuntimeError::DeadlineExceeded,
                ));
            }
            Ok(result)
        }
    }
}
