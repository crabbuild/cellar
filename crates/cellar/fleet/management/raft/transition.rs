// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator status and exact continuation of managed range transitions.

use super::*;
use crate::raft::{
    cell_groups::{CellGroupControllerError, CellGroupTransferActivation, TransitionPhase},
    completion::RuntimeError,
    fleet_catalog::ReopenedManagedCellGroupTransition,
};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{GroupId, IncarnationId, LogPosition},
};
use serde::{Deserialize, Serialize};

/// The plan digest and rooted progress for one managed split or merge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionStatus {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub observed_fleet_revision: u64,
    pub plan_digest: String,
    /// Absent until the accepted Fleet intent has a directory journal.
    pub phase: Option<TransitionPhase>,
    pub child_archives_verified: bool,
    pub parent_holds_released: bool,
    /// Sampled at the reported Fleet revision. Directory cleanup precedes
    /// Fleet completion and may already be visible while this is still true.
    pub fleet_slot_held: bool,
}

impl RaftFleetControl {
    /// Inspect the exact admitted operation without advancing it.
    pub async fn transition_status(
        &self,
        namespace: &str,
        operation: &str,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionStatus> {
        let directory = self.managed_namespace(namespace)?;
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let observation = self
            .catalog
            .observe_managed_transition(directory, operation, timeout)
            .await?
            .ok_or_else(missing_transition)?;
        let journal = observation.transition.journal.as_ref();
        Ok(RaftFleetTransitionStatus {
            catalog_id: self.catalog_id.clone(),
            namespace_id: namespace.into(),
            operation_id: hex::encode(operation.bytes()),
            observed_fleet_revision: observation.fleet_revision,
            plan_digest: hex::encode(observation.transition.plan.digest()),
            phase: journal.map(|journal| journal.phase()),
            child_archives_verified: journal
                .is_some_and(|journal| journal.child_archives().is_some()),
            parent_holds_released: journal.is_some_and(|journal| journal.parent_holds_released()),
            fleet_slot_held: observation.fleet_slot_held,
        })
    }
}

/// One explicitly selected terminal step. Keep it with the request ID across
/// an uncertain reply so a retry has the same meaning after progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetTransitionResumeStep {
    ConfirmChildren,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionResume {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub expected_revision: u64,
    pub plan_digest: String,
    pub step: RaftFleetTransitionResumeStep,
}

impl RaftFleetTransitionResume {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::catalog_id(&self.plan_digest)?;
        records::id(namespace)?;
        records::id(operation)?;
        records::id(&self.request_id)?;
        records::revision(self.expected_revision)?;
        if self.namespace_id != namespace || self.operation_id != operation {
            return Err(RaftFleetFailure::invalid("transition selector mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionResumeAcceptance {
    pub request: RaftFleetTransitionResume,
    /// Revision that applied this request, including on exact replay.
    pub applied_revision: u64,
    pub observed_fleet_revision: u64,
}

impl RaftFleetControl {
    /// Resume one terminal step of the exact admitted split or merge.
    pub async fn resume_transition(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionResume,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionResumeAcceptance> {
        request.validate(namespace, operation)?;
        self.check_catalog(&request.catalog_id)?;
        let directory = self.managed_namespace(namespace)?;
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let turn = Turn::new(&self.domain, timeout)?;
        let transition = self
            .authorize_transition_operator(
                directory,
                operation,
                principal,
                &request.plan_digest,
                turn.remaining()?,
            )
            .await?;
        let identity = FleetRequestIdentity {
            id: records::id(&request.request_id)?,
            operation: operation.bytes(),
            principal: principal.into(),
            expected_revision: request.expected_revision,
        };
        let snapshot = self.catalog.read(turn.remaining()?).await?;
        let commit = match request.step {
            RaftFleetTransitionResumeStep::ConfirmChildren => {
                self.catalog
                    .confirm_managed_transfer_children(
                        &snapshot,
                        &identity,
                        &transition,
                        directory,
                        &self.archive_pool,
                        turn.remaining()?,
                    )
                    .await?
            }
            RaftFleetTransitionResumeStep::Complete => {
                let parent_release = self
                    .node_observer
                    .as_ref()
                    .map(|observer| (&self.archive_pool, observer));
                self.catalog
                    .complete_managed_transfer_with_parent_release(
                        &snapshot,
                        &identity,
                        &transition,
                        directory,
                        parent_release,
                        turn.remaining()?,
                    )
                    .await?
            }
        };
        Ok(RaftFleetTransitionResumeAcceptance {
            request: request.clone(),
            applied_revision: commit.applied_revision,
            observed_fleet_revision: commit.snapshot.revision(),
        })
    }
}

/// Activate one exact child on the operator endpoint's managed voter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionActivationRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub plan_digest: String,
    pub group_id: String,
    pub incarnation_id: String,
}

impl RaftFleetTransitionActivationRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::catalog_id(&self.plan_digest)?;
        for id in [
            namespace,
            operation,
            &self.request_id,
            &self.group_id,
            &self.incarnation_id,
        ] {
            records::id(id)?;
        }
        if self.namespace_id != namespace || self.operation_id != operation {
            return Err(RaftFleetFailure::invalid("transition selector mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionActivationPosition {
    pub index: u64,
    pub term: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionActivation {
    pub request: RaftFleetTransitionActivationRequest,
    pub phase: TransitionPhase,
    /// Absent while this call has only a provisional proposal.
    pub applied: Option<RaftFleetTransitionActivationPosition>,
}

impl RaftFleetControl {
    /// Submit or reconcile an exact child activation. A configured voter can
    /// commit the operation; an already staged proof can be reconciled by any
    /// controller that can reach the Fleet catalog.
    pub async fn activate_transition_child(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionActivationRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionActivation> {
        request.validate(namespace, operation)?;
        self.check_catalog(&request.catalog_id)?;
        let directory = self.managed_namespace(namespace)?;
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let group = GroupId::new(records::id(&request.group_id)?)
            .map_err(|_| RaftFleetFailure::invalid("child group ID"))?;
        let incarnation = IncarnationId::new(records::id(&request.incarnation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("child incarnation ID"))?;
        let config = directory.namespace_config();
        let history = GroupHistory {
            cluster: config.cluster,
            namespace: config.namespace,
            group,
            incarnation,
        };
        let turn = Turn::new(&self.domain, timeout)?;
        let transition = self
            .authorize_transition_operator(
                directory,
                operation,
                principal,
                &request.plan_digest,
                turn.remaining()?,
            )
            .await?;
        let result = |phase, applied: Option<LogPosition>| RaftFleetTransitionActivation {
            request: request.clone(),
            phase,
            applied: applied.map(|position| RaftFleetTransitionActivationPosition {
                index: position.index.get(),
                term: position.term.get(),
            }),
        };
        if let Some((position, journal)) = self
            .catalog
            .resume_managed_transfer_activation(
                &transition,
                directory,
                history,
                None,
                turn.remaining()?,
            )
            .await?
        {
            return Ok(result(journal.phase(), Some(position)));
        }
        let observer = self
            .node_observer
            .as_ref()
            .ok_or_else(|| RaftFleetFailure {
                code: RaftFleetFailureCode::Unsupported,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "Use a managed endpoint with a voter for the selected child.".into(),
            })?;
        let voter = observer
            .group(history)
            .map_err(|error| activation_failure(CellGroupControllerError::Runtime(error)))?;
        match voter
            .activate_managed_transfer_child(&transition, turn.remaining()?)
            .await
            .map_err(activation_failure)?
        {
            CellGroupTransferActivation::Accepted(_) => {
                Ok(result(TransitionPhase::Published, None))
            }
            CellGroupTransferActivation::Applied(ack) => {
                let (position, journal) = self
                    .catalog
                    .resume_managed_transfer_activation(
                        &transition,
                        directory,
                        history,
                        Some(*ack),
                        turn.remaining()?,
                    )
                    .await?
                    .ok_or(FleetCatalogError::Unconfirmed)?;
                Ok(result(journal.phase(), Some(position)))
            }
        }
    }

    async fn authorize_transition_operator(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        principal: &str,
        plan_digest: &str,
        timeout: Duration,
    ) -> Result<ReopenedManagedCellGroupTransition> {
        let transition = self
            .catalog
            .reopen_managed_transition(directory, operation, timeout)
            .await?
            .ok_or_else(missing_transition)?;
        if transition.request.principal != principal {
            return Err(RaftFleetFailure {
                code: RaftFleetFailureCode::Forbidden,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "The admitted transition belongs to another principal.".into(),
            });
        }
        if plan_digest != hex::encode(transition.plan.digest()) {
            return Err(FleetCatalogError::Conflict.into());
        }
        Ok(transition)
    }
}

fn activation_failure(error: CellGroupControllerError) -> RaftFleetFailure {
    let (code, message) = match error {
        CellGroupControllerError::Runtime(RuntimeError::NotFound) => (
            RaftFleetFailureCode::Blocked,
            "The endpoint has no local voter for the selected child.",
        ),
        CellGroupControllerError::Runtime(RuntimeError::NotLeader | RuntimeError::NotReady) => (
            RaftFleetFailureCode::Unavailable,
            "Use the current ready voter leader or retry the exact request.",
        ),
        CellGroupControllerError::Runtime(RuntimeError::Overloaded) => (
            RaftFleetFailureCode::Busy,
            "The voter is busy. Retry the exact request.",
        ),
        _ => (
            RaftFleetFailureCode::Unavailable,
            "The child activation could not be confirmed. Reconcile the exact request.",
        ),
    };
    RaftFleetFailure {
        code,
        outcome: RaftFleetOutcome::Unknown,
        message: message.into(),
    }
}

fn missing_transition() -> RaftFleetFailure {
    RaftFleetFailure {
        code: RaftFleetFailureCode::NotFound,
        outcome: RaftFleetOutcome::Unknown,
        message: "The operation is absent at the observed Fleet revision.".into(),
    }
}
