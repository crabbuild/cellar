// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only operator observations of managed range transitions.

use super::*;
use crate::raft::cell_groups::TransitionPhase;
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
            .catalog
            .reopen_managed_transition(directory, operation, turn.remaining()?)
            .await?
            .ok_or_else(missing_transition)?;
        if transition.request.principal != principal {
            return Err(RaftFleetFailure {
                code: RaftFleetFailureCode::Forbidden,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "The admitted transition belongs to another principal.".into(),
            });
        }
        if request.plan_digest != hex::encode(transition.plan.digest()) {
            return Err(FleetCatalogError::Conflict.into());
        }
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

fn missing_transition() -> RaftFleetFailure {
    RaftFleetFailure {
        code: RaftFleetFailureCode::NotFound,
        outcome: RaftFleetOutcome::Unknown,
        message: "The operation is absent at the observed Fleet revision.".into(),
    }
}
