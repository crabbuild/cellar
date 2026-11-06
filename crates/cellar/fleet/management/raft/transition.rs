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
            .ok_or_else(|| RaftFleetFailure {
                code: RaftFleetFailureCode::NotFound,
                outcome: RaftFleetOutcome::Unknown,
                message: "The operation is absent at the observed Fleet revision.".into(),
            })?;
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
