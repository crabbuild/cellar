// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator dispatch to the current managed parent voter.

use super::*;
use crate::raft::{
    cell_groups::{CellGroupControllerError, CellGroupSealOutcome, TransitionPhase},
    completion::RuntimeError,
};
use serde::{Deserialize, Serialize};

/// Submit or observe the authorized seal for one exact parent history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionSourceSealRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub plan_digest: String,
    pub source: RaftFleetTransitionGroup,
}

impl RaftFleetTransitionSourceSealRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::catalog_id(&self.plan_digest)?;
        for id in [
            namespace,
            operation,
            &self.request_id,
            &self.source.group_id,
            &self.source.incarnation_id,
        ] {
            records::id(id)?;
        }
        if self.namespace_id != namespace || self.operation_id != operation {
            return Err(RaftFleetFailure::invalid("transition selector mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionSourceCut {
    pub index: u64,
    pub term: u64,
    pub authorization_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionSourceSeal {
    pub request: RaftFleetTransitionSourceSealRequest,
    /// A missing cut means the voter has admitted a proposal. The exact
    /// request must be retried to obtain its committed, durably applied cut.
    pub applied: Option<RaftFleetTransitionSourceCut>,
}

impl RaftFleetControl {
    /// Submit or reconcile a seal on a locally hosted, authorized parent.
    /// The voter checks its current boot and applied membership before it can
    /// propose. Reconcile a lost result using the same request before publication.
    pub async fn seal_transition_source(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionSourceSealRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionSourceSeal> {
        request.validate(namespace, operation)?;
        self.check_catalog(&request.catalog_id)?;
        let directory = self.managed_namespace(namespace)?;
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let history = request.source.history(directory)?;
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
        let (parents, _) = directory
            .transition_routes(&transition.plan)
            .map_err(FleetCatalogError::from)?;
        if !parents.iter().any(|route| route.history == history)
            || !transition.journal.as_ref().is_some_and(|journal| {
                matches!(
                    journal.phase(),
                    TransitionPhase::SealAuthorized
                        | TransitionPhase::Sealed
                        | TransitionPhase::Installed
                        | TransitionPhase::Archived
                )
            })
        {
            return Err(FleetCatalogError::Conflict.into());
        }
        let observer = self
            .node_observer
            .as_ref()
            .ok_or_else(|| RaftFleetFailure {
                code: RaftFleetFailureCode::Unsupported,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "Use a managed endpoint with a voter for the selected parent.".into(),
            })?;
        let voter = observer
            .group(history)
            .map_err(|error| seal_failure(CellGroupControllerError::Runtime(error)))?;
        let applied = match voter
            .seal_managed_source(&transition, turn.remaining()?)
            .await
            .map_err(seal_failure)?
        {
            CellGroupSealOutcome::Accepted(_) => None,
            CellGroupSealOutcome::Applied(seal) => Some(RaftFleetTransitionSourceCut {
                index: seal.position().index.get(),
                term: seal.position().term.get(),
                authorization_digest: hex::encode(seal.authorization()),
            }),
        };
        Ok(RaftFleetTransitionSourceSeal {
            request: request.clone(),
            applied,
        })
    }
}

fn seal_failure(error: CellGroupControllerError) -> RaftFleetFailure {
    let (code, message) = match error {
        CellGroupControllerError::Runtime(RuntimeError::NotFound) => (
            RaftFleetFailureCode::Blocked,
            "The endpoint has no local voter for the selected parent.",
        ),
        CellGroupControllerError::Runtime(RuntimeError::NotLeader | RuntimeError::NotReady) => (
            RaftFleetFailureCode::Unavailable,
            "Use the current ready parent leader or retry the exact request.",
        ),
        CellGroupControllerError::Runtime(RuntimeError::Overloaded) => (
            RaftFleetFailureCode::Busy,
            "The parent voter is busy. Retry the exact request.",
        ),
        _ => (
            RaftFleetFailureCode::Unavailable,
            "The parent seal could not be confirmed. Reconcile the exact request.",
        ),
    };
    RaftFleetFailure {
        code,
        outcome: RaftFleetOutcome::Unknown,
        message: message.into(),
    }
}
