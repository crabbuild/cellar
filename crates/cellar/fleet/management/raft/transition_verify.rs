// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent archive verification for an installed managed transfer.

use super::*;
use crate::raft::cell_groups::TransitionPhase;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionArchiveVerification {
    pub request: RaftFleetTransitionPrepareRequest,
    pub archive_digest: String,
    pub phase: TransitionPhase,
}

impl RaftFleetControl {
    /// Rebuild the installed children from the retained sealed parent cuts,
    /// then select the verified transfer archive. A retry can reopen the
    /// selected archive after the directory has advanced.
    pub async fn verify_transition_archive(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionPrepareRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionArchiveVerification> {
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
        let journal = transition.journal.ok_or(FleetCatalogError::Conflict)?;
        let journal =
            if journal.phase() == TransitionPhase::Installed {
                let services = self.transition_preparation.as_ref().ok_or_else(|| {
                    RaftFleetFailure {
                    code: RaftFleetFailureCode::Unsupported,
                    outcome: RaftFleetOutcome::NotSubmitted,
                    message:
                        "Archive verification requires a trusted verifier on the management server."
                            .into(),
                }
                })?;
                self.catalog
                    .resume_managed_transfer_archive(
                        directory,
                        operation,
                        &services.verifier,
                        services.recovery.clone(),
                        turn.remaining()?,
                    )
                    .await?
            } else {
                journal
            };
        let digest = journal
            .transfer_archive()
            .ok_or(FleetCatalogError::Conflict)?;
        Ok(RaftFleetTransitionArchiveVerification {
            request: request.clone(),
            archive_digest: hex::encode(digest),
            phase: journal.phase(),
        })
    }
}
