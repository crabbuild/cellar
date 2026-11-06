// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent archive verification during a managed transfer.

use super::*;
use crate::raft::{
    cell_groups::{CellGroupArchiveCoverage, CellGroupArchiveHold, TransitionPhase},
    fleet_catalog::FleetRecoveryVoterLineage,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionArchiveVerification {
    pub request: RaftFleetTransitionPrepareRequest,
    pub archive_digest: String,
    pub phase: TransitionPhase,
}

/// Private replay state bound to a single observed Fleet and directory root.
/// Retaining this value keeps the local replay pins alive. The remote holds
/// still belong to the operator that acquired them.
pub struct RaftFleetVerifiedTransitionRecovery {
    basis: RaftFleetTransitionRecoveryBasis,
    archives: Vec<CellGroupArchiveCoverage>,
    lineages: Vec<FleetRecoveryVoterLineage>,
}

impl RaftFleetVerifiedTransitionRecovery {
    pub fn basis(&self) -> &RaftFleetTransitionRecoveryBasis {
        &self.basis
    }

    pub fn archives(&self) -> &[CellGroupArchiveCoverage] {
        &self.archives
    }

    /// The fence must cover these Fleet and archive lineages, along with the
    /// platform's complete inventory of possible old writers.
    pub fn required_voter_lineages(&self) -> &[FleetRecoveryVoterLineage] {
        &self.lineages
    }
}

impl RaftFleetControl {
    /// Replay the caller's retained hold for every selected range, in
    /// `basis.archive_sources()` order. Every hold must still be the selected
    /// archive head. This read does not lock topology or fence old voters.
    /// An eventual fencing operation must revalidate these exact inputs.
    pub async fn verify_transition_recovery_archives(
        &self,
        principal: &str,
        basis: &RaftFleetTransitionRecoveryBasis,
        holds: Vec<CellGroupArchiveHold>,
        timeout: Duration,
    ) -> Result<RaftFleetVerifiedTransitionRecovery> {
        self.check_catalog(&basis.catalog_id)?;
        let directory = self.managed_namespace(&basis.namespace_id)?;
        let operation = ProposalId::new(records::id(&basis.operation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let turn = Turn::new(&self.domain, timeout)?;
        let transition = self
            .authorize_transition_operator(
                directory,
                operation,
                principal,
                &basis.plan_digest,
                turn.remaining()?,
            )
            .await?;
        let observation = self
            .catalog
            .observe_managed_transition_recovery(directory, operation, turn.remaining()?)
            .await?;
        let current = RaftFleetTransitionRecoveryBasis::from_observation(
            self.catalog_id(),
            &basis.namespace_id,
            operation,
            &observation,
        );
        if current != *basis {
            return Err(FleetCatalogError::Conflict.into());
        }
        let services = self.transition_preparation.as_ref().ok_or_else(|| RaftFleetFailure {
            code: RaftFleetFailureCode::Unsupported,
            outcome: RaftFleetOutcome::NotSubmitted,
            message: "Recovery archive verification requires a trusted verifier on the management server.".into(),
        })?;
        let verified = self
            .catalog
            .verify_managed_transition_recovery_archives(
                directory,
                &observation,
                &transition.request,
                holds,
                &services.verifier,
                turn.remaining()?,
            )
            .await?;
        Ok(RaftFleetVerifiedTransitionRecovery {
            basis: current,
            archives: verified.archives,
            lineages: verified.lineages,
        })
    }

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

    /// Select independently replayed archive coverage for the active children.
    /// Repeating the request can return the historical selected digest.
    pub async fn verify_transition_child_archives(
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
        let journal = transition
            .journal
            .as_ref()
            .ok_or(FleetCatalogError::Conflict)?;
        let (digest, phase) = if let Some(digest) = journal.child_archives() {
            (digest, journal.phase())
        } else {
            if journal.phase() != TransitionPhase::Active {
                return Err(FleetCatalogError::Conflict.into());
            }
            let services = self.transition_preparation.as_ref().ok_or_else(|| RaftFleetFailure {
                code: RaftFleetFailureCode::Unsupported,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "Child archive verification requires a trusted verifier on the management server.".into(),
            })?;
            let verified = self
                .catalog
                .verify_managed_transfer_child_archives(
                    &transition,
                    directory,
                    &services.verifier,
                    turn.remaining()?,
                )
                .await?;
            (
                verified
                    .child_archives()
                    .ok_or(FleetCatalogError::Unconfirmed)?,
                verified.phase(),
            )
        };
        Ok(RaftFleetTransitionArchiveVerification {
            request: request.clone(),
            archive_digest: hex::encode(digest),
            phase,
        })
    }
}
