// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator setup for a pending managed transfer child on one reserved voter.

use super::*;
use crate::raft::{
    catalog::CatalogError,
    cell_groups::{CellGroupControllerError, TransitionPhase},
    completion::RuntimeError,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetTransitionChildStep {
    Install,
    Attach,
    Quorum,
    InitializeArchive,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionChildSetupRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub plan_digest: String,
    pub child: RaftFleetTransitionGroup,
    pub step: RaftFleetTransitionChildStep,
}

impl RaftFleetTransitionChildSetupRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::catalog_id(&self.plan_digest)?;
        for id in [
            namespace,
            operation,
            &self.request_id,
            &self.child.group_id,
            &self.child.incarnation_id,
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
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
pub enum RaftFleetTransitionChildSetupResult {
    Installed {
        member: u64,
        node_id: String,
        disk_lineage: String,
        storage_instance: String,
    },
    Attached,
    ArchiveInitialized,
    Quorum {
        index: u64,
        term: u64,
        phase: TransitionPhase,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionChildSetup {
    pub request: RaftFleetTransitionChildSetupRequest,
    /// The exact local setup result or a selected historical quorum witness.
    pub result: RaftFleetTransitionChildSetupResult,
}

impl RaftFleetControl {
    /// Set up the exact pending child on a reserved local voter. Install
    /// independently replays the selected sealed archives. Attach requires
    /// the local receipt and a durable install majority. Quorum selects a
    /// fresh-term witness on the child leader or reopens its selected proof.
    /// InitializeArchive binds the activated child's first archive identity.
    pub async fn setup_transition_child(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionChildSetupRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionChildSetup> {
        request.validate(namespace, operation)?;
        self.check_catalog(&request.catalog_id)?;
        let directory = self.managed_namespace(namespace)?;
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let history = request.child.history(directory)?;
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
        let phase = transition.journal.as_ref().map(|journal| journal.phase());
        match (request.step, phase) {
            (RaftFleetTransitionChildStep::Install, Some(TransitionPhase::SealAuthorized))
            | (
                RaftFleetTransitionChildStep::InitializeArchive,
                Some(TransitionPhase::Active | TransitionPhase::Retired),
            )
            | (
                RaftFleetTransitionChildStep::Install
                | RaftFleetTransitionChildStep::Attach
                | RaftFleetTransitionChildStep::Quorum,
                Some(
                    TransitionPhase::Sealed
                    | TransitionPhase::Installed
                    | TransitionPhase::Archived,
                ),
            ) => {}
            _ => return Err(FleetCatalogError::Conflict.into()),
        }
        let (_, children) = directory
            .transition_routes(&transition.plan)
            .map_err(FleetCatalogError::from)?;
        let child = children
            .into_iter()
            .find(|route| route.history == history)
            .ok_or(FleetCatalogError::Conflict)?;
        let transition = Arc::new(transition);
        let result = match request.step {
            RaftFleetTransitionChildStep::Install => {
                let observer = self.child_setup_voter()?;
                let services = self.transition_preparation.as_ref().ok_or_else(|| {
                    RaftFleetFailure {
                        code: RaftFleetFailureCode::Unsupported,
                        outcome: RaftFleetOutcome::NotSubmitted,
                        message: "Child installation requires a trusted archive verifier on the management server.".into(),
                    }
                })?;
                let sources = self
                    .catalog
                    .replay_managed_sealed_sources(
                        directory,
                        operation,
                        &services.verifier,
                        turn.remaining()?,
                    )
                    .await?;
                let installed = observer
                    .install_managed_transfer_child(
                        transition,
                        sources,
                        history,
                        services.recovery.clone(),
                        turn.remaining()?,
                    )
                    .await
                    .map_err(setup_failure)?;
                RaftFleetTransitionChildSetupResult::Installed {
                    member: installed.member.get(),
                    node_id: hex::encode(installed.node.bytes()),
                    disk_lineage: hex::encode(installed.disk.bytes()),
                    storage_instance: hex::encode(installed.engine.bytes()),
                }
            }
            RaftFleetTransitionChildStep::Attach => {
                self.child_setup_voter()?
                    .attach_managed_transfer_child(transition, child, turn.remaining()?)
                    .await
                    .map_err(setup_failure)?;
                RaftFleetTransitionChildSetupResult::Attached
            }
            RaftFleetTransitionChildStep::InitializeArchive => {
                self.catalog
                    .initialize_managed_transfer_child_archive(
                        &transition,
                        directory,
                        history,
                        &self.archive_pool,
                        turn.remaining()?,
                    )
                    .await?;
                RaftFleetTransitionChildSetupResult::ArchiveInitialized
            }
            RaftFleetTransitionChildStep::Quorum => {
                let selected = self
                    .catalog
                    .resume_managed_transfer_child_quorum(
                        &transition,
                        directory,
                        history,
                        None,
                        turn.remaining()?,
                    )
                    .await?;
                let (position, journal) = if let Some(selected) = selected {
                    selected
                } else {
                    let proof = self
                        .child_setup_voter()?
                        .group(history)
                        .map_err(|error| setup_failure(CellGroupControllerError::Runtime(error)))?
                        .managed_pending_transfer_quorum(turn.remaining()?)
                        .await
                        .map_err(setup_failure)?;
                    self.catalog
                        .resume_managed_transfer_child_quorum(
                            &transition,
                            directory,
                            history,
                            Some(proof),
                            turn.remaining()?,
                        )
                        .await?
                        .ok_or(FleetCatalogError::Unconfirmed)?
                };
                RaftFleetTransitionChildSetupResult::Quorum {
                    index: position.index.get(),
                    term: position.term.get(),
                    phase: journal.phase(),
                }
            }
        };
        Ok(RaftFleetTransitionChildSetup {
            request: request.clone(),
            result,
        })
    }

    fn child_setup_voter(&self) -> Result<&RaftCells> {
        self.node_observer.as_ref().ok_or_else(|| RaftFleetFailure {
            code: RaftFleetFailureCode::Unsupported,
            outcome: RaftFleetOutcome::NotSubmitted,
            message: "Use a managed endpoint on a reserved voter for the selected child.".into(),
        })
    }
}

fn setup_failure(error: CellGroupControllerError) -> RaftFleetFailure {
    tracing::debug!(target: "cellar::raft::transition_setup", ?error, "managed child setup failed");
    let (code, message) = match error {
        CellGroupControllerError::Catalog(error) => match error.as_ref() {
            CatalogError::Conflict(_) | CatalogError::Fenced | CatalogError::NotInitialMember => {
                return FleetCatalogError::Conflict.into();
            }
            CatalogError::FleetAuthority(error)
                if matches!(error.as_ref(), FleetCatalogError::Conflict) =>
            {
                return FleetCatalogError::Conflict.into();
            }
            CatalogError::FleetAuthority(error)
                if matches!(error.as_ref(), FleetCatalogError::Overloaded) =>
            {
                return FleetCatalogError::Overloaded.into();
            }
            _ => (
                RaftFleetFailureCode::Unavailable,
                "The destination setup could not be confirmed. Reconcile the exact request.",
            ),
        },
        CellGroupControllerError::Runtime(RuntimeError::Overloaded) => (
            RaftFleetFailureCode::Busy,
            "The destination voter is busy. Retry the exact request.",
        ),
        _ => (
            RaftFleetFailureCode::Unavailable,
            "The destination setup could not be confirmed. Reconcile the exact request.",
        ),
    };
    RaftFleetFailure {
        code,
        outcome: RaftFleetOutcome::Unknown,
        message: message.into(),
    }
}
