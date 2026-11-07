// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator dispatch to the current managed parent voter.

use super::*;
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{
        CellGroupControllerError, CellGroupSealOutcome, CellGroupSourceRetirement, TransitionPhase,
    },
    completion::RuntimeError,
    fleet_catalog::ReopenedManagedCellGroupTransition,
};
use cellar_logic::cell_group::GroupHistory;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionSourceArchive {
    pub request: RaftFleetTransitionSourceSealRequest,
    /// The applied parent cut independently verified from its retained archive.
    pub sealed: RaftFleetTransitionSourceCut,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionSourceRetirement {
    pub request: RaftFleetTransitionSourceSealRequest,
    pub phase: TransitionPhase,
    /// Durable parent position, even while other parents remain unsettled.
    pub applied: Option<RaftFleetTransitionActivationPosition>,
}

enum SourceAction {
    Seal,
    Retire,
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
        let turn = Turn::new(&self.domain, timeout)?;
        let (_, history, transition) = self
            .authorize_source_action(
                principal,
                namespace,
                operation,
                request,
                &turn,
                SourceAction::Seal,
            )
            .await?;
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
            .map_err(|error| source_failure(CellGroupControllerError::Runtime(error)))?;
        let applied = match voter
            .seal_managed_source(&transition, turn.remaining()?)
            .await
            .map_err(source_failure)?
        {
            CellGroupSealOutcome::Accepted(_) => None,
            CellGroupSealOutcome::Applied(seal) => Some(source_cut(&seal)),
        };
        Ok(RaftFleetTransitionSourceSeal {
            request: request.clone(),
            applied,
        })
    }

    /// Verify and retain the selected parent's sealed archive. This can run
    /// on an archive-enabled management endpoint without a local voter.
    pub async fn capture_transition_source(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionSourceSealRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionSourceArchive> {
        let turn = Turn::new(&self.domain, timeout)?;
        let (directory, history, transition) = self
            .authorize_source_action(
                principal,
                namespace,
                operation,
                request,
                &turn,
                SourceAction::Seal,
            )
            .await?;
        let services = self
            .transition_preparation
            .as_ref()
            .ok_or_else(|| RaftFleetFailure {
                code: RaftFleetFailureCode::Unsupported,
                outcome: RaftFleetOutcome::NotSubmitted,
                message:
                    "Source capture requires a trusted archive verifier on the management server."
                        .into(),
            })?;
        let hold = self
            .catalog
            .capture_managed_sealed_source(
                directory,
                transition.plan.operation(),
                history,
                &services.verifier,
                turn.remaining()?,
            )
            .await?;
        let seal = hold
            .marker()
            .source_seal()
            .ok_or(FleetCatalogError::Corrupt("verified source has no seal"))?;
        Ok(RaftFleetTransitionSourceArchive {
            request: request.clone(),
            sealed: source_cut(seal),
        })
    }

    /// Submit the parent's tombstone after child archive coverage is selected,
    /// or reopen the exact acknowledgement without a local voter.
    pub async fn retire_transition_source(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionSourceSealRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionSourceRetirement> {
        let turn = Turn::new(&self.domain, timeout)?;
        let (directory, history, transition) = self
            .authorize_source_action(
                principal,
                namespace,
                operation,
                request,
                &turn,
                SourceAction::Retire,
            )
            .await?;
        let result = |phase, applied: Option<cellar_logic::consensus::LogPosition>| {
            RaftFleetTransitionSourceRetirement {
                request: request.clone(),
                phase,
                applied: applied.map(|position| RaftFleetTransitionActivationPosition {
                    index: position.index.get(),
                    term: position.term.get(),
                }),
            }
        };
        if let Some((position, journal)) = self
            .catalog
            .resume_managed_source_retirement(
                &transition,
                directory,
                &self.archive_pool,
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
                message: "Use a managed endpoint with a voter for the selected parent.".into(),
            })?;
        let voter = observer
            .group(history)
            .map_err(|error| source_failure(CellGroupControllerError::Runtime(error)))?;
        match voter
            .retire_managed_source(&transition, turn.remaining()?)
            .await
            .map_err(source_failure)?
        {
            CellGroupSourceRetirement::Accepted(_) => Ok(result(TransitionPhase::Active, None)),
            CellGroupSourceRetirement::Applied(ack) => {
                let (position, journal) = self
                    .catalog
                    .resume_managed_source_retirement(
                        &transition,
                        directory,
                        &self.archive_pool,
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

    /// Drain and detach one retired local source after every final member has
    /// applied its tombstone. The retained engine and storage remain charged.
    pub async fn quiesce_transition_source(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionSourceSealRequest,
        timeout: Duration,
    ) -> Result<()> {
        let turn = Turn::new(&self.domain, timeout)?;
        let (directory, history, transition) = self
            .authorize_source_action(
                principal,
                namespace,
                operation,
                request,
                &turn,
                SourceAction::Retire,
            )
            .await?;
        let observer = self
            .node_observer
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?;
        self.catalog
            .quiesce_managed_transfer_source(
                &transition,
                directory,
                observer,
                history,
                turn.remaining()?,
            )
            .await
            .map_err(Into::into)
    }

    async fn authorize_source_action<'a>(
        &'a self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionSourceSealRequest,
        turn: &Turn<'_>,
        action: SourceAction,
    ) -> Result<(
        &'a RangeDirectory,
        GroupHistory,
        ReopenedManagedCellGroupTransition,
    )> {
        request.validate(namespace, operation)?;
        self.check_catalog(&request.catalog_id)?;
        let directory = self.managed_namespace(namespace)?;
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let history = request.source.history(directory)?;
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
            || !transition
                .journal
                .as_ref()
                .is_some_and(|journal| match action {
                    SourceAction::Seal => matches!(
                        journal.phase(),
                        TransitionPhase::SealAuthorized
                            | TransitionPhase::Sealed
                            | TransitionPhase::Installed
                            | TransitionPhase::Archived
                    ),
                    SourceAction::Retire => {
                        matches!(
                            journal.phase(),
                            TransitionPhase::Active | TransitionPhase::Retired
                        ) && journal.child_archives().is_some()
                    }
                })
        {
            return Err(FleetCatalogError::Conflict.into());
        }
        Ok((directory, history, transition))
    }
}

fn source_cut(seal: &crate::raft::cell_groups::GroupSourceSeal) -> RaftFleetTransitionSourceCut {
    RaftFleetTransitionSourceCut {
        index: seal.position().index.get(),
        term: seal.position().term.get(),
        authorization_digest: hex::encode(seal.authorization()),
    }
}

fn source_failure(error: CellGroupControllerError) -> RaftFleetFailure {
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
            "The parent operation could not be confirmed. Reconcile the exact request.",
        ),
    };
    RaftFleetFailure {
        code,
        outcome: RaftFleetOutcome::Unknown,
        message: message.into(),
    }
}
