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
    /// Exact active-source preparation selected by the journal.
    pub preparation_digest: Option<String>,
    /// Exact independently verified transfer archive selected by the journal.
    pub transfer_archive_digest: Option<String>,
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
            preparation_digest: journal
                .and_then(|journal| journal.preparation())
                .map(hex::encode),
            transfer_archive_digest: journal
                .and_then(|journal| journal.transfer_archive())
                .map(hex::encode),
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

/// Select pre-copy for the exact admitted plan. Archive inputs and scratch
/// resources come from the trusted operator server configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionPrepareRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub plan_digest: String,
}

impl RaftFleetTransitionPrepareRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::catalog_id(&self.plan_digest)?;
        for id in [namespace, operation, &self.request_id] {
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
pub struct RaftFleetTransitionPreparation {
    pub request: RaftFleetTransitionPrepareRequest,
    pub preparation_digest: String,
    /// Exact retries can observe a later phase with the same preparation.
    pub phase: TransitionPhase,
}

#[derive(Clone)]
pub(super) struct Preparation {
    verifier: crate::raft::fleet_catalog::FleetGroupArchiveVerifier,
    recovery: crate::raft::cell_groups::CellGroupRecoveryPool,
}

impl RaftFleetControl {
    /// Configure private physical replay resources and bounded restore
    /// admission for operator split and merge preparation.
    pub fn with_transition_preparer(
        mut self,
        verifier: crate::raft::fleet_catalog::FleetGroupArchiveVerifier,
        recovery: crate::raft::cell_groups::CellGroupRecoveryPool,
    ) -> Self {
        self.transition_preparation = Some(Preparation { verifier, recovery });
        self
    }

    /// Prepare the admitted plan from operation-owned archive cuts. Only
    /// Planned work can enter physical replay. Later exact retries return the
    /// rooted preparation digest, with no extra allocation or authority.
    pub async fn prepare_transition(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionPrepareRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionPreparation> {
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
        let journal = match journal.phase() {
            TransitionPhase::Planned => {
                let services = self.transition_preparation.as_ref().ok_or_else(|| {
                    RaftFleetFailure {
                        code: RaftFleetFailureCode::Unsupported,
                        outcome: RaftFleetOutcome::NotSubmitted,
                        message: "Transition preparation requires a trusted archive verifier on the management server.".into(),
                    }
                })?;
                self.catalog
                    .allocate_managed_preparation_holds(
                        directory,
                        operation,
                        &self.archive_pool,
                        turn.remaining()?,
                    )
                    .await?;
                let prepared = self
                    .catalog
                    .resume_managed_preparation(
                        directory,
                        operation,
                        &services.verifier,
                        services.recovery.clone(),
                        turn.remaining()?,
                    )
                    .await?;
                prepared.journal().clone()
            }
            TransitionPhase::Cancelled => return Err(FleetCatalogError::Conflict.into()),
            _ => journal,
        };
        let preparation_digest = journal.preparation().ok_or(FleetCatalogError::Conflict)?;
        Ok(RaftFleetTransitionPreparation {
            request: request.clone(),
            preparation_digest: hex::encode(preparation_digest),
            phase: journal.phase(),
        })
    }
}

/// Select the irreversible seal decision for the exact prepared transition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionSealRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub plan_digest: String,
    pub preparation_digest: String,
}

impl RaftFleetTransitionSealRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        for digest in [
            &self.catalog_id,
            &self.plan_digest,
            &self.preparation_digest,
        ] {
            records::catalog_id(digest)?;
        }
        for id in [namespace, operation, &self.request_id] {
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
pub struct RaftFleetTransitionSealAuthorization {
    pub request: RaftFleetTransitionSealRequest,
    /// The observed phase can advance after the seal decision.
    pub phase: TransitionPhase,
}

impl RaftFleetControl {
    /// Authorize source sealing for the exact durable pre-copy. A retry can
    /// observe the same authorization after the transition has advanced.
    pub async fn authorize_transition_seal(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionSealRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionSealAuthorization> {
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
        if journal.preparation().map(hex::encode).as_deref()
            != Some(request.preparation_digest.as_str())
        {
            return Err(FleetCatalogError::Conflict.into());
        }
        let phase = match journal.phase() {
            TransitionPhase::Prepared => self
                .catalog
                .authorize_managed_transition_seal(
                    &transition.request,
                    directory,
                    &transition.plan,
                    turn.remaining()?,
                )
                .await?
                .phase(),
            TransitionPhase::Planned | TransitionPhase::Cancelled => {
                return Err(FleetCatalogError::Conflict.into());
            }
            phase => phase,
        };
        Ok(RaftFleetTransitionSealAuthorization {
            request: request.clone(),
            phase,
        })
    }
}

/// Select the child ranges for an exact verified transfer archive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionPublishRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub plan_digest: String,
    pub archive_digest: String,
}

impl RaftFleetTransitionPublishRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        for digest in [&self.catalog_id, &self.plan_digest, &self.archive_digest] {
            records::catalog_id(digest)?;
        }
        for id in [namespace, operation, &self.request_id] {
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
pub struct RaftFleetTransitionPublication {
    pub request: RaftFleetTransitionPublishRequest,
    /// Original namespace directory revision, including on historical retry.
    pub directory_revision: u64,
    pub tree_digest: String,
    pub publication_digest: String,
}

impl RaftFleetControl {
    /// Atomically publish the admitted child partition, or return its exact
    /// immutable publication certificate after an uncertain response.
    pub async fn publish_transition(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionPublishRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionPublication> {
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
        let archive = journal
            .transfer_archive()
            .ok_or(FleetCatalogError::Conflict)?;
        if hex::encode(archive) != request.archive_digest {
            return Err(FleetCatalogError::Conflict.into());
        }
        let publication = match journal.phase() {
            TransitionPhase::Archived => {
                self.catalog
                    .publish_managed_transfer(
                        &transition,
                        directory,
                        &self.archive_pool,
                        turn.remaining()?,
                    )
                    .await?
            }
            TransitionPhase::Published | TransitionPhase::Active | TransitionPhase::Retired => self
                .domain
                .run(Box::pin(crate::asyncrt::timeout(
                    turn.remaining()?,
                    Box::pin(directory.publication(&transition.plan)),
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(|_| FleetCatalogError::Unconfirmed)?
                .map_err(|error| match error {
                    crate::raft::catalog::CatalogError::Unconfirmed => {
                        FleetCatalogError::Unconfirmed
                    }
                    error => FleetCatalogError::from(error),
                })?
                .ok_or(FleetCatalogError::Corrupt("missing transfer publication"))?,
            _ => return Err(FleetCatalogError::Conflict.into()),
        };
        if publication.operation() != operation || publication.transfer_archive() != Some(archive) {
            return Err(FleetCatalogError::Corrupt("transfer publication binding").into());
        }
        let tree = publication.tree_digest().ok_or(FleetCatalogError::Corrupt(
            "empty transfer publication tree",
        ))?;
        Ok(RaftFleetTransitionPublication {
            request: request.clone(),
            directory_revision: publication.revision(),
            tree_digest: hex::encode(tree),
            publication_digest: hex::encode(publication.journal_digest()),
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
