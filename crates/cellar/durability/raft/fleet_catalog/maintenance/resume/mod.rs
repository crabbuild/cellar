// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Returned-boot verification, scoped physical resume and terminal capacity.
mod basis;
mod leadership;
mod receiver;
mod records;
mod verification;
use super::rejoin::ReturnedCut;
use super::runtime::{proof, records as runtime_records};
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_catalog::resources::Authority;
use crate::raft::fleet_stop::{
    FleetStopAction, FleetStopRequest, LocalResume, ResumeMode, StopSelector,
};
use crate::raft::physical_inventory::RaftPhysicalInventory;
use cellar_logic::consensus::{GroupScope, MemberId};
use futures_util::future::BoxFuture;
pub(crate) use leadership::ResumeLeadershipCommand;
use records::{Completion, Intent};
use verification::Book;
pub use verification::FleetRestartResumeVerification;

/// Separate human consent for one returned boot and intended open profile.
/// Refresh after known Root drift with a fresh identity and the same child/scope.
#[derive(Clone, Debug)]
pub struct FleetRestartResumeRequest {
    pub identity: FleetRequestIdentity,
    pub returning: FleetRequestIdentity,
    pub attestation: FleetCapacityAttestation,
    pub resume: ProposalId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetRestartResumeCompleteRequest {
    pub identity: FleetRequestIdentity,
    pub resume: FleetRequestIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetRestartResumePhase {
    Paused,
    Resumed,
}

impl From<ResumeMode> for FleetRestartResumePhase {
    fn from(mode: ResumeMode) -> Self {
        match mode {
            ResumeMode::Paused => Self::Paused,
            ResumeMode::Resumed => Self::Resumed,
        }
    }
}

/// Progress is descriptive, with repaired histories distinct from intact copies.
#[derive(Clone, Debug)]
pub enum FleetRestartResumeProgress {
    Unrelated {
        group: GroupScope,
    },
    Verified {
        group: GroupScope,
        current_member: Option<MemberId>,
        retained_member: Option<MemberId>,
        replaced: bool,
    },
    HandoffRequested {
        group: GroupScope,
        from: MemberId,
        to: MemberId,
    },
}

pub struct FleetRestartResumeAdmission {
    record: Intent,
    applied: u64,
    observed: u64,
}

impl FleetRestartResumeAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request.identity
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }

    /// Data only. The receiving owner independently renews physical/quorum proof.
    pub fn resume_request(&self) -> Result<FleetStopRequest> {
        FleetStopRequest::new(self.record.effect_selector()?, FleetStopAction::Resume)
            .map_err(|_| FleetCatalogError::Corrupt("resume selector"))
    }
}

/// Historical terminal fact. Current target decisions still require current
/// capacity authorization and fresh occupancy; no response creates a lease.
pub struct FleetRestartResumeCompletion {
    record: Completion,
    applied: u64,
    observed: u64,
}

impl FleetRestartResumeCompletion {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.fact.request.identity
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }

    pub fn retained_copies(&self) -> u64 {
        self.record.fact.retained
    }

    pub fn replaced_histories(&self) -> u64 {
        self.record.fact.replaced
    }

    pub fn intact(&self) -> bool {
        self.record.fact.replaced == 0
    }
}

impl FleetRestartResumeRequest {
    fn validate(&self) -> Result<()> {
        validate_request(&self.identity)?;
        validate_request(&self.returning)?;
        self.attestation.validate()?;
        if self.identity.operation != self.returning.operation
            || self.identity.expected_revision <= self.returning.expected_revision
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(())
    }

    fn same_scope(&self, other: &Self) -> bool {
        self.identity.operation == other.identity.operation
            && self.returning == other.returning
            && self.attestation == other.attestation
            && self.resume == other.resume
    }
}

impl FleetCatalog {
    pub fn prepare_restart_resume<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        request: &'a FleetRestartResumeRequest,
        namespaces: &'a [NamespaceCatalog],
        observer: &'a RaftCells,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<FleetRestartResumeVerification>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            request.validate()?;
            let deadline = Deadline::new(self, timeout)?;
            let (cut, basis) = self
                .run(deadline.remaining()?, async {
                    if self.inner.current().await?.token != snapshot.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    self.inner
                        .domain
                        .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)??;
                    let cut = self
                        .checked_returned_cut(snapshot, request.identity.operation)
                        .await?;
                    Intent::check_scope(request, &cut)?;
                    self.check_restart_capacity_input(snapshot, &cut.node, &request.attestation)
                        .await?;
                    let node = NodeId::new(cut.node.node)
                        .map_err(|_| FleetCatalogError::Corrupt("resume node"))?;
                    let pending = self.restart_pending_work_inner(snapshot, node).await?;
                    if pending.pending_births != 0
                        || pending.active_birth.is_some()
                        || !pending.movements.is_empty()
                        || !pending.reservations.is_empty()
                    {
                        return Err(FleetCatalogError::InventoryIncomplete);
                    }
                    let basis = basis::Basis::capture(snapshot, &cut);
                    basis.check_current(self).await?;
                    Ok((cut, basis))
                })
                .await?;
            FleetRestartResumeVerification::new(
                verification::VerificationInput {
                    catalog: self,
                    snapshot,
                    request,
                    observer,
                    cut,
                    basis,
                    local: None,
                },
                &deadline,
            )
            .await
        })
    }

    pub async fn restart_resume_admission(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartResumeRequest,
        timeout: Duration,
    ) -> Result<FleetRestartResumeAdmission> {
        self.check_snapshot(snapshot)?;
        request.validate()?;
        self.run(timeout, async {
            let record = self
                .resume_intent_candidate(snapshot, &request.identity)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            if !record.request.same_scope(request) {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let applied = self
                .resume_applied(snapshot, &record.mutation()?)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            Ok(FleetRestartResumeAdmission {
                record,
                applied,
                observed: snapshot.revision(),
            })
        })
        .await
    }

    pub async fn resume_restart_maintenance(
        &self,
        admission: &FleetRestartResumeAdmission,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<()> {
        let ack = observer
            .request_peer_fleet_stop(
                admission.resume_request()?,
                admission.observed_revision(),
                timeout,
            )
            .await?;
        if ack.action() != FleetStopAction::Resume
            || ack.operation() != admission.record.request.identity.operation
            || ack.request() != admission.record.request.resume
            || ack.process() != admission.record.request.attestation.boot.process
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(())
    }

    pub async fn restart_resume_completion(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartResumeCompleteRequest,
        timeout: Duration,
    ) -> Result<FleetRestartResumeCompletion> {
        self.check_snapshot(snapshot)?;
        validate_request(&request.identity)?;
        validate_request(&request.resume)?;
        self.run(timeout, async {
            let record = self
                .resume_completion_candidate(snapshot, request)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let applied = self
                .resume_applied(snapshot, &record.mutation(self)?)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            Ok(FleetRestartResumeCompletion {
                record,
                applied,
                observed: snapshot.revision(),
            })
        })
        .await
    }

    /// Discover a terminal operation fact after another controller completed it.
    /// This does not restore any runtime health or current capacity lease.
    pub async fn restart_resumed(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<FleetRestartResumeCompletion>> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let Some(bytes) = self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &Completion::key(operation.bytes()),
                )
                .await?
            else {
                return Ok(None);
            };
            let projected: records::Completed = decode(records::COMPLETE_KIND, &bytes, 4096)?;
            if projected.request.identity.operation != operation.bytes() {
                return Err(FleetCatalogError::Corrupt("resume completion key"));
            }
            let record = self
                .resume_completion_candidate(snapshot, &projected.request)
                .await?
                .ok_or(FleetCatalogError::Corrupt(
                    "missing resume completion request",
                ))?;
            if record.bytes()? != bytes {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let applied = self
                .resume_applied(snapshot, &record.mutation(self)?)
                .await?
                .ok_or(FleetCatalogError::Corrupt("unadmitted resume completion"))?;
            Ok(Some(FleetRestartResumeCompletion {
                record,
                applied,
                observed: snapshot.revision(),
            }))
        })
        .await
    }

    async fn resume_applied(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<Option<u64>> {
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(mutation.id),
                hash(&encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?),
            )
            .await?;
        if applied.is_some_and(|r| mutation.expected_revision.checked_add(1) != Some(r)) {
            return Err(FleetCatalogError::Corrupt("resume journal revision"));
        }
        Ok(applied)
    }

    async fn resume_intent_candidate(
        &self,
        snapshot: &FleetSnapshot,
        identity: &FleetRequestIdentity,
    ) -> Result<Option<Intent>> {
        let Some((mutation, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(identity.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(None);
        };
        mutation.validate()?;
        let bytes = mutation
            .changes
            .iter()
            .find(|c| c.table == FleetTable::Operations && c.key == Intent::key(identity.operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Intent = decode(records::INTENT_KIND, bytes, 4096)?;
        let cut = self
            .historical_returned_cut(snapshot, identity.operation)
            .await?;
        record.validate(&cut)?;
        if record.request.identity != *identity || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    async fn current_resume_intent(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<Intent> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Intent::key(operation),
            )
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let record: Intent = decode(records::INTENT_KIND, &bytes, 4096)?;
        let canonical = self
            .resume_intent_candidate(snapshot, &record.request.identity)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing resume consent"))?;
        if record != canonical
            || self
                .resume_applied(snapshot, &record.mutation()?)
                .await?
                .is_none()
        {
            return Err(FleetCatalogError::Corrupt("unadmitted resume consent"));
        }
        Ok(record)
    }

    async fn resume_completion_candidate(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartResumeCompleteRequest,
    ) -> Result<Option<Completion>> {
        let Some((mutation, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(request.identity.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(None);
        };
        let record = Completion::from_mutation(self, &mutation)?;
        if record.fact.request.identity != request.identity
            || record.fact.request.resume != request.resume
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let intent = self
            .resume_intent_candidate(snapshot, &request.resume)
            .await?
            .ok_or(FleetCatalogError::Corrupt("resume completion parent"))?;
        if self
            .resume_applied(snapshot, &intent.mutation()?)
            .await?
            .is_none()
        {
            return Err(FleetCatalogError::Corrupt("unadmitted resume parent"));
        }
        let cut = self
            .historical_returned_cut(snapshot, request.identity.operation)
            .await?;
        record.validate(self, &intent, &cut)?;
        Ok(Some(record))
    }

    pub(in crate::raft::fleet_catalog) fn check_restart_resume_mutation(
        &self,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if authority != Authority::RestartResume
            && mutation.changes.iter().any(|c| {
                c.table == FleetTable::Operations
                    && (c.key.starts_with(records::INTENT_PREFIX)
                        || c.key.starts_with(records::COMPLETE_PREFIX))
            })
        {
            return Err(FleetCatalogError::Invalid("restart resume authority"));
        }
        Ok(())
    }

    pub(in crate::raft::fleet_catalog) fn check_restart_resume_capacity_request(
        &self,
        mutation: &FleetMutation,
        request: &FleetCapacityRequest,
        node: &super::super::nodes::NodeRecord,
    ) -> Result<()> {
        let completion = Completion::from_mutation(self, mutation)?;
        if completion.capacity_request() != *request || completion.node != *node {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(())
    }
}
