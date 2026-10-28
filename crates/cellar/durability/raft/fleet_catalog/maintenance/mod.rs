// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable preparation for an intact-engine restart. Admission is not readiness.
mod birth_action;
mod cancellation;
mod gate;
mod gate_cancel;
mod operator;
pub(crate) use operator::RestartServiceWork;
mod records;
mod rejoin;
mod resume;
mod runtime;
mod settle;
mod settlement;
mod stop;
use super::evacuation::CanonicalRequest;
use super::histories::validate_request;
use super::*;
use crate::raft::catalog::NamespaceCatalog;
use cellar_logic::consensus::{NodeId, ProcessGeneration, ProposalId};
pub use gate::{FleetRestartGateAdmission, FleetRestartGateRequest};
pub use gate_cancel::FleetRestartGateCancelRequest;
pub use operator::{
    FleetRestartOperatorAction, FleetRestartOperatorActionAdmission,
    FleetRestartOperatorActionRequest, FleetRestartOperatorAdmission,
    FleetRestartOperatorCancelAdmission, FleetRestartOperatorCancelRequest,
    FleetRestartOperatorCancelled, FleetRestartOperatorCompleted, FleetRestartOperatorDriver,
    FleetRestartOperatorPhase, FleetRestartOperatorProgress, FleetRestartOperatorRequest,
    FleetRestartOperatorStatus, FleetRestartService, FleetRestartServiceOptions,
    FleetRestartServiceStatus,
};
pub(super) use records::{Record, KIND};
pub use rejoin::{FleetRestartReturnAdmission, FleetRestartReturnRequest};
pub(crate) use resume::ResumeLeadershipCommand;
pub use resume::{
    FleetRestartResumeAdmission, FleetRestartResumeCompleteRequest, FleetRestartResumeCompletion,
    FleetRestartResumePhase, FleetRestartResumeProgress, FleetRestartResumeRequest,
    FleetRestartResumeVerification,
};
pub use runtime::{
    FleetRestartHistoryProgress, FleetRestartReadyAdmission, FleetRestartReadyRequest,
    FleetRestartRuntimeCandidate, FleetRestartRuntimeProgress, FleetRestartVerification,
};
pub(crate) use runtime::{RestartActionContext, RestartLeadershipCommand};
pub use settle::{
    FleetRestartBirthAction, FleetRestartBirthResolution, FleetRestartSettlementBlocker,
    FleetRestartSettlementProgress,
};
pub use settlement::FleetRestartPendingWork;
pub use stop::{
    FleetRestartOfflineAdmission, FleetRestartOfflineRequest, FleetRestartStopAdmission,
    FleetRestartStopRefreshRequest, FleetRestartStopRequest,
};

/// Exact human intent. Grace postpones replacement only after owned stop is
/// admitted; admission itself neither starts its clock nor permits shutdown.
#[derive(Clone, Debug)]
pub struct FleetRestartRequest {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub expected_process: ProcessGeneration,
    pub grace_seconds: u16,
}

impl FleetRestartRequest {
    fn validate(&self) -> Result<()> {
        validate_request(&self.identity)?;
        if !(1..=300).contains(&self.grace_seconds) {
            return Err(FleetCatalogError::Invalid(
                "restart grace must be 1..=300 seconds",
            ));
        }
        Ok(())
    }
}

/// Journal-confirmed historical preparation, never a RestartReady receipt or
/// permission to stop, boot, activate capacity or release a disruption slot.
pub struct FleetRestartAdmission {
    record: Record,
    applied: u64,
    observed: u64,
}

impl FleetRestartAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn boot(&self) -> Result<crate::raft::fleet_stop::FleetBootBinding> {
        self.record.node.boot_binding(self.record.catalog)
    }

    pub fn enrollment_epoch(&self) -> u64 {
        self.record.node.epoch
    }

    pub fn grace_seconds(&self) -> u16 {
        self.record.grace_seconds
    }

    pub fn namespaces(&self) -> usize {
        self.record.namespaces
    }

    pub fn histories(&self) -> u64 {
        self.record.histories
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

impl FleetCatalog {
    /// Atomically exclude new targets and claim the shared planned-disruption
    /// slot after complete namespace/history inventory. Existing voters and
    /// consensus remain intact. No stop/rejoin capability is issued here.
    pub async fn begin_restart_maintenance(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartRequest,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        request.validate()?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        self.run(timeout, async {
            let current = self.inner.current().await?;
            let canonical = CanonicalRequest::restart(
                self,
                &request.identity,
                request.node.bytes(),
                request.expected_process.bytes(),
                request.grace_seconds,
            )?;
            let canonical_present = self.check_operation_request(&canonical).await?;
            let prior = self.restart_candidate(&request.identity).await?;
            if let Some(record) = &prior {
                if !canonical_present || !record.matches(request) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let mutation = record.mutation()?;
                if self
                    .inner
                    .committed(
                        &current.head,
                        &hex::encode(request.identity.id),
                        hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
                    )
                    .await?
                    .is_some()
                {
                    return self.commit_inner(snapshot, &mutation).await;
                }
            }
            if current.token != snapshot.token
                || request.identity.expected_revision != snapshot.revision()
            {
                return Err(FleetCatalogError::Conflict);
            }
            if self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &hex::encode(request.identity.operation),
                )
                .await?
                .is_some()
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            if let Some(active) = self.active_disruption(snapshot).await? {
                return Err(FleetCatalogError::DisruptionInProgress {
                    operation: active.operation(),
                });
            }
            let inventory = self
                .inner
                .domain
                .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                .await
                .map_err(|_| FleetCatalogError::Closed)??;
            let mut node = self.host_from_record(snapshot, request.node).await?.record;
            if node.boot.as_ref().map(|b| b.process) != Some(request.expected_process.bytes()) {
                return Err(FleetCatalogError::Conflict);
            }
            node.begin_restart(request.identity.operation)?;
            let record = Record {
                format: 1,
                catalog: self.birth_owner()?.catalog,
                request: request.identity.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                namespaces: inventory.namespaces(),
                histories: inventory.histories(),
                grace_seconds: request.grace_seconds,
                node,
            };
            record.validate(self)?;
            if prior.as_ref().is_some_and(|old| *old != record) {
                return Err(FleetCatalogError::PayloadConflict);
            }
            self.inner
                .create_exact(&canonical.key(), &canonical.bytes()?, 4096)
                .await?;
            self.commit_inner(snapshot, &record.mutation()?).await
        })
        .await
    }

    pub async fn restart_maintenance_admission(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<FleetRestartAdmission> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let bytes = self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &hex::encode(operation.bytes()),
                )
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let record: Record = decode(KIND, &bytes, 4096)?;
            record.validate(self)?;
            if record.request.operation != operation.bytes() {
                return Err(FleetCatalogError::Corrupt("restart operation key"));
            }
            let applied = self.confirm_restart(snapshot, &record).await?;
            Ok(FleetRestartAdmission {
                record,
                applied,
                observed: snapshot.revision(),
            })
        })
        .await
    }

    async fn restart_candidate(&self, request: &FleetRequestIdentity) -> Result<Option<Record>> {
        let Some((mutation, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(request.id)),
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
            .find(|c| c.table == FleetTable::Operations && c.key == hex::encode(request.operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Record = decode(KIND, bytes, 4096)?;
        record.validate(self)?;
        if record.request != *request || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    pub(super) async fn confirm_restart(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
    ) -> Result<u64> {
        let canonical = CanonicalRequest::restart(
            self,
            &record.request,
            record.node.node,
            record
                .node
                .boot
                .as_ref()
                .ok_or(FleetCatalogError::Corrupt("restart boot"))?
                .process,
            record.grace_seconds,
        )?;
        if !self.check_operation_request(&canonical).await?
            || self.restart_candidate(&record.request).await?.as_ref() != Some(record)
        {
            return Err(FleetCatalogError::Corrupt("restart request projection"));
        }
        let mutation = record.mutation()?;
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(record.request.id),
                hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("unadmitted restart projection"))?;
        if record.request.expected_revision.checked_add(1) != Some(applied) {
            return Err(FleetCatalogError::Corrupt("restart applied revision"));
        }
        Ok(applied)
    }
}
