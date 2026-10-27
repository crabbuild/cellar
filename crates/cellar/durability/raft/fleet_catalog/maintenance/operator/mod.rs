// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable maintenance orchestration. Intent does not claim physical readiness.
mod attempt;
mod cancellation;
mod driver;
mod records;
use super::super::resources::Authority;
use super::*;
use crate::raft::catalog::NamespaceCatalog;
pub use cancellation::{
    FleetRestartOperatorCancelAdmission, FleetRestartOperatorCancelRequest,
    FleetRestartOperatorCancelled,
};
pub use driver::{
    FleetRestartOperatorDriver, FleetRestartOperatorPhase, FleetRestartOperatorProgress,
};
use futures_util::future::BoxFuture;
use records::{Pointer, Record};

/// The human fixes the complete preparation scope before a controller acts.
#[derive(Clone, Debug)]
pub struct FleetRestartOperatorRequest {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub expected_process: ProcessGeneration,
    pub grace_seconds: u16,
    pub birth_resolution: FleetRestartBirthResolution,
}

/// Journal-confirmed operation intent. This is no gate, Ready, stop, new boot,
/// capacity or disruption-slot capability. The executor must use those APIs.
pub struct FleetRestartOperatorAdmission {
    record: Record,
    applied: u64,
    observed: u64,
}

impl FleetRestartOperatorAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn boot(&self) -> &crate::raft::fleet_stop::FleetBootBinding {
        &self.record.boot
    }

    pub fn grace_seconds(&self) -> u16 {
        self.record.grace_seconds
    }

    pub fn birth_resolution(&self) -> FleetRestartBirthResolution {
        self.record.birth_resolution
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

impl FleetCatalog {
    /// Admit one durable operation for an explicitly configured executor. This
    /// fixes the original boot and per-birth choice while its actual M1 admission
    /// still must acquire the shared disruption slot. It performs no shutdown.
    pub fn begin_restart_operator<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        request: &'a FleetRestartOperatorRequest,
        namespaces: &'a [NamespaceCatalog],
        timeout: Duration,
    ) -> BoxFuture<'a, Result<FleetRestartOperatorAdmission>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            validate_request(&request.identity)?;
            if !(1..=300).contains(&request.grace_seconds) {
                return Err(FleetCatalogError::Invalid(
                    "restart grace must be 1..=300 seconds",
                ));
            }
            self.run(timeout, async {
                let current = self.inner.current().await?;
                if let Some(record) = self.restart_operator_candidate(&request.identity).await? {
                    if !record.matches(request) {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    if let Some(applied) = self.restart_operator_applied(&current, &record).await? {
                        return Ok(FleetRestartOperatorAdmission {
                            record,
                            applied,
                            observed: current.revision(),
                        });
                    }
                }
                if current.token != snapshot.token
                    || request.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                if let Some(active) = self.active_restart_operator_inner(snapshot).await? {
                    return Err(FleetCatalogError::DisruptionInProgress {
                        operation: active.record.request.operation,
                    });
                }
                if let Some(active) = self.active_disruption(snapshot).await? {
                    return Err(FleetCatalogError::DisruptionInProgress {
                        operation: active.operation(),
                    });
                }
                if self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &Record::key(request.identity.operation),
                    )
                    .await?
                    .is_some()
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                self.inner
                    .domain
                    .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)??;
                let node = self.host_from_record(snapshot, request.node).await?;
                if node.state() != FleetNodeState::Active
                    || node.record.isolation.is_some()
                    || node.boot_binding()?.process != request.expected_process.bytes()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.confirm_boot(snapshot, &node.record).await?;
                let record = Record {
                    format: 1,
                    request: request.identity.clone(),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    boot: node.boot_binding()?,
                    grace_seconds: request.grace_seconds,
                    birth_resolution: request.birth_resolution,
                };
                record.validate(self)?;
                let committed = self
                    .commit_with_disruption(
                        snapshot,
                        &record.mutation()?,
                        None,
                        Authority::RestartOperator,
                        || Ok(()),
                    )
                    .await?;
                Ok(FleetRestartOperatorAdmission {
                    record,
                    applied: committed.applied_revision,
                    observed: committed.snapshot.revision(),
                })
            })
            .await
        })
    }

    pub fn restart_operator<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<FleetRestartOperatorAdmission>>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            self.run(timeout, self.restart_operator_inner(snapshot, operation))
                .await
        })
    }

    pub fn active_restart_operator<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<FleetRestartOperatorAdmission>>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            self.run(timeout, self.active_restart_operator_inner(snapshot))
                .await
        })
    }

    async fn restart_operator_inner(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
    ) -> Result<Option<FleetRestartOperatorAdmission>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Record::key(operation.bytes()),
            )
            .await?
        else {
            return Ok(None);
        };
        let record: Record = decode(records::KIND, &bytes, 4096)?;
        record.validate(self)?;
        if record.request.operation != operation.bytes()
            || self
                .restart_operator_candidate(&record.request)
                .await?
                .as_ref()
                != Some(&record)
        {
            return Err(FleetCatalogError::Corrupt("operator restart projection"));
        }
        let applied = self
            .restart_operator_applied(snapshot, &record)
            .await?
            .ok_or(FleetCatalogError::Corrupt("unadmitted operator restart"))?;
        Ok(Some(FleetRestartOperatorAdmission {
            record,
            applied,
            observed: snapshot.revision(),
        }))
    }

    async fn active_restart_operator_inner(
        &self,
        snapshot: &FleetSnapshot,
    ) -> Result<Option<FleetRestartOperatorAdmission>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                records::ACTIVE,
            )
            .await?
        else {
            return Ok(None);
        };
        let pointer: Pointer = decode(records::POINTER_KIND, &bytes, 4096)?;
        let admission = self
            .restart_operator_inner(snapshot, pointer.operation()?)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing operator restart"))?;
        if !pointer.matches(&admission.record)? {
            return Err(FleetCatalogError::Corrupt("operator restart pointer"));
        }
        Ok(Some(admission))
    }

    async fn restart_operator_candidate(
        &self,
        request: &FleetRequestIdentity,
    ) -> Result<Option<Record>> {
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
            .find(|c| c.table == FleetTable::Operations && c.key == Record::key(request.operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Record = decode(records::KIND, bytes, 4096)?;
        record.validate(self)?;
        if record.request != *request || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    async fn restart_operator_applied(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
    ) -> Result<Option<u64>> {
        let mutation = record.mutation()?;
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(mutation.id),
                hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
            )
            .await?;
        if applied.is_some_and(|r| mutation.expected_revision.checked_add(1) != Some(r)) {
            return Err(FleetCatalogError::Corrupt("operator restart revision"));
        }
        Ok(applied)
    }

    pub(in crate::raft::fleet_catalog) fn check_restart_operator_mutation(
        &self,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if authority != Authority::RestartOperator
            && mutation.changes.iter().any(|c| {
                c.table == FleetTable::Operations
                    && (c.key == records::ACTIVE
                        || c.key.starts_with(records::PREFIX)
                        || c.key.starts_with(attempt::PREFIX)
                        || cancellation::protected(&c.key))
            })
        {
            return Err(FleetCatalogError::Invalid("restart operator authority"));
        }
        Ok(())
    }
}
