// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained-engine return intent grants no capacity or application resume.
mod boot;
mod records;
use super::super::{nodes::NodeRecord, resources::Authority};
use super::gate::Gate;
use super::*;
use crate::raft::fleet_stop::{LifecyclePurpose, LifecycleUpdate, StopSelector};
use crate::raft::storage::ReplicaLog;
pub(super) use boot::ReturnedCut;
use records::{Return, KIND, LIMIT, PREFIX};

#[derive(Clone, Debug)]
pub struct FleetRestartReturnRequest {
    pub identity: FleetRequestIdentity,
    pub offline: FleetRequestIdentity,
    pub node: NodeId,
    pub expected_process: ProcessGeneration,
    pub next_process: ProcessGeneration,
}

/// Canonical return consent. Only the exclusive retained-engine owner can
/// consume it; admission preserves exclusion, resource charges and disruption.
pub struct FleetRestartReturnAdmission {
    record: Return,
    applied: u64,
    observed: u64,
}

impl FleetRestartReturnAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn next_process(&self) -> Result<ProcessGeneration> {
        ProcessGeneration::new(self.record.process)
            .map_err(|_| FleetCatalogError::Corrupt("restart return process"))
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

impl FleetCatalog {
    pub async fn admit_restart_return(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartReturnRequest,
        timeout: Duration,
    ) -> Result<FleetRestartReturnAdmission> {
        self.check_snapshot(snapshot)?;
        validate_request(&request.identity)?;
        validate_request(&request.offline)?;
        if request.identity.operation != request.offline.operation
            || request.identity.expected_revision <= request.offline.expected_revision
            || request.next_process == request.expected_process
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        self.run(timeout, async {
            let current = self.inner.current().await?;
            let prior = self.restart_return_candidate(&request.identity).await?;
            if let Some(record) = &prior {
                if !record.matches(request) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                if let Some(applied) = self.restart_return_applied(&current, record).await? {
                    return Ok(FleetRestartReturnAdmission {
                        record: record.clone(),
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
            if self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &Return::key(request.identity.operation),
                )
                .await?
                .is_some()
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let (offline, _, gate) = self
                .checked_restart_offline(snapshot, request.identity.operation)
                .await?;
            self.require_return_origin(snapshot, &gate).await?;
            let record = Return {
                format: 1,
                request: request.identity.clone(),
                offline: request.offline.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                closure: offline.closure,
                gate_parent: gate.request.id,
                gate_digest: gate.digest()?,
                node: gate.node.node,
                old_process: gate.node.boot_binding(gate.preparation.catalog)?.process,
                process: request.next_process.bytes(),
            };
            record.validate(self, &gate)?;
            if offline.request != request.offline
                || !record.matches(request)
                || prior.as_ref().is_some_and(|old| old != &record)
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let committed = self
                .commit_with_disruption(
                    snapshot,
                    &record.mutation()?,
                    None,
                    Authority::RestartReturn,
                    || Ok(()),
                )
                .await?;
            Ok(FleetRestartReturnAdmission {
                record,
                applied: committed.applied_revision,
                observed: committed.snapshot.revision(),
            })
        })
        .await
    }

    async fn require_return_origin(&self, snapshot: &FleetSnapshot, gate: &Gate) -> Result<()> {
        if self
            .checked_restart_gate(snapshot, gate.request.operation)
            .await?
            != *gate
        {
            return Err(FleetCatalogError::Conflict);
        }
        self.require_restart_settlement_idle(snapshot, gate.request.operation)
            .await
    }

    async fn restart_return_candidate(
        &self,
        identity: &FleetRequestIdentity,
    ) -> Result<Option<Return>> {
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
            .find(|c| c.table == FleetTable::Operations && c.key == Return::key(identity.operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Return = decode(KIND, bytes, LIMIT)?;
        let gate = self
            .restart_gate_candidate_for_id(record.gate_parent, identity.operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("restart return gate"))?;
        record.validate(self, &gate)?;
        if record.request != *identity || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    async fn restart_return_applied(
        &self,
        snapshot: &FleetSnapshot,
        record: &Return,
    ) -> Result<Option<u64>> {
        let applied = self
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(record.request.id),
                hash(&encode(REQUEST_KIND, &record.mutation()?, REQUEST_LIMIT)?),
            )
            .await?;
        if applied.is_some_and(|r| record.request.expected_revision.checked_add(1) != Some(r)) {
            return Err(FleetCatalogError::Corrupt("restart return revision"));
        }
        Ok(applied)
    }

    async fn checked_restart_return(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<(Return, StopSelector, Gate)> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Return::key(operation),
            )
            .await?
            .ok_or(FleetCatalogError::Invalid(
                "maintenance restart requires joined owner proof",
            ))?;
        let record: Return = decode(KIND, &bytes, LIMIT)?;
        let canonical = self
            .restart_return_candidate(&record.request)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing restart return request"))?;
        let (offline, stop, gate) = self.checked_restart_offline(snapshot, operation).await?;
        if canonical != record
            || self
                .restart_return_applied(snapshot, &record)
                .await?
                .is_none()
            || offline.request != record.offline
            || offline.closure != record.closure
        {
            return Err(FleetCatalogError::Corrupt("unadmitted restart return"));
        }
        record.validate(self, &gate)?;
        Ok((record, stop.selector(&gate)?, gate))
    }

    pub(in crate::raft::fleet_catalog) fn check_restart_return_mutation(
        &self,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if authority != Authority::RestartReturn
            && mutation.changes.iter().any(|c| {
                c.table == FleetTable::Operations
                    && (c.key.starts_with(PREFIX) || c.key.starts_with(boot::PREFIX))
            })
        {
            return Err(FleetCatalogError::Invalid("restart return authority"));
        }
        Ok(())
    }

    /// Reuse the node mutation pass's exact snapshot lookup and decoded rows.
    /// This adds neither another I/O turn nor a nested async future to commits.
    pub(in crate::raft::fleet_catalog) fn check_restart_return_node_change(
        &self,
        old: Option<&NodeRecord>,
        next: Option<&NodeRecord>,
        authority: Authority,
    ) -> Result<()> {
        if authority != Authority::RestartReturn {
            if let Some(old) = old.filter(|n| n.state == FleetNodeState::RestartPending) {
                let next = next.ok_or(FleetCatalogError::Conflict)?;
                if next.state == FleetNodeState::Active && authority != Authority::RestartResume {
                    return Err(FleetCatalogError::Invalid("restart resume authority"));
                }
                if old.boot != next.boot || old.storage_instance != next.storage_instance {
                    return Err(FleetCatalogError::Invalid("restart return boot authority"));
                }
            }
        }
        Ok(())
    }
}
