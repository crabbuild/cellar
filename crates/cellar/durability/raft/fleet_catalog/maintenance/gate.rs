// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root the higher gate epoch before issuing any reversible physical capability.
use super::super::{
    disruption::Disruption,
    nodes::{NodeRecord, NODE_KIND},
};
use super::*;
use crate::raft::fleet_stop::{FleetStopAction, FleetStopRequest, LifecyclePurpose, StopSelector};
use serde::Deserialize;

const KIND: u8 = 78;
const LIMIT: usize = 4096;

#[derive(Clone, Debug)]
pub struct FleetRestartGateRequest {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub expected_process: ProcessGeneration,
    pub gate: ProposalId,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Gate {
    format: u8,
    pub request: FleetRequestIdentity,
    input_root: [u8; 32],
    pub disruption: [u8; 32],
    pub preparation: Record,
    pub node: NodeRecord,
    child: [u8; 16],
}

pub struct FleetRestartGateAdmission {
    record: Gate,
    applied: u64,
    observed: u64,
}

impl FleetRestartGateAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }

    pub fn enrollment_epoch(&self) -> u64 {
        self.record.node.epoch
    }

    /// Request data only. The receiver checks current root, exact boot and its
    /// serialized lifecycle guard. This is never a readiness or stop proof.
    pub fn quiesce_request(&self) -> Result<FleetStopRequest> {
        FleetStopRequest::new(
            self.record.selector(self.record.child)?,
            FleetStopAction::Quiesce,
        )
        .map_err(|_| FleetCatalogError::Corrupt("maintenance quiesce request"))
    }
}

impl Gate {
    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(hash(&encode(KIND, self, LIMIT)?))
    }

    pub fn key(operation: [u8; 16]) -> String {
        format!("restart-gate/{}", hex::encode(operation))
    }

    pub fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.preparation.validate(catalog)?;
        self.node.validate(&catalog.inner)?;
        ProposalId::new(self.child)
            .map_err(|_| FleetCatalogError::Corrupt("maintenance gate ID"))?;
        let mut expected = self.preparation.node.clone();
        expected.advance_restart_gate(self.request.operation)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.node != expected
            || self.request.operation != self.preparation.request.operation
            || self.request.expected_revision <= self.preparation.request.expected_revision
            || self.disruption != hash(&encode(super::records::KIND, &self.preparation, 4096)?)
        {
            return Err(FleetCatalogError::Corrupt("maintenance gate admission"));
        }
        Ok(())
    }

    fn matches(&self, request: &FleetRestartGateRequest) -> bool {
        self.request == request.identity
            && self.node.node == request.node.bytes()
            && self.node.boot.as_ref().map(|boot| boot.process)
                == Some(request.expected_process.bytes())
            && self.child == request.gate.bytes()
    }

    pub fn selector(&self, child: [u8; 16]) -> Result<StopSelector> {
        let selector = StopSelector {
            purpose: LifecyclePurpose::Maintenance,
            boot: self.node.boot_binding(self.preparation.catalog)?,
            epoch: self.node.epoch,
            operation: self.request.operation,
            finalization: self.request.id,
            request: child,
        };
        selector
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("maintenance gate selector"))?;
        Ok(selector)
    }

    pub fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(self.node.node),
                    value: Some(encode(NODE_KIND, &self.node, 4096)?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::key(self.request.operation),
                    value: Some(encode(KIND, self, LIMIT)?),
                },
            ],
        })
    }

    async fn applied(
        &self,
        catalog: &FleetCatalog,
        snapshot: &FleetSnapshot,
    ) -> Result<Option<u64>> {
        self.validate(catalog)?;
        let mutation = self.mutation()?;
        let stored = catalog.restart_gate_candidate(&self.request).await?.ok_or(
            FleetCatalogError::Corrupt("missing maintenance gate request"),
        )?;
        if stored != *self {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let applied = catalog
            .inner
            .committed(
                &snapshot.head,
                &hex::encode(self.request.id),
                hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
            )
            .await?;
        if applied.is_some_and(|r| self.request.expected_revision.checked_add(1) != Some(r)) {
            return Err(FleetCatalogError::Corrupt(
                "maintenance gate applied revision",
            ));
        }
        Ok(applied)
    }

    fn admission(self, applied: u64, observed: u64) -> FleetRestartGateAdmission {
        FleetRestartGateAdmission {
            record: self,
            applied,
            observed,
        }
    }
}

impl FleetCatalog {
    pub async fn admit_restart_gate(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartGateRequest,
        timeout: Duration,
    ) -> Result<FleetRestartGateAdmission> {
        self.check_snapshot(snapshot)?;
        validate_request(&request.identity)?;
        self.run(timeout, async {
            let current = self.inner.current().await?;
            let prior = self.restart_gate_candidate(&request.identity).await?;
            if let Some(record) = &prior {
                if !record.matches(request) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                if let Some(applied) = record.applied(self, &current).await? {
                    return Ok(record.clone().admission(applied, current.revision()));
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
                    &Gate::key(request.identity.operation),
                )
                .await?
                .is_some()
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let active = self
                .active_disruption(snapshot)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let Disruption::Restart(preparation) = active else {
                return Err(FleetCatalogError::DisruptionInProgress {
                    operation: active.operation(),
                });
            };
            if preparation.request.operation != request.identity.operation
                || preparation.node.node != request.node.bytes()
                || preparation.node.boot.as_ref().map(|boot| boot.process)
                    != Some(request.expected_process.bytes())
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let mut node = self.host_from_record(snapshot, request.node).await?.record;
            if node != preparation.node {
                return Err(FleetCatalogError::Conflict);
            }
            node.advance_restart_gate(request.identity.operation)?;
            let record = Gate {
                format: 1,
                request: request.identity.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                disruption: snapshot
                    .head
                    .disruption
                    .ok_or(FleetCatalogError::Conflict)?,
                preparation,
                node,
                child: request.gate.bytes(),
            };
            record.validate(self)?;
            if prior.as_ref().is_some_and(|old| *old != record) {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let mutation = record.mutation()?;
            mutation.validate()?;
            let committed = self.commit_inner(snapshot, &mutation).await?;
            Ok(record.admission(committed.applied_revision, committed.snapshot.revision()))
        })
        .await
    }

    /// The catalog owns the authenticated RPC. A successful return confirms
    /// this physical gate's bounded quiescence turn, never RestartReady or stop.
    pub async fn quiesce_restart(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartGateRequest,
        observer: &crate::application::consensus::RaftCells,
        timeout: Duration,
    ) -> Result<FleetRestartGateAdmission> {
        let deadline = super::super::deadline::Deadline::new(self, timeout)?;
        let admitted = self
            .admit_restart_gate(snapshot, request, deadline.remaining()?)
            .await?;
        let accepted = observer
            .request_peer_fleet_stop(
                admitted.quiesce_request()?,
                admitted.observed_revision(),
                deadline.remaining()?,
            )
            .await?;
        if accepted.action() != FleetStopAction::Quiesce
            || accepted.request() != request.gate.bytes()
            || accepted.operation() != request.identity.operation
            || accepted.process() != request.expected_process.bytes()
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        deadline.remaining()?;
        Ok(admitted)
    }

    async fn restart_gate_candidate(&self, request: &FleetRequestIdentity) -> Result<Option<Gate>> {
        let record = self
            .restart_gate_candidate_for_id(request.id, request.operation)
            .await?;
        if record
            .as_ref()
            .is_some_and(|record| record.request != *request)
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(record)
    }

    pub(super) async fn restart_gate_candidate_for_id(
        &self,
        id: [u8; 16],
        operation: [u8; 16],
    ) -> Result<Option<Gate>> {
        let Some((mutation, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(id)),
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
            .find(|c| c.table == FleetTable::Operations && c.key == Gate::key(operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Gate = decode(KIND, bytes, LIMIT)?;
        record.validate(self)?;
        if record.request.id != id
            || record.request.operation != operation
            || record.mutation()? != mutation
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    pub(super) async fn checked_restart_gate(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<Gate> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &Gate::key(operation),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let record: Gate = decode(KIND, &bytes, LIMIT)?;
        record.validate(self)?;
        if record.request.operation != operation || record.applied(self, snapshot).await?.is_none()
        {
            return Err(FleetCatalogError::Corrupt("unadmitted maintenance gate"));
        }
        let active = self
            .active_disruption(snapshot)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let Disruption::Restart(preparation) = active else {
            return Err(FleetCatalogError::Conflict);
        };
        if preparation != record.preparation
            || snapshot.head.disruption != Some(record.disruption)
            || self
                .host_from_record(
                    snapshot,
                    NodeId::new(record.node.node)
                        .map_err(|_| FleetCatalogError::Corrupt("gate node"))?,
                )
                .await?
                .record
                != record.node
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(record)
    }

    pub(in crate::raft::fleet_catalog) async fn verify_restart_gate_request(
        &self,
        snapshot: &FleetSnapshot,
        selector: &StopSelector,
        action: FleetStopAction,
    ) -> Result<()> {
        let record = self
            .checked_restart_gate(snapshot, selector.operation)
            .await?;
        if selector.purpose != LifecyclePurpose::Maintenance
            || record.selector(selector.request)? != *selector
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        match action {
            FleetStopAction::Quiesce if selector.request == record.child => Ok(()),
            FleetStopAction::Fence => Ok(()),
            _ => Err(FleetCatalogError::Invalid(
                "maintenance gate grants no stop",
            )),
        }
    }
}
