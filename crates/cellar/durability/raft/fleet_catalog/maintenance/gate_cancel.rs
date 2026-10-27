// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical cancellation fencing precedes Ready and shared-slot release.
use super::super::{
    nodes::{NodeRecord, NODE_KIND},
    resources::Authority,
};
use super::gate::Gate;
use super::*;
use crate::raft::fleet_stop::{FleetStopAction, FleetStopRequest};
use serde::Deserialize;

const KIND: u8 = 79;
const LIMIT: usize = 4096;

#[derive(Clone, Debug)]
pub struct FleetRestartGateCancelRequest {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub expected_process: ProcessGeneration,
    /// Keep this child identity across fresh parent requests after a known CAS
    /// conflict. The synced fence at the same epoch cannot be rebound.
    pub fence: ProposalId,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancellation {
    format: u8,
    request: FleetRequestIdentity,
    input_root: [u8; 32],
    // Resolve the immutable canonical parent without nesting it in a 4-KiB row.
    gate_parent: [u8; 16],
    gate_digest: [u8; 32],
    fence: [u8; 16],
    node: NodeRecord,
}

impl Cancellation {
    fn key(operation: [u8; 16]) -> String {
        format!("restart-gate-cancelled/{}", hex::encode(operation))
    }

    fn validate(&self, catalog: &FleetCatalog, gate: &Gate) -> Result<()> {
        validate_request(&self.request)?;
        gate.validate(catalog)?;
        self.node.validate(&catalog.inner)?;
        ProposalId::new(self.fence)
            .map_err(|_| FleetCatalogError::Corrupt("restart cancellation fence"))?;
        let mut expected = gate.node.clone();
        expected.cancel_restart(self.request.operation)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.node != expected
            || self.gate_parent != gate.request.id
            || self.gate_digest != gate.digest()?
            || self.request.operation != gate.request.operation
            || self.request.expected_revision <= gate.request.expected_revision
        {
            return Err(FleetCatalogError::Corrupt("restart gate cancellation"));
        }
        Ok(())
    }

    async fn checked_gate(&self, catalog: &FleetCatalog) -> Result<Gate> {
        let gate = catalog
            .restart_gate_candidate_for_id(self.gate_parent, self.request.operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing cancellation gate"))?;
        self.validate(catalog, &gate)?;
        Ok(gate)
    }

    fn matches(&self, request: &FleetRestartGateCancelRequest) -> bool {
        self.request == request.identity
            && self.node.node == request.node.bytes()
            && self.node.boot.as_ref().map(|b| b.process) == Some(request.expected_process.bytes())
            && self.fence == request.fence.bytes()
    }

    fn mutation(&self) -> Result<FleetMutation> {
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
                FleetChange {
                    table: FleetTable::Policy,
                    key: super::super::disruption::SLOT_KEY.into(),
                    value: None,
                },
            ],
        })
    }
}

impl FleetCatalog {
    /// Own the synced exact-process fence before releasing the operation. An
    /// unknown response retries the identical parent and child. A definite CAS
    /// loss needs a fresh parent revision/ID and the same child fence.
    pub async fn cancel_restart_gate(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRestartGateCancelRequest,
        observer: Option<&crate::application::consensus::RaftCells>,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(&request.identity)?;
        let deadline = super::super::deadline::Deadline::new(self, timeout)?;
        let start = self
            .run(deadline.remaining()?, async {
                let current = self.inner.current().await?;
                let prior = self.restart_gate_cancellation(request).await?;
                if let Some(record) = &prior {
                    let mutation = record.mutation()?;
                    if let Some(applied_revision) = self
                        .inner
                        .committed(
                            &current.head,
                            &hex::encode(request.identity.id),
                            hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
                        )
                        .await?
                    {
                        return Ok(Err(FleetCommit {
                            snapshot: current,
                            applied_revision,
                        }));
                    }
                }
                if current.token != snapshot.token
                    || request.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let gate = self
                    .checked_restart_gate(snapshot, request.identity.operation)
                    .await?;
                if gate.node.node != request.node.bytes()
                    || gate.node.boot.as_ref().map(|b| b.process)
                        != Some(request.expected_process.bytes())
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let mut node = gate.node.clone();
                node.cancel_restart(request.identity.operation)?;
                let record = Cancellation {
                    format: 1,
                    request: request.identity.clone(),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    gate_parent: gate.request.id,
                    gate_digest: gate.digest()?,
                    fence: request.fence.bytes(),
                    node,
                };
                record.validate(self, &gate)?;
                if prior.as_ref().is_some_and(|old| *old != record) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let mutation = record.mutation()?;
                mutation.validate()?;
                self.inner
                    .create_exact(
                        &format!("requests/{}", hex::encode(request.identity.id)),
                        &encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?,
                        REQUEST_LIMIT,
                    )
                    .await?;
                Ok(Ok((record, mutation, gate)))
            })
            .await?;
        let (record, mutation, gate) = match start {
            Ok(work) => work,
            Err(committed) => return Ok(committed),
        };
        let fence = FleetStopRequest::new(gate.selector(record.fence)?, FleetStopAction::Fence)
            .map_err(|_| FleetCatalogError::Corrupt("restart cancellation selector"))?;
        let accepted = observer
            .ok_or(FleetCatalogError::Invalid(
                "restart cancellation requires observer",
            ))?
            .request_peer_fleet_stop(fence, snapshot.revision(), deadline.remaining()?)
            .await?;
        if accepted.action() != FleetStopAction::Fence
            || accepted.request() != record.fence
            || accepted.operation() != request.identity.operation
            || accepted.process() != request.expected_process.bytes()
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        self.run(
            deadline.remaining()?,
            self.commit_with_disruption(
                snapshot,
                &mutation,
                Some(gate.disruption),
                Authority::None,
                || {
                    deadline.remaining()?;
                    Ok(())
                },
            ),
        )
        .await
    }

    async fn restart_gate_cancellation(
        &self,
        request: &FleetRestartGateCancelRequest,
    ) -> Result<Option<Cancellation>> {
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
        mutation.validate()?;
        let bytes = mutation
            .changes
            .iter()
            .find(|c| {
                c.table == FleetTable::Operations
                    && c.key == Cancellation::key(request.identity.operation)
            })
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Cancellation = decode(KIND, bytes, LIMIT)?;
        record.checked_gate(self).await?;
        if !record.matches(request) || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }
}
