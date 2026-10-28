// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cancel exclusion without granting placement or erasing retained work.
use super::super::histories::validate_request;
use super::super::nodes::{NodeRecord, NODE_KIND};
use super::*;
use crate::raft::fleet_stop::FleetStopAction;
use cellar_logic::consensus::ProcessGeneration;
use serde::Deserialize;

const KIND: u8 = 75;
const LIMIT: usize = 4096;

/// Stable child fence identity. Retain it across a changed-root cancellation
/// retry: another ID at the same local fence epoch is not interchangeable.
#[derive(Clone, Debug)]
pub struct FleetEvacuationCancelFence {
    pub process: ProcessGeneration,
    pub request: ProposalId,
}

#[derive(Clone, Debug)]
pub struct FleetEvacuationCancelRequest {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub fence: Option<FleetEvacuationCancelFence>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fence {
    process: [u8; 16],
    request: [u8; 16],
    finalization: [u8; 16],
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    format: u8,
    catalog: [u8; 32],
    request: FleetRequestIdentity,
    input_root: [u8; 32],
    disruption: [u8; 32],
    from: FleetNodeState,
    fence: Option<Fence>,
    node: NodeRecord,
}

/// Journal-confirmed cancellation. The node is Ready at the original revision,
/// with no capacity grant. Existing data, bindings and resource debits survive.
pub struct FleetEvacuationCancellation {
    record: Record,
    applied: u64,
    observed: u64,
}

impl FleetEvacuationCancellation {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn node(&self) -> [u8; 16] {
        self.record.node.node
    }

    pub fn process(&self) -> Option<[u8; 16]> {
        self.record.fence.as_ref().map(|f| f.process)
    }

    pub fn fence_id(&self) -> Option<[u8; 16]> {
        self.record.fence.as_ref().map(|f| f.request)
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

fn key(operation: [u8; 16]) -> String {
    format!("evacuation-cancelled/{}", hex::encode(operation))
}

impl Record {
    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.node.validate(&catalog.inner)?;
        if self.format != 1
            || self.catalog != catalog.birth_owner()?.catalog
            || self.input_root == [0; 32]
            || self.disruption == [0; 32]
            || self.node.state != FleetNodeState::Ready
            || self.node.operation().is_some()
            || self.node.isolation.is_some()
            || self.node.epoch < 3
            || !matches!(
                self.from,
                FleetNodeState::Draining | FleetNodeState::Finalizing
            )
            || (self.from == FleetNodeState::Finalizing) != self.fence.is_some()
        {
            return Err(FleetCatalogError::Corrupt("evacuation cancellation"));
        }
        if let Some(fence) = &self.fence {
            for id in [fence.process, fence.request, fence.finalization] {
                ProposalId::new(id)
                    .map_err(|_| FleetCatalogError::Corrupt("cancellation fence"))?;
            }
            if self.node.boot.as_ref().map(|b| b.process) != Some(fence.process) {
                return Err(FleetCatalogError::Corrupt("cancellation process"));
            }
        }
        Ok(())
    }

    fn matches(&self, request: &FleetEvacuationCancelRequest) -> bool {
        self.request == request.identity
            && self.node.node == request.node.bytes()
            && match (&self.fence, &request.fence) {
                (None, None) => true,
                (Some(a), Some(b)) => {
                    a.process == b.process.bytes() && a.request == b.request.bytes()
                }
                _ => false,
            }
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
                    value: Some(encode(NODE_KIND, &self.node, LIMIT)?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: key(self.request.operation),
                    value: Some(encode(KIND, self, LIMIT)?),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: super::super::evacuation::SLOT_KEY.into(),
                    value: None,
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
        let (stored, _) = catalog
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(self.request.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing cancellation request"))?;
        if stored != mutation {
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
            return Err(FleetCatalogError::Corrupt("cancellation applied revision"));
        }
        Ok(applied)
    }

    fn admission(self, applied: u64, observed: u64) -> FleetEvacuationCancellation {
        FleetEvacuationCancellation {
            record: self,
            applied,
            observed,
        }
    }
}

impl FleetCatalog {
    /// Cancel a live exclusion. Before finalization no stop can be admitted.
    /// After finalization this method owns an authenticated exact-process fence
    /// RPC and requires its synced acknowledgement before the root CAS. Accepted
    /// stops cannot be cancelled. Ready is not Active or a capacity certificate.
    /// Exact committed retries need no observer; failed CAS retains all charges.
    pub async fn cancel_evacuation(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetEvacuationCancelRequest,
        observer: Option<&RaftCells>,
        timeout: Duration,
    ) -> Result<FleetEvacuationCancellation> {
        self.check_snapshot(snapshot)?;
        validate_request(&request.identity)?;
        let deadline = Deadline::new(self, timeout)?;
        let start = self
            .run(deadline.remaining()?, async {
                let current = self.inner.current().await?;
                let previous = self.cancellation_request(request).await?;
                if let Some(record) = &previous {
                    if let Some(applied) = record.applied(self, &current).await? {
                        return Ok(Err(record.clone().admission(applied, current.revision())));
                    }
                }
                if current.token != snapshot.token
                    || request.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let host = self.host_from_record(snapshot, request.node).await?;
                let mut node = host.record;
                let from = node.state;
                let dispatch = match from {
                    FleetNodeState::Draining => {
                        if request.fence.is_some() {
                            return Err(FleetCatalogError::Invalid(
                                "unfinalized cancellation has no stop fence",
                            ));
                        }
                        let active = self
                            .checked_active_evacuation(
                                snapshot,
                                ProposalId::new(request.identity.operation)
                                    .map_err(|_| FleetCatalogError::Invalid("operation"))?,
                            )
                            .await?;
                        if active.failure_boot().is_some() {
                            return Err(FleetCatalogError::Invalid(
                                "failure isolation cannot be cancelled",
                            ));
                        }
                        if active.admission().node() != request.node {
                            return Err(FleetCatalogError::PayloadConflict);
                        }
                        None
                    }
                    FleetNodeState::Finalizing => {
                        let fence = request.fence.as_ref().ok_or(FleetCatalogError::Invalid(
                            "finalized cancellation requires process and stable fence ID",
                        ))?;
                        let record = self
                            .stop_record(snapshot, request.identity.operation)
                            .await?;
                        if record.node != node
                            || node.boot.as_ref().map(|b| b.process) != Some(fence.process.bytes())
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                        Some(
                            crate::raft::fleet_stop::FleetStopRequest::new(
                                record.stop_selector(fence.request)?,
                                FleetStopAction::Fence,
                            )
                            .map_err(|_| FleetCatalogError::Corrupt("cancellation selector"))?,
                        )
                    }
                    _ => return Err(FleetCatalogError::Conflict),
                };
                let fence = dispatch.as_ref().map(|d| Fence {
                    process: d.selector.boot.process,
                    request: d.selector.request,
                    finalization: d.selector.finalization,
                });
                node.cancel_evacuation(request.identity.operation)?;
                let record = Record {
                    format: 1,
                    catalog: self.birth_owner()?.catalog,
                    request: request.identity.clone(),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    disruption: snapshot
                        .head
                        .disruption
                        .ok_or(FleetCatalogError::Conflict)?,
                    from,
                    fence,
                    node,
                };
                record.validate(self)?;
                if previous.as_ref().is_some_and(|old| *old != record) {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let mutation = record.mutation()?;
                mutation.validate()?;
                // Persist canonical intent before the local narrowing effect. A
                // lost fence response cannot rebind this ID to another boot/fence.
                self.inner
                    .create_exact(
                        &format!("requests/{}", hex::encode(mutation.id)),
                        &encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?,
                        REQUEST_LIMIT,
                    )
                    .await?;
                Ok(Ok((record, mutation, dispatch)))
            })
            .await?;
        let (record, mutation, dispatch) = match start {
            Ok(x) => x,
            Err(receipt) => return Ok(receipt),
        };
        if let Some(dispatch) = dispatch {
            let expected = dispatch.selector.clone();
            let accepted = observer
                .ok_or(FleetCatalogError::Invalid(
                    "cancellation requires a running observer",
                ))?
                .request_peer_fleet_stop(dispatch, snapshot.revision(), deadline.remaining()?)
                .await?;
            if accepted.action() != FleetStopAction::Fence
                || accepted.request() != expected.request
                || accepted.operation() != expected.operation
                || accepted.process() != expected.boot.process
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
        }
        self.run(deadline.remaining()?, async {
            let committed = self
                .commit_with_disruption(
                    snapshot,
                    &mutation,
                    Some(record.disruption),
                    super::super::resources::Authority::None,
                    || {
                        deadline.remaining()?;
                        Ok(())
                    },
                )
                .await?;
            let applied = record
                .applied(self, &committed.snapshot)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            deadline.remaining()?;
            Ok(record.admission(applied, committed.snapshot.revision()))
        })
        .await
    }

    async fn cancellation_request(
        &self,
        request: &FleetEvacuationCancelRequest,
    ) -> Result<Option<Record>> {
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
            .find(|c| c.table == FleetTable::Operations && c.key == key(request.identity.operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Record = decode(KIND, bytes, LIMIT)?;
        record.validate(self)?;
        if !record.matches(request) || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    /// Historical observation only. It cannot grant current eligibility or
    /// release a later operation's disruption slot.
    pub async fn evacuation_cancellation(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<FleetEvacuationCancellation>> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let Some(bytes) = self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &key(operation.bytes()),
                )
                .await?
            else {
                return Ok(None);
            };
            let record: Record = decode(KIND, &bytes, LIMIT)?;
            record.validate(self)?;
            if record.request.operation != operation.bytes() {
                return Err(FleetCatalogError::Corrupt("cancellation operation key"));
            }
            let applied = record
                .applied(self, snapshot)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            Ok(Some(record.admission(applied, snapshot.revision())))
        })
        .await
    }
}
