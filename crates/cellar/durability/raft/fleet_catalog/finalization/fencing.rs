// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A durable no-new-boot boundary, separate from physical stop and completion.
use super::super::histories::validate_request;
use super::super::nodes::{FleetHostAuthorization, NodeRecord, NODE_KIND};
use super::*;
use crate::raft::host::RaftPeerNodeObservation;
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, ProcessGeneration};
use serde::Deserialize;

const FENCE_KIND: u8 = 58;

fn key(operation: [u8; 16]) -> String {
    format!("finalizing/{}", hex::encode(operation))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    format: u8,
    catalog: [u8; 32],
    pub(super) request: FleetRequestIdentity,
    input_root: [u8; 32],
    pub(super) candidate: [u8; 32],
    pub(super) disruption: [u8; 32],
    pub(super) node: NodeRecord,
}

impl Record {
    pub(super) fn stop_selector(
        &self,
        request: ProposalId,
    ) -> Result<crate::raft::fleet_stop::StopSelector> {
        if self.node.isolation.is_some() {
            return Err(FleetCatalogError::Conflict);
        }
        let selector = crate::raft::fleet_stop::StopSelector {
            purpose: crate::raft::fleet_stop::LifecyclePurpose::Evacuation,
            boot: self.node.boot_binding(self.catalog)?,
            epoch: self.node.epoch,
            operation: self.request.operation,
            finalization: self.request.id,
            request: request.bytes(),
        };
        selector
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("stop selector"))?;
        Ok(selector)
    }

    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.node.validate(&catalog.inner)?;
        if self.format != 1
            || self.catalog != catalog.birth_owner()?.catalog
            || self.input_root == [0; 32]
            || self.candidate == [0; 32]
            || self.disruption == [0; 32]
            || self.node.state != FleetNodeState::Finalizing
            || self.node.operation() != Some(self.request.operation)
            || self.node.boot.is_none()
        {
            return Err(FleetCatalogError::Corrupt("finalization boot fence"));
        }
        Ok(())
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
                    key: key(self.request.operation),
                    value: Some(encode(FENCE_KIND, self, 4096)?),
                },
            ],
        })
    }

    async fn confirm(&self, catalog: &FleetCatalog, snapshot: &FleetSnapshot) -> Result<u64> {
        self.validate(catalog)?;
        let mutation = self.mutation()?;
        mutation.validate()?;
        let request_key = hex::encode(self.request.id);
        let (original, _) = catalog
            .inner
            .read::<FleetMutation>(
                &format!("requests/{request_key}"),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing finalization request"))?;
        if original != mutation {
            return Err(FleetCatalogError::Corrupt(
                "finalization request projection",
            ));
        }
        let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
        let applied = catalog
            .inner
            .committed(&snapshot.head, &request_key, digest)
            .await?
            .ok_or(FleetCatalogError::Corrupt("unadmitted finalization"))?;
        if self.request.expected_revision.checked_add(1) != Some(applied) {
            return Err(FleetCatalogError::Corrupt("finalization applied revision"));
        }
        let candidate = Candidate::read(catalog, self.candidate).await?;
        if candidate.basis.operation != self.request.operation
            || candidate.basis.node != self.node.node
            || candidate.basis.epoch.checked_add(1) != Some(self.node.epoch)
            || candidate.basis.slot() != self.disruption
        {
            return Err(FleetCatalogError::Corrupt("finalization candidate scope"));
        }
        catalog.confirm_boot(snapshot, &self.node).await?;
        if self.node.isolation.is_some() {
            catalog.confirm_node_isolation(snapshot, &self.node).await?;
        }
        Ok(applied)
    }

    fn admission(self, applied: u64, observed: u64) -> FleetFinalizationAdmission {
        FleetFinalizationAdmission {
            record: self,
            applied,
            observed,
        }
    }
}

/// A journal-confirmed no-new-boot transition for one exact physical process.
/// This historical observation grants no stop, terminal removal, physical
/// resource release, infrastructure deletion or disruption-slot release.
pub struct FleetFinalizationAdmission {
    record: Record,
    applied: u64,
    observed: u64,
}

impl FleetFinalizationAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    /// External isolation admission, when this fence uses deployment evidence.
    /// A fenced isolated node has no graceful stop selector or closure receipt.
    pub fn isolation_request(&self) -> Option<[u8; 16]> {
        self.record.node.isolation
    }

    /// Request data only. The receiving host must revalidate the rooted fence
    /// and persist its exact local guard before issuing any Node stop.
    pub fn stop_request(
        &self,
        request: ProposalId,
    ) -> Result<crate::raft::fleet_stop::FleetStopRequest> {
        crate::raft::fleet_stop::FleetStopRequest::new(
            self.record.stop_selector(request)?,
            crate::raft::fleet_stop::FleetStopAction::Stop,
        )
        .map_err(|_| FleetCatalogError::Corrupt("stop request"))
    }

    /// Fence delayed stops at this epoch. This does not cancel the rooted
    /// evacuation, restore eligibility or release the disruption slot.
    pub fn stop_fence_request(
        &self,
        request: ProposalId,
    ) -> Result<crate::raft::fleet_stop::FleetStopRequest> {
        crate::raft::fleet_stop::FleetStopRequest::new(
            self.record.stop_selector(request)?,
            crate::raft::fleet_stop::FleetStopAction::Fence,
        )
        .map_err(|_| FleetCatalogError::Corrupt("stop fence request"))
    }

    pub fn operation(&self) -> [u8; 16] {
        self.record.request.operation
    }

    pub fn candidate_digest(&self) -> [u8; 32] {
        self.record.candidate
    }

    pub fn node(&self) -> Result<NodeId> {
        NodeId::new(self.record.node.node)
            .map_err(|_| FleetCatalogError::Corrupt("finalization node"))
    }

    pub fn disk(&self) -> Result<DiskLineage> {
        self.record.node.disk_identity()
    }

    pub fn storage_instance(&self) -> Result<EngineInstanceId> {
        EngineInstanceId::new(
            self.record
                .node
                .storage_instance
                .ok_or(FleetCatalogError::Corrupt("finalization engine"))?,
        )
        .map_err(|_| FleetCatalogError::Corrupt("finalization engine"))
    }

    pub fn process(&self) -> Result<ProcessGeneration> {
        ProcessGeneration::new(
            self.record
                .node
                .boot
                .as_ref()
                .ok_or(FleetCatalogError::Corrupt("missing finalization boot"))?
                .process,
        )
        .map_err(|_| FleetCatalogError::Corrupt("finalization process"))
    }

    pub fn enrollment_epoch(&self) -> u64 {
        self.record.node.epoch
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

enum Start {
    Committed(Box<FleetFinalizationAdmission>),
    Ready(Box<FleetHostAuthorization>),
}

enum FinalizationSource<'a> {
    Process(&'a RaftCells),
    Isolation(ProposalId),
}

impl FinalizationSource<'_> {
    fn isolation(&self) -> Option<[u8; 16]> {
        match self {
            Self::Process(_) => None,
            Self::Isolation(id) => Some(id.bytes()),
        }
    }
}

impl FleetEvacuationCandidate {
    /// Fence a reachable process using an owned current process observation.
    pub async fn begin_finalization(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        observer: &RaftCells,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetFinalizationAdmission> {
        self.begin_finalization_inner(
            snapshot,
            request,
            FinalizationSource::Process(observer),
            namespaces,
            timeout,
        )
        .await
    }

    /// Fence an already quarantined exact boot using its admitted deployment
    /// isolation evidence. The candidate still requires complete inventory,
    /// survivor quorum and physical archive replay. No graceful stop selector
    /// or terminal removal is granted by this transition.
    pub async fn begin_isolated_finalization(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        isolation: ProposalId,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetFinalizationAdmission> {
        self.begin_finalization_inner(
            snapshot,
            request,
            FinalizationSource::Isolation(isolation),
            namespaces,
            timeout,
        )
        .await
    }

    /// Atomically fence further boots after complete inventory/candidate checks
    /// and an owned fresh process probe. Existing process work remains owned;
    /// a successful fence neither stops that process nor certifies completion.
    /// Exact committed retries reconcile without requiring live probe inputs.
    async fn begin_finalization_inner(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        source: FinalizationSource<'_>,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetFinalizationAdmission> {
        let catalog = &self.catalog;
        catalog.check_snapshot(snapshot)?;
        validate_request(request)?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        if request.operation != self.candidate.basis.operation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let deadline = Deadline::new(catalog, timeout)?;
        let start = catalog
            .run(deadline.remaining()?, async {
                let current = catalog.inner.current().await?;
                if let Some(record) = catalog.finalization_request(request).await? {
                    if record.candidate != self.digest
                        || record.node.isolation != source.isolation()
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    let mutation = record.mutation()?;
                    let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
                    if catalog
                        .inner
                        .committed(&current.head, &hex::encode(request.id), digest)
                        .await?
                        .is_some()
                    {
                        let applied = record.confirm(catalog, &current).await?;
                        return Ok(Start::Committed(Box::new(
                            record.admission(applied, current.revision()),
                        )));
                    }
                }
                if current.token != snapshot.token
                    || request.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.candidate.basis.check_current(catalog).await?;
                if catalog
                    .inner
                    .lookup(
                        current.head.tables[FleetTable::Operations.slot()],
                        &key(request.operation),
                    )
                    .await?
                    .is_some()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                catalog
                    .inner
                    .domain
                    .run(Box::pin(
                        catalog.check_inventory_inner(&current, namespaces),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)??;
                let node = NodeId::new(self.candidate.basis.node)
                    .map_err(|_| FleetCatalogError::Corrupt("candidate node"))?;
                let target = match &source {
                    FinalizationSource::Process(_) => catalog.host_from(&current, node).await?,
                    FinalizationSource::Isolation(id) => {
                        let target = catalog.host_from_record(&current, node).await?;
                        if target.record.isolation != Some(id.bytes()) {
                            return Err(FleetCatalogError::Conflict);
                        }
                        catalog
                            .confirm_node_isolation(&current, &target.record)
                            .await?;
                        target
                    }
                };
                catalog.confirm_boot(&current, &target.record).await?;
                Ok(Start::Ready(Box::new(target)))
            })
            .await?;
        let target = match start {
            Start::Committed(admission) => {
                deadline.remaining()?;
                return Ok(*admission);
            }
            Start::Ready(target) => target,
        };
        if let FinalizationSource::Process(observer) = source {
            let observation = observer
                .inspect_peer_node(target.node(), snapshot.revision(), deadline.remaining()?)
                .await
                .map_err(FleetCatalogError::Observation)?;
            deadline.remaining()?;
            check_process(&target, &observation)?;
        }
        catalog
            .run(deadline.remaining()?, async {
                let current = catalog.inner.current().await?;
                if current.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                self.candidate.basis.check_current(catalog).await?;
                let mut node = target.record.clone();
                node.begin_finalization(request.operation)?;
                let record = Record {
                    format: 1,
                    catalog: catalog.birth_owner()?.catalog,
                    request: request.clone(),
                    input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                    candidate: self.digest,
                    disruption: self.candidate.basis.slot(),
                    node,
                };
                record.validate(catalog)?;
                let mutation = record.mutation()?;
                mutation.validate()?;
                let committed = catalog
                    .commit_checked(snapshot, &mutation, || {
                        deadline.remaining()?;
                        Ok(())
                    })
                    .await?;
                let applied = record.confirm(catalog, &committed.snapshot).await?;
                deadline.remaining()?;
                Ok(record.admission(applied, committed.snapshot.revision()))
            })
            .await
    }
}

fn check_process(
    target: &FleetHostAuthorization,
    observed: &RaftPeerNodeObservation,
) -> Result<()> {
    let node = &observed.view;
    let boot = target
        .record
        .boot
        .as_ref()
        .ok_or(FleetCatalogError::Corrupt("finalization boot"))?;
    let current = node
        .current_enrollment
        .as_ref()
        .ok_or(FleetCatalogError::Unconfirmed)?;
    if node.node.identity.node != target.node()
        || node.fleet_boot.as_ref()
            != Some(
                &target
                    .record
                    .boot_binding(target.catalog.birth_owner()?.catalog)?,
            )
        || node.node.identity.disk != target.disk()
        || node.node.identity.process.bytes() != boot.process
        || Some(node.node.storage_instance) != target.storage_instance()
        || current.fleet != target.fleet()
        || current.epoch != target.epoch()
        || current.state != FleetNodeState::Draining
        || current.physical_host != target.physical_host()
        || current.capability_digest != target.capability_digest()
        || current.revision < target.revision()
        || node
            .fleet_trust_revision
            .is_none_or(|revision| revision < target.revision())
    {
        return Err(FleetCatalogError::Unconfirmed);
    }
    Ok(())
}

const OPERATOR_STOP_KIND: u8 = 74;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorStop {
    format: u8,
    catalog: [u8; 32],
    request: FleetRequestIdentity,
    node: [u8; 16],
    process: [u8; 16],
    finalization: [u8; 32],
}

impl FleetCatalog {
    /// Permanently bind human stop consent before RPC dispatch. This immutable
    /// record is descriptive, never a substitute for the receiver's current
    /// root/boot guard or a signed post-join closure. Every retry is canonical.
    pub async fn admit_operator_stop(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        node: NodeId,
        process: ProcessGeneration,
        timeout: Duration,
    ) -> Result<crate::raft::fleet_stop::FleetStopRequest> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        self.run(timeout, async {
            let bytes = self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &key(request.operation),
                )
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let record: Record = decode(FENCE_KIND, &bytes, 4096)?;
            record.validate(self)?;
            record.confirm(self, snapshot).await?;
            if record.node.node != node.bytes()
                || record.node.boot.as_ref().map(|b| b.process) != Some(process.bytes())
                || record.node.isolation.is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
            let consent = OperatorStop {
                format: 1,
                catalog: self.birth_owner()?.catalog,
                request: request.clone(),
                node: node.bytes(),
                process: process.bytes(),
                finalization: hash(&bytes),
            };
            let key = format!("operator-stops/{}", hex::encode(request.id));
            match self
                .inner
                .read::<OperatorStop>(&key, OPERATOR_STOP_KIND, 4096)
                .await?
            {
                Some((prior, _)) if prior != consent => {
                    return Err(FleetCatalogError::PayloadConflict)
                }
                Some(_) => (),
                None => {
                    if self.inner.current().await?.token != snapshot.token
                        || request.expected_revision != snapshot.revision()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    self.stop_record(snapshot, request.operation).await?;
                    self.inner
                        .create_exact(&key, &encode(OPERATOR_STOP_KIND, &consent, 4096)?, 4096)
                        .await?;
                }
            }
            crate::raft::fleet_stop::FleetStopRequest::new(
                record.stop_selector(
                    ProposalId::new(request.id)
                        .map_err(|_| FleetCatalogError::Invalid("stop ID"))?,
                )?,
                crate::raft::fleet_stop::FleetStopAction::Stop,
            )
            .map_err(|_| FleetCatalogError::Corrupt("operator stop selector"))
        })
        .await
    }

    pub(crate) fn closure_sink(
        &self,
        selector: &crate::raft::fleet_stop::StopSelector,
    ) -> Result<crate::raft::fleet_stop::Sink> {
        selector
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("stop selector"))?;
        let owner = self.birth_owner()?;
        if selector.boot.catalog != owner.catalog || selector.boot.fleet != self.inner.incarnation {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(crate::raft::fleet_stop::Sink {
            bucket: self.inner.bucket.clone(),
            key: self.inner.key(&format!(
                "closures/{}/{}",
                hex::encode(selector.boot.boot),
                hex::encode(selector.request)
            )),
        })
    }

    pub(crate) async fn verify_local_stop(
        &self,
        selector: &crate::raft::fleet_stop::StopSelector,
        action: crate::raft::fleet_stop::FleetStopAction,
        origin: Option<&crate::raft::fleet_stop::BootBinding>,
        timeout: Duration,
    ) -> Result<()> {
        selector
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("stop selector"))?;
        self.run(timeout, async {
            let current = self.inner.current().await?;
            if let Some(origin) = origin {
                origin
                    .validate()
                    .map_err(|_| FleetCatalogError::Invalid("stop origin"))?;
                let node = NodeId::new(origin.node)
                    .map_err(|_| FleetCatalogError::Invalid("stop origin node"))?;
                let sender = self.host_from_record(&current, node).await?;
                if !sender.permits_transport() || sender.boot_binding()? != *origin {
                    return Err(FleetCatalogError::Conflict);
                }
                self.confirm_boot(&current, &sender.record).await?;
            }
            if selector.purpose == crate::raft::fleet_stop::LifecyclePurpose::Maintenance {
                if action == crate::raft::fleet_stop::FleetStopAction::Stop {
                    self.verify_restart_stop_request(&current, selector).await?;
                } else {
                    self.verify_restart_gate_request(&current, selector, action)
                        .await?;
                }
                if self.inner.current().await?.token != current.token {
                    return Err(FleetCatalogError::Conflict);
                }
                return Ok(());
            }
            if action == crate::raft::fleet_stop::FleetStopAction::Quiesce {
                return Err(FleetCatalogError::Invalid(
                    "evacuation has no reversible gate",
                ));
            }
            let record = self.stop_record(&current, selector.operation).await?;
            let request = ProposalId::new(selector.request)
                .map_err(|_| FleetCatalogError::Invalid("stop request"))?;
            if record.stop_selector(request)? != *selector {
                return Err(FleetCatalogError::PayloadConflict);
            }
            if action == crate::raft::fleet_stop::FleetStopAction::Stop {
                Candidate::read(self, record.candidate)
                    .await?
                    .basis
                    .check_stop_inputs(self, &current)?;
            }
            if self.inner.current().await?.token != current.token {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn stop_record(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<Record> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &key(operation),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let record: Record = decode(FENCE_KIND, &bytes, 4096)?;
        record.validate(self)?;
        if record.request.operation != operation {
            return Err(FleetCatalogError::Corrupt("stop operation key"));
        }
        record.confirm(self, snapshot).await?;
        self.check_evacuation_slot(snapshot, operation, record.disruption)
            .await?;
        let node =
            NodeId::new(record.node.node).map_err(|_| FleetCatalogError::Corrupt("stop node"))?;
        if self.host_from_record(snapshot, node).await?.record != record.node {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(record)
    }

    /// Verify the boot-pinned signature produced by the actual surrounding host
    /// after its domain joins. This grants no terminal state/slot transition.
    pub async fn verify_physical_closure(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        request: ProposalId,
        timeout: Duration,
    ) -> Result<crate::raft::fleet_stop::VerifiedFleetClosure> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            let selector = self
                .stop_record(snapshot, operation.bytes())
                .await?
                .stop_selector(request)?;
            let proof = self.closure_sink(&selector)?.verify(&selector).await?;
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(proof)
        })
        .await
    }

    async fn finalization_request(&self, request: &FleetRequestIdentity) -> Result<Option<Record>> {
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
        let value = mutation
            .changes
            .iter()
            .find(|change| {
                change.table == FleetTable::Operations && change.key == key(request.operation)
            })
            .and_then(|change| change.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Record = decode(FENCE_KIND, value, 4096)?;
        record.validate(self)?;
        if record.request != *request || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    /// Historical receipt only, also after terminal removal. Retaining or
    /// reconstructing its stop data cannot bypass the receiver's current fence.
    pub async fn finalization_receipt(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<FleetFinalizationAdmission>> {
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
            let record: Record = decode(FENCE_KIND, &bytes, 4096)?;
            record.validate(self)?;
            if record.request.operation != operation.bytes() {
                return Err(FleetCatalogError::Corrupt("finalization key"));
            }
            let applied = record.confirm(self, snapshot).await?;
            Ok(Some(record.admission(applied, snapshot.revision())))
        })
        .await
    }

    /// Historical journal-confirmed boot fencing, including after adapter
    /// recreation. Current stop authorization and terminal proof are separate.
    pub async fn evacuation_finalization(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<FleetFinalizationAdmission> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let bytes = self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &key(operation.bytes()),
                )
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let record: Record = decode(FENCE_KIND, &bytes, 4096)?;
            record.validate(self)?;
            if record.request.operation != operation.bytes() {
                return Err(FleetCatalogError::Corrupt("finalization key"));
            }
            let applied = record.confirm(self, snapshot).await?;
            self.check_evacuation_slot(snapshot, operation.bytes(), record.disruption)
                .await?;
            Ok(record.admission(applied, snapshot.revision()))
        })
        .await
    }
}
