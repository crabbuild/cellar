//! Inventory-consuming exclusion admission. This does not authorize shutdown,
//! certify node absence or release any membership/physical reservation.
use super::histories::{history_mutation, validate_request};
use super::nodes::{NodeRecord, NODE_KIND};
use super::*;
use crate::raft::catalog::NamespaceCatalog;
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, NodeId, ProposalId};
use serde::Deserialize;

pub(super) use super::disruption::SLOT_KEY;

pub(super) const ADMISSION_KIND: u8 = 53;
const OPERATION_REQUEST_KIND: u8 = 54;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum OperationKind {
    Evacuate,
    Restart {
        process: [u8; 16],
        grace_seconds: u16,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CanonicalRequest {
    format: u8,
    kind: OperationKind,
    catalog: [u8; 32],
    request: FleetRequestIdentity,
    node: [u8; 16],
}

impl CanonicalRequest {
    fn new(catalog: &FleetCatalog, request: &FleetRequestIdentity, node: [u8; 16]) -> Result<Self> {
        Ok(Self {
            format: 1,
            kind: OperationKind::Evacuate,
            catalog: catalog.birth_owner()?.catalog,
            request: request.clone(),
            node,
        })
    }

    pub(super) fn restart(
        catalog: &FleetCatalog,
        request: &FleetRequestIdentity,
        node: [u8; 16],
        process: [u8; 16],
        grace_seconds: u16,
    ) -> Result<Self> {
        let mut canonical = Self::new(catalog, request, node)?;
        canonical.kind = OperationKind::Restart {
            process,
            grace_seconds,
        };
        Ok(canonical)
    }

    pub(super) fn bytes(&self) -> Result<Vec<u8>> {
        encode(OPERATION_REQUEST_KIND, self, 4096)
    }

    pub(super) fn key(&self) -> String {
        format!("operation-requests/{}", hex::encode(self.request.operation))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    format: u8,
    catalog: [u8; 32],
    request: FleetRequestIdentity,
    input_root: [u8; 32],
    namespaces: usize,
    histories: u64,
    node: NodeRecord,
}

/// A rooted exclusion admission at its original revision. This is not a
/// restart-ready, evacuation-complete or infrastructure-removal certificate.
#[derive(Clone, Debug)]
pub struct FleetEvacuationAdmission {
    operation: ProposalId,
    node: NodeId,
    disk: DiskLineage,
    storage_instance: EngineInstanceId,
    enrollment_epoch: u64,
    inventory_revision: u64,
    namespaces: usize,
    histories: u64,
    applied_revision: u64,
    observed_revision: u64,
}

/// An active, still-matching disruption at one exact root. Unlike a retained
/// admission observation, this cannot be obtained from an old Operations entry.
pub(super) struct ActiveEvacuation {
    admission: FleetEvacuationAdmission,
    slot: [u8; 32],
}

impl ActiveEvacuation {
    pub(super) fn admission(&self) -> &FleetEvacuationAdmission {
        &self.admission
    }

    pub(super) fn slot(&self) -> [u8; 32] {
        self.slot
    }
}

impl FleetEvacuationAdmission {
    pub fn operation(&self) -> ProposalId {
        self.operation
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    pub fn disk(&self) -> DiskLineage {
        self.disk
    }

    pub fn storage_instance(&self) -> EngineInstanceId {
        self.storage_instance
    }

    pub fn enrollment_epoch(&self) -> u64 {
        self.enrollment_epoch
    }

    pub fn inventory_revision(&self) -> u64 {
        self.inventory_revision
    }

    pub fn namespaces(&self) -> usize {
        self.namespaces
    }

    pub fn histories(&self) -> u64 {
        self.histories
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied_revision
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed_revision
    }
}

impl Record {
    pub(super) fn operation(&self) -> [u8; 16] {
        self.request.operation
    }

    fn observation(
        &self,
        catalog: &FleetCatalog,
        snapshot: &FleetSnapshot,
        applied_revision: u64,
    ) -> Result<FleetEvacuationAdmission> {
        let host = catalog.host_record(snapshot, self.node.clone())?;
        Ok(FleetEvacuationAdmission {
            operation: ProposalId::new(self.request.operation)
                .map_err(|_| FleetCatalogError::Corrupt("evacuation operation"))?,
            node: host.node(),
            disk: host.disk(),
            storage_instance: host
                .storage_instance()
                .ok_or(FleetCatalogError::Corrupt("evacuation engine"))?,
            enrollment_epoch: host.epoch(),
            inventory_revision: self.request.expected_revision,
            namespaces: self.namespaces,
            histories: self.histories,
            applied_revision,
            observed_revision: snapshot.revision(),
        })
    }

    pub(super) fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.node.validate(&catalog.inner)?;
        if self.format != 1
            || self.catalog != catalog.birth_owner()?.catalog
            || self.input_root == [0; 32]
            || !(1..=namespaces::MAX_NAMESPACES).contains(&self.namespaces)
            || self.histories > HISTORY_LIMIT
            || self.node.state != FleetNodeState::Draining
            || self.node.operation() != Some(self.request.operation)
            || self.node.storage_instance.is_none()
            || self.request.expected_revision.checked_add(1).is_none()
        {
            return Err(FleetCatalogError::Corrupt("evacuation admission"));
        }
        Ok(())
    }

    pub(super) fn mutation(&self) -> Result<FleetMutation> {
        let bytes = encode(ADMISSION_KIND, self, 4096)?;
        Ok(history_mutation(
            &self.request,
            vec![
                FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(self.node.node),
                    value: Some(encode(NODE_KIND, &self.node, 4096)?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: hex::encode(self.request.operation),
                    value: Some(bytes.clone()),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: SLOT_KEY.into(),
                    value: Some(bytes),
                },
            ],
        ))
    }

    pub(super) fn check_basis(&self, snapshot: &FleetSnapshot) -> Result<()> {
        if self.request.expected_revision != snapshot.revision()
            || self.input_root != hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?)
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }
}

impl FleetCatalog {
    pub(super) async fn check_evacuation_slot(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
        slot: [u8; 32],
    ) -> Result<()> {
        let record = self
            .active_disruption(snapshot)
            .await?
            .ok_or(FleetCatalogError::Conflict)?
            .evacuation()?;
        if record.request.operation != operation || snapshot.head.disruption != Some(slot) {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    /// Admit exclusion of an Active node. The complete rooted namespace and
    /// history inventory is reread inside this turn before one CAS records the
    /// operation, claims the disruption slot and changes Active to Draining.
    /// No serving group is stopped, removed or declared absent by this method.
    /// Retry an unknown result with exactly the original request and node.
    pub async fn begin_evacuation(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        node: NodeId,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        self.run(timeout, async {
            self.inner
                .domain
                .run(Box::pin(self.begin_evacuation_inner(
                    snapshot, request, node, namespaces, false,
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
        })
        .await
    }

    /// Operator-controlled graceful departure requires the current completed
    /// placement to exclude this node. Exact committed retries reconcile first;
    /// the result remains historical exclusion, never removal or stop authority.
    pub async fn begin_evacuation_after_placement(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        node: NodeId,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        self.run(timeout, async {
            self.inner
                .domain
                .run(Box::pin(self.begin_evacuation_inner(
                    snapshot, request, node, namespaces, true,
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
        })
        .await
    }

    async fn begin_evacuation_inner(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        node: NodeId,
        namespaces: &[NamespaceCatalog],
        require_placement: bool,
    ) -> Result<FleetCommit> {
        let current = self.inner.current().await?;
        let canonical = CanonicalRequest::new(self, request, node.bytes())?;
        let canonical_present = self.check_operation_request(&canonical).await?;
        let prior = self.evacuation_candidate(request).await?;
        if let Some((record, mutation)) = &prior {
            if !canonical_present {
                return Err(FleetCatalogError::Corrupt(
                    "missing canonical evacuation request",
                ));
            }
            if record.node.node != node.bytes() {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let digest = hash(&encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?);
            if self
                .inner
                .committed(&current.head, &hex::encode(request.id), digest)
                .await?
                .is_some()
            {
                // Reconcile a committed admission without issuing another
                // exclusion, or demanding live inputs for an old observation.
                return self.commit_inner(snapshot, mutation).await;
            }
        }
        if current.token != snapshot.token || request.expected_revision != snapshot.revision() {
            return Err(FleetCatalogError::Conflict);
        }
        if self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &hex::encode(request.operation),
            )
            .await?
            .is_some()
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        if let Some(record) = self.active_disruption(snapshot).await? {
            return Err(FleetCatalogError::DisruptionInProgress {
                operation: record.operation(),
            });
        }
        if require_placement {
            self.check_departure_placement(snapshot, node).await?;
        }
        let inventory = self.check_inventory_inner(snapshot, namespaces).await?;
        let mut next = self
            .node_records(snapshot)
            .await?
            .into_iter()
            .find(|n| n.node == node.bytes())
            .ok_or(FleetCatalogError::Conflict)?;
        next.begin_evacuation(request.operation)?;
        next.validate(&self.inner)?;
        let record = Record {
            format: 1,
            catalog: self.birth_owner()?.catalog,
            request: request.clone(),
            input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
            namespaces: inventory.namespaces(),
            histories: inventory.histories(),
            node: next,
        };
        record.validate(self)?;
        let mutation = record.mutation()?;
        mutation.validate()?;
        if let Some((previous, original)) = prior {
            previous.check_basis(snapshot)?;
            if previous != record || original != mutation {
                return Err(FleetCatalogError::PayloadConflict);
            }
        }
        // This permanent identity precedes root admission. A losing CAS leaves
        // no exclusion authority, but cannot rebind the same operation ID to a
        // new preview, principal, mutation identity or node.
        self.inner
            .create_exact(
                &canonical.key(),
                &encode(OPERATION_REQUEST_KIND, &canonical, 4096)?,
                4096,
            )
            .await?;
        self.commit_inner(snapshot, &mutation).await
    }

    pub(super) async fn check_operation_request(
        &self,
        expected: &CanonicalRequest,
    ) -> Result<bool> {
        let found = self
            .inner
            .read::<CanonicalRequest>(&expected.key(), OPERATION_REQUEST_KIND, 4096)
            .await?;
        match found {
            Some((actual, _)) if actual == *expected => Ok(true),
            Some(_) => Err(FleetCatalogError::PayloadConflict),
            None => Ok(false),
        }
    }

    async fn evacuation_candidate(
        &self,
        request: &FleetRequestIdentity,
    ) -> Result<Option<(Record, FleetMutation)>> {
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
            .find(|c| c.table == FleetTable::Operations && c.key == hex::encode(request.operation))
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Record = decode(ADMISSION_KIND, value, 4096)?;
        record.validate(self)?;
        if record.request != *request || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some((record, mutation)))
    }

    pub(super) async fn confirm_evacuation(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
    ) -> Result<u64> {
        let canonical = CanonicalRequest::new(self, &record.request, record.node.node)?;
        if !self.check_operation_request(&canonical).await? {
            return Err(FleetCatalogError::Corrupt(
                "missing canonical evacuation request",
            ));
        }
        let Some((original, mutation)) = self.evacuation_candidate(&record.request).await? else {
            return Err(FleetCatalogError::Corrupt("missing evacuation request"));
        };
        if original != *record {
            return Err(FleetCatalogError::Corrupt("evacuation request projection"));
        }
        let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
        let applied = self
            .inner
            .committed(&snapshot.head, &hex::encode(record.request.id), digest)
            .await?
            .ok_or(FleetCatalogError::Corrupt(
                "unadmitted evacuation projection",
            ))?;
        if record.request.expected_revision.checked_add(1) != Some(applied) {
            return Err(FleetCatalogError::Corrupt("evacuation applied revision"));
        }
        Ok(applied)
    }

    /// Called within a catalog turn. The immutable request/journal and slot
    /// must agree, and the current node must still be the admitted Draining
    /// record. A historical admission alone cannot start final verification.
    pub(super) async fn checked_active_evacuation(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
    ) -> Result<ActiveEvacuation> {
        self.check_snapshot(snapshot)?;
        let record = self
            .active_disruption(snapshot)
            .await?
            .ok_or(FleetCatalogError::Conflict)?
            .evacuation()?;
        if record.request.operation != operation.bytes() {
            return Err(FleetCatalogError::Conflict);
        }
        let current = self
            .node_records(snapshot)
            .await?
            .into_iter()
            .find(|node| node.node == record.node.node)
            .ok_or(FleetCatalogError::Conflict)?;
        if !current.draining_successor(&record.node) {
            return Err(FleetCatalogError::Conflict);
        }
        if current != record.node {
            if current.isolation.is_some() {
                self.confirm_node_isolation(snapshot, &current).await?;
            } else {
                let boot = current.boot.as_ref().ok_or(FleetCatalogError::Conflict)?;
                if current.epoch != boot.epoch || boot.epoch <= record.node.epoch {
                    return Err(FleetCatalogError::Conflict);
                }
                self.confirm_boot(snapshot, &current).await?;
            }
        }
        // active_disruption already checked this exact original journal link.
        let applied = record
            .request
            .expected_revision
            .checked_add(1)
            .ok_or(FleetCatalogError::Limit)?;
        let mut admission = record.observation(self, snapshot, applied)?;
        admission.enrollment_epoch = current.epoch;
        Ok(ActiveEvacuation {
            admission,
            slot: snapshot
                .head
                .disruption
                .ok_or(FleetCatalogError::Corrupt("missing active disruption"))?,
        })
    }

    /// Inspect the retained original admission. Its old checked inventory and
    /// node epoch grant no lease, shutdown permission or removal certificate.
    pub async fn evacuation_admission(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<FleetEvacuationAdmission> {
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
            let record: Record = decode(ADMISSION_KIND, &bytes, 4096)?;
            record.validate(self)?;
            if record.request.operation != operation.bytes() {
                return Err(FleetCatalogError::Corrupt("evacuation operation key"));
            }
            let applied_revision = self.confirm_evacuation(snapshot, &record).await?;
            record.observation(self, snapshot, applied_revision)
        })
        .await
    }
}
