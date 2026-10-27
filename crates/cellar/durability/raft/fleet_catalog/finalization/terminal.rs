// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Consume runtime verification and distinct joined/isolation evidence in one CAS.
use super::super::histories::validate_request;
use super::super::nodes::{NodeRecord, NODE_KIND};
use super::*;
use crate::raft::inspection::InspectionError;
use serde::Deserialize;

const KIND: u8 = 63;
const LIMIT: usize = 4096;
const MAX_PROOF_AGE_MS: u64 = 60_000;

/// Exact terminal intent. The stop identifies a signed joined-owner closure;
/// request data and historical candidates alone cannot authorize completion.
#[derive(Clone, Debug)]
pub struct FleetRemovalRequest {
    pub identity: FleetRequestIdentity,
    pub stop: ProposalId,
}

/// Terminal isolated-node intent. This references the permanent deployment
/// isolation admission for the exact fenced boot. It is not a stop request.
#[derive(Clone, Debug)]
pub struct FleetIsolatedRemovalRequest {
    pub identity: FleetRequestIdentity,
    pub isolation: ProposalId,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum RemovalProof {
    Joined { request: [u8; 16] },
    Isolated { request: [u8; 16] },
}

impl RemovalProof {
    fn id(self) -> [u8; 16] {
        match self {
            Self::Joined { request } | Self::Isolated { request } => request,
        }
    }

    fn matches_node(self, node: &NodeRecord) -> bool {
        match self {
            Self::Joined { .. } => node.isolation.is_none(),
            Self::Isolated { request } => node.isolation == Some(request),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    format: u8,
    catalog: [u8; 32],
    request: FleetRequestIdentity,
    proof: RemovalProof,
    finalization: [u8; 16],
    input_root: [u8; 32],
    candidate: [u8; 32],
    proof_digest: [u8; 32],
    disruption: [u8; 32],
    node: NodeRecord,
}

/// Journal-confirmed historical removal. Engine/disk identity and its retained
/// data remain tombstoned. This never grants archive or volume deletion.
pub struct FleetRemovalAdmission {
    record: Record,
    applied: u64,
    observed: u64,
}

impl FleetRemovalAdmission {
    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn stop_id(&self) -> [u8; 16] {
        self.record.proof.id()
    }

    pub fn operation(&self) -> [u8; 16] {
        self.record.request.operation
    }

    pub fn node(&self) -> [u8; 16] {
        self.record.node.node
    }

    pub fn candidate_digest(&self) -> [u8; 32] {
        self.record.candidate
    }

    pub fn closure_digest(&self) -> [u8; 32] {
        self.record.proof_digest
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

/// Terminal acceptance using trusted deployment isolation. Its evidence digest
/// is distinct from a graceful joined-owner closure. Retained disk and engine
/// state remain tombstoned and cannot be reclaimed from this receipt.
pub struct FleetIsolatedRemovalAdmission {
    record: Record,
    applied: u64,
    observed: u64,
}

impl FleetIsolatedRemovalAdmission {
    pub fn operation(&self) -> [u8; 16] {
        self.record.request.operation
    }

    pub fn node(&self) -> [u8; 16] {
        self.record.node.node
    }

    pub fn candidate_digest(&self) -> [u8; 32] {
        self.record.candidate
    }

    pub fn isolation_request(&self) -> [u8; 16] {
        self.record.proof.id()
    }

    pub fn isolation_digest(&self) -> [u8; 32] {
        self.record.proof_digest
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

struct Completed {
    record: Record,
    applied: u64,
    observed: u64,
}

/// Waiting is produced only by read-only survivor inspection before the Root
/// mutation. An error from the mutation or its confirmation is never waiting.
pub(crate) enum FleetRemovalAttempt<T> {
    Complete(T),
    Waiting(InspectionError),
}

impl<T> FleetRemovalAttempt<T> {
    fn into_result(self) -> Result<T> {
        match self {
            Self::Complete(value) => Ok(value),
            Self::Waiting(error) => Err(FleetCatalogError::Observation(error)),
        }
    }

    fn preflight_failure(error: FleetCatalogError) -> Result<Self> {
        match error {
            FleetCatalogError::Observation(
                error @ (InspectionError::Unavailable
                | InspectionError::Unreachable
                | InspectionError::Overloaded
                | InspectionError::StalePeer),
            ) => Ok(Self::Waiting(error)),
            error => Err(error),
        }
    }
}

fn key(operation: [u8; 16]) -> String {
    format!("removed/{}", hex::encode(operation))
}

impl Record {
    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.node.validate(&catalog.inner)?;
        if self.format != 1
            || self.catalog != catalog.birth_owner()?.catalog
            || self.proof.id() == [0; 16]
            || !self.proof.matches_node(&self.node)
            || self.finalization == [0; 16]
            || [
                self.input_root,
                self.candidate,
                self.proof_digest,
                self.disruption,
            ]
            .contains(&[0; 32])
            || self.node.state != FleetNodeState::Removed
            || self.node.operation() != Some(self.request.operation)
            || self.node.boot.is_none()
        {
            return Err(FleetCatalogError::Corrupt("terminal removal"));
        }
        Ok(())
    }

    fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            expected_revision: self.request.expected_revision,
            principal: self.request.principal.clone(),
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
        mutation.validate()?;
        let request_key = hex::encode(self.request.id);
        let (stored, _) = catalog
            .inner
            .read::<FleetMutation>(
                &format!("requests/{request_key}"),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing removal request"))?;
        if stored != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let applied = catalog
            .inner
            .committed(
                &snapshot.head,
                &request_key,
                hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
            )
            .await?;
        if applied
            .is_some_and(|applied| self.request.expected_revision.checked_add(1) != Some(applied))
        {
            return Err(FleetCatalogError::Corrupt("removal applied revision"));
        }
        Ok(applied)
    }

    fn admission(self, applied: u64, observed: u64) -> Completed {
        Completed {
            record: self,
            applied,
            observed,
        }
    }
}

impl FleetEvacuationVerification {
    fn check_terminal_freshness(&self, now: u64) -> Result<()> {
        if self.book.basis.finalization.is_none() {
            return Err(FleetCatalogError::Conflict);
        }
        if self.oldest_sample.is_some_and(|sample| {
            now.checked_sub(sample)
                .is_none_or(|age| age > MAX_PROOF_AGE_MS)
        }) {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(())
    }
}

impl FleetCatalog {
    /// Read a journal-confirmed joined removal, without re-running or granting
    /// current physical proof. No tombstone permits deletion or identity reuse.
    pub async fn removal_receipt(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<FleetRemovalAdmission>> {
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
                return Err(FleetCatalogError::Corrupt("removal key"));
            }
            if !matches!(record.proof, RemovalProof::Joined { .. }) {
                return Err(FleetCatalogError::Conflict);
            }
            let applied = record
                .applied(self, snapshot)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            Ok(Some(FleetRemovalAdmission {
                record,
                applied,
                observed: snapshot.revision(),
            }))
        })
        .await
    }

    /// Remove only after complete runtime verification in Finalizing and an
    /// exact boot-pinned signed closure. Root receipt, node tombstone and shared
    /// disruption release are one CAS. Every physical reservation remains.
    ///
    /// Exact committed retries may omit verification and namespace handles;
    /// they reconcile the permanent journal before any live prerequisites.
    pub async fn complete_evacuation(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRemovalRequest,
        verification: Option<&mut FleetEvacuationVerification>,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetRemovalAdmission> {
        self.complete_evacuation_attempt(snapshot, request, verification, namespaces, timeout)
            .await?
            .into_result()
    }

    pub(crate) async fn complete_evacuation_attempt(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRemovalRequest,
        verification: Option<&mut FleetEvacuationVerification>,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetRemovalAttempt<FleetRemovalAdmission>> {
        match self
            .complete_removal_inner(
                snapshot,
                &request.identity,
                RemovalProof::Joined {
                    request: request.stop.bytes(),
                },
                verification,
                namespaces,
                timeout,
            )
            .await?
        {
            FleetRemovalAttempt::Complete(completed) => {
                Ok(FleetRemovalAttempt::Complete(FleetRemovalAdmission {
                    record: completed.record,
                    applied: completed.applied,
                    observed: completed.observed,
                }))
            }
            FleetRemovalAttempt::Waiting(error) => Ok(FleetRemovalAttempt::Waiting(error)),
        }
    }

    /// Remove a permanently isolated exact boot after complete current
    /// inventory, fresh survivor quorum and physical archive replay. The
    /// terminal receipt, retained node tombstone and disruption-slot release
    /// share one CAS. Exact committed retries require no live verification.
    pub async fn complete_isolated_evacuation(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetIsolatedRemovalRequest,
        verification: Option<&mut FleetEvacuationVerification>,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetIsolatedRemovalAdmission> {
        let completed = self
            .complete_removal_inner(
                snapshot,
                &request.identity,
                RemovalProof::Isolated {
                    request: request.isolation.bytes(),
                },
                verification,
                namespaces,
                timeout,
            )
            .await?
            .into_result()?;
        Ok(FleetIsolatedRemovalAdmission {
            record: completed.record,
            applied: completed.applied,
            observed: completed.observed,
        })
    }

    async fn complete_removal_inner(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        proof: RemovalProof,
        verification: Option<&mut FleetEvacuationVerification>,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetRemovalAttempt<Completed>> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        let deadline = Deadline::new(self, timeout)?;
        if let Some(admission) = self
            .run(deadline.remaining()?, async {
                let current = self.inner.current().await?;
                if let Some((mutation, _)) = self
                    .inner
                    .read::<FleetMutation>(
                        &format!("requests/{}", hex::encode(request.id)),
                        REQUEST_KIND,
                        REQUEST_LIMIT,
                    )
                    .await?
                {
                    mutation.validate()?;
                    let value = mutation
                        .changes
                        .iter()
                        .find(|change| {
                            change.table == FleetTable::Operations
                                && change.key == key(request.operation)
                        })
                        .and_then(|change| change.value.as_deref())
                        .ok_or(FleetCatalogError::PayloadConflict)?;
                    let record: Record = decode(KIND, value, LIMIT)?;
                    record.validate(self)?;
                    if record.request != *request
                        || record.proof != proof
                        || record.mutation()? != mutation
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    if let Some(applied) = record.applied(self, &current).await? {
                        return Ok(Some(record.admission(applied, current.revision())));
                    }
                }
                if current.token != snapshot.token
                    || request.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(None)
            })
            .await?
        {
            deadline.remaining()?;
            return Ok(FleetRemovalAttempt::Complete(admission));
        }
        let verification = verification.ok_or(FleetCatalogError::InventoryIncomplete)?;
        if !Arc::ptr_eq(&self.inner, &verification.catalog.inner)
            || verification.book.basis.operation != request.operation
            || verification.book.basis.finalization.is_none()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let candidate = verification
            .publish_candidate(deadline.remaining()?)
            .await?;
        verification.oldest_sample = match survivors::refresh_terminal(
            verification,
            &candidate.candidate,
            &deadline,
        )
        .await
        {
            Ok(sampled) => sampled,
            Err(error) => return FleetRemovalAttempt::preflight_failure(error),
        };
        // Archive work can outlast a health sample. Reprobe every survivor
        // process immediately before the terminal turn, including a controller
        // that also owns one of the surviving replicas.
        if let Some(observer) = &verification.observer {
            let local = observer
                .observe_node(deadline.remaining()?)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            for node in &verification.survivor_nodes {
                let target = self
                    .run(deadline.remaining()?, self.host_from(snapshot, *node))
                    .await?;
                let view = if *node == local.node.identity.node {
                    local.clone()
                } else {
                    match observer
                        .inspect_peer_node(*node, snapshot.revision(), deadline.remaining()?)
                        .await
                    {
                        Ok(observation) => observation.view,
                        Err(error) => {
                            return FleetRemovalAttempt::preflight_failure(
                                FleetCatalogError::Observation(error),
                            );
                        }
                    }
                };
                let current = view
                    .current_enrollment
                    .as_ref()
                    .ok_or(FleetCatalogError::Unconfirmed)?;
                if target.state() != FleetNodeState::Active
                    || view.fleet_boot.as_ref()
                        != Some(&target.record.boot_binding(self.birth_owner()?.catalog)?)
                    || view.node.identity.node != target.node()
                    || view.node.identity.disk != target.disk()
                    || Some(view.node.storage_instance) != target.storage_instance()
                    || current.fleet != target.fleet()
                    || current.epoch != target.epoch()
                    || current.state != FleetNodeState::Active
                    || current.physical_host != target.physical_host()
                    || current.capability_digest != target.capability_digest()
                    || current.revision < snapshot.revision()
                    || view
                        .fleet_trust_revision
                        .is_none_or(|cut| cut < snapshot.revision())
                {
                    return Err(FleetCatalogError::Unconfirmed);
                }
            }
        }
        self.run(deadline.remaining()?, async {
            let current = self.inner.current().await?;
            if current.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            verification.book.basis.check_current(self).await?;
            verification.book.basis.check(self, snapshot)?;
            let now = self
                .inner
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            verification.check_terminal_freshness(now)?;
            self.inner
                .domain
                .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                .await
                .map_err(|_| FleetCatalogError::Closed)??;
            let fence = self.stop_record(snapshot, request.operation).await?;
            if verification.book.basis.finalization != Some(fence.request.id) {
                return Err(FleetCatalogError::Conflict);
            }
            let proof_digest = match proof {
                RemovalProof::Joined { request } => {
                    let selector = fence.stop_selector(
                        ProposalId::new(request)
                            .map_err(|_| FleetCatalogError::Invalid("stop request"))?,
                    )?;
                    self.closure_sink(&selector)?
                        .verify(&selector)
                        .await?
                        .digest()
                }
                RemovalProof::Isolated { request } => {
                    if fence.node.isolation != Some(request) {
                        return Err(FleetCatalogError::Conflict);
                    }
                    self.confirm_node_isolation(snapshot, &fence.node)
                        .await?
                        .digest
                }
            };
            let mut node = fence.node;
            node.complete_removal(request.operation)?;
            let record = Record {
                format: 1,
                catalog: self.birth_owner()?.catalog,
                request: request.clone(),
                proof,
                finalization: fence.request.id,
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                candidate: candidate.digest,
                proof_digest,
                disruption: fence.disruption,
                node,
            };
            record.validate(self)?;
            let mutation = record.mutation()?;
            mutation.validate()?;
            let committed = self
                .commit_with_disruption(
                    snapshot,
                    &mutation,
                    Some(record.disruption),
                    super::super::resources::Authority::None,
                    || {
                        deadline.remaining()?;
                        let now = self
                            .inner
                            .domain
                            .mono_ms()
                            .map_err(|_| FleetCatalogError::Closed)?;
                        verification.check_terminal_freshness(now)
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
        .map(FleetRemovalAttempt::Complete)
    }
}
