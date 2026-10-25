// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Typed readiness admission. An Active descriptor alone is not quantified
//! placement capacity; consumers require the current opaque authorization.
use super::histories::validate_request;
use super::nodes::NODE_KIND;
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::fleet_stop::BootBinding;
use crate::raft::host::RaftPeerNodeObservation;
use crate::raft::inspection::RaftNodeProfile;
use cellar_logic::consensus::NodeId;
use serde::Deserialize;

const KIND: u8 = 60;

/// Explicit deployment attestation, not a kernel observation. The authenticated
/// operator must retain the referenced evidence and revoke/requalify this exact
/// boot before changing its platform limits. No inferred or default limits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCapacityAttestation {
    pub boot: BootBinding,
    pub profile_revision: u64,
    pub profile_digest: [u8; 32],
    pub cpu_millis: u32,
    pub memory_bytes: u64,
    pub durable_bytes: u64,
    pub engine_slots: u32,
    pub memory_swap_disabled: bool,
    pub used_memory_bytes: u64,
    pub used_durable_bytes: u64,
    pub evidence_digest: [u8; 32],
    pub evidence_reference: String,
}

impl FleetCapacityAttestation {
    fn same_profile(&self, other: &Self) -> bool {
        self.profile_digest == other.profile_digest
            && self.cpu_millis == other.cpu_millis
            && self.memory_bytes == other.memory_bytes
            && self.durable_bytes == other.durable_bytes
            && self.engine_slots == other.engine_slots
            && self.memory_swap_disabled == other.memory_swap_disabled
    }

    fn validate(&self) -> Result<()> {
        self.boot
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("capacity boot"))?;
        if self.profile_revision == 0
            || self.profile_revision >= u64::MAX - 2
            || self.profile_digest == [0; 32]
            || self.cpu_millis == 0
            || self.cpu_millis > 1_024_000
            || self.memory_bytes == 0
            || self.durable_bytes == 0
            || !(1..=32_768).contains(&self.engine_slots)
            || !self.memory_swap_disabled
            || self.used_memory_bytes > self.memory_bytes
            || self.used_durable_bytes > self.durable_bytes
            || self.evidence_digest == [0; 32]
            || self.evidence_reference.is_empty()
            || self.evidence_reference.len() > 256
            || self.evidence_reference.chars().any(char::is_control)
        {
            return Err(FleetCatalogError::Invalid("capacity attestation"));
        }
        Ok(())
    }
}

/// Digest of the explicit running placement profile. Dynamic drain state is
/// included: qualification cannot reactivate an excluded or draining process.
pub fn fleet_capacity_profile_digest(profile: &RaftNodeProfile) -> Result<[u8; 32]> {
    profile
        .validate(4096, 1)
        .map_err(|_| FleetCatalogError::Invalid("capacity profile"))?;
    Ok(hash(
        &serde_json::to_vec(&crate::raft::inspection_wire::Profile::from(profile))
            .map_err(|_| FleetCatalogError::Invalid("capacity profile encoding"))?,
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FleetCapacityAction {
    Qualify,
    Activate,
}

impl FleetCapacityAction {
    fn state(self) -> FleetNodeState {
        match self {
            Self::Qualify => FleetNodeState::Ready,
            Self::Activate => FleetNodeState::Active,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCapacityRequest {
    pub identity: FleetRequestIdentity,
    pub attestation: FleetCapacityAttestation,
    pub action: FleetCapacityAction,
}

impl FleetCapacityRequest {
    fn validate(&self) -> Result<()> {
        validate_request(&self.identity)?;
        self.attestation.validate()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    format: u8,
    catalog: [u8; 32],
    request: FleetCapacityRequest,
    input_root: [u8; 32],
    node_digest: [u8; 32],
    epoch: u64,
}

impl Record {
    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        self.request.validate()?;
        if self.format != 1
            || self.catalog != catalog.birth_owner()?.catalog
            || self.catalog != self.request.attestation.boot.catalog
            || self.input_root == [0; 32]
            || self.node_digest == [0; 32]
            || self.epoch <= self.request.attestation.boot.epoch
            || self.epoch >= u64::MAX - 2
        {
            return Err(FleetCatalogError::Corrupt("capacity receipt"));
        }
        Ok(())
    }
}

fn key(node: [u8; 16]) -> String {
    format!("capacity/{}", hex::encode(node))
}

/// Historical acceptance only. A later boot, exclusion or resource change may
/// invalidate target eligibility; use authorize_capacity at each new decision.
pub struct FleetCapacityAdmission {
    record: Record,
    applied: u64,
    observed: u64,
}

impl FleetCapacityAdmission {
    pub fn node(&self) -> [u8; 16] {
        self.record.request.attestation.boot.node
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }

    pub fn state(&self) -> FleetNodeState {
        self.record.request.action.state()
    }
}

/// Quantified capacity at one exact current root/boot. This value grants no
/// membership, reservation, absence proof, future health lease or platform edit.
pub struct FleetCapacityAuthorization {
    record: Record,
    revision: u64,
}

impl FleetCapacityAuthorization {
    /// Attested platform bounds only. Each reservation must also use fresh
    /// occupancy and the running profile/startup bounds, which can be smaller.
    pub fn attestation(&self) -> &FleetCapacityAttestation {
        &self.record.request.attestation
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn receive_replicas(&self) -> bool {
        self.record.request.action == FleetCapacityAction::Activate
    }
}

impl FleetCatalog {
    /// Own the authenticated probe and complete inventory scan; caller-built or
    /// deserialized observation values cannot substitute for either. Exact
    /// committed retries reconcile before live inputs, with no hidden CAS retry.
    pub async fn admit_node_capacity(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetCapacityRequest,
        observer: &RaftCells,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetCapacityAdmission> {
        self.check_snapshot(snapshot)?;
        request.validate()?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        let deadline = super::deadline::Deadline::new(self, timeout)?;
        let start = self
            .run(deadline.remaining()?, async {
                let current = self.inner.current().await?;
                if let Some((mutation, record)) = self.capacity_request(request).await? {
                    if let Some(applied) = self.capacity_applied(&current, &mutation).await? {
                        return Ok(Err(FleetCapacityAdmission {
                            record,
                            applied,
                            observed: current.revision(),
                        }));
                    }
                }
                if current.token != snapshot.token
                    || request.identity.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.inner
                    .domain
                    .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)??;
                let node = NodeId::new(request.attestation.boot.node)
                    .map_err(|_| FleetCatalogError::Invalid("capacity node"))?;
                let target = self.host_from(snapshot, node).await?;
                if !matches!(
                    target.state(),
                    FleetNodeState::Registered | FleetNodeState::Ready | FleetNodeState::Active
                ) || target.boot_binding()? != request.attestation.boot
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.confirm_boot(snapshot, &target.record).await?;
                if let Some(previous) = self.rooted_capacity(snapshot, node).await? {
                    let old = &previous.request.attestation;
                    if request.attestation.profile_revision < old.profile_revision {
                        return Err(FleetCatalogError::Conflict);
                    }
                    if request.attestation.profile_revision == old.profile_revision
                        && !request.attestation.same_profile(old)
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    // An already qualified active process needs the maintenance
                    // protocol before changing limits. An intact new boot must
                    // qualify again, even if it reuses the process hint.
                    if target.state() == FleetNodeState::Active
                        && previous.epoch == target.epoch()
                        && old.boot == request.attestation.boot
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                Ok(Ok(target))
            })
            .await?;
        let target = match start {
            Ok(target) => target,
            Err(admission) => {
                deadline.remaining()?;
                return Ok(admission);
            }
        };
        let observation = observer
            .inspect_peer_node(target.node(), snapshot.revision(), deadline.remaining()?)
            .await
            .map_err(FleetCatalogError::Observation)?;
        deadline.remaining()?;
        check_observation(&target, &request.attestation, &observation)?;
        self.run(deadline.remaining()?, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            let mut node = target.record.clone();
            node.activate_capacity(request.action.state())?;
            let node_bytes = encode(NODE_KIND, &node, 4096)?;
            let record = Record {
                format: 1,
                catalog: self.birth_owner()?.catalog,
                request: request.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                node_digest: hash(&node_bytes),
                epoch: node.epoch,
            };
            record.validate(self)?;
            let mutation = FleetMutation {
                id: request.identity.id,
                operation: request.identity.operation,
                principal: request.identity.principal.clone(),
                expected_revision: request.identity.expected_revision,
                changes: vec![
                    FleetChange {
                        table: FleetTable::Nodes,
                        key: hex::encode(node.node),
                        value: Some(node_bytes),
                    },
                    FleetChange {
                        table: FleetTable::Operations,
                        key: key(node.node),
                        value: Some(encode(KIND, &record, 4096)?),
                    },
                ],
            };
            mutation.validate()?;
            let commit = self
                .commit_checked(snapshot, &mutation, || {
                    deadline.remaining()?;
                    Ok(())
                })
                .await?;
            let (_, confirmed) = self
                .capacity_request(request)
                .await?
                .ok_or(FleetCatalogError::Corrupt("missing capacity request"))?;
            let applied = self
                .capacity_applied(&commit.snapshot, &mutation)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            deadline.remaining()?;
            Ok(FleetCapacityAdmission {
                record: confirmed,
                applied,
                observed: commit.snapshot.revision(),
            })
        })
        .await
    }

    /// Read one current rooted proof. A changed boot/epoch/profile or missing
    /// journal link refuses even when generic metadata still says Active.
    pub async fn authorize_capacity(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
        timeout: Duration,
    ) -> Result<FleetCapacityAuthorization> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            let authorization = self.capacity_from(snapshot, node).await?;
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(authorization)
        })
        .await
    }

    // Called only inside an owned catalog turn. Callers retain the surrounding
    // current-root checks, avoiding nested turn acquisition during admission.
    pub(super) async fn capacity_from(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
    ) -> Result<FleetCapacityAuthorization> {
        let record = self
            .rooted_capacity(snapshot, node)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let target = self.host_from(snapshot, node).await?;
        if target.state() != record.request.action.state()
            || target.epoch() != record.epoch
            || target.boot_binding()? != record.request.attestation.boot
            || hash(&encode(NODE_KIND, &target.record, 4096)?) != record.node_digest
        {
            return Err(FleetCatalogError::Conflict);
        }
        self.confirm_boot(snapshot, &target.record).await?;
        Ok(FleetCapacityAuthorization {
            record,
            revision: snapshot.revision(),
        })
    }

    async fn capacity_applied(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<Option<u64>> {
        let digest = hash(&encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?);
        let applied = self
            .inner
            .committed(&snapshot.head, &hex::encode(mutation.id), digest)
            .await?;
        if applied
            .is_some_and(|revision| mutation.expected_revision.checked_add(1) != Some(revision))
        {
            return Err(FleetCatalogError::Corrupt("capacity applied revision"));
        }
        Ok(applied)
    }

    async fn rooted_capacity(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
    ) -> Result<Option<Record>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &key(node.bytes()),
            )
            .await?
        else {
            return Ok(None);
        };
        let record: Record = decode(KIND, &bytes, 4096)?;
        record.validate(self)?;
        let (mutation, original) = self
            .capacity_request(&record.request)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing capacity request"))?;
        if record.request.attestation.boot.node != node.bytes()
            || encode(KIND, &original, 4096)? != bytes
            || self.capacity_applied(snapshot, &mutation).await?.is_none()
        {
            return Err(FleetCatalogError::Corrupt("unadmitted capacity"));
        }
        Ok(Some(record))
    }

    async fn capacity_request(
        &self,
        request: &FleetCapacityRequest,
    ) -> Result<Option<(FleetMutation, Record)>> {
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
        if mutation.id != request.identity.id
            || mutation.operation != request.identity.operation
            || mutation.principal != request.identity.principal
            || mutation.expected_revision != request.identity.expected_revision
            || mutation.changes.len() != 2
            || mutation.changes[0].table != FleetTable::Nodes
            || mutation.changes[0].key != hex::encode(request.attestation.boot.node)
            || mutation.changes[1].table != FleetTable::Operations
            || mutation.changes[1].key != key(request.attestation.boot.node)
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let record: Record = decode(
            KIND,
            mutation.changes[1]
                .value
                .as_deref()
                .ok_or(FleetCatalogError::Corrupt("capacity receipt deletion"))?,
            4096,
        )?;
        record.validate(self)?;
        if record.request != *request {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let node_bytes = mutation.changes[0]
            .value
            .as_deref()
            .ok_or(FleetCatalogError::Corrupt("capacity node deletion"))?;
        let node: super::nodes::NodeRecord = decode(NODE_KIND, node_bytes, 4096)?;
        node.validate(&self.inner)?;
        if hash(node_bytes) != record.node_digest
            || node.epoch != record.epoch
            || node.state != request.action.state()
            || node.boot_binding(record.catalog)? != request.attestation.boot
        {
            return Err(FleetCatalogError::Corrupt("capacity mutation node"));
        }
        Ok(Some((mutation, record)))
    }
}

pub(super) fn check_observation(
    target: &FleetHostAuthorization,
    attestation: &FleetCapacityAttestation,
    sample: &RaftPeerNodeObservation,
) -> Result<()> {
    let view = &sample.view;
    let current = view
        .current_enrollment
        .as_ref()
        .ok_or(FleetCatalogError::Unconfirmed)?;
    let bounds = &view.startup_bounds;
    if view.fleet_boot.as_ref() != Some(&attestation.boot)
        || view.node.identity.node != target.node()
        || view.node.identity.disk != target.disk()
        || Some(view.node.storage_instance) != target.storage_instance()
        || current.fleet != target.fleet()
        || current.epoch != target.epoch()
        || current.state != target.state()
        || current.physical_host != target.physical_host()
        || current.capability_digest != target.capability_digest()
        || current.revision < target.revision()
        || view
            .fleet_trust_revision
            .is_none_or(|revision| revision < target.revision())
        || view.pending_history_registrations != 0
        || view.node.profile.draining
        || fleet_capacity_profile_digest(&view.node.profile)? != attestation.profile_digest
        || view.node.profile.group_capacity
            > bounds
                .host_groups
                .min(bounds.consensus_groups)
                .min(bounds.engine_groups)
        || view.node.profile.group_capacity > attestation.engine_slots
        || view.node.groups > attestation.engine_slots
        || view.node.reserved_bytes > attestation.durable_bytes
        || view.node.profile.byte_capacity > attestation.durable_bytes
        || bounds.bounded_buffer_bytes > attestation.memory_bytes
    {
        return Err(FleetCatalogError::Conflict);
    }
    Ok(())
}
