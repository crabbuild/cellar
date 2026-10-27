// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Permanent boot quarantine from trusted deployment isolation evidence.
//! This is distinct from a signed graceful-shutdown/physical-join receipt.

use super::nodes::{NodeRecord, NODE_KIND};
use super::*;
use crate::raft::fleet_stop::BootBinding;
use cellar_logic::consensus::{NodeId, ProposalId};
use serde::Deserialize;

const KIND: u8 = 65;
const PREFIX: &str = "node-isolation/";

/// Trusted deployment assertion: this exact process and all its execution
/// workers have stopped, and the platform prevents it from restarting. The
/// caller must authenticate that evidence through its deployment adapter before
/// submission. Cellar validates scope/journaling, not the external platform.
/// Network timeout, missing heartbeat or an advisory health sample is not this
/// evidence. Retain the volume and engine; this grants no storage reclamation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetIsolationAttestation {
    pub boot: BootBinding,
    pub evidence_digest: [u8; 32],
    pub evidence_reference: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetNodeIsolationRequest {
    pub identity: FleetRequestIdentity,
    pub attestation: FleetIsolationAttestation,
}

impl FleetNodeIsolationRequest {
    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        histories::validate_request(&self.identity)?;
        let a = &self.attestation;
        a.boot.validate().map_err(|_| invalid())?;
        if a.boot.catalog != catalog.birth_owner()?.catalog
            || a.boot.fleet != catalog.inner.incarnation
            || a.evidence_digest == [0; 32]
            || a.evidence_reference.is_empty()
            || a.evidence_reference.len() > 256
            || a.evidence_reference.chars().any(char::is_control)
        {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Historical acceptance of external evidence and permanent boot quarantine.
/// It is neither a graceful shutdown receipt nor an evacuation certificate.
pub struct FleetNodeIsolation {
    boot: BootBinding,
    applied: u64,
    observed: u64,
}

impl FleetNodeIsolation {
    pub fn boot(&self) -> &BootBinding {
        &self.boot
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

pub(super) struct IsolationProof {
    pub boot: BootBinding,
    pub digest: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    format: u8,
    request: FleetNodeIsolationRequest,
    input_root: [u8; 32],
    node: NodeRecord,
}

fn invalid() -> FleetCatalogError {
    FleetCatalogError::Invalid("node isolation")
}

fn key(node: [u8; 16]) -> String {
    format!("{PREFIX}{}", hex::encode(node))
}

impl Record {
    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        self.request.validate(catalog)?;
        self.node.validate(&catalog.inner)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.node.isolation != Some(self.request.identity.id)
            || self.node.operation() != Some(self.request.identity.operation)
            || self.node.state != FleetNodeState::Draining
            || self.node.boot_binding(catalog.birth_owner()?.catalog)?
                != self.request.attestation.boot
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.identity.id,
            operation: self.request.identity.operation,
            principal: self.request.identity.principal.clone(),
            expected_revision: self.request.identity.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: key(self.node.node),
                    value: Some(encode(KIND, self, 4096)?),
                },
                FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(self.node.node),
                    value: Some(encode(NODE_KIND, &self.node, 4096)?),
                },
            ],
        })
    }
}

impl FleetCatalog {
    /// Record external isolation after a rooted evacuation exclusion. This
    /// does not stop a process. The caller supplies already-established platform
    /// evidence; one CAS pins it and closes further boot/transport admissions.
    /// Keep the old physical volume and resources. Exact committed retries
    /// reconcile before current node or disruption prerequisites.
    pub async fn attest_node_isolation(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetNodeIsolationRequest,
        timeout: Duration,
    ) -> Result<FleetNodeIsolation> {
        self.check_snapshot(snapshot)?;
        request.validate(self)?;
        self.run(timeout, async {
            let current = self.inner.current().await?;
            let previous = self.isolation_request(request).await?;
            if let Some(record) = &previous {
                if let Some(applied) = self.isolation_applied(&current, record).await? {
                    return Ok(FleetNodeIsolation {
                        boot: request.attestation.boot.clone(),
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
            let active = self
                .checked_active_evacuation(
                    snapshot,
                    ProposalId::new(request.identity.operation).map_err(|_| invalid())?,
                )
                .await?;
            if active.admission().node().bytes() != request.attestation.boot.node {
                return Err(FleetCatalogError::Conflict);
            }
            let mut node = self
                .host_from(snapshot, active.admission().node())
                .await?
                .record;
            if node.state != FleetNodeState::Draining
                || node.boot_binding(self.birth_owner()?.catalog)? != request.attestation.boot
                || node.isolation.is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
            node.epoch = node.epoch.checked_add(1).ok_or(FleetCatalogError::Limit)?;
            node.isolation = Some(request.identity.id);
            let record = Record {
                format: 1,
                request: request.clone(),
                input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                node,
            };
            record.validate(self)?;
            let mutation = record.mutation()?;
            if let Some(old) = previous {
                if old.mutation()? != mutation {
                    return Err(FleetCatalogError::PayloadConflict);
                }
            }
            mutation.validate()?;
            let committed = self
                .commit_with_disruption(
                    snapshot,
                    &mutation,
                    None,
                    resources::Authority::Isolation,
                    || Ok(()),
                )
                .await?;
            let applied = self
                .isolation_applied(&committed.snapshot, &record)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            Ok(FleetNodeIsolation {
                boot: request.attestation.boot.clone(),
                applied,
                observed: committed.snapshot.revision(),
            })
        })
        .await
    }

    async fn isolation_request(
        &self,
        request: &FleetNodeIsolationRequest,
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
            .first()
            .and_then(|c| c.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Record =
            decode(KIND, bytes, 4096).map_err(|_| FleetCatalogError::PayloadConflict)?;
        record.validate(self)?;
        if record.request != *request || record.mutation()? != mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    async fn isolation_applied(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
    ) -> Result<Option<u64>> {
        let mutation = record.mutation()?;
        let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
        let applied = self
            .inner
            .committed(&snapshot.head, &hex::encode(mutation.id), digest)
            .await?;
        if applied.is_some_and(|r| mutation.expected_revision.checked_add(1) != Some(r)) {
            return Err(FleetCatalogError::Corrupt("isolation applied revision"));
        }
        Ok(applied)
    }

    pub(super) async fn confirm_node_isolation(
        &self,
        snapshot: &FleetSnapshot,
        node: &NodeRecord,
    ) -> Result<IsolationProof> {
        let id = node.isolation.ok_or(FleetCatalogError::Conflict)?;
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &key(node.node),
            )
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let record: Record = decode(KIND, &bytes, 4096)?;
        record.validate(self)?;
        let original = self
            .isolation_request(&record.request)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if record.request.identity.id != id
            || !node.boot_origin_matches(&record.node)
            || node.operation() != record.node.operation()
            || node.epoch < record.node.epoch
            || original.mutation()? != record.mutation()?
            || self.isolation_applied(snapshot, &record).await?.is_none()
        {
            return Err(FleetCatalogError::Unconfirmed);
        }
        Ok(IsolationProof {
            boot: record.request.attestation.boot,
            digest: hash(&bytes),
        })
    }

    pub(super) async fn check_isolation_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        authority: resources::Authority,
    ) -> Result<()> {
        let protected = mutation
            .changes
            .iter()
            .any(|c| c.table == FleetTable::Operations && c.key.starts_with(PREFIX));
        if protected {
            if authority != resources::Authority::Isolation || mutation.changes.len() != 2 {
                return Err(invalid());
            }
            let record: Record = decode(
                KIND,
                mutation.changes[0].value.as_deref().ok_or_else(invalid)?,
                4096,
            )?;
            record.validate(self)?;
            if record.mutation()? != *mutation
                || record.input_root != hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?)
            {
                return Err(invalid());
            }
            if self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &key(record.node.node),
                )
                .await?
                .is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
            let mut original = self
                .host_from(
                    snapshot,
                    NodeId::new(record.node.node).map_err(|_| invalid())?,
                )
                .await?
                .record;
            if original.state != FleetNodeState::Draining
                || original.operation() != Some(record.request.identity.operation)
                || original.isolation.is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
            original.epoch = original
                .epoch
                .checked_add(1)
                .ok_or(FleetCatalogError::Limit)?;
            original.isolation = Some(record.request.identity.id);
            if original != record.node {
                return Err(invalid());
            }
        }
        for change in mutation
            .changes
            .iter()
            .filter(|c| c.table == FleetTable::Nodes)
        {
            let old = self
                .inner
                .lookup(snapshot.head.tables[FleetTable::Nodes.slot()], &change.key)
                .await?
                .and_then(|bytes| decode::<NodeRecord>(NODE_KIND, &bytes, 4096).ok());
            let next = change
                .value
                .as_deref()
                .and_then(|bytes| decode::<NodeRecord>(NODE_KIND, bytes, 4096).ok());
            self.check_restart_return_node_change(old.as_ref(), next.as_ref(), authority)?;
            if let Some(old) = old.as_ref().filter(|old| old.isolation.is_some()) {
                let next = next.as_ref().ok_or_else(invalid)?;
                next.validate(&self.inner)?;
                if next.isolation != old.isolation
                    || next.operation() != old.operation()
                    || next.epoch < old.epoch
                    || !next.boot_origin_matches(old)
                {
                    return Err(invalid());
                }
            } else if next.as_ref().is_some_and(|next| next.isolation.is_some()) && !protected {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
