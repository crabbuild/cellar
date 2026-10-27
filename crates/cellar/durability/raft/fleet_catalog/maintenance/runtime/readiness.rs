// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Point-in-time readiness can only be published by a live, matched walk.
use super::*;
use crate::raft::fleet_catalog::resources::Authority;
use crate::raft::fleet_stop::FleetBootBinding;
use serde::Deserialize;

const KIND: u8 = 85;
const PREFIX: &str = "restart-ready/";

#[derive(Clone, Debug)]
pub struct FleetRestartReadyRequest {
    pub identity: FleetRequestIdentity,
    pub node: NodeId,
    pub expected_process: ProcessGeneration,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    format: u8,
    request: FleetRequestIdentity,
    input_root: [u8; 32],
    gate_parent: [u8; 16],
    gate_digest: [u8; 32],
    candidate: [u8; 32],
}

/// Journal-confirmed RestartReady assessment. The target stays quiesced and
/// RestartPending, with its disruption slot held. This is historical evidence:
/// guarded maintenance stop must independently renew the live proofs.
pub struct FleetRestartReadyAdmission {
    record: Ready,
    boot: FleetBootBinding,
    epoch: u64,
    candidate: FleetRestartRuntimeCandidate,
    applied: u64,
    observed: u64,
}

impl FleetRestartReadyAdmission {
    pub(in crate::raft::fleet_catalog) async fn check_current(
        &self,
        catalog: &FleetCatalog,
    ) -> Result<()> {
        self.candidate.candidate.basis.check_current(catalog).await
    }

    pub fn identity(&self) -> &FleetRequestIdentity {
        &self.record.request
    }

    pub fn boot(&self) -> &FleetBootBinding {
        &self.boot
    }

    pub fn enrollment_epoch(&self) -> u64 {
        self.epoch
    }

    pub fn candidate(&self) -> &FleetRestartRuntimeCandidate {
        &self.candidate
    }

    pub fn applied_revision(&self) -> u64 {
        self.applied
    }

    pub fn observed_revision(&self) -> u64 {
        self.observed
    }
}

impl Ready {
    fn key(operation: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(operation))
    }

    fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: Self::key(self.request.operation),
                value: Some(encode(KIND, self, records::LIMIT)?),
            }],
        })
    }
}

impl FleetRestartVerification {
    /// Publish M5 from owned reconciliation, without accepting serialized
    /// candidates as authority. All voting samples must still fit the original
    /// freshness envelope at the Root CAS. An expired walk must be repeated.
    /// Retry a lost response with the same request, or use `restart_readiness`.
    pub fn publish_ready<'a>(
        &'a mut self,
        request: &'a FleetRestartReadyRequest,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<FleetRestartReadyAdmission>> {
        Box::pin(async move {
            validate_request(&request.identity)?;
            let catalog = self.catalog.clone();
            let deadline = Deadline::new(&catalog, timeout)?;
            let started = proof::now(&catalog)?;
            if request.identity.operation != self.gate.request.operation
                || request.node.bytes() != self.gate.node.node
                || request.expected_process.bytes()
                    != self
                        .gate
                        .node
                        .boot_binding(self.gate.preparation.catalog)?
                        .process
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            // Historical confirmation comes first, even after a later fence,
            // cancellation, restart or unrelated Root update.
            if let Some(prior) = catalog
                .restart_readiness(request, deadline.remaining()?)
                .await?
            {
                return Ok(prior);
            }
            self.check_ready_samples(started)?;
            let digest = self.save_candidate(&deadline).await?;
            self.check_ready_samples(started)?;
            catalog
                .run(deadline.remaining()?, async {
                    let current = catalog.inner.current().await?;
                    self.book.basis.check_current(&catalog).await?;
                    if request.identity.expected_revision != current.revision() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let record = Ready {
                        format: 1,
                        request: request.identity.clone(),
                        input_root: hash(&encode(HEAD_KIND, &current.head, ROOT_LIMIT)?),
                        gate_parent: self.gate.request.id,
                        gate_digest: self.gate.digest()?,
                        candidate: digest,
                    };
                    let mutation = record.mutation()?;
                    mutation.validate()?;
                    // A pending immutable intent may be retried only byte-for-byte.
                    if catalog
                        .ready_candidate(request)
                        .await?
                        .is_some_and(|old| old != record)
                    {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    catalog
                        .commit_with_disruption(
                            &current,
                            &mutation,
                            None,
                            Authority::RestartReadiness,
                            || {
                                deadline.remaining()?;
                                self.check_ready_samples(started)
                            },
                        )
                        .await?;
                    Ok(())
                })
                .await?;
            catalog
                .restart_readiness(request, deadline.remaining()?)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)
        })
    }

    pub(super) fn check_ready_samples(&self, started: u64) -> Result<()> {
        if self.book.matched != self.book.voting
            || self.book.voting != 0 && self.oldest_matched_sample.is_none()
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        proof::fresh(
            &self.catalog,
            started,
            self.oldest_matched_sample.unwrap_or(started),
        )
    }
}

impl FleetCatalog {
    /// Recover an exact historical readiness receipt after a controller restart
    /// or lost CAS confirmation. Reading it never resumes admission or stops a
    /// process, and does not claim that its runtime observations remain fresh.
    pub async fn restart_readiness(
        &self,
        request: &FleetRestartReadyRequest,
        timeout: Duration,
    ) -> Result<Option<FleetRestartReadyAdmission>> {
        validate_request(&request.identity)?;
        self.run(timeout, self.restart_readiness_inner(request))
            .await
    }

    pub(in crate::raft::fleet_catalog) async fn restart_readiness_inner(
        &self,
        request: &FleetRestartReadyRequest,
    ) -> Result<Option<FleetRestartReadyAdmission>> {
        let current = self.inner.current().await?;
        let Some(record) = self.ready_candidate(request).await? else {
            return Ok(None);
        };
        let mutation = record.mutation()?;
        let Some(applied) = self
            .inner
            .committed(
                &current.head,
                &hex::encode(record.request.id),
                hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
            )
            .await?
        else {
            return Ok(None);
        };
        let gate = self
            .restart_gate_candidate_for_id(record.gate_parent, record.request.operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("restart readiness gate"))?;
        let candidate = self
            .inner
            .read::<Candidate>(
                &format!("restart-candidates/{}", hex::encode(record.candidate)),
                records::CANDIDATE_KIND,
                records::LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("restart readiness candidate"))?
            .0;
        if applied
            != record
                .request
                .expected_revision
                .checked_add(1)
                .ok_or(FleetCatalogError::Limit)?
            || record.format != 1
            || record.input_root == [0; 32]
            || record.gate_digest != gate.digest()?
            || record.request.expected_revision <= gate.request.expected_revision
            || record.request.expected_revision < candidate.basis.revision
            || candidate.format != 1
            || candidate.physical == [0; 32]
            || candidate.count != candidate.basis.histories
            || candidate.covered_physical > candidate.count
            || candidate.voting.checked_add(candidate.retained) != Some(candidate.covered_physical)
            || candidate.matched != candidate.voting
            || candidate.index.is_none() != (candidate.count == 0)
            || hash(&encode(
                records::CANDIDATE_KIND,
                &candidate,
                records::LIMIT,
            )?) != record.candidate
        {
            return Err(FleetCatalogError::Corrupt("restart readiness receipt"));
        }
        candidate.basis.check_gate(&gate)?;
        let gate_mutation = gate.mutation()?;
        if self
            .inner
            .committed(
                &current.head,
                &hex::encode(gate.request.id),
                hash(&encode(REQUEST_KIND, &gate_mutation, REQUEST_LIMIT)?),
            )
            .await?
            != gate.request.expected_revision.checked_add(1)
        {
            return Err(FleetCatalogError::Corrupt("restart readiness parent"));
        }
        Ok(Some(FleetRestartReadyAdmission {
            boot: gate.node.boot_binding(gate.preparation.catalog)?,
            epoch: gate.node.epoch,
            candidate: FleetRestartRuntimeCandidate {
                digest: record.candidate,
                candidate,
            },
            record,
            applied,
            observed: current.revision(),
        }))
    }

    async fn ready_candidate(&self, request: &FleetRestartReadyRequest) -> Result<Option<Ready>> {
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
            .find(|change| {
                change.table == FleetTable::Operations
                    && change.key == Ready::key(request.identity.operation)
            })
            .and_then(|change| change.value.as_deref())
            .ok_or(FleetCatalogError::PayloadConflict)?;
        let record: Ready = decode(KIND, bytes, records::LIMIT)?;
        let gate = self
            .restart_gate_candidate_for_id(record.gate_parent, record.request.operation)
            .await?
            .ok_or(FleetCatalogError::Corrupt("restart readiness gate"))?;
        if record.request != request.identity
            || record.mutation()? != mutation
            || gate.node.node != request.node.bytes()
            || gate.node.boot_binding(gate.preparation.catalog)?.process
                != request.expected_process.bytes()
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(Some(record))
    }

    pub(in crate::raft::fleet_catalog) fn check_restart_readiness_mutation(
        &self,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if authority != Authority::RestartReadiness
            && mutation.changes.iter().any(|change| {
                change.table == FleetTable::Operations && change.key.starts_with(PREFIX)
            })
        {
            return Err(FleetCatalogError::Invalid("restart readiness authority"));
        }
        Ok(())
    }
}
