// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Portable selectors rehydrate managed recovery from durable proofs. No local
//! replay pin, archive proof or serving authority is serialized between owners.
use super::*;
use crate::raft::{
    catalog::DisasterFenceAuthority,
    fleet_catalog::{
        FleetFencedTransitionRecovery, FleetTransitionRecoveryBarrier,
        FleetTransitionRecoveryFenceRequest, FleetTransitionRecoveryIntent,
        PreparedManagedRecoveryFence,
    },
};
use serde::{Deserialize, Serialize};

/// Retain this exact selection when handing a recovery to another process.
/// Its digests are selectors, not proofs or caller authorization. Every reopen
/// authenticates the principal, verifies the live directory barrier and replays
/// the held S3 closure using the receiving process's configured verifier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionRecoverySelection {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub intent_digest: String,
    pub directory_digest: String,
    pub fence_digest: String,
}

impl RaftFleetTransitionRecoverySelection {
    pub fn validate(&self) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::id(&self.namespace_id)?;
        records::id(&self.operation_id)?;
        for digest in [
            &self.intent_digest,
            &self.directory_digest,
            &self.fence_digest,
        ] {
            records::catalog_id(digest)?;
        }
        Ok(())
    }

    /// Capture selectors from an already verified request. The configured
    /// management owner must still check the catalog and namespace on reopen.
    pub fn from_request(
        catalog: &str,
        request: &FleetTransitionRecoveryFenceRequest,
    ) -> Result<Self> {
        let namespace = request
            .intent()
            .required_voter_lineages()
            .first()
            .ok_or(FleetCatalogError::InventoryIncomplete)?
            .group
            .namespace;
        let selected = Self {
            catalog_id: catalog.into(),
            namespace_id: hex::encode(namespace.bytes()),
            operation_id: hex::encode(request.intent().operation()),
            intent_digest: hex::encode(request.intent().digest()),
            directory_digest: hex::encode(request.barrier().directory_digest()),
            fence_digest: hex::encode(request.digest()),
        };
        selected.validate()?;
        Ok(selected)
    }

    fn barrier(&self) -> Result<FleetTransitionRecoveryBarrier> {
        let digest = |value: &str| {
            let mut bytes = [0; 32];
            hex::decode_to_slice(value, &mut bytes)
                .map_err(|_| RaftFleetFailure::invalid("managed recovery digest"))?;
            Ok::<_, RaftFleetFailure>(bytes)
        };
        Ok(FleetTransitionRecoveryBarrier::from_selection(
            digest(&self.intent_digest)?,
            digest(&self.directory_digest)?,
        ))
    }

    fn matches(&self, request: &FleetTransitionRecoveryFenceRequest) -> bool {
        self.intent_digest == hex::encode(request.intent().digest())
            && self.directory_digest == hex::encode(request.barrier().directory_digest())
            && self.fence_digest == hex::encode(request.digest())
    }
}

impl RaftFleetControl {
    /// Select the exact durable intent's directory barrier and export a
    /// controller challenge after independent archive replay. This closes old
    /// catalog startup; the controller must still permanently fence old hosts.
    /// It never invokes a fencing authority or publishes a replacement.
    pub async fn transition_recovery_challenge(
        &self,
        principal: &str,
        namespace: &str,
        intent: &FleetTransitionRecoveryIntent,
        timeout: Duration,
    ) -> Result<(RaftFleetTransitionRecoverySelection, Vec<u8>)> {
        let turn = Turn::new(&self.domain, timeout)?;
        let services = self
            .transition_preparation
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?;
        let operation = hex::encode(intent.operation());
        let barrier = self
            .select_transition_recovery_barrier(
                principal,
                namespace,
                &operation,
                intent.digest(),
                turn.remaining()?,
            )
            .await?;
        let directory = self.managed_namespace(namespace)?;
        let prepared = self
            .catalog
            .prepare_managed_recovery_fence(
                directory,
                ProposalId::new(intent.operation())
                    .map_err(|_| RaftFleetFailure::invalid("recovery operation ID"))?,
                principal,
                barrier,
                &services.verifier,
                turn.remaining()?,
            )
            .await?;
        let request = prepared.request();
        Ok((
            RaftFleetTransitionRecoverySelection::from_request(self.catalog_id(), request)?,
            request.encode_challenge()?,
        ))
    }

    /// Reopen an already persisted permanent fence in a different process.
    /// A missing receipt refuses; this read never calls `authority.fence`,
    /// selects a barrier or creates a receipt. Verification uses the configured
    /// trusted authority, never a key supplied by the serialized selector.
    pub async fn reopen_fenced_transition_recovery(
        &self,
        principal: &str,
        selected: &RaftFleetTransitionRecoverySelection,
        authority: &dyn DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest>,
        timeout: Duration,
    ) -> Result<FleetFencedTransitionRecovery> {
        let turn = Turn::new(&self.domain, timeout)?;
        let prepared = self
            .prepare_recovery_selection(principal, selected, turn.remaining()?)
            .await?;
        let evidence = prepared
            .evidence()
            .ok_or(FleetCatalogError::Unconfirmed)?
            .to_vec();
        turn.run(authority.verify(prepared.request(), &evidence))
            .await?
            .map_err(FleetCatalogError::Storage)?;
        let directory = self.managed_namespace(&selected.namespace_id)?;
        Ok(self
            .catalog
            .confirm_managed_recovery_fence(directory, prepared, evidence, turn.remaining()?)
            .await?)
    }

    /// Execute or reconcile the controller fence for an exact portable
    /// selection. Validate every selector before invoking the authority. A lost
    /// response leaves an uncertain result; retry this same selection.
    pub async fn fence_selected_transition_recovery(
        &self,
        principal: &str,
        selected: &RaftFleetTransitionRecoverySelection,
        authority: &dyn DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest>,
        timeout: Duration,
    ) -> Result<FleetFencedTransitionRecovery> {
        let turn = Turn::new(&self.domain, timeout)?;
        let prepared = self
            .prepare_recovery_selection(principal, selected, turn.remaining()?)
            .await?;
        let evidence = match prepared.evidence() {
            Some(evidence) => evidence.to_vec(),
            None => turn
                .run(authority.fence(prepared.request()))
                .await?
                .map_err(FleetCatalogError::Storage)?,
        };
        PreparedManagedRecoveryFence::check_evidence(&evidence)?;
        turn.run(authority.verify(prepared.request(), &evidence))
            .await?
            .map_err(FleetCatalogError::Storage)?;
        let directory = self.managed_namespace(&selected.namespace_id)?;
        Ok(self
            .catalog
            .confirm_managed_recovery_fence(directory, prepared, evidence, turn.remaining()?)
            .await?)
    }

    async fn prepare_recovery_selection(
        &self,
        principal: &str,
        selected: &RaftFleetTransitionRecoverySelection,
        timeout: Duration,
    ) -> Result<PreparedManagedRecoveryFence> {
        selected.validate()?;
        self.check_catalog(&selected.catalog_id)?;
        let directory = self.managed_namespace(&selected.namespace_id)?;
        let services = self
            .transition_preparation
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?;
        let prepared = self
            .catalog
            .prepare_managed_recovery_fence(
                directory,
                ProposalId::new(records::id(&selected.operation_id)?)
                    .map_err(|_| RaftFleetFailure::invalid("recovery operation ID"))?,
                principal,
                selected.barrier()?,
                &services.verifier,
                timeout,
            )
            .await?;
        if !selected.matches(prepared.request()) {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        Ok(prepared)
    }
}
