// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact infrastructure receipts for an interrupted managed transition.
use super::{histories::catalog_error, managed_transitions::transfer_archive_error, *};
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{CellGroupArchiveCoverage, CellGroupArchiveRegistry},
};
use cellar_logic::consensus::ProposalId;
use serde::Deserialize;

const KIND: u8 = 223;
const LIMIT: usize = 20 * 1024;
const MAX_EVIDENCE: usize = 4096;

/// Bind the infrastructure fence to one durable intent and its directory barrier.
/// The authority must also consult its complete inventory of possible old writers.
pub struct FleetTransitionRecoveryFenceRequest {
    intent: FleetTransitionRecoveryIntent,
    barrier: FleetTransitionRecoveryBarrier,
    digest: [u8; 32],
}

impl FleetTransitionRecoveryFenceRequest {
    pub fn intent(&self) -> &FleetTransitionRecoveryIntent {
        &self.intent
    }

    pub fn barrier(&self) -> FleetTransitionRecoveryBarrier {
        self.barrier
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Read-back proof of a verified permanent infrastructure fence. Retains the
/// independently replayed source images for a subsequent recovery stage.
pub struct FleetFencedTransitionRecovery {
    request: FleetTransitionRecoveryFenceRequest,
    receipt_digest: [u8; 32],
    archives: Vec<CellGroupArchiveCoverage>,
}

impl FleetFencedTransitionRecovery {
    pub fn request(&self) -> &FleetTransitionRecoveryFenceRequest {
        &self.request
    }

    pub fn receipt_digest(&self) -> [u8; 32] {
        self.receipt_digest
    }

    pub fn archives(&self) -> &[CellGroupArchiveCoverage] {
        &self.archives
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    request: [u8; 32],
    evidence: Vec<u8>,
}

pub(crate) struct PreparedManagedRecoveryFence {
    request: FleetTransitionRecoveryFenceRequest,
    cuts: Vec<(CellGroupArchiveRegistry, CellGroupArchiveCoverage)>,
    receipt: Option<Receipt>,
}

impl PreparedManagedRecoveryFence {
    pub(crate) fn request(&self) -> &FleetTransitionRecoveryFenceRequest {
        &self.request
    }

    pub(crate) fn evidence(&self) -> Option<&[u8]> {
        self.receipt
            .as_ref()
            .map(|receipt| receipt.evidence.as_slice())
    }

    pub(crate) fn check_evidence(evidence: &[u8]) -> Result<()> {
        if evidence.is_empty() || evidence.len() > MAX_EVIDENCE {
            return Err(FleetCatalogError::Invalid(
                "transition fence evidence length",
            ));
        }
        Ok(())
    }
}

impl FleetCatalog {
    pub(crate) async fn prepare_managed_recovery_fence(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        principal: &str,
        barrier: FleetTransitionRecoveryBarrier,
        verifier: &FleetGroupArchiveVerifier,
        timeout: Duration,
    ) -> Result<PreparedManagedRecoveryFence> {
        self.run(timeout, async {
            let intent = self
                .read_managed_recovery_intent(directory, operation, principal)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            if intent.digest() != barrier.intent_digest() {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let mut binding = Vec::from(&b"cellar-managed-transition-fence-1\0"[..]);
            binding.extend_from_slice(&intent.digest());
            binding.extend_from_slice(&barrier.directory_digest());
            let request = FleetTransitionRecoveryFenceRequest {
                intent,
                barrier,
                digest: hash(&binding),
            };
            self.require_managed_fence_barrier(directory, &request)
                .await?;
            let receipt = self
                .inner
                .read::<Receipt>(&receipt_key(operation.bytes()), KIND, LIMIT)
                .await?
                .map(|(receipt, _)| receipt);
            if let Some(receipt) = &receipt {
                if receipt.request != request.digest {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                PreparedManagedRecoveryFence::check_evidence(&receipt.evidence)?;
            }
            let transition = ProposalId::new(request.intent.transition())
                .map_err(|_| FleetCatalogError::Corrupt("recovery transition operation"))?;
            let domain = &self.inner.domain;
            let storage = directory.namespace_config().image_storage;
            let mut cuts = Vec::with_capacity(request.intent.archive_holds().len());
            for hold in request.intent.archive_holds() {
                let route = hold.marker().route();
                let registry = domain
                    .run(Box::pin(directory.open_managed_recovery_archive(
                        domain,
                        verifier.archives(),
                        route,
                        transition,
                        barrier.directory_digest(),
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(transfer_archive_error)?;
                registry.require_selected_hold(domain, hold).await?;
                let receiver = verifier.receiver(domain, route, storage).await?;
                let coverage =
                    Box::pin(registry.verify_hold(domain, hold, verifier.tails(), receiver))
                        .await?;
                cuts.push((registry, coverage));
            }
            self.require_managed_fence_cuts(&cuts).await?;
            self.require_managed_fence_barrier(directory, &request)
                .await?;
            Ok(PreparedManagedRecoveryFence {
                request,
                cuts,
                receipt,
            })
        })
        .await
    }

    /// Persist independently verified evidence before checking the cuts again.
    /// A late archive write may make the cut unusable, even though the fence
    /// completed. Retain its receipt so that this outcome survives a restart.
    pub(crate) async fn confirm_managed_recovery_fence(
        &self,
        directory: &RangeDirectory,
        prepared: PreparedManagedRecoveryFence,
        evidence: Vec<u8>,
        timeout: Duration,
    ) -> Result<FleetFencedTransitionRecovery> {
        self.run(timeout, async {
            let PreparedManagedRecoveryFence {
                request,
                cuts,
                receipt,
            } = prepared;
            let record = Receipt {
                request: request.digest,
                evidence,
            };
            let bytes = encode(KIND, &record, LIMIT)?;
            if receipt.is_none() {
                self.inner
                    .create_exact(&receipt_key(request.intent.operation()), &bytes, LIMIT)
                    .await?;
            }
            self.require_managed_fence_cuts(&cuts).await?;
            self.require_managed_fence_barrier(directory, &request)
                .await?;
            Ok(FleetFencedTransitionRecovery {
                request,
                receipt_digest: hash(&bytes),
                archives: cuts.into_iter().map(|(_, coverage)| coverage).collect(),
            })
        })
        .await
    }

    async fn require_managed_fence_barrier(
        &self,
        directory: &RangeDirectory,
        request: &FleetTransitionRecoveryFenceRequest,
    ) -> Result<()> {
        let intent = &request.intent;
        let snapshot = self.inner.current().await?;
        let permit = self
            .managed_recovery_permit_at(&snapshot, directory, intent.basis())
            .await?;
        let selected = self
            .inner
            .domain
            .run(Box::pin(directory.observe_managed_recovery_barrier(
                intent.digest(),
                intent.basis(),
                intent.archive_holds(),
                &permit,
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(catalog_error)?;
        if selected != Some(request.barrier.directory_digest()) {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    async fn require_managed_fence_cuts(
        &self,
        cuts: &[(CellGroupArchiveRegistry, CellGroupArchiveCoverage)],
    ) -> Result<()> {
        for (registry, coverage) in cuts {
            registry
                .require_selected_hold(&self.inner.domain, coverage.hold())
                .await?;
        }
        Ok(())
    }
}

fn receipt_key(operation: [u8; 16]) -> String {
    format!(
        "managed-transition-recovery-fences/{}",
        hex::encode(operation)
    )
}
