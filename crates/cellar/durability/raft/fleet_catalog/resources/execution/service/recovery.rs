// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable failure exclusion precedes a retryable trusted platform effect.
use super::*;
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_catalog::disruption::Disruption;
use crate::raft::fleet_stop::BootBinding;
use cellar_logic::consensus::{NodeId, ProposalId};
use sha2::{Digest, Sha256};

/// Rooted permanent isolation intent, recovered from the operation journal.
/// This is not evidence that the boot is stopped or permission to delete data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetFailureRecoveryContext {
    operation: ProposalId,
    boot: BootBinding,
}

impl FleetFailureRecoveryContext {
    pub fn operation(&self) -> ProposalId {
        self.operation
    }

    pub fn boot(&self) -> &BootBinding {
        &self.boot
    }
}

/// Trusted deployment integration for automatic failure recovery. Installation
/// explicitly authorizes irreversible isolation of detected failed boots.
/// Futures are deadline-bound and dropped on shutdown; external effects may
/// still complete. Reconcile retries by the same operation and exact boot,
/// including after controller loss. Never target a node name's replacement.
/// A successful response must be stable and prove the process and every worker
/// stopped, with restart prevented. Retain the volume, engine and resources.
/// Network failure, missing container discovery and timeouts are not proof.
pub trait FleetFailureRecoveryPlatform: Send + Sync {
    /// Complete managed namespace handles; the catalog independently checks
    /// rooted coverage before admitting exclusion and at terminal removal.
    /// Called throughout verification; this callback performs no platform mutation.
    fn namespaces<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<NamespaceCatalog>>> + Send + 'a>>;

    fn isolate<'a>(
        &'a self,
        context: &'a FleetFailureRecoveryContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetIsolationAttestation>> + Send + 'a>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetFailureRecoveryPhase {
    IsolationPending,
    Repairing,
    VerifyingFinalization,
    VerifyingRemoval,
    Removed,
}

/// Local diagnostics. Only Removed follows a journal-confirmed terminal
/// receipt; no phase authorizes storage reclamation or grants a health lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetFailureRecoveryStatus {
    pub context: FleetFailureRecoveryContext,
    pub phase: FleetFailureRecoveryPhase,
    pub verified_histories: u64,
    pub total_histories: u64,
    pub removed_revision: Option<u64>,
}

pub(super) struct Target {
    pub context: FleetFailureRecoveryContext,
    pub isolation: Option<ProposalId>,
    pub finalizing: bool,
}

pub(super) struct Recovery {
    pub detection: super::detection::Detection,
    platform: Arc<dyn FleetFailureRecoveryPlatform>,
    evidence: Arc<dyn FleetIsolatedRepairEvidence>,
    repair: Option<(ProposalId, super::isolated::IsolatedRepairService)>,
    removal: super::removal::Removal,
    last: Option<FleetFailureRecoveryContext>,
}

impl Recovery {
    pub fn new(
        platform: Arc<dyn FleetFailureRecoveryPlatform>,
        evidence: Arc<dyn FleetIsolatedRepairEvidence>,
        options: FleetFailureRecoveryOptions,
    ) -> Self {
        Self {
            detection: super::detection::Detection::new(options.detection.clone()),
            removal: super::removal::Removal::new(options, platform.clone()),
            last: None,
            platform,
            evidence,
            repair: None,
        }
    }

    async fn reconcile_completed(
        &mut self,
        catalog: &FleetCatalog,
        cut: &FleetSnapshot,
        deadline: &Deadline<'_>,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) -> Result<()> {
        let Some(context) = &self.last else {
            return Ok(());
        };
        let Some(receipt) = catalog
            .isolated_removal_receipt(cut, context.operation, deadline.remaining()?)
            .await?
        else {
            return Ok(());
        };
        if receipt.node() != context.boot.node {
            return Err(FleetCatalogError::Conflict);
        }
        status.send_modify(|s| {
            if let Some(r) = &mut s.failure_recovery {
                r.phase = FleetFailureRecoveryPhase::Removed;
                r.removed_revision = Some(receipt.applied_revision());
            }
        });
        // The immutable committed receipt is settled. Do not reread it on
        // every idle detection round or retain its runtime proof workspace.
        self.last = None;
        self.repair = None;
        self.removal.clear();
        Ok(())
    }

    pub fn pass_delay(&self, options: &FleetMovementServiceOptions) -> Option<Duration> {
        self.repair
            .as_ref()
            .and_then(|(_, r)| r.pass_delay(options))
    }

    pub async fn round(
        &mut self,
        catalog: &FleetCatalog,
        observer: &RaftCells,
        cut: &FleetSnapshot,
        options: &FleetMovementServiceOptions,
        deadline: &Deadline<'_>,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) -> Result<()> {
        let active = catalog
            .run(deadline.remaining()?, async {
                if catalog.inner.current().await?.token != cut.token {
                    return Err(FleetCatalogError::Conflict);
                }
                let Some(disruption) = catalog.active_disruption(cut).await? else {
                    return Ok(None);
                };
                let operation = disruption.operation();
                let Disruption::Evacuation(record) = disruption else {
                    return Err(FleetCatalogError::DisruptionInProgress { operation });
                };
                let boot = record
                    .failure_boot()
                    .cloned()
                    .ok_or(FleetCatalogError::DisruptionInProgress { operation })?;
                let operation = ProposalId::new(operation)
                    .map_err(|_| FleetCatalogError::Corrupt("failure operation"))?;
                let node = NodeId::new(boot.node)
                    .map_err(|_| FleetCatalogError::Corrupt("failure node"))?;
                let host = catalog.host_from_record(cut, node).await?;
                if host.record.operation() != Some(operation.bytes()) {
                    return Err(FleetCatalogError::Conflict);
                }
                let finalizing = match host.state() {
                    FleetNodeState::Draining => {
                        catalog.checked_active_evacuation(cut, operation).await?;
                        false
                    }
                    FleetNodeState::Finalizing => true,
                    _ => return Err(FleetCatalogError::Conflict),
                };
                if host.record.boot_binding(catalog.birth_owner()?.catalog)? != boot {
                    return Err(FleetCatalogError::Conflict);
                }
                let isolation = host
                    .record
                    .isolation
                    .map(ProposalId::new)
                    .transpose()
                    .map_err(|_| FleetCatalogError::Corrupt("failure isolation"))?;
                if isolation.is_some() {
                    catalog.confirm_node_isolation(cut, &host.record).await?;
                }
                if finalizing && isolation.is_none() {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(Some(Target {
                    context: FleetFailureRecoveryContext { operation, boot },
                    isolation,
                    finalizing,
                }))
            })
            .await;
        if matches!(&active, Err(FleetCatalogError::DisruptionInProgress { .. })) {
            // A different operation may claim the slot after our terminal CAS
            // committed but before its acknowledgement was reconciled.
            self.reconcile_completed(catalog, cut, deadline, status)
                .await?;
        }
        let Some(target) = active? else {
            self.repair = None;
            self.removal.clear();
            self.reconcile_completed(catalog, cut, deadline, status)
                .await?;
            self.detection
                .round(catalog, observer, cut, deadline)
                .await?;
            let now = catalog
                .inner
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            let view = self.detection.view(now);
            let Some(failed) = view
                .nodes
                .iter()
                .find(|n| n.health == FleetNodeHealth::Unavailable)
            else {
                return Ok(());
            };
            let expires_at = now
                .checked_add(
                    5000u64
                        .checked_sub(failed.sample_age_ms.ok_or(FleetCatalogError::Unconfirmed)?)
                        .ok_or(FleetCatalogError::Unconfirmed)?,
                )
                .ok_or(FleetCatalogError::Limit)?;
            let namespaces = self.platform.namespaces(deadline.remaining()?).await?;
            let operation = identity(b"operation", &failed.boot, cut.revision())?;
            let request = FleetRequestIdentity {
                id: identity(b"exclusion", &failed.boot, cut.revision())?,
                operation,
                principal: "fleet-failure-recovery".into(),
                expected_revision: cut.revision(),
            };
            catalog
                .begin_failure_evacuation(
                    cut,
                    &request,
                    (failed.boot.clone(), expires_at),
                    &namespaces,
                    deadline.remaining()?,
                )
                .await?;
            // Do not dispatch an external effect in the admission turn. The
            // next turn must recover and authenticate the durable intent.
            return Ok(());
        };
        let context = &target.context;
        if self.last.as_ref() != Some(context) {
            self.removal.clear();
            self.repair = None;
            self.last = Some(context.clone());
            status.send_modify(|s| s.isolated_repair = None);
        }
        status.send_modify(|s| {
            if s.failure_recovery
                .as_ref()
                .is_none_or(|r| r.context != *context)
            {
                s.failure_recovery = Some(FleetFailureRecoveryStatus {
                    context: context.clone(),
                    phase: FleetFailureRecoveryPhase::IsolationPending,
                    verified_histories: 0,
                    total_histories: 0,
                    removed_revision: None,
                });
            }
            if target.isolation.is_some() {
                if let Some(r) = &mut s.failure_recovery {
                    if r.phase == FleetFailureRecoveryPhase::IsolationPending {
                        r.phase = FleetFailureRecoveryPhase::Repairing;
                    }
                }
            }
        });
        if target.isolation.is_none() {
            let attestation = self
                .platform
                .isolate(context, deadline.remaining()?)
                .await?;
            if attestation.boot != context.boot {
                return Err(FleetCatalogError::Invalid("platform isolation boot"));
            }
            let request = FleetNodeIsolationRequest {
                identity: FleetRequestIdentity {
                    id: identity(b"isolation", &context.boot, cut.revision())?,
                    operation: context.operation.bytes(),
                    principal: "fleet-failure-recovery".into(),
                    expected_revision: cut.revision(),
                },
                attestation,
            };
            catalog
                .attest_node_isolation(cut, &request, deadline.remaining()?)
                .await?;
            return Ok(());
        }
        if target.finalizing {
            // The durable finalization record is revalidated by preparation;
            // the Draining-only repair admission must not gate this phase.
            return self
                .removal
                .round(catalog, observer, cut, &target, deadline, status)
                .await;
        }
        if self
            .repair
            .as_ref()
            .is_none_or(|(operation, _)| *operation != context.operation)
        {
            self.repair = Some((
                context.operation,
                super::isolated::IsolatedRepairService::new(
                    context.operation,
                    self.evidence.clone(),
                ),
            ));
        }
        if let Some((_, repair)) = &mut self.repair {
            repair
                .round(catalog, observer, cut, options, deadline, status)
                .await?;
        }
        let ready = status.borrow().isolated_repair.as_ref().is_some_and(|r| {
            r.operation == context.operation && r.inventory_exhausted && r.blocked_histories == 0
        });
        if ready {
            self.removal
                .round(catalog, observer, cut, &target, deadline, status)
                .await?;
        }
        Ok(())
    }
}

fn identity(phase: &[u8], boot: &BootBinding, revision: u64) -> Result<[u8; 16]> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-failure-recovery-1");
    hash.update(phase);
    hash.update(revision.to_be_bytes());
    hash.update(
        serde_json::to_vec(boot)
            .map_err(|_| FleetCatalogError::Invalid("failure boot encoding"))?,
    );
    let bytes: [u8; 32] = hash.finalize().into();
    let id: [u8; 16] = bytes[..16]
        .try_into()
        .map_err(|_| FleetCatalogError::Invalid("failure identity"))?;
    ProposalId::new(id).map_err(|_| FleetCatalogError::Invalid("failure identity"))?;
    Ok(id)
}
