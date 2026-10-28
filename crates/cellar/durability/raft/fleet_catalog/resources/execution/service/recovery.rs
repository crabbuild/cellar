// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable failure exclusion precedes a retryable trusted platform effect.
use super::*;
use crate::raft::catalog::NamespaceCatalog;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_catalog::disruption::Disruption;
use crate::raft::fleet_stop::BootBinding;
use cellar_logic::consensus::ProposalId;
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
    /// rooted coverage before admitting exclusion. This callback performs no
    /// platform mutation.
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
}

/// Local diagnostics. Repairing and inventory exhaustion do not certify RF3,
/// terminal node removal, disruption-slot release or storage reclamation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetFailureRecoveryStatus {
    pub context: FleetFailureRecoveryContext,
    pub phase: FleetFailureRecoveryPhase,
}

pub(super) struct Recovery {
    pub detection: super::detection::Detection,
    platform: Arc<dyn FleetFailureRecoveryPlatform>,
    evidence: Arc<dyn FleetIsolatedRepairEvidence>,
    repair: Option<(ProposalId, super::isolated::IsolatedRepairService)>,
}

impl Recovery {
    pub fn new(
        platform: Arc<dyn FleetFailureRecoveryPlatform>,
        evidence: Arc<dyn FleetIsolatedRepairEvidence>,
        options: FleetFailureDetectionOptions,
    ) -> Self {
        Self {
            detection: super::detection::Detection::new(options),
            platform,
            evidence,
            repair: None,
        }
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
                let active = catalog.checked_active_evacuation(cut, operation).await?;
                let host = catalog
                    .host_from_record(cut, active.admission().node())
                    .await?;
                if host.record.boot_binding(catalog.birth_owner()?.catalog)? != boot {
                    return Err(FleetCatalogError::Conflict);
                }
                let isolated = host.record.isolation.is_some();
                if isolated {
                    catalog.confirm_node_isolation(cut, &host.record).await?;
                }
                Ok(Some((
                    FleetFailureRecoveryContext { operation, boot },
                    isolated,
                )))
            })
            .await?;
        let Some((context, isolated)) = active else {
            self.repair = None;
            status.send_modify(|s| {
                s.failure_recovery = None;
                s.isolated_repair = None;
            });
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
        status.send_modify(|s| {
            s.failure_recovery = Some(FleetFailureRecoveryStatus {
                context: context.clone(),
                phase: if isolated {
                    FleetFailureRecoveryPhase::Repairing
                } else {
                    FleetFailureRecoveryPhase::IsolationPending
                },
            })
        });
        if !isolated {
            let attestation = self
                .platform
                .isolate(&context, deadline.remaining()?)
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
