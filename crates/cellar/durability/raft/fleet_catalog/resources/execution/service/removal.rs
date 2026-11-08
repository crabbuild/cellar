// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One historical proof per turn; only the existing terminal verifier releases
//! the slot. A restarted worker repeats replay and current survivor checks.
use super::recovery::Target;
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_catalog::{
    FleetEvacuationArchive, FleetEvacuationVerification, FleetGroupArchiveVerifier,
    FleetHistoryState, FleetIsolatedRemovalRequest,
};
use cellar_logic::consensus::{NodeId, ProposalId};
use sha2::{Digest, Sha256};

/// Explicit replay ownership for automatic repair and terminal removal. The
/// embedding runtime owns shared private image inventories and replay pools.
/// These services grant no archive proof until complete physical replay succeeds.
#[derive(Clone)]
pub struct FleetFailureRecoveryOptions {
    pub detection: FleetFailureDetectionOptions,
    pub archive: FleetGroupArchiveVerifier,
}

impl FleetFailureRecoveryOptions {
    pub(super) fn validate(&self) -> Result<()> {
        self.detection.validate()
    }
}

struct Session {
    snapshot: FleetSnapshot,
    operation: ProposalId,
    walk: FleetEvacuationVerification,
    checked: u64,
}

pub(super) struct Removal {
    archive: FleetGroupArchiveVerifier,
    platform: Arc<dyn FleetFailureRecoveryPlatform>,
    session: Option<Session>,
}

impl Removal {
    pub fn new(
        options: FleetFailureRecoveryOptions,
        platform: Arc<dyn FleetFailureRecoveryPlatform>,
    ) -> Self {
        Self {
            archive: options.archive,
            platform,
            session: None,
        }
    }

    pub fn clear(&mut self) {
        self.session = None;
    }

    pub fn round<'a>(
        &'a mut self,
        catalog: &'a FleetCatalog,
        observer: &'a RaftCells,
        cut: &'a FleetSnapshot,
        target: &'a Target,
        deadline: &'a Deadline<'_>,
        status: &'a watch::Sender<FleetMovementServiceStatus>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        // Inventory and archive replay own a complete phase. Keep their future
        // state out of the enclosing recovery poll frames while preserving the
        // worker's existing admission, original deadline and joined lifetime.
        Box::pin(async move {
            let isolation = target.isolation.ok_or(FleetCatalogError::Unconfirmed)?;
            if self.session.as_ref().is_some_and(|s| {
                s.snapshot.token != cut.token || s.operation != target.context.operation()
            }) {
                self.clear();
            }
            let phase = if target.finalizing {
                FleetFailureRecoveryPhase::VerifyingRemoval
            } else {
                FleetFailureRecoveryPhase::VerifyingFinalization
            };
            status.send_modify(|s| {
                if let Some(r) = &mut s.failure_recovery {
                    r.phase = phase;
                    r.verified_histories = self.session.as_ref().map_or(0, |s| s.checked);
                    r.total_histories = cut.head.history_count;
                }
            });
            let namespaces = self.platform.namespaces(deadline.remaining()?).await?;
            if self.session.is_none() {
                let walk = catalog
                    .prepare_evacuation_verification(
                        cut,
                        target.context.operation(),
                        &namespaces,
                        deadline.remaining()?,
                    )
                    .await?;
                self.session = Some(Session {
                    snapshot: cut.clone(),
                    operation: target.context.operation(),
                    walk,
                    checked: 0,
                });
            }
            let session = self.session.as_mut().ok_or(FleetCatalogError::Conflict)?;
            if let Some(history) = session.walk.next_history(deadline.remaining()?).await? {
                match history.state() {
                    FleetHistoryState::Cancelled { .. } => {
                        session.walk.verify_cancelled(deadline.remaining()?).await?
                    }
                    FleetHistoryState::Retired { .. } => {
                        session.walk.verify_retired(deadline.remaining()?).await?
                    }
                    FleetHistoryState::Live => {
                        // Rooted current membership chooses candidates. The walk
                        // independently authenticates all three and excludes the
                        // failed node before accepting actual archive replay.
                        let (voters, _) = catalog
                            .run(
                                deadline.remaining()?,
                                catalog.settled_group_voters(cut, &history),
                            )
                            .await?;
                        let nodes: [NodeId; 3] = voters
                            .into_keys()
                            .collect::<Vec<_>>()
                            .try_into()
                            .map_err(|_| FleetCatalogError::InventoryIncomplete)?;
                        let scope = history
                            .binding()
                            .scope()
                            .map_err(|_| FleetCatalogError::Corrupt("recovery removal scope"))?;
                        let namespace = namespaces
                            .iter()
                            .find(|n| n.namespace_config().namespace == scope.namespace)
                            .ok_or(FleetCatalogError::InventoryIncomplete)?;
                        let fixed = session
                            .walk
                            .freeze_live(observer, nodes, deadline.remaining()?)
                            .await?;
                        let registry = catalog
                            .run(
                                deadline.remaining()?,
                                self.archive
                                    .open(&catalog.inner.domain, namespace, &history),
                            )
                            .await?;
                        let hold = session
                            .walk
                            .acquire_archive_hold(&registry, deadline.remaining()?)
                            .await?;
                        let receiver = catalog
                            .run(
                                deadline.remaining()?,
                                self.archive.receiver(
                                    &catalog.inner.domain,
                                    fixed.root().route(),
                                    namespace.namespace_config().image_storage,
                                ),
                            )
                            .await?;
                        session
                            .walk
                            .verify_live(
                                observer,
                                nodes,
                                FleetEvacuationArchive {
                                    registry: &registry,
                                    hold: &hold,
                                    tails: self.archive.tails(),
                                    receiver,
                                },
                                deadline.remaining()?,
                            )
                            .await?;
                    }
                    _ => return Err(FleetCatalogError::InventoryIncomplete),
                }
                session.checked = session
                    .checked
                    .checked_add(1)
                    .ok_or(FleetCatalogError::Limit)?;
                status.send_modify(|s| {
                    if let Some(r) = &mut s.failure_recovery {
                        r.verified_histories = session.checked;
                    }
                });
                return Ok(());
            }
            let candidate = session
                .walk
                .publish_candidate(deadline.remaining()?)
                .await?;
            // Include immutable candidate bytes in the request identity. A new
            // proof after controller loss cannot reuse an earlier payload's ID.
            // Competing attempts at the same Root can only admit one successor.
            let request = request(target, cut.revision(), candidate.digest());
            if target.finalizing {
                let receipt = catalog
                    .complete_isolated_evacuation(
                        cut,
                        &FleetIsolatedRemovalRequest {
                            identity: request,
                            isolation,
                        },
                        Some(&mut session.walk),
                        &namespaces,
                        deadline.remaining()?,
                    )
                    .await?;
                status.send_modify(|s| {
                    if let Some(r) = &mut s.failure_recovery {
                        r.phase = FleetFailureRecoveryPhase::Removed;
                        r.removed_revision = Some(receipt.applied_revision());
                    }
                });
            } else {
                candidate
                    .begin_isolated_finalization(
                        cut,
                        &request,
                        isolation,
                        &namespaces,
                        deadline.remaining()?,
                    )
                    .await?;
                status.send_modify(|s| {
                    if let Some(r) = &mut s.failure_recovery {
                        r.phase = FleetFailureRecoveryPhase::VerifyingRemoval;
                        r.verified_histories = 0;
                    }
                });
            }
            self.clear();
            Ok(())
        })
    }
}

fn request(target: &Target, revision: u64, candidate: [u8; 32]) -> FleetRequestIdentity {
    let mut digest = Sha256::new();
    digest.update(b"cellar-failure-removal-1");
    digest.update(target.context.operation().bytes());
    digest.update(revision.to_be_bytes());
    digest.update([u8::from(target.finalizing)]);
    digest.update(candidate);
    let bytes: [u8; 32] = digest.finalize().into();
    let mut id = [0; 16];
    id.copy_from_slice(&bytes[..16]);
    FleetRequestIdentity {
        id,
        operation: target.context.operation().bytes(),
        expected_revision: revision,
        principal: "fleet-failure-recovery".into(),
    }
}
