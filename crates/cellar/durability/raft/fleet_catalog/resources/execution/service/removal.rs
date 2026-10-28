// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One historical proof per turn; only the existing terminal verifier releases
//! the slot. A restarted worker repeats replay and current survivor checks.
use super::recovery::Target;
use super::*;
use crate::raft::archive::{ArchiveError, ArchiveLimits, ArchiveVerifier};
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::fleet_catalog::{
    FleetEvacuationArchive, FleetEvacuationVerification, FleetHistoryState,
    FleetIsolatedRemovalRequest,
};
use cellar_logic::consensus::{NodeId, ProposalId};
use cellar_ltx::LtxHost;
use sha2::{Digest, Sha256};

/// Explicit replay ownership for automatic repair and terminal removal. The
/// embedding runtime owns the scratch store and its disk/memory limits. These
/// services grant no archive proof until physical verification succeeds.
pub struct FleetFailureRecoveryOptions {
    pub detection: FleetFailureDetectionOptions,
    pub archive_host: LtxHost,
    pub archive_verifier: ArchiveVerifier,
    pub archive_limits: ArchiveLimits,
}

impl FleetFailureRecoveryOptions {
    pub(super) fn validate(&self) -> Result<()> {
        self.detection.validate()?;
        self.archive_limits.validate().map_err(archive_error)
    }
}

struct Session {
    snapshot: FleetSnapshot,
    operation: ProposalId,
    walk: FleetEvacuationVerification,
    checked: u64,
}

pub(super) struct Removal {
    host: LtxHost,
    verifier: ArchiveVerifier,
    limits: ArchiveLimits,
    platform: Arc<dyn FleetFailureRecoveryPlatform>,
    session: Option<Session>,
}

impl Removal {
    pub fn new(
        options: FleetFailureRecoveryOptions,
        platform: Arc<dyn FleetFailureRecoveryPlatform>,
    ) -> Self {
        Self {
            host: options.archive_host,
            verifier: options.archive_verifier,
            limits: options.archive_limits,
            platform,
            session: None,
        }
    }

    pub fn clear(&mut self) {
        self.session = None;
    }

    pub async fn round(
        &mut self,
        catalog: &FleetCatalog,
        observer: &RaftCells,
        cut: &FleetSnapshot,
        target: &Target,
        deadline: &Deadline<'_>,
        status: &watch::Sender<FleetMovementServiceStatus>,
    ) -> Result<()> {
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
                        .find(|n| n.config().namespace == scope.namespace)
                        .ok_or(FleetCatalogError::InventoryIncomplete)?;
                    let birth = catalog
                        .inner
                        .domain
                        .run(namespace.birth(&scope.cell))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)??
                        .ok_or(FleetCatalogError::InventoryIncomplete)?;
                    if birth.scope() != &scope || birth.digest() != history.birth_digest() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let archive = catalog
                        .inner
                        .domain
                        .run(async {
                            namespace.cell_archive(&birth, self.host.clone(), self.limits)
                        })
                        .await
                        .map_err(|_| FleetCatalogError::Closed)??;
                    session
                        .walk
                        .freeze_live(observer, nodes, deadline.remaining()?)
                        .await?;
                    let metadata = catalog
                        .inner
                        .domain
                        .run(archive.recovery_metadata())
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(archive_error)?
                        .ok_or(FleetCatalogError::Unconfirmed)?;
                    let reference = metadata.reference();
                    // The selected immutable reference must cover the fixed
                    // cut; archive publication may catch up on a later turn.
                    session
                        .walk
                        .verify_live(
                            observer,
                            nodes,
                            FleetEvacuationArchive {
                                archive: &archive,
                                reference: &reference,
                                verifier: &self.verifier,
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

fn archive_error(error: ArchiveError) -> FleetCatalogError {
    match error {
        ArchiveError::Invalid(reason) => FleetCatalogError::Invalid(reason),
        ArchiveError::Corrupt(reason) => FleetCatalogError::Corrupt(reason),
        ArchiveError::Conflict => FleetCatalogError::Conflict,
        ArchiveError::Limit => FleetCatalogError::Limit,
        ArchiveError::Unconfirmed | ArchiveError::Timeout => FleetCatalogError::Unconfirmed,
        ArchiveError::Stopped => FleetCatalogError::Closed,
        error => FleetCatalogError::Storage(error.into()),
    }
}
