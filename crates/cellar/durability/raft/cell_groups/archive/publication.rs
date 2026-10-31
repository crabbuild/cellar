// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Source-authorized, physically verified publication. Neither a serialized
//! locator nor a registry allocation can substitute for these two proofs.
use super::super::PreparedGroupManifest;
use super::{
    graph::{Graph, Root, ROOT_DOMAIN},
    ownership::{Allocation, Descriptor},
    *,
};
use crate::raft::{cache::GroupLog, member_state::GroupMembership};
use raft::prelude::ConfState;

pub(super) struct SourceCut {
    group: GroupLog<GroupHistory>,
    root: PreparedGroupManifest,
    configuration: ConfState,
    membership: GroupMembership,
    _reservation: Arc<Permit>,
}

impl SourceCut {
    pub(super) fn from_capture(
        group: GroupLog<GroupHistory>,
        root: PreparedGroupManifest,
        configuration: ConfState,
        membership: GroupMembership,
        reservation: Arc<Permit>,
    ) -> Self {
        Self {
            group,
            root,
            configuration,
            membership,
            _reservation: reservation,
        }
    }

    fn check(&self) -> Result<()> {
        let cache = self.group.read().map_err(CellGroupCheckpointError::from)?;
        let target = self.root.marker().position();
        let current = cache
            .group_manifest
            .as_ref()
            .ok_or(CellGroupArchiveError::Invalid(
                "publication source root missing",
            ))?;
        if cache.applied.position.index < target.index
            || cache.hard_state.commit < target.index.get()
            || current.route() != self.root.marker().route()
            || (current.position().index == target.index && current != self.root.marker())
            || cache
                .term(target.index.get())
                .is_some_and(|term| term != target.term.get())
        {
            return Err(CellGroupArchiveError::Invalid(
                "archive publication source changed",
            ));
        }
        // A later local checkpoint may compact the captured prefix. Its initial
        // committed-source observation remains sealed to this live engine.
        Ok(())
    }
}

/// Persist `intent()` before commit. Holds the exact source and independently
/// recovered closure through CAS/readback; it cannot be deserialized or forged.
pub struct PreparedCellGroupArchivePublication {
    upload: CellGroupArchiveUpload,
    intent: CellGroupArchiveIntent,
    verified: VerifiedCellGroupArchiveRecovery,
    source: Arc<SourceCut>,
}

impl PreparedCellGroupArchivePublication {
    pub fn intent(&self) -> &CellGroupArchiveIntent {
        &self.intent
    }

    pub async fn commit(self, domain: &ExecutionDomain) -> Result<CellGroupArchiveCoverage> {
        domain
            .run(async {
                let _io = self.upload.admit()?;
                self.source.check()?;
                let hold = match self.upload.registry().commit(&self.intent).await? {
                    Allocation::Hold(hold) => *hold,
                    _ => {
                        return Err(CellGroupArchiveError::Invalid(
                            "publication allocation purpose",
                        ))
                    }
                };
                self.source.check()?;
                Ok(CellGroupArchiveCoverage(Arc::new(Coverage {
                    hold,
                    verified: self.verified,
                })))
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

struct Coverage {
    hold: CellGroupArchiveHold,
    verified: VerifiedCellGroupArchiveRecovery,
}

/// Historical complete data coverage with an explicit remote hold. Clones keep
/// local verification admission/pins. The remote hold is retired explicitly by
/// its durable owner after joined use; this grants no quorum or serving right.
#[derive(Clone)]
pub struct CellGroupArchiveCoverage(Arc<Coverage>);

impl CellGroupArchiveCoverage {
    pub fn hold(&self) -> &CellGroupArchiveHold {
        &self.0.hold
    }

    pub fn recovery(&self) -> &VerifiedCellGroupArchiveRecovery {
        &self.0.verified
    }

    pub fn marker(&self) -> &GroupManifest {
        self.0.verified.manifest().marker()
    }
}

impl CellGroupArchiveUpload {
    /// Recover every dependency and replay every entry, then prepare one exact
    /// conditional publication intent. Missing B prevents coverage of later A.
    /// Callbacks, engine installation and S3 command settlement do not run here.
    pub async fn prepare_publication(
        &self,
        domain: &ExecutionDomain,
        tails: &CellGroupArchiveTailPool,
        uploaded: &CellGroupArchiveUploadedTail,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<PreparedCellGroupArchivePublication> {
        domain
            .run(async {
                let source = uploaded.source.clone();
                source.check()?;
                if source.root.marker().route() != self.registry().route() {
                    return Err(CellGroupArchiveError::Invalid(
                        "publication registry/source route",
                    ));
                }
                {
                    let _io = self.admit()?;
                    self.registry().pending(&self.ticket()).await?;
                }
                let reference = uploaded.reference();
                let verified = self.verify_tail(domain, tails, reference, receiver).await?;
                if verified.manifest().marker() != source.root.marker()
                    || verified.configuration() != &source.configuration
                    || verified.membership() != &source.membership
                {
                    return Err(CellGroupArchiveError::Invalid(
                        "archive publication exact cut",
                    ));
                }
                source.check()?;
                let io = self.admit()?;
                let root = Root {
                    domain: ROOT_DOMAIN.into(),
                    marker: source.root.marker().clone(),
                    tail: reference.clone(),
                };
                let object = self
                    .put_generation(
                        self.registry().id(),
                        self.ticket().generation(),
                        "roots",
                        serde_json::to_vec(&root).map_err(|_| CellGroupArchiveError::Limit)?,
                        8192,
                    )
                    .await?;
                let descriptor = Descriptor {
                    object,
                    marker: root.marker,
                };
                let graph = Graph::load(
                    self,
                    domain,
                    &descriptor,
                    tails.limits,
                    io.clone(),
                    self.retained(),
                    self.max_objects(),
                )
                .await?;
                self.allows(&graph)?;
                let intent = self
                    .registry()
                    .prepare_publish(self.ticket(), descriptor)
                    .await?;
                Ok(PreparedCellGroupArchivePublication {
                    upload: self.clone(),
                    intent,
                    verified,
                    source,
                })
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

impl CellGroupArchiveRegistry {
    /// Reconcile only a publication already present under its exact persisted
    /// intent. This never sends a publishing CAS or substitutes a newer head.
    pub async fn reconcile_publication(
        &self,
        domain: &ExecutionDomain,
        intent: &CellGroupArchiveIntent,
        tails: &CellGroupArchiveTailPool,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<CellGroupArchiveCoverage> {
        let hold = domain
            .run(async {
                let _io = self.archive.admit()?;
                self.publication_hold(intent).await
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)??;
        self.verify_hold(domain, &hold, tails, receiver).await
    }

    /// Read only the exact held immutable root, independently recover all data,
    /// and recheck the persistent hold before returning historical coverage.
    pub async fn verify_hold(
        &self,
        domain: &ExecutionDomain,
        hold: &CellGroupArchiveHold,
        tails: &CellGroupArchiveTailPool,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<CellGroupArchiveCoverage> {
        domain
            .run(async {
                let io = self.archive.admit()?;
                let descriptor = self.referenced(hold).await?;
                let bytes = self.archive.get("roots", &descriptor.object, 8192).await?;
                let root: Root = serde_json::from_slice(&bytes)
                    .map_err(|_| CellGroupArchiveError::Invalid("held root encoding"))?;
                if root.domain != ROOT_DOMAIN || root.marker != descriptor.marker {
                    return Err(CellGroupArchiveError::Invalid("held root binding"));
                }
                drop(io);
                let verified = self
                    .archive
                    .verify_tail(domain, tails, &root.tail, receiver)
                    .await?;
                if verified.manifest().marker() != &root.marker {
                    return Err(CellGroupArchiveError::Invalid("held recovery cut"));
                }
                let _io = self.archive.admit()?;
                self.referenced(hold).await?;
                Ok(CellGroupArchiveCoverage(Arc::new(Coverage {
                    hold: hold.clone(),
                    verified,
                })))
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}
