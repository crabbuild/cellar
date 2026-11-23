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
    configuration: Vec<u8>,
    membership: GroupMembership,
    _reservation: Arc<Permit>,
}

impl SourceCut {
    pub(super) fn from_capture(
        group: GroupLog<GroupHistory>,
        root: PreparedGroupManifest,
        configuration: ConfState,
        membership: GroupMembership,
        source_budget: Arc<Budget>,
    ) -> Result<Self> {
        root.images.visit(&mut |_, _, pin, _, _| -> Result<()> {
        if matches!(pin, super::super::CellImagePin::Extents(pin) if pin.reference().encoded_length() > 1024 * 1024) {
            return Err(CellGroupArchiveError::Limit);
        }
        Ok(())
    })?;
        // Replay rebuilds ConfState through raft-rs, whose set vectors may use
        // a different order. Retain the complete validated canonical encoding;
        // vector order must not permanently stop a valid archive publisher.
        let configuration = crate::raft::manifest::encode_conf(&configuration)
            .map_err(|_| CellGroupArchiveError::Invalid("tail configuration encoding"))?;
        let mut source_bytes = root
            .reserved_bytes()
            .checked_add(
                configuration
                    .len()
                    .checked_mul(4)
                    .ok_or(CellGroupArchiveError::Limit)?,
            )
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or(CellGroupArchiveError::Limit)?;
        source_bytes = source_bytes
            .checked_add(
                serde_json::to_vec(&membership)
                    .map_err(|_| CellGroupArchiveError::Limit)?
                    .len()
                    .checked_mul(4)
                    .ok_or(CellGroupArchiveError::Limit)?,
            )
            .ok_or(CellGroupArchiveError::Limit)?;
        root.images.visit(&mut |_, _, pin, _, _| -> Result<()> {
            if let super::super::CellImagePin::Extents(pin) = pin {
                source_bytes = source_bytes
                    .checked_add(
                        (pin.reference().encoded_length() as usize)
                            .checked_mul(2)
                            .ok_or(CellGroupArchiveError::Limit)?,
                    )
                    .ok_or(CellGroupArchiveError::Limit)?;
            }
            Ok(())
        })?;
        let source_reservation = source_budget
            .acquire(source_bytes)
            .map(Arc::new)
            .ok_or(CellGroupArchiveError::Busy)?;
        Ok(Self {
            group,
            root,
            configuration,
            membership,
            _reservation: source_reservation,
        })
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
    payload_root: Descriptor,
}

impl PreparedCellGroupArchivePublication {
    pub fn intent(&self) -> &CellGroupArchiveIntent {
        &self.intent
    }

    pub async fn commit(
        self,
        domain: &impl CellGroupArchiveExecution,
    ) -> Result<CellGroupArchiveCoverage> {
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
                self.upload
                    .registry()
                    .pin_payloads(&self.verified, &self.payload_root, true)
                    .await?;
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

/// Opaque physical-verification identity for scheduling. It grants no
/// coverage and retains neither the hold nor physical replay admission.
#[derive(Clone)]
pub(in crate::raft::cell_groups) struct CoverageIdentity(std::sync::Weak<Coverage>);

impl CoverageIdentity {
    pub(in crate::raft::cell_groups) fn same_snapshot(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
pub(in crate::raft::cell_groups) struct CoverageUse {
    hold: CellGroupArchiveHold,
    coverage: std::sync::Weak<Coverage>,
}

impl CoverageUse {
    pub(in crate::raft::cell_groups) fn marker(&self) -> &GroupManifest {
        self.hold.marker()
    }

    pub(in crate::raft::cell_groups) fn retain_coverage(&self) -> Option<CellGroupArchiveCoverage> {
        self.coverage.upgrade().map(CellGroupArchiveCoverage)
    }

    pub(in crate::raft::cell_groups) fn same_snapshot(&self, other: &Self) -> bool {
        self.hold == other.hold && std::sync::Weak::ptr_eq(&self.coverage, &other.coverage)
    }

    pub(in crate::raft::cell_groups) fn retains(&self, hold: &CellGroupArchiveHold) -> bool {
        &self.hold == hold && self.coverage.strong_count() != 0
    }

    pub(in crate::raft::cell_groups) fn live(&self) -> bool {
        self.coverage.strong_count() != 0
    }
}

/// Historical complete data coverage with an explicit remote hold. Clones keep
/// local verification admission/pins. The remote hold is retired explicitly by
/// its durable owner after joined use; this grants no quorum or serving right.
#[derive(Clone)]
pub struct CellGroupArchiveCoverage(Arc<Coverage>);

/// A thin observation of actual verification custody. Unlike a hold descriptor,
/// it cannot outlive physical verification and cannot mint coverage on upgrade.
pub(in crate::raft::cell_groups) struct WeakCoverage(std::sync::Weak<Coverage>);

impl WeakCoverage {
    pub(in crate::raft::cell_groups) fn retain(&self) -> Option<CellGroupArchiveCoverage> {
        self.0.upgrade().map(CellGroupArchiveCoverage)
    }
}

impl CellGroupArchiveCoverage {
    pub(in crate::raft::cell_groups) fn identity(&self) -> CoverageIdentity {
        CoverageIdentity(Arc::downgrade(&self.0))
    }

    pub(in crate::raft::cell_groups) fn downgrade(&self) -> WeakCoverage {
        WeakCoverage(Arc::downgrade(&self.0))
    }

    pub(in crate::raft) fn retain_transition_source(mut self) -> Result<Self> {
        Arc::get_mut(&mut self.0)
            .ok_or(CellGroupArchiveError::Busy)?
            .verified
            .retain_transition_source()?;
        Ok(self)
    }

    pub(in crate::raft::cell_groups) fn retain_use(&self) -> CoverageUse {
        CoverageUse {
            hold: self.0.hold.clone(),
            coverage: Arc::downgrade(&self.0),
        }
    }

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
        domain: &impl CellGroupArchiveExecution,
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
                // The upload pool already reserves this bounded dependency
                // graph. Populate it from the exact indices consumed by the
                // independent physical recovery, rather than downloading them
                // a second time after writing the root. Failed verification
                // cannot return a graph or a publication capability.
                let mut graph = Graph::new(self.max_objects());
                let verified = self
                    .verify_tail_recorded(domain, tails, reference, receiver, Some(&mut graph))
                    .await?;
                // raft-rs emits these sets in hash iteration order. Compare
                // the same validated canonical encoding used by archive replay;
                // order alone must not refuse an independently applied cut.
                let configuration = crate::raft::manifest::encode_conf(verified.configuration())
                    .map_err(|_| {
                        CellGroupArchiveError::Invalid("replayed configuration encoding")
                    })?;
                if verified.manifest().marker() != source.root.marker()
                    || configuration != source.configuration
                    || verified.membership() != &source.membership
                {
                    return Err(CellGroupArchiveError::Invalid(
                        "archive publication exact cut",
                    ));
                }
                source.check()?;
                let _io = self.admit()?;
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
                graph.confirm_root(self, &descriptor, &verified).await?;
                self.allows(&graph)?;
                self.registry()
                    .pin_payloads(&verified, &descriptor, false)
                    .await?;
                let payload_root = descriptor.clone();
                let intent = self
                    .registry()
                    .prepare_publish(self.ticket(), descriptor)
                    .await?;
                Ok(PreparedCellGroupArchivePublication {
                    upload: self.clone(),
                    intent,
                    verified,
                    source,
                    payload_root,
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
        domain: &impl CellGroupArchiveExecution,
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
        domain: &impl CellGroupArchiveExecution,
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
                // A historical hold can retire concurrently with verification.
                // Only a newly selected publication may adopt command/upload
                // ownership; reading an older cut must not retire a live turn.
                self.pin_payloads(&verified, &descriptor, false).await?;
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
