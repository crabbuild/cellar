// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Non-serving data replay. Never constructs Raft tickets, changes a hard state,
//! installs a snapshot, invokes application callbacks or grants a quorum fence.
use super::super::{apply::Storage, CellGroupApplyImages, CellImagePin, PreparedGroupManifest};
use super::{tail::Cut, *};
use crate::raft::member_state::GroupMembership;
use raft::prelude::{ConfState, Entry, EntryType};
use std::sync::atomic::{AtomicBool, Ordering};

/// Complete receiver-local state after independently replaying the selected
/// checkpoint and contiguous tail. Keeps physical pins and replay admission;
/// this supplies no published head, GC lease, serving right or S3 settlement.
pub struct VerifiedCellGroupArchiveRecovery {
    pub(super) payloads: Vec<super::payloads::VerifiedDependency>,
    pub(super) payload_archive: Option<CellGroupArchive>,
    reference: CellGroupArchiveTailReference,
    checkpoint: VerifiedCellGroupArchiveCheckpoint,
    root: PreparedGroupManifest,
    configuration: ConfState,
    membership: GroupMembership,
    _reservation: Option<Arc<Permit>>,
    _retained: Option<crate::raft::budget::RetainedBytes>,
}

impl VerifiedCellGroupArchiveRecovery {
    /// Publication has finished all independent replay before entering its CAS.
    /// Preserve immutable custody and its original byte banks while releasing
    /// the now-joined replay and receiver operations to another group.
    pub(super) fn retain_publication(&mut self) -> Result<()> {
        self.checkpoint.checkpoint.retain_publication()?;
        self.retain_transition_source()
    }

    /// Restore ordinary proof custody after publication. A competing replay
    /// refuses this handoff; the durable publishing intent still reconciles.
    pub(super) fn resume_publication(&mut self) -> Result<()> {
        self.checkpoint.checkpoint.resume_publication()?;
        if let Some(retained) = self._retained.take() {
            match retained.try_resume() {
                Ok(reservation) => self._reservation = Some(Arc::new(reservation)),
                Err(retained) => {
                    self._retained = Some(retained);
                    return Err(CellGroupArchiveError::Busy);
                }
            }
        }
        Ok(())
    }

    /// Transition staging reads immutable verified roots. Release the joined
    /// replay lane, retaining conservative charges for both roots, extent
    /// manifests, membership and configuration in the same tail byte bank.
    pub(super) fn retain_transition_source(&mut self) -> Result<()> {
        let base = self.checkpoint.checkpoint().manifest();
        let cells = base
            .pinned_images()
            .checked_add(self.root.pinned_images())
            .ok_or(CellGroupArchiveError::Limit)?;
        let bytes = cells
            .checked_mul(1024 * 1024)
            .and_then(|n| n.checked_add(base.images.reserved_bytes()))
            .and_then(|n| n.checked_add(self.root.images.reserved_bytes()))
            .and_then(|n| n.checked_add(1024 * 1024))
            .and_then(|n| n.checked_add(self.payloads.len().checked_mul(4096)?))
            .ok_or(CellGroupArchiveError::Limit)?;
        let Some(reservation) = self._reservation.take() else {
            return Ok(());
        };
        let reservation = match Arc::try_unwrap(reservation) {
            Ok(reservation) => reservation,
            Err(reservation) => {
                self._reservation = Some(reservation);
                return Err(CellGroupArchiveError::Busy);
            }
        };
        self._retained = Some(
            reservation
                .retain_bytes(bytes)
                .ok_or(CellGroupArchiveError::Limit)?,
        );
        Ok(())
    }

    pub fn reference(&self) -> &CellGroupArchiveTailReference {
        &self.reference
    }

    pub fn checkpoint(&self) -> &VerifiedCellGroupArchiveCheckpoint {
        &self.checkpoint
    }

    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.root
    }

    pub fn configuration(&self) -> &ConfState {
        &self.configuration
    }

    pub fn membership(&self) -> &GroupMembership {
        &self.membership
    }

    /// Static unmanaged recovery only. A fence controller must independently
    /// exclude later joins and retired hosts from its deployment inventory.
    pub(crate) fn check_disaster_target(
        &self,
        target: &crate::raft::catalog::CellGroupBirthRequest,
    ) -> Result<()> {
        if !self.configuration.voters_outgoing.is_empty()
            || !self.configuration.learners_next.is_empty()
            || self.configuration.auto_leave
            || self
                .membership
                .repair_status()
                .map_err(CellGroupCheckpointError::from)?
                .is_some()
            || self.membership.fleet_status().is_some()
        {
            return Err(CellGroupArchiveError::Invalid(
                "recovery requires static unmanaged membership",
            ));
        }
        for member in target.voters().iter().chain(target.learners()) {
            if member.member.get() <= self.membership.high_water()
                || self.membership.has_node_binding(member.node)
            {
                return Err(CellGroupArchiveError::Invalid(
                    "reused recovery member or node",
                ));
            }
        }
        self.check_recovery_cell_incarnation(target.route().history.incarnation)
    }

    pub(crate) fn check_recovery_cell_incarnation(
        &self,
        incarnation: cellar_logic::consensus::IncarnationId,
    ) -> Result<()> {
        self.root.images.visit(&mut |_, leaf, _, _, _| {
            if leaf.version.history.incarnation == incarnation {
                return Err(CellGroupArchiveError::Invalid(
                    "reused recovery cell incarnation",
                ));
            }
            Ok(())
        })
    }
}

pub(super) fn replay(
    reference: CellGroupArchiveTailReference,
    checkpoint: VerifiedCellGroupArchiveCheckpoint,
    cut: Cut,
    entries: Vec<Entry>,
    limits: CellGroupArchiveTailLimits,
    reservation: Arc<Permit>,
    cancelled: Arc<AtomicBool>,
) -> Result<VerifiedCellGroupArchiveRecovery> {
    let store = checkpoint.checkpoint().store().clone();
    store.require_live()?;
    let mut root = checkpoint.checkpoint().manifest().clone();
    if !store.owner.owns(&root) {
        return Err(CellGroupArchiveError::Invalid(
            "archive replay physical owner",
        ));
    }
    let mut conf = checkpoint
        .checkpoint()
        .snapshot()
        .get_metadata()
        .get_conf_state()
        .clone();
    let mut membership = checkpoint.checkpoint().membership().clone();
    let mut owner = store.storage.try_lock().map_err(|error| match error {
        std::sync::TryLockError::WouldBlock => CellGroupArchiveError::Busy,
        std::sync::TryLockError::Poisoned(_) => {
            CellGroupArchiveError::Invalid("poisoned archive replay owner")
        }
    })?;
    if owner.images.memory_bound() > limits.max_apply_bytes
        || root.pinned_images() > limits.max_cells as usize
    {
        return Err(CellGroupArchiveError::Limit);
    }
    for entry in &entries {
        if cancelled.load(Ordering::Acquire) {
            return Err(CellGroupArchiveError::Stopped);
        }
        store.require_live()?;
        // Control replay is the same canonical pure transition used by the live
        // owner; cell bytes still go through the ordinary LTX/base-chain checks.
        let change = if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
            Some(
                super::super::membership::transition(
                    entry,
                    root.marker().route(),
                    &conf,
                    &membership,
                )
                .map_err(CellGroupCheckpointError::from)?,
            )
        } else {
            None
        };
        root = owner
            .prepare_entry(&root, entry, Some((&conf, &membership)), true)
            .map_err(CellGroupArchiveError::Replay)?;
        if root.pinned_images() > limits.max_cells as usize {
            return Err(CellGroupArchiveError::Limit);
        }
        if let Some(change) = change {
            conf = change.conf;
            membership = change.membership;
        }
    }
    if cancelled.load(Ordering::Acquire) {
        return Err(CellGroupArchiveError::Stopped);
    }
    let encoded = crate::raft::manifest::encode_conf(&conf)
        .map_err(|_| CellGroupArchiveError::Invalid("replayed configuration"))?;
    if root.marker() != &cut.marker || encoded != cut.conf || membership != cut.membership {
        return Err(CellGroupArchiveError::Invalid("replayed complete cut"));
    }
    let verified = if entries.is_empty() {
        // This invocation just independently received every checkpoint object
        // and verified/synced the complete physical closure, including warm
        // destinations. With no replayed entries, the exact checked cut above
        // is already that receiver's preparation. Keep its actual pins and
        // admission instead of traversing/sealing the same closure again.
        root
    } else {
        // Recheck every final physical image and page after replay, including
        // cached results. Replayed bytes need their own complete preparation.
        let Storage {
            tree,
            pages,
            images,
            ..
        } = &mut *owner;
        tree.prepare_archive_complete(&store.owner, pages, root.marker().clone(), &mut |leaf| {
            if cancelled.load(Ordering::Acquire) {
                return Err(super::super::ManifestError::Invalid(
                    "archive replay stopped",
                ));
            }
            let (_, pin) = root
                .pin_cell(&leaf.version.history.cell)
                .filter(|(expected, _)| *expected == leaf)
                .ok_or(super::super::ManifestError::Invalid(
                    "replayed leaf missing",
                ))?;
            match (&*images, &pin) {
                (_, CellImagePin::Empty) => (),
                (CellGroupApplyImages::Materialized { .. }, CellImagePin::Materialized(pin)) => {
                    pin.verify_and_sync()
                        .map_err(|e| super::super::ManifestError::Io(std::io::Error::other(e)))?;
                }
                (CellGroupApplyImages::Extents { images, .. }, CellImagePin::Extents(pin)) => {
                    if pin.reference().encoded_length() > 1024 * 1024 {
                        return Err(super::super::ManifestError::Invalid(
                            "replay extent metadata limit",
                        ));
                    }
                    images
                        .seal_image_root(
                            pin.manifest().clone(),
                            &mut vec![0; cellar_ltx::internal::EXTENT_BYTES as usize],
                        )
                        .map_err(|e| super::super::ManifestError::Io(std::io::Error::other(e)))?;
                }
                _ => {
                    return Err(super::super::ManifestError::Invalid(
                        "replay representation",
                    ))
                }
            }
            Ok(pin)
        })
        .map_err(CellGroupCheckpointError::from)?
    };
    if cancelled.load(Ordering::Acquire) {
        return Err(CellGroupArchiveError::Stopped);
    }
    store.require_live()?;
    drop(owner);
    Ok(VerifiedCellGroupArchiveRecovery {
        payloads: Vec::new(),
        payload_archive: None,
        reference,
        checkpoint,
        root: verified,
        configuration: conf,
        membership,
        _reservation: Some(reservation),
        _retained: None,
    })
}
