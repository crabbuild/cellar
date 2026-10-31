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
    reference: CellGroupArchiveTailReference,
    checkpoint: VerifiedCellGroupArchiveCheckpoint,
    root: PreparedGroupManifest,
    configuration: ConfState,
    membership: GroupMembership,
    _reservation: Arc<Permit>,
}

impl VerifiedCellGroupArchiveRecovery {
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
    let applier = checkpoint.checkpoint().applier().clone();
    let selected = applier
        .group
        .group_manifest()
        .map_err(CellGroupCheckpointError::from)?;
    let mut root = checkpoint.checkpoint().manifest().clone();
    let mut conf = checkpoint
        .checkpoint()
        .snapshot()
        .get_metadata()
        .get_conf_state()
        .clone();
    let mut membership = checkpoint.checkpoint().membership().clone();
    let mut owner = applier.storage.try_lock().map_err(|error| match error {
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
        applier
            .group
            .cache_revision()
            .map_err(CellGroupCheckpointError::from)?;
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
    // Recheck every final physical image and page, even after warm replay. An
    // in-memory pin or a cached destination is never a substitute for bytes.
    let Storage {
        tree,
        pages,
        images,
        ..
    } = &mut *owner;
    let verified = tree
        .prepare_complete(
            &applier.group,
            pages,
            root.marker().root(),
            root.marker().position(),
            root.marker().membership_revision(),
            |leaf| {
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
                    (
                        CellGroupApplyImages::Materialized { .. },
                        CellImagePin::Materialized(pin),
                    ) => {
                        pin.verify_and_sync().map_err(|e| {
                            super::super::ManifestError::Io(std::io::Error::other(e))
                        })?;
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
                            .map_err(|e| {
                                super::super::ManifestError::Io(std::io::Error::other(e))
                            })?;
                    }
                    _ => {
                        return Err(super::super::ManifestError::Invalid(
                            "replay representation",
                        ))
                    }
                }
                Ok(pin)
            },
        )
        .map_err(CellGroupCheckpointError::from)?;
    if cancelled.load(Ordering::Acquire) {
        return Err(CellGroupArchiveError::Stopped);
    }
    if applier
        .group
        .group_manifest()
        .map_err(CellGroupCheckpointError::from)?
        != selected
    {
        return Err(CellGroupArchiveError::Invalid(
            "replay destination selection changed",
        ));
    }
    drop(owner);
    Ok(VerifiedCellGroupArchiveRecovery {
        reference,
        checkpoint,
        root: verified,
        configuration: conf,
        membership,
        _reservation: reservation,
    })
}
