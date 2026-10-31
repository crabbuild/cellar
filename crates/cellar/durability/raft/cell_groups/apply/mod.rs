// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Worker-owned physical preparation; only the Raft owner can publish its result.

pub(super) mod images;
mod recovery;
mod summary;
mod transition;
pub use images::CellGroupApplyImages;

use super::*;
use crate::application::consensus::group_protocol::GroupEntryLimits;
use crate::raft::{
    cache::GroupLog,
    completion::{entry_digest, entry_position},
    execution::{storage, ExecutionError},
};
use cellar_logic::cell_group::GroupHistory;
use std::sync::{Arc, Mutex};

pub(super) struct Storage {
    pub(super) tree: ManifestTree,
    pub(super) pages: ManifestPageStore,
    pub(super) images: CellGroupApplyImages,
    limits: GroupEntryLimits,
    max_effect_rows: u32,
}

/// Owns the already opened manifest directory and shared physical image stores.
/// Construction performs no I/O. Opening/recovering the stores belongs on the
/// owning execution domain; transfer the same page owner used for startup.
#[derive(Clone)]
pub struct CellGroupApplier {
    pub(super) group: GroupLog<GroupHistory>,
    pub(super) storage: Arc<Mutex<Storage>>,
}

impl CellGroupApplier {
    pub fn new(
        group: GroupLog<GroupHistory>,
        pages: ManifestPageStore,
        images: CellGroupApplyImages,
        manifest_limits: ManifestLimits,
        limits: GroupEntryLimits,
        max_effect_rows: u32,
    ) -> Result<Self, ExecutionError> {
        limits.validate().map_err(storage)?;
        images.validate()?;
        if !(1..=1_000_000).contains(&max_effect_rows) {
            return Err(ExecutionError::Invalid("apply effect summary limit"));
        }
        let marker = group
            .group_manifest()
            .map_err(storage)?
            .ok_or(ExecutionError::Invalid("group manifest missing"))?;
        let tree = ManifestTree::new(marker.route().clone(), manifest_limits).map_err(storage)?;
        Ok(Self {
            group,
            storage: Arc::new(Mutex::new(Storage {
                tree,
                pages,
                images,
                limits,
                max_effect_rows,
            })),
        })
    }

    /// Capture on the blocking domain: only the exact retained snapshot from
    /// this applier's engine and page owner may become a network source.
    pub(crate) fn capture_checkpoint(
        &self,
        pool: &CellGroupCheckpointPool,
        snapshot: &::raft::prelude::Snapshot,
    ) -> Result<CellGroupCheckpoint, CellGroupCheckpointError> {
        let root = {
            let storage = self
                .storage
                .lock()
                .map_err(|_| CellGroupCheckpointError::Invalid("source page owner"))?;
            PreparedGroupManifest::checkpoint_from_store(&self.group, &storage.pages)?
        };
        let source = pool.capture_root(&self.group, &root, true)?;
        if source.snapshot()? != *snapshot {
            return Err(CellGroupCheckpointError::Invalid(
                "retained snapshot changed",
            ));
        }
        Ok(source)
    }

    pub fn history(&self) -> GroupHistory {
        self.group.identity().scope
    }

    pub(crate) fn belongs_to_engine(&self, engine: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.group.owner, engine)
    }

    pub(crate) fn check_startup(&self, root: &PreparedGroupManifest) -> Result<(), ExecutionError> {
        let owner = self.storage.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => ExecutionError::Busy,
            std::sync::TryLockError::Poisoned(_) => {
                ExecutionError::Invalid("poisoned registration applier")
            }
        })?;
        if !self.belongs_to_engine(&root.engine_owner)
            || self.history() != root.marker().route().history
            || !Arc::ptr_eq(&owner.pages.owner, &root.page_owner)
            || self.group.group_manifest().map_err(storage)?.as_ref() != Some(root.marker())
        {
            return Err(ExecutionError::Invalid(
                "registration selected root/page owner",
            ));
        }
        root.require_checkpoint(&self.group).map_err(storage)?;
        Ok(())
    }

    pub(crate) fn reservation_bytes(&self, work: &CellGroupApply) -> Result<usize, ExecutionError> {
        self.check(work)?;
        let owner = self.storage.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => ExecutionError::Busy,
            std::sync::TryLockError::Poisoned(_) => {
                ExecutionError::Invalid("poisoned group applier")
            }
        })?;
        work.base
            .reserved_bytes()
            .checked_add(work.entry.data.len())
            .and_then(|n| n.checked_add(owner.images.memory_bound()))
            .and_then(|n| n.checked_add(4096))
            .ok_or(ExecutionError::Invalid("apply reservation overflow"))
    }

    fn check(&self, work: &CellGroupApply) -> Result<(), ExecutionError> {
        if !Arc::ptr_eq(&self.group.owner, &work.base.engine_owner)
            || work.base.marker().route().history != self.history()
            || work.ticket.position() != entry_position(&work.entry)
            || !matches!(
                work.entry.get_entry_type(),
                ::raft::prelude::EntryType::EntryNormal
                    | ::raft::prelude::EntryType::EntryConfChangeV2
            )
        {
            return Err(ExecutionError::Invalid("group apply identity/entry"));
        }
        let cache = self.group.read().map_err(storage)?;
        if cache.group_manifest.as_ref() != Some(work.base.marker())
            || cache.hard_state.commit < work.entry.index
            || cache.entries.get(&work.entry.index) != Some(&work.entry)
            || !cache.membership.permits_group(self.group.identity())
        {
            return Err(ExecutionError::Invalid("group apply committed base"));
        }
        drop(cache);
        if work.ticket.entry_digest() != entry_digest(&work.entry).map_err(storage)? {
            return Err(ExecutionError::Invalid("group apply entry digest"));
        }
        Ok(())
    }

    /// Called only by the shared blocking SQL worker. The try-lock detects an
    /// invalid concurrent owner instead of blocking one worker behind another.
    pub(crate) fn prepare(
        &self,
        work: &CellGroupApply,
    ) -> Result<PreparedGroupManifest, ExecutionError> {
        self.check(work)?;
        let mut owner = self.storage.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => ExecutionError::Busy,
            std::sync::TryLockError::Poisoned(_) => {
                ExecutionError::Invalid("poisoned group applier")
            }
        })?;
        let next = if work.entry.get_entry_type() == ::raft::prelude::EntryType::EntryConfChangeV2 {
            let cache = self.group.read().map_err(storage)?;
            owner.prepare_entry(
                &work.base,
                &work.entry,
                Some((&cache.conf_state, &cache.membership)),
                false,
            )?
        } else {
            owner.prepare_entry(&work.base, &work.entry, None, false)?
        };
        self.check(work)?;
        Ok(next)
    }
}
