// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Two already committed, independent cell edits may share page preparation.
//! Their selectors, apply tickets and durable publication remain individual.

use super::super::root::PreparedCellUpdate;
use super::*;
use crate::application::consensus::group_protocol::{CellGroupEntry, GroupCommand};
use crate::raft::performance::{Counter, Operation};
use protobuf::Message as _;
use raft::prelude::Entry;

pub(crate) struct PrefetchedApply {
    pub(crate) entry: Entry,
    pub(crate) prepared: PreparedGroupManifest,
    pub(crate) reservation: Arc<crate::raft::budget::Permit>,
}

pub(crate) struct ApplyPreparation {
    pub(crate) first: PreparedGroupManifest,
    pub(crate) next: Option<PrefetchedApply>,
    pub(crate) reused: bool,
}

struct PreparedSuccessor {
    entry: Entry,
    prepared_image: Option<Arc<crate::raft::execution::CellPreparedImage>>,
}

impl CellGroupApplier {
    fn preparation_storage(&self) -> Result<std::sync::MutexGuard<'_, Storage>, ExecutionError> {
        self.storage.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => ExecutionError::Busy,
            std::sync::TryLockError::Poisoned(_) => {
                ExecutionError::Invalid("poisoned group applier")
            }
        })
    }

    fn prefetch_reservation_bytes(
        &self,
        work: &CellGroupApply,
    ) -> Result<Option<usize>, ExecutionError> {
        let Some(next) = work.ticket.prefetch.as_ref().and_then(|slot| slot.get()) else {
            return Ok(None);
        };
        let owner = self.preparation_storage()?;
        let bytes = work
            .base
            .reserved_bytes()
            .checked_mul(2)
            .and_then(|n| {
                next.encoded_bytes
                    .checked_mul(2)
                    .and_then(|input| n.checked_add(input))
            })
            .and_then(|n| n.checked_add(owner.images.memory_bound()))
            .and_then(|n| n.checked_add(super::super::page_store::DIRECTORY_BATCH_BYTES * 2))
            .and_then(|n| n.checked_add(65536))
            .ok_or(ExecutionError::Invalid("prefetch reservation overflow"))?;
        Ok(Some(bytes))
    }

    pub(crate) fn prepare(
        &self,
        work: &CellGroupApply,
        performance: &Arc<crate::raft::performance::Registry>,
        cached: Option<PrefetchedApply>,
        reserve: impl FnOnce(usize) -> Result<Option<Arc<crate::raft::budget::Permit>>, ExecutionError>,
    ) -> Result<ApplyPreparation, ExecutionError> {
        self.check(work)?;
        if work.ticket.prepared_image.as_ref().is_some_and(|image| {
            !image.accepts(
                &self.group.owner,
                self.history(),
                work.ticket.position().term,
                &work.entry.data,
            )
        }) {
            return Err(ExecutionError::Invalid(
                "committed preparation input/owner binding",
            ));
        }
        let observations = ApplyObservations(Some(performance));
        if let Some(cached) = cached {
            if cached.entry == work.entry
                && cached.prepared.base.as_ref() == Some(work.base.marker())
                && cached.prepared.marker().position() == work.ticket.position()
                && Arc::ptr_eq(&cached.prepared.engine_owner, &self.group.owner)
            {
                let owner = self.preparation_storage()?;
                if Arc::ptr_eq(&owner.pages.owner, &cached.prepared.page_owner) {
                    owner.recheck_prefetch(work, &cached.prepared, observations)?;
                    drop(owner);
                    self.check(work)?;
                    return Ok(ApplyPreparation {
                        first: cached.prepared,
                        next: None,
                        reused: true,
                    });
                }
            }
        }
        // Image I/O often outlives the next commit. Sample the live hint only
        // after this first image and its effect summary are completely ready.
        // No successor body or pin can be obtained before the reserve callback.
        let first = self
            .preparation_storage()?
            .prepare_first_cell(work, observations)?;
        let Some(first) = first else {
            return Ok(ApplyPreparation {
                first: self.prepare_single(work, performance)?,
                next: None,
                reused: false,
            });
        };
        self.check(work)?;
        let reservation = match self.prefetch_reservation_bytes(work)? {
            Some(bytes) => reserve(bytes)?,
            None => {
                performance.increment(Counter::CellApplyPrefetchNoSuccessor, 1);
                None
            }
        };
        let next = if reservation.is_some() {
            self.prepared_successor(work)?
        } else {
            None
        };
        let mut owner = self.preparation_storage()?;
        let following = match next.as_ref() {
            Some(next) => owner.prepare_successor(work, &first, next, observations)?,
            None => None,
        };
        let (first, next) = if let Some(second) = following {
            let next = next.ok_or(ExecutionError::Invalid("prepared successor missing"))?;
            let reservation = reservation.ok_or(ExecutionError::Invalid(
                "prepared successor admission missing",
            ))?;
            let Storage { tree, pages, .. } = &mut *owner;
            let [first, prepared] = observations.measure(Operation::SqlManifestPrepare, || {
                tree.prepare_pair(pages, &work.base, [first, second])
                    .map_err(storage)
            })?;
            (
                first,
                Some(PrefetchedApply {
                    entry: next.entry,
                    prepared,
                    reservation,
                }),
            )
        } else {
            if reservation.is_some() {
                performance.increment(Counter::CellApplyPrefetchSkipped, 1);
            }
            (
                owner.finish_cell_update(&work.base, first, observations)?,
                None,
            )
        };
        drop(owner);
        self.check(work)?;
        Ok(ApplyPreparation {
            first,
            next,
            reused: false,
        })
    }

    // The callback has already anchored the extra reservation outside the
    // physical task, through its actual join, before this copies or pins input.
    fn prepared_successor(
        &self,
        work: &CellGroupApply,
    ) -> Result<Option<PreparedSuccessor>, ExecutionError> {
        let Some(next) = work.ticket.prefetch.as_ref().and_then(|slot| slot.get()) else {
            return Ok(None);
        };
        if work.entry.index.checked_add(1) != Some(next.position.index.get())
            || next.position.term.get() < work.entry.term
        {
            return Ok(None);
        }
        let cache = self.group.read().map_err(storage)?;
        let Some(entry) = cache.entries.get(&next.position.index.get()) else {
            return Ok(None);
        };
        if entry_position(entry) != next.position
            || entry.compute_size() as usize > next.encoded_bytes
            || !work.ticket.covers_prefetch(&self.group, &cache, next)
        {
            return Ok(None);
        }
        let entry = entry.clone();
        drop(cache);
        if entry_digest(&entry).map_err(storage)? != next.digest {
            return Ok(None);
        }
        Ok(Some(PreparedSuccessor {
            entry,
            prepared_image: next
                .prepared_image
                .as_ref()
                .and_then(std::sync::Weak::upgrade),
        }))
    }
}

impl Storage {
    fn prepare_first_cell(
        &self,
        work: &CellGroupApply,
        observations: ApplyObservations<'_>,
    ) -> Result<Option<PreparedCellUpdate>, ExecutionError> {
        if work.entry.get_entry_type() != raft::prelude::EntryType::EntryNormal
            || work.entry.data.is_empty()
        {
            return Ok(None);
        }
        let decoded =
            CellGroupEntry::decode(&work.entry.data, work.base.marker().route(), self.limits)
                .map_err(storage)?;
        if !matches!(
            &decoded.command,
            GroupCommand::Mutation(_) | GroupCommand::Enroll { .. }
        ) {
            return Ok(None);
        }
        // Preserve the ordinary page-owner/base checks before any image I/O.
        self.tree
            .prepare_unchanged(&self.pages, &work.base, entry_position(&work.entry))
            .map_err(storage)?;
        self.prepare_cell_update(
            &work.base,
            &work.entry,
            decoded,
            false,
            work.ticket.prepared_image.as_deref(),
            observations,
        )
    }

    fn prepare_successor(
        &self,
        work: &CellGroupApply,
        first: &PreparedCellUpdate,
        next: &PreparedSuccessor,
        observations: ApplyObservations<'_>,
    ) -> Result<Option<PreparedCellUpdate>, ExecutionError> {
        if work.base.marker().root().depth() > 14 {
            return Ok(None);
        }
        let Ok(decoded) =
            CellGroupEntry::decode(&next.entry.data, work.base.marker().route(), self.limits)
        else {
            return Ok(None);
        };
        let cell = match &decoded.command {
            GroupCommand::Mutation(m) if work.base.pin_cell(&m.cell.cell).is_some() => &m.cell.cell,
            GroupCommand::Enroll { cell, .. } if work.base.pin_cell(&cell.cell).is_none() => {
                &cell.cell
            }
            _ => return Ok(None),
        };
        if cell == &first.leaf.version.history.cell {
            return Ok(None);
        }
        if next.prepared_image.as_ref().is_some_and(|image| {
            !image.accepts(
                &work.base.engine_owner,
                work.base.marker().route().history,
                entry_position(&next.entry).term,
                &next.entry.data,
            )
        }) {
            return Err(ExecutionError::Invalid(
                "prefetch image input/owner binding",
            ));
        }
        self.prepare_cell_update(
            &work.base,
            &next.entry,
            decoded,
            false,
            next.prepared_image.as_deref(),
            observations,
        )
    }

    fn recheck_prefetch(
        &self,
        work: &CellGroupApply,
        prepared: &PreparedGroupManifest,
        observations: ApplyObservations<'_>,
    ) -> Result<(), ExecutionError> {
        let decoded =
            CellGroupEntry::decode(&work.entry.data, work.base.marker().route(), self.limits)
                .map_err(storage)?;
        let cell = match decoded.command {
            GroupCommand::Mutation(m) => m.cell.cell,
            GroupCommand::Enroll { cell, .. } => cell.cell,
            _ => return Err(ExecutionError::Invalid("prefetch command kind")),
        };
        let (leaf, pin) = prepared
            .pin_cell(&cell)
            .ok_or(ExecutionError::Invalid("prefetch cell missing"))?;
        observations.measure(Operation::SqlManifestRecheck, || {
            if self
                .tree
                .get(&self.pages, prepared.marker().root(), &cell)
                .map_err(storage)?
                .as_ref()
                != Some(leaf)
            {
                return Err(ExecutionError::Invalid("prefetch page bytes changed"));
            }
            Ok(())
        })?;
        // The exact committed input produced this summary. Recheck the actual
        // immutable bytes and identity, so cached pins cannot hide corruption.
        observations.measure(Operation::SqlImagePrepare, || {
            self.images.recheck_prefetched_pin(&pin)
        })
    }
}

impl CellGroupApplyImages {
    fn recheck_prefetched_pin(&self, pin: &CellImagePin) -> Result<(), ExecutionError> {
        match (self, pin) {
            (_, CellImagePin::Empty) => Ok(()),
            (Self::Materialized { images, .. }, CellImagePin::Materialized(pin))
                if images.owns(pin) =>
            {
                images.recheck_owned(pin).map_err(storage)
            }
            (Self::Extents { images, .. }, CellImagePin::Extents(pin))
                if images.owns_image(pin) =>
            {
                images
                    .seal_image_root(
                        pin.manifest().clone(),
                        &mut vec![0; cellar_ltx::internal::EXTENT_BYTES as usize],
                    )
                    .map_err(storage)?;
                Ok(())
            }
            _ => Err(ExecutionError::Invalid("prefetch image owner")),
        }
    }
}
