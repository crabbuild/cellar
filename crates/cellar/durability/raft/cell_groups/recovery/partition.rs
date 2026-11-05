// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Copy exact sealed cell data into bounded, non-serving child stores.
use super::{
    super::*,
    import::{copy_image, Stop},
    CellGroupRecoveryLimits, CellGroupRecoveryPool,
};
use crate::application::consensus::cell_routing_key;
use cellar_logic::{
    cell_group::{CellId, GroupRoute},
    consensus::{LogIndex, LogPosition, Term},
};
use cellar_ltx::stream_snapshot::ScratchPool;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

type Result<T> = std::result::Result<T, CellGroupArchiveError>;

/// Dedicated child preparation stores. The store route must be an exact
/// child of the selected transition, in the original plan order.
pub struct CellGroupTransferTarget {
    pub store: CellGroupArchiveStore,
    pub scratch: ScratchPool,
    pub directory: PathBuf,
}

/// Prepared physical data under the child's route. No log, membership,
/// engine or application owner exists at this boundary.
pub struct PreparedCellGroupTransferChild {
    pins: ManifestPins,
    store: CellGroupArchiveStore,
}

impl PreparedCellGroupTransferChild {
    pub fn route(&self) -> &GroupRoute {
        self.store.route()
    }

    pub fn root(&self) -> ManifestRoot {
        self.pins.root()
    }

    pub fn pin_cell(&self, cell: &CellId) -> Option<(&CellManifestLeaf, CellImagePin)> {
        self.pins
            .get(&cell_routing_key(self.route().history.namespace, cell))
    }
}

/// Complete staged partition with both the original source coverage and the
/// new child pins held until joined use ends. The transition owner retains
/// the original remote holds separately until their durable retirement.
pub struct PreparedCellGroupTransfer {
    children: Vec<PreparedCellGroupTransferChild>,
    sources: VerifiedCellGroupTransitionSources,
    _admission: Arc<crate::raft::budget::Permit>,
}

impl PreparedCellGroupTransfer {
    pub fn children(&self) -> &[PreparedCellGroupTransferChild] {
        &self.children
    }

    pub fn sources(&self) -> &VerifiedCellGroupTransitionSources {
        &self.sources
    }
}

impl CellGroupRecoveryPool {
    /// Materialize every child from the directory-selected held parent cuts.
    /// Preserve the logical image, version witness, enrollment and effect
    /// summary. Rebase only the physical installation position to genesis.
    /// A prepared child still requires a separate committed install and activation.
    pub async fn prepare_transfer_partition(
        &self,
        domain: &impl CellGroupArchiveExecution,
        sources: VerifiedCellGroupTransitionSources,
        targets: Vec<CellGroupTransferTarget>,
    ) -> Result<PreparedCellGroupTransfer> {
        domain
            .run(Box::pin(async {
                let admission = self.admit()?;
                let stop = Stop(Arc::new(AtomicBool::new(false)));
                let cancelled = stop.0.clone();
                let limits = self.limits;
                let work = domain
                    .blocking(move || {
                        let children = partition(&sources, targets, limits, &cancelled)?;
                        Ok(PreparedCellGroupTransfer {
                            children,
                            sources,
                            _admission: admission,
                        })
                    })
                    .map_err(|_| CellGroupArchiveError::Stopped)?;
                work.await.map_err(CellGroupArchiveError::Worker)?
            }))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

fn partition(
    sources: &VerifiedCellGroupTransitionSources,
    targets: Vec<CellGroupTransferTarget>,
    limits: CellGroupRecoveryLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<PreparedCellGroupTransferChild>> {
    if targets.len() != sources.children().len()
        || targets
            .iter()
            .zip(sources.children())
            .any(|(target, route)| target.store.route() != route || !target.directory.is_absolute())
    {
        return Err(CellGroupArchiveError::Invalid(
            "transition child destinations",
        ));
    }
    let check = || {
        if cancelled.load(Ordering::Acquire) {
            return Err(CellGroupArchiveError::Stopped);
        }
        Ok(())
    };
    check()?;
    let mut total_cells = 0usize;
    let mut total_bytes = 0u64;
    for source in sources.sources() {
        let root = source.recovery().manifest();
        total_cells = total_cells
            .checked_add(root.pinned_images())
            .filter(|n| *n <= limits.max_cells as usize)
            .ok_or(CellGroupArchiveError::Limit)?;
        root.images.visit(&mut |_, leaf, _, _, _| -> Result<()> {
            check()?;
            if leaf.geometry.length > limits.max_image_bytes {
                return Err(CellGroupArchiveError::Limit);
            }
            total_bytes = total_bytes
                .checked_add(leaf.geometry.length)
                .filter(|bytes| *bytes <= limits.max_restore_bytes)
                .ok_or(CellGroupArchiveError::Limit)?;
            Ok(())
        })?;
    }
    // Both input and output ranges are ordered by the rooted plan. Lock each
    // private child store in that order without waiting behind another owner.
    let mut stores = Vec::with_capacity(targets.len());
    for target in &targets {
        target.store.require_live()?;
        stores.push(
            target
                .store
                .storage
                .try_lock()
                .map_err(|error| match error {
                    std::sync::TryLockError::WouldBlock => CellGroupArchiveError::Busy,
                    std::sync::TryLockError::Poisoned(_) => {
                        CellGroupArchiveError::Invalid("poisoned transfer destination")
                    }
                })?,
        );
    }
    let mut pins = vec![ManifestPins::default(); targets.len()];
    let genesis = LogPosition {
        index: LogIndex::new(1),
        term: Term::new(1),
    };
    let mut child = 0;
    for source in sources.sources() {
        let source_store = source.recovery().checkpoint().checkpoint().store();
        source_store.require_live()?;
        source
            .recovery()
            .manifest()
            .images
            .visit(&mut |_, old, pin, _, _| -> Result<()> {
                let key =
                    cell_routing_key(old.version.history.namespace, &old.version.history.cell);
                while targets
                    .get(child)
                    .is_some_and(|target| !target.store.route().range.contains(&key))
                {
                    child += 1;
                }
                let target = targets
                    .get(child)
                    .ok_or(CellGroupArchiveError::Invalid("unassigned transition cell"))?;
                let live = || {
                    check()?;
                    source_store.require_live()?;
                    target.store.require_live()?;
                    Ok(())
                };
                live()?;
                let apply::Storage {
                    tree,
                    pages,
                    images,
                    ..
                } = &mut *stores[child];
                let mut leaf = old.clone();
                leaf.last_mutation = genesis;
                let pin = copy_image(
                    images,
                    pin,
                    &mut leaf,
                    &target.scratch,
                    &target.directory,
                    &live,
                )?;
                if leaf.version != old.version {
                    return Err(CellGroupArchiveError::Invalid(
                        "transfer changed cell image",
                    ));
                }
                pin.check(&leaf, target.store.route().schema_digest)
                    .map_err(CellGroupCheckpointError::from)?;
                let root = tree
                    .update(pages, pins[child].root(), leaf.clone(), None)
                    .map_err(CellGroupCheckpointError::from)?;
                pins[child] = pins[child]
                    .updated(tree, pages, root, &leaf, &pin)
                    .map_err(CellGroupCheckpointError::from)?;
                Ok(())
            })?;
    }
    for ((store, pins), target) in stores.iter_mut().zip(&pins).zip(&targets) {
        check()?;
        target.store.require_live()?;
        store
            .tree
            .verify(&store.pages, pins.root(), genesis, |_| Ok(()))
            .map_err(CellGroupCheckpointError::from)?;
        store
            .pages
            .sync_directory()
            .map_err(CellGroupCheckpointError::from)?;
    }
    if pins.iter().map(ManifestPins::len).sum::<usize>() != total_cells {
        return Err(CellGroupArchiveError::Invalid(
            "transition partition cell count",
        ));
    }
    drop(stores);
    Ok(targets
        .into_iter()
        .zip(pins)
        .map(|(target, pins)| PreparedCellGroupTransferChild {
            pins,
            store: target.store,
        })
        .collect())
}
