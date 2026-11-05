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
        Arc, OnceLock,
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
    data: ChildData,
    store: CellGroupArchiveStore,
}

enum ChildData {
    PreCopy(ManifestPins),
    Sealed {
        genesis: Box<PreparedGroupManifest>,
        engine: Arc<OnceLock<Arc<()>>>,
    },
}

impl PreparedCellGroupTransferChild {
    pub(super) fn store(&self) -> &CellGroupArchiveStore {
        &self.store
    }

    pub fn route(&self) -> &GroupRoute {
        self.store.route()
    }

    pub fn root(&self) -> ManifestRoot {
        match &self.data {
            ChildData::PreCopy(pins) => pins.root(),
            ChildData::Sealed { genesis, .. } => genesis.marker().root(),
        }
    }

    pub fn pin_cell(&self, cell: &CellId) -> Option<(&CellManifestLeaf, CellImagePin)> {
        match &self.data {
            ChildData::PreCopy(pins) => {
                pins.get(&cell_routing_key(self.route().history.namespace, cell))
            }
            ChildData::Sealed { genesis, .. } => genesis.pin_cell(cell),
        }
    }

    /// A synced non-serving genesis exists only for the final sealed-source
    /// partition. The archive store owns it until an engine-bound install.
    pub fn genesis(&self) -> Option<&PreparedGroupManifest> {
        match &self.data {
            ChildData::PreCopy(_) => None,
            ChildData::Sealed { genesis, .. } => Some(genesis),
        }
    }

    pub(super) fn install_owner(&self) -> Option<&Arc<OnceLock<Arc<()>>>> {
        match &self.data {
            ChildData::PreCopy(_) => None,
            ChildData::Sealed { engine, .. } => Some(engine),
        }
    }

    /// Open the applier on the exact page store handed to the durable engine.
    /// Recover and verify the selected root on a blocking owner before attach.
    pub fn installed_applier(
        &self,
        permit: &CellGroupBootstrapPermit,
    ) -> std::result::Result<CellGroupApplier, crate::raft::completion::RuntimeError> {
        use crate::raft::completion::RuntimeError;
        let group = permit.group();
        if permit.route() != self.route()
            || !self
                .install_owner()
                .and_then(|engine| engine.get())
                .is_some_and(|engine| Arc::ptr_eq(engine, &group.owner))
        {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(CellGroupApplier {
            group: group.clone(),
            storage: self.store.storage.clone(),
        })
    }
}

/// Complete staged partition with both the original source coverage and the
/// new child pins held until joined use ends. The transition owner retains
/// the original remote holds separately until their durable retirement.
pub struct PreparedCellGroupTransfer {
    partition: StagedCellGroupPartition,
    sources: VerifiedCellGroupTransitionSources,
}

pub(crate) struct StagedCellGroupPartition {
    pub(crate) children: Vec<PreparedCellGroupTransferChild>,
    _admission: Arc<crate::raft::budget::Permit>,
}

impl PreparedCellGroupTransfer {
    pub fn children(&self) -> &[PreparedCellGroupTransferChild] {
        &self.partition.children
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
        let origin = sealed_origin(&sources)?;
        let partition = self
            .stage_partition(
                domain,
                sources.sources().to_vec(),
                sources.children().to_vec(),
                targets.into_iter().map(Some).collect(),
                Some(origin),
            )
            .await?;
        Ok(PreparedCellGroupTransfer { partition, sources })
    }

    /// Use the complete verified partition to prepare only the local child.
    /// Cells in other ranges are validated against the plan without copying them.
    pub(crate) async fn prepare_transfer_child(
        &self,
        domain: &impl CellGroupArchiveExecution,
        sources: &VerifiedCellGroupTransitionSources,
        target: CellGroupTransferTarget,
    ) -> Result<PreparedCellGroupTransferChild> {
        let origin = sealed_origin(sources)?;
        let routes = sources.children();
        let selected = routes
            .iter()
            .position(|route| route == target.store.route())
            .ok_or(CellGroupArchiveError::Invalid("unselected transfer child"))?;
        let mut targets: Vec<_> = (0..routes.len()).map(|_| None).collect();
        targets[selected] = Some(target);
        let mut partition = self
            .stage_partition(
                domain,
                sources.sources().to_vec(),
                routes.to_vec(),
                targets,
                Some(origin),
            )
            .await?;
        partition
            .children
            .pop()
            .ok_or(CellGroupArchiveError::Invalid("missing prepared child"))
    }

    pub(crate) async fn stage_partition(
        &self,
        domain: &impl CellGroupArchiveExecution,
        sources: Vec<CellGroupArchiveCoverage>,
        routes: Vec<GroupRoute>,
        targets: Vec<Option<CellGroupTransferTarget>>,
        origin: Option<GroupTransferOrigin>,
    ) -> Result<StagedCellGroupPartition> {
        domain
            .run(Box::pin(async {
                let admission = self.admit()?;
                let stop = Stop(Arc::new(AtomicBool::new(false)));
                let cancelled = stop.0.clone();
                let limits = self.limits;
                let work = domain
                    .blocking(move || {
                        let children = partition(
                            &sources,
                            &routes,
                            targets,
                            limits,
                            &cancelled,
                            origin.map(|origin| (origin, admission.clone())),
                        )?;
                        Ok(StagedCellGroupPartition {
                            children,
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

fn sealed_origin(sources: &VerifiedCellGroupTransitionSources) -> Result<GroupTransferOrigin> {
    let (operation, plan) = sources.transition_plan();
    let sealed = sources
        .journal()
        .sealed_sources()
        .ok_or(CellGroupArchiveError::Invalid(
            "missing sealed transfer sources",
        ))?;
    Ok(GroupTransferOrigin::new(operation, plan, sealed))
}

fn partition(
    sources: &[CellGroupArchiveCoverage],
    routes: &[GroupRoute],
    targets: Vec<Option<CellGroupTransferTarget>>,
    limits: CellGroupRecoveryLimits,
    cancelled: &AtomicBool,
    transfer: Option<(GroupTransferOrigin, Arc<crate::raft::budget::Permit>)>,
) -> Result<Vec<PreparedCellGroupTransferChild>> {
    if targets.len() != routes.len()
        || targets.iter().zip(routes).any(|(target, route)| {
            target.as_ref().is_some_and(|target| {
                target.store.route() != route || !target.directory.is_absolute()
            })
        })
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
    for source in sources {
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
        let store = if let Some(target) = target {
            target.store.require_live()?;
            Some(
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
            )
        } else {
            None
        };
        stores.push(store);
    }
    let mut pins = vec![ManifestPins::default(); targets.len()];
    let genesis = LogPosition {
        index: LogIndex::new(1),
        term: Term::new(1),
    };
    let mut child = 0;
    let mut selected_cells = 0;
    for source in sources {
        let source_store = source.recovery().checkpoint().checkpoint().store();
        source_store.require_live()?;
        source
            .recovery()
            .manifest()
            .images
            .visit(&mut |_, old, pin, _, _| -> Result<()> {
                let key =
                    cell_routing_key(old.version.history.namespace, &old.version.history.cell);
                while routes
                    .get(child)
                    .is_some_and(|route| !route.range.contains(&key))
                {
                    child += 1;
                }
                let Some(target) = targets
                    .get(child)
                    .ok_or(CellGroupArchiveError::Invalid("unassigned transition cell"))?
                else {
                    return Ok(());
                };
                selected_cells += 1;
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
                } = stores[child]
                    .as_deref_mut()
                    .ok_or(CellGroupArchiveError::Invalid("missing transfer store"))?;
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
    if pins.iter().map(ManifestPins::len).sum::<usize>() != selected_cells {
        return Err(CellGroupArchiveError::Invalid(
            "transition partition cell count",
        ));
    }
    let mut children = Vec::with_capacity(targets.len());
    for ((store, pins), target) in stores.iter_mut().zip(pins).zip(&targets) {
        let (Some(store), Some(target)) = (store.as_deref_mut(), target) else {
            continue;
        };
        check()?;
        target.store.require_live()?;
        let apply::Storage { tree, pages, .. } = store;
        let data = match &transfer {
            Some((origin, admission)) => {
                let marker = GroupManifest::transfer_genesis(
                    target.store.route().clone(),
                    pins.root(),
                    origin.clone(),
                )
                .map_err(CellGroupCheckpointError::from)?;
                let mut prepared = tree
                    .prepare_archive_complete(&target.store.owner, pages, marker, &mut |leaf| {
                        check().map_err(|_| ManifestError::Quarantined)?;
                        let key = cell_routing_key(
                            leaf.version.history.namespace,
                            &leaf.version.history.cell,
                        );
                        pins.get(&key)
                            .filter(|(selected, _)| *selected == leaf)
                            .map(|(_, pin)| pin)
                            .ok_or(ManifestError::Invalid("child transfer image closure"))
                    })
                    .map_err(CellGroupCheckpointError::from)?;
                prepared.recovery_admission = Some(admission.clone());
                ChildData::Sealed {
                    genesis: Box::new(prepared),
                    engine: Arc::new(OnceLock::new()),
                }
            }
            None => {
                tree.verify(pages, pins.root(), genesis, |_| Ok(()))
                    .map_err(CellGroupCheckpointError::from)?;
                pages
                    .sync_directory()
                    .map_err(CellGroupCheckpointError::from)?;
                ChildData::PreCopy(pins)
            }
        };
        children.push(PreparedCellGroupTransferChild {
            data,
            store: target.store.clone(),
        });
    }
    Ok(children)
}
