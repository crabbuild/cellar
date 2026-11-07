// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reconcile all engine-selected closures before bounded orphan collection.
use super::*;
use crate::raft::{cache::GroupLog, execution::ExecutionError, storage::LogError};
use cellar_logic::cell_group::GroupHistory;
use cellar_ltx::{
    generation::{GenerationError, GenerationStore, ImageId},
    internal::{ExtentRootReference, NodeExtentStore, StoreError, EXTENT_BYTES},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, TryLockError},
};

/// One maintenance request's aggregate bounds, including duplicate retained cuts.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupReclaimLimits {
    pub max_selected_cells: u32,
    pub max_files: usize,
    pub max_page_scan: usize,
}

impl Default for CellGroupReclaimLimits {
    fn default() -> Self {
        Self {
            max_selected_cells: 4096,
            max_files: 256,
            max_page_scan: 256,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellGroupReclaimReport {
    pub groups: usize,
    pub selected_roots: usize,
    pub selected_cells: u32,
    pub pages_scanned: usize,
    pub page_files: usize,
    pub image_files: usize,
}

#[derive(Debug)]
pub enum CellGroupReclaimError {
    Invalid(&'static str),
    Limit,
    Busy,
    Log(LogError),
    Recovery(ExecutionError),
    Manifest(ManifestError),
    Image(GenerationError),
    Extent(StoreError),
}

impl std::fmt::Display for CellGroupReclaimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cell-group storage reclamation: {self:?}")
    }
}

impl std::error::Error for CellGroupReclaimError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Log(e) => Some(e),
            Self::Recovery(e) => Some(e),
            Self::Manifest(e) => Some(e),
            Self::Image(e) => Some(e),
            Self::Extent(e) => Some(e),
            _ => None,
        }
    }
}

type Result<T> = std::result::Result<T, CellGroupReclaimError>;

pub(crate) struct ReclaimCells {
    appliers: Vec<CellGroupApplier>,
    retained: Vec<PreparedGroupManifest>,
    limits: CellGroupReclaimLimits,
}

impl ReclaimCells {
    pub(crate) fn new(
        mut appliers: Vec<CellGroupApplier>,
        retained: Vec<PreparedGroupManifest>,
        limits: CellGroupReclaimLimits,
    ) -> Result<Self> {
        if appliers.is_empty()
            || appliers.len() > 4096
            || retained.len() > 4096
            || !(1..=1_000_000).contains(&limits.max_selected_cells)
            || !(1..=4096).contains(&limits.max_files)
            || !(1..=4096).contains(&limits.max_page_scan)
        {
            return Err(CellGroupReclaimError::Limit);
        }
        appliers.sort_by_key(CellGroupApplier::history);
        if appliers
            .windows(2)
            .any(|a| a[0].history() == a[1].history())
        {
            return Err(CellGroupReclaimError::Invalid("duplicate group applier"));
        }
        Ok(Self {
            appliers,
            retained,
            limits,
        })
    }

    pub(crate) fn reservation_bytes(&self) -> Result<usize> {
        // Full recovered trees, reference inventories, one extent verifier and
        // bounded group/retained-root owners stay charged through physical work.
        ManifestPins::maximum_bytes(self.limits.max_selected_cells)
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.appliers.len().checked_mul(4096)?))
            .and_then(|n| n.checked_add(self.retained.len().checked_mul(1024)?))
            .and_then(|n| n.checked_add(EXTENT_BYTES as usize + 65536))
            .ok_or(CellGroupReclaimError::Limit)
    }

    /// Runs on the engine worker: group creation/publication cannot interleave.
    pub(crate) fn run(
        self,
        engine: &Arc<()>,
        groups: Vec<GroupLog<GroupHistory>>,
    ) -> Result<CellGroupReclaimReport> {
        if groups.len() != self.appliers.len()
            || groups.iter().zip(&self.appliers).any(|(g, a)| {
                g.identity() != a.group.identity()
                    || !Arc::ptr_eq(&g.owner, engine)
                    || !a.belongs_to_engine(engine)
            })
        {
            return Err(CellGroupReclaimError::Invalid(
                "incomplete or foreign engine inventory",
            ));
        }
        let mut owners = self
            .appliers
            .iter()
            .map(|a| {
                a.storage.try_lock().map_err(|e| match e {
                    TryLockError::WouldBlock => CellGroupReclaimError::Busy,
                    TryLockError::Poisoned(_) => {
                        CellGroupReclaimError::Invalid("poisoned group storage")
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut report = CellGroupReclaimReport {
            groups: groups.len(),
            ..Default::default()
        };
        // Refuse the aggregate cell bound before recovery allocates any trees.
        for group in &groups {
            let current = group
                .group_manifest()
                .map_err(CellGroupReclaimError::Log)?
                .ok_or(CellGroupReclaimError::Invalid("selected manifest missing"))?;
            let checkpoint = group
                .checkpoint_manifest()
                .map_err(CellGroupReclaimError::Log)?;
            for marker in std::iter::once(&current).chain(checkpoint.as_ref()) {
                report.selected_cells = report
                    .selected_cells
                    .checked_add(marker.root().cells())
                    .filter(|n| *n <= self.limits.max_selected_cells)
                    .ok_or(CellGroupReclaimError::Limit)?;
                report.selected_roots += 1;
            }
        }
        for root in &self.retained {
            report.selected_cells = report
                .selected_cells
                .checked_add(root.marker().root().cells())
                .filter(|n| *n <= self.limits.max_selected_cells)
                .ok_or(CellGroupReclaimError::Limit)?;
            report.selected_roots += 1;
        }
        let mut roots = Vec::with_capacity(groups.len());
        let mut stores = Vec::<Images>::new();
        for (applier, owner) in self.appliers.iter().zip(&mut owners) {
            let (current, checkpoint) = applier
                .recover_roots(owner)
                .map_err(CellGroupReclaimError::Recovery)?;
            let mut selected = vec![current];
            selected.extend(checkpoint);
            let extras: Vec<_> = self
                .retained
                .iter()
                .filter(|r| r.marker().route().history == applier.history())
                .cloned()
                .collect();
            for extra in &extras {
                if !Arc::ptr_eq(&extra.engine_owner, engine)
                    || !Arc::ptr_eq(&extra.page_owner, &owner.pages.owner)
                {
                    return Err(CellGroupReclaimError::Invalid("retained root owner"));
                }
            }
            selected.extend(extras);
            let index = match stores.iter().position(|s| s.same(&owner.images)) {
                Some(index) => index,
                None => {
                    stores.push(Images::new(&owner.images));
                    stores.len() - 1
                }
            };
            for root in &selected {
                stores[index].retain(root)?;
            }
            roots.push(selected);
        }
        if self.retained.iter().any(|r| {
            !self
                .appliers
                .iter()
                .any(|a| a.history() == r.marker().route().history)
        }) {
            return Err(CellGroupReclaimError::Invalid(
                "retained root outside engine",
            ));
        }
        // No deletion until every selected closure and owner has been checked.
        for store in &stores {
            store.reconcile()?;
        }
        for (applier, owner) in self.appliers.iter().zip(&mut owners) {
            let extras: Vec<_> = self
                .retained
                .iter()
                .filter(|r| r.marker().route().history == applier.history())
                .cloned()
                .collect();
            owner
                .pages
                .reconcile_retained(&extras)
                .map_err(CellGroupReclaimError::Manifest)?;
        }
        let mut files = self.limits.max_files;
        let mut scan = self.limits.max_page_scan;
        for owner in &mut owners {
            if scan == 0 || files == 0 {
                break;
            }
            let pages = owner
                .pages
                .reclaim(scan.min(files))
                .map_err(CellGroupReclaimError::Manifest)?;
            scan -= pages.scanned;
            files -= pages.pages;
            report.pages_scanned += pages.scanned;
            report.page_files += pages.pages;
        }
        for store in &stores {
            if files == 0 {
                break;
            }
            let removed = store.reclaim(files)?;
            files -= removed;
            report.image_files += removed;
        }
        // Retain every recovered tree until all file operations finish.
        drop(roots);
        Ok(report)
    }
}

enum Images {
    Materialized(GenerationStore, BTreeSet<ImageId>),
    Extents(NodeExtentStore, BTreeMap<[u8; 32], ExtentRootReference>),
}

impl Images {
    fn new(images: &CellGroupApplyImages) -> Self {
        match images {
            CellGroupApplyImages::Materialized { images, .. } => {
                Self::Materialized(images.clone(), BTreeSet::new())
            }
            CellGroupApplyImages::Extents { images, .. } => {
                Self::Extents(images.clone(), BTreeMap::new())
            }
        }
    }

    fn same(&self, images: &CellGroupApplyImages) -> bool {
        match (self, images) {
            (Self::Materialized(a, _), CellGroupApplyImages::Materialized { images: b, .. }) => {
                a.same_inventory(b)
            }
            (Self::Extents(a, _), CellGroupApplyImages::Extents { images: b, .. }) => {
                a.same_inventory(b)
            }
            _ => false,
        }
    }

    fn retain(&mut self, root: &PreparedGroupManifest) -> Result<()> {
        root.images.visit(&mut |_, _, pin, _, _| {
            match (&mut *self, pin) {
                (_, CellImagePin::Empty) => (),
                (Self::Materialized(store, refs), CellImagePin::Materialized(pin))
                    if store.owns(pin) =>
                {
                    refs.insert(pin.id());
                }
                (Self::Extents(store, refs), CellImagePin::Extents(pin))
                    if store.owns_image(pin) =>
                {
                    let reference = pin.reference();
                    if refs
                        .insert(reference.root().bytes(), reference)
                        .is_some_and(|old| old != reference)
                    {
                        return Err(CellGroupReclaimError::Invalid("conflicting extent root"));
                    }
                }
                _ => return Err(CellGroupReclaimError::Invalid("image inventory owner")),
            }
            Ok(())
        })
    }

    fn reconcile(&self) -> Result<()> {
        match self {
            Self::Materialized(store, refs) => store
                .retain_only(&refs.iter().copied().collect::<Vec<_>>())
                .map_err(CellGroupReclaimError::Image),
            Self::Extents(store, refs) => store
                .retain_only(&refs.values().copied().collect::<Vec<_>>())
                .map_err(CellGroupReclaimError::Extent),
        }
    }

    fn reclaim(&self, files: usize) -> Result<usize> {
        match self {
            Self::Materialized(store, _) => store
                .reclaim(files)
                .map(|r| r.files)
                .map_err(CellGroupReclaimError::Image),
            Self::Extents(store, _) => store.reclaim(files).map_err(CellGroupReclaimError::Extent),
        }
    }
}
