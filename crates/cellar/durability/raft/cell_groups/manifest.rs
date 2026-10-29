// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable authenticated AVL pages. Every cell occupies one bounded page;
//! changing a cell copies O(log cells) pages, preserving all other image roots.

use super::page_store::ManifestPageStore;
use super::page_wire::{decode, encode, Page, PageRef};
use crate::application::consensus::{cell_routing_key, LtxImage};
use cellar_logic::{cell_group::*, consensus::LogPosition};
use std::cmp::Ordering;

#[derive(Debug)]
pub enum ManifestError {
    Invalid(&'static str),
    Limit,
    Conflict,
    Quarantined,
    Io(std::io::Error),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group manifest: {self:?}")
    }
}

impl std::error::Error for ManifestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ManifestError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Geometry is logical data. Local generation names never enter a replicated
/// manifest commitment; each replica resolves and pins its own image storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellImageGeometry {
    pub length: u64,
    pub page_size: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct ManifestLimits {
    pub max_cells: u32,
    pub max_depth: u8,
}

impl Default for ManifestLimits {
    fn default() -> Self {
        Self {
            max_cells: 1_000_000,
            max_depth: 32,
        }
    }
}

impl ManifestLimits {
    pub(super) fn validate(self) -> Result<(), ManifestError> {
        if self.max_cells == 0
            || self.max_cells > 1_000_000
            || self.max_depth == 0
            || self.max_depth > 32
        {
            return Err(ManifestError::Limit);
        }
        Ok(())
    }
}

/// Authenticated scheduling hints. Callbacks and delivery identity still come
/// from the selected cell image; a summary grants no execution authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectSummary {
    pub next_alarm_unix_millis: Option<u64>,
    pub next_outbox_unix_millis: Option<u64>,
    pub pending_outbox: u64,
    pub pending_inbox: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellManifestLeaf {
    pub version: CellVersion,
    pub geometry: CellImageGeometry,
    pub last_mutation: LogPosition,
    pub effects: EffectSummary,
}

impl CellManifestLeaf {
    pub(super) fn validate(&self, route: &GroupRoute) -> Result<CellKey, ManifestError> {
        let key = cell_routing_key(route.history.namespace, &self.version.history.cell);
        route
            .check_cell(&self.version.history, &key)
            .map_err(|_| ManifestError::Invalid("cell route"))?;
        self.last_mutation
            .validate()
            .map_err(|_| ManifestError::Invalid("mutation position"))?;
        if self.last_mutation.index.get() == 0 {
            return Err(ManifestError::Invalid("zero mutation index"));
        }
        let image = LtxImage::try_from(self.version.image)
            .map_err(|_| ManifestError::Invalid("image commitment"))?;
        if !cellar_ltx::ltx::is_valid_page_size(self.geometry.page_size)
            || self.geometry.length > cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES
            || !self
                .geometry
                .length
                .is_multiple_of(u64::from(self.geometry.page_size))
            || (image.txid == 0) != (self.geometry.length == 0)
            || (image.txid == 0 && image.checksum != 0)
        {
            return Err(ManifestError::Invalid("image geometry"));
        }
        if let CellImageCommitment::Extents { manifest_bytes, .. } = self.version.image.commitment {
            let expected = 112
                + 32 * self
                    .geometry
                    .length
                    .div_ceil(cellar_ltx::internal::EXTENT_BYTES);
            if self.geometry.length == 0 || u64::from(manifest_bytes) != expected {
                return Err(ManifestError::Invalid("extent geometry"));
            }
        }
        Ok(key)
    }
}

/// An untrusted tree descriptor until checked against an authoritative group
/// root and traversed by `verify`. It is never an applied marker or image seal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ManifestRoot(pub(super) Option<PageRef>);

impl ManifestRoot {
    pub fn cells(self) -> u32 {
        self.0.map_or(0, |p| p.cells)
    }

    pub fn depth(self) -> u8 {
        self.0.map_or(0, |p| p.height)
    }

    pub fn digest(self) -> Option<[u8; 32]> {
        self.0.map(|p| p.digest)
    }
}

/// Synchronous operations belong on the owning blocking execution domain.
/// Retain every root used by apply/read/snapshot/transfer before reclamation.
/// This layer performs no page deletion and cannot publish serving authority.
pub struct ManifestTree {
    pub(super) route: GroupRoute,
    limits: ManifestLimits,
}

impl ManifestTree {
    pub fn new(route: GroupRoute, limits: ManifestLimits) -> Result<Self, ManifestError> {
        limits.validate()?;
        for bound in [route.range.lower(), route.range.upper()] {
            if let CellBound::Key(key) = bound {
                if &cell_routing_key(route.history.namespace, key.cell()) != key {
                    return Err(ManifestError::Invalid("range hash"));
                }
            }
        }
        Ok(Self { route, limits })
    }

    pub fn get(
        &self,
        store: &ManifestPageStore,
        root: ManifestRoot,
        cell: &CellId,
    ) -> Result<Option<CellManifestLeaf>, ManifestError> {
        let key = cell_routing_key(self.route.history.namespace, cell);
        let mut cursor = root.0;
        let mut remaining = self.limits.max_depth;
        while let Some(reference) = cursor {
            if remaining == 0 {
                return Err(ManifestError::Limit);
            }
            remaining -= 1;
            let page = self.read(store, reference)?;
            match key.cmp(&page.leaf.validate(&self.route)?) {
                Ordering::Less => cursor = page.left,
                Ordering::Greater => cursor = page.right,
                Ordering::Equal => return Ok(Some(page.leaf)),
            }
        }
        Ok(None)
    }

    /// Compare-and-replace one logical cell. `None` requires absence; transfer
    /// installation may insert a retained sequence. Enrollment authorization,
    /// image sealing and the group's contiguous apply proof belong to the caller.
    pub fn update(
        &self,
        store: &mut ManifestPageStore,
        root: ManifestRoot,
        leaf: CellManifestLeaf,
        expected: Option<&CellVersion>,
    ) -> Result<ManifestRoot, ManifestError> {
        let key = leaf.validate(&self.route)?;
        let current = self.get(store, root, &leaf.version.history.cell)?;
        match (&current, expected) {
            (None, None) => {
                if root.cells() >= self.limits.max_cells {
                    return Err(ManifestError::Limit);
                }
            }
            (Some(old), Some(expected)) if &old.version == expected => {
                if old.version.history != leaf.version.history
                    || old.version.sequence.checked_next().ok() != Some(leaf.version.sequence)
                    || leaf.version.image.epoch != old.version.image.epoch
                    || leaf.version.image.txid <= old.version.image.txid
                    || leaf.last_mutation.index <= old.last_mutation.index
                    || leaf.last_mutation.term < old.last_mutation.term
                {
                    return Err(ManifestError::Conflict);
                }
            }
            _ => return Err(ManifestError::Conflict),
        }
        self.insert(store, root.0, leaf, &key, 0)
            .map(|p| ManifestRoot(Some(p)))
    }

    /// Walk the entire authenticated closure before recovery admission. The
    /// callback must independently verify/pin each actual image and receipt state.
    /// No success or partial callback traversal is itself a quorum proof.
    pub fn verify(
        &self,
        store: &ManifestPageStore,
        root: ManifestRoot,
        applied: LogPosition,
        mut image: impl FnMut(&CellManifestLeaf) -> Result<(), ManifestError>,
    ) -> Result<(), ManifestError> {
        applied
            .validate()
            .map_err(|_| ManifestError::Invalid("applied position"))?;
        let mut visited = 0;
        self.walk(
            store,
            root.0,
            (None, None),
            applied,
            &mut visited,
            &mut image,
        )?;
        if visited != root.cells() {
            return Err(ManifestError::Invalid("cell count"));
        }
        Ok(())
    }

    fn walk(
        &self,
        store: &ManifestPageStore,
        reference: Option<PageRef>,
        bounds: (Option<&CellKey>, Option<&CellKey>),
        applied: LogPosition,
        visited: &mut u32,
        image: &mut impl FnMut(&CellManifestLeaf) -> Result<(), ManifestError>,
    ) -> Result<(), ManifestError> {
        let (lower, upper) = bounds;
        let Some(reference) = reference else {
            return Ok(());
        };
        *visited = visited.checked_add(1).ok_or(ManifestError::Limit)?;
        if *visited > self.limits.max_cells {
            return Err(ManifestError::Limit);
        }
        let page = self.read(store, reference)?;
        let key = page.leaf.validate(&self.route)?;
        if lower.is_some_and(|l| key <= *l)
            || upper.is_some_and(|u| key >= *u)
            || page.leaf.last_mutation.index > applied.index
            || page.leaf.last_mutation.term > applied.term
            || (page.leaf.last_mutation.index == applied.index
                && page.leaf.last_mutation.term != applied.term)
        {
            return Err(ManifestError::Invalid("tree order or apply cut"));
        }
        self.walk(
            store,
            page.left,
            (lower, Some(&key)),
            applied,
            visited,
            image,
        )?;
        image(&page.leaf)?;
        self.walk(
            store,
            page.right,
            (Some(&key), upper),
            applied,
            visited,
            image,
        )
    }

    pub(super) fn read(
        &self,
        store: &ManifestPageStore,
        reference: PageRef,
    ) -> Result<Page, ManifestError> {
        let bytes = store.read(reference.digest)?;
        let page = decode(&bytes, &self.route, self.limits)?;
        if page.reference(&bytes, self.limits)? != reference {
            return Err(ManifestError::Invalid("page reference"));
        }
        Ok(page)
    }

    fn save(&self, store: &mut ManifestPageStore, page: Page) -> Result<PageRef, ManifestError> {
        let bytes = encode(&page, &self.route, self.limits)?;
        let reference = page.reference(&bytes, self.limits)?;
        store.put(reference.digest, &bytes)?;
        Ok(reference)
    }

    fn insert(
        &self,
        store: &mut ManifestPageStore,
        root: Option<PageRef>,
        leaf: CellManifestLeaf,
        key: &CellKey,
        depth: u8,
    ) -> Result<PageRef, ManifestError> {
        if depth >= self.limits.max_depth {
            return Err(ManifestError::Limit);
        }
        let Some(reference) = root else {
            return self.save(
                store,
                Page {
                    leaf,
                    left: None,
                    right: None,
                },
            );
        };
        let mut page = self.read(store, reference)?;
        match key.cmp(&page.leaf.validate(&self.route)?) {
            Ordering::Less => {
                page.left = Some(self.insert(store, page.left, leaf, key, depth + 1)?)
            }
            Ordering::Greater => {
                page.right = Some(self.insert(store, page.right, leaf, key, depth + 1)?)
            }
            Ordering::Equal => page.leaf = leaf,
        }
        self.balance(store, page)
    }

    fn balance(
        &self,
        store: &mut ManifestPageStore,
        mut page: Page,
    ) -> Result<PageRef, ManifestError> {
        let height = |r: Option<PageRef>| r.map_or(0, |p| i16::from(p.height));
        let balance = height(page.left) - height(page.right);
        if balance > 1 {
            let mut left =
                self.read(store, page.left.ok_or(ManifestError::Invalid("rotation"))?)?;
            if height(left.left) < height(left.right) {
                let mut middle =
                    self.read(store, left.right.ok_or(ManifestError::Invalid("rotation"))?)?;
                left.right = middle.left;
                middle.left = Some(self.save(store, left)?);
                page.left = middle.right;
                middle.right = Some(self.save(store, page)?);
                return self.save(store, middle);
            }
            page.left = left.right;
            left.right = Some(self.save(store, page)?);
            return self.save(store, left);
        }
        if balance < -1 {
            let mut right =
                self.read(store, page.right.ok_or(ManifestError::Invalid("rotation"))?)?;
            if height(right.right) < height(right.left) {
                let mut middle =
                    self.read(store, right.left.ok_or(ManifestError::Invalid("rotation"))?)?;
                right.left = middle.right;
                middle.right = Some(self.save(store, right)?);
                page.right = middle.left;
                middle.left = Some(self.save(store, page)?);
                return self.save(store, middle);
            }
            page.right = right.left;
            right.left = Some(self.save(store, page)?);
            return self.save(store, right);
        }
        self.save(store, page)
    }
}
