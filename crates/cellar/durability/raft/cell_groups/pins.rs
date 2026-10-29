// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent physical ownership follows the authenticated AVL topology. Cloning
//! a root is constant-time; changing a cell copies only its path and rotations.

use super::{
    page_wire::PageRef, CellImagePin, CellManifestLeaf, ManifestError, ManifestPageStore,
    ManifestRoot, ManifestTree,
};
use crate::application::consensus::cell_routing_key;
use cellar_logic::cell_group::{CellKey, MAX_CELL_ID_BYTES};
use std::{cmp::Ordering, sync::Arc};

#[derive(Clone, Default)]
pub(crate) struct ManifestPins(Option<Arc<PinnedPage>>);

struct PinnedPage {
    _page: super::page_store::ManifestPagePin,
    reference: PageRef,
    key: CellKey,
    leaf: CellManifestLeaf,
    image: CellImagePin,
    left: ManifestPins,
    right: ManifestPins,
    bytes: usize,
}

impl ManifestPins {
    pub(crate) fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |p| p.reference.cells as usize)
    }

    pub(crate) fn root(&self) -> ManifestRoot {
        ManifestRoot(self.0.as_ref().map(|p| p.reference))
    }

    pub(crate) fn maximum_bytes(cells: u32) -> usize {
        cells as usize
            * (std::mem::size_of::<PinnedPage>()
                + 2 * std::mem::size_of::<usize>()
                + 3 * MAX_CELL_ID_BYTES)
    }

    /// Conservatively charge shared subtrees to every owner. Include all three cell
    /// ID allocations and Arc control blocks, not just the image handle size.
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.0.as_ref().map_or(0, |p| p.bytes)
    }

    pub(super) fn get(&self, key: &CellKey) -> Option<(&CellManifestLeaf, CellImagePin)> {
        self.find(key).map(|p| (&p.leaf, p.image.clone()))
    }

    fn find(&self, key: &CellKey) -> Option<&Arc<PinnedPage>> {
        let mut cursor = &self.0;
        while let Some(page) = cursor {
            match key.cmp(&page.key) {
                Ordering::Less => cursor = &page.left.0,
                Ordering::Greater => cursor = &page.right.0,
                Ordering::Equal => return Some(page),
            }
        }
        None
    }

    fn node(
        reference: PageRef,
        key: CellKey,
        leaf: CellManifestLeaf,
        image: CellImagePin,
        left: Self,
        right: Self,
        page: super::page_store::ManifestPagePin,
    ) -> Self {
        let bytes = std::mem::size_of::<PinnedPage>()
            + 2 * std::mem::size_of::<usize>()
            + key.cell().as_str().len()
            + leaf.version.history.cell.as_str().len()
            + leaf.enrollment.cell().cell.as_str().len()
            + left.reserved_bytes()
            + right.reserved_bytes();
        Self(Some(Arc::new(PinnedPage {
            _page: page,
            reference,
            key,
            leaf,
            image,
            left,
            right,
            bytes,
        })))
    }

    /// The caller first verifies the complete root's ordering and applied cut.
    /// Every read rechecks content addressing; the second walk cannot silently
    /// substitute different leaves after that verification.
    pub(super) fn complete(
        tree: &ManifestTree,
        store: &mut ManifestPageStore,
        root: ManifestRoot,
        image: &mut impl FnMut(&CellManifestLeaf) -> Result<CellImagePin, ManifestError>,
    ) -> Result<Self, ManifestError> {
        let Some(reference) = root.0 else {
            return Ok(Self::default());
        };
        let page = tree.read(store, reference)?;
        let left = Self::complete(tree, store, ManifestRoot(page.left), image)?;
        let pin = image(&page.leaf)?;
        pin.check(&page.leaf, tree.route.schema_digest)?;
        let right = Self::complete(tree, store, ManifestRoot(page.right), image)?;
        store.sync_page(reference.digest)?;
        let key = cell_routing_key(
            tree.route.history.namespace,
            &page.leaf.version.history.cell,
        );
        Ok(Self::node(
            reference,
            key,
            page.leaf,
            pin,
            left,
            right,
            store.pin(reference.digest)?,
        ))
    }

    /// Only accepts the root produced by the tree's compare-and-replace method.
    /// Search the old tree by the new page's key so AVL rotations also find and
    /// reuse unchanged subtrees. Disk reads/node allocations stay O(log cells);
    /// the bounded in-memory searches take O(log² cells) comparisons.
    pub(super) fn updated(
        &self,
        tree: &ManifestTree,
        store: &ManifestPageStore,
        root: ManifestRoot,
        changed: &CellManifestLeaf,
        image: &CellImagePin,
    ) -> Result<Self, ManifestError> {
        let Some(reference) = root.0 else {
            return Ok(Self::default());
        };
        let page = tree.read(store, reference)?;
        let key = cell_routing_key(
            tree.route.history.namespace,
            &page.leaf.version.history.cell,
        );
        let previous = self.find(&key);
        if let Some(old) = previous {
            if old.reference == reference {
                return Ok(Self(Some(old.clone())));
            }
        }
        let pin = if page.leaf == *changed {
            image.clone()
        } else {
            let old = previous
                .filter(|old| old.leaf == page.leaf)
                .ok_or(ManifestError::Invalid("unexpected changed leaf"))?;
            old.image.clone()
        };
        let left = self.updated(tree, store, ManifestRoot(page.left), changed, image)?;
        let right = self.updated(tree, store, ManifestRoot(page.right), changed, image)?;
        Ok(Self::node(
            reference,
            key,
            page.leaf,
            pin,
            left,
            right,
            store.pin(reference.digest)?,
        ))
    }
}
