// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated next-due hints prune cold subtrees without hydrating SQLite.
//! Hints are scheduling data, never execution or delivery authority.
use super::page_wire::PageRef;
use super::{
    CellManifestLeaf, ManifestError, ManifestLimits, ManifestPageStore, ManifestRoot, ManifestTree,
};
use crate::application::consensus::cell_routing_key;
use cellar_logic::cell_group::{CellId, CellKey};

pub(super) fn next(
    tree: &ManifestTree,
    store: &ManifestPageStore,
    root: ManifestRoot,
    now: u64,
    after: Option<&CellId>,
    limits: ManifestLimits,
) -> Result<Option<CellManifestLeaf>, ManifestError> {
    let after = after.map(|cell| cell_routing_key(tree.route().history.namespace, cell));
    // One cursor path can precede a path to the successor; wrapping adds at
    // most one more root-to-leaf path. Exhaustion is an error, never "no work".
    let mut remaining = usize::from(limits.max_depth) * 3;
    let selected = find(tree, store, root.0, now, after.as_ref(), &mut remaining)?;
    if selected.is_some() || after.is_none() {
        return Ok(selected);
    }
    find(tree, store, root.0, now, None, &mut remaining)
}

fn find(
    tree: &ManifestTree,
    store: &ManifestPageStore,
    reference: Option<PageRef>,
    now: u64,
    after: Option<&CellKey>,
    remaining: &mut usize,
) -> Result<Option<CellManifestLeaf>, ManifestError> {
    let Some(reference) =
        reference.filter(|p| p.next_due_unix_millis.is_some_and(|due| due <= now))
    else {
        return Ok(None);
    };
    *remaining = remaining.checked_sub(1).ok_or(ManifestError::Limit)?;
    let page = tree.read(store, reference)?;
    let key = cell_routing_key(
        tree.route().history.namespace,
        &page.leaf.version.history.cell,
    );
    if after.is_none_or(|after| &key > after) {
        if let Some(selected) = find(tree, store, page.left, now, after, remaining)? {
            return Ok(Some(selected));
        }
        if page
            .leaf
            .effects
            .next_due_unix_millis()
            .is_some_and(|due| due <= now)
        {
            return Ok(Some(page.leaf));
        }
    }
    find(tree, store, page.right, now, after, remaining)
}

#[cfg(test)]
#[path = "effect_index_tests.rs"]
mod tests;
