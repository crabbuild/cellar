// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete group-state storage. A synced page is data, not Raft authority.

mod manifest;
mod page_store;
mod page_wire;

pub use manifest::{
    CellImageGeometry, CellManifestLeaf, EffectSummary, ManifestError, ManifestLimits,
    ManifestRoot, ManifestTree,
};
pub use page_store::{ManifestPageStore, ManifestStoreUsage};
