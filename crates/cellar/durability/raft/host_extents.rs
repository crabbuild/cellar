// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node inventory, complete native retention recovery and joined GC work.
//! The node supplies physical policy; immutable namespaces select commitments.

use super::*;
use cellar_ltx::{internal::EXTENT_BYTES, stream_snapshot::ScratchLimits};

pub use cellar_ltx::internal::{
    CacheLimits as RaftExtentCacheLimits, NodeExtentLimits as RaftExtentLimits,
};

/// Shared physical admission for opt-in extent namespaces. Limits cover the
/// entire node, including closed roots, incoming data and private workspaces.
#[derive(Clone, Copy, Debug)]
pub struct RaftExtentOptions {
    pub inventory: RaftExtentLimits,
    pub cache: RaftExtentCacheLimits,
    pub cache_scratch: ScratchLimits,
    pub gc_interval_ms: u64,
    pub gc_batch_files: usize,
}

impl Default for RaftExtentOptions {
    fn default() -> Self {
        Self {
            inventory: RaftExtentLimits::default(),
            cache: RaftExtentCacheLimits::default(),
            cache_scratch: ScratchLimits {
                max_bytes: 256 * 1024 * 1024,
                max_files: 64,
            },
            gc_interval_ms: 1000,
            gc_batch_files: 64,
        }
    }
}

impl RaftExtentOptions {
    pub(super) fn validate(&self, execution: ExecutionLimits) -> anyhow::Result<()> {
        self.inventory.validate()?;
        self.cache_scratch.validate()?;
        let workspace = RootPrivateOptions::workspace_bounds(execution)?.reservation()?;
        let extents = execution.max_image_bytes.div_ceil(EXTENT_BYTES) as usize;
        anyhow::ensure!(
            self.inventory.max_bytes >= workspace.saturating_add(4 * execution.max_image_bytes)
                && self.inventory.max_roots >= 4
                && self.inventory.max_objects >= 4 * extents
                && self.inventory.max_root_extents >= 4 * extents
                && self.inventory.max_references >= 4,
            "extent inventory cannot admit current, snapshot, prepare and receive ownership"
        );
        anyhow::ensure!(
            self.cache.max_cached_bytes >= EXTENT_BYTES
                && self.cache.max_cached_extents > 0
                && self.cache_scratch.max_bytes >= 2 * EXTENT_BYTES
                && self.cache_scratch.max_files >= 2
                && (10..=60_000).contains(&self.gc_interval_ms)
                && (1..=4096).contains(&self.gc_batch_files),
            "invalid extent cache or reclamation bounds"
        );
        Ok(())
    }
}
