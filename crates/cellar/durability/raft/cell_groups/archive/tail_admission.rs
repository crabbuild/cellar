// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::ManifestPins;
use super::*;

/// Additional node-wide admission for committed-tail capture and independent
/// LTX replay. Share this pool across all group bindings. The ordinary archive
/// pool still charges I/O and indexes; checkpoint pools still charge their cuts.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupArchiveTailLimits {
    pub max_operations: usize,
    pub max_retained_bytes: usize,
    pub max_entries: usize,
    pub max_tail_bytes: usize,
    pub max_cells: u32,
    /// Must cover the destination store's captured image workspace bound.
    pub max_apply_bytes: usize,
}

impl Default for CellGroupArchiveTailLimits {
    fn default() -> Self {
        Self {
            max_operations: 1,
            max_retained_bytes: 512 * 1024 * 1024,
            max_entries: 1024,
            max_tail_bytes: 16 * 1024 * 1024,
            max_cells: 64,
            max_apply_bytes: 128 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct CellGroupArchiveTailPool {
    pub(super) limits: CellGroupArchiveTailLimits,
    budget: Arc<Budget>,
    workspace: usize,
}

impl CellGroupArchiveTailPool {
    pub fn new(limits: CellGroupArchiveTailLimits) -> Result<Self> {
        if !(1..=4096).contains(&limits.max_operations)
            || !(1..=65_536).contains(&limits.max_entries)
            || !(1024..=512 * 1024 * 1024).contains(&limits.max_tail_bytes)
            || !(1..=1_000_000).contains(&limits.max_cells)
            || !(4 * 1024 * 1024..=512 * 1024 * 1024).contains(&limits.max_apply_bytes)
        {
            return Err(CellGroupArchiveError::Limit);
        }
        // Base, predecessor and successor roots can coexist while one image
        // transition executes. Each cell may retain a <=1 MiB extent manifest.
        let metadata = ManifestPins::maximum_bytes(limits.max_cells)
            .checked_add(
                (limits.max_cells as usize)
                    .checked_mul(1024 * 1024)
                    .ok_or(CellGroupArchiveError::Limit)?,
            )
            .and_then(|n| n.checked_mul(3));
        let workspace = metadata
            .and_then(|n| n.checked_add(limits.max_tail_bytes.checked_mul(4)?))
            .and_then(|n| n.checked_add(limits.max_apply_bytes))
            .and_then(|n| n.checked_add(limits.max_entries.checked_mul(512)?))
            .and_then(|n| n.checked_add(1024 * 1024))
            .ok_or(CellGroupArchiveError::Limit)?;
        if workspace > limits.max_retained_bytes {
            return Err(CellGroupArchiveError::Limit);
        }
        Ok(Self {
            limits,
            budget: Budget::new(limits.max_retained_bytes, limits.max_operations),
            workspace,
        })
    }

    pub fn usage(&self) -> CellGroupArchiveUsage {
        let (retained_bytes, operations) = self.budget.usage();
        CellGroupArchiveUsage {
            retained_bytes,
            operations,
        }
    }

    pub(super) fn admit(&self) -> Result<Arc<Permit>> {
        self.budget
            .acquire(self.workspace)
            .map(Arc::new)
            .ok_or(CellGroupArchiveError::Busy)
    }
}
