// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Separate import admission: the held source already owns archive replay and
//! checkpoint permits. Reacquiring either single-slot pool would deadlock.
use super::super::{CellGroupArchiveError, CellGroupArchiveUsage, ManifestPins};
use crate::raft::budget::{Budget, Permit};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub struct CellGroupRecoveryLimits {
    pub max_operations: usize,
    pub max_retained_bytes: usize,
    pub max_cells: u32,
    pub max_image_bytes: u64,
    pub max_restore_bytes: u64,
}

impl Default for CellGroupRecoveryLimits {
    fn default() -> Self {
        Self {
            max_operations: 1,
            max_retained_bytes: 256 * 1024 * 1024,
            max_cells: 64,
            max_image_bytes: 256 * 1024 * 1024,
            max_restore_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Share across the node. The returned preparation retains admission until its
/// physical pins are dropped; cancellation does not release entered worker debt.
#[derive(Clone)]
pub struct CellGroupRecoveryPool {
    pub(super) limits: CellGroupRecoveryLimits,
    pub(super) budget: Arc<Budget>,
    workspace: usize,
}

impl CellGroupRecoveryPool {
    pub fn new(limits: CellGroupRecoveryLimits) -> Result<Self, CellGroupArchiveError> {
        if !(1..=4096).contains(&limits.max_operations)
            || !(1..=1_000_000).contains(&limits.max_cells)
            || !(512..=cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES)
                .contains(&limits.max_image_bytes)
            || limits.max_restore_bytes < limits.max_image_bytes
        {
            return Err(CellGroupArchiveError::Limit);
        }
        // Source metadata has its own retained archive admission. Account the
        // target tree, temporary AVL paths, both verification walks, all extent
        // references and two full extent buffers without duplicating an inventory.
        let extents_per_image = limits
            .max_image_bytes
            .div_ceil(cellar_ltx::internal::EXTENT_BYTES);
        let workspace = usize::try_from(extents_per_image)
            .ok()
            .and_then(|count| count.checked_mul(1024))
            .and_then(|bytes| bytes.checked_add(1024 * 1024))
            .and_then(|bytes| bytes.checked_mul(limits.max_cells as usize))
            .and_then(|bytes| {
                bytes.checked_add(ManifestPins::maximum_bytes(limits.max_cells).checked_mul(4)?)
            })
            .and_then(|bytes| bytes.checked_add(3 * cellar_ltx::internal::EXTENT_BYTES as usize))
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

    pub(super) fn admit(&self) -> Result<Arc<Permit>, CellGroupArchiveError> {
        self.budget
            .acquire(self.workspace)
            .map(Arc::new)
            .ok_or(CellGroupArchiveError::Busy)
    }
}
