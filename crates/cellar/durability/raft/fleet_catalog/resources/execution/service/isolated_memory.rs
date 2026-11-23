// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep live repair data charged without overlapping non-live model buffers
//! with quorum metadata. Every phase keeps the same operation/item owner.
use super::*;
use crate::raft::budget::{Budget, Permit};

pub(super) struct Memory {
    retained: Permit,
    planning_bytes: usize,
}

impl Memory {
    pub(super) fn new(budget: &Arc<Budget>, nodes: usize) -> Result<Self> {
        let planning_bytes = ROOT_LIMIT
            .checked_mul(2)
            .and_then(|n| n.checked_add(nodes.checked_mul(512 * 1024)?))
            .ok_or(FleetCatalogError::Limit)?;
        // Preserve the full model's intrinsic-fit check and initial admission.
        let mut retained =
            super::super::topology::reserve_collection_memory(budget, planning_bytes, 0)?;
        if !retained.shrink_to(crate::raft::physical_inventory::RESPONSE_BYTES) {
            return Err(FleetCatalogError::Corrupt("isolated source reservation"));
        }
        Ok(Self {
            retained,
            planning_bytes,
        })
    }

    pub(super) fn planning(&mut self) -> Result<()> {
        // A refusal keeps the source proof charged until its caller drops it.
        if !self.retained.grow_to(self.planning_bytes) {
            return Err(FleetCatalogError::Overloaded);
        }
        Ok(())
    }

    pub(super) fn admission(&mut self) -> Result<()> {
        // No per-node physical inventories/model survive Planning::plan. The
        // validated inputs (<=64 small attestations/namespace handles) and the
        // selected scalar request remain within the two root-buffer envelope.
        if !self.retained.shrink_to(2 * ROOT_LIMIT) {
            return Err(FleetCatalogError::Corrupt("isolated admission reservation"));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "isolated_memory_tests.rs"]
mod tests;
