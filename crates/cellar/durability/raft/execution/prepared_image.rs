// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact live producer data, independently replayed and durably sealed already.
//! Custody stays with the pending producer; submission clones carry only Weak.

use super::Preview;
use crate::raft::cell_groups::CellImagePin;
use cellar_logic::cell_group::GroupHistory;
use cellar_ltx::{apply::ApplyLimits, ltx::DecodeLimits};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(crate) struct PreparedCellImage {
    engine: Arc<()>,
    group: GroupHistory,
    payload: [u8; 32],
    limits: ApplyLimits,
    image: CellImagePin,
}

impl PreparedCellImage {
    /// Call only after the preview's full independent replay and persistence
    /// barriers. This capability grants no commit, serving or retirement proof.
    pub(super) fn new(
        engine: Arc<()>,
        group: GroupHistory,
        bytes: &[u8],
        limits: ApplyLimits,
        preview: &Preview,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            group,
            payload: Sha256::digest(bytes).into(),
            limits,
            image: preview.cell_pin(),
        })
    }

    pub(crate) fn matches(&self, engine: &Arc<()>, group: GroupHistory, bytes: &[u8]) -> bool {
        Arc::ptr_eq(&self.engine, engine)
            && self.group == group
            && self.payload == <[u8; 32]>::from(Sha256::digest(bytes))
    }

    pub(crate) fn image(&self, limits: ApplyLimits) -> Option<&CellImagePin> {
        // A different validation profile takes ordinary independent replay.
        // Exhaustive destructuring makes new apply-limit fields require review.
        (limit_key(self.limits) == limit_key(limits)).then_some(&self.image)
    }
}

fn limit_key(limits: ApplyLimits) -> (DecodeLimits, usize, usize, u64, usize) {
    let ApplyLimits {
        decode,
        max_files,
        max_total_encoded_bytes,
        max_total_decoded_bytes,
        max_total_pages,
    } = limits;
    (
        decode,
        max_files,
        max_total_encoded_bytes,
        max_total_decoded_bytes,
        max_total_pages,
    )
}

#[cfg(test)]
#[path = "prepared_image_tests.rs"]
mod tests;
