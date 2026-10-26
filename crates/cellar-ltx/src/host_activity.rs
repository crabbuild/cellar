// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Completed operation observations. A changing counter is scheduling input,
//! never an integrity, persistence, ownership or publication proof.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Clone, Default)]
pub struct IoActivity(Arc<AtomicU64>);

impl IoActivity {
    pub fn sequence(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Record only completed work. Saturation stops observations rather than
    /// allowing a wrap to reinterpret old activity as a fresh generation.
    pub fn record(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |before| {
                Some(before.saturating_add(1))
            });
    }
}
