// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#[derive(Clone, Debug)]
pub struct RaftInspectionScanOptions {
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub groups_per_batch: usize,
}

impl Default for RaftInspectionScanOptions {
    fn default() -> Self {
        Self {
            interval_ms: 250,
            timeout_ms: 5000,
            groups_per_batch: 4,
        }
    }
}

impl RaftInspectionScanOptions {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (10..=60_000).contains(&self.interval_ms)
                && (1..=60_000).contains(&self.timeout_ms)
                && (1..=16).contains(&self.groups_per_batch),
            "invalid inspection scan bounds"
        );
        Ok(())
    }
}
