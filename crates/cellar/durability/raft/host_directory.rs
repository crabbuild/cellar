// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit bounds for the physical-node directory owner.

#[derive(Clone, Debug)]
pub struct RaftNodeDirectoryOptions {
    /// Independent host timer interval; no application traffic is required.
    pub poll_ms: u64,
    /// Original deadline shared by every operation in one discovery batch.
    pub timeout_ms: u64,
    /// At most this many bounded node records are observed concurrently.
    pub batch_nodes: usize,
}

impl Default for RaftNodeDirectoryOptions {
    fn default() -> Self {
        Self {
            poll_ms: 250,
            timeout_ms: 5000,
            batch_nodes: 4,
        }
    }
}

impl RaftNodeDirectoryOptions {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (10..=60_000).contains(&self.poll_ms)
                && (1..=60_000).contains(&self.timeout_ms)
                && (1..=8).contains(&self.batch_nodes),
            "invalid node directory bounds"
        );
        Ok(())
    }
}
