// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared bounds for the owned alarm/outbox scheduler.
use anyhow::{ensure, Result};

#[derive(Clone, Copy, Debug)]
pub struct RaftEffectOptions {
    pub interval_ms: u64,
    pub operation_timeout_ms: u64,
    pub retry_window_ms: u64,
    pub metadata_bytes: usize,
}

impl Default for RaftEffectOptions {
    fn default() -> Self {
        Self {
            interval_ms: 100,
            operation_timeout_ms: 5000,
            retry_window_ms: 60_000,
            metadata_bytes: 512 * 1024,
        }
    }
}

impl RaftEffectOptions {
    pub(crate) fn validate(self, host: &crate::raft::host::RaftHostOptions) -> Result<()> {
        ensure!(
            (10..=60_000).contains(&self.interval_ms)
                && (1..=60_000).contains(&self.operation_timeout_ms)
                && self
                    .operation_timeout_ms
                    .checked_add(100)
                    .is_some_and(|n| n <= self.retry_window_ms)
                && self.retry_window_ms <= host.execution.max_retry_window_ms
                && (64 * 1024..=8 * 1024 * 1024).contains(&self.metadata_bytes),
            "invalid effect scheduler bounds"
        );
        Ok(())
    }
}
