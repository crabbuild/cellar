// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::archive::ArchiveLimits;

/// Node-owned archive I/O and new-command retention admission. S3 success still
/// requires its exact verified recovery proof.
#[derive(Clone, Copy, Debug)]
pub struct RaftArchiveOptions {
    pub limits: ArchiveLimits,
    /// Maximum cells per immutable-object publication batch (1..=2). The
    /// total retained cuts across all batches remain bounded by `max_producers`.
    pub max_batch_groups: usize,
    pub retry_ms: u64,
    /// Large-image peers wait while a competing publisher's conditional hint
    /// keeps changing. This is a scheduling grace period, never a lease of
    /// cell ownership or permission to acknowledge unverified archive data.
    pub publication_stale_ms: u64,
    /// Maximum distinct cells retained across capture, pending batches and
    /// publication (1..=4). Each slot retains its complete capture reservation
    /// and bounded recovery sets until publication completes.
    /// Object I/O overlaps; physical replay shares one exclusive workspace.
    pub max_producers: usize,
    /// Maximum local archive encoding/verification jobs across all producers
    /// on this node (1..=4). Foreground recovery and command executors retain
    /// their separate admission. Object-store network windows are unchanged.
    pub max_blocking_jobs: usize,
    /// New callbacks reserve their maximum image/envelope size, reduced to the
    /// verified physical size after preparation. Temporary reservation pressure
    /// waits within request admission bounds; retained log/image backlog that
    /// cannot fit a new reservation refuses fresh callbacks (not receipt lookup).
    pub max_unarchived_bytes: u64,
    /// Data-bearing entries; protocol-only entries also consume the byte cap.
    pub max_unarchived_entries: usize,
}

impl Default for RaftArchiveOptions {
    fn default() -> Self {
        Self {
            limits: ArchiveLimits::default(),
            max_batch_groups: 2,
            retry_ms: 100,
            publication_stale_ms: 5000,
            max_producers: 2,
            max_blocking_jobs: 1,
            max_unarchived_bytes: 256 * 1024 * 1024,
            max_unarchived_entries: 4096,
        }
    }
}

impl RaftArchiveOptions {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.limits.validate()?;
        anyhow::ensure!(
            (1..=4).contains(&self.max_producers),
            "invalid archive producer limit"
        );
        anyhow::ensure!(
            (1..=4).contains(&self.max_blocking_jobs),
            "invalid archive physical job limit"
        );
        anyhow::ensure!(
            (1..=super::archive::bundle::MAX_GROUPS).contains(&self.max_batch_groups)
                && (1..=60_000).contains(&self.retry_ms)
                && (100..=60_000).contains(&self.publication_stale_ms)
                && (4096..=256 * 1024 * 1024 * 1024).contains(&self.max_unarchived_bytes)
                && (1..=65_536).contains(&self.max_unarchived_entries),
            "invalid archive admission limits"
        );
        Ok(())
    }
}
