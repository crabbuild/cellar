//! Bounded node-owned archive producers and one exclusive replay workspace.
//! SQL workers capture immutable cuts; network/archive replay never occupies a
//! cell's private execution connection or creates a task for each resident cell.

use super::archive::{apply_limits, ArchiveError, ArchiveLimits, ArchiveVerifier};
use cellar_logic::consensus::GroupScope;
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use std::collections::BTreeSet;
use std::path::PathBuf;

/// Node-owned archive I/O and new-command retention admission. S3 success still
/// requires its exact verified recovery proof.
#[derive(Clone, Copy, Debug)]
pub struct RaftArchiveOptions {
    pub limits: ArchiveLimits,
    pub retry_ms: u64,
    /// Concurrent publications across distinct cells (1..=4). Each reserves a
    /// complete capture and retains its bounded recovery sets until completion.
    /// Object I/O overlaps; physical replay shares one exclusive workspace.
    pub max_producers: usize,
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
            retry_ms: 100,
            max_producers: 2,
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
            (1..=60_000).contains(&self.retry_ms)
                && (4096..=16 * 1024 * 1024 * 1024).contains(&self.max_unarchived_bytes)
                && (1..=65_536).contains(&self.max_unarchived_entries),
            "invalid archive admission limits"
        );
        Ok(())
    }
}

pub(crate) struct Owner {
    pub verifier: ArchiveVerifier,
    pub cancellation: tokio::sync::watch::Sender<bool>,
    /// A slot covers capture, object I/O, physical verification and completion.
    /// The same cell cannot publish twice concurrently, even after SQL is idle.
    pub inflight: BTreeSet<GroupScope>,
    pub cursor: Option<GroupScope>,
    /// Empty inventory is retried by maintenance, not by every peer frame.
    pub scan_at: u64,
}

impl Owner {
    pub async fn open(
        directory: PathBuf,
        host: cellar_ltx::LtxHost,
        nonce: ImageId,
        mut apply: cellar_ltx::apply::ApplyLimits,
    ) -> anyhow::Result<Self> {
        let filesystem = host.filesystem();
        let verifier = host
            .run_blocking(move || {
                apply.decode.max_encoded_bytes = apply_limits().decode.max_encoded_bytes;
                apply.max_total_encoded_bytes = apply_limits().max_total_encoded_bytes;
                let limits = GenerationLimits {
                    apply,
                    max_generations: 4,
                    max_bytes: 512 * 1024 * 1024,
                };
                let images = GenerationStore::open(directory, filesystem, limits)
                    .map_err(ArchiveError::Images)?;
                // This directory contains verification copies only, never an
                // engine marker or the only acknowledged application image.
                images.retain_only(&[]).map_err(ArchiveError::Images)?;
                images.reclaim(4096).map_err(ArchiveError::Images)?;
                ArchiveVerifier::new(images, nonce)
            })
            .await??;
        let (cancellation, _) = tokio::sync::watch::channel(false);
        Ok(Self {
            verifier,
            cancellation,
            inflight: BTreeSet::new(),
            cursor: None,
            scan_at: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RaftArchiveOptions;

    #[test]
    fn archive_producers_require_a_bounded_nonempty_window() {
        for max_producers in [0, 5, usize::MAX] {
            assert!(RaftArchiveOptions {
                max_producers,
                ..Default::default()
            }
            .validate()
            .is_err());
        }
        for max_producers in 1..=4 {
            assert!(RaftArchiveOptions {
                max_producers,
                ..Default::default()
            }
            .validate()
            .is_ok());
        }
    }
}
