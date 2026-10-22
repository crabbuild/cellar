//! One node-owned archive producer and an exclusively owned replay workspace.
//! SQL workers capture immutable cuts; network/archive replay never occupies a
//! cell's private execution connection or creates a task for each resident cell.

use super::archive::{apply_limits, ArchiveError, ArchiveLimits, ArchiveVerifier};
use cellar_logic::consensus::GroupScope;
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use std::path::PathBuf;

/// Node-owned archive I/O and new-command retention admission. One producer is
/// active per node; S3 success still requires its exact verified recovery proof.
#[derive(Clone, Copy, Debug)]
pub struct RaftArchiveOptions {
    pub limits: ArchiveLimits,
    pub retry_ms: u64,
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
            max_unarchived_bytes: 256 * 1024 * 1024,
            max_unarchived_entries: 4096,
        }
    }
}

impl RaftArchiveOptions {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.limits.validate()?;
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
    pub busy: bool,
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
            busy: false,
            cursor: None,
            scan_at: 0,
        })
    }
}
