//! One node-owned archive producer and an exclusively owned replay workspace.
//! SQL workers capture immutable cuts; network/archive replay never occupies a
//! cell's private execution connection or creates a task for each resident cell.

use super::archive::{
    apply_limits, ArchiveError, ArchiveLimits, ArchiveVerifier, CellArchive, CommittedArchiveCut,
};
use cellar_logic::consensus::{GroupScope, LogPosition};
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use std::path::PathBuf;

/// Node-owned archive I/O and new-command retention admission. One producer is
/// active per node; S3 success still requires its exact verified recovery proof.
#[derive(Clone, Copy, Debug)]
pub struct RaftArchiveOptions {
    pub limits: ArchiveLimits,
    /// Maximum simultaneously retained cuts/recovery sets (1..=2). Capture
    /// reservation capacity is this count times `limits.max_capture_bytes`.
    pub max_batch_groups: usize,
    pub retry_ms: u64,
    /// New business callbacks are refused before the retained log/image backlog
    /// plus conservative in-flight reservations can exceed this admission cap.
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
            max_unarchived_bytes: 256 * 1024 * 1024,
            max_unarchived_entries: 4096,
        }
    }
}

impl RaftArchiveOptions {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.limits.validate()?;
        anyhow::ensure!(
            (1..=super::archive::bundle::MAX_GROUPS).contains(&self.max_batch_groups)
                && (1..=60_000).contains(&self.retry_ms)
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
    pub pending: Vec<Captured>,
}

pub(crate) struct Captured {
    pub scope: GroupScope,
    pub archive: CellArchive,
    pub cut: CommittedArchiveCut,
    pub reservation: Option<super::budget::Permit>,
}

pub(crate) struct Completed {
    pub scope: GroupScope,
    pub position: LogPosition,
    pub result: Result<(), ArchiveError>,
}

pub(crate) async fn publish(captured: Vec<Captured>, verifier: ArchiveVerifier) -> Vec<Completed> {
    let mut identities = Vec::with_capacity(captured.len());
    let mut reservations = Vec::with_capacity(captured.len());
    let inputs = captured
        .into_iter()
        .map(|capture| {
            identities.push((capture.scope, capture.cut.position()));
            reservations.push(capture.reservation);
            (capture.archive, capture.cut)
        })
        .collect();
    let result = CellArchive::publish_batch(inputs, &verifier).await;
    let results = match result {
        Ok(results) if results.len() == identities.len() => results,
        Ok(_) => (0..identities.len())
            .map(|_| Err(ArchiveError::Unconfirmed))
            .collect(),
        Err(error) => {
            let mut error = Some(error);
            (0..identities.len())
                .map(|_| Err(error.take().unwrap_or(ArchiveError::Unconfirmed)))
                .collect()
        }
    };
    // The reservations remain owned through physical replay and every head CAS.
    let completed = identities
        .into_iter()
        .zip(results)
        .map(|((scope, position), result)| {
            let result = result.and_then(|set| {
                if set.scope() != &scope
                    || set.position().index < position.index
                    || set.position().index == position.index
                        && set.position().term != position.term
                {
                    return Err(ArchiveError::Corrupt("archive output coverage"));
                }
                Ok(())
            });
            Completed {
                scope,
                position,
                result,
            }
        })
        .collect();
    drop(reservations);
    completed
}

impl Owner {
    pub async fn open(
        directory: PathBuf,
        host: cellar_ltx::LtxHost,
        nonce: ImageId,
        mut apply: cellar_ltx::apply::ApplyLimits,
        accounting: cellar_ltx::generation::GenerationAccounting,
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
                let images =
                    GenerationStore::open_observed(directory, filesystem, limits, &accounting)
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
            pending: Vec::new(),
        })
    }
}
