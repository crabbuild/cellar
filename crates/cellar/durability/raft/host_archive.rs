// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded node-owned archive producers and one exclusive replay workspace.
//! SQL workers capture immutable cuts; network/archive replay never occupies a
//! cell's private execution connection or creates a task for each resident cell.

use super::archive::{
    apply_limits, ArchiveError, ArchiveLimits, ArchiveVerifier, CellArchive, CommittedArchiveCut,
};
use cellar_logic::consensus::{GroupScope, LogPosition, NodeIdentity};
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};

#[derive(Clone)]
pub(crate) struct PhysicalBudget {
    slots: Arc<Semaphore>,
    stop: watch::Receiver<bool>,
    performance: Arc<super::performance::Registry>,
}

impl PhysicalBudget {
    fn new(
        max_jobs: usize,
        stop: watch::Receiver<bool>,
        performance: Arc<super::performance::Registry>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (1..=4).contains(&max_jobs),
            "invalid archive physical job limit"
        );
        Ok(Self {
            slots: Arc::new(Semaphore::new(max_jobs)),
            stop,
            performance,
        })
    }

    pub(crate) fn host(&self, host: cellar_ltx::LtxHost) -> cellar_ltx::LtxHost {
        use super::performance::Operation;
        let now = host.clone();
        let age = host.clone();
        let read = host.clone();
        let work = host.clone();
        let budget = self.clone();
        cellar_ltx::LtxHost::new(
            move || now.now_unix_millis(),
            move |path| age.file_age(path),
            move |path| {
                let read = read.clone();
                async move { read.read_file(path).await }
            },
            move |job| {
                let work = work.clone();
                let budget = budget.clone();
                async move {
                    let admission = budget.performance.timer(Operation::ArchivePhysicalAdmission);
                    let stopped = async {
                        let mut stop = budget.stop.clone();
                        loop {
                            if *stop.borrow_and_update() {
                                return;
                            }
                            if stop.changed().await.is_err() {
                                return;
                            }
                        }
                    };
                    let permit = crate::asyncrt::select_biased! { "archive shutdown cancels queued physical admission before dispatch; entered owners retain their slots until completion";
                        _ = stopped => Err(cellar_ltx::HostTaskError::new("archive physical work stopped")),
                        permit = budget.slots.clone().acquire_owned() => {
                            permit
                                .map_err(|_| cellar_ltx::HostTaskError::new("archive physical admission closed"))
                                .and_then(|permit| {
                                    if *budget.stop.borrow() || budget.stop.has_changed().is_err() {
                                        Err(cellar_ltx::HostTaskError::new("archive physical work stopped"))
                                    } else {
                                        Ok(permit)
                                    }
                                })
                        },
                    };
                    admission.finish(permit.is_ok());
                    let permit = permit?;
                    let queued = budget.performance.timer(Operation::ArchivePhysicalQueue);
                    work.run_blocking(move || {
                        // A cancelled waiter cannot refund a job that is still
                        // queued on or executing in the owning domain.
                        let _permit = permit;
                        queued.finish(true);
                        job();
                    })
                    .await
                }
            },
        )
        .with_filesystem(host.filesystem())
    }
}

pub(crate) struct PhysicalOptions {
    pub max_jobs: usize,
    pub performance: Arc<super::performance::Registry>,
}

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

pub(super) fn publication_owner(identity: NodeIdentity) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-archive-publication-owner-1");
    hash.update(identity.node.bytes());
    hash.update(identity.disk.bytes());
    hash.update(identity.process.bytes());
    hash.finalize().into()
}

pub(crate) struct Owner {
    pub verifier: ArchiveVerifier,
    pub physical: PhysicalBudget,
    pub cancellation: tokio::sync::watch::Sender<bool>,
    /// A slot covers capture, object I/O, physical verification and completion.
    /// The same cell cannot publish twice concurrently, even after SQL is idle.
    pub inflight: BTreeSet<GroupScope>,
    pub cursor: Option<GroupScope>,
    /// Empty inventory is retried by maintenance, not by every peer frame.
    pub scan_at: u64,
    /// First pending capture's deadline; slow unrelated SQL cannot extend it.
    pub publish_at: u64,
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

/// Select the next eligible cell in cyclic key order, starting after the cursor.
pub(super) fn next_scope<'a, T>(
    cells: &'a BTreeMap<GroupScope, T>,
    cursor: Option<&GroupScope>,
    mut eligible: impl FnMut(&GroupScope, &T) -> bool,
) -> Option<&'a GroupScope> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    // Eligibility reads live group state. Seek before evaluating it, and use
    // disjoint ranges so an unsuccessful scan inspects each cell only once.
    let next = if let Some(cursor) = cursor {
        cells
            .range((Excluded(cursor), Unbounded))
            .chain(cells.range((Unbounded, Included(cursor))))
            .find(|(scope, cell)| eligible(scope, cell))
    } else {
        cells.iter().find(|(scope, cell)| eligible(scope, cell))
    };
    next.map(|(scope, _)| scope)
}

pub(crate) async fn publish(
    captured: Vec<Captured>,
    verifier: ArchiveVerifier,
    prepared: super::snapshot::prepared::Cache,
    physical: PhysicalBudget,
) -> Vec<Completed> {
    let expected: Vec<_> = captured
        .iter()
        .map(|capture| (capture.scope.clone(), capture.cut.position()))
        .collect();
    // One execution-domain worker drives each admitted batch, bounded by the
    // existing producer limit. Attempt-local snapshot files may close during a
    // retry, an error or final completion. Drive object I/O on this owner too,
    // so none of those synchronous closes can occupy the driver's poll stack.
    // The driver joins the worker before releasing its producer slot.
    let producer = crate::asyncrt::try_blocking(move || {
        crate::asyncrt::block_on(publish_owned(captured, verifier, prepared, physical))
    });
    let result = match producer {
        Ok(producer) => producer.await.map_err(|error| {
            ArchiveError::Worker(cellar_ltx::host::HostTaskError::new(error.to_string()))
        }),
        Err(_) => Err(ArchiveError::Stopped),
    };
    match result {
        Ok(completed) => completed,
        Err(error) => {
            let mut error = Some(error);
            expected
                .into_iter()
                .map(|(scope, position)| Completed {
                    scope,
                    position,
                    result: Err(error.take().unwrap_or(ArchiveError::Unconfirmed)),
                })
                .collect()
        }
    }
}

async fn publish_owned(
    captured: Vec<Captured>,
    verifier: ArchiveVerifier,
    prepared: super::snapshot::prepared::Cache,
    physical: PhysicalBudget,
) -> Vec<Completed> {
    let mut identities = Vec::with_capacity(captured.len());
    let mut reservations = Vec::with_capacity(captured.len());
    let inputs = captured
        .into_iter()
        .map(|capture| {
            identities.push((capture.scope, capture.cut.position()));
            reservations.push(capture.reservation);
            (
                capture.archive.with_publication_budget(&physical),
                capture.cut,
            )
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
    let completed = identities
        .into_iter()
        .zip(results)
        .map(|((scope, position), result)| {
            if let Err(error) = &result {
                release_idle_source_on_scratch_pressure(&prepared, error);
            }
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
    // Physical cleanup precedes capture refund, on this same blocking owner.
    drop(reservations);
    completed
}

/// Called by the admitted physical publisher after failed payload cleanup.
/// A reusable idle source must not permanently occupy a small scratch bank.
/// Live consumers keep their files; the original typed failure/retry remains.
pub(super) fn release_idle_source_on_scratch_pressure(
    prepared: &super::snapshot::prepared::Cache,
    error: &ArchiveError,
) {
    if error.scratch_full() {
        let _ = prepared.trim_idle();
    }
}

impl Owner {
    pub async fn open(
        directory: PathBuf,
        host: cellar_ltx::LtxHost,
        nonce: ImageId,
        mut apply: cellar_ltx::apply::ApplyLimits,
        accounting: cellar_ltx::generation::GenerationAccounting,
        snapshot_scratch: cellar_ltx::stream_snapshot::ScratchPool,
        scheduling: PhysicalOptions,
    ) -> anyhow::Result<Self> {
        let (cancellation, _) = watch::channel(false);
        let physical = PhysicalBudget::new(
            scheduling.max_jobs,
            cancellation.subscribe(),
            scheduling.performance,
        )?;
        let host = physical.host(host);
        let filesystem = host.filesystem();
        let verifier = host
            .run_blocking(move || {
                apply.decode.max_encoded_bytes = apply_limits().decode.max_encoded_bytes;
                apply.max_total_encoded_bytes = apply_limits().max_total_encoded_bytes;
                let limits = GenerationLimits {
                    apply,
                    max_generations: 4,
                    max_bytes: (512 * 1024 * 1024).max(4 * apply.decode.max_image_bytes),
                };
                let images = GenerationStore::open_with_scratch(
                    directory,
                    filesystem,
                    limits,
                    Some(&accounting),
                    snapshot_scratch,
                )
                .map_err(ArchiveError::Images)?;
                // This directory contains verification copies only, never an
                // engine marker or the only acknowledged application image.
                images.retain_only(&[]).map_err(ArchiveError::Images)?;
                images.reclaim(4096).map_err(ArchiveError::Images)?;
                ArchiveVerifier::new(images, nonce)
            })
            .await??;
        Ok(Self {
            verifier,
            physical,
            cancellation,
            inflight: BTreeSet::new(),
            cursor: None,
            scan_at: 0,
            publish_at: 0,
            pending: Vec::new(),
        })
    }
}

#[cfg(test)]
#[path = "host_archive_tests.rs"]
mod tests;
