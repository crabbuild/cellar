//! Bounded node-owned archive producers and one exclusive replay workspace.
//! SQL workers capture immutable cuts; network/archive replay never occupies a
//! cell's private execution connection or creates a task for each resident cell.

use super::archive::{
    apply_limits, ArchiveError, ArchiveLimits, ArchiveVerifier, CellArchive, CommittedArchiveCut,
};
use cellar_logic::consensus::{GroupScope, LogPosition};
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// Node-owned archive I/O and new-command retention admission. S3 success still
/// requires its exact verified recovery proof.
#[derive(Clone, Copy, Debug)]
pub struct RaftArchiveOptions {
    pub limits: ArchiveLimits,
    /// Maximum cells per immutable-object publication batch (1..=2). The
    /// total retained cuts across all batches remain bounded by `max_producers`.
    pub max_batch_groups: usize,
    pub retry_ms: u64,
    /// Maximum distinct cells retained across capture, pending batches and
    /// publication (1..=4). Each slot retains its complete capture reservation
    /// and bounded recovery sets until publication completes.
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
            max_batch_groups: 2,
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
            inflight: BTreeSet::new(),
            cursor: None,
            scan_at: 0,
            publish_at: 0,
            pending: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{next_scope, RaftArchiveOptions};
    use cellar_logic::consensus::{ClusterId, GroupId, GroupScope, IncarnationId, NamespaceId};
    use std::collections::BTreeMap;

    fn scope(index: u16) -> GroupScope {
        GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: format!("Counter:{index:04}"),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        }
    }

    #[test]
    fn archive_selection_does_not_inspect_cells_before_the_cursor() {
        let cells = (0..1500)
            .map(|i| (scope(i), ()))
            .collect::<BTreeMap<_, _>>();
        let cursor = scope(1200);
        let mut inspected = Vec::new();
        let selected = next_scope(&cells, Some(&cursor), |key, _| {
            inspected.push(key.clone());
            true
        });
        assert_eq!(selected, Some(&scope(1201)));
        assert_eq!(
            inspected.len(),
            1,
            "eligibility scanned the preceding cells"
        );
        assert_eq!(inspected, vec![scope(1201)]);
    }

    #[test]
    fn archive_selection_wraps_once_and_preserves_cyclic_eligibility() {
        let cells = (0..6).map(|i| (scope(i), ())).collect::<BTreeMap<_, _>>();
        for cursor in [
            None,
            Some(scope(0)),
            Some(scope(3)),
            Some(scope(5)),
            Some(scope(9)),
        ] {
            for mask in 0..64 {
                let expected = cells
                    .keys()
                    .filter(|key| cursor.as_ref().is_none_or(|cursor| *key > cursor))
                    .chain(
                        cells
                            .keys()
                            .filter(|key| cursor.as_ref().is_some_and(|cursor| *key <= cursor)),
                    )
                    .find(|key| {
                        let index: u16 = key.cell[8..].parse().unwrap();
                        mask & (1 << index) != 0
                    });
                let mut inspected = Vec::new();
                let selected = next_scope(&cells, cursor.as_ref(), |key, _| {
                    inspected.push(key.clone());
                    let index: u16 = key.cell[8..].parse().unwrap();
                    mask & (1 << index) != 0
                });
                assert_eq!(selected, expected);
                let mut unique = inspected.clone();
                unique.sort();
                unique.dedup();
                assert_eq!(inspected.len(), unique.len(), "a cell was inspected twice");
            }
        }
        assert!(next_scope::<()>(&BTreeMap::new(), None, |_, _| panic!("empty map")).is_none());
    }

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
