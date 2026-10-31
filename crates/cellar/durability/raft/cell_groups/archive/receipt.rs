// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read the exact receipt from independently recovered, immutable archive bytes.
use super::*;
use crate::raft::cell_groups::{CellGroupApplyImages, CellImagePin};
use crate::raft::execution::{CellCommittedReceipt, CommandRequest, ExecutionLimits};
use std::sync::atomic::{AtomicBool, Ordering};

pub(in crate::raft::cell_groups) fn command(
    coverage: &CellGroupArchiveCoverage,
    store: &CellGroupArchiveStore,
    expected: &CellCommittedReceipt,
    mut limits: ExecutionLimits,
    deadline: u64,
    cancelled: Arc<AtomicBool>,
) -> Result<bool> {
    store.require_live()?;
    let root = coverage.recovery().manifest();
    if root.marker().route() != store.route() || !store.owner.owns(root) {
        return Err(CellGroupArchiveError::Invalid(
            "S3 receipt verifier owner/route",
        ));
    }
    if root.marker().position().index < expected.position.index {
        return Ok(false);
    }
    let (leaf, pin) = root
        .pin_cell(&expected.version.history.cell)
        .ok_or(CellGroupArchiveError::Invalid("S3 receipt cell missing"))?;
    if leaf.version.history != expected.version.history {
        return Err(CellGroupArchiveError::Invalid("S3 receipt history"));
    }
    if leaf.version.sequence < expected.version.sequence {
        return Ok(false);
    }
    if leaf.version == expected.version && leaf.witness != expected.witness {
        return Err(CellGroupArchiveError::Invalid("S3 receipt decision"));
    }
    let images = store
        .storage
        .try_lock()
        .map_err(|_| CellGroupArchiveError::Busy)?
        .images
        .clone();
    let database = match (images, pin) {
        (CellGroupApplyImages::Materialized { services, .. }, CellImagePin::Materialized(pin)) => {
            crate::storage::open_immutable_generation(
                pin,
                crate::storage::ImmutableQueryOptions {
                    services,
                    sqlite_vec: false,
                },
            )
        }
        (
            CellGroupApplyImages::Extents {
                services,
                cache_directory,
                cache_pool,
                cache,
                ..
            },
            CellImagePin::Extents(pin),
        ) => {
            let image = pin
                .view()
                .image(cache_directory, cache_pool, cache)
                .map_err(|e| CellGroupArchiveError::Replay(crate::raft::execution::storage(e)))?;
            crate::storage::open_immutable_extent_image(
                image,
                crate::storage::ImmutableQueryOptions {
                    services,
                    sqlite_vec: false,
                },
            )
        }
        _ => return Err(CellGroupArchiveError::Invalid("S3 receipt image")),
    }
    .map_err(|e| CellGroupArchiveError::Replay(crate::raft::execution::storage(e)))?;
    database
        .connection()
        .progress_handler(
            1000,
            Some(move || {
                cancelled.load(Ordering::Acquire) || crate::asyncrt::mono_ms() >= deadline
            }),
        )
        .map_err(|e| CellGroupArchiveError::Replay(crate::raft::execution::storage(e)))?;
    // The pending output already owns the original bytes. Bound the additional
    // row allocation before lookup; identity and the entire retained row match.
    limits.max_result_bytes = expected.receipt.result().len();
    let request = CommandRequest {
        id: expected.receipt.id,
        operation: expected.receipt.operation.clone(),
        contract_digest: expected.receipt.contract_digest,
        request_digest: expected.receipt.request_digest,
        durability: expected.receipt.minimum_durability,
        retry_until_ms: expected.receipt.retry_until_ms,
    };
    let receipt = crate::raft::receipts::lookup_cell_identity(
        database.connection(),
        &request,
        &limits,
        leaf.version.sequence.get(),
    )
    .map_err(CellGroupArchiveError::Replay)?;
    if receipt.as_ref() != Some(&expected.receipt) {
        return Err(CellGroupArchiveError::Invalid("S3 receipt identity/result"));
    }
    store.require_live()?;
    Ok(true)
}
