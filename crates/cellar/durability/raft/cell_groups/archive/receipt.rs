// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read the exact receipt from independently recovered, immutable archive bytes.
use super::*;
use crate::raft::cell_groups::{CellGroupApplyImages, CellImagePin, CellSqlReceiptLookup};
use crate::raft::execution::{
    CellCommittedReceipt, CommandRequest, ExecutionError, ExecutionLimits,
};
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
    let database = open(store, pin, deadline, cancelled)?;
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

pub(in crate::raft::cell_groups) struct LookupRead<'a> {
    pub request: &'a CommandRequest,
    pub limits: ExecutionLimits,
    pub deadline: u64,
    pub cancelled: Arc<AtomicBool>,
}

/// Return the actual archived observation at or beyond the quorum selection.
/// Absence remains ambiguous; it never resolves a submitted producer.
pub(in crate::raft::cell_groups) fn lookup(
    coverage: &CellGroupArchiveCoverage,
    store: &CellGroupArchiveStore,
    selected: &mut CellSqlReceiptLookup,
    read: LookupRead<'_>,
    reserve_result: impl FnMut(usize) -> std::result::Result<(), ExecutionError>,
) -> Result<bool> {
    store.require_live()?;
    let root = coverage.recovery().manifest();
    if root.marker().route() != store.route() || !store.owner.owns(root) {
        return Err(CellGroupArchiveError::Invalid(
            "S3 lookup verifier owner/route",
        ));
    }
    let floor = selected.observation();
    let Some((leaf, pin)) = root.pin_cell(&floor.version.history.cell) else {
        return Ok(false);
    };
    if leaf.version.history != floor.version.history {
        return Err(CellGroupArchiveError::Invalid("S3 lookup history"));
    }
    if leaf.version.sequence < floor.version.sequence {
        return Ok(false);
    }
    if leaf.version == floor.version && leaf.witness != floor.witness {
        return Err(CellGroupArchiveError::Invalid("S3 lookup decision"));
    }
    let database = open(store, pin, read.deadline, read.cancelled)?;
    let receipt = crate::raft::receipts::lookup_cell_identity_admitted(
        database.connection(),
        read.request,
        &read.limits,
        leaf.version.sequence.get(),
        reserve_result,
    )
    .map_err(CellGroupArchiveError::Replay)?;
    // An equal logical version cannot disagree with the quorum read. A known
    // retained result also remains exact across later images; expiry stays typed.
    if (leaf.version == floor.version || selected.receipt().is_some())
        && receipt.as_ref() != selected.receipt()
    {
        return Err(CellGroupArchiveError::Invalid("S3 lookup identity/result"));
    }
    store.require_live()?;
    selected.select_archived(receipt, leaf.observation());
    Ok(true)
}

fn open(
    store: &CellGroupArchiveStore,
    pin: CellImagePin,
    deadline: u64,
    cancelled: Arc<AtomicBool>,
) -> Result<crate::storage::ImmutableQuery> {
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
    Ok(database)
}
