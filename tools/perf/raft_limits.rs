// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit node-wide banks. Producer/job/batch settings are upper ceilings;
//! the shared archive scheduler currently processes one physical group at a time.
use super::*;

pub fn shared(
    host: &RaftHostOptions,
    groups: usize,
    occupancy: usize,
) -> Result<CellGroupNodeLimits> {
    let capacity = host.max_groups;
    let sources = host
        .archive
        .max_producers
        .min(host.archive.max_blocking_jobs)
        .min(capacity);
    ensure!(sources > 0, "empty checkpoint producer bank");
    Ok(CellGroupNodeLimits {
        namespaces: CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 256 * 1024 * 1024,
        },
        controller: CellGroupControllerLimits {
            max_groups: capacity,
            max_parallel_apply: host.blocking_workers.min(capacity),
            retry_interval_ms: host.poll_ms,
            max_commands: host.max_requests,
            max_command_bytes: host.max_pending_bytes,
            checkpoints: (host.checkpoint_entries > 0).then_some(CellGroupCheckpointPolicy {
                interval_entries: host.checkpoint_entries as u64,
                max_parallel: sources,
            }),
        },
        sql: CellSqlLimits {
            workers: host.blocking_workers,
            queue_per_worker: host.worker_queue,
            max_pending_cells: host.max_requests,
            group_pending_cells: host.max_requests.min(occupancy),
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 256 * 1024 * 1024,
        },
        provisioning: CellGroupProvisionLimits {
            max_preparations: sources,
            max_preparation_bytes: 512 * 1024 * 1024,
            max_parallel_preparations: sources,
            max_waiters: host.max_requests,
            peer_revision: 1,
        },
        directory: DirectoryLimits {
            max_groups: u32::try_from(groups)?,
            ..Default::default()
        },
        dispatch: CellDispatchLimits {
            max_cached_routes: 1024,
            route_cache_bytes: 1024 * 1024,
            max_invocations: host.max_requests,
            retained_bytes: host.max_pending_bytes,
            max_input_bytes: host.max_input_bytes,
        },
        manifest: ManifestLimits {
            max_cells: u32::try_from(occupancy)?,
            max_depth: 32,
        },
        entries: GroupEntryLimits {
            envelope: host.execution.envelope,
            ..Default::default()
        },
        manifest_pages: 32_768,
        manifest_bytes: 64 * 1024 * 1024,
        retained_roots: 16,
        max_effect_rows: 1024,
        checkpoints: Some(CellGroupCheckpointLimits {
            max_sources: sources,
            max_retained_bytes: 32 * 1024 * 1024,
            max_cells: u32::try_from(occupancy)?,
            max_transfer_bytes: 256 * 1024 * 1024,
            max_chunk_bytes: 64 * 1024,
        }),
        archive: Some(CellGroupNodeArchiveOptions {
            io: CellGroupArchiveLimits::default(),
            uploads: CellGroupArchiveUploadLimits::default(),
            tails: tail_limits(host, occupancy)?,
        }),
    })
}

/// Back one shared live-image bank plus the actual rooted writer workspace.
/// The generic 64-GiB extent default is not a lab disk reservation.
pub fn extents(host: &RaftHostOptions) -> Result<RaftExtentOptions> {
    use cellar::sdk::consensus::verification::RootPrivateOptions;
    let workspace = RootPrivateOptions::workspace_bounds(host.execution)?.reservation()?;
    let mut options = RaftExtentOptions::default();
    options.inventory.max_bytes = workspace
        .checked_add(host.images.max_bytes)
        .and_then(|n| n.checked_add(4 * host.execution.max_image_bytes))
        .context("shared extent inventory reservation")?;
    Ok(options)
}

pub fn retention(log: &mut ReplicaLogOptions, requested_bytes: u64) -> Result<()> {
    // The requested unarchived ceiling also bounds all retained entry debt.
    // Shared ledger ceilings can be tighter; never silently enlarge either.
    let limits = &mut log.cell_group_retention;
    limits.node.bytes = limits.node.bytes.min(usize::try_from(requested_bytes)?);
    limits.group.bytes = limits.group.bytes.min(limits.node.bytes);
    limits.progress_node.bytes = limits.progress_node.bytes.min(limits.node.bytes / 8);
    limits.progress_group.bytes = limits.progress_group.bytes.min(limits.group.bytes / 8);
    Ok(())
}

/// Keep the existing 512 MiB replay bank. Wider groups reserve more manifest
/// metadata, so archive smaller tails rather than increasing node memory.
/// The apply charge follows both configured image backends, including their
/// one-extent scratch buffer, instead of reserving an unrelated 128 MiB.
fn tail_limits(host: &RaftHostOptions, occupancy: usize) -> Result<CellGroupArchiveTailLimits> {
    use cellar::sdk::consensus::verification::cell_groups::CellGroupArchiveTailPool;
    let decoded = host
        .images
        .apply
        .max_total_decoded_bytes
        .max(u64::try_from(host.execution.envelope.max_decoded_bytes)?);
    let mut limits = CellGroupArchiveTailLimits {
        max_cells: u32::try_from(occupancy)?,
        max_apply_bytes: usize::try_from(decoded)?
            .checked_add(4 * 1024 * 1024)
            .context("archive apply workspace")?,
        ..Default::default()
    };
    while CellGroupArchiveTailPool::new(limits).is_err() {
        limits.max_tail_bytes /= 2;
        ensure!(
            limits.max_tail_bytes >= 128 * 1024,
            "archive replay cannot fit the fixed node bank"
        );
    }
    Ok(limits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_supported_occupancy_fits_the_original_replay_bank() -> Result<()> {
        let host = RaftHostOptions::default();
        for occupancy in 1..=crate::raft_groups::MAX_OCCUPANCY {
            let limits = tail_limits(&host, occupancy)?;
            assert_eq!(limits.max_cells, occupancy as u32);
            assert_eq!(limits.max_retained_bytes, 512 * 1024 * 1024);
            assert!(limits.max_tail_bytes <= CellGroupArchiveTailLimits::default().max_tail_bytes);
            assert!(limits.max_tail_bytes >= 128 * 1024);
        }
        Ok(())
    }
}
