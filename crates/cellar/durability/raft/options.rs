// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit bounds and disk ownership for one node-wide durable log.

use super::storage::LogError;
use cellar_logic::consensus::{DiskLineage, NodeId, ProcessGeneration};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogOpenMode {
    Create,
    Resume,
}

#[derive(Clone, Debug)]
pub struct ReplicaLogOptions {
    pub directory: PathBuf,
    pub node: NodeId,
    pub disk: DiskLineage,
    pub process: ProcessGeneration,
    pub mode: LogOpenMode,
    pub max_groups: usize,
    pub queue_capacity: usize,
    pub max_pending_bytes: usize,
    pub max_batch_bytes: usize,
    pub max_cache_bytes: usize,
    pub max_entries_per_group: usize,
    pub target_file_bytes: u64,
    pub purge_threshold_bytes: u64,
    /// Minimum interval between dirty-engine purge passes on the storage worker.
    /// `None` leaves physical file reclamation to explicit `ReplicaLog::purge`.
    /// Purging never advances a group's durable checkpoint.
    pub purge_interval_ms: Option<u64>,
    /// Shared anonymous snapshot source/index/receive disk and file admission.
    /// Immutable images, private SQLite/WAL and retained logs are separate.
    pub snapshot_scratch: cellar_ltx::stream_snapshot::ScratchLimits,
}

impl ReplicaLogOptions {
    pub fn new(
        directory: PathBuf,
        node: NodeId,
        disk: DiskLineage,
        process: ProcessGeneration,
        mode: LogOpenMode,
    ) -> Self {
        Self {
            directory,
            node,
            disk,
            process,
            mode,
            max_groups: 4096,
            queue_capacity: 32,
            max_pending_bytes: 64 * 1024 * 1024,
            max_batch_bytes: 32 * 1024 * 1024,
            max_cache_bytes: 256 * 1024 * 1024,
            max_entries_per_group: 16_384,
            target_file_bytes: 16 * 1024 * 1024,
            purge_threshold_bytes: 64 * 1024 * 1024,
            purge_interval_ms: Some(1000),
            snapshot_scratch: Default::default(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), LogError> {
        self.snapshot_scratch
            .validate()
            .map_err(|e| LogError::Invalid(e.to_string()))?;
        if self.directory.as_os_str().is_empty()
            || self.directory.to_str().is_none()
            || self.max_groups == 0
            || self.max_groups > 32_768
            || self.queue_capacity == 0
            || self.queue_capacity > 1024
            || self.max_batch_bytes == 0
            || self.max_batch_bytes > 64 * 1024 * 1024
            || self.max_pending_bytes < self.max_batch_bytes
            || self.max_pending_bytes > 512 * 1024 * 1024
            || self.max_cache_bytes == 0
            || self.max_cache_bytes > 2 * 1024 * 1024 * 1024
            || self.max_entries_per_group == 0
            || self.max_entries_per_group > 65_536
            || self.target_file_bytes < 4096
            || self.target_file_bytes > 128 * 1024 * 1024
            || self.purge_threshold_bytes < self.target_file_bytes
            || self
                .purge_interval_ms
                .is_some_and(|ms| !(1..=60_000).contains(&ms))
        {
            return Err(LogError::Invalid("invalid Raft log options".into()));
        }
        Ok(())
    }
}

/// Node-wide consensus limits. `None` ticks are for an explicit external clock.
#[derive(Clone, Debug)]
pub struct RaftRuntimeOptions {
    pub tick_interval_ms: Option<u64>,
    pub election_seed: Option<[u8; 32]>,
    pub max_groups: usize,
    /// Ordinary requests. Eight additional queue slots are reserved for physical
    /// completions so saturated work can settle. Cell-group completion requests
    /// bound retained manifest metadata with max_ready_bytes.
    /// Transport hint admission has a separate 256 slots/1 MiB; the worker
    /// coalesces retained hints to one small record per current group member.
    pub max_requests: usize,
    /// Inbound peer queues retain this node-wide charge through execution. Each
    /// group additionally admits at most (max_requests / 8).clamp(1, 32) items
    /// and max(max_request_bytes / 4, max_message_bytes + 4096) bytes, including
    /// per-request overhead. A single stalled group cannot fill default bounds.
    pub max_request_bytes: usize,
    pub max_events: usize,
    pub max_event_bytes: usize,
    /// Stop new Ready collection at this threshold; retain already collected work.
    pub max_ready_bytes: usize,
    /// Stop new committed-entry collection here; a collected Ready is retained.
    pub max_apply_bytes: usize,
    pub max_group_apply_bytes: usize,
    pub max_batch_groups: usize,
    pub max_message_bytes: usize,
    pub max_proposal_bytes: usize,
    pub max_members_per_group: usize,
    /// Bounds protocol read states and a separate shared-cell read/pin budget.
    /// At most (max_pending_reads / 8).clamp(1, 32) read pins belong to one group.
    pub max_pending_reads: usize,
    /// Logical image bytes plus 4 KiB per waiting read or selected pin, retained
    /// through query execution. Separate from transport request capacity; both
    /// node and group pin admission use this byte ceiling. No memory is allocated
    /// merely by configuring this limit.
    pub max_read_pin_bytes: usize,
    /// Shared-group creation admission counts applied cells and pending accepted
    /// enrollments. A lower limit does not prevent existing cells from serving.
    /// max_requests also bounds leader-local pending cell acceptance records.
    pub max_cells_per_group: u32,
}

impl Default for RaftRuntimeOptions {
    fn default() -> Self {
        Self {
            tick_interval_ms: Some(100),
            election_seed: None,
            max_groups: 4096,
            max_requests: 256,
            max_request_bytes: 64 * 1024 * 1024,
            max_events: 256,
            max_event_bytes: 128 * 1024 * 1024,
            max_ready_bytes: 256 * 1024 * 1024,
            max_apply_bytes: 64 * 1024 * 1024,
            max_group_apply_bytes: 32 * 1024 * 1024,
            max_batch_groups: 64,
            max_message_bytes: 20 * 1024 * 1024,
            max_proposal_bytes: 16 * 1024 * 1024,
            max_members_per_group: 7,
            max_pending_reads: 256,
            max_read_pin_bytes: 64 * 1024 * 1024,
            max_cells_per_group: 1_000_000,
        }
    }
}

impl RaftRuntimeOptions {
    pub(crate) fn validate(
        &self,
        log: &ReplicaLogOptions,
    ) -> Result<(), super::completion::RuntimeError> {
        if self.tick_interval_ms.is_some_and(|n| n == 0 || n > 60_000)
            || self.max_groups == 0
            || self.max_groups > log.max_groups
            || self.max_requests == 0
            || self.max_requests > 4096
            || self.max_events == 0
            || self.max_events > 4096
            || self.max_proposal_bytes == 0
            || self.max_proposal_bytes > 16 * 1024 * 1024
            || self.max_message_bytes < self.max_proposal_bytes + 4096
            || self.max_message_bytes > 32 * 1024 * 1024
            || self.max_request_bytes < self.max_message_bytes + 4096
            || self.max_request_bytes > 512 * 1024 * 1024
            || self.max_event_bytes < self.max_message_bytes + 4096
            || self.max_event_bytes > 512 * 1024 * 1024
            || self.max_ready_bytes < self.max_event_bytes
            || self.max_ready_bytes > 1024 * 1024 * 1024
            || self.max_group_apply_bytes < self.max_proposal_bytes + 4096
            || self.max_group_apply_bytes > self.max_apply_bytes
            || self.max_apply_bytes > 512 * 1024 * 1024
            || self.max_batch_groups == 0
            || self.max_batch_groups > 256
            || self.max_members_per_group < 3
            || self.max_members_per_group > 16
            || self.max_pending_reads == 0
            || self.max_pending_reads > 4096
            || self.max_read_pin_bytes < 4096
            || self.max_read_pin_bytes as u128
                > (cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES as u128 + 4096)
                    * self.max_pending_reads as u128
            || self.max_cells_per_group == 0
            || self.max_cells_per_group > 1_000_000
        {
            return Err(super::completion::RuntimeError::Invalid(
                "invalid runtime bounds".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_pin_capacity_is_independent_of_transport_and_bounds_images() {
        let log = ReplicaLogOptions::new(
            PathBuf::from("read-bounds"),
            NodeId::new([1; 16]).unwrap(),
            DiskLineage::new([2; 16]).unwrap(),
            ProcessGeneration::new([3; 16]).unwrap(),
            LogOpenMode::Create,
        );
        let mut options = RaftRuntimeOptions::default();
        assert!(options.validate(&log).is_ok());
        let maximum_pin = cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES as u128 + 4096;
        // On narrower platforms the accounting type itself bounds the image.
        options.max_read_pin_bytes = usize::try_from(maximum_pin).unwrap_or(usize::MAX);
        assert_eq!(options.max_request_bytes, 64 * 1024 * 1024);
        assert!(options.validate(&log).is_ok());
        options.max_read_pin_bytes = 4095;
        assert!(options.validate(&log).is_err());
        options.max_read_pin_bytes = 4096;
        assert!(options.validate(&log).is_ok());
        let envelope = maximum_pin * options.max_pending_reads as u128;
        if let Ok(envelope) = usize::try_from(envelope) {
            options.max_read_pin_bytes = envelope;
            assert!(options.validate(&log).is_ok());
            if let Some(too_large) = envelope.checked_add(1) {
                options.max_read_pin_bytes = too_large;
                assert!(options.validate(&log).is_err());
            }
        }
    }
}
