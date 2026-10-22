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
        }
    }

    pub(crate) fn validate(&self) -> Result<(), LogError> {
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
    /// Ordinary requests. Eight additional queue slots and 32 KiB are reserved
    /// for physical apply/snapshot completions so saturated work can settle.
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
    pub max_pending_reads: usize,
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
        {
            return Err(super::completion::RuntimeError::Invalid(
                "invalid runtime bounds".into(),
            ));
        }
        Ok(())
    }
}
