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
