// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/// Captured resource ceilings for one cell's filesystem. Transfer ceilings are
/// independent of logical file sizes, so large files use bounded positional I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct FilesystemLimits {
    pub max_file_bytes: u64,
    pub max_logical_bytes: u64,
    pub max_allocated_bytes: u64,
    pub max_inodes: u64,
    pub max_transfer_bytes: usize,
    pub max_file_helper_bytes: usize,
    pub max_handles: usize,
    /// Live descriptors across all cells in the owning execution domain.
    pub max_host_handles: usize,
    pub max_path_bytes: usize,
    pub max_path_depth: usize,
    pub max_discarded_ranges: usize,
    pub gc_batch_chunks: usize,
    pub max_transaction_operations: usize,
    pub max_transaction_bytes: usize,
    pub max_directory_entries: usize,
    pub max_namespace_directories: usize,
}

impl Default for FilesystemLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 1 << 30,
            max_logical_bytes: 4 << 30,
            max_allocated_bytes: 1 << 30,
            max_inodes: 100_000,
            max_transfer_bytes: 1 << 20,
            max_file_helper_bytes: 1 << 20,
            max_handles: 256,
            max_host_handles: 16_384,
            max_path_bytes: 4096,
            max_path_depth: 64,
            max_discarded_ranges: 64,
            gc_batch_chunks: 64,
            max_transaction_operations: 1024,
            max_transaction_bytes: 4 << 20,
            max_directory_entries: 1024,
            max_namespace_directories: 1024,
        }
    }
}

impl FilesystemLimits {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.max_file_bytes > 0
                && self.max_file_bytes <= i64::MAX as u64
                && self.max_logical_bytes > 0
                && self.max_logical_bytes <= i64::MAX as u64
                && self.max_allocated_bytes >= super::CHUNK_BYTES as u64
                && self.max_allocated_bytes <= i64::MAX as u64
                && self.max_inodes > 0
                && self.max_inodes <= i64::MAX as u64,
            "filesystem byte/inode limits must fit positive SQLite integers"
        );
        for (name, value) in [
            ("transfer", self.max_transfer_bytes),
            ("whole-file helper", self.max_file_helper_bytes),
            ("handles", self.max_handles),
            ("host handles", self.max_host_handles),
            ("path bytes", self.max_path_bytes),
            ("path depth", self.max_path_depth),
            ("discarded ranges", self.max_discarded_ranges),
            ("GC batch", self.gc_batch_chunks),
            ("transaction operations", self.max_transaction_operations),
            ("transaction bytes", self.max_transaction_bytes),
            ("directory entries", self.max_directory_entries),
            ("namespace directories", self.max_namespace_directories),
        ] {
            anyhow::ensure!(
                value > 0 && value <= i32::MAX as usize,
                "filesystem {name} limit must fit a positive protocol integer"
            );
        }
        Ok(())
    }
}
