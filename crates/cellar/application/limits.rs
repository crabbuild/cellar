// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! API budgets captured by an embedded host before it builds any cells.
//!
//! `unrestricted()` removes Cellar's compatibility ceilings. SQLite's compiled
//! limits, the structured-value codec, integer ranges and the storage provider
//! still constrain operations. Admission, durability and ownership are separate.
//! Filesystem transfers, handles and transaction work retain explicit finite
//! bounds under `unrestricted()`; filesystem size/inode quotas can use i64 maxima.

use anyhow::{ensure, Result};

/// Limits applied to every cell in one execution domain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellLimits {
    pub filesystem: super::filesystem::FilesystemLimits,
    pub kv: KvLimits,
    pub sql: SqlLimits,
    pub blob: BlobLimits,
    pub queue: cellar_logic::queue::Limits,
    pub events: cellar_logic::event_stream::Limits,
    pub workflow: WorkflowLimits,
}

impl CellLimits {
    pub fn unrestricted() -> Self {
        Self {
            filesystem: super::filesystem::FilesystemLimits {
                max_file_bytes: i64::MAX as u64,
                max_logical_bytes: i64::MAX as u64,
                max_allocated_bytes: i64::MAX as u64,
                max_inodes: i64::MAX as u64,
                ..Default::default()
            },
            kv: KvLimits {
                max_key_bytes: usize::MAX,
                max_value_bytes: usize::MAX,
                max_metadata_bytes: usize::MAX,
                max_bulk_keys: usize::MAX,
                max_list_limit: (i64::MAX - 1) as usize,
                min_expiration_ttl_ms: 0,
            },
            sql: SqlLimits {
                connection: None,
                max_rows: usize::MAX,
                max_result_bytes: usize::MAX,
            },
            blob: BlobLimits {
                max_list_limit: i64::MAX as usize,
                max_multipart_backlog_bytes: usize::MAX,
            },
            queue: cellar_logic::queue::Limits::unrestricted(),
            events: cellar_logic::event_stream::Limits::unrestricted(),
            workflow: WorkflowLimits {
                max_value_bytes: usize::MAX,
                max_stream_bytes: usize::MAX,
                max_batch_size: usize::MAX,
                max_id_bytes: usize::MAX,
                max_step_name_units: usize::MAX,
                max_event_type_bytes: usize::MAX,
                max_retention_ms: None,
                max_sleep_ms: None,
                max_retries: u32::MAX,
            },
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.filesystem.validate()?;
        self.events.validate()?;
        for (name, value) in [
            ("kv.max_key_bytes", self.kv.max_key_bytes),
            ("kv.max_value_bytes", self.kv.max_value_bytes),
            ("kv.max_metadata_bytes", self.kv.max_metadata_bytes),
            ("kv.max_bulk_keys", self.kv.max_bulk_keys),
            ("kv.max_list_limit", self.kv.max_list_limit),
            ("sql.max_rows", self.sql.max_rows),
            ("sql.max_result_bytes", self.sql.max_result_bytes),
            ("blob.max_list_limit", self.blob.max_list_limit),
            (
                "blob.max_multipart_backlog_bytes",
                self.blob.max_multipart_backlog_bytes,
            ),
            ("workflow.max_value_bytes", self.workflow.max_value_bytes),
            ("workflow.max_stream_bytes", self.workflow.max_stream_bytes),
            ("workflow.max_batch_size", self.workflow.max_batch_size),
            ("workflow.max_id_bytes", self.workflow.max_id_bytes),
            (
                "workflow.max_step_name_units",
                self.workflow.max_step_name_units,
            ),
            (
                "workflow.max_event_type_bytes",
                self.workflow.max_event_type_bytes,
            ),
        ] {
            ensure!(value > 0, "{name} must be positive");
        }
        ensure!(
            self.kv.min_expiration_ttl_ms >= 0,
            "KV expiry floor must be nonnegative"
        );
        ensure!(
            self.kv.max_list_limit < i64::MAX as usize,
            "KV list limit must leave room for its continuation row"
        );
        ensure!(
            self.blob.max_list_limit <= i64::MAX as usize,
            "Blob list limit exceeds the listing protocol"
        );
        if let Some(connection) = self.sql.connection {
            connection.validate()?;
        }
        self.queue.validate().map_err(anyhow::Error::new)?;
        for maximum in [self.workflow.max_retention_ms, self.workflow.max_sleep_ms]
            .into_iter()
            .flatten()
        {
            ensure!(
                maximum > 0 && maximum < i64::MAX as u64,
                "Workflow duration limit must fit a positive timestamp"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvLimits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_bulk_keys: usize,
    /// Omitted list sizes still default to 1,000; explicitly requested pages
    /// can grow to this ceiling.
    pub max_list_limit: usize,
    pub min_expiration_ttl_ms: i64,
}

impl Default for KvLimits {
    fn default() -> Self {
        use cellar_logic::kv;
        Self {
            max_key_bytes: kv::MAX_KEY_BYTES,
            max_value_bytes: kv::MAX_VALUE_BYTES,
            max_metadata_bytes: kv::MAX_METADATA_BYTES,
            max_bulk_keys: kv::MAX_BULK_KEYS,
            max_list_limit: kv::MAX_LIST_LIMIT,
            min_expiration_ttl_ms: kv::MIN_EXPIRATION_TTL_MS,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlLimits {
    /// `None` keeps SQLite's native compiled resource ceilings. ATTACH and
    /// SQLite worker threads remain disabled under either policy.
    pub connection: Option<SqliteLimits>,
    pub max_rows: usize,
    pub max_result_bytes: usize,
}

impl Default for SqlLimits {
    fn default() -> Self {
        Self {
            connection: Some(SqliteLimits::default()),
            max_rows: crate::application::sql::SQL_MAX_ROWS,
            max_result_bytes: crate::application::sql::SQL_MAX_RESULT_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqliteLimits {
    pub max_value_bytes: i32,
    pub max_statement_bytes: i32,
    pub max_columns: i32,
    pub max_expression_depth: i32,
    pub max_compound_selects: i32,
    pub max_vm_operations: i32,
    pub max_function_arguments: i32,
    pub max_like_pattern_bytes: i32,
    pub max_bind_parameters: i32,
    pub max_trigger_depth: i32,
}

impl Default for SqliteLimits {
    fn default() -> Self {
        Self {
            max_value_bytes: 2_200_000,
            max_statement_bytes: 100_000,
            max_columns: 100,
            max_expression_depth: 100,
            max_compound_selects: 5,
            max_vm_operations: 25_000,
            max_function_arguments: 127,
            max_like_pattern_bytes: 50,
            max_bind_parameters: 100,
            max_trigger_depth: 10,
        }
    }
}

impl SqliteLimits {
    fn validate(&self) -> Result<()> {
        ensure!(
            [
                self.max_value_bytes,
                self.max_statement_bytes,
                self.max_columns,
                self.max_expression_depth,
                self.max_compound_selects,
                self.max_vm_operations,
                self.max_function_arguments,
                self.max_like_pattern_bytes,
                self.max_bind_parameters,
                self.max_trigger_depth,
            ]
            .into_iter()
            .all(|value| value > 0),
            "SQLite resource limits must be positive"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobLimits {
    /// Omitted list sizes still default to 1,000.
    pub max_list_limit: usize,
    /// Bytes of multipart parts waiting for earlier parts of one upload.
    pub max_multipart_backlog_bytes: usize,
}

impl Default for BlobLimits {
    fn default() -> Self {
        Self {
            max_list_limit: 1000,
            max_multipart_backlog_bytes: 256 << 20,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkflowLimits {
    pub max_value_bytes: usize,
    /// Aggregate persisted stream chunks, including captures awaiting retry cleanup.
    pub max_stream_bytes: usize,
    pub max_batch_size: usize,
    pub max_id_bytes: usize,
    pub max_step_name_units: usize,
    pub max_event_type_bytes: usize,
    /// `None` permits any finite retention within the timestamp range.
    /// Omitted retention still defaults to 30 days.
    pub max_retention_ms: Option<u64>,
    /// Applies to sleeps and event waits. `None` removes the one-year ceiling.
    pub max_sleep_ms: Option<u64>,
    pub max_retries: u32,
}

impl Default for WorkflowLimits {
    fn default() -> Self {
        Self {
            max_value_bytes: 1 << 20,
            max_stream_bytes: 1 << 30,
            max_batch_size: 100,
            max_id_bytes: 100,
            max_step_name_units: 256,
            max_event_type_bytes: 100,
            max_retention_ms: Some(30 * 86_400_000),
            max_sleep_ms: Some(365 * 86_400_000),
            max_retries: 10_000,
        }
    }
}

pub(crate) fn current() -> CellLimits {
    crate::asyncrt::services().cell_limits()
}
