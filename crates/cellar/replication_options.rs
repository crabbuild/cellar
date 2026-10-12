// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Immutable production replication policy supplied by the application.

pub use crate::ltx_repl::CompactionConfig;
use std::time::Duration;

/// Settings captured before any replication task starts. Durations are rounded
/// up to milliseconds, matching the engine's monotonic durability deadlines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationOptions {
    /// Bucket upload cadence. Zero disables pacing.
    pub flush_interval: Duration,
    pub durability_timeout: Duration,
    pub compaction: Option<CompactionConfig>,
    pub sync_concurrency: usize,
    pub restore_concurrency: usize,
    pub hydration_concurrency: usize,
    pub log_batch_interval: Duration,
    /// Superseded-epoch GC grace. None disables deletion; Some(ZERO) enables
    /// deletion without a grace period. Live epochs are never deleted by GC.
    pub epoch_gc_grace: Option<Duration>,
    /// Background hydration rate in bytes/second; zero disables hydration.
    pub hydrate_bytes_per_second: u64,
    pub paged_restore: bool,
    pub paged_min_bytes: u64,
    /// Non-Queue WAL truncate threshold. Zero disables truncation; Queue
    /// streams always retain passive checkpoints regardless of this value.
    pub truncate_pages: u32,
}

impl Default for ReplicationOptions {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(2),
            durability_timeout: Duration::from_secs(10),
            compaction: Some(CompactionConfig {
                min_txids: 256,
                min_bytes: 32 << 20,
                concurrency: 2,
            }),
            sync_concurrency: 64,
            restore_concurrency: 64,
            hydration_concurrency: 1,
            log_batch_interval: Duration::from_millis(crate::queue_batching::timing().log_ms),
            epoch_gc_grace: None,
            hydrate_bytes_per_second: 16 << 20,
            paged_restore: true,
            paged_min_bytes: 256 << 20,
            truncate_pages: 128,
        }
    }
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    duration
        .as_millis()
        .saturating_add(u128::from(
            !duration.subsec_nanos().is_multiple_of(1_000_000),
        ))
        .min(u64::MAX as u128) as u64
}

impl ReplicationOptions {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.durability_timeout.is_zero(),
            "durability_timeout must be positive"
        );
        for (name, limit) in [
            ("sync_concurrency", self.sync_concurrency),
            ("restore_concurrency", self.restore_concurrency),
            ("hydration_concurrency", self.hydration_concurrency),
        ] {
            anyhow::ensure!(
                limit > 0 && limit <= tokio::sync::Semaphore::MAX_PERMITS,
                "{name} must fit a positive semaphore limit"
            );
        }
        if let Some(config) = self.compaction {
            anyhow::ensure!(
                config.min_txids >= 2,
                "compaction min_txids must be at least 2"
            );
            anyhow::ensure!(
                config.min_bytes > 0
                    && config.min_bytes <= crate::ltx_repl::COMPACTION_MAX_INPUT_BYTES,
                "compaction min_bytes must be positive and within the supported input bound"
            );
            anyhow::ensure!(
                config.concurrency > 0 && config.concurrency <= tokio::sync::Semaphore::MAX_PERMITS,
                "compaction concurrency must fit a positive semaphore limit"
            );
        }
        Ok(())
    }

    /// Resolve the CLI's environment at construction. Library constructors
    /// take explicit options and never call this adapter.
    pub fn from_env() -> anyhow::Result<Self> {
        let defaults = Self::default();
        let retention = crate::env_vars::optional::<u64>("CELLAR_LTX_RETENTION_SECS")?;
        let mb = |name, default| -> anyhow::Result<u64> {
            crate::env_vars::with_default::<u64>(name, default)?
                .checked_mul(1 << 20)
                .ok_or_else(|| anyhow::anyhow!("{name} exceeds the supported byte range"))
        };
        let options = Self {
            compaction: crate::ltx_repl::compaction_config_from_env()?,
            flush_interval: Duration::from_millis(crate::env_vars::with_default(
                "CELLAR_LOG_FLUSH_MS",
                2000u64,
            )?),
            durability_timeout: Duration::from_secs(crate::env_vars::positive_or(
                "CELLAR_LTX_DURABILITY_TIMEOUT_SECS",
                10,
            )?),
            epoch_gc_grace: retention.filter(|s| *s > 0).map(Duration::from_secs),
            hydrate_bytes_per_second: mb(
                "CELLAR_LTX_HYDRATE_MBPS",
                defaults.hydrate_bytes_per_second >> 20,
            )?,
            paged_restore: crate::env_vars::flag("CELLAR_LTX_PAGED", defaults.paged_restore)?,
            paged_min_bytes: mb("CELLAR_LTX_PAGED_MIN_MB", defaults.paged_min_bytes >> 20)?,
            truncate_pages: crate::env_vars::with_default(
                "CELLAR_LTX_TRUNCATE_PAGES",
                defaults.truncate_pages,
            )?,
            ..defaults
        };
        options.validate()?;
        Ok(options)
    }
}
