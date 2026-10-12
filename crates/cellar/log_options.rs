// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Per-host fleet log policy, fixed before a node's first ensemble opens.

pub use crate::node_log::HedgeMode;
pub use cellar_logic::log_evict::EvictionPolicy;
use std::time::Duration;

/// Per-follower admission bounds for a windowed log lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogWindowOptions {
    pub max_appends: usize,
    pub max_bytes: u64,
}

/// Application-selected policy for the production log manager. Defaults read
/// no environment. All replacement ensembles retain these exact options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogOptions {
    pub eviction: EvictionPolicy,
    /// Fixed milliseconds; Fixed(0) disables hedging.
    pub hedge: HedgeMode,
    /// Concurrent rounds when no per-follower window is selected.
    pub pipeline_depth: usize,
    /// A window also determines the ship loop's maximum concurrent rounds.
    pub window: Option<LogWindowOptions>,
    /// Select framed ordered streams instead of individual append requests.
    /// The supplied transport must support `LogTransport::open_stream`.
    pub ordered_streams: bool,
    /// Maximum wait for the lease writer to persist a folded log publication.
    pub record_publish_timeout: Duration,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            eviction: EvictionPolicy::default(),
            hedge: HedgeMode::Adaptive,
            pipeline_depth: 4,
            window: None,
            ordered_streams: false,
            record_publish_timeout: Duration::from_secs(10),
        }
    }
}

impl LogOptions {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.pipeline_depth > 0,
            "log pipeline_depth must be positive"
        );
        anyhow::ensure!(
            !self.record_publish_timeout.is_zero(),
            "log record_publish_timeout must be positive"
        );
        if let Some(window) = self.window {
            anyhow::ensure!(
                window.max_appends > 0 && window.max_bytes > 0,
                "log window append and byte limits must be positive"
            );
            u64::try_from(window.max_appends)?;
        }
        anyhow::ensure!(
            self.eviction.backstop_ms > 0
                && self.eviction.window_ms > 0
                && self.eviction.min_samples > 0
                && self.eviction.sibling_factor > 0
                && self.eviction.hedge_factor > 0,
            "log eviction backstop, sample window, sample count and factors must be positive"
        );
        Ok(())
    }

    /// Resolve the CLI's environment once, before constructing a manager.
    pub fn from_env() -> anyhow::Result<Self> {
        let appends = crate::env_vars::with_default::<usize>("CELLAR_LOG_WINDOW", 0)?;
        let bytes = crate::env_vars::with_default::<u64>("CELLAR_LOG_WINDOW_BYTES", 8 << 20)?;
        let ordered_streams = match crate::env_vars::value("CELLAR_LOG_TRANSPORT")?.as_deref() {
            None | Some("http") => false,
            Some("stream") => true,
            Some(value) => {
                anyhow::bail!("CELLAR_LOG_TRANSPORT must be http or stream, not {value:?}")
            }
        };
        let options = Self {
            eviction: crate::node_log::eviction_policy_from_env()?,
            hedge: crate::env_vars::optional::<u64>("CELLAR_LOG_HEDGE_MS")?
                .map_or(HedgeMode::Adaptive, HedgeMode::Fixed),
            pipeline_depth: crate::env_vars::positive_or("CELLAR_LOG_PIPELINE", 4)?,
            window: (appends > 0).then_some(LogWindowOptions {
                max_appends: appends,
                max_bytes: bytes,
            }),
            ordered_streams,
            ..Default::default()
        };
        options.validate()?;
        Ok(options)
    }

    pub(crate) fn rounds(&self) -> usize {
        self.window
            .map_or(self.pipeline_depth, |window| window.max_appends)
    }
}
