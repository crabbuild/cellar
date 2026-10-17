//! Scheduled occurrence controls and bounded execution-history queries.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Clone, Debug)]
pub struct Scheduled {
    pub scheduled_ms: i64,
    pub cron: String,
    no_retry: Arc<AtomicBool>,
}

impl Scheduled {
    /// Create one occurrence; clones share only this occurrence's retry decision.
    pub(super) fn new(scheduled_ms: i64, cron: String) -> Self {
        Self {
            scheduled_ms,
            cron,
            no_retry: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Observe whether this occurrence's handler declined retry.
    pub(super) fn retry_declined(&self) -> bool {
        self.no_retry.load(Ordering::Acquire)
    }

    /// Decline retry if this handler fails. Late calls only change this
    /// controller's flag; they cannot settle or change another occurrence.
    pub fn no_retry(&self) {
        self.no_retry.store(true, Ordering::Release);
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Configuration {
    pub prefix: String,
    pub deployment_version: String,
    pub schedule_revision: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub before: Option<u64>,
    #[serde(default = "page_size")]
    pub limit: u32,
    pub expression: Option<String>,
}

fn page_size() -> u32 {
    50
}

impl Query {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=100).contains(&self.limit),
            "execution page limit must be 1–100"
        );
        ensure!(
            self.before.is_none_or(|v| v <= i64::MAX as u64),
            "invalid execution cursor"
        );
        ensure!(
            self.expression.as_ref().is_none_or(|v| v.len() <= 256),
            "expression is too long"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Scheduled;

    #[test]
    fn cron_retry_control_is_shared_only_within_one_occurrence() {
        let first = Scheduled::new(1_000, "* * * * *".into());
        let retained = first.clone();
        let next = Scheduled::new(2_000, first.cron.clone());
        retained.no_retry();
        assert!(first.retry_declined());
        assert!(!next.retry_declined());
    }
}
