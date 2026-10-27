// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-selected maintenance for durable provider bindings. Every tick receives
//! fresh native event authority; registration never retains an application turn.
pub(crate) mod controller;
mod registry;
pub(crate) use controller::{maintain, recover};
pub use registry::{supervise, Supervision};

use super::{container_bindings::BindingCheckpoint, Context, HandlerFuture};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

/// Retention is measured from the original durable reservation, not inspection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenancePolicy {
    pub inspect_interval_ms: u64,
    pub maximum_lifetime_ms: u64,
    pub attempt_timeout_ms: u64,
    pub idle_timeout_ms: Option<u64>,
}

impl MaintenancePolicy {
    pub fn validate(self) -> Result<Self> {
        ensure!(
            (10..=3_600_000).contains(&self.inspect_interval_ms),
            "invalid provider inspection interval"
        );
        ensure!(
            (10..=86_400_000).contains(&self.maximum_lifetime_ms),
            "invalid provider maximum lifetime"
        );
        ensure!(
            (10..=120_000).contains(&self.attempt_timeout_ms),
            "invalid provider maintenance timeout"
        );
        ensure!(
            self.idle_timeout_ms
                .is_none_or(|ms| ms > 0 && ms <= self.maximum_lifetime_ms),
            "invalid provider idle timeout"
        );
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceAction {
    Inspect,
    Delete,
}

/// Repeated deletion requires provider-specific exact-resource validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteSafety {
    InspectAfterSubmission,
    RevalidateExactResource,
}

/// Credential-free registration identity; the backend retains host credentials.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BackendIdentity {
    pub binding: String,
    pub provider: String,
    pub configuration_digest: String,
}

/// Executes one bounded native maintenance event. A backend must correlate the
/// expected checkpoint before any effect and use its exact resource/request.
/// It may checkpoint observations with BindingJournal, but never create compute.
/// A lost result is uncertainty, not permission to repeat a create or command.
/// The controller holds the binding's lifecycle guard through this call; a
/// backend must not reacquire it or dispatch through a guarded event session.
pub trait MaintenanceBackend: Send + Sync + 'static {
    fn identity(&self) -> BackendIdentity;

    fn delete_safety(&self) -> DeleteSafety {
        DeleteSafety::InspectAfterSubmission
    }

    fn maintain(
        self: std::sync::Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
    ) -> HandlerFuture<()>;
}

/// Proven pre-dispatch refusal after a retained generation's local deadline.
#[derive(Debug)]
pub struct RetentionExpired;

impl std::fmt::Display for RetentionExpired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider binding retention deadline expired")
    }
}

impl std::error::Error for RetentionExpired {}
