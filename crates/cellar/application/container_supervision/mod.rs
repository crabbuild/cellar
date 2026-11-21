// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-selected maintenance for durable provider bindings. Every tick receives
//! fresh owner event authority; registration never retains an application turn.
pub(crate) mod controller;
mod registry;
pub(crate) use controller::{maintain, recover};
pub use registry::{supervise, Supervision};

use super::{
    container_bindings::BindingCheckpoint,
    container_execution::{ExecutionFenceAuthority, ExecutionGrant},
    Context, HandlerFuture,
};
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendIdentity {
    pub binding: String,
    pub provider: String,
    pub configuration_digest: String,
}

/// Executes one bounded maintenance event. A backend must correlate the
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

/// Shared-group maintenance whose provider requests enforce an execution claim.
/// Register with `Supervision::register_fenced`. The framework selects and pins
/// the controller, proves its claim before dispatch, and keeps lifecycle
/// admission through result settlement. There is no direct-backend fallback.
pub trait FencedMaintenanceBackend: Send + Sync + 'static {
    fn identity(&self) -> BackendIdentity;

    /// Immutable host configuration, with no I/O at registration. Its identity
    /// must match this backend; the configuration digest covers the controller.
    fn authority(&self) -> std::sync::Arc<dyn ExecutionFenceAuthority>;

    fn delete_safety(&self) -> DeleteSafety {
        DeleteSafety::InspectAfterSubmission
    }

    /// Select private metadata preparation without I/O. DeleteIntent is valid
    /// only for a known running resource selected for deletion. The framework
    /// stages that exact transition before proving the work claim and fence;
    /// it does not indicate that Delete was submitted. Opt in only when the
    /// backend independently distinguishes desired intent from submission.
    fn prepare(
        &self,
        _expected: &BindingCheckpoint,
        _action: MaintenanceAction,
    ) -> Result<MaintenancePreparation> {
        Ok(MaintenancePreparation::Unchanged)
    }

    /// Correlate the exact resource and enforce the supplied claim on every
    /// provider request. After changing the journal, refresh the grant and
    /// await its effect gate before further I/O. Do not reacquire the lifecycle
    /// mutex or create compute. Lost outcomes retain their original identities.
    /// Inspect can carry an already retired checkpoint when its original proof
    /// response was lost. Confirm that exact observation without provider I/O;
    /// the framework has proved it and fenced older execution before entry.
    fn maintain(
        self: std::sync::Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
        grant: ExecutionGrant,
    ) -> HandlerFuture<()>;
}

/// Bounded preparation choices; the framework validates and stages the record.
/// No backend can supply a confirmed outcome or arbitrary checkpoint here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenancePreparation {
    Unchanged,
    DeleteIntent,
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
