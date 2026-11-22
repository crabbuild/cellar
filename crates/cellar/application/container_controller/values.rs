// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::application::containers::{BackendIdentity, ExecutionClaim};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// An independently owned controller directory. Its identity and limits are
/// immutable across reopening; do not place this directory on a voter disk.
#[derive(Clone, Debug)]
pub struct ExecutionControllerOptions {
    pub directory: PathBuf,
    /// Zero when creating; create generates a fresh ID. Persist the returned
    /// options outside this directory and pass that exact ID when opening.
    pub controller_id: [u8; 16],
    pub backend: BackendIdentity,
    pub limits: ExecutionControllerLimits,
}

/// Admission bounds. Exhaustion refuses new operations before provider entry.
/// Operation receipts and revoked histories are never implicitly forgotten.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionControllerLimits {
    pub max_cells: usize,
    pub max_incarnations_per_cell: usize,
    pub max_operations_per_incarnation: usize,
    pub max_record_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_result_bytes: usize,
    pub max_inflight: usize,
    pub operation_timeout_ms: u64,
}

impl Default for ExecutionControllerLimits {
    fn default() -> Self {
        Self {
            max_cells: 1024,
            max_incarnations_per_cell: 16,
            max_operations_per_incarnation: 4096,
            max_record_bytes: 4 * 1024 * 1024,
            max_payload_bytes: 64 * 1024,
            max_result_bytes: 64 * 1024,
            max_inflight: 64,
            operation_timeout_ms: 30_000,
        }
    }
}

impl ExecutionControllerOptions {
    pub(super) fn fenced_backend(&self) -> BackendIdentity {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"cellar-controller-configuration-1");
        hash.update(self.controller_id);
        for value in [
            &self.backend.binding,
            &self.backend.provider,
            &self.backend.configuration_digest,
        ] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        let n = self.limits;
        for value in [
            n.max_cells as u64,
            n.max_incarnations_per_cell as u64,
            n.max_operations_per_incarnation as u64,
            n.max_record_bytes as u64,
            n.max_payload_bytes as u64,
            n.max_result_bytes as u64,
            n.max_inflight as u64,
            n.operation_timeout_ms,
        ] {
            hash.update(value.to_be_bytes());
        }
        BackendIdentity {
            binding: self.backend.binding.clone(),
            provider: self.backend.provider.clone(),
            configuration_digest: hex::encode(hash.finalize()),
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.directory.is_absolute(),
            "controller directory must be absolute"
        );
        ensure!(self.controller_id != [0; 16], "empty controller identity");
        crate::storage::container_bindings::validate_name(&self.backend.binding)?;
        crate::storage::container_bindings::validate_name(&self.backend.provider)?;
        ensure!(
            hex_digest(&self.backend.configuration_digest),
            "invalid controller configuration digest"
        );
        let n = self.limits;
        ensure!(
            (1..=16_384).contains(&n.max_cells)
                && (1..=256).contains(&n.max_incarnations_per_cell)
                && (1..=65_536).contains(&n.max_operations_per_incarnation)
                && (16_384..=4 * 1024 * 1024).contains(&n.max_record_bytes)
                && (1..=1024 * 1024).contains(&n.max_payload_bytes)
                && (1..=1024 * 1024).contains(&n.max_result_bytes)
                && (1..=1024).contains(&n.max_inflight)
                && (1..=60_000).contains(&n.operation_timeout_ms),
            "invalid controller bounds"
        );
        Ok(())
    }
}

pub(super) fn hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A finite operation class; payloads and credentials never become journal keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOperationKind {
    Create,
    Inspect,
    Read,
    Execute,
    Write,
    Delete,
    Signal,
    Configure,
}

impl ExecutionOperationKind {
    pub(super) fn is_observation(self) -> bool {
        matches!(self, Self::Read | Self::Inspect)
    }
}

/// Original operation identity. The first accepted deadline is retained and
/// cannot be extended by retries. Payloads are supplied separately; only their
/// digest is persisted before dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionOperation {
    pub id: [u8; 16],
    pub kind: ExecutionOperationKind,
    pub payload_digest: [u8; 32],
    pub expires_unix_ms: u64,
}

impl ExecutionOperation {
    pub fn new(
        id: [u8; 16],
        kind: ExecutionOperationKind,
        payload: &[u8],
        expires_unix_ms: u64,
    ) -> Result<Self> {
        ensure!(
            id != [0; 16] && expires_unix_ms > 0,
            "invalid controller operation identity"
        );
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update(b"cellar-controller-operation-1");
        digest.update(serde_json::to_vec(&kind)?);
        digest.update(payload);
        Ok(Self {
            id,
            kind,
            payload_digest: digest.finalize().into(),
            expires_unix_ms,
        })
    }

    pub(super) fn matches_payload(&self, payload: &[u8]) -> Result<bool> {
        Ok(
            Self::new(self.id, self.kind, payload, self.expires_unix_ms)?.payload_digest
                == self.payload_digest,
        )
    }
}

/// Original submitted metadata for recovery, including its original generation.
/// This supplies no execution permission for the old claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPendingOperation {
    pub claim: ExecutionClaim,
    pub operation: ExecutionOperation,
}

/// Completed bytes are retained before acknowledgement. Refused proves that
/// this exact operation had no provider effect; an error never supplies that proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOutcome {
    Completed(Vec<u8>),
    Refused,
    ExpiredBeforeSubmission,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionControllerFailure {
    Busy,
    Capacity,
    StaleClaim,
    ConflictingRequest,
    RevokedHistory,
    RecoveryPending,
    UnknownOperation,
    Unavailable,
}

impl std::fmt::Display for ExecutionControllerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Busy => "execution controller is busy",
            Self::Capacity => "execution controller capacity exhausted before submission",
            Self::StaleClaim => "execution controller refused a stale claim",
            Self::ConflictingRequest => "execution controller request identity conflicts",
            Self::RevokedHistory => "execution history is permanently revoked or not authorized",
            Self::RecoveryPending => "execution history revocation is unresolved",
            Self::UnknownOperation => "original provider operation requires reconciliation",
            Self::Unavailable => "execution controller state is unavailable",
        })
    }
}

impl std::error::Error for ExecutionControllerFailure {}

/// The provider is reachable only through the controller's authenticated host
/// boundary. Every method runs under the logical binding's exclusive admission.
/// Implementations must not expose an alternate raw provider path.
/// Validate the operation kind against the decoded payload; Read/Inspect must
/// never start work or mutate resources. Provider fencing must survive its own
/// restart or refuse service until independently re-established.
pub trait ExecutionControllerBackend: Send + Sync + 'static {
    fn identity(&self) -> BackendIdentity;

    /// Revoke all previous execution and pending submissions, including open
    /// streams, and prove they cannot resume. Repeating this exact transition
    /// must be safe after controller cancellation/crash. An absent previous
    /// claim still requires checking the owned provider namespace for old debt.
    /// A successor is metadata, not permission to execute a business operation.
    fn revoke(
        self: std::sync::Arc<Self>,
        previous: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        unresolved: Vec<ExecutionPendingOperation>,
    ) -> crate::application::HandlerFuture<()>;

    /// Permanently fence the old Cellar hosts/history as well as all external
    /// execution before a fresh incarnation is admitted. The recovery secret
    /// authorizes this request; it is not a fencing proof. Integrations must use
    /// their infrastructure fencing authority and repeat an interrupted
    /// transition idempotently. There is no permissive default.
    fn revoke_history(
        self: std::sync::Arc<Self>,
        old: cellar_logic::cell_group::CellHistory,
        successor: cellar_logic::cell_group::CellHistory,
        previous: Option<ExecutionClaim>,
        unresolved: Vec<ExecutionPendingOperation>,
    ) -> crate::application::HandlerFuture<()>;

    fn execute(
        self: std::sync::Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> crate::application::HandlerFuture<ExecutionOutcome>;

    /// Observe/reconcile the original request; never replay a create, command
    /// or write. None means uncertainty. Refused requires authoritative proof
    /// that the original submission cannot execute in the future.
    fn reconcile(
        self: std::sync::Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> crate::application::HandlerFuture<Option<ExecutionOutcome>>;
}
