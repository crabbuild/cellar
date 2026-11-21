// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use cellar::sdk::containers::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub format: u8,
    pub identity: BackendIdentity,
    pub limits: ExecutionControllerLimits,
    pub claim: ExecutionClaim,
    pub action: Action,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Action {
    Activate,
    Execute {
        operation: ExecutionOperation,
        payload: Vec<u8>,
    },
    Reconcile {
        original: ExecutionPendingOperation,
    },
    Pending,
    Lifecycle {
        kind: ExecutionOperationKind,
    },
    SealUnsubmitted {
        operation: ExecutionOperation,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Response {
    pub format: u8,
    pub result: std::result::Result<Value, Failure>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Value {
    Activated,
    Outcome(ExecutionOutcome),
    Pending(Vec<ExecutionPendingOperation>),
    Lifecycle(Option<ExecutionPendingOperation>),
    Sealed(ExecutionPendingOperation),
}

/// Finite errors only: never forward backend messages, credentials or payloads.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Failure {
    Busy,
    Capacity,
    StaleClaim,
    ConflictingRequest,
    RevokedHistory,
    RecoveryPending,
    UnknownOperation,
    Unavailable,
}

impl Failure {
    pub fn from_error(error: anyhow::Error) -> Self {
        match error.downcast_ref::<ExecutionControllerFailure>() {
            Some(ExecutionControllerFailure::Busy) => Self::Busy,
            Some(ExecutionControllerFailure::Capacity) => Self::Capacity,
            Some(ExecutionControllerFailure::StaleClaim) => Self::StaleClaim,
            Some(ExecutionControllerFailure::ConflictingRequest) => Self::ConflictingRequest,
            Some(ExecutionControllerFailure::RevokedHistory) => Self::RevokedHistory,
            Some(ExecutionControllerFailure::RecoveryPending) => Self::RecoveryPending,
            Some(ExecutionControllerFailure::Unavailable) => Self::Unavailable,
            _ => Self::UnknownOperation,
        }
    }

    pub fn into_error(self) -> ExecutionControllerFailure {
        match self {
            Self::Busy => ExecutionControllerFailure::Busy,
            Self::Capacity => ExecutionControllerFailure::Capacity,
            Self::StaleClaim => ExecutionControllerFailure::StaleClaim,
            Self::ConflictingRequest => ExecutionControllerFailure::ConflictingRequest,
            Self::RevokedHistory => ExecutionControllerFailure::RevokedHistory,
            Self::RecoveryPending => ExecutionControllerFailure::RecoveryPending,
            Self::UnknownOperation => ExecutionControllerFailure::UnknownOperation,
            Self::Unavailable => ExecutionControllerFailure::Unavailable,
        }
    }
}

pub(super) async fn read<T: DeserializeOwned>(
    stream: &mut (impl AsyncRead + Unpin),
    maximum: usize,
) -> Result<T> {
    let length = stream.read_u32().await? as usize;
    ensure!(
        length > 0 && length <= maximum,
        "controller frame exceeds transport bound"
    );
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(super) async fn write(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
    maximum: usize,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(
        bytes.len() <= maximum,
        "controller frame exceeds transport bound"
    );
    stream.write_u32(u32::try_from(bytes.len())?).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}
