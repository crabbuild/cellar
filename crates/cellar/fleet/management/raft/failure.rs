// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetFailureCode {
    InvalidInput,
    Conflict,
    PayloadConflict,
    Blocked,
    Limit,
    Busy,
    Unavailable,
    NotFound,
    Unsupported,
    WaitTimeout,
    AuthRequired,
    Forbidden,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetOutcome {
    /// Rejected locally before dispatch to the coordination backend.
    NotSubmitted,
    /// This result cannot establish whether the canonical mutation committed.
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetFailure {
    pub code: RaftFleetFailureCode,
    pub outcome: RaftFleetOutcome,
    pub message: String,
}

impl RaftFleetFailure {
    /// Stable operator CLI result classes. Unknown mutations retain their IDs.
    pub fn exit_code(&self) -> i32 {
        use RaftFleetFailureCode as C;
        match self.code {
            C::InvalidInput => 2,
            C::Conflict | C::PayloadConflict => 3,
            C::Blocked | C::Unsupported | C::Forbidden | C::AuthRequired => 4,
            C::WaitTimeout => 6,
            C::Limit | C::Busy | C::Unavailable | C::NotFound => 5,
        }
    }

    pub fn invalid(message: &str) -> Self {
        Self {
            code: RaftFleetFailureCode::InvalidInput,
            outcome: RaftFleetOutcome::NotSubmitted,
            message: message.into(),
        }
    }

    pub fn unsupported() -> Self {
        Self {
            code: RaftFleetFailureCode::Unsupported,
            outcome: RaftFleetOutcome::NotSubmitted,
            message: "Managed Raft operator access is not configured for this fleet.".into(),
        }
    }
}

impl From<FleetCatalogError> for RaftFleetFailure {
    fn from(error: FleetCatalogError) -> Self {
        use RaftFleetFailureCode as C;
        let (code, message) = match error {
            FleetCatalogError::Invalid(reason) => (C::InvalidInput, reason),
            FleetCatalogError::Conflict => (
                C::Conflict,
                "Fleet inputs changed; reconcile the exact request before replacing it.",
            ),
            FleetCatalogError::PayloadConflict => (
                C::PayloadConflict,
                "The request ID already has a different canonical payload.",
            ),
            FleetCatalogError::InventoryIncomplete => (
                C::Blocked,
                "The complete managed namespace inventory is required.",
            ),
            FleetCatalogError::DisruptionInProgress { .. } => (
                C::Blocked,
                "Another fleet operation owns the disruption slot.",
            ),
            FleetCatalogError::SettlementInProgress { .. } => (
                C::Blocked,
                "A restart birth decision is unresolved; reconcile settlement before cancellation.",
            ),
            FleetCatalogError::Resource(_) | FleetCatalogError::Topology(_) => (
                C::Blocked,
                "Fleet resource or topology prerequisites are not satisfied.",
            ),
            FleetCatalogError::Limit => {
                (C::Limit, "The bounded fleet operation exceeds its limit.")
            }
            FleetCatalogError::Overloaded => (
                C::Busy,
                "Fleet control is busy; retain the exact request for retry.",
            ),
            FleetCatalogError::Corrupt(_) | FleetCatalogError::MissingRoot => {
                (C::Unavailable, "Fleet metadata could not be verified.")
            }
            FleetCatalogError::Unconfirmed
            | FleetCatalogError::Storage(_)
            | FleetCatalogError::Observation(_)
            | FleetCatalogError::Closed => (
                C::Unavailable,
                "Fleet state is unavailable or unconfirmed; reconcile the exact request.",
            ),
        };
        // A failed backend call is not a negative admission certificate. In
        // particular an older identical request may still be in flight.
        Self {
            code,
            outcome: RaftFleetOutcome::Unknown,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RaftFleetFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RaftFleetFailure {}
