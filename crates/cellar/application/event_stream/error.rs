// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};

/// Machine-readable event outcome. A code alone never authorizes replay of a
/// business command; retain that operation's identity or exact receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    TransactionRequired,
    WrongDomain,
    EventFamilyMismatch,
    OutboxFull,
    Unavailable,
    OwnershipLost,
    Ambiguous,
    Forbidden,
    NotInitialized,
    ConfigConflict,
    DestinationDeleted,
    SequenceGap,
    PayloadTooLarge,
    InvalidRequest,
    StreamFull,
    ProducerLimit,
    CursorExpired,
    InvalidCursor,
    BatchTooSmall,
    UnsupportedProtocol,
    SubscriptionLimit,
    SubscriptionPaused,
    SubscriptionReset,
    LeaseBusy,
    LeaseExpired,
    StaleReceipt,
    AckObsolete,
    InboxFull,
    TooManyWaiters,
    OperationLimit,
    OperationUnknown,
    ProducerSealed,
    PendingDelivery,
}

impl ErrorCode {
    /// The exact spelling used in event RPC and diagnostic JSON.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransactionRequired => "TransactionRequired",
            Self::WrongDomain => "WrongDomain",
            Self::EventFamilyMismatch => "EventFamilyMismatch",
            Self::OutboxFull => "OutboxFull",
            Self::Unavailable => "Unavailable",
            Self::OwnershipLost => "OwnershipLost",
            Self::Ambiguous => "Ambiguous",
            Self::Forbidden => "Forbidden",
            Self::NotInitialized => "NotInitialized",
            Self::ConfigConflict => "ConfigConflict",
            Self::DestinationDeleted => "DestinationDeleted",
            Self::SequenceGap => "SequenceGap",
            Self::PayloadTooLarge => "PayloadTooLarge",
            Self::InvalidRequest => "InvalidRequest",
            Self::StreamFull => "StreamFull",
            Self::ProducerLimit => "ProducerLimit",
            Self::CursorExpired => "CursorExpired",
            Self::InvalidCursor => "InvalidCursor",
            Self::BatchTooSmall => "BatchTooSmall",
            Self::UnsupportedProtocol => "UnsupportedProtocol",
            Self::SubscriptionLimit => "SubscriptionLimit",
            Self::SubscriptionPaused => "SubscriptionPaused",
            Self::SubscriptionReset => "SubscriptionReset",
            Self::LeaseBusy => "LeaseBusy",
            Self::LeaseExpired => "LeaseExpired",
            Self::StaleReceipt => "StaleReceipt",
            Self::AckObsolete => "AckObsolete",
            Self::InboxFull => "InboxFull",
            Self::TooManyWaiters => "TooManyWaiters",
            Self::OperationLimit => "OperationLimit",
            Self::OperationUnknown => "OperationUnknown",
            Self::ProducerSealed => "ProducerSealed",
            Self::PendingDelivery => "PendingDelivery",
        }
    }

    pub(crate) const BLOCKED_REASONS: [Self; 7] = [
        Self::Forbidden,
        Self::DestinationDeleted,
        Self::SequenceGap,
        Self::PayloadTooLarge,
        Self::UnsupportedProtocol,
        Self::ConfigConflict,
        Self::ProducerSealed,
    ];

    pub(crate) fn is_permanent(self) -> bool {
        Self::BLOCKED_REASONS.contains(&self)
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

impl DeliveryError {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: serde_json::Value::Null,
        }
    }

    /// Whether automatic outbox dispatch stops this lane until repair.
    ///
    /// This is not a general retry policy for SDK calls. For example, an
    /// ambiguous result may already have committed, and a revoked grant can be
    /// repaired even though it stops automatic delivery.
    pub fn is_permanent(&self) -> bool {
        self.code.is_permanent()
    }
}

impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DeliveryError {}

pub(crate) fn error(code: ErrorCode, message: impl Into<String>) -> anyhow::Error {
    DeliveryError::new(code, message).into()
}

pub(crate) const MAX_REPLY_BYTES: usize = 64 << 20;
const MAX_ERROR_BYTES: usize = 16 << 10;
const MAX_ERROR_MESSAGE_BYTES: usize = 4096;

fn ambiguous_reply() -> anyhow::Error {
    error(
        ErrorCode::Ambiguous,
        "event RPC returned an invalid result after submission",
    )
}

/// Apply only to acceptance of an already-submitted mutation's success body.
/// Broker error branches and local preflight failures must bypass this helper.
pub(super) fn submitted_result<T>(result: anyhow::Result<T>) -> anyhow::Result<T> {
    result.map_err(|_| ambiguous_reply())
}

pub(crate) fn decode_reply(data: &crate::engine_api::RpcData) -> anyhow::Result<serde_json::Value> {
    let bytes = data.payload_bytes();
    if bytes > MAX_REPLY_BYTES {
        return Err(ambiguous_reply());
    }
    let value = crate::application::decode_rpc_result(data)
        .and_then(|value| value.to_json())
        .map_err(|_| ambiguous_reply())?;
    unwrap_reply(value)
}

pub(crate) fn unwrap_reply(value: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let serde_json::Value::Object(mut fields) = value else {
        return Err(ambiguous_reply());
    };
    // The broker writes exactly one branch. A contradictory result cannot
    // authorize success or a permanent refusal after a submitted mutation.
    if fields.len() != 1 {
        return Err(ambiguous_reply());
    }
    if let Some(value) = fields.remove("ok") {
        return Ok(value);
    }
    let failure = fields.remove("error").ok_or_else(ambiguous_reply)?;
    if !failure
        .get("message")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|message| message.len() <= MAX_ERROR_MESSAGE_BYTES)
    {
        return Err(ambiguous_reply());
    }
    super::encoding::json(&failure, MAX_ERROR_BYTES).map_err(|_| ambiguous_reply())?;
    let failure: DeliveryError = serde_json::from_value(failure).map_err(|_| ambiguous_reply())?;
    Err(failure.into())
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
