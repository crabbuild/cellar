use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    TransactionRequired,
    WrongDomain,
    EventFamilyMismatch,
    OutboxFull,
    Unavailable,
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

    pub fn is_permanent(&self) -> bool {
        self.code.is_permanent()
    }
}

impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for DeliveryError {}

pub(crate) fn error(code: ErrorCode, message: impl Into<String>) -> anyhow::Error {
    DeliveryError::new(code, message).into()
}

pub(crate) fn unwrap_reply(value: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    if let Some(error) = value.get("error") {
        return Err(serde_json::from_value::<DeliveryError>(error.clone())?.into());
    }
    value
        .get("ok")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("invalid event RPC reply"))
}
