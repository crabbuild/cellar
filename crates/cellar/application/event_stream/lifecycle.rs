//! Owner-routed subscription administration and durable operation receipts.

use super::{authority::Action, error::error, Cursor, ErrorCode, Stream, Subscription};
use crate::application::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionIdentity {
    pub resource: String,
    pub name: String,
    pub id: String,
    pub generation: String,
}

impl SubscriptionIdentity {
    pub(crate) fn validate(&self) -> Result<()> {
        for id in [&self.resource, &self.id, &self.generation] {
            super::store::validate_id(id)?;
        }
        if self.name.is_empty() || self.name.len() > 128 {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid subscription identity",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseSummary {
    #[serde(with = "super::decimal")]
    pub generation: i64,
    #[serde(with = "super::decimal")]
    pub first: i64,
    #[serde(with = "super::decimal")]
    pub last: i64,
    #[serde(with = "super::decimal")]
    pub expires_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionStatus {
    pub identity: SubscriptionIdentity,
    pub state: String,
    pub required: bool,
    #[serde(with = "super::decimal")]
    pub checkpoint: i64,
    #[serde(with = "super::decimal")]
    pub earliest: i64,
    #[serde(with = "super::decimal")]
    pub end: i64,
    #[serde(with = "super::decimal")]
    pub retry_at_ms: i64,
    pub lease: Option<LeaseSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChangeAction {
    Pause,
    Resume,
    Reset { cursor: Cursor },
    Delete { force: bool },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub operation: String,
    pub expected: SubscriptionIdentity,
    pub action: ChangeAction,
    pub reason: String,
}

impl Change {
    pub(crate) fn validate(&self) -> Result<()> {
        super::store::validate_id(&self.operation)?;
        self.expected.validate()?;
        if self.reason.trim().is_empty() || self.reason.len() > 512 {
            return Err(error(
                ErrorCode::InvalidRequest,
                "a bounded reason is required for subscription administration",
            ));
        }
        if let ChangeAction::Reset { cursor } = &self.action {
            if cursor.resource != self.expected.resource
                || cursor.partition != 0
                || cursor.next <= 0
            {
                return Err(error(
                    ErrorCode::InvalidCursor,
                    "reset cursor belongs to another resource",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OffsetRange {
    #[serde(with = "super::decimal")]
    pub first: i64,
    #[serde(with = "super::decimal")]
    pub last: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleReceipt {
    pub operation: String,
    pub previous: SubscriptionIdentity,
    pub current: SubscriptionIdentity,
    pub action: ChangeAction,
    pub state: String,
    #[serde(with = "super::decimal")]
    pub checkpoint: i64,
    pub skipped: Option<OffsetRange>,
    pub reason: String,
    #[serde(with = "super::decimal")]
    pub at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditRecord {
    #[serde(with = "super::decimal")]
    pub sequence: i64,
    pub principal: String,
    pub grant: String,
    pub receipt: LifecycleReceipt,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditPage {
    pub records: Vec<AuditRecord>,
    #[serde(with = "super::decimal")]
    pub next: i64,
    pub has_more: bool,
}

impl<E> Subscription<E> {
    pub async fn status(&self, context: &Context) -> Result<SubscriptionStatus> {
        self.stream.check_domain()?;
        let action = if self
            .stream
            .binding
            .subscriptions
            .iter()
            .any(|name| name == &self.name)
        {
            Action::Consume
        } else {
            Action::Administer
        };
        let value = super::call_bound(
            context,
            &self.stream.binding,
            action,
            "__streamSubscriptionStatus",
            cellar_native::clone::StructuredValue::from_json(json!([{"subscription":self.name}]))?,
        )
        .await?;
        let status: SubscriptionStatus = serde_json::from_value(value)?;
        self.check_identity(&status.identity)?;
        Ok(status)
    }

    pub(crate) fn check_identity(&self, identity: &SubscriptionIdentity) -> Result<()> {
        identity.validate()?;
        if identity.resource != self.stream.resource().id || identity.name != self.name {
            return Err(error(
                ErrorCode::SubscriptionReset,
                "subscription response names a different resource",
            ));
        }
        Ok(())
    }

    /// Apply a guarded administrative operation. Retry an ambiguous response
    /// with identical arguments and operation identity. Replays return the
    /// original receipt; use status to observe subsequent changes.
    pub async fn change(&self, context: &Context, change: &Change) -> Result<LifecycleReceipt> {
        self.stream.check_domain()?;
        change.validate()?;
        self.check_identity(&change.expected)?;
        let value = super::call_bound(
            context,
            &self.stream.binding,
            Action::Administer,
            "__streamSubscriptionChange",
            cellar_native::clone::StructuredValue::from_json(
                json!([{"subscription":self.name,"change":change}]),
            )?,
        )
        .await?;
        let receipt: LifecycleReceipt = serde_json::from_value(value)?;
        if receipt.operation != change.operation || receipt.previous != change.expected {
            return Err(error(
                ErrorCode::ConfigConflict,
                "subscription operation acceptance mismatch",
            ));
        }
        self.check_identity(&receipt.current)?;
        Ok(receipt)
    }
}

impl<E> Stream<E> {
    pub async fn audit(
        &self,
        context: &Context,
        after: i64,
        max_records: usize,
    ) -> Result<AuditPage> {
        self.check_domain()?;
        if after < 0 || max_records == 0 || max_records > 100 {
            return Err(error(ErrorCode::InvalidRequest, "invalid audit pagination"));
        }
        let value = super::call_bound(
            context,
            &self.binding,
            Action::Administer,
            "__streamAudit",
            cellar_native::clone::StructuredValue::from_json(
                json!([{"after":after.to_string(),"max_records":max_records}]),
            )?,
        )
        .await?;
        Ok(serde_json::from_value(value)?)
    }
}
