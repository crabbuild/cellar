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
pub struct SubscriptionObservation {
    /// Sampled owner time, clamped to the durable logical clock. A read does
    /// not advance that clock or expire/delete a lease.
    #[serde(with = "super::decimal")]
    pub observed_ms: i64,
    /// Unacknowledged offsets, including history already removed by retention.
    #[serde(with = "super::decimal")]
    pub lag_records: i64,
    #[serde(with = "super::decimal")]
    pub retained_lag_records: i64,
    /// Age since append of the first retained unacknowledged record. None
    /// means there is no retained unread record, not that lost history is new.
    #[serde(with = "super::decimal::optional")]
    pub oldest_retained_age_ms: Option<i64>,
    pub history_expired: bool,
    /// Whether the retained lease has not expired at this sample's time.
    pub lease_active: bool,
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
    pub observation: SubscriptionObservation,
}

impl SubscriptionStatus {
    pub(crate) fn validate(&self) -> Result<()> {
        self.identity.validate()?;
        let lag =
            cellar_logic::event_stream::subscription_lag(self.earliest, self.end, self.checkpoint)
                .map_err(|_| error(ErrorCode::InvalidRequest, "invalid subscription offsets"))?;
        let observation = &self.observation;
        if !matches!(
            self.state.as_str(),
            "active" | "paused" | "expired" | "deleted"
        ) || observation.observed_ms < 0
            || observation.lag_records != lag.records
            || observation.retained_lag_records != lag.retained_records
            || observation.history_expired != lag.history_expired
            || observation.oldest_retained_age_ms.is_some() != (lag.retained_records > 0)
            || observation
                .oldest_retained_age_ms
                .is_some_and(|age| age < 0 || age > observation.observed_ms)
            || observation.lease_active
                != self
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.expires_ms > observation.observed_ms)
            || self.lease.as_ref().is_some_and(|lease| {
                lease.generation <= 0
                    || lease.first <= self.checkpoint
                    || lease.last < lease.first
                    || lease.last >= self.end
                    || lease.expires_ms < 0
            })
            || (self.state == "deleted" && self.lease.is_some())
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid subscription observation",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
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

impl LifecycleReceipt {
    pub(crate) fn validate(&self) -> Result<()> {
        self.previous.validate().map_err(|_| {
            error(
                ErrorCode::ConfigConflict,
                "invalid previous subscription identity",
            )
        })?;
        self.current.validate().map_err(|_| {
            error(
                ErrorCode::ConfigConflict,
                "invalid current subscription identity",
            )
        })?;
        super::store::validate_id(&self.operation)
            .map_err(|_| error(ErrorCode::ConfigConflict, "invalid lifecycle operation"))?;
        if self.previous.resource != self.current.resource
            || self.previous.id != self.current.id
            || self.previous.name != self.current.name
            || self.checkpoint < 0
            || self.checkpoint == i64::MAX
            || self.at_ms < 0
            || self.reason.trim().is_empty()
            || self.reason.len() > 512
            || self
                .operator
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 256)
            || self
                .skipped
                .as_ref()
                .is_some_and(|r| r.first <= 0 || r.last < r.first || r.last == i64::MAX)
        {
            return Err(error(
                ErrorCode::ConfigConflict,
                "invalid subscription lifecycle receipt",
            ));
        }
        let changed = self.previous.generation != self.current.generation;
        let valid = match &self.action {
            ChangeAction::Pause => !changed && self.state == "paused" && self.skipped.is_none(),
            ChangeAction::Resume => !changed && self.state == "active" && self.skipped.is_none(),
            ChangeAction::Reset { cursor } => {
                changed
                    && self.state == "active"
                    && cursor.resource == self.previous.resource
                    && cursor.partition == 0
                    && cursor.next > 0
                    && self.checkpoint == cursor.next - 1
                    && self
                        .skipped
                        .as_ref()
                        .is_none_or(|r| r.last == self.checkpoint)
            }
            ChangeAction::Delete { force } => {
                changed
                    && self.state == "deleted"
                    && (*force || self.skipped.is_none())
                    && self
                        .skipped
                        .as_ref()
                        .is_none_or(|r| self.checkpoint.checked_add(1) == Some(r.first))
            }
        };
        if !valid {
            return Err(error(
                ErrorCode::ConfigConflict,
                "lifecycle action conflicts with receipt",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_acceptance(
        &self,
        change: &Change,
        operator: Option<&str>,
    ) -> Result<()> {
        self.validate()?;
        if self.operation != change.operation
            || self.previous != change.expected
            || self.action != change.action
            || self.reason != change.reason
            || self.operator.as_deref() != operator
        {
            return Err(error(
                ErrorCode::ConfigConflict,
                "subscription operation acceptance mismatch",
            ));
        }
        Ok(())
    }
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

impl AuditPage {
    pub(crate) fn validate(
        &self,
        resource: &super::Resource,
        after: i64,
        maximum: usize,
    ) -> Result<()> {
        if after < 0
            || !(1..=100).contains(&maximum)
            || self.records.len() > maximum
            || (self.has_more && self.records.is_empty())
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "invalid audit page bounds",
            ));
        }
        let mut tail = after;
        let mut bytes = 0usize;
        let mut operations = std::collections::HashSet::new();
        for record in &self.records {
            record.receipt.validate()?;
            if record.sequence <= tail
                || !operations.insert(record.receipt.operation.as_str())
                || record.receipt.previous.resource != resource.id
                || record.principal.is_empty()
                || record.principal.len() > 256
                || record.grant.is_empty()
                || record.grant.len() > 256
            {
                return Err(error(
                    ErrorCode::ConfigConflict,
                    "audit record identity or order conflicts",
                ));
            }
            bytes = bytes
                .checked_add(serde_json::to_vec(&record.receipt)?.len())
                .ok_or_else(|| error(ErrorCode::PayloadTooLarge, "audit receipt byte overflow"))?;
            if bytes > 512 << 10 {
                return Err(error(
                    ErrorCode::PayloadTooLarge,
                    "audit receipt budget exceeded",
                ));
            }
            tail = record.sequence;
        }
        if self.next != tail {
            return Err(error(
                ErrorCode::InvalidCursor,
                "audit continuation conflicts with records",
            ));
        }
        Ok(())
    }
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
        status.validate()?;
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
        let receipt: LifecycleReceipt = serde_json::from_value(value)
            .map_err(|_| error(ErrorCode::InvalidRequest, "invalid lifecycle response"))?;
        receipt.validate_acceptance(change, None)?;
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
        accept_audit(self.resource(), after, max_records, value)
    }
}

fn accept_audit(
    resource: &super::Resource,
    after: i64,
    maximum: usize,
    value: serde_json::Value,
) -> Result<AuditPage> {
    if after < 0 || !(1..=100).contains(&maximum) {
        return Err(error(
            ErrorCode::InvalidRequest,
            "invalid audit request bounds",
        ));
    }
    let rows = value["records"]
        .as_array()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "missing audit records"))?;
    if rows.len() > maximum {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "audit response exceeds record budget",
        ));
    }
    let page: AuditPage = serde_json::from_value(value)
        .map_err(|_| error(ErrorCode::InvalidRequest, "invalid audit response"))?;
    page.validate(resource, after, maximum)?;
    Ok(page)
}

#[cfg(test)]
mod observation_tests {
    use super::*;

    #[test]
    fn observation_acceptance_rejects_inconsistent_metrics_and_keeps_decimal_precision(
    ) -> Result<()> {
        let value = json!({"identity":{"resource":"00000000000000000000000000000001","name":"projection","id":"00000000000000000000000000000002","generation":"00000000000000000000000000000003"},"state":"expired","required":false,"checkpoint":"0","earliest":i64::MAX.to_string(),"end":i64::MAX.to_string(),"retry_at_ms":"0","lease":null,"observation":{"observed_ms":i64::MAX.to_string(),"lag_records":(i64::MAX-1).to_string(),"retained_lag_records":"0","oldest_retained_age_ms":null,"history_expired":true,"lease_active":false}});
        let status: SubscriptionStatus = serde_json::from_value(value.clone())?;
        status.validate()?;
        assert_eq!(serde_json::to_value(status)?, value);
        for (key, invalid) in [
            ("lag_records", json!("0")),
            ("retained_lag_records", json!("1")),
            ("oldest_retained_age_ms", json!("0")),
            ("history_expired", json!(false)),
            ("lease_active", json!(true)),
            ("observed_ms", json!("-1")),
        ] {
            let mut invalid_value = value.clone();
            invalid_value["observation"][key] = invalid;
            assert!(serde_json::from_value::<SubscriptionStatus>(invalid_value)?
                .validate()
                .is_err());
        }
        let mut noncanonical = value;
        noncanonical["observation"]["lag_records"] = json!("01");
        assert!(serde_json::from_value::<SubscriptionStatus>(noncanonical).is_err());
        Ok(())
    }
}

#[cfg(test)]
#[path = "lifecycle_acceptance_tests.rs"]
mod acceptance_tests;
