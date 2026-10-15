//! Durable subscription handles and exact batch receipts. A receipt identifies
//! processing authority; fetching a batch never advances its checkpoint.

use super::{authority::Action, error::error, ErrorCode, Record, Stream};
use crate::application::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub(crate) resource: String,
    pub(crate) partition: u32,
    pub(crate) name: String,
    pub(crate) subscription: String,
    pub(crate) incarnation: String,
    #[serde(with = "super::decimal")]
    pub(crate) generation: i64,
    pub(crate) token: String,
    #[serde(with = "super::decimal")]
    pub(crate) first: i64,
    #[serde(with = "super::decimal")]
    pub(crate) last: i64,
}

impl Receipt {
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "v1.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        if encoded.len() > 2048 {
            return Err(error(ErrorCode::StaleReceipt, "receipt is too large"));
        }
        let encoded = encoded
            .strip_prefix("v1.")
            .ok_or_else(|| error(ErrorCode::StaleReceipt, "unsupported receipt format"))?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| error(ErrorCode::StaleReceipt, "invalid receipt encoding"))?;
        let receipt: Self = serde_json::from_slice(&bytes)
            .map_err(|_| error(ErrorCode::StaleReceipt, "invalid receipt fields"))?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for value in [&self.resource, &self.subscription, &self.incarnation] {
            super::store::validate_id(value)
                .map_err(|_| error(ErrorCode::StaleReceipt, "invalid receipt identity"))?;
        }
        if self.partition != 0
            || self.name.is_empty()
            || self.name.len() > 128
            || self.token.len() != 64
            || !self
                .token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.generation <= 0
            || self.first <= 0
            || self.last < self.first
        {
            return Err(error(ErrorCode::StaleReceipt, "invalid delivery receipt"));
        }
        Ok(())
    }

    pub(crate) fn policy(&self) -> Result<cellar_logic::event_stream::Receipt> {
        self.validate()?;
        Ok(cellar_logic::event_stream::Receipt {
            subscription: hex::decode(&self.subscription)?.try_into().unwrap(),
            incarnation: hex::decode(&self.incarnation)?.try_into().unwrap(),
            generation: self.generation,
            token: hex::decode(&self.token)?.try_into().unwrap(),
            first: self.first,
            last: self.last,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BatchOptions {
    pub max_records: usize,
    pub max_bytes: usize,
    pub wait_ms: i64,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_records: 100,
            max_bytes: 1 << 20,
            wait_ms: 30_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Acknowledgment {
    Acknowledged,
    AlreadyAcknowledged,
    Obsolete,
}

pub struct Subscription<E> {
    pub(crate) stream: Stream<E>,
    pub(crate) name: String,
    resolved: Arc<Mutex<Option<super::SubscriptionIdentity>>>,
}

impl<E> Clone for Subscription<E> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            name: self.name.clone(),
            resolved: self.resolved.clone(),
        }
    }
}

pub struct Batch<E> {
    subscription: Subscription<E>,
    receipt: Receipt,
    pub records: Vec<Record>,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

impl<E> Stream<E> {
    pub fn subscription(&self, name: &str) -> Result<Subscription<E>> {
        self.check_domain()?;
        if name.is_empty()
            || name.len() > 128
            || (!self.binding.administer
                && !self
                    .binding
                    .subscriptions
                    .iter()
                    .any(|allowed| allowed == name))
        {
            return Err(error(
                ErrorCode::Forbidden,
                "binding does not grant this subscription",
            ));
        }
        Ok(Subscription {
            stream: self.clone(),
            name: name.into(),
            resolved: Arc::new(Mutex::new(None)),
        })
    }
}

impl<E> Subscription<E> {
    pub fn name(&self) -> &str {
        &self.name
    }

    async fn call(&self, context: &Context, method: &str, mut body: Value) -> Result<Value> {
        self.stream.check_domain()?;
        body["subscription"] = json!(self.name);
        super::call_bound(
            context,
            &self.stream.binding,
            Action::Consume,
            method,
            cellar_native::clone::StructuredValue::from_json(json!([body]))?,
        )
        .await
    }

    pub async fn next(&self, context: &Context, options: BatchOptions) -> Result<Option<Batch<E>>> {
        // One identity for this whole transport call. An embedding that retries
        // ambiguous calls can use next_with_id to preserve it across reconnects.
        self.next_with_id(context, &super::store::uuid("subscription-fetch"), options)
            .await
    }

    pub async fn next_with_id(
        &self,
        context: &Context,
        fetch_id: &str,
        options: BatchOptions,
    ) -> Result<Option<Batch<E>>> {
        super::store::validate_id(fetch_id)?;
        if options.max_records == 0 || options.max_bytes == 0 || options.wait_ms < 0 {
            return Err(error(ErrorCode::InvalidRequest, "invalid fetch budgets"));
        }
        let limits = crate::application::limits::current().events;
        let cached = self.resolved.lock().unwrap().clone();
        let identity = if let Some(identity) = cached {
            identity
        } else {
            let observed = self.status(context).await?.identity;
            let mut slot = self.resolved.lock().unwrap();
            if slot.as_ref().is_some_and(|identity| identity != &observed) {
                return Err(error(
                    ErrorCode::SubscriptionReset,
                    "subscription changed while resolving its identity",
                ));
            }
            slot.get_or_insert(observed).clone()
        };
        let value = self
            .call(
                context,
                "__streamNext",
                json!({"fetch_id":fetch_id,"expected":&identity,
            "max_records":options.max_records.min(limits.batch_records()),
            "max_bytes":options.max_bytes.min(limits.batch_bytes()),
            "wait_ms":options.wait_ms.min(limits.max_poll_ms).to_string()}),
            )
            .await?;
        if value.is_null() {
            return Ok(None);
        }
        let (receipt, records, issued_at_ms, expires_at_ms) = super::error::submitted_result(
            accept_batch(self.stream.resource(), &identity, options, limits, value),
        )?;
        Ok(Some(Batch {
            subscription: self.clone(),
            receipt,
            records,
            issued_at_ms,
            expires_at_ms,
        }))
    }

    fn check_receipt(&self, receipt: &Receipt) -> Result<()> {
        receipt.validate()?;
        if receipt.resource != self.stream.resource().id || receipt.name != self.name {
            return Err(error(
                ErrorCode::StaleReceipt,
                "receipt belongs to another subscription",
            ));
        }
        Ok(())
    }

    pub async fn ack(&self, context: &Context, receipt: &Receipt) -> Result<Acknowledgment> {
        self.check_receipt(receipt)?;
        let value = self
            .call(context, "__streamAck", json!({"receipt":receipt.encode()?}))
            .await?;
        accept_acknowledgment(&value)
    }

    pub async fn retry(&self, context: &Context, receipt: &Receipt, delay_ms: i64) -> Result<()> {
        self.check_receipt(receipt)?;
        if delay_ms < 0
            || delay_ms
                > crate::application::limits::current()
                    .events
                    .max_retry_delay_ms
        {
            return Err(error(
                ErrorCode::InvalidRequest,
                "retry delay exceeds policy",
            ));
        }
        let value = self
            .call(
                context,
                "__streamRetry",
                json!({"receipt":receipt.encode()?,"delay_ms":delay_ms.to_string()}),
            )
            .await?;
        super::error::submitted_result(accept_retry(delay_ms, &value))
    }

    /// An absolute expiry is retry-safe: repeating the request cannot add time.
    pub async fn extend_until(
        &self,
        context: &Context,
        receipt: &Receipt,
        expires_ms: i64,
    ) -> Result<i64> {
        self.check_receipt(receipt)?;
        if expires_ms < 0 {
            return Err(error(ErrorCode::InvalidRequest, "negative lease expiry"));
        }
        let value = self
            .call(
                context,
                "__streamExtend",
                json!({"receipt":receipt.encode()?,"expires_ms":expires_ms.to_string()}),
            )
            .await?;
        super::error::submitted_result(accept_extension(expires_ms, &value))
    }
}

fn accept_acknowledgment(value: &Value) -> Result<Acknowledgment> {
    super::error::submitted_result(match value["status"].as_str() {
        Some("acknowledged") => Ok(Acknowledgment::Acknowledged),
        Some("already_acknowledged") => Ok(Acknowledgment::AlreadyAcknowledged),
        Some("obsolete") => Ok(Acknowledgment::Obsolete),
        _ => Err(error(
            ErrorCode::StaleReceipt,
            "invalid acknowledgment response",
        )),
    })
}

fn accept_retry(delay: i64, value: &Value) -> Result<()> {
    let deadline = super::subscription_store::integer(value, "retry_at_ms")?;
    if deadline < delay || deadline < 0 {
        return Err(error(
            ErrorCode::StaleReceipt,
            "retry response deadline conflicts with delay",
        ));
    }
    Ok(())
}

fn accept_extension(requested: i64, value: &Value) -> Result<i64> {
    let accepted = super::subscription_store::integer(value, "expires_ms")?;
    if accepted < requested || accepted <= 0 {
        return Err(error(
            ErrorCode::StaleReceipt,
            "extension response shortened requested expiry",
        ));
    }
    Ok(accepted)
}

fn accept_batch(
    resource: &super::Resource,
    identity: &super::SubscriptionIdentity,
    options: BatchOptions,
    limits: cellar_logic::event_stream::Limits,
    value: Value,
) -> Result<(Receipt, Vec<Record>, i64, i64)> {
    identity.validate()?;
    let receipt = Receipt::decode(
        value["receipt"]
            .as_str()
            .ok_or_else(|| error(ErrorCode::StaleReceipt, "missing batch receipt"))?,
    )?;
    if receipt.resource != resource.id
        || identity.resource != resource.id
        || receipt.name != identity.name
        || receipt.subscription != identity.id
        || receipt.incarnation != identity.generation
    {
        return Err(error(
            ErrorCode::SubscriptionReset,
            "batch names another subscription incarnation",
        ));
    }
    let issued = super::subscription_store::integer(&value, "issued_ms")?;
    let expires = super::subscription_store::integer(&value, "expires_ms")?;
    if issued < 0
        || expires <= issued
        || expires
            .checked_sub(issued)
            .is_none_or(|duration| duration > limits.max_lease_lifetime_ms)
    {
        return Err(error(
            ErrorCode::StaleReceipt,
            "invalid batch lease interval",
        ));
    }
    let rows = value["records"]
        .as_array()
        .ok_or_else(|| error(ErrorCode::InvalidRequest, "missing batch records"))?;
    let count = options.max_records.min(limits.batch_records());
    let budget = options.max_bytes.min(limits.batch_bytes());
    if count == 0 || budget == 0 || rows.len() > count {
        return Err(error(
            ErrorCode::PayloadTooLarge,
            "batch exceeds receiver limits",
        ));
    }
    if rows.is_empty()
        || i64::try_from(rows.len()).ok()
            != receipt
                .last
                .checked_sub(receipt.first)
                .and_then(|n| n.checked_add(1))
    {
        return Err(error(
            ErrorCode::StaleReceipt,
            "batch does not match receipt",
        ));
    }
    let mut records = Vec::new();
    let mut total = 0usize;
    for (index, row) in rows.iter().enumerate() {
        let offset = super::subscription_store::integer(row, "offset")?;
        if receipt.first.checked_add(i64::try_from(index)?) != Some(offset) {
            return Err(error(
                ErrorCode::StaleReceipt,
                "batch skipped or repeated an offset",
            ));
        }
        let record =
            super::client::decode_record(resource, row, limits, budget.saturating_sub(total))?;
        total = total
            .checked_add(record.encoded.len())
            .ok_or_else(|| error(ErrorCode::PayloadTooLarge, "batch byte overflow"))?;
        records.push(record);
    }
    Ok((receipt, records, issued, expires))
}

#[cfg(test)]
#[path = "subscription_tests.rs"]
mod tests;

impl<E> Batch<E> {
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }

    pub async fn ack(&self, context: &Context) -> Result<Acknowledgment> {
        self.subscription.ack(context, &self.receipt).await
    }

    pub async fn retry(&self, context: &Context, delay_ms: i64) -> Result<()> {
        self.subscription
            .retry(context, &self.receipt, delay_ms)
            .await
    }

    pub async fn extend_until(&self, context: &Context, expires_ms: i64) -> Result<i64> {
        self.subscription
            .extend_until(context, &self.receipt, expires_ms)
            .await
    }
}
