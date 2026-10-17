// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Queue message values, wire codecs and producer budgets.
use super::error;
use anyhow::{ensure, Result};
use cellar_native::clone::{StructuredValue, Value as CloneValue};
use serde_json::Value;

/// Validate producer delay against a captured host policy.
pub(super) fn check_delay(delay: u32, limits: cellar_logic::queue::Limits) -> Result<u32> {
    ensure!(
        delay <= limits.max_delay_seconds,
        "QUEUE_ERROR: delaySeconds must be an integer from 0 to {}",
        limits.max_delay_seconds
    );
    Ok(delay)
}

/// The encoding that the queue producer selected for one message body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueContentType {
    Text,
    Bytes,
    Json,
    Structured,
}

impl QueueContentType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Bytes => "bytes",
            Self::Json => "json",
            // The native structured-clone encoding.
            Self::Structured => "structured",
        }
    }
}

/// One message handed from a queue cell to a consumer handler.
pub struct QueueMessage {
    pub id: String,
    pub timestamp_ms: i64,
    pub body: Vec<u8>,
    pub content_type: QueueContentType,
    pub attempts: u16,
}

/// The queue state observed when a cell leases a batch.
pub struct QueueMetrics {
    pub backlog_count: f64,
    pub backlog_bytes: f64,
    pub oldest_message_timestamp_ms: Option<i64>,
}

/// One leased batch handed to the application handlers.
pub struct QueueBatch {
    pub queue: String,
    pub messages: Vec<QueueMessage>,
    pub metrics: QueueMetrics,
}

/// The batch-wide retry decision made by a queue handler.
#[derive(Debug, Eq, PartialEq)]
pub struct QueueRetryBatch {
    pub retry: bool,
    pub delay_seconds: Option<i32>,
}

/// An explicit retry decision made for one message.
#[derive(Debug, Eq, PartialEq)]
pub struct QueueRetryMessage {
    pub msg_id: String,
    pub delay_seconds: Option<i32>,
}

/// How the queue handler itself completed. Infrastructure failures still use
/// the outer `Result`, so a handler exception can preserve earlier acks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueOutcome {
    Ok,
    Exception,
}

/// The handler outcome and the settlement instructions it made before return.
#[derive(Debug, Eq, PartialEq)]
pub struct QueueDispatchResult {
    pub outcome: QueueOutcome,
    pub error: Option<String>,
    pub ack_all: bool,
    pub retry_batch: QueueRetryBatch,
    pub explicit_acks: Vec<String>,
    pub retry_messages: Vec<QueueRetryMessage>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SendOptions {
    pub delay_seconds: Option<u32>,
}

pub enum Body {
    Text(String),
    Bytes(Vec<u8>),
    Json(Value),
    /// JSON data with UTF-16 strings or binary64 values that serde_json cannot
    /// retain. This uses JSON.stringify data semantics and the JSON wire type.
    JsonValue(StructuredValue),
    Structured(StructuredValue),
}

pub struct BatchEntry {
    pub body: Body,
    pub delay_seconds: Option<u32>,
}

pub(super) struct Encoded {
    pub body: Vec<u8>,
    pub content_type: String,
    pub delay: u32,
}

/// Encode one message, refusing an excess body or delay before host submission.
pub(super) fn encode(
    body: Body,
    delay: u32,
    limits: cellar_logic::queue::Limits,
) -> Result<Encoded> {
    let (body, content_type) = match body {
        Body::Text(text) => (text.into_bytes(), "text"),
        Body::Bytes(bytes) => (bytes, "bytes"),
        Body::Json(value) => (encode_json(&StructuredValue::from_json(value)?)?, "json"),
        Body::JsonValue(value) => (encode_json(&value)?, "json"),
        Body::Structured(value) => (
            cellar_native::clone::encode(&value)
                .map_err(|_| error("the message body cannot be cloned"))?,
            "structured",
        ),
    };
    ensure!(
        body.len() <= limits.max_message_bytes,
        "QUEUE_ERROR: a message is at most {} bytes, got {}",
        limits.max_message_bytes,
        body.len()
    );
    Ok(Encoded {
        body,
        content_type: content_type.into(),
        delay: check_delay(delay, limits)?,
    })
}

fn encode_json(value: &StructuredValue) -> Result<Vec<u8>> {
    Ok(cellar_native::json::stringify(value)
        .map_err(|_| error("the JSON message body cannot be serialized"))?
        .ok_or_else(|| error("the JSON message body cannot be undefined"))?
        .into_bytes())
}

/// Decode a delivery while retaining JSON values that need structured encoding.
pub(super) fn decode(body: Vec<u8>, content_type: QueueContentType) -> Result<Body> {
    let text = |bytes: &[u8]| -> Result<String> {
        Ok(
            cellar_native::encoding::TextDecoder::new("utf-8", Default::default())?
                .decode(bytes, false)?,
        )
    };
    Ok(match content_type {
        QueueContentType::Text => Body::Text(text(&body)?),
        QueueContentType::Bytes => Body::Bytes(body),
        QueueContentType::Json => {
            let value = cellar_native::json::parse_utf8(&text(&body)?)?;
            // Keep the convenient serde view only when it retains every
            // string and number. JSON.parse accepts -0 and overflow to Infinity.
            let preserves_numbers = |value: &CloneValue| !matches!(value,CloneValue::Number(number) if !number.is_finite() || (*number==0.0 && number.is_sign_negative()));
            let safe = preserves_numbers(&value.root)
                && value.objects.iter().all(|object| match object {
                    cellar_native::clone::Object::Object(properties) => {
                        properties.iter().all(|(_, value)| preserves_numbers(value))
                    }
                    cellar_native::clone::Object::DenseArray { elements, .. } => {
                        elements.iter().all(preserves_numbers)
                    }
                    _ => false,
                });
            let plain = if safe { value.to_json().ok() } else { None };
            match plain {
                Some(value) => Body::Json(value),
                None => Body::JsonValue(value),
            }
        }
        QueueContentType::Structured => Body::Structured(cellar_native::clone::decode(&body)?),
    })
}

#[cfg(test)]
mod tests {
    use crate::application::{limits, queue};

    #[test]
    fn queue_codec_enforces_the_supplied_budget_without_a_host() -> anyhow::Result<()> {
        let mut limits = limits::CellLimits::default().queue;
        limits.max_message_bytes = 3;
        limits.max_delay_seconds = 2;
        let encoded = queue::message::encode(queue::Body::Bytes(vec![1, 2, 3]), 2, limits)?;
        assert_eq!(encoded.body, [1, 2, 3]);
        assert!(queue::message::encode(queue::Body::Bytes(vec![0; 4]), 2, limits).is_err());
        assert!(queue::message::encode(queue::Body::Bytes(vec![]), 3, limits).is_err());
        let queue::Body::JsonValue(value) =
            queue::message::decode(b"-0".to_vec(), queue::QueueContentType::Json)?
        else {
            panic!("negative zero must retain its structured representation");
        };
        assert!(
            matches!(value.root, cellar_native::clone::Value::Number(number) if number == 0.0 && number.is_sign_negative())
        );
        Ok(())
    }
}
