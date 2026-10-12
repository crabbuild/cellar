//! Native Queue producers, consumer settlement handles and the broker cell.
//! Broker policy and fleet dispatch remain the existing Rust implementations.

mod broker;
mod wire;

use super::{decode_rpc_result, encode_rpc, Context, HandlerFuture, Output};
use crate::engine_api::{
    QueueBatch, QueueDispatchResult, QueueOutcome, QueueRetryBatch, QueueRetryMessage,
};
pub use crate::engine_api::{QueueContentType, QueueMetrics};
use anyhow::{anyhow, ensure, Result};
use cellar_native::clone::{StructuredValue, Value as CloneValue};
use futures_util::FutureExt;
use indexmap::{IndexMap, IndexSet};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

pub(crate) use broker::create_cell;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Config {
    pub consumers: HashMap<String, Consumer>,
    pub lease_duration_ms: i64,
    pub limits: Limits,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Consumer {
    pub script: String,
    pub max_batch_size: usize,
    pub max_batch_timeout_ms: i64,
    pub max_retries: u16,
    pub max_concurrency: Option<u16>,
    pub retry_delay_seconds: Option<u32>,
    pub dead_letter_queue: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Limits {
    pub producer_group_ms: u64,
    pub max_concurrency: u16,
    pub retention_ms: i64,
    pub sweep_batch_rows: usize,
}

#[derive(Clone, Debug)]
pub struct Producer {
    queue: Arc<str>,
    delivery_delay: u32,
    json_default: bool,
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
pub struct SendResult {
    pub metrics: QueueMetrics,
}

pub(super) struct Encoded {
    pub body: Vec<u8>,
    pub content_type: String,
    pub delay: u32,
}
pub(super) fn error(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("QUEUE_ERROR: {message}")
}
pub(super) fn now() -> i64 {
    crate::ownership_store::now_ms() as i64
}
pub(super) fn check_delay(delay: u32) -> Result<u32> {
    ensure!(
        delay <= cellar_logic::queue::MAX_DELAY_SECONDS,
        "QUEUE_ERROR: delaySeconds must be an integer from 0 to {}",
        cellar_logic::queue::MAX_DELAY_SECONDS
    );
    Ok(delay)
}
fn encode(body: Body, delay: u32) -> Result<Encoded> {
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
        body.len() <= cellar_logic::queue::MAX_MESSAGE_BYTES,
        "QUEUE_ERROR: a message is at most {} bytes, got {}",
        cellar_logic::queue::MAX_MESSAGE_BYTES,
        body.len()
    );
    Ok(Encoded {
        body,
        content_type: content_type.into(),
        delay: check_delay(delay)?,
    })
}
fn encode_json(value: &StructuredValue) -> Result<Vec<u8>> {
    Ok(cellar_native::json::stringify(value)
        .map_err(|_| error("the JSON message body cannot be serialized"))?
        .ok_or_else(|| error("the JSON message body cannot be undefined"))?
        .into_bytes())
}

impl Producer {
    pub(crate) fn new(queue: String, delivery_delay: u32, json_default: bool) -> Self {
        Self {
            queue: queue.into(),
            delivery_delay,
            json_default,
        }
    }
    pub fn name(&self) -> &str {
        &self.queue
    }
    pub async fn send(
        &self,
        context: &Context,
        body: Body,
        options: SendOptions,
    ) -> Result<SendResult> {
        let entry = encode(body, options.delay_seconds.unwrap_or(self.delivery_delay))?;
        self.send_encoded(context, vec![entry]).await
    }
    /// Apply the deployment's default content type to an ordinary owned value.
    pub async fn send_value(
        &self,
        context: &Context,
        value: StructuredValue,
        options: SendOptions,
    ) -> Result<SendResult> {
        let body = if self.json_default {
            Body::JsonValue(value)
        } else {
            Body::Structured(value)
        };
        self.send(context, body, options).await
    }
    pub async fn send_batch(
        &self,
        context: &Context,
        messages: impl IntoIterator<Item = BatchEntry>,
        options: SendOptions,
    ) -> Result<SendResult> {
        let delay = check_delay(options.delay_seconds.unwrap_or(self.delivery_delay))?;
        let mut entries = Vec::new();
        let mut bytes = 0;
        for message in messages {
            ensure!(
                entries.len() < cellar_logic::queue::MAX_BATCH_MESSAGES,
                "QUEUE_ERROR: a batch has at most {} messages",
                cellar_logic::queue::MAX_BATCH_MESSAGES
            );
            let entry = encode(message.body, message.delay_seconds.unwrap_or(delay))?;
            bytes += entry.body.len();
            ensure!(
                bytes <= cellar_logic::queue::MAX_SEND_BATCH_BYTES,
                "QUEUE_ERROR: a batch is at most {} bytes, got {bytes}",
                cellar_logic::queue::MAX_SEND_BATCH_BYTES
            );
            entries.push(entry);
        }
        ensure!(
            !entries.is_empty(),
            "QUEUE_ERROR: sendBatch() needs at least one message"
        );
        self.send_encoded(context, entries).await
    }
    async fn send_encoded(&self, context: &Context, entries: Vec<Encoded>) -> Result<SendResult> {
        let controls=entries.iter().map(|entry|json!({"body":null,"contentType":entry.content_type,"size":entry.body.len(),"delaySeconds":entry.delay})).collect::<Vec<_>>();
        let mut graph = StructuredValue::from_json(json!([{ "entries":controls }]))?;
        let body = wire::array(&graph, &graph.root)?[0].clone();
        let objects = wire::array(&graph, wire::field(&graph, &body, "entries"))?;
        for (object, entry) in objects.iter().zip(entries) {
            wire::set_bytes(&mut graph, object, "body", entry.body)?;
        }
        let metrics = self.call(context, "__queueSend", graph).await?;
        Ok(SendResult {
            metrics: metrics_from(&metrics)?,
        })
    }
    pub async fn metrics(&self, context: &Context) -> Result<QueueMetrics> {
        metrics_from(
            &self
                .call(
                    context,
                    "__queueMetrics",
                    StructuredValue::from_json(json!([]))?,
                )
                .await?,
        )
    }
    async fn call(&self, context: &Context, method: &str, args: StructuredValue) -> Result<Value> {
        let result = context
            .rpc_cell(
                crate::engine_api::queue_cell_scope(&self.queue),
                Some(self.queue.to_string()),
                method.into(),
                encode_rpc(&args)?,
            )
            .await?;
        decode_rpc_result(&result)?.to_json()
    }
}
fn metrics_from(value: &Value) -> Result<QueueMetrics> {
    Ok(QueueMetrics {
        backlog_count: value["backlogCount"]
            .as_f64()
            .ok_or_else(|| error("invalid Queue metrics"))?,
        backlog_bytes: value["backlogBytes"]
            .as_f64()
            .ok_or_else(|| error("invalid Queue metrics"))?,
        oldest_message_timestamp_ms: value["oldestMessageTimestamp"].as_i64(),
    })
}
fn decode(body: Vec<u8>, content_type: QueueContentType) -> Result<Body> {
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

struct Decisions {
    active: bool,
    ack_all: bool,
    retry_batch: QueueRetryBatch,
    acks: IndexSet<String>,
    retries: IndexMap<String, Option<i32>>,
}
impl Decisions {
    fn active(&self) -> Result<()> {
        ensure!(
            self.active,
            "Queue event methods cannot be called after the handler returns."
        );
        Ok(())
    }
}
pub struct Message {
    pub id: String,
    pub timestamp_ms: i64,
    pub body: Body,
    pub attempts: u16,
    decisions: Arc<Mutex<Decisions>>,
}
impl Message {
    pub fn ack(&self) -> Result<()> {
        let mut state = self.decisions.lock().unwrap();
        state.active()?;
        if !state.ack_all && !state.retry_batch.retry && !state.retries.contains_key(&self.id) {
            state.acks.insert(self.id.clone());
        }
        Ok(())
    }
    pub fn retry(&self, delay_seconds: Option<i32>) -> Result<()> {
        let mut state = self.decisions.lock().unwrap();
        state.active()?;
        if !state.ack_all
            && !state.acks.contains(&self.id)
            && (delay_seconds.is_some() || !state.retries.contains_key(&self.id))
        {
            state.retries.insert(self.id.clone(), delay_seconds);
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Event {
    inner: Arc<EventData>,
}
struct EventData {
    queue: String,
    messages: Vec<Message>,
    metrics: QueueMetrics,
    decisions: Arc<Mutex<Decisions>>,
}
impl Event {
    fn new(incoming: QueueBatch) -> Result<Self> {
        let decisions = Arc::new(Mutex::new(Decisions {
            active: true,
            ack_all: false,
            retry_batch: QueueRetryBatch {
                retry: false,
                delay_seconds: None,
            },
            acks: IndexSet::new(),
            retries: IndexMap::new(),
        }));
        let messages = incoming
            .messages
            .into_iter()
            .map(|message| {
                Ok(Message {
                    id: message.id,
                    timestamp_ms: message.timestamp_ms,
                    body: decode(message.body, message.content_type)?,
                    attempts: message.attempts,
                    decisions: decisions.clone(),
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            inner: Arc::new(EventData {
                queue: incoming.queue,
                messages,
                metrics: incoming.metrics,
                decisions,
            }),
        })
    }
    pub fn queue(&self) -> &str {
        &self.inner.queue
    }
    pub fn messages(&self) -> &[Message] {
        &self.inner.messages
    }
    pub fn metrics(&self) -> &QueueMetrics {
        &self.inner.metrics
    }
    pub fn ack_all(&self) -> Result<()> {
        let mut state = self.inner.decisions.lock().unwrap();
        state.active()?;
        if !state.retry_batch.retry {
            state.ack_all = true;
        }
        Ok(())
    }
    pub fn retry_all(&self, delay_seconds: Option<i32>) -> Result<()> {
        let mut state = self.inner.decisions.lock().unwrap();
        state.active()?;
        if !state.ack_all {
            state.retry_batch.retry = true;
            if delay_seconds.is_some() {
                state.retry_batch.delay_seconds = delay_seconds;
            }
        }
        Ok(())
    }
}
struct Lifetime(Arc<Mutex<Decisions>>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.lock().unwrap().active = false;
    }
}

/// Preserve decisions made before an error/panic and retire every retained
/// message/batch handle when the handler completes or its future is dropped.
pub fn dispatch<F, Fut>(incoming: QueueBatch, handler: F) -> HandlerFuture<Output>
where
    F: FnOnce(Event) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    Box::pin(async move {
        let event = Event::new(incoming)?;
        let lifetime = Lifetime(event.inner.decisions.clone());
        let state = event.inner.decisions.clone();
        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event)))
        {
            Ok(future) => std::panic::AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .map_err(panic_error)
                .and_then(|result| result),
            Err(panic) => Err(panic_error(panic)),
        };
        drop(lifetime);
        let state = state.lock().unwrap();
        Ok(Output::Queue(QueueDispatchResult {
            outcome: if result.is_ok() {
                QueueOutcome::Ok
            } else {
                QueueOutcome::Exception
            },
            error: result.err().map(|error| error.to_string()),
            ack_all: state.ack_all,
            retry_batch: QueueRetryBatch {
                retry: state.retry_batch.retry,
                delay_seconds: state.retry_batch.delay_seconds,
            },
            explicit_acks: state.acks.iter().cloned().collect(),
            retry_messages: state
                .retries
                .iter()
                .map(|(id, delay)| QueueRetryMessage {
                    msg_id: id.clone(),
                    delay_seconds: *delay,
                })
                .collect(),
        }))
    })
}
fn panic_error(panic: Box<dyn std::any::Any + Send>) -> anyhow::Error {
    anyhow!(
        "Queue handler panicked: {}",
        panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("unknown panic")
    )
}
