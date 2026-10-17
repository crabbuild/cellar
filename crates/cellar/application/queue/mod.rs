//! Native Queue producers, consumer settlement handles and the broker cell.
//! Broker policy and fleet dispatch remain the existing Rust implementations.

mod broker;
mod configuration;
pub(crate) mod message;
mod wire;

use super::{decode_rpc_result, encode_rpc, Context, HandlerFuture, Output};
use crate::engine_api::{
    QueueBatch, QueueDispatchResult, QueueOutcome, QueueRetryBatch, QueueRetryMessage,
};
use anyhow::{anyhow, ensure, Result};
use cellar_native::clone::StructuredValue;
use futures_util::FutureExt;
use indexmap::{IndexMap, IndexSet};
use message::{decode, Encoded};
pub use message::{BatchEntry, Body, QueueContentType, QueueMetrics, SendOptions};
use serde_json::{json, Value};
use std::future::Future;
use std::sync::{Arc, Mutex};

pub(crate) use broker::create_cell;
pub(crate) use configuration::{BrokerConfig as Config, ConsumerConfig as Consumer};

#[derive(Clone, Debug)]
pub struct Producer {
    queue: Arc<str>,
    delivery_delay: u32,
    json_default: bool,
}

pub struct SendResult {
    pub metrics: QueueMetrics,
}

pub(super) fn error(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("QUEUE_ERROR: {message}")
}

pub(super) fn now() -> i64 {
    crate::ownership_store::now_ms() as i64
}

pub(super) fn check_delay(delay: u32) -> Result<u32> {
    message::check_delay(delay, crate::application::limits::current().queue)
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
        let entry = message::encode(
            body,
            options.delay_seconds.unwrap_or(self.delivery_delay),
            crate::application::limits::current().queue,
        )?;
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
        let mut bytes = 0usize;
        for message in messages {
            ensure!(
                entries.len()
                    < usize::from(
                        crate::application::limits::current()
                            .queue
                            .max_batch_messages
                    ),
                "QUEUE_ERROR: a batch has at most {} messages",
                usize::from(
                    crate::application::limits::current()
                        .queue
                        .max_batch_messages
                )
            );
            let entry = message::encode(
                message.body,
                message.delay_seconds.unwrap_or(delay),
                crate::application::limits::current().queue,
            )?;
            bytes = bytes
                .checked_add(entry.body.len())
                .ok_or_else(|| error("batch size overflow"))?;
            ensure!(
                bytes
                    <= crate::application::limits::current()
                        .queue
                        .max_send_batch_bytes,
                "QUEUE_ERROR: a batch is at most {} bytes, got {bytes}",
                crate::application::limits::current()
                    .queue
                    .max_send_batch_bytes
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
