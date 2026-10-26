// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded pull at a time; cancellation drops the native response stream.
use crate::{values, Validation};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::engine_api::{HttpChunkStream, HttpResponse};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::{watch, Mutex};

pub(crate) struct Reader {
    stream: Mutex<Option<HttpChunkStream>>,
    cancelled: watch::Sender<bool>,
}

impl Reader {
    /// Return the head and bounded prefix without polling the body stream.
    pub(crate) fn open(mut response: HttpResponse, limit: usize) -> Result<(Value, Option<Self>)> {
        ensure!(
            response.websocket.is_none(),
            "response stream cannot carry a WebSocket upgrade"
        );
        ensure!(
            (200..=599).contains(&response.status),
            "invalid response status"
        );
        ensure!(
            response.body.len() <= limit,
            "response prefix exceeds body limit"
        );
        values::headers(&response.headers)?;
        let head = json!({"status":response.status,"headers":response.headers,"body":STANDARD.encode(response.body)});
        let reader = response.stream.take().map(|stream| Self {
            stream: Mutex::new(Some(stream)),
            cancelled: watch::channel(false).0,
        });
        Ok((head, reader))
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.send_replace(true);
        if let Ok(mut stream) = self.stream.try_lock() {
            stream.take();
        }
    }

    pub(crate) async fn read(&self, limit: usize) -> Result<Value> {
        // A second read must not queue an unbounded set of body waiters.
        let mut stream = self.stream.try_lock().context(Validation)?;
        let mut cancelled = self.cancelled.subscribe();
        ensure!(!*cancelled.borrow(), "response reader cancelled");
        let next = cellar::asyncrt::select_biased! { "response cancellation wins a pending read";
            _ = cancelled.changed() => anyhow::bail!("response reader cancelled"),
            next = async {
                match stream.as_mut() {
                    Some(stream) => stream.next().await.transpose().map_err(anyhow::Error::msg),
                    None => Ok(None),
                }
            } => next?,
        };
        if let Some(bytes) = &next {
            ensure!(bytes.len() <= limit, "response chunk exceeds body limit");
        }
        Ok(json!({"done":next.is_none(),"body":STANDARD.encode(next.unwrap_or_default())}))
    }
}

#[cfg(test)]
#[path = "response_tests.rs"]
mod tests;
