// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One pull-based incoming HTTP body, owned by its original invocation.

use anyhow::{ensure, Context as _, Result};
use bytes::Bytes;
use cellar::engine_api::HttpChunkStream;
use futures_util::StreamExt;
use std::sync::{Mutex, OnceLock};
use tokio::sync::watch;

#[derive(Default)]
pub(crate) struct Reader {
    stream: Mutex<Option<Source>>,
    cancellation: OnceLock<watch::Sender<bool>>,
}

struct Source {
    stream: HttpChunkStream,
    pending: Bytes,
}

impl Reader {
    pub fn install(&self, stream: HttpChunkStream) -> Result<()> {
        ensure!(
            self.cancellation.set(watch::channel(false).0).is_ok(),
            "incoming request body already installed"
        );
        *self.stream.lock().unwrap() = Some(Source {
            stream,
            pending: Bytes::new(),
        });
        Ok(())
    }

    pub fn cancel(&self) -> Result<()> {
        self.cancellation
            .get()
            .context("no incoming request body")?
            .send_replace(true);
        let stream = self.stream.lock().unwrap().take();
        drop(stream);
        Ok(())
    }

    pub async fn read(&self, maximum: usize) -> Result<Option<Vec<u8>>> {
        ensure!(
            maximum > 0,
            "incoming request read requires a positive limit"
        );
        let mut cancellation = self
            .cancellation
            .get()
            .context("no incoming request body")?
            .subscribe();
        ensure!(!*cancellation.borrow(), "incoming request body cancelled");
        let mut source = self
            .stream
            .lock()
            .unwrap()
            .take()
            .context("incoming request body is busy or closed")?;
        let chunk = if source.pending.is_empty() {
            let chunk = cellar::asyncrt::select_biased! { "incoming body cancellation releases its source before another pull";
                _ = cancellation.changed() => anyhow::bail!("incoming request body cancelled"),
                result = source.stream.next() => result.transpose().map_err(anyhow::Error::msg)?,
            };
            chunk.map(|chunk| {
                // Move the producer's allocation without copying it. Retain
                // just that chunk and pull again only after it is consumed.
                source.pending = Bytes::from(chunk);
                let count = maximum.min(source.pending.len());
                source.pending.split_to(count).to_vec()
            })
        } else {
            let count = maximum.min(source.pending.len());
            Some(source.pending.split_to(count).to_vec())
        };
        // Serialize restoration with cancel's take. A cancelled read must not
        // put its source back after cancellation has already released the slot.
        let mut slot = self.stream.lock().unwrap();
        ensure!(!*cancellation.borrow(), "incoming request body cancelled");
        *slot = Some(source);
        Ok(chunk)
    }
}

#[cfg(test)]
#[path = "request_body_tests.rs"]
mod tests;
