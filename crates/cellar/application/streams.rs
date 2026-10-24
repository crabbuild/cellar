// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rust byte pipes connected to cellar's HTTP ownership and event driver.
use super::{
    http::{Headers, Response},
    Context, Request,
};
use crate::engine_api::{HttpChunkStream, RequestBody};
use anyhow::Result;
pub use cellar_native::streams::{
    ByteRead, ByteReadFuture, ByteReadable, ByteReader, ByteView, ByteWritable, ByteWriter,
    FixedLengthStream, IdentityTransformStream, StreamWait,
};
use cellar_native::web::AbortReason;
use futures_util::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

struct HttpBytes {
    reader: ByteReader,
    pending: Option<ByteReadFuture>,
    finished: bool,
}

impl Stream for HttpBytes {
    type Item = std::result::Result<Vec<u8>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if self.pending.is_none() {
            self.pending = Some(self.reader.read());
        }
        match Pin::new(self.pending.as_mut().unwrap()).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.pending = None;
                match result {
                    Ok(read) if read.done => {
                        self.finished = true;
                        Poll::Ready(None)
                    }
                    Ok(read) => Poll::Ready(Some(Ok(read
                        .value
                        .expect("an internal non-final read contains bytes")
                        .into_bytes()))),
                    Err(reason) => {
                        self.finished = true;
                        Poll::Ready(Some(Err(reason.to_string())))
                    }
                }
            }
        }
    }
}

impl Drop for HttpBytes {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self
                .reader
                .cancel(Some(AbortReason::error("The HTTP body was dropped")));
        }
    }
}

/// Claim the shared readable once. The transport consumes it after a handler
/// returns; it needs no cell authority. Dropping transport cancels the producer.
pub fn http_byte_stream(readable: &ByteReadable) -> Result<HttpChunkStream> {
    Ok(Box::pin(HttpBytes {
        reader: readable.get_reader()?,
        pending: None,
        finished: false,
    }))
}

struct ProducerGuard {
    readable: ByteReadable,
    finished: bool,
}

impl ProducerGuard {
    fn finish(mut self, result: Result<()>) -> Result<()> {
        let result = match result {
            Ok(()) if !self.readable.is_closed() => Err(anyhow::anyhow!(
                "The byte stream producer finished without closing its stream"
            )),
            result => result,
        };
        if let Err(error) = &result {
            let reason = error
                .downcast_ref::<AbortReason>()
                .cloned()
                .unwrap_or_else(|| AbortReason::error(&error.to_string()));
            self.readable.error(reason);
        }
        self.finished = true;
        result
    }
}

impl Drop for ProducerGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.readable
                .error(AbortReason::error("The byte stream producer was dropped"));
        }
    }
}

impl Context {
    /// Publish a response chunk only after proving the writes it can reveal.
    /// Use this in an owned producer registered with `wait_until_byte_stream`;
    /// keeping a producer alive alone does not prove its post-response writes.
    pub async fn write_stream_chunk(&self, writer: &ByteWriter, bytes: Vec<u8>) -> Result<()> {
        self.check_turn()?;
        let gate =
            crate::host_gates::egress_gate_request(&self.io, cellar_logic::Channel::Response);
        crate::host_gates::await_egress_gate(gate)
            .await
            .map_err(anyhow::Error::msg)?;
        self.check_turn()?;
        writer.write(bytes).await?;
        self.check_turn()?;
        Ok(())
    }

    /// Retain producer work in this invocation. Retirement and shutdown drop
    /// it and fail both stream endpoints, including an unwakeable producer.
    pub fn wait_until_byte_stream(
        &self,
        readable: &ByteReadable,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) -> Result<()> {
        self.check_turn()?;
        let guard = ProducerGuard {
            readable: readable.clone(),
            finished: false,
        };
        self.wait_until(async move {
            let result = future.await;
            guard.finish(result)
        })
    }
}

impl Response {
    pub fn from_byte_stream(
        status: u16,
        readable: &ByteReadable,
        headers: &Headers,
    ) -> Result<Self> {
        Ok(Self::streaming(
            status,
            http_byte_stream(readable)?,
            headers.response_wire(),
        ))
    }
}

impl Request {
    /// Routed stream requests retain the host body claim and advertise known
    /// lengths (or chunked transfer) using the existing service-call policy.
    pub fn with_byte_stream(
        context: &Context,
        url: &str,
        method: &str,
        headers: &Headers,
        readable: &ByteReadable,
    ) -> Result<Self> {
        context.check_turn()?;
        let method = super::http::normalize_method(method)?;
        let headers = Headers::from_headers(headers);
        if !headers.has("content-length")? && !headers.has("transfer-encoding")? {
            if let Some(length) = readable.expected_length() {
                headers.set("content-length", &length.to_string())?;
            } else {
                headers.set("transfer-encoding", "chunked")?;
            }
        }
        let wire = headers.request_wire()?;
        let stream = http_byte_stream(readable)?;
        let id = crate::http_streams::register_body_stream(stream).map_err(anyhow::Error::msg)?;
        context.io.own_body_stream(id);
        Ok(Self {
            url: url.into(),
            method,
            headers: wire,
            body: RequestBody::Stream(id),
            entrypoint: None,
        })
    }
}
