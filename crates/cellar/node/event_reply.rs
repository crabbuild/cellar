// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Peer RPC results remain cancellable and bounded after their proven head.
use super::{event_failure, remote_rpc_failure};
use crate::application::event_stream::{DeliveryError, ErrorCode, MAX_RPC_REPLY_BYTES};
use anyhow::Result;
use http_body_util::BodyExt;
use hyper::body::{Body, Bytes};
use tokio::sync::oneshot;

pub(super) async fn receive<B>(
    body: B,
    event_call: bool,
    cancel: Option<&mut oneshot::Receiver<()>>,
    deadline_ms: Option<u64>,
) -> Result<Bytes>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let collecting = async {
        if event_call {
            let collecting = bounded(body, MAX_RPC_REPLY_BYTES);
            if let Some(deadline_ms) = deadline_ms {
                let remaining_ms = deadline_ms.saturating_sub(crate::asyncrt::mono_ms());
                if remaining_ms == 0 {
                    return Err(timed_out());
                }
                crate::asyncrt::select_biased! {
                    "a captured event RPC deadline wins an undelivered body";
                    _ = crate::asyncrt::sleep(std::time::Duration::from_millis(remaining_ms)) => Err(timed_out()),
                    collected = collecting => collected,
                }
            } else {
                collecting.await
            }
        } else {
            // Ordinary values need the same dense accumulation and scheduling
            // fairness as event replies. Keep the generic forwarding ceiling
            // so existing untyped values retain their supported sizes; typed
            // message limits remain an independent validation boundary.
            accumulate(
                body,
                super::MAX_PEER_FORWARD_BODY_BYTES,
                ReplyKind::Ordinary,
            )
            .await
        }
    };
    let result = match cancel {
        Some(cancel) => crate::asyncrt::select_biased! {
            "an RPC cancellation wins an undelivered result body";
            _ = cancel => Err(anyhow::anyhow!("Durable Object RPC cancelled")),
            collected = collecting => collected,
        },
        None => collecting.await,
    };
    if event_call {
        result.map_err(event_failure::dispatched)
    } else {
        result
    }
}

fn timed_out() -> anyhow::Error {
    DeliveryError::new(
        ErrorCode::Ambiguous,
        "event RPC body deadline elapsed after submission",
    )
    .into()
}

#[derive(Clone, Copy)]
enum ReplyKind {
    Event,
    Ordinary,
}

impl ReplyKind {
    fn exhausted(self, message: &'static str) -> anyhow::Error {
        // Execution can have committed. A protocol/allocation refusal never
        // receives the transport retry tag or claims that effects rolled back.
        match self {
            Self::Event => DeliveryError::new(ErrorCode::Ambiguous, message).into(),
            Self::Ordinary => crate::application::rpc::Failure::new(
                crate::application::rpc::Code::ResourceExhausted,
            )
            .into(),
        }
    }
}

async fn bounded<B>(body: B, limit: usize) -> Result<Bytes>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    accumulate(body, limit, ReplyKind::Event).await
}

async fn accumulate<B>(body: B, limit: usize, kind: ReplyKind) -> Result<Bytes>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if body.size_hint().lower() > limit as u64 {
        return Err(kind.exhausted("event RPC result exceeds its byte budget"));
    }
    let mut body = std::pin::pin!(body);
    let mut bytes = Vec::new();
    let mut frames = 0usize;
    let mut copied = 0usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            remote_rpc_failure(anyhow::Error::new(error)).context("remote RPC body")
        })?;
        frames += 1;
        if let Ok(data) = frame.into_data() {
            let length = bytes
                .len()
                .checked_add(data.len())
                .filter(|length| *length <= limit)
                .ok_or_else(|| kind.exhausted("event RPC result exceeds its byte budget"))?;
            if length > bytes.capacity() {
                let capacity = length
                    .max(bytes.capacity().saturating_mul(2))
                    .max(4096)
                    .min(limit);
                bytes
                    .try_reserve_exact(capacity - bytes.len())
                    .map_err(|_| {
                        kind.exhausted("event RPC result buffer could not be allocated")
                    })?;
            }
            copied = copied.saturating_add(data.len());
            bytes.extend_from_slice(&data);
        }
        // Do not retain unused trailer maps or one allocation per tiny frame.
        // Yield even for empty/trailer frames so readiness cannot starve the
        // caller's cancellation or immutable deadline.
        if frames >= 64 || copied >= 64 << 10 {
            cooperate().await;
            frames = 0;
            copied = 0;
        }
    }
    Ok(Bytes::from(bytes))
}

async fn cooperate() {
    let mut yielded = false;
    futures_util::future::poll_fn(|context| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
#[path = "event_reply_tests.rs"]
mod tests;
