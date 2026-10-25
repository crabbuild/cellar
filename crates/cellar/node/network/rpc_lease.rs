// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A native RPC reply's finite value followed by its live transport lifetime.
//! The peer tunnel still owns routing/authentication/output proof. This framing
//! does not create a separate application endpoint or grant callback authority.
use super::HttpReply;
use crate::{application::rpc::Lease, engine_api::RpcData};
use anyhow::{ensure, Context as _, Result};
use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, StreamBody};
use hyper::{
    body::{Body, Frame},
    Response, StatusCode,
};

pub(super) const HEADER: &str = "x-cellar-rpc-lease";
const VERSION: &str = "1";
const PREFIX: usize = 5;

fn maximum() -> usize {
    cellar_native::clone::Limits::default().bytes
}

pub(super) fn response(value: RpcData, lease: Lease) -> Result<HttpReply> {
    ensure!(
        !lease.is_closed(),
        "RPC lease already closed before publication"
    );
    let (kind, content_type, bytes) = match value {
        RpcData::Json(value) => (1, "application/json", Bytes::from(value)),
        RpcData::Structured(value) => (2, "application/octet-stream", value),
        RpcData::Leased { .. } => anyhow::bail!("nested RPC lease"),
    };
    ensure!(
        bytes.len() <= maximum(),
        "RPC leased result exceeds byte limit"
    );
    let mut first = BytesMut::with_capacity(PREFIX + bytes.len());
    first.extend_from_slice(&[kind]);
    first.extend_from_slice(&u32::try_from(bytes.len())?.to_be_bytes());
    first.extend_from_slice(&bytes);
    let closed = Box::pin(lease.closed().wait());
    let stream = futures_util::stream::unfold(
        (Some(first.freeze()), lease, closed),
        |(first, lease, closed)| async move {
            match first {
                Some(first) => Some((
                    Ok::<_, std::io::Error>(Frame::data(first)),
                    (None, lease, closed),
                )),
                None => {
                    closed.await;
                    None
                }
            }
        },
    );
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(HEADER, VERSION)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .body(StreamBody::new(stream).boxed_unsync())?)
}

pub(super) fn is_leased(headers: &hyper::HeaderMap) -> Result<bool> {
    match headers.get(HEADER) {
        None => Ok(false),
        Some(value) => {
            ensure!(value == VERSION, "unsupported RPC lease protocol");
            Ok(true)
        }
    }
}

struct Reader(crate::asyncrt::TaskHandle<()>);

impl Drop for Reader {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn receive<B>(mut body: B) -> Result<RpcData>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let mut bytes = BytesMut::new();
    let mut expected = None;
    loop {
        let frame = body
            .frame()
            .await
            .context("truncated leased RPC result")?
            .map_err(anyhow::Error::new)?;
        let chunk = frame
            .into_data()
            .map_err(|_| anyhow::anyhow!("unexpected leased RPC trailers"))?;
        let remaining = expected.unwrap_or(PREFIX).saturating_sub(bytes.len());
        let take = remaining.min(chunk.len());
        bytes.extend_from_slice(&chunk[..take]);
        let mut extra = &chunk[take..];
        if expected.is_none() && bytes.len() == PREFIX {
            ensure!(matches!(bytes[0], 1 | 2), "invalid leased RPC value type");
            let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
            ensure!(length <= maximum(), "leased RPC value exceeds byte limit");
            expected = Some(PREFIX + length);
            let take = length.min(extra.len());
            bytes.extend_from_slice(&extra[..take]);
            extra = &extra[take..];
        }
        ensure!(extra.is_empty(), "unexpected data after leased RPC value");
        if expected == Some(bytes.len()) {
            break;
        }
    }
    let kind = bytes[0];
    let payload = bytes.split_off(PREFIX).freeze();
    let value = if kind == 1 {
        RpcData::Json(
            String::from_utf8(payload.to_vec()).context("invalid leased RPC JSON encoding")?,
        )
    } else {
        RpcData::Structured(payload)
    };
    let lease = Lease::new();
    let closed = lease.closed();
    let revocation = lease.control().revoke_on_drop();
    // This task owns only peer body I/O. It never receives an SDK context. Its
    // handle remains owned by the lease, and the node domain joins cancellation.
    let reader = crate::asyncrt::try_spawn(async move {
        // Also revoke on execution-domain shutdown or a task cancelled before
        // its first poll. An externally retained value must not appear live.
        let _revocation = revocation;
        crate::asyncrt::select_biased! { "dropping the caller lease cancels its peer-body reader";
            _ = closed.wait() => {},
            _ = async {
                // Only EOF is valid after the finite value. Any later data,
                // trailer or error revokes the transport without retrying effects.
                while let Some(frame) = body.frame().await {
                    match frame {
                        Ok(frame) if frame.data_ref().is_some_and(|bytes| bytes.is_empty()) => {},
                        _ => break,
                    }
                }
            } => {},
        }
    })
    .context("RPC lease transport domain closed")?;
    lease.hold(Reader(reader))?;
    value.with_lease(lease)
}

#[cfg(test)]
#[path = "rpc_lease_tests.rs"]
mod tests;
