// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use anyhow::Result;
use http_body_util::Full;
use hyper::{service::service_fn, Request};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

struct OwnedBody<B, S> {
    body: B,
    _sender: S,
}

impl<B, S> Body for OwnedBody<B, S>
where
    B: Body<Data = Bytes> + Unpin,
    S: Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        Pin::new(&mut self.get_mut().body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

async fn peer_roundtrip(revoke: bool) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let lease = Lease::new();
    let released = lease.closed();
    let control = lease.control();
    let pending = Arc::new(Mutex::new(Some(lease)));
    let server = crate::asyncrt::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let service = service_fn(move |_request| {
            let lease = pending.lock().unwrap().take();
            async move {
                response(
                    RpcData::Structured(Bytes::from_static(b"value")),
                    lease.unwrap(),
                )
            }
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(socket), service)
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    let socket = tokio::net::TcpStream::connect(address).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(socket)).await?;
    let client = crate::asyncrt::spawn(connection);
    let request = Request::builder()
        .uri("/")
        .header("host", "peer")
        .body(Full::new(Bytes::new()))?;
    let reply = sender.send_request(request).await?;
    assert!(is_leased(reply.headers())?);
    // The real pooled body owns its request sender. When both the body and
    // sender disappear, the HTTP/1 connection must release the server lease.
    let result = receive(OwnedBody {
        body: reply.into_body(),
        _sender: sender,
    })
    .await
    .context("decode leased RPC value")?;
    let RpcData::Leased { value, lease } = result else {
        panic!("lease was discarded")
    };
    assert!(matches!(*value, RpcData::Structured(ref bytes) if bytes.as_ref() == b"value"));
    assert!(!lease.is_closed());
    if revoke {
        let closed = lease.closed();
        control.close();
        crate::asyncrt::timeout(Duration::from_secs(2), closed.wait()).await?;
        assert!(lease.is_closed());
    }
    drop(lease);
    crate::asyncrt::timeout(Duration::from_secs(2), released.wait()).await?;
    let client = crate::asyncrt::timeout(Duration::from_secs(2), client).await??;
    if let Err(error) = client {
        ensure!(
            !revoke && error.is_incomplete_message(),
            "unexpected client lease-close error: {error}"
        );
    }
    let server = crate::asyncrt::timeout(Duration::from_secs(2), server).await??;
    if let Err(error) = server {
        let transport = error
            .downcast_ref::<hyper::Error>()
            .context("server failure was not transport closure")?;
        ensure!(
            !revoke && transport.is_incomplete_message(),
            "unexpected server lease-close error: {error}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn caller_drop_and_server_revocation_close_actual_http1_leases() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            peer_roundtrip(false).await?;
            peer_roundtrip(true).await
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn malformed_or_oversize_metadata_refuses_without_waiting_for_a_body() -> Result<()> {
    let too_large = u32::try_from(maximum())? + 1;
    for prefix in [
        vec![9, 0, 0, 0, 0],
        [vec![1], too_large.to_be_bytes().to_vec()].concat(),
        vec![2, 0, 0, 0, 1, 42, 99],
        vec![1, 0, 0, 0, 1, 0xff],
        vec![2, 0, 0],
    ] {
        let body = Full::new(Bytes::from(prefix));
        assert!(receive(body).await.is_err());
    }
    let mut headers = hyper::HeaderMap::new();
    headers.insert(HEADER, "2".parse()?);
    assert!(is_leased(&headers).is_err());
    Ok(())
}

#[tokio::test]
async fn byte_fragmented_values_preserve_their_lifetime_and_encoding() -> Result<()> {
    use futures_util::StreamExt;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bytes = [vec![1, 0, 0, 0, 4], b"null".to_vec()].concat();
            let frames = futures_util::stream::iter(
                bytes
                    .into_iter()
                    .map(|byte| Ok::<_, Infallible>(Frame::data(Bytes::from(vec![byte])))),
            )
            .chain(futures_util::stream::pending());
            let result = receive(StreamBody::new(frames)).await?;
            let RpcData::Leased { value, lease } = result else {
                panic!("lease missing")
            };
            assert!(matches!(*value, RpcData::Json(ref value) if value == "null"));
            let closed = lease.closed();
            lease.close();
            closed.wait().await;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn domain_shutdown_revokes_a_result_retained_outside_the_domain() -> Result<()> {
    use futures_util::StreamExt;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let first = futures_util::stream::once(std::future::ready(Ok::<_, Infallible>(
                Frame::data(Bytes::from_static(b"\x01\0\0\0\x04null")),
            )))
            .chain(futures_util::stream::pending());
            receive(StreamBody::new(first)).await
        })
        .await??;
    let RpcData::Leased { lease, .. } = result else {
        panic!("missing lease")
    };
    let closed = lease.closed();
    domain.shutdown().await;
    // Shutdown joins the reader, so revocation and its notification must
    // already be complete. No timer may depend on the closed domain.
    assert!(futures_util::FutureExt::now_or_never(closed.wait()).is_some());
    assert!(lease.is_closed());
    Ok(())
}
