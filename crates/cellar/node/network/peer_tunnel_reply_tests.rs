// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reply buffer ownership and bounded transport delivery.
use super::*;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[tokio::test]
async fn rpc_reply_data_handoff_retains_admission_until_its_last_alias_drops() -> anyhow::Result<()>
{
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            max_inbound_rpc_requests: 1,
            ..Default::default()
        })?;
    let alias = domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let reservation = connection.request().unwrap();
            let payload = Bytes::from(vec![b'x'; 8 * 1024]);
            let payload_pointer = payload.as_ptr();
            let mut body = RetainedRpcBody::new(full_body(payload), reservation, None).unwrap();
            drop(connection);
            let held = services.inbound_rpc_usage();
            let data = body.frame().await.unwrap()?.into_data().unwrap();
            assert_eq!(
                data.as_ptr(),
                payload_pointer,
                "reply handoff copied its payload"
            );
            let alias = data.slice(1..data.len() - 1);
            drop((body, data));
            assert_eq!(services.inbound_rpc_usage(), held,
            "HTTP body retirement released admission while a transport frame still owns its bytes");
            let second = services.admit_rpc_tunnel("second").unwrap();
            assert!(
                second.request().is_none(),
                "retained reply bytes lost their request slot"
            );
            drop(second);
            Ok::<_, anyhow::Error>(alias)
        })
        .await??;
    domain.shutdown().await;
    assert_eq!(domain.services().inbound_rpc_usage().active_requests, 1);
    drop(alias);
    assert_eq!(domain.services().inbound_rpc_usage(), Default::default());
    Ok(())
}

async fn reply_write_deadline(caller_budget: bool) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            rpc_tunnel_idle_timeout: if caller_budget {
                std::time::Duration::from_secs(1)
            } else {
                std::time::Duration::from_millis(30)
            },
            ..Default::default()
        })?;
    let result = domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let mut expiry = Box::pin(connection.idle());
            std::future::poll_fn(|cx| {
                assert!(expiry.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let deadline = caller_budget.then(|| crate::asyncrt::mono_ms() + 10);
            let mut body = RetainedRpcBody::new(
                full_body(Bytes::from_static(b"pending reply")),
                request,
                deadline,
            )
            .unwrap();
            let data = body.frame().await.unwrap()?.into_data().unwrap();
            drop(body);
            let expired =
                crate::asyncrt::timeout(std::time::Duration::from_millis(200), expiry).await;
            assert_eq!(services.inbound_rpc_usage().active_requests, 1);
            drop((data, connection));
            assert_eq!(services.inbound_rpc_usage(), Default::default());
            anyhow::ensure!(
                expired.is_ok(),
                "retained reply bytes disabled the receiving write deadline"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn rpc_reply_write_deadline_preserves_the_original_caller_budget() -> anyhow::Result<()> {
    reply_write_deadline(true).await
}

#[tokio::test]
async fn rpc_reply_write_deadline_bounds_unbudgeted_transport() -> anyhow::Result<()> {
    reply_write_deadline(false).await
}

#[tokio::test]
async fn rpc_expired_reply_is_refused_before_yielding_business_bytes() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let result = RetainedRpcBody::new(
                full_body(Bytes::from_static(b"late result")),
                request,
                Some(crate::asyncrt::mono_ms()),
            );
            assert!(matches!(
                result,
                Err(crate::application::rpc::Code::DeadlineExceeded)
            ));
            drop(connection);
            assert_eq!(services.inbound_rpc_usage(), Default::default());
        })
        .await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn rpc_delivered_bytes_remove_the_write_deadline_before_native_retirement(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            rpc_tunnel_idle_timeout: std::time::Duration::from_secs(1),
            ..Default::default()
        })?;
    let result = domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let native = request.ticket();
            let mut body = RetainedRpcBody::new(
                full_body(Bytes::from_static(b"delivered")),
                request,
                Some(crate::asyncrt::mono_ms() + 10),
            )
            .unwrap();
            let data = body.frame().await.unwrap()?.into_data().unwrap();
            drop((data, body));
            let expired =
                crate::asyncrt::timeout(std::time::Duration::from_millis(30), connection.idle())
                    .await;
            assert_eq!(services.inbound_rpc_usage().active_requests, 1);
            drop((native, connection));
            assert_eq!(services.inbound_rpc_usage(), Default::default());
            anyhow::ensure!(
                expired.is_err(),
                "a delivered reply left its deadline attached to native cleanup"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

/// Accept the response head and part of its first frame, then leave the actual
/// socket write pending. The independent connection timer must still run.
struct StalledWriter {
    stream: tokio::net::TcpStream,
    remaining: usize,
    blocked: Arc<tokio::sync::Notify>,
}

impl AsyncRead for StalledWriter {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for StalledWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.remaining == 0 {
            self.blocked.notify_one();
            return std::task::Poll::Pending;
        }
        let allowed = bytes.len().min(self.remaining);
        let result = std::pin::Pin::new(&mut self.stream).poll_write(cx, &bytes[..allowed]);
        if let std::task::Poll::Ready(Ok(written)) = result {
            self.remaining -= written;
        }
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

struct ObservedBody {
    body: RetainedRpcBody,
    dropped: Arc<AtomicBool>,
}

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

impl Drop for ObservedBody {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn rpc_stalled_socket_expires_after_hyper_drops_its_body_and_retains_the_frame(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            max_inbound_rpc_requests: 1,
            rpc_tunnel_idle_timeout: std::time::Duration::from_secs(1),
            ..Default::default()
        })?;
    let result = domain.run(async {
        let services = crate::asyncrt::services();
        let client = reqwest::Client::builder().no_proxy().build()?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let blocked = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let block = blocked.clone();
        let observation = dropped.clone();
        let server = crate::asyncrt::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let connection = crate::asyncrt::services().admit_rpc_tunnel("peer").unwrap();
            let idle = connection.clone();
            let service = service_fn(move |_: Request<Incoming>| {
                let reservation = connection.request().unwrap();
                let body = RetainedRpcBody::new(full_body(Bytes::from(vec![b'x'; 8192])), reservation,
                    Some(crate::asyncrt::mono_ms() + 200)).unwrap();
                let response = Response::new(ObservedBody { body, dropped: observation.clone() });
                async move { Ok::<_, Infallible>(response) }
            });
            let io = StalledWriter { stream, remaining: 256, blocked: block };
            let serving = rpc_server_builder().serve_connection(TokioIo::new(io), service);
            let expired = crate::asyncrt::select_biased! { "a stalled write cannot suppress its independent RPC deadline";
                _ = idle.idle() => true,
                _ = serving => false,
            };
            anyhow::ensure!(expired, "stalled socket completed before the reply deadline");
            Ok::<_, anyhow::Error>(())
        });
        let result = async {
            let reply = client.get(format!("http://{address}/")).send().await?;
            crate::asyncrt::timeout(std::time::Duration::from_secs(1), blocked.notified()).await?;
            anyhow::ensure!(dropped.load(Ordering::SeqCst), "Hyper did not hand its body to a transport frame");
            anyhow::ensure!(services.inbound_rpc_usage().active_requests == 1,
                "pending socket bytes lost their request reservation");
            let body = crate::asyncrt::timeout(std::time::Duration::from_secs(1), reply.bytes()).await?;
            anyhow::ensure!(body.is_err(), "a timed-out partial RPC reply became clean EOF");
            Ok::<_, anyhow::Error>(())
        }.await;
        if result.is_err() { server.abort(); }
        let joined = server.await;
        result?;
        joined??;
        anyhow::ensure!(services.inbound_rpc_usage() == Default::default(), "stalled socket retained admission after join");
        Ok::<_, anyhow::Error>(())
    }).await?;
    domain.shutdown().await;
    result
}
