// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reuse must leave an inactive peer enough time to observe the next request.
use super::*;
use futures_util::task::AtomicWaker;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
struct ReadGate {
    blocked: AtomicBool,
    observed: AtomicBool,
    waiter: AtomicWaker,
}

impl ReadGate {
    fn release(&self) {
        self.blocked.store(false, Ordering::SeqCst);
        self.waiter.wake();
    }
}

/// Hold the old connection's next request before Hyper sees its headers. New
/// connections bypass this gate. This models the SDK fault proxy retaining an
/// already submitted request while the receiver's independent idle timer runs.
struct GatedRead {
    socket: tokio::net::TcpStream,
    gate: Option<Arc<ReadGate>>,
}

impl AsyncRead for GatedRead {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(gate) = &self.gate {
            if gate.blocked.load(Ordering::SeqCst) {
                gate.waiter.register(cx.waker());
                if gate.blocked.load(Ordering::SeqCst) {
                    gate.observed.store(true, Ordering::SeqCst);
                    return std::task::Poll::Pending;
                }
            }
        }
        std::pin::Pin::new(&mut self.socket).poll_read(cx, bytes)
    }
}

impl AsyncWrite for GatedRead {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.socket).poll_write(cx, bytes)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn rpc_reuse_retires_an_old_tunnel_before_delayed_submission_crosses_idle_expiry(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            rpc_tunnel_idle_timeout: std::time::Duration::from_millis(800),
            ..Default::default()
        })?;
    let result = domain
        .run(async {
            let (mut app, mut routes) = super::super::capacity_tests::app("peer")?;
            app.operation_deadline_ms = 5_000;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?.to_string();
            let connections = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(ReadGate::default());
            let control = gate.clone();
            let observed = connections.clone();
            let serving = app.clone();
            let server = crate::asyncrt::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let first = observed.fetch_add(1, Ordering::SeqCst) == 0;
                    let socket = GatedRead {
                        socket,
                        gate: first.then(|| control.clone()),
                    };
                    let serving = serving.clone();
                    crate::asyncrt::spawn(async move {
                        let service = service_fn(move |request| {
                            let reply = accept(request, serving.clone());
                            async move { Ok::<_, Infallible>(reply) }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(socket), service)
                            .with_upgrades()
                            .await;
                    })
                    .detach();
                }
            });
            let http = reqwest::Client::builder().no_proxy().build()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            let peer = Peer {
                http: &http,
                auth: &auth,
                addr: &address,
                node: "peer",
            };
            let key = tunnel_key(&peer, "rpc");
            let args = crate::engine_api::RpcData::Structured(Bytes::new());
            let call = || {
                rpc(
                    &peer,
                    "Counter:reuse-probe",
                    None,
                    "example.counter.v1.Counter/GetCount",
                    &args,
                    RpcControl {
                        reservation: None,
                        request_id: None,
                        authority: None,
                        deadline_ms: None,
                    },
                )
            };
            let result = async {
                let warm = call()
                    .await
                    .context("warm RPC failed before the delayed-submission cut")?;
                assert_eq!(warm.status(), StatusCode::SERVICE_UNAVAILABLE);
                warm.into_body().collect().await?;
                assert_eq!(connections.load(Ordering::SeqCst), 1);
                // Cross the advertised half-window, but prove the receiver is still
                // alive. The failure must come from submitting on the old socket,
                // rather than checking out an already closed connection.
                crate::asyncrt::sleep(std::time::Duration::from_millis(500)).await;
                assert_eq!(
                    crate::asyncrt::services().inbound_rpc_usage().open_tunnels,
                    1,
                    "fixture lost its receiving tunnel before the submission cut"
                );
                gate.blocked.store(true, Ordering::SeqCst);
                let reply =
                    crate::asyncrt::timeout(std::time::Duration::from_secs(2), call()).await;
                gate.release();
                let reply = reply
                    .context("delayed RPC exceeded its observation deadline")?
                    .with_context(|| {
                        format!(
                            "delayed RPC failed: old-socket read held={}, connections={}",
                            gate.observed.load(Ordering::SeqCst),
                            connections.load(Ordering::SeqCst)
                        )
                    })?;
                assert_eq!(reply.status(), StatusCode::SERVICE_UNAVAILABLE);
                // The active exchange still owns its result. Let its cache window
                // expire before consuming EOF: parking must not renew eligibility,
                // discard the reply, or cancel an already submitted operation.
                crate::asyncrt::sleep(std::time::Duration::from_millis(500)).await;
                assert_eq!(
                    crate::asyncrt::services().inbound_rpc_usage().open_tunnels,
                    1,
                    "fixture lost its new receiving tunnel before reply retirement"
                );
                reply.into_body().collect().await?;
                assert_eq!(
                    connections.load(Ordering::SeqCst),
                    2,
                    "an aging tunnel was reused instead of retiring before submission"
                );
                assert!(
                    checkout(&key).is_none(),
                    "late reply retirement renewed cache eligibility"
                );
                assert!(matches!(
                    routes.try_recv(),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                ));
                Ok::<_, anyhow::Error>(())
            }
            .await;
            gate.release();
            drop(checkout(&key));
            server.abort();
            let _ = server.await;
            result
        })
        .await;
    domain.shutdown().await;
    assert_eq!(domain.services().inbound_rpc_usage(), Default::default());
    result?
}

#[test]
fn rpc_idle_metadata_has_exact_canonical_integer_bounds() -> anyhow::Result<()> {
    for value in [1u32, 30_000, u32::MAX] {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(RPC_IDLE_HEADER, hyper::header::HeaderValue::from(value));
        assert_eq!(rpc_idle_interval(&headers)?, value);
    }
    for value in ["", "0", "00", "01", "+1", "-1", "1.0", "4294967296", "٣٠"] {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            RPC_IDLE_HEADER,
            hyper::header::HeaderValue::from_bytes(value.as_bytes())?,
        );
        assert!(
            rpc_idle_interval(&headers).is_err(),
            "accepted invalid idle metadata"
        );
    }
    Ok(())
}

#[tokio::test]
async fn rpc_upgrade_refuses_missing_repeated_and_invalid_idle_metadata() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (app, mut routes) = super::super::capacity_tests::app("peer")?;
            let cases = [
                vec![],
                vec!["0"],
                vec!["01"],
                vec!["+1"],
                vec!["4294967296"],
                vec!["10", "20"],
            ];
            let http = reqwest::Client::builder().no_proxy().build()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            for values in cases {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?.to_string();
                let serving = app.clone();
                let server = crate::asyncrt::spawn(async move {
                    let (socket, _) = listener.accept().await?;
                    let service = service_fn(move |request| {
                        let mut reply = accept(request, serving.clone());
                        reply.headers_mut().remove(RPC_IDLE_HEADER);
                        for value in &values {
                            reply.headers_mut().append(
                                RPC_IDLE_HEADER,
                                hyper::header::HeaderValue::from_static(value),
                            );
                        }
                        async move { Ok::<_, Infallible>(reply) }
                    });
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(socket), service)
                        .with_upgrades()
                        .await?;
                    Ok::<_, anyhow::Error>(())
                });
                let peer = Peer {
                    http: &http,
                    auth: &auth,
                    addr: &address,
                    node: "peer",
                };
                // A malformed/mixed peer cannot mint a sender for a business call.
                let sender = establish(&peer, "rpc").await;
                assert!(
                    sender.is_err(),
                    "invalid upgrade metadata produced an RPC sender"
                );
                assert!(checkout(&tunnel_key(&peer, "rpc")).is_none());
                server.await??;
            }
            assert!(matches!(
                routes.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    assert_eq!(domain.services().inbound_rpc_usage(), Default::default());
    result?
}

#[tokio::test]
async fn rpc_reuse_rejects_a_stopped_owner_even_before_its_io_observes_shutdown(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let (sender, connection, owner) = domain
        .run(async {
            let (client, owner) = tokio::io::duplex(4096);
            let (inner, connection) = hyper::client::conn::http1::Builder::new()
                .handshake::<_, TunnelBody>(TokioIo::new(client))
                .await?;
            let domain = crate::asyncrt::services().domain_token().unwrap();
            let expires_ms = domain.mono_ms()? + 10_000;
            let sender = TunnelSender {
                inner,
                reuse: Some(ReuseWindow { domain, expires_ms }),
            };
            assert!(sender.reusable());
            Ok::<_, anyhow::Error>((sender, connection, owner))
        })
        .await??;
    domain.shutdown().await;
    // Keep the raw I/O future open and unpolled to distinguish the owner clock
    // from Hyper's eventual channel closure. No ambient domain is installed.
    assert!(!sender.inner.is_closed());
    assert!(!sender.reusable());
    drop((sender, connection, owner));
    Ok(())
}

#[tokio::test]
async fn rpc_upgrade_requires_one_compatible_reuse_capability_before_admission(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (app, mut routes) = super::super::capacity_tests::app("peer")?;
            let http = reqwest::Client::builder().no_proxy().build()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            for values in [vec![], vec!["0"], vec!["2"], vec!["1", "1"]] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let serving = app.clone();
                let server = crate::asyncrt::spawn(async move {
                    let (socket, _) = listener.accept().await?;
                    let service = service_fn(move |request| {
                        let reply = accept(request, serving.clone());
                        async move { Ok::<_, Infallible>(reply) }
                    });
                    hyper::server::conn::http1::Builder::new()
                        .keep_alive(false)
                        .serve_connection(TokioIo::new(socket), service)
                        .with_upgrades()
                        .await?;
                    Ok::<_, anyhow::Error>(())
                });
                let mut request = http
                    .get(format!("http://{address}/peer/tunnel"))
                    .header(hyper::header::UPGRADE, TUNNEL_PROTOCOL)
                    .header(hyper::header::CONNECTION, "upgrade")
                    .header(VERSION_HEADER, TUNNEL_VERSION)
                    .header(KIND_HEADER, "rpc")
                    .header(
                        peer_auth::RESPONSE_VERSION_HEADER,
                        peer_auth::PROTOCOL_VERSION_TEXT,
                    );
                for value in values {
                    request = request.header(RPC_REUSE_HEADER, value);
                }
                let reply = auth
                    .sign(request, "GET", "/peer/tunnel", b"", "peer")?
                    .send()
                    .await?;
                assert_eq!(reply.status(), StatusCode::UPGRADE_REQUIRED);
                reply.bytes().await?;
                assert_eq!(
                    crate::asyncrt::services().inbound_rpc_usage(),
                    Default::default(),
                    "an incompatible peer consumed receiving RPC admission"
                );
                server.await??;
            }
            assert!(matches!(
                routes.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}
