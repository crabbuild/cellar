// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise capacity refusal through the real authenticated peer tunnel.
use super::*;
use std::time::Duration;

fn app(name: &str) -> anyhow::Result<(AppHandle, mpsc::UnboundedReceiver<Message>)> {
    let (tx, messages) = mpsc::unbounded_channel();
    let (websockets, _) = mpsc::unbounded_channel();
    Ok((
        AppHandle {
            tx,
            runtime: None,
            #[cfg(all(test, cellar_internal_tests))]
            scripted_activity_host: None,
            reload: crate::generation::reload_channel().0,
            peer_http: reqwest::Client::builder().no_proxy().build()?,
            peer_auth: Arc::new(PeerAuth::new([0; 32], name)?),
            advertise: "127.0.0.1:1".into(),
            websockets,
            max_outbound_websockets: 1,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fleet_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            public_in_flight: Arc::new(AtomicUsize::new(0)),
            drain_pins: Default::default(),
            follower: None,
            trust_forwarded_headers: false,
            max_request_body_bytes: 1024,
            operation_deadline_ms: 80,
        },
        messages,
    ))
}

#[tokio::test]
async fn capacity_refusals_stop_at_the_original_fetch_deadline() -> anyhow::Result<()> {
    refused(false).await
}

#[tokio::test]
async fn capacity_refusal_backoff_observes_caller_cancellation() -> anyhow::Result<()> {
    refused(true).await
}

async fn refused(cancel: bool) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (mut origin, mut routes) = app("origin")?;
            if cancel {
                origin.operation_deadline_ms = 60_000;
            }
            let (peer, mut requests) = app("peer")?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?.to_string();
            let attempts = Arc::new(AtomicUsize::new(0));
            let count = attempts.clone();
            let invalidations = Arc::new(AtomicUsize::new(0));
            let invalidation_count = invalidations.clone();
            let (canceller, cancelled) = oneshot::channel();
            let mut canceller = Some(canceller);
            let actor = crate::asyncrt::spawn(async move {
                while let Some(message) = requests.recv().await {
                    if let Message::Request {
                        reply,
                        capacity_handoff,
                        ..
                    } = message
                    {
                        assert!(capacity_handoff);
                        count.fetch_add(1, Ordering::SeqCst);
                        let _ = reply.send(Err(RequestError::CapacityExhausted));
                    }
                }
            });
            let routing = crate::asyncrt::spawn(async move {
                while let Some(message) = routes.recv().await {
                    match message {
                        Message::Request { request, reply, .. } => {
                            let _ = reply.send(Ok(Routed {
                                request,
                                route: Route::Remote {
                                    node: "peer".into(),
                                    addr: address.clone(),
                                    epoch: 0,
                                    peer_protocol: peer_auth::PROTOCOL_VERSION,
                                    capacity_sample_ms: Some(100),
                                },
                            }));
                        }
                        Message::InvalidateRemote {
                            reply,
                            capacity_sample_ms,
                            ..
                        } => {
                            assert_eq!(capacity_sample_ms, Some(100));
                            invalidation_count.fetch_add(1, Ordering::SeqCst);
                            let _ = reply.send(());
                            if cancel {
                                if let Some(sender) = canceller.take() {
                                    let _ = sender.send(());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            });
            let server = crate::asyncrt::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let app = peer.clone();
                    crate::asyncrt::spawn(async move {
                        let service = service_fn(move |request| {
                            let reply = peer_tunnel::accept(request, app.clone());
                            async move { Ok::<_, Infallible>(reply) }
                        });
                        let _ = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .with_upgrades()
                            .await;
                    })
                    .detach();
                }
            });
            let (reply, receive) = oneshot::channel();
            let body = crate::engine_api::RequestBody::Bytes(Bytes::from_static(b"mutation"));
            let call = DoCallReq {
                request_id: None,
                cancel: if cancel { Some(cancelled) } else { None },
                deliver_abort_to_handler: false,
                scope: "Order:a".into(),
                name: None,
                url: "http://test/mutation".into(),
                method: "POST".into(),
                body_guard: crate::http_streams::RequestBodyGuard::of(&body),
                body,
                headers: vec![],
                reply,
                order: None,
                parent: None,
            };
            let outcome =
                crate::asyncrt::timeout(Duration::from_secs(2), dispatch_do_call(origin, call))
                    .await;
            actor.abort();
            routing.abort();
            server.abort();
            let _ = actor.await;
            let _ = routing.await;
            let _ = server.await;
            outcome.with_context(|| {
                format!(
                    "capacity refusal ignored the original deadline/cancellation: cancel={cancel}, peer_refusals={}, invalidations={}",
                    attempts.load(Ordering::SeqCst),
                    invalidations.load(Ordering::SeqCst)
                )
            })?;
            let error = receive
                .await?
                .err()
                .context("refused request unexpectedly succeeded")?;
            assert!(
                attempts.load(Ordering::SeqCst) > 0,
                "test never reached a capacity refusal"
            );
            if cancel {
                assert!(error.to_string().contains("cancelled"), "{error:#}");
            } else {
                assert!(
                    error
                        .downcast_ref::<crate::engine_api::CellOverloaded>()
                        .is_some(),
                    "{error:#}"
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}
