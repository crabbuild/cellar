// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Safe pooled retry requires an actual returned, unserialized request.
use super::*;

#[tokio::test]
async fn receiving_rpc_tunnel_and_upload_quotas_refuse_without_blocking_cleanup(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            max_inbound_rpc_requests: 1,
            max_inbound_rpc_bytes: 128 * 1024,
            max_inbound_rpc_tunnels: 2,
            max_rpc_tunnels_per_peer: 1,
            ..Default::default()
        })?;
    let result = domain
        .run(async {
            let (mut app, mut routes) = super::super::capacity_tests::app("peer")?;
            app.operation_deadline_ms = 5_000;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?.to_string();
            let server = crate::asyncrt::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let app = app.clone();
                    crate::asyncrt::spawn(async move {
                        let service = service_fn(move |request| {
                            let reply = accept(request, app.clone());
                            async move { Ok::<_, Infallible>(reply) }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .with_upgrades()
                            .await;
                    })
                    .detach();
                }
            });
            let http = reqwest::Client::builder().no_proxy().build()?;
            let first_auth = PeerAuth::new([0; 32], "first")?;
            let second_auth = PeerAuth::new([0; 32], "second")?;
            let third_auth = PeerAuth::new([0; 32], "third")?;
            let first_peer = Peer {
                http: &http,
                auth: &first_auth,
                addr: &address,
                node: "peer",
            };
            let mut first = establish(&first_peer, "rpc").await?;
            let error = match establish(&first_peer, "rpc").await {
                Err(error) => error,
                Ok(_) => panic!("per-peer tunnel quota was bypassed"),
            };
            assert_eq!(
                error
                    .downcast_ref::<crate::application::rpc::Failure>()
                    .unwrap()
                    .code(),
                crate::application::rpc::Code::ResourceExhausted
            );
            let mut second = establish(
                &Peer {
                    http: &http,
                    auth: &second_auth,
                    addr: &address,
                    node: "peer",
                },
                "rpc",
            )
            .await?;
            assert!(establish(
                &Peer {
                    http: &http,
                    auth: &third_auth,
                    addr: &address,
                    node: "peer"
                },
                "rpc"
            )
            .await
            .is_err());
            let services = crate::asyncrt::services();
            assert_eq!(services.inbound_rpc_usage().open_tunnels, 2);
            assert_eq!(services.inbound_rpc_usage().peers, 2);
            let (release, waiting) = tokio::sync::oneshot::channel();
            let chunks = futures_util::stream::once(async { Ok::<_, String>(vec![1]) }).chain(
                futures_util::stream::once(async move {
                    let _ = waiting.await;
                    Ok(Vec::new())
                }),
            );
            let first_request = Request::builder()
                .method("POST")
                .uri("/")
                .header(SCOPE_HEADER, "Counter:upload")
                .header(METHOD_HEADER, "example.counter.v1.Counter/GetCount")
                .body(stream_body(Box::pin(chunks)))?;
            let sending = crate::asyncrt::spawn(async move {
                let reply = first.send_request(first_request).await?;
                let status = reply.status();
                reply.into_body().collect().await?;
                Ok::<_, anyhow::Error>(status)
            });
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
                while services.inbound_rpc_usage().active_requests != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            let request = Request::builder()
                .method("POST")
                .uri("/")
                .header(SCOPE_HEADER, "Counter:other")
                .header(METHOD_HEADER, "example.counter.v1.Counter/GetCount")
                .body(full_body(Bytes::new()))?;
            let reply = second.send_request(request).await?;
            assert_eq!(reply.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(
                rpc_failure::remote(reply.status(), reply.headers())
                    .unwrap()
                    .outcome(),
                crate::application::rpc::EffectOutcome::Unknown
            );
            assert_eq!(reply.headers()[hyper::header::CONNECTION], "close");
            reply.into_body().collect().await?;
            assert_eq!(services.inbound_rpc_usage().active_requests, 1);
            release.send(()).unwrap();
            assert_eq!(sending.await??, StatusCode::SERVICE_UNAVAILABLE);
            assert!(matches!(
                routes.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            drop(second);
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
                while services.inbound_rpc_usage() != Default::default() {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            server.abort();
            let _ = server.await;
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    assert_eq!(domain.services().inbound_rpc_usage(), Default::default());
    result
}

#[tokio::test]
async fn idle_receiving_rpc_tunnel_closes_and_releases_its_peer_slot() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            rpc_tunnel_idle_timeout: std::time::Duration::from_millis(200),
            ..Default::default()
        })?;
    let result = domain
        .run(async {
            let (app, _routes) = super::super::capacity_tests::app("peer")?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?.to_string();
            let server = crate::asyncrt::spawn(async move {
                let (stream, _) = listener.accept().await?;
                let service = service_fn(move |request| {
                    let reply = accept(request, app.clone());
                    async move { Ok::<_, Infallible>(reply) }
                });
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await?;
                Ok::<_, anyhow::Error>(())
            });
            let http = reqwest::Client::builder().no_proxy().build()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            let sender = establish(
                &Peer {
                    http: &http,
                    auth: &auth,
                    addr: &address,
                    node: "peer",
                },
                "rpc",
            )
            .await?;
            assert_eq!(
                crate::asyncrt::services().inbound_rpc_usage().open_tunnels,
                1
            );
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
                while !sender.is_closed()
                    || crate::asyncrt::services().inbound_rpc_usage() != Default::default()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            assert_eq!(
                crate::asyncrt::services().inbound_rpc_usage(),
                Default::default()
            );
            server.await??;
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn budgeted_rpc_upload_expires_without_body_completion_or_admission() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (app, mut routes) = super::super::capacity_tests::app("peer")?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?.to_string();
            // Keep the actor sender alive through the no-admission assertion,
            // even when an unfinished upload closes its tunnel service.
            let serving = app.clone();
            let server = crate::asyncrt::spawn(async move {
                let (stream, _) = listener.accept().await?;
                let service = service_fn(move |request| {
                    let reply = accept(request, serving.clone());
                    async move { Ok::<_, Infallible>(reply) }
                });
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await?;
                Ok::<_, anyhow::Error>(())
            });
            let http = reqwest::Client::builder().no_proxy().build()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            let peer = Peer {
                http: &http,
                auth: &auth,
                addr: &address,
                node: "peer",
            };
            let mut sender = establish(&peer, "rpc").await?;
            for (budget, expected) in [
                (Some("01"), StatusCode::BAD_REQUEST),
                (Some("0"), StatusCode::GATEWAY_TIMEOUT),
                // Without a budget this follows the ordinary no-runtime policy.
                (None, StatusCode::SERVICE_UNAVAILABLE),
            ] {
                let mut request = Request::builder()
                    .method("POST")
                    .uri("/")
                    .header(SCOPE_HEADER, "Counter:upload")
                    .header(METHOD_HEADER, "example.counter.v1.Counter/GetCount")
                    .header(hyper::header::CONTENT_TYPE, "application/octet-stream");
                if let Some(budget) = budget {
                    request = request.header(rpc_deadline::BUDGET_HEADER, budget);
                }
                let request = request.body(full_body(Bytes::new()))?;
                let reply = crate::asyncrt::timeout(
                    std::time::Duration::from_secs(2),
                    sender.send_request(request),
                )
                .await??;
                assert_eq!(reply.status(), expected);
                reply.into_body().collect().await?;
            }
            let chunks = futures_util::stream::iter([Ok(vec![0_u8])])
                .chain(futures_util::stream::pending::<Result<Vec<u8>, String>>());
            let request = Request::builder()
                .method("POST")
                .uri("/")
                .header(SCOPE_HEADER, "Counter:upload")
                .header(METHOD_HEADER, "example.counter.v1.Counter/GetCount")
                .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
                .header(rpc_deadline::BUDGET_HEADER, "50")
                .body(stream_body(Box::pin(chunks)))?;
            // The raw sender deliberately has no call timer and never finishes
            // uploading. Only the receiver's independent budget can finish this.
            let reply = crate::asyncrt::timeout(
                std::time::Duration::from_secs(2),
                sender.send_request(request),
            )
            .await??;
            assert_eq!(reply.status(), StatusCode::GATEWAY_TIMEOUT);
            assert!(rpc_deadline::remote_failure(reply.status(), reply.headers()).is_some());
            reply.into_body().collect().await?;
            assert!(matches!(
                routes.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            drop(app);
            drop(sender);
            server.await??;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn dead_tunnel_returns_exact_unsent_request_for_safe_retry() -> anyhow::Result<()> {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let result = domain
        .run(async {
            let (client, owner) = tokio::io::duplex(4096);
            let (sender, connection) = hyper::client::conn::http1::Builder::new()
                .handshake::<_, TunnelBody>(TokioIo::new(client))
                .await?;
            drop(owner);
            // Run the connection to its terminal EOF before attempting the request.
            // It cannot write any bytes to the already-closed peer.
            let _ = crate::asyncrt::timeout(std::time::Duration::from_secs(2), connection)
                .await
                .context("closed tunnel driver did not finish")?;
            let mut request = Request::builder()
                .method("POST")
                .uri("/")
                .header("x-cells-request-id", "unchanged")
                .body(full_body(Bytes::from_static(b"immutable payload")))?;
            let deadline = crate::asyncrt::mono_ms() + 2_000;
            rpc_deadline::write(&mut request, Some(deadline))?;
            let original_budget: u64 = request.headers()[rpc_deadline::BUDGET_HEADER]
                .to_str()?
                .parse()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            let key = TunnelKey {
                address: "dead-test-tunnel".into(),
                kind: "rpc",
                target: "owner".into(),
                scope: auth.tunnel_scope(),
            };
            let mut failure = match send_on_tunnel(&key, sender, request).await {
                Ok(_) => anyhow::bail!("dead tunnel returned a response"),
                Err(failure) => failure,
            };
            let mut recovered = failure
                .take_message()
                .context("dead tunnel did not return its unsent request")?;
            crate::asyncrt::sleep(std::time::Duration::from_millis(20)).await;
            rpc_deadline::write(&mut recovered, Some(deadline))?;
            let retried_budget: u64 = recovered.headers()[rpc_deadline::BUDGET_HEADER]
                .to_str()?
                .parse()?;
            assert!(
                retried_budget < original_budget,
                "unsent retry renewed its budget"
            );
            assert_eq!(recovered.headers()["x-cells-request-id"], "unchanged");
            assert_eq!(recovered.method(), hyper::Method::POST);
            assert_eq!(
                recovered.into_body().collect().await?.to_bytes(),
                Bytes::from_static(b"immutable payload")
            );
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn idle_tunnel_churn_evicts_old_peers_at_the_global_bound() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let mut pool = IdleTunnels::default();
            let prefix = format!(
                "idle-bound-{}-",
                crate::engine_api::request_id_string(crate::engine_api::next_request_id())
            );
            let auth = PeerAuth::new([0; 32], "origin")?;
            let key = |index| TunnelKey {
                address: format!("{prefix}{index}"),
                kind: "rpc",
                target: "owner".into(),
                scope: auth.tunnel_scope(),
            };
            let mut owners = Vec::new();
            let mut drivers = Vec::new();
            for index in 0..=TUNNEL_POOL_CAP {
                let (client, owner) = tokio::io::duplex(4096);
                let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
                    .handshake::<_, TunnelBody>(TokioIo::new(client))
                    .await?;
                drivers.push(crate::asyncrt::spawn(async move {
                    let _ = connection.await;
                }));
                sender.ready().await?;
                owners.push(owner);
                pool.park(&key(index), sender);
            }
            // Use the production checkout path, not container-size assertions.
            // Old peers must yield their idle slot to the newly observed peer.
            let oldest = pool.checkout(&key(0));
            let newest = pool.checkout(&key(TUNNEL_POOL_CAP));
            let evicted = oldest.is_none();
            let retained = newest.is_some();
            drop(oldest);
            drop(newest);
            drop(pool);
            drop(owners);
            for driver in drivers {
                driver.await?;
            }
            anyhow::ensure!(
                evicted,
                "idle pool retained more than {TUNNEL_POOL_CAP} distinct live peer tunnels"
            );
            anyhow::ensure!(retained, "idle pool discarded the newest ready peer");
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn submitted_rpc_reply_loss_cannot_enter_the_unsent_retry_path() -> anyhow::Result<()> {
    use futures_util::FutureExt as _;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let fallback = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = fallback.local_addr()?.to_string();
            let (client, owner) = tokio::io::duplex(4096);
            let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
                .handshake::<_, TunnelBody>(TokioIo::new(client))
                .await?;
            let driver = crate::asyncrt::spawn(async move {
                let _ = connection.await;
            });
            let (observed, received) = tokio::sync::oneshot::channel();
            let observed = Arc::new(std::sync::Mutex::new(Some(observed)));
            let server = crate::asyncrt::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let observed = observed.clone();
                    async move {
                        let identity = request.headers()[REQUEST_ID_HEADER].clone();
                        let method = request.headers()[METHOD_HEADER].clone();
                        let bytes = request
                            .into_body()
                            .collect()
                            .await
                            .map_err(std::io::Error::other)?
                            .to_bytes();
                        // The complete request has crossed the transport boundary.
                        // Lose the reply before even its headers reach the caller.
                        observed
                            .lock()
                            .unwrap()
                            .take()
                            .unwrap()
                            .send((identity, method, bytes))
                            .unwrap();
                        Err::<HttpReply, _>(std::io::Error::other("intentional owner reply loss"))
                    }
                });
                assert!(hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(owner), service)
                    .await
                    .is_err());
            });
            sender.ready().await?;
            let http = reqwest::Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(1))
                .build()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            let peer = Peer {
                http: &http,
                auth: &auth,
                addr: &address,
                node: "owner",
            };
            let key = tunnel_key(&peer, "rpc");
            park(&key, sender);
            let identity = crate::engine_api::next_request_id();
            let method = "example.counter.v1.Counter/Increment";
            let payload = Bytes::from_static(b"one submitted request");
            let outcome = crate::asyncrt::timeout(
                std::time::Duration::from_secs(2),
                rpc(
                    &peer,
                    "Counter:lost-reply",
                    Some("lost-reply"),
                    method,
                    &crate::engine_api::RpcData::Structured(payload.clone()),
                    RpcControl {
                        request_id: Some(identity),
                        authority: None,
                        deadline_ms: Some(crate::asyncrt::mono_ms() + 1000),
                    },
                ),
            )
            .await
            .context("lost reply incorrectly waited for a replay tunnel")?;
            let failure = outcome.err().context("lost reply unexpectedly succeeded")?;
            let (actual_id, actual_method, actual_body) = received.await?;
            assert_eq!(actual_id, crate::engine_api::request_id_string(identity));
            assert_eq!(actual_method, method);
            assert_eq!(actual_body, payload);
            assert_eq!(
                classify_remote_attempt(&failure),
                cellar_logic::routing::Attempt::Ambiguous
            );
            let mut routing = RemoteRouteRetry::new(1000);
            assert_eq!(
                routing.failed(classify_remote_attempt(&failure)),
                RemoteRetryAction::Stop
            );
            assert!(
                fallback.accept().now_or_never().is_none(),
                "submitted RPC opened a replay tunnel"
            );
            assert!(
                checkout(&key).is_none(),
                "failed reply returned a poisoned tunnel to the pool"
            );
            server.await?;
            driver.await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn pooled_rpc_requires_its_original_credential_scope() -> anyhow::Result<()> {
    changed_peer_cannot_reuse_authenticated_tunnel(false).await
}

#[tokio::test]
async fn pooled_rpc_does_not_reuse_a_different_target_session() -> anyhow::Result<()> {
    changed_peer_cannot_reuse_authenticated_tunnel(true).await
}

async fn changed_peer_cannot_reuse_authenticated_tunnel(change_target: bool) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (app, mut routes) = super::super::capacity_tests::app("peer")?;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?.to_string();
            let handshakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let seen = handshakes.clone();
            let serving = app.clone();
            let (stop, mut stopped) = tokio::sync::oneshot::channel();
            let server = crate::asyncrt::spawn(async move {
                let mut connections = Vec::new();
                while let Some(stream) = accept_until_stopped(&listener, &mut stopped).await? {
                    let app = serving.clone();
                    let seen = seen.clone();
                    connections.push(crate::asyncrt::spawn(async move {
                        let service = service_fn(move |request| {
                            seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let reply = accept(request, app.clone());
                            async move { Ok::<_, Infallible>(reply) }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .with_upgrades()
                            .await;
                    }));
                }
                for connection in connections {
                    connection.await?;
                }
                Ok::<_, anyhow::Error>(())
            });
            let http = reqwest::Client::builder()
                .no_proxy()
                .timeout(std::time::Duration::from_secs(2))
                .build()?;
            let original_auth = PeerAuth::new([0; 32], "origin")?;
            let other_auth = PeerAuth::new([1; 32], "origin")?;
            let original = Peer {
                http: &http,
                auth: &original_auth,
                addr: &address,
                node: "peer",
            };
            let changed = Peer {
                http: &http,
                auth: if change_target {
                    &original_auth
                } else {
                    &other_auth
                },
                addr: &address,
                node: if change_target {
                    "another-session"
                } else {
                    "peer"
                },
            };
            let args = crate::engine_api::RpcData::Structured(Bytes::new());
            let call = |peer| {
                rpc(
                    peer,
                    "Counter:auth-probe",
                    None,
                    "example.counter.v1.Counter/GetCount",
                    &args,
                    RpcControl {
                        request_id: None,
                        authority: None,
                        deadline_ms: None,
                    },
                )
            };
            let warm = call(&original).await?;
            assert_eq!(warm.status(), StatusCode::SERVICE_UNAVAILABLE);
            // A clean EOF is necessary to return the authenticated sender to cache.
            warm.into_body().collect().await?;
            let cached = checkout(&tunnel_key(&original, "rpc"));
            anyhow::ensure!(
                cached.is_some(),
                "authentication fixture did not cache its warm tunnel"
            );
            park(&tunnel_key(&original, "rpc"), cached.unwrap());
            let result = call(&changed).await;
            let refused = result.is_err();
            let stale = result
                .as_ref()
                .err()
                .is_some_and(|error| error.downcast_ref::<StaleTunnelRoute>().is_some());
            let unauthorized = result.as_ref().err().is_some_and(|error| {
                error.to_string()
                    == format!("tunnel establishment answered {}", StatusCode::UNAUTHORIZED)
            });
            if let Ok(response) = result {
                response.into_body().collect().await?;
            }
            // Close this fixture's cached inner connections before joining its domain.
            while let Some(sender) = checkout(&tunnel_key(&original, "rpc")) {
                drop(sender);
            }
            while let Some(sender) = checkout(&tunnel_key(&changed, "rpc")) {
                drop(sender);
            }
            let attempted = handshakes.load(std::sync::atomic::Ordering::SeqCst);
            assert!(matches!(
                routes.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            drop(http);
            let _ = stop.send(());
            server.await??;
            anyhow::ensure!(
                refused,
                "changed peer credentials/session reused an authenticated tunnel"
            );
            anyhow::ensure!(
                attempted == 2,
                "changed peer skipped its own authentication handshake"
            );
            anyhow::ensure!(
                !change_target || stale,
                "wrong target lost trusted stale-route classification"
            );
            anyhow::ensure!(
                change_target || unauthorized,
                "changed credentials did not reach the peer's authentication refusal"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

async fn accept_until_stopped(
    listener: &tokio::net::TcpListener,
    stopped: &mut tokio::sync::oneshot::Receiver<()>,
) -> std::io::Result<Option<tokio::net::TcpStream>> {
    crate::asyncrt::select_biased! { "test server stops before accepting more tunnels";
        _ = stopped => Ok(None),
        accepted = listener.accept() => accepted.map(|(stream, _)| Some(stream)),
    }
}
