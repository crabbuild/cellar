// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Safe pooled retry requires an actual returned, unserialized request.
use super::*;

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
            let mut failure = match send_on_tunnel("dead-test-tunnel", sender, request).await {
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
