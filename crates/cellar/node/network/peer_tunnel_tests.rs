// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Safe pooled retry requires an actual returned, unserialized request.
use super::*;

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
            let request = Request::builder()
                .method("POST")
                .uri("/")
                .header("x-cells-request-id", "unchanged")
                .body(full_body(Bytes::from_static(b"immutable payload")))?;
            let mut failure = match send_on_tunnel("dead-test-tunnel", sender, request).await {
                Ok(_) => anyhow::bail!("dead tunnel returned a response"),
                Err(failure) => failure,
            };
            let recovered = failure
                .take_message()
                .context("dead tunnel did not return its unsent request")?;
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
