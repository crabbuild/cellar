// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A peer with an incomplete upload cannot retain the primitive entrance forever.
use super::*;
use std::{
    pin::Pin,
    sync::atomic::AtomicBool,
    task::{Context, Poll},
};

struct IncompleteBody {
    first: Option<bytes::Bytes>,
    dropped: Arc<AtomicBool>,
}

impl futures_util::Stream for IncompleteBody {
    type Item = std::io::Result<bytes::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.first.take() {
            Some(bytes) => Poll::Ready(Some(Ok(bytes))),
            None => Poll::Pending,
        }
    }
}

impl Drop for IncompleteBody {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

pub(super) async fn qualify(cluster: &ManagedCluster) -> Result<()> {
    let info = cluster.nodes[0].info();
    let name = format!("{NAMESPACE}/0");
    let path = format!(
        "/runtime/{}?name={}",
        cellar::engine_api::kv_cell_scope(NAMESPACE, 0),
        url::form_urlencoded::byte_serialize(name.as_bytes()).collect::<String>()
    );
    let payload = serde_json::to_vec(
        &json!({"op":"put","key":"incomplete-upload","value":[42],"tag":"bytes"}),
    )?;
    let dropped = Arc::new(AtomicBool::new(false));
    let body = IncompleteBody {
        first: Some(bytes::Bytes::copy_from_slice(&payload[..7])),
        dropped: dropped.clone(),
    };
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()?;
    let request = client
        .post(format!("http://{}{path}", info.internal_address))
        .body(reqwest::Body::wrap_stream(body));
    let auth = cellar::peer_auth::PeerAuth::new([11; 32], "operator-upload-fixture")?;
    let request = auth.sign(request, "POST", &path, &payload, &info.node)?;
    let started = cellar::asyncrt::mono_ms();
    // The fixture's node policy is three seconds. This is an observer, not a
    // replacement request deadline, and fails the original unbounded collector.
    let response = cellar::asyncrt::timeout(Duration::from_millis(3500), request.send())
        .await
        .context(
            "primitive entrance retained an incomplete upload beyond its original deadline",
        )??;
    let status = response.status();
    ensure!(
        status == 504,
        "incomplete primitive upload returned {status}"
    );
    ensure!(response
        .headers()
        .get("x-cellar-rpc-failure")
        .is_some_and(|v| v == "DeadlineExceeded"));
    ensure!(response
        .headers()
        .get("connection")
        .is_some_and(|v| v == "close"));
    ensure!(response.text().await? == "RPC deadline exceeded");
    drop(client);
    cellar::asyncrt::timeout(Duration::from_millis(100), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("deadline did not release the upload stream")?;
    ensure!(
        admin(cluster, json!({"op":"get","key":"incomplete-upload"})).await?["found"] == false,
        "incomplete upload reached the primitive adapter"
    );
    public_ok(cluster, 0, "Read").await?;
    eprintln!("primitive incomplete upload expired, released its stream and left no mutation: elapsed_ms={}", cellar::asyncrt::mono_ms() - started);
    Ok(())
}
