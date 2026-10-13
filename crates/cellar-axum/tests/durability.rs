// The test coordinator owns its proof observation deadlines outside the node.
#![allow(clippy::disallowed_methods)]
mod support;
use anyhow::{ensure, Result};
use axum::{extract::State, routing::get, Json, Router};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition, AxumCell, CellContext};
use std::sync::Arc;

async fn count(
    CellContext(context): CellContext,
    State(owner): State<&'static str>,
    request: axum::extract::Request,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let result = (|| {
        let storage = context.storage()?;
        let mut count = storage
            .get_json("count")?
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if request.method() == axum::http::Method::POST {
            count += 1;
            storage.put_json("count", &serde_json::json!(count))?;
        }
        Ok(Json(serde_json::json!({"count":count,"owner":owner})))
    })();
    result.map_err(|error: anyhow::Error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
    })
}

fn definition(owner: &'static str) -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", move |_, _| {
            Ok(Arc::new(AxumCell::new(
                Router::new()
                    .route("/counters/{name}", get(count).post(count))
                    .with_state(owner),
            )))
        })?
        .cell_route("/counters/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_owner_and_handoff_recover_the_same_acknowledged_cell() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
    let a = support::start_with(
        &directory.path().join("a"),
        false,
        "",
        "a",
        Some(bucket.clone()),
        || definition("a"),
    )
    .await?;
    let client = support::client()?;
    let first: serde_json::Value = client
        .post(format!("{}/counters/alpha", a.url))
        .send()
        .await?
        .json()
        .await?;
    ensure!(first["count"] == 1 && first["owner"] == "a");
    let b = support::start_with(
        &directory.path().join("b"),
        true,
        "",
        "b",
        Some(bucket),
        || definition("b"),
    )
    .await?;
    let remote: serde_json::Value = client
        .post(format!("{}/counters/alpha", b.url))
        .send()
        .await?
        .json()
        .await?;
    ensure!(
        remote["count"] == 2 && remote["owner"] == "a",
        "remote owner route: {remote}"
    );
    a.stop_mode(cellar::sdk::ShutdownMode::Handoff).await?;
    let next: serde_json::Value = client
        .post(format!("{}/counters/alpha", b.url))
        .send()
        .await?
        .json()
        .await?;
    ensure!(
        next["count"] == 3 && next["owner"] == "b",
        "handoff recovery: {next}"
    );
    b.stop().await?;
    Ok(())
}

#[path = "support/store.rs"]
mod store;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_heads_wait_for_real_replication_and_fail_when_proof_fails() -> Result<()> {
    use std::{sync::atomic::Ordering, time::Duration};
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let mut bucket =
            cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
        let control = Arc::new(store::ProofControl::default());
        bucket.store = Arc::new(store::ProofStore {
            inner: bucket.store.clone(),
            control: control.clone(),
        });
        let host = support::start_with(
            &directory.path().join("node"),
            external,
            "",
            "proof",
            Some(bucket),
            || definition("proof"),
        )
        .await?;
        let client = support::client()?;
        // Materialize the database before faulting the next business write.
        ensure!(
            client
                .get(format!("{}/counters/alpha", host.url))
                .send()
                .await?
                .status()
                == 200
        );
        control.mode.store(1, Ordering::SeqCst);
        let request = client.post(format!("{}/counters/alpha", host.url)).send();
        tokio::pin!(request);
        ensure!(
            tokio::time::timeout(Duration::from_millis(200), &mut request)
                .await
                .is_err(),
            "head escaped without replication proof"
        );
        tokio::time::timeout(Duration::from_secs(5), control.entered.notified()).await?;
        let observed = client.get(format!("{}/counters/alpha", host.url)).send();
        tokio::pin!(observed);
        ensure!(
            tokio::time::timeout(Duration::from_millis(200), &mut observed)
                .await
                .is_err(),
            "read exposed an unproven observed position"
        );
        control.mode.store(0, Ordering::SeqCst);
        control.release.add_permits(8);
        let response = tokio::time::timeout(Duration::from_secs(5), request).await??;
        ensure!(response.status() == 200);
        ensure!(response.json::<serde_json::Value>().await?["count"] == 1);
        let observed = tokio::time::timeout(Duration::from_secs(5), observed).await??;
        ensure!(
            observed.status() == 200 && observed.json::<serde_json::Value>().await?["count"] == 1
        );
        control.mode.store(2, Ordering::SeqCst);
        let response = client
            .post(format!("{}/counters/alpha", host.url))
            .send()
            .await;
        if let Ok(response) = response {
            ensure!(
                !response.status().is_success(),
                "failed proof acknowledged a write"
            );
        }
        control.mode.store(0, Ordering::SeqCst);
        host.stop().await?;
    }
    Ok(())
}
