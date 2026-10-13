mod support;
use anyhow::{ensure, Result};
use axum::{
    extract::{Path, Query},
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition, AxumCell, CellContext, CurrentCell};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
struct Count {
    count: u64,
    class: String,
    name: String,
}

type Error = (axum::http::StatusCode, String);

fn error(error: impl std::fmt::Display) -> Error {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        error.to_string(),
    )
}

async fn read(
    CellContext(context): CellContext,
    CurrentCell(identity): CurrentCell,
    Path(name): Path<String>,
) -> Result<Json<Count>, Error> {
    let count = context
        .storage()
        .map_err(error)?
        .get_json("count")
        .map_err(error)?
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    Ok(Json(Count {
        count,
        class: identity.class,
        name,
    }))
}

async fn write(
    context: CellContext,
    identity: CurrentCell,
    name: Path<String>,
) -> Result<Json<Count>, Error> {
    let storage = context.0.storage().map_err(error)?;
    let count = storage
        .get_json("count")
        .map_err(error)?
        .and_then(|value| value.as_u64())
        .unwrap_or(0)
        + 1;
    storage
        .put_json("count", &serde_json::json!(count))
        .map_err(error)?;
    read(context, identity, name).await
}

fn definition() -> Result<ApplicationDefinition> {
    ApplicationBuilder::new(Router::new().route("/health", get(|| async { "ready" })))
        .cell("Counter", |_, _| {
            Ok(Arc::new(AxumCell::new(
                Router::new().route("/counters/{name}", get(read).post(write)),
            )))
        })?
        .cell_route("/counters/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_handlers_persist_and_recover_through_both_http_hosts() -> Result<()> {
    let client = support::client()?;
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", definition).await?;
        let health = client.get(format!("{}/health", host.url)).send().await?;
        ensure!(health.status() == 200 && health.text().await? == "ready");
        for count in 1..=2 {
            let response = client
                .post(format!("{}/counters/alpha", host.url))
                .send()
                .await?;
            ensure!(
                response.status() == 200,
                "counter failed: {}",
                response.text().await?
            );
            let response: Count = response.json().await?;
            ensure!(
                response.count == count && response.class == "Counter" && response.name == "alpha"
            );
        }
        let other: Count = client
            .get(format!("{}/counters/beta", host.url))
            .send()
            .await?
            .json()
            .await?;
        ensure!(other.count == 0 && other.name == "beta");
        let refused = client
            .delete(format!("{}/counters/alpha", host.url))
            .send()
            .await?;
        ensure!(refused.status() == 405, "unsupported method was accepted");
        ensure!(
            client
                .request(
                    reqwest::Method::TRACE,
                    format!("{}/counters/alpha", host.url)
                )
                .send()
                .await?
                .status()
                == 501
        );
        if external {
            ensure!(
                client
                    .get(format!("{}/outside", host.url))
                    .send()
                    .await?
                    .text()
                    .await?
                    == "ordinary Axum"
            );
        }
        host.stop().await?;
        let restarted = support::start(directory.path(), external, "", definition).await?;
        let recovered: Count = client
            .get(format!("{}/counters/alpha", restarted.url))
            .send()
            .await?
            .json()
            .await?;
        ensure!(recovered.count == 2, "acknowledged count was lost");
        restarted.stop().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn axum_mounting_preserves_canonical_routes_and_encoded_cell_names() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let host = support::start(directory.path(), true, "/api", definition).await?;
    let client = support::client()?;
    let response = client
        .post(format!("{}/counters/snow%20day?view=count", host.url))
        .send()
        .await?;
    ensure!(
        response.status() == 200,
        "mounted route failed: {}",
        response.text().await?
    );
    let count: Count = response.json().await?;
    ensure!(count.count == 1 && count.name == "snow day");
    ensure!(
        client
            .get(format!("{}/peer/probe", host.url))
            .send()
            .await?
            .status()
            == 404
    );
    host.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn namespace_requirements_are_checked_before_node_startup() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let definition = || {
        ApplicationBuilder::new(Router::new())
            .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))?
            .cell_route("/counters/{name}", "MISSING", "Counter", "name")?
            .build()
    };
    let result = support::start(directory.path(), true, "", definition).await;
    match result {
        Ok(host) => {
            host.stop().await?;
            anyhow::bail!("invalid namespace was admitted");
        }
        Err(error) => ensure!(format!("{error:#}").contains("MISSING")),
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
struct Payload {
    value: String,
}

#[derive(Deserialize)]
struct View {
    label: String,
}

async fn echo(
    CellContext(context): CellContext,
    CurrentCell(identity): CurrentCell,
    Path(name): Path<String>,
    Query(view): Query<View>,
    headers: HeaderMap,
    Json(payload): Json<Payload>,
) -> Result<Json<serde_json::Value>, Error> {
    context
        .storage()
        .map_err(error)?
        .put_json("length", &serde_json::json!(payload.value.len()))
        .map_err(error)?;
    let items = headers
        .get_all("x-item")
        .iter()
        .map(|value| value.to_str().map(str::to_owned).map_err(error))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(serde_json::json!({
        "value": payload.value,
        "name": name,
        "class": identity.class,
        "label": view.label,
        "items": items,
    })))
}

fn echo_definition() -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", |_, _| {
            Ok(Arc::new(AxumCell::new(
                Router::new().route("/echo/{name}", post(echo)),
            )))
        })?
        .cell_route("/echo/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_request_extractors_preserve_buffered_and_streamed_bodies_under_both_hosts(
) -> Result<()> {
    let client = support::client()?;
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", echo_definition).await?;
        let url = format!("{}/echo/snow%20day?label=round%20trip", host.url);
        // Native ingress streams Content-Length bodies at 1 MiB; stay below
        // Axum Json's default 2 MiB limit while exercising that branch.
        for length in [8, (1 << 20) + 128] {
            let payload = Payload {
                value: "x".repeat(length),
            };
            let mut headers = reqwest::header::HeaderMap::new();
            headers.append("x-item", "first".parse()?);
            headers.append("x-item", "second".parse()?);
            let response = client
                .post(&url)
                .headers(headers)
                .json(&payload)
                .send()
                .await?;
            ensure!(
                response.status() == 200,
                "JSON extraction failed: {}",
                response.text().await?
            );
            let body: serde_json::Value = response.json().await?;
            ensure!(body["value"] == payload.value && body["name"] == "snow day");
            ensure!(body["class"] == "Counter" && body["label"] == "round trip");
            ensure!(
                body["items"] == serde_json::json!(["first", "second"]),
                "duplicate headers changed: {}",
                body["items"]
            );
        }
        let response = client
            .post(&url)
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await?;
        ensure!(
            response.status() == 400,
            "malformed JSON did not retain Axum's rejection"
        );
        let response = client.post(&url).body("{}").send().await?;
        ensure!(
            response.status() == 415,
            "missing JSON content type was accepted"
        );
        host.stop().await?;
    }
    Ok(())
}
