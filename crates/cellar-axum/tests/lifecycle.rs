// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
#[path = "../../cellar/tests/support/actor_lease_trace.rs"]
#[allow(dead_code)]
mod lease_trace;
mod support;
use anyhow::{ensure, Result};
use axum::{body::Body, extract::State, response::Response, routing::get, Router};
use cellar::sdk::{Context, NodeOutcome, ShutdownMode};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition, CellContext};
use http_body_util::BodyExt;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Default)]
struct Signals {
    context: Mutex<Option<Context>>,
    entered: Notify,
    released: Notify,
}

async fn holding(
    CellContext(context): CellContext,
    State(signals): State<Arc<Signals>>,
) -> Response {
    *signals.context.lock().unwrap() = Some(context.clone());
    signals.entered.notify_one();
    let body = futures_util::stream::once(async move {
        signals.released.notified().await;
        Ok::<_, std::io::Error>("done")
    });
    Response::new(Body::from_stream(body))
}

fn definition(signals: Arc<Signals>) -> Result<ApplicationDefinition> {
    ApplicationBuilder::new(
        Router::new()
            .route("/hold", get(holding))
            .with_state(signals),
    )
    .cell("Counter", |_, _| {
        Ok(Arc::new(cellar_axum::AxumCell::new(Router::new())))
    })?
    .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_body_is_counted_and_bounded_shutdown_reports_an_incomplete_drain() -> Result<()> {
    let trace = lease_trace::install(None);
    trace.mark("open-body host starting");
    let directory = tempfile::tempdir()?;
    let signals = Arc::new(Signals::default());
    let factory = signals.clone();
    let host = support::start(directory.path(), true, "", move || {
        definition(factory.clone())
    })
    .await?;
    trace.mark("open-body host started");
    let ingress = host.ingress.as_ref().unwrap().clone();
    let response = ingress
        .call(http::Request::builder().uri("/hold").body(Body::empty())?)
        .await;
    trace.mark("open-body response head received");
    if response.status() != 200 {
        let status = response.status();
        let headers = response.headers().clone();
        let entered = signals.context.lock().unwrap().is_some();
        let closed = host.node.is_closed();
        let services = host.node.services();
        // Read only a failed head's diagnostic body. The successful stream is
        // deliberately held and must never be polled to make this check pass.
        let body = tokio::time::timeout(
            Duration::from_secs(1),
            http_body_util::Limited::new(response.into_body(), 16 * 1024).collect(),
        )
        .await;
        let body = match body {
            Ok(Ok(body)) => String::from_utf8_lossy(&body.to_bytes()).into_owned(),
            Ok(Err(error)) => format!("diagnostic body refused: {error}"),
            Err(_) => "diagnostic body timed out".into(),
        };
        let observations = trace.snapshot();
        let work = services.application_work();
        let resources = services.rpc_response_usage();
        let outcome = host.cancel().await;
        anyhow::bail!(
            "open-body response head was {status}; headers={headers:?}; body={body}; \
             handler_entered={entered}; node_closed={closed}; work={work:?}; \
             response_admission={resources:?}; lease_observations={observations:?}; \
             joined node cancellation={outcome:?}"
        );
    }
    signals.entered.notified().await;
    let report = tokio::time::timeout(
        Duration::from_secs(7),
        host.shutdown_report(ShutdownMode::Preserve),
    )
    .await??;
    ensure!(
        report.drain_timed_out && !report.ingress_drained,
        "open stream was called drained: {report:?}"
    );
    ensure!(ingress.is_closed());
    let retained = signals.context.lock().unwrap().take().unwrap();
    ensure!(
        retained.wait_until(async { Ok(()) }).is_err(),
        "retired event retained authority"
    );
    drop(response);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_cancel_and_domain_isolation_are_preserved() -> Result<()> {
    let a_dir = tempfile::tempdir()?;
    let b_dir = tempfile::tempdir()?;
    let a = support::start(a_dir.path(), true, "", || {
        ApplicationBuilder::new(Router::new().route("/value", get(|| async { "a" })))
            .cell("Counter", |_, _| {
                Ok(Arc::new(cellar_axum::AxumCell::new(Router::new())))
            })?
            .build()
    })
    .await?;
    let b = support::start(b_dir.path(), true, "", || {
        ApplicationBuilder::new(Router::new().route("/value", get(|| async { "b" })))
            .cell("Counter", |_, _| {
                Ok(Arc::new(cellar_axum::AxumCell::new(Router::new())))
            })?
            .build()
    })
    .await?;
    let old = a.ingress.as_ref().unwrap().clone();
    ensure!(a.cancel().await? == NodeOutcome::Cancelled);
    ensure!(
        old.call(http::Request::builder().uri("/value").body(Body::empty())?)
            .await
            .status()
            == 503
    );
    ensure!(
        support::client()?
            .get(format!("{}/value", b.url))
            .send()
            .await?
            .text()
            .await?
            == "b"
    );
    b.stop().await?;
    Ok(())
}
