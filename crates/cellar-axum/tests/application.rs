// The test coordinator owns its timeout outside any application domain.
#![allow(clippy::disallowed_methods)]
mod support;
use anyhow::{ensure, Result};
use axum::{
    body::Body,
    middleware::{self, Next},
    response::Response,
    routing::get,
    Router,
};
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition, AxumCell, CellContext};
use std::sync::Arc;
use tokio::sync::Notify;

async fn auth(request: axum::extract::Request, next: Next) -> Response {
    if request
        .headers()
        .get("authorization")
        .is_none_or(|v| v != "Bearer test")
    {
        return Response::builder()
            .status(401)
            .body(Body::from("unauthorized"))
            .unwrap();
    }
    next.run(request).await
}

async fn named(
    CellContext(context): CellContext,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let result = async {
        let response = context
            .exported_entrypoint("private")?
            .fetch(
                &context,
                cellar::sdk::Request {
                    url: "https://private/value".into(),
                    method: "GET".into(),
                    headers: Vec::new(),
                    body: Vec::new().into(),
                    entrypoint: None,
                },
            )
            .await?;
        cellar_axum::http::response(response)
    }
    .await;
    result.map_err(|error: anyhow::Error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn middleware_covers_public_and_named_http_entrypoints() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", || {
            Ok(
                ApplicationBuilder::new(Router::new().route("/named", get(named)))
                    .entrypoint(
                        "private",
                        Router::new().route("/value", get(|| async { "secret" })),
                    )?
                    .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))?
                    .build()?
                    .layer(middleware::from_fn(auth)),
            )
        })
        .await?;
        let client = support::client()?;
        ensure!(
            client
                .get(format!("{}/named", host.url))
                .send()
                .await?
                .status()
                == 401
        );
        // Outer authorization cannot bypass the independently protected entrypoint.
        ensure!(
            client
                .get(format!("{}/named", host.url))
                .header("authorization", "Bearer test")
                .send()
                .await?
                .status()
                == 401
        );
        host.stop().await?;
    }
    Ok(())
}

struct Alarm(Arc<Notify>);

impl Cell for Alarm {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if matches!(invocation, Invocation::Alarm { .. }) {
                context.storage()?.delete_alarm()?;
                self.0.notify_one();
                Ok(Output::Done)
            } else {
                anyhow::bail!("unsupported delegate event")
            }
        })
    }
}

async fn arm(
    CellContext(context): CellContext,
) -> Result<&'static str, (axum::http::StatusCode, String)> {
    context
        .storage()
        .and_then(|storage| storage.set_alarm(cellar::asyncrt::wall_ms() + 50))
        .map(|_| "armed")
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

fn alarm_definition(signal: Arc<Notify>) -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", move |_, _| {
            Ok(Arc::new(AxumCell::with_cell(
                Router::new()
                    .route("/alarms/{name}", get(arm))
                    .with_state(()),
                Arc::new(Alarm(signal.clone())),
            )))
        })?
        .cell_route("/alarms/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_http_cell_events_reach_the_sdk_delegate() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let signal = Arc::new(Notify::new());
        let factory = signal.clone();
        let host = support::start(directory.path(), external, "", move || {
            alarm_definition(factory.clone())
        })
        .await?;
        ensure!(
            support::client()?
                .get(format!("{}/alarms/alpha", host.url))
                .send()
                .await?
                .status()
                == 200
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), signal.notified()).await?;
        host.stop().await?;
    }
    Ok(())
}
