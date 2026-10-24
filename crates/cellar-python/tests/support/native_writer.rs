// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise a native Rust writer against a snapshot held by Python in the same
//! cell. Each writer retains its own real SDK invocation and execution domain.
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_python::PythonApplication;
use serde_json::{json, Value};
use std::sync::Arc;

pub struct ApplicationWithWriter(pub Arc<PythonApplication>);

impl Application for ApplicationWithWriter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(NativeWriter(
            self.0.create_cell(context, identity)?,
        )))
    }
}

struct NativeWriter(Arc<dyn Cell>);

impl Cell for NativeWriter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/native-overload"))
        {
            return Box::pin(async move {
                context.storage()?.transaction_sync(|tx| {
                    let count = tx
                        .get_json("overload-committed")?
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0);
                    tx.put_json("overload-committed", &json!(count + 1))
                })?;
                // Model a callee propagating a nested admission failure after
                // its own write. This verdict alone cannot prove rollback.
                Err(cellar::cell_dispatch::RoutedRequestError(
                    cellar_logic::RequestError::CapacityExhausted,
                )
                .into())
            });
        }
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/native-write")) {
            return Box::pin(async move {
                context.storage()?.transaction_sync(|tx| {
                    tx.put_json("native-conflict", &json!(41))?;
                    tx.put_json("native-only", &json!(true))
                })?;
                Ok(Output::Http(
                    http::Response::new(200, b"true".to_vec(), Vec::new()).into_http(&context)?,
                ))
            });
        }
        self.0.clone().invoke(context, event)
    }
}

pub async fn qualify(client: &reqwest::Client, url: &str) -> Result<()> {
    let classification: Value = client
        .get(format!("{url}/overload-after-write"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        // The native SDK returns a callee's handler failure without its inner
        // typed route verdict. Python must retain uncertainty and never replay.
        classification == json!({"code":"Ambiguous","outcome":"unknown","count":1}),
        "committed overload result: {classification}"
    );
    let attempt = client.get(format!("{url}/native-conflict")).send();
    let writer = async {
        loop {
            if client
                .get(format!("{url}/native-snapshot-ready"))
                .send()
                .await?
                .error_for_status()?
                .json::<bool>()
                .await?
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        ensure!(
            client
                .post(format!("{url}/native-write"))
                .send()
                .await?
                .error_for_status()?
                .json::<bool>()
                .await?
        );
        client
            .get(format!("{url}/native-snapshot-release"))
            .send()
            .await?
            .error_for_status()?;
        Ok::<_, anyhow::Error>(())
    };
    let (attempt, writer) = futures_util::future::join(attempt, writer).await;
    writer?;
    let result: Value = attempt?.error_for_status()?.json().await?;
    ensure!(
        result == json!({"conflict": true, "value": 41, "native": true, "python": false}),
        "Python overwrote a native writer or partially committed: {result}"
    );
    Ok(())
}
