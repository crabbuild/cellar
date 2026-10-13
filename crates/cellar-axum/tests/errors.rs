mod support;
use anyhow::{ensure, Result};
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition};
use std::sync::Arc;

struct Overloaded;

impl Cell for Overloaded {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { Err(cellar::engine_api::CellOverloaded.into()) })
    }
}

fn definition() -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", |_, _| Ok(Arc::new(Overloaded)))?
        .cell_route("/counters/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarding_preserves_framework_overload_classification() -> Result<()> {
    for external in [false, true] {
        let dir = tempfile::tempdir()?;
        let host = support::start(dir.path(), external, "", definition).await?;
        let response = support::client()?
            .get(format!("{}/counters/alpha", host.url))
            .send()
            .await?;
        ensure!(response.status() == 503, "{}", response.status());
        ensure!(response.headers().contains_key("retry-after"));
        ensure!(response.headers().get("x-cellar-overload").is_some());
        host.stop().await?;
    }
    Ok(())
}
