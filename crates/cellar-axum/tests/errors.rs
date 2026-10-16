#[path = "../../cellar/tests/support/actor_lease_trace.rs"]
// The shared fixture's renewal assertion is used by native_actor.
#[allow(dead_code)]
mod lease_trace;
mod support;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use cellar_axum::{ApplicationBuilder, ApplicationDefinition};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Overloaded(Arc<AtomicUsize>);

impl Cell for Overloaded {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(cellar::engine_api::CellOverloaded.into())
        })
    }
}

fn definition(entered: Arc<AtomicUsize>) -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", move |_, _| {
            Ok(Arc::new(Overloaded(entered.clone())))
        })?
        .cell_route("/counters/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarding_preserves_framework_overload_classification() -> Result<()> {
    let trace = lease_trace::install();
    for external in [false, true] {
        let dir = tempfile::tempdir()?;
        let entered = Arc::new(AtomicUsize::new(0));
        let observed = entered.clone();
        trace.mark(&format!("starting overload host external={external}"));
        let host = support::start(dir.path(), external, "", move || {
            definition(observed.clone())
        })
        .await
        .with_context(|| format!("starting overload host external={external}"))?;
        trace.mark(&format!("overload host started external={external}"));
        let result: Result<()> = async {
            let response = support::client()?
                .get(format!("{}/counters/alpha", host.url))
                .send()
                .await
                .context("forwarded overload response head")?;
            trace.mark(&format!("overload response head external={external}"));
            ensure!(response.status() == 503, "{}", response.status());
            ensure!(response.headers().contains_key("retry-after"));
            ensure!(response.headers().get("x-cellar-overload").is_some());
            ensure!(
                entered.load(Ordering::SeqCst) == 1,
                "classified overload did not come from exactly one handler invocation"
            );
            Ok(())
        }
        .await;
        if let Err(error) = result {
            eprintln!(
                "overload forwarding failed: external={external}, handler_entries={}, \
                 node_closed={}, error={error:#}, lease_observations={:#?}",
                entered.load(Ordering::SeqCst),
                host.node.is_closed(),
                trace.snapshot()
            );
            let outcome = host.cancel().await;
            return Err(error.context(format!(
                "overload forwarding external={external}; joined node cancellation: {outcome:?}"
            )));
        }
        host.stop().await?;
    }
    Ok(())
}
