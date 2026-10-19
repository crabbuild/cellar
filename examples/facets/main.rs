use crate::{counter, support};
use cellar::sdk::{
    facets::Startup,
    loader::{Outbound, WorkerCode},
    Cell, Context, HandlerFuture, Invocation, Output, Registry,
};
use std::sync::Arc;

pub struct Supervisor;

impl Cell for Supervisor {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            support::method(&request, &["GET", "POST"])?;
            let facets = context.facets()?;
            let child = facets.get(&context, "counter", |startup| {
                Box::pin(async move {
                    let application = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
                        .cell("Leaf", |_, _| Ok(Arc::new(counter::Counter)))?;
                    let mut code = WorkerCode::new(Arc::new(application));
                    code.outbound = Outbound::Deny;
                    let worker = startup.loader("WORKERS")?.load(&startup, code)?;
                    Ok(Startup::new(worker.cell_class("Leaf")))
                })
            })?;
            Ok(Output::Http(child.fetch(&context, request).await?))
        })
    }
}
