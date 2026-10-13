use crate::{loader, support};
use cellar::sdk::{
    facets::Startup,
    loader::{Outbound, WorkerCode},
    Cell, Context, HandlerFuture, Invocation, Output,
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
                    let mut code = WorkerCode::new(loader::child()?);
                    code.outbound = Outbound::Deny;
                    let worker = startup.loader("WORKERS")?.load(&startup, code)?;
                    Ok(Startup::new(worker.cell_class("Leaf")))
                })
            })?;
            Ok(Output::Http(child.fetch(&context, request).await?))
        })
    }
}
