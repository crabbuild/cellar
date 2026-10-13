use crate::{counter, support};
use anyhow::Result;
use cellar::sdk::{
    loader::{Outbound, WorkerCode},
    Application, Context, Invocation, Output, Registry, Request,
};
use std::sync::Arc;

pub fn child() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|context, event| {
            Box::pin(async move {
                let Invocation::Fetch(_) = event else {
                    return Ok(Output::Done);
                };
                support::json(
                    &context,
                    serde_json::json!({
                        "loaded": "compiled Rust",
                        "environment": context.loaded_environment()?.map(|v| v.to_json()).transpose()?,
                    }),
                )
            })
        })
        .cell("Leaf", |_, _| Ok(Arc::new(counter::Counter)))?,
    ))
}

pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["GET"])?;
    let mut code = WorkerCode::new(child()?);
    code.outbound = Outbound::Deny;
    code.environment = Some(cellar::sdk::primitives::clone::StructuredValue::from_json(
        serde_json::json!({ "team": "examples" }),
    )?);
    let worker = context.loader("WORKERS")?.load(context, code)?;
    let response = worker.entrypoint("default").fetch(context, request).await?;
    let background = context.clone();
    context.wait_until(async move {
        background
            .namespace("COUNTERS")?
            .get_by_name("loaded-calls")
            .rpc(
                &background,
                "increment",
                cellar::engine_api::RpcData::Json("[]".into()),
            )
            .await?;
        Ok(())
    })?;
    Ok(Output::Http(response))
}
