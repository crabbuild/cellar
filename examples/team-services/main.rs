use crate::{counter, support};
use anyhow::Result;
use cellar::sdk::{Application, Invocation, Output, Registry};
use std::sync::Arc;
pub fn api() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(Registry::new(|context, event| {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            Ok(Output::Http(
                context.service("LEDGER")?.fetch(&context, request).await?,
            ))
        })
    })))
}
pub fn ledger() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|context, event| {
            Box::pin(async move {
                let Invocation::Fetch(request) = event else {
                    return Ok(Output::Done);
                };
                counter::fetch(&context, request).await
            })
        })
        .cell("Counter", |_, _| Ok(support::cell(counter::Counter)))?,
    ))
}
