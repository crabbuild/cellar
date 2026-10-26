// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A native SDK barrier proves shutdown retains the actual JS bridge, rather
//! than merely waiting for an application-local JS promise to resolve.
use anyhow::Result;
use cellar::sdk::*;
use std::sync::Arc;

#[derive(Default)]
pub struct Signals {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

pub struct RetainedApplication(pub Arc<dyn Application>, pub Arc<Signals>);

struct RetainedCell(Arc<dyn Cell>, Arc<Signals>);

impl Application for RetainedApplication {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.0.validate_deployment(deployment)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(RetainedCell(
            self.0.create_cell(context, identity)?,
            self.1.clone(),
        )))
    }
}

impl Cell for RetainedCell {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if matches!(&event, Invocation::CellRpc { method, .. } if method == "nativeGate") {
                self.1.entered.notify_one();
                self.1.release.notified().await;
                return Ok(Output::Rpc(cellar::engine_api::RpcData::Json(
                    "null".into(),
                )));
            }
            self.0.clone().invoke(context, event).await
        })
    }
}
