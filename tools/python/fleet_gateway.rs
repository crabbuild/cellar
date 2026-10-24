// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native routing for the same Python fleet cell and original invocation scope.
use anyhow::{ensure, Result};
use cellar::sdk::{
    http::Url, Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use cellar_python::PythonApplication;
use std::sync::Arc;

pub(super) struct EntityGateway {
    python: Arc<PythonApplication>,
}

impl EntityGateway {
    pub(super) fn new(python: Arc<PythonApplication>) -> Self {
        Self { python }
    }
}

fn binding(options: &cellar::engine_api::WorkerConfigOptions) -> Result<()> {
    ensure!(
        options
            .bindings
            .iter()
            .any(|(binding, class)| binding == "ENTITIES" && class == "Entity"),
        "native entity route requires ENTITIES bound to Entity"
    );
    Ok(())
}

impl Application for EntityGateway {
    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.python.validate_configuration(options)?;
        binding(options)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.python.validate_deployment(deployment)?;
        binding(&deployment.options)
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        let Invocation::Fetch(request) = event else {
            return self.python.clone().invoke(context, event);
        };
        Box::pin(async move {
            // Match the native fleet application's route. The namespace stub
            // retains admission, ownership, cancellation and response durability.
            let url = Url::new(&request.url, None)?;
            let name = url
                .pathname()
                .strip_prefix("/cells/")
                .filter(|name| !name.is_empty())
                .ok_or_else(|| anyhow::anyhow!("expected /cells/NAME"))?
                .to_owned();
            Ok(Output::Http(
                context
                    .namespace("ENTITIES")?
                    .get_by_name(&name)
                    .fetch(&context, request)
                    .await?,
            ))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.python.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.python.create_cell(context, identity)
    }
}
