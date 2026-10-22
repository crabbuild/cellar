use anyhow::{anyhow, ensure, Result};
use axum::Router;
use cellar::sdk::{
    workflow, Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output, Registry,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tower::ServiceExt;

/// Axum HTTP routing with SDK delegation for cells and non-HTTP invocations.
pub struct AxumApplication {
    router: Router,
    entrypoints: HashMap<String, Router>,
    delegate: Arc<dyn Application>,
    bindings: HashSet<(String, String)>,
}

impl AxumApplication {
    pub fn new(router: Router) -> Self {
        Self::with_application(
            router,
            Arc::new(Registry::new(|_, _| {
                Box::pin(async { Err(anyhow!("no non-HTTP application handler registered")) })
            })),
        )
    }

    pub fn with_application(router: Router, delegate: Arc<dyn Application>) -> Self {
        Self {
            router,
            delegate,
            entrypoints: HashMap::new(),
            bindings: HashSet::new(),
        }
    }

    pub(crate) fn require_bindings(mut self, bindings: HashSet<(String, String)>) -> Self {
        self.bindings = bindings;
        self
    }

    pub fn entrypoint(mut self, name: impl Into<String>, router: Router) -> Result<Self> {
        let name = name.into();
        ensure!(
            !name.is_empty() && name != "default",
            "entrypoint must have a non-default name"
        );
        ensure!(
            !self.entrypoints.contains_key(&name),
            "duplicate entrypoint {name}"
        );
        self.entrypoints.insert(name, router);
        Ok(self)
    }
}

impl Application for AxumApplication {
    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.delegate.validate_configuration(options)?;
        crate::builder::validate_bindings(&self.bindings, options)
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        let router = match &invocation {
            Invocation::Fetch(request) => {
                match request.entrypoint.as_ref().map(|entry| entry.name.as_str()) {
                    None | Some("default") => Some(self.router.clone()),
                    Some(name) => self.entrypoints.get(name).cloned(),
                }
            }
            _ => None,
        };
        let Some(router) = router else {
            return self.delegate.clone().invoke(context, invocation);
        };
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                unreachable!("Fetch router selected");
            };
            let request = crate::http::request(&context, request)?;
            let response = router
                .oneshot(request)
                .await
                .unwrap_or_else(|never| match never {});
            Ok(Output::Http(crate::http::output(&context, response)?))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.delegate.supports_cell(class)
    }

    fn resolve_raft_class(
        &self,
        class: &str,
    ) -> std::result::Result<Arc<cellar::sdk::RaftClass>, cellar::sdk::RegistrationError> {
        self.delegate.resolve_raft_class(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.delegate.create_cell(context, identity)
    }

    fn supports_workflow(&self, class: &str) -> bool {
        self.delegate.supports_workflow(class)
    }

    fn run_workflow(
        self: Arc<Self>,
        class: String,
        context: Context,
        event: workflow::Event,
        steps: workflow::Steps,
    ) -> HandlerFuture<cellar_native::clone::StructuredValue> {
        self.delegate
            .clone()
            .run_workflow(class, context, event, steps)
    }
}
