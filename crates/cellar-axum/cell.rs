use anyhow::anyhow;
use axum::Router;
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use std::sync::Arc;
use tower::ServiceExt;

/// One activation's router. Each invocation injects its own current Context.
pub struct AxumCell {
    router: Router,
    delegate: Option<Arc<dyn Cell>>,
}

impl AxumCell {
    pub fn new(router: Router) -> Self {
        Self {
            router,
            delegate: None,
        }
    }

    pub fn with_cell(router: Router, delegate: Arc<dyn Cell>) -> Self {
        Self {
            router,
            delegate: Some(delegate),
        }
    }
}

impl Cell for AxumCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        match invocation {
            Invocation::Fetch(request) => Box::pin(async move {
                let request = crate::http::request(&context, request)?;
                let response = self
                    .router
                    .clone()
                    .oneshot(request)
                    .await
                    .unwrap_or_else(|never| match never {});
                Ok(Output::Http(crate::http::output(&context, response)?))
            }),
            invocation => match &self.delegate {
                Some(delegate) => delegate.clone().invoke(context, invocation),
                None => Box::pin(async { Err(anyhow!("no non-HTTP cell handler registered")) }),
            },
        }
    }
}
