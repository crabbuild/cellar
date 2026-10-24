// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Route canonical order URLs to their cell before entering Python.
//! FastAPI still processes the original HTTP request inside that cell.
use anyhow::Result;
use cellar::sdk::{
    http, Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use cellar_python::PythonApplication;
use std::sync::Arc;

pub(super) struct Orders(pub Arc<PythonApplication>);

fn order_name(method: &str, url: &str) -> Option<String> {
    if !matches!(method, "GET" | "POST" | "PATCH") {
        return None;
    }
    let url = http::Url::new(url, None).ok()?;
    let name = url.pathname().strip_prefix("/orders/")?.to_owned();
    (1..=64).contains(&name.len()).then_some(())?;
    name.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        .then_some(name)
}

fn route_failure(error: &anyhow::Error) -> cellar::engine_api::HttpResponse {
    // Match the Python cell.fetch classification, including unknown outcomes
    // after a submitted mutation. Never expose native/backend error chains.
    let failure = cellar_guest::failure::envelope("cell.fetch", error);
    cellar::engine_api::HttpResponse {
        status: 503,
        body: serde_json::json!({
            "detail": "Cellar operation failed",
            "code": failure["code"],
            "outcome": failure["outcome"],
        })
        .to_string()
        .into_bytes(),
        headers: vec![("content-type".into(), "application/json".into())],
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    }
}

impl Application for Orders {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if let Invocation::Fetch(request) = event {
            if let Some(name) = order_name(&request.method, &request.url) {
                return Box::pin(async move {
                    let response = async {
                        context
                            .namespace("ORDERS")?
                            .get_by_name(&name)
                            .fetch(&context, request)
                            .await
                    }
                    .await;
                    Ok(Output::Http(match response {
                        Ok(response) => response,
                        Err(error) => route_failure(&error),
                    }))
                });
            }
            return self.0.clone().invoke(context, Invocation::Fetch(request));
        }
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.0.create_cell(context, identity)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.0.validate_configuration(options)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.0.validate_deployment(deployment)
    }
}

#[cfg(test)]
mod tests {
    use super::{order_name, route_failure};

    #[test]
    fn route_failures_preserve_uncertainty_without_exposing_error_chains() {
        use cellar::cell_dispatch::RoutedRequestError;
        use cellar_logic::RequestError;
        for (error, code) in [
            (
                anyhow::anyhow!("secret backend path or request body"),
                "Ambiguous",
            ),
            (
                anyhow::Error::new(cellar::sdk::ApplicationOverloaded),
                "Overloaded",
            ),
            (
                anyhow::Error::new(RoutedRequestError(RequestError::NodeFenced)),
                "OwnershipLost",
            ),
            (
                anyhow::Error::new(RoutedRequestError(RequestError::DurabilityUnproven)),
                "Ambiguous",
            ),
            (
                anyhow::Error::new(RoutedRequestError(RequestError::CapacityExhausted)),
                "Overloaded",
            ),
        ] {
            let response = route_failure(&error.context("private request details"));
            assert_eq!(response.status, 503);
            assert_eq!(
                response.headers,
                [("content-type".into(), "application/json".into())]
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
                serde_json::json!({"detail":"Cellar operation failed", "code":code, "outcome":"unknown"})
            );
            assert!(response.write_position.is_none() && response.observed_position.is_none());
        }
    }

    #[test]
    fn only_canonical_supported_order_routes_enter_cells_directly() {
        for method in ["GET", "POST", "PATCH"] {
            assert_eq!(
                order_name(method, "https://orders/orders/order_1-A?tag=x"),
                Some("order_1-A".into())
            );
        }
        for path in [
            "/health",
            "/docs",
            "/openapi.json",
            "/orders/",
            "/orders/a/",
            "/orders/a/b",
            "/orders/a.b",
            "/orders/%61",
            "/orders/%2f",
            "/orders/caf%C3%A9",
        ] {
            assert_eq!(
                order_name("GET", &format!("https://orders{path}")),
                None,
                "{path}"
            );
        }
        assert!(order_name("GET", &format!("https://orders/orders/{}", "a".repeat(64))).is_some());
        assert!(order_name("GET", &format!("https://orders/orders/{}", "a".repeat(65))).is_none());
        assert!(order_name("DELETE", "https://orders/orders/a").is_none());
    }
}
