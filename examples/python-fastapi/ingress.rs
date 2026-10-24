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

impl Application for Orders {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if let Invocation::Fetch(request) = event {
            if let Some(name) = order_name(&request.method, &request.url) {
                return Box::pin(async move {
                    Ok(Output::Http(
                        context
                            .namespace("ORDERS")?
                            .get_by_name(&name)
                            .fetch(&context, request)
                            .await?,
                    ))
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
    use super::order_name;

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
