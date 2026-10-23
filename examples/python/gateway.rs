//! Explicit native ingress for Python cells; ordinary HTTP stays in Python.
use anyhow::{ensure, Result};
use cellar::sdk::{
    http, Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use cellar_python::PythonApplication;
use std::sync::Arc;

pub(super) struct CounterGateway {
    python: Arc<PythonApplication>,
}

impl CounterGateway {
    pub(super) fn new(python: Arc<PythonApplication>) -> Self {
        Self { python }
    }
}

fn counter_binding(options: &cellar::engine_api::WorkerConfigOptions) -> Result<()> {
    ensure!(
        options
            .bindings
            .iter()
            .any(|(binding, class)| binding == "COUNTERS" && class == "Counter"),
        "native counter route requires COUNTERS bound to Counter"
    );
    Ok(())
}

fn counter_name(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/native-counters/")?;
    let name = rest.strip_suffix("/alarm").unwrap_or(rest);
    // A deliberately small public route grammar. Other paths remain with the
    // Python HTTP handler, including its original /counters/NAME route.
    (!name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then_some(name)
}

impl Application for CounterGateway {
    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.python.validate_configuration(options)?;
        counter_binding(options)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.python.validate_deployment(deployment)?;
        counter_binding(&deployment.options)
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if let Invocation::Fetch(request) = event {
            let name = http::Url::new(&request.url, None)
                .ok()
                .filter(|url| url.href() == request.url)
                .and_then(|url| counter_name(&url.pathname()).map(str::to_owned));
            if let Some(name) = name {
                return Box::pin(async move {
                    // Use the original invocation's namespace stub. The native
                    // host still owns admission, fencing, cancellation and the
                    // response durability proof; Python still owns cell logic.
                    Ok(Output::Http(
                        context
                            .namespace("COUNTERS")?
                            .get_by_name(&name)
                            .fetch(&context, request)
                            .await?,
                    ))
                });
            }
            return self
                .python
                .clone()
                .invoke(context, Invocation::Fetch(request));
        }
        self.python.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.python.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.python.create_cell(context, identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_routes_require_one_bounded_counter_name() {
        for (path, expected) in [
            ("/native-counters/demo", Some("demo")),
            ("/native-counters/demo/alarm", Some("demo")),
            ("/native-counters/a-B_2", Some("a-B_2")),
            ("/native-counters/", None),
            ("/native-counters//alarm", None),
            ("/native-counters/demo/other", None),
            ("/native-counters/demo/alarm/extra", None),
            ("/native-counters/a%2Fb", None),
            ("/counters/demo", None),
            ("/health", None),
        ] {
            assert_eq!(counter_name(path), expected, "{path}");
        }
        let longest = format!("/native-counters/{}", "a".repeat(128));
        assert!(counter_name(&longest).is_some());
        assert!(counter_name(&(longest + "a")).is_none());
    }
}
