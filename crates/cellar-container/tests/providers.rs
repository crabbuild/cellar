// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Provider dispatch without a local container declaration or Docker daemon.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_container::{
    Adapter, Limits, Operation, Provider, ProviderFuture, ProviderSession, Session, Validation,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;

#[derive(Default)]
struct Probe {
    calls: AtomicUsize,
    dropped: AtomicUsize,
    retained: Mutex<Option<Arc<Session>>>,
}

struct Backend(Arc<Probe>);

impl Provider for Backend {
    fn name(&self) -> &str {
        "remote-fixture"
    }

    fn operations(&self) -> &[Operation] {
        &[Operation::Running, Operation::Start]
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Box::new(Backend(self.0.clone()))
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl ProviderSession for Backend {
    fn execute<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        _: &'a Limits,
    ) -> ProviderFuture<'a> {
        Box::pin(async move {
            ensure!(context.identity().is_some());
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            if operation == Operation::Running {
                return Ok(json!(true));
            }
            ensure!(context.storage()?.get_json("before-provider")? == Some(json!(true)));
            match input["options"]["entrypoint"][0].as_str() {
                Some("uncertain") => anyhow::bail!("response lost after submission"),
                Some("deep") => {
                    let mut value = Value::Null;
                    for _ in 0..65 {
                        value = json!([value]);
                    }
                    Ok(value)
                }
                _ => Ok(json!("remote-run")),
            }
        })
    }
}

struct Root {
    adapter: Adapter,
    probe: Arc<Probe>,
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let session = Arc::new(self.adapter.session());
            let limits = Limits {
                max_body_bytes: 1024,
            };
            let valid = if request.url.ends_with("/capabilities") {
                let value = session
                    .execute(&context, Operation::Capabilities, json!({}), &limits)
                    .await?;
                ensure!(
                    value["provider"] == "remote-fixture" && value["environment"] == "unspecified"
                );
                ensure!(
                    value["operations"]
                        == json!([
                            "container.capabilities",
                            "container.running",
                            "container.start"
                        ])
                );
                ensure!(value["max_body_bytes"] == 1024 && value["max_resources"].is_null());
                ensure!(value["synchronous_running"] == false);
                true
            } else if request.url.ends_with("/snapshot") {
                session
                    .running_snapshot(&context)
                    .unwrap_err()
                    .is::<Validation>()
            } else if request.url.ends_with("/open") {
                ensure!(
                    session
                        .execute(&context, Operation::Running, json!({}), &limits)
                        .await?
                        == json!(true)
                );
                context
                    .storage()?
                    .put_json("before-provider", &json!(true))?;
                ensure!(
                    session
                        .execute(&context, Operation::Start, json!({"options":{}}), &limits)
                        .await?
                        == json!("remote-run")
                );
                *self.probe.retained.lock().unwrap() = Some(session);
                true
            } else if request.url.ends_with("/retained") {
                let retained = self.probe.retained.lock().unwrap().clone().unwrap();
                retained
                    .execute(&context, Operation::Running, json!({}), &limits)
                    .await
                    .is_err()
            } else {
                let (op, input, refusal) = if request.url.ends_with("/unsupported") {
                    (Operation::Signal, json!({"signal":15}), true)
                } else if request.url.ends_with("/invalid") {
                    (Operation::Start, json!({"options":{"entrypoint":42}}), true)
                } else if request.url.ends_with("/uncertain") {
                    (
                        Operation::Start,
                        json!({"options":{"entrypoint":["uncertain"]}}),
                        false,
                    )
                } else {
                    (
                        Operation::Start,
                        json!({"options":{"entrypoint":["deep"]}}),
                        false,
                    )
                };
                let error = session
                    .execute(&context, op, input, &limits)
                    .await
                    .unwrap_err();
                error.is::<Validation>() == refusal
            };
            Ok(Output::Http(
                http::Response::from_json(&json!(valid))?.into_http(&context)?,
            ))
        })
    }
}

struct App(Arc<Root>);

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let response = context
                .namespace("COUNTERS")?
                .get_by_name("alice")
                .fetch(&context, request)
                .await?;
            Ok(Output::Http(response))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Counter"
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(self.0.clone())
    }
}

#[test]
fn custom_provider_preserves_authority_capabilities_and_failure_provenance() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let directory = tempfile::tempdir()?;
        let probe = Arc::new(Probe::default());
        let root = Arc::new(Root { adapter: Adapter::new(Arc::new(Backend(probe.clone()))), probe: probe.clone() });
        let (node, address) = native::start(directory.path(), Arc::new(App(root))).await?;
        let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build()?;
        for (path, calls) in [("snapshot",0), ("open",2), ("retained",2), ("unsupported",2), ("invalid",2), ("uncertain",3), ("deep",4)] {
            let response = client.get(format!("{address}/{path}")).send().await?;
            ensure!(response.status().is_success());
            ensure!(response.text().await? == "true", "{path}");
            ensure!(probe.calls.load(Ordering::SeqCst) == calls, "{path} reached provider unexpectedly");
        }
        probe.retained.lock().unwrap().take();
        let outcome = node.shutdown(ShutdownMode::Preserve).await?;
        ensure!(matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
        ensure!(probe.dropped.load(Ordering::SeqCst) >= 7, "session resources were retained");
        Ok(())
    })
}
