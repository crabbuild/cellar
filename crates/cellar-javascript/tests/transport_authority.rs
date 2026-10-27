// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "node", feature = "embed"))]
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_guest::{transport::Session, Limits};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

#[path = "support/native.rs"]
mod native;

#[derive(Default)]
struct Probe {
    retained: Mutex<Option<(Arc<Session>, Value)>>,
    ready: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

struct Root(Arc<Probe>);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            if context.identity().unwrap().name.as_deref() == Some("peer") {
                return Ok(Output::Http(
                    http::Response::new(200, vec![0, 255, 42], vec![]).into_http(&context)?,
                ));
            }
            let limits = Limits {
                max_value_bytes: 4096,
                max_body_bytes: 64,
                max_batch_operations: 2,
            };
            if request.url.ends_with("/hold") {
                let session = Arc::new(Session::default());
                let head = session.execute(&context, "cell.fetch_stream", json!({
                    "binding":"COUNTERS", "name":"peer",
                    "request":{"url":"http://peer/", "method":"GET", "headers":[], "body":""},
                }), &limits).await?;
                *self.0.retained.lock().unwrap() = Some((session, head));
                self.0.ready.notify_one();
                self.0.release.notified().await;
            } else {
                let (session, head) = self.0.retained.lock().unwrap().as_ref().unwrap().clone();
                ensure!(
                    session
                        .execute(
                            &context,
                            "response.read",
                            json!({"id":head["forward"]}),
                            &limits
                        )
                        .await
                        .is_err(),
                    "foreign event borrowed a body reader"
                );
                ensure!(
                    session.output(&context, &head).is_err(),
                    "foreign event forwarded the body"
                );
                ensure!(
                    session
                        .execute(
                            &context,
                            "response.cancel",
                            json!({"id":head["forward"]}),
                            &limits
                        )
                        .await
                        .is_err(),
                    "foreign event cancelled the body"
                );
            }
            Ok(Output::Http(
                http::Response::new(200, b"ok".to_vec(), vec![]).into_http(&context)?,
            ))
        })
    }
}

struct App(Arc<Probe>);

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let name = if request.url.contains("/bob/") {
                "bob"
            } else {
                "alice"
            };
            Ok(Output::Http(
                context
                    .namespace("COUNTERS")?
                    .get_by_name(name)
                    .fetch(&context, request)
                    .await?,
            ))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Counter"
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(Root(self.0.clone())))
    }
}

#[test]
fn native_response_capabilities_refuse_foreign_live_and_later_events() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let directory = tempfile::tempdir()?;
        let probe = Arc::new(Probe::default());
        let (node, address) = native::start_configured(directory.path(), Arc::new(App(probe.clone())), |config, _, _| {
            config.runtime.handler_budget = std::time::Duration::from_secs(5);
        }).await?;
        let client = reqwest::Client::builder().timeout(std::time::Duration::from_secs(3)).build()?;
        let held_client = client.clone();
        let held_address = address.clone();
        let hold = tokio::spawn(async move { held_client.get(format!("{held_address}/alice/hold")).send().await });
        tokio::time::timeout(std::time::Duration::from_secs(2), probe.ready.notified()).await?;
        let foreign = client.get(format!("{address}/bob/check")).send().await?;
        ensure!(foreign.status().is_success() && foreign.text().await? == "ok");
        ensure!(!hold.is_finished(), "original event expired before foreign-event checks");
        probe.release.notify_one();
        ensure!(hold.await??.error_for_status()?.text().await? == "ok");
        let later = client.get(format!("{address}/alice/check")).send().await?;
        ensure!(later.status().is_success() && later.text().await? == "ok");
        probe.retained.lock().unwrap().take();
        let shutdown = node.shutdown(ShutdownMode::Preserve).await?;
        ensure!(matches!(shutdown, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
        Ok(())
    })
}
