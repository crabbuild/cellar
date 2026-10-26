// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "node", feature = "embed"))]
use anyhow::{Context as _, Result};
use cellar::sdk::{NodeOutcome, ShutdownMode};
use cellar_javascript::{EventKind, Options, Source};
use std::sync::{Arc, Mutex};

// Both backends use the same native fixture identities and socket registry.
static HOST: Mutex<()> = Mutex::new(());
#[path = "support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/language_streaming.rs"]
mod streaming;

fn source() -> String {
    format!(
        "{}\n{}",
        include_str!("../../../packages/cellar-sdk/index.js").replace("export ", ""),
        include_str!("support/streaming.js")
    )
}

fn registration(source: Source) -> Source {
    source.events([EventKind::Fetch]).cell_events(
        "Counter",
        [
            EventKind::Fetch,
            EventKind::WebSocketOpen,
            EventKind::WebSocketMessage,
            EventKind::WebSocketClose,
        ],
    )
}

fn exercise(application: Arc<cellar_javascript::JavaScriptApplication>) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let directory = tempfile::tempdir()?;
            let (node, address) = native::start(directory.path(), application.clone()).await?;
            let result = streaming::check(&address).await;
            let usage = application.bridge_usage();
            // Join after a request failure too, and retain supervision evidence
            // without replacing the original network or handler failure.
            let outcome = node.shutdown(ShutdownMode::Preserve).await;
            result.with_context(|| format!("streaming bridge={usage:?} shutdown={outcome:?}"))?;
            let outcome = outcome?;
            let NodeOutcome::Stopped(report) = outcome else {
                anyhow::bail!("node failed")
            };
            anyhow::ensure!(
                report.application_work_drained && !report.drain_timed_out,
                "streaming work leaked: {report:?}"
            );
            Ok(())
        })
}

#[cfg(feature = "node")]
#[test]
// Test bootstrap writes an input module before starting any node execution.
#[allow(clippy::disallowed_methods)]
fn node_live_sse_and_websockets() -> Result<()> {
    let _host = HOST.lock().unwrap();
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    std::fs::write(&entry, format!("{}\nexport default app;", source()))?;
    cellar_javascript::NodeRuntime::run(
        Options {
            max_body_bytes: 4096,
            ..Default::default()
        },
        cellar_javascript::NodeOptions::new(entry),
        registration(Source::module()),
        exercise,
    )
}

#[cfg(feature = "embed")]
#[test]
fn embedded_live_sse_and_websockets() -> Result<()> {
    let _host = HOST.lock().unwrap();
    cellar_javascript::JavaScriptRuntime::run(
        Options {
            max_body_bytes: 4096,
            ..Default::default()
        },
        registration(Source::new(
            format!("{}\nvar CellarApp = app;", source()),
            "streaming.js",
        )),
        exercise,
    )
}
