// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "embed", feature = "node"))]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, Options, Source};
use std::{path::Path, sync::Arc, time::Duration};
#[allow(dead_code)]
#[path = "support/native.rs"]
mod native;

fn options() -> Options {
    Options {
        turn_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

fn source() -> Source {
    let code = include_str!("support/synchronous.mjs").replace("export ", "");
    Source::new(
        format!("var CellarApp = (() => {{{code}; return {{cells, fetch}}; }})();"),
        "synchronous.mjs",
    )
    .events([EventKind::Fetch])
    .cell("Counter")
}

fn exercise(path: &Path, app: Arc<dyn Application>, first: u64) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let (node, address) = native::start_configured(path, app, |config, _, _| {
            config.runtime.handler_budget = Duration::from_secs(5);
        }).await?;
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
        for (name, expected) in [("alice", first), ("bob", first.div_ceil(2)), ("alice", first + 1)] {
            let reply = client.get(format!("{address}/{name}")).send().await?;
            let status = reply.status();
            let body = reply.text().await?;
            ensure!(status.is_success() && body == expected.to_string(), "{name}: {status}: {body}");
        }
        let result = node.shutdown(ShutdownMode::Preserve).await?;
        ensure!(matches!(result, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
        Ok(())
    })
}

#[cfg(feature = "embed")]
#[test]
fn embedded_synchronous_storage_survives_runtime_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    for first in [1, 3] {
        cellar_javascript::JavaScriptRuntime::run(options(), source(), |app| {
            exercise(directory.path(), app, first)
        })?;
    }
    Ok(())
}

#[cfg(feature = "node")]
#[test]
fn node_synchronous_storage_survives_runtime_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("synchronous.mjs");
    std::fs::write(&entry, include_str!("support/synchronous.mjs"))?;
    for first in [1, 3] {
        cellar_javascript::NodeRuntime::run(
            options(),
            cellar_javascript::NodeOptions::new(&entry),
            source(),
            |app| exercise(directory.path(), app, first),
        )?;
    }
    Ok(())
}
