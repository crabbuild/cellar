// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "node", feature = "embed"))]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, Options, Source};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
#[allow(dead_code)]
#[path = "support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/container_peer.rs"]
mod peer;

fn source(provider: Arc<peer::AuditedProvider>) -> Source {
    let code = include_str!("support/containers.mjs").replace("export ", "");
    Source::new(
        format!("var CellarApp = (() => {{{code}; return {{cells, fetch}}; }})();"),
        "containers.mjs",
    )
    .events([EventKind::Fetch])
    .cell("Counter")
    .container_provider(provider)
}

fn exercise(application: Arc<dyn Application>, provider: Arc<peer::AuditedProvider>) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async move {
        tokio::time::timeout(Duration::from_secs(45), async move {
            let directory = tempfile::tempdir()?;
            let peer = peer::Peer::start().await?;
            let (node, address) = native::start_configured(directory.path(), application,
                |config, services, graph| peer.configure(config, services, graph)).await?;
            let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
            for path in ["all", "borrow", "leak", "cancel"] {
                let started = tokio::time::Instant::now();
                let response = client.get(format!("{address}/{path}")).send().await?;
                let status = response.status();
                let body = response.text().await?;
                if path == "cancel" {
                    ensure!(!status.is_success(), "pending receive must expire");
                    ensure!(started.elapsed() >= Duration::from_secs(9), "receive failed before the configured handler deadline");
                } else {
                    ensure!(status.is_success() && body == "ok", "{path}: {status}: {body}");
                }
                tokio::time::timeout(Duration::from_secs(3), async {
                    while peer.websockets.load(Ordering::SeqCst) != 0 { tokio::time::sleep(Duration::from_millis(10)).await; }
                }).await?;
            }
            let outcome = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
            peer.check_options();
            ensure!(provider.calls.load(Ordering::SeqCst) > 20);
            ensure!(provider.sessions.load(Ordering::SeqCst) > 1);
            ensure!(peer.engine.containers.lock().unwrap().is_empty());
            Ok(())
        }).await?
    })
}

#[cfg(feature = "node")]
#[test]
fn node_container_capabilities() -> Result<()> {
    use cellar_javascript::{NodeOptions, NodeRuntime};
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("containers.mjs");
    std::fs::write(&entry, include_str!("support/containers.mjs"))?;
    let provider = Arc::new(peer::AuditedProvider::default());
    NodeRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        NodeOptions::new(entry),
        source(provider.clone()),
        |application| exercise(application, provider),
    )
}

#[cfg(feature = "embed")]
#[test]
fn embedded_container_capabilities() -> Result<()> {
    let provider = Arc::new(peer::AuditedProvider::default());
    cellar_javascript::JavaScriptRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        source(provider.clone()),
        |application| exercise(application, provider),
    )
}
