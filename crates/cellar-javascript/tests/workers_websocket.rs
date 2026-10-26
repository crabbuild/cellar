// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, NodeOptions, NodeRuntime, Options, Source};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};

#[path = "support/native.rs"]
mod native;

#[path = "../../cellar/tests/support/container_peer.rs"]
mod peer;

#[test]
fn workers_port_websockets_keep_original_event_io_and_caller_proof() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/compat");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "fixture build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    let directory = tempfile::tempdir()?;
    NodeRuntime::run(
        Options {
            max_body_bytes: 2048,
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/web-websocket.mjs")),
        Source::module()
            .workers_lifetimes()
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::CellRpc]),
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(run(directory.path(), application))
        },
    )
}

async fn run(
    path: &std::path::Path,
    application: Arc<cellar_javascript::JavaScriptApplication>,
) -> Result<()> {
    let peer = peer::Peer::start_with_http_port(8080).await?;
    let (node, address) =
        native::start_configured(path, application.clone(), |config, services, graph| {
            peer.configure(config, services, graph)
        })
        .await?;
    let result = exercise(&address).await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(shutdown, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "input work did not drain"
    );
    let usage = application.bridge_usage();
    ensure!(
        usage.active_invocations == 0 && usage.retained_bytes == 0,
        "input resources leaked: {usage:?}"
    );
    ensure!(
        peer.websockets.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "socket transport leaked"
    );
    result
}

async fn call(client: &reqwest::Client, address: &str, route: &str) -> Result<Value> {
    let response = client.get(format!("{address}/{route}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(status.is_success(), "{route}: {status}: {text}");
    Ok(serde_json::from_str(&text)?)
}

async fn exercise(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let open = call(&client, address, "open").await?;
    ensure!(
        open["status"] == 101 && open["upgrade"] == "websocket",
        "opening headers: {open}"
    );
    call(&client, address, "send?value=first").await?;
    call(&client, address, "send?value=second").await?;
    call(&client, address, "binary").await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = call(&client, address, "state").await?;
            if state["received"] == 3 {
                ensure!(
                    state["messages"] == serde_json::json!(["first", "second", [0, 255, 42]]),
                    "socket order: {state}"
                );
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("socket message deadline")??;
    let foreign = call(&client, address, "borrow?owner=foreign").await?;
    ensure!(
        foreign["code"] == "ContextMismatch",
        "foreign activation: {foreign}"
    );
    call(&client, address, "close").await?;
    for owner in ["socket-owner", "unused-owner"] {
        if owner == "unused-owner" {
            call(&client, address, "unused?owner=unused-owner").await?;
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if call(&client, address, &format!("state?owner={owner}")).await?["ready"] == 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("socket close deadline")??;
    }
    Ok(())
}
