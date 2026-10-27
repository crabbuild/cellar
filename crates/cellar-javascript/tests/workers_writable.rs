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

#[test]
fn rpc_writable_streams_retain_producer_authority_backpressure_and_cleanup() -> Result<()> {
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
    let application = NodeRuntime::run(
        Options {
            max_body_bytes: 1024,
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/web-writable.mjs")),
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
    )?;
    // Native node drain retires cell authority. The still-running engine may
    // briefly own cancellation/retirement frames at that point. Its final byte
    // reservation census belongs after NodeRuntime::run has closed and joined
    // the transport, not to a concurrent sample of the live engine.
    let usage = application.bridge_usage();
    ensure!(
        usage.stopped && usage.active_invocations == 0 && usage.retained_bytes == 0,
        "joined writer runtime retained resources: {usage:?}"
    );
    Ok(())
}

async fn run(
    path: &std::path::Path,
    application: Arc<cellar_javascript::JavaScriptApplication>,
) -> Result<Arc<cellar_javascript::JavaScriptApplication>> {
    let (node, address) = native::start_configured(path, application.clone(), |config, _, _| {
        // The new test holds writer/close acknowledgements for independent requests.
        config.runtime.handler_budget = Duration::from_secs(10);
    })
    .await?;
    let result = exercise(&address).await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    let usage = application.bridge_usage();
    result.with_context(|| format!("workers_writable: bridge={usage:?}, shutdown={shutdown:?}"))?;
    let shutdown = shutdown?;
    ensure!(
        matches!(shutdown, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "writer work did not drain"
    );
    ensure!(
        usage.active_invocations == 0,
        "writer invocations did not drain: {usage:?}"
    );
    Ok(application)
}

async fn state(client: &reqwest::Client, address: &str, token: &str) -> Result<Value> {
    Ok(client
        .get(format!("{address}/state"))
        .query(&[("token", token)])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn release(client: &reqwest::Client, address: &str, token: &str, gate: &str) -> Result<()> {
    let response = client
        .get(format!("{address}/release"))
        .query(&[("token", token), ("gate", gate)])
        .send()
        .await?;
    ensure!(response.status().as_u16() == 204 && response.bytes().await?.is_empty());
    Ok(())
}

async fn observe(
    client: &reqwest::Client,
    address: &str,
    token: &str,
    field: &str,
) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let value = state(client, address, token).await?;
            if value[field] == true {
                return Ok(value);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("writer observation deadline")?
}

async fn exercise(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let duplex: Value = client
        .get(format!("{address}/duplex"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        duplex["bytes"] == 4096 && duplex["valid"] == true,
        "mixed stream result lost bytes or aliases: {duplex}"
    );
    let request_client = client.clone();
    let request_address = address.to_owned();
    let pending = tokio::spawn(async move {
        request_client
            .get(format!("{request_address}/gated?token=gated"))
            .send()
            .await
    });
    let first = observe(&client, address, "gated", "writing").await?;
    ensure!(
        first["bytes"] == 0 && first["locked"] == true && !pending.is_finished(),
        "writer acknowledged before the sink accepted bytes"
    );
    release(&client, address, "gated", "write").await?;
    let closing = observe(&client, address, "gated", "closing").await?;
    ensure!(
        closing["bytes"] == 4096
            && closing["valid"] == true
            && closing["closed"] == false
            && !pending.is_finished(),
        "close acknowledged before the sink completed"
    );
    release(&client, address, "gated", "close").await?;
    let completed: Value = pending.await??.error_for_status()?.json().await?;
    ensure!(completed["closed"] == true && completed["background"] == false);
    release(&client, address, "gated", "background").await?;
    observe(&client, address, "gated", "background").await?;
    for mode in ["abort", "bad-reason", "error"] {
        let value: Value = client
            .get(format!("{address}/{mode}?token={mode}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(value["closed"] == false && value["locked"] == true);
        if mode == "error" {
            ensure!(
                value["bytes"] == 608 && value["valid"] == true,
                "failed write erased or repeated the accepted prefix: {value}"
            );
        } else {
            ensure!(value["aborted"] == true && value["bytes"] == 0);
        }
        if mode == "abort" {
            ensure!(value["reason"] == "caller left");
        }
    }
    let abandoned = client
        .get(format!("{address}/abandon?token=abandon"))
        .send()
        .await?;
    ensure!(abandoned.status().as_u16() == 204 && abandoned.bytes().await?.is_empty());
    observe(&client, address, "abandon", "aborted").await?;
    let stale: Value = client
        .get(format!("{address}/stale"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        matches!(
            stale["code"].as_str(),
            Some("ContextExpired" | "ContextMismatch")
        ),
        "later event admitted an expired writer: {stale}"
    );
    ensure!(state(&client, address, "abandon").await?["bytes"] == 0);
    Ok(())
}
