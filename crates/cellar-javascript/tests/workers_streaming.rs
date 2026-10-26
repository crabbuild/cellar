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

fn fixture() -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/compat");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "fixture build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    Ok(root.join("dist/web-streams.mjs"))
}

#[test]
fn web_streams_forward_through_workers_and_objects_with_owned_cleanup() -> Result<()> {
    let directory = tempfile::tempdir()?;
    NodeRuntime::run(
        Options {
            max_body_bytes: 256,
            ..Default::default()
        },
        NodeOptions::new(fixture()?),
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
    let (node, address) =
        native::start_configured(path, application.clone(), |config, _, graph| {
            // The new stream fixtures intentionally await independent release events.
            config.runtime.handler_budget = Duration::from_secs(5);
            graph.primary.options.do_classes = vec!["Counter".into()];
            graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
        })
        .await?;
    let result = exercise(&address).await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(shutdown, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "stream work did not drain"
    );
    let usage = application.bridge_usage();
    ensure!(
        usage.active_invocations == 0 && usage.retained_bytes == 0,
        "stream admission did not release: {usage:?}"
    );
    result
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
    ensure!(
        response.status().as_u16() == 204 && response.bytes().await?.is_empty(),
        "release response"
    );
    Ok(())
}

async fn exercise(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let mut stream = client
        .get(format!("{address}/stream?token=phases"))
        .send()
        .await?
        .error_for_status()?;
    ensure!(stream.headers()["x-stream"] == "before-first-byte");
    ensure!(stream.headers().get_all("set-cookie").iter().count() == 2);
    ensure!(
        state(&client, address, "phases").await?["phase"] == 0,
        "headers waited for first body data"
    );
    release(&client, address, "phases", "first").await?;
    ensure!(stream.chunk().await?.context("first chunk")?.as_ref() == [0, 255, 42]);
    ensure!(state(&client, address, "phases").await?["phase"] == 1);
    release(&client, address, "phases", "second").await?;
    ensure!(stream.bytes().await?.as_ref() == [43, 0, 255]);
    ensure!(state(&client, address, "phases").await?["phase"] == 2);

    let large = client
        .get(format!("{address}/large"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    ensure!(
        large.len() == 6 * 128,
        "stream was capped by cumulative body size"
    );
    for (index, chunk) in large.as_chunks::<128>().0.iter().enumerate() {
        ensure!(chunk.iter().all(|byte| *byte == index as u8));
    }
    let empty = client.get(format!("{address}/empty")).send().await?;
    ensure!(empty.status().as_u16() == 204 && empty.bytes().await?.is_empty());

    for mode in ["error", "cancel", "finish"] {
        let mut stream = client
            .get(format!("{address}/{mode}?token={mode}"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            stream
                .chunk()
                .await?
                .context("initial stream bytes")?
                .as_ref()
                == [0, 255, 42]
        );
        if mode == "finish" {
            release(&client, address, mode, "second").await?;
            ensure!(
                stream.bytes().await?.as_ref() == [43, 0, 255],
                "clean EOF waited for independent background work"
            );
        } else if mode == "error" {
            release(&client, address, mode, "second").await?;
            ensure!(
                stream.bytes().await.is_err(),
                "producer failure became clean EOF"
            );
        } else {
            drop(stream);
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if state(&client, address, mode).await?["cancelled"] == 1 {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await??;
        }
        ensure!(
            state(&client, address, mode).await?["background"] == false,
            "unreleased background job completed"
        );
        release(&client, address, mode, "background").await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if state(&client, address, mode).await?["background"] == true {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
    }
    let oversized = client
        .get(format!("{address}/oversized?token=oversized"))
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        oversized.bytes().await.is_err(),
        "oversized chunk became a complete response"
    );
    ensure!(
        state(&client, address, "oversized").await?["phase"] == 1,
        "failed body erased a committed effect"
    );
    Ok(())
}
