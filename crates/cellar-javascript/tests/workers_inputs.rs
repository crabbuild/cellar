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
fn rpc_stream_arguments_transfer_incrementally_and_join_cleanup() -> Result<()> {
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
        NodeOptions::new(root.join("dist/web-inputs.mjs")),
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
    let (node, address) = native::start_configured(path, application.clone(), |config, _, _| {
        // The new test holds input/close acknowledgements for independent requests.
        config.runtime.handler_budget = Duration::from_secs(10);
    })
    .await?;
    let result = exercise(&address).await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    let usage = application.bridge_usage();
    result.with_context(|| format!("workers_inputs: bridge={usage:?}, shutdown={shutdown:?}"))?;
    let shutdown = shutdown?;
    ensure!(
        matches!(shutdown, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "input work did not drain"
    );
    ensure!(
        usage.active_invocations == 0 && usage.retained_bytes == 0,
        "input resources leaked: {usage:?}"
    );
    Ok(())
}

async fn exercise(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    for mode in ["read", "write"] {
        let request = client.get(format!("{address}/{mode}?token={mode}")).send();
        let task = tokio::spawn(request);
        let observation = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if task.is_finished() {
                    anyhow::bail!("input request settled before first observation");
                }
                let state: Value = client
                    .get(format!("{address}/state?token={mode}"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if mode == "read" && state["remote"].as_u64().is_some_and(|n| n > 0)
                    || mode == "write" && state["remote"] == "writing"
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if observation.as_ref().map_or(true, |result| result.is_err()) {
            if task.is_finished() {
                let response = task.await??;
                let status = response.status();
                anyhow::bail!(
                    "input {mode} failed before release: {status}: {}",
                    response.text().await?
                );
            }
            observation.context("incremental input was not observed before release")??;
        }
        ensure!(
            !task.is_finished(),
            "input finished before independent release"
        );
        client
            .get(format!("{address}/release?token={mode}"))
            .send()
            .await?
            .error_for_status()?;
        let response = task.await??;
        let status = response.status();
        let bytes = response.bytes().await?;
        ensure!(
            status.is_success(),
            "input {mode}: {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        let value: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            value["locked"] == true,
            "source ownership was not transferred: {value}"
        );
        if mode == "read" {
            ensure!(
                value["total"] == 4096 && value["valid"] == true,
                "read: {value}"
            );
        } else {
            ensure!(
                value["bytes"] == 4096 && value["closed"] == true,
                "write: {value}"
            );
        }
    }
    let echo = client
        .get(format!("{address}/echo?token=echo"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    ensure!(
        echo.len() == 4096 && echo.iter().enumerate().all(|(i, b)| *b == (i % 256) as u8),
        "returned input stream changed bytes"
    );
    for mode in ["defer", "forward", "delegate"] {
        let result: Value = client
            .get(format!("{address}/{mode}?token={mode}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            result["total"] == 4096 && result["valid"] == true,
            "{mode}: {result}"
        );
    }
    let cancel: Value = client
        .get(format!("{address}/cancel?token=cancel"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        cancel["done"] == true && cancel["reason"] == "remote cancelled",
        "cancel: {cancel}"
    );
    client
        .get(format!("{address}/unused?token=unused"))
        .send()
        .await?
        .error_for_status()?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state: Value = client
                .get(format!("{address}/state?token=unused"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if state["local"]["cancelled"] == true {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("unused input retained its caller")??;
    for mode in ["quota", "callback"] {
        let value: Value = client
            .get(format!("{address}/{mode}?token={mode}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            value["refused"] == true && value["locked"] == false,
            "failed admission kept ownership: {value}"
        );
        if mode == "quota" {
            ensure!(
                value["code"] == "ValidationError",
                "not a native quota refusal: {value}"
            );
        }
    }
    let calls: Value = client
        .get(format!("{address}/state?token=unused-calls"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        calls["remote"] == 1,
        "refused admission entered the user method: {calls}"
    );
    let failure: Value = client
        .get(format!("{address}/source-error?token=source-error"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        failure["message"] == "deliberate upload failure" && failure["prefix"] == 4096,
        "source error lost accepted prefix: {failure}"
    );
    let error_stack: Value = client
        .get(format!("{address}/state?token=input-error-stack"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        error_stack["remote"].is_null(),
        "input failure exposed a producer stack: {error_stack}"
    );
    Ok(())
}
