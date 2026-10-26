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
fn rpc_responses_preserve_metadata_streaming_authority_and_cleanup() -> Result<()> {
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
        NodeOptions::new(root.join("dist/web-messages.mjs")),
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
            config.runtime.handler_budget = Duration::from_secs(5);
            graph.primary.options.do_classes = vec!["Counter".into()];
            graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
        })
        .await?;
    let result = exercise(&address).await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    let usage = application.bridge_usage();
    result.with_context(|| format!("workers_messages: bridge={usage:?}, shutdown={shutdown:?}"))?;
    let shutdown = shutdown?;
    ensure!(
        matches!(shutdown, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "RPC response work did not drain"
    );
    ensure!(
        usage.active_invocations == 0 && usage.retained_bytes == 0,
        "RPC response resources leaked: {usage:?}"
    );
    Ok(())
}

async fn state(client: &reqwest::Client, address: &str, token: &str) -> Result<Value> {
    Ok(client
        .get(format!("{address}/state?token={token}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn exercise(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let metadata: Value = client
        .get(format!("{address}/metadata"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        metadata
            == serde_json::json!({"response":true,"alias":true,"status":204,"header":"yes","empty":true,"error":"error","errorStatus":0}),
        "response metadata: {metadata}"
    );
    for mode in ["download", "error", "cancel"] {
        let mut response = client
            .get(format!("{address}/{mode}?token={mode}"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(response.status().as_u16() == 206 && response.headers()["x-download"] == mode);
        ensure!(response.headers().get_all("set-cookie").iter().count() == 2);
        let prefix = response.chunk().await?.context("missing download prefix")?;
        ensure!(
            prefix.as_ref() == [1_u8; 1024],
            "download buffered or changed the first chunk"
        );
        ensure!(
            state(&client, address, mode).await? == 1,
            "producer passed the unreleased gate"
        );
        if mode == "cancel" {
            drop(response);
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if state(&client, address, mode).await? == "cancelled" {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await??;
        } else {
            client
                .get(format!("{address}/release?token={mode}"))
                .send()
                .await?
                .error_for_status()?;
            if mode == "error" {
                ensure!(
                    response.bytes().await.is_err(),
                    "source failure became clean EOF"
                );
            } else {
                let bytes = response.bytes().await?;
                ensure!(
                    bytes.len() == 2048
                        && bytes[..1024] == [2_u8; 1024]
                        && bytes[1024..] == [3_u8; 1024]
                );
                ensure!(state(&client, address, mode).await? == "finished");
            }
        }
    }
    client
        .get(format!("{address}/unused?token=unused"))
        .send()
        .await?
        .error_for_status()?;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state(&client, address, "unused").await? == "cancelled" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

#[test]
fn rpc_requests_and_http_arguments_keep_body_signal_and_authority() -> Result<()> {
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
            max_body_bytes: 8192,
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/http-rpc.mjs")),
        Source::module()
            .workers_lifetimes()
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::CellRpc]),
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let (node, address) = native::start_configured(
                        directory.path(),
                        application.clone(),
                        |config, _, _| {
                            // This fixture independently gates upload completion
                            // and abort; existing budgets remain unchanged.
                            config.runtime.handler_budget = Duration::from_secs(10);
                        },
                    )
                    .await?;
                    let result = exercise_http_arguments(&address).await;
                    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
                    let usage = application.bridge_usage();
                    result.with_context(|| {
                        format!("HTTP RPC bridge={usage:?}, shutdown={shutdown:?}")
                    })?;
                    ensure!(
                        matches!(shutdown?, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
                        "HTTP RPC work did not drain"
                    );
                    ensure!(
                        usage.active_invocations == 0 && usage.retained_bytes == 0,
                        "HTTP RPC resources leaked: {usage:?}"
                    );
                    Ok(())
                })
        },
    )
}

async fn exercise_http_arguments(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let refused: Value = client
        .get(format!("{address}/refuse"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        refused == serde_json::json!({"refused":true,"entered":null}),
        "aborted request entered the callee: {refused}"
    );
    let unused: Value = client
        .get(format!("{address}/unused"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        unused == serde_json::json!({"request":true,"method":"PUT"}),
        "unconsumed Request prevented response completion: {unused}"
    );
    let roundtrip: Value = client
        .get(format!("{address}/roundtrip"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        roundtrip == serde_json::json!({"body":"returned body","method":"PUT"}),
        "returned Request could not be forwarded as an argument: {roundtrip}"
    );
    let upload = tokio::spawn(client.get(format!("{address}/upload?token=input")).send());
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if state(&client, address, "input")
                .await?
                .as_u64()
                .is_some_and(|n| n > 0)
            {
                break;
            }
            ensure!(
                !upload.is_finished(),
                "HTTP input settled before its prefix was observed"
            );
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("HTTP argument prefix deadline")??;
    ensure!(!upload.is_finished(), "HTTP upload ignored its source gate");
    client
        .get(format!("{address}/release?token=input"))
        .send()
        .await?
        .error_for_status()?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while state(&client, address, "input").await? != "uploaded" {
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("HTTP argument EOF deadline")??;
    ensure!(
        !upload.is_finished(),
        "EOF discarded the request abort channel"
    );
    client
        .get(format!("{address}/abort?token=input"))
        .send()
        .await?
        .error_for_status()?;
    let response = upload.await??;
    let status = response.status();
    let bytes = response.bytes().await?;
    ensure!(
        status.is_success(),
        "HTTP upload {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    let result: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        result
            == serde_json::json!({"count":32785,"valid":true,"method":"POST","url":"https://service.test/upload","header":"binary","responseStatus":206,"responseText":"response argument","reason":"client abort"}),
        "HTTP argument round-trip: {result}"
    );
    let returned = tokio::spawn(client.get(format!("{address}/returned")).send());
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let ready: bool = client
                .get(format!("{address}/returned-state"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if ready {
                break;
            }
            ensure!(
                !returned.is_finished(),
                "returned Request settled before observation"
            );
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("returned Request EOF deadline")??;
    client
        .get(format!("{address}/abort-returned"))
        .send()
        .await?
        .error_for_status()?;
    let result: Value = returned.await??.error_for_status()?.json().await?;
    ensure!(
        result
            == serde_json::json!({"body":"returned body","method":"PUT","header":"request","reason":"producer abort","proof":"committed"}),
        "returned Request round-trip: {result}"
    );
    Ok(())
}
