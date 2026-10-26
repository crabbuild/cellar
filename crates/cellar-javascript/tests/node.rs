// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "embed")]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, JavaScriptRuntime, Options, Source};
use std::time::{Duration, Instant};
#[path = "support/native.rs"]
mod native;
use native::start;

#[test]
#[allow(clippy::disallowed_methods)] // Observe startup wall time in this host fixture.
fn typescript_npm_routing_storage_and_restart() -> Result<()> {
    let source = Source::new(
        include_str!("../../../examples/javascript/app.bundle.js"),
        "app.ts",
    )
    .events([EventKind::Fetch])
    .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]);
    let directory = tempfile::tempdir()?;
    // A fresh V8 runtime and activation after restart must read durable SQLite.
    for expected in [3, 6] {
        JavaScriptRuntime::run(
            Options {
                turn_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            source.clone(),
            |application| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(async {
                        let starting = Instant::now();
                        let (node, address) = start(directory.path(), application).await?;
                        let startup_ms = starting.elapsed().as_millis();
                        let building_client = Instant::now();
                        let client = reqwest::Client::new();
                        let client_ms = building_client.elapsed().as_millis();
                        let mut phase = "increment";
                        let result = async {
                            let reply = client
                                .post(format!("{address}/alice"))
                                .body("3")
                                .send()
                                .await?;
                            let status = reply.status();
                            let body = reply.text().await?;
                            ensure!(status.is_success(), "request failed: {status}: {body}");
                            ensure!(
                                serde_json::from_str::<serde_json::Value>(&body)?["count"] == expected
                            );
                            phase = "read incremented cell";
                            let reply = client.get(format!("{address}/alice")).send().await?;
                            ensure!(reply.status().is_success());
                            ensure!(
                                serde_json::from_str::<serde_json::Value>(&reply.text().await?)?
                                    ["count"]
                                    == expected
                            );
                            phase = "read distinct cell";
                            let reply = client.get(format!("{address}/bob")).send().await?;
                            ensure!(
                                serde_json::from_str::<serde_json::Value>(&reply.text().await?)?
                                    ["count"]
                                    == 0
                            );
                            // Zod validation must fail without modifying the cell.
                            phase = "reject invalid increment";
                            ensure!(!client
                                .post(format!("{address}/alice"))
                                .body("101")
                                .send()
                                .await?
                                .status()
                                .is_success());
                            phase = "read after refusal";
                            let reply = client.get(format!("{address}/alice")).send().await?;
                            ensure!(
                                serde_json::from_str::<serde_json::Value>(&reply.text().await?)?
                                    ["count"]
                                    == expected
                            );
                            Ok::<_, anyhow::Error>(())
                        }
                        .await;
                        // Join after a request error too. Losing the listener can
                        // mean fencing; preserve that outcome with the primary
                        // error instead of dropping its supervision evidence.
                        let outcome = node.shutdown(ShutdownMode::Preserve).await;
                        result.with_context(|| format!(
                            "V8 HTTP/restart expected={expected} phase={phase} startup_ms={startup_ms} client_ms={client_ms} outcome={outcome:?}"
                        ))?;
                        let outcome = outcome?;
                        let NodeOutcome::Stopped(report) = outcome else {
                            anyhow::bail!("node failed on shutdown")
                        };
                        ensure!(
                            report.ingress_drained
                                && report.application_work_drained
                                && report.cells_drained
                                && !report.drain_timed_out
                        );
                        Ok(())
                    })
            },
        )?;
    }
    Ok(())
}
