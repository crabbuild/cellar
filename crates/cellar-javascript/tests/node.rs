#![cfg(feature = "embed")]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, JavaScriptRuntime, Options, Source};
use std::time::Duration;
#[path = "support/native.rs"]
mod native;
use native::start;

#[test]
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
                        let (node, address) = start(directory.path(), application).await?;
                        let client = reqwest::Client::new();
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
                        let reply = client.get(format!("{address}/alice")).send().await?;
                        ensure!(reply.status().is_success());
                        ensure!(
                            serde_json::from_str::<serde_json::Value>(&reply.text().await?)?
                                ["count"]
                                == expected
                        );
                        let reply = client.get(format!("{address}/bob")).send().await?;
                        ensure!(
                            serde_json::from_str::<serde_json::Value>(&reply.text().await?)?
                                ["count"]
                                == 0
                        );
                        // Zod validation must fail without modifying the cell.
                        ensure!(!client
                            .post(format!("{address}/alice"))
                            .body("101")
                            .send()
                            .await?
                            .status()
                            .is_success());
                        let reply = client.get(format!("{address}/alice")).send().await?;
                        ensure!(
                            serde_json::from_str::<serde_json::Value>(&reply.text().await?)?
                                ["count"]
                                == expected
                        );
                        let outcome = node.shutdown(ShutdownMode::Preserve).await?;
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
