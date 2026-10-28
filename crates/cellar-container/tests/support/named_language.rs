// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The same real-node allocation contract for every language facade.
#![allow(dead_code, clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use std::{sync::Arc, time::Duration};
#[path = "../../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "native_engine.rs"]
mod peer;

pub fn exercise(application: Arc<dyn Application>) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let directory = tempfile::tempdir()?;
        let peer = peer::Peer::start().await?;
        let (node, address) = native::start_with_configuration(directory.path(), application,
            vec!["Counter".into(), "Sandbox".into()],
            vec![("COUNTERS".into(), "Counter".into()), ("SANDBOXES".into(), "Sandbox".into())],
            |config, services, graph| peer.configure(config, services, graph)).await?;
        let result = async {
            let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
            for path in ["outside", "start", "borrow", "destroy", "recreate", "retire"] {
                let response = client.get(format!("{address}/{path}")).send().await?;
                let status = response.status();
                let text = response.text().await?;
                ensure!(status.is_success() && text == "ok", "{path}: {status}: {text}");
            }
            tokio::time::timeout(Duration::from_secs(45), async {
                loop {
                    let response = client.get(format!("{address}/status")).send().await?;
                    ensure!(response.status().is_success());
                    if response.text().await? == "deleted" { break; }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok::<_, anyhow::Error>(())
            }).await??;
            ensure!(peer.engine.app_creates() == 3, "facade replayed a named allocation");
            ensure!(peer.engine.containers.lock().unwrap().is_empty(), "parent retirement leaked resources");
            Ok::<_, anyhow::Error>(())
        }.await;
        let outcome = node.shutdown(ShutdownMode::Preserve).await?;
        ensure!(matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained));
        peer.stop().await?;
        result
    })
}
