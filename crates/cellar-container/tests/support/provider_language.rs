// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared acceptance assertions for each actual language runtime.
#![allow(dead_code, clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
#[path = "../../../cellar-javascript/tests/support/native.rs"]
mod native;

pub async fn exercise(
    application: Arc<dyn Application>,
    peer: crate::peer::Peer,
    provider: &str,
    commands: usize,
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (node, address) =
        native::start_configured(directory.path(), application, |config, _, _| {
            config.runtime.handler_budget = Duration::from_secs(5);
        })
        .await?;
    let result = async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        for path in ["first", "second", "destroy"] {
            let response = client
                .get(format!("{address}/{provider}/{path}"))
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            ensure!(
                status.is_success() && body == "ok",
                "{path}: {status}: {body}"
            );
        }
        ensure!(
            peer.state.creates.load(Ordering::SeqCst) == 1
                && peer.state.deletes.load(Ordering::SeqCst) == 1
        );
        ensure!(peer.state.commands.load(Ordering::SeqCst) == commands);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(outcome,NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out)
    );
    peer.stop().await?;
    result
}
