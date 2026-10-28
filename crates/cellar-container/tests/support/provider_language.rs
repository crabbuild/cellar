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

/// Keep a real provider unavailable to maintenance so the guest can observe
/// local expiry before explicit destruction. No cloud protocol is substituted.
struct Unavailable(Arc<dyn containers::MaintenanceBackend>);

impl containers::MaintenanceBackend for Unavailable {
    fn identity(&self) -> containers::BackendIdentity {
        self.0.identity()
    }

    fn maintain(
        self: Arc<Self>,
        _: Context,
        _: containers::BindingCheckpoint,
        _: containers::MaintenanceAction,
    ) -> HandlerFuture<()> {
        Box::pin(async { anyhow::bail!("qualification backend unavailable") })
    }
}

pub async fn exercise(
    application: Arc<dyn Application>,
    peer: crate::peer::Peer,
    provider: &str,
    commands: usize,
) -> Result<()> {
    let mut supervision = containers::Supervision::default();
    for provider in [peer.provider()?, peer.provider_binding("backup")?] {
        supervision.register(
            Arc::new(Unavailable(provider.maintenance_backend().unwrap())),
            containers::MaintenancePolicy {
                inspect_interval_ms: 5000,
                maximum_lifetime_ms: 5000,
                attempt_timeout_ms: 1000,
                idle_timeout_ms: None,
            },
        )?;
    }
    let application = containers::supervise(application, supervision);
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
        for path in ["first", "second", "expired", "destroy"] {
            if path == "expired" {
                tokio::time::sleep(Duration::from_millis(5100)).await;
            }
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
            peer.state.creates.load(Ordering::SeqCst) == 2
                && peer.state.deletes.load(Ordering::SeqCst) == 2
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
