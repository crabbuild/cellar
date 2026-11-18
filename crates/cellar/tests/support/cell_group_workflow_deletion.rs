// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public deletion must settle while cancelling an active Workflow generation.

use super::*;

pub(super) async fn exercise(
    cluster: &mut ManagedCluster,
    signals: &contract::Signals,
) -> Result<()> {
    for cycle in 0..8 {
        signals.hung_dropped.store(false, Ordering::SeqCst);
        call(cluster, "create", "delete-held").await?;
        cellar::asyncrt::timeout(TIMEOUT, signals.hung_started.notified()).await?;
        // No release is sent: only deletion can cancel this pending callback.
        // A fresh generation must not inherit a test-owned notification permit.
        call(cluster, "delete", "delete-held")
            .await
            .with_context(|| format!("active Workflow deletion cycle {cycle}"))?;
        cellar::asyncrt::timeout(TIMEOUT, async {
            while !signals.hung_dropped.load(Ordering::SeqCst) {
                cellar::asyncrt::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .context("deleted Workflow callback did not join")?;
        let absent = call(cluster, "status", "delete-held").await;
        ensure!(
            absent.is_err_and(|error| error.to_string().contains("instance does not exist")),
            "deleted Workflow did not report its absence"
        );
    }
    call(cluster, "create", "recovery").await?;
    state(cluster, "recovery", "waiting").await?;
    named_service::restart(cluster).await?;
    state(cluster, "recovery", "waiting").await?;
    call(cluster, "delete", "recovery").await?;
    call(cluster, "create", "recovery").await?;
    state(cluster, "recovery", "waiting").await?;
    ensure!(signals.first.load(Ordering::SeqCst) == 2);
    call(cluster, "event", "recovery").await?;
    state(cluster, "recovery", "complete").await?;
    ensure!(signals.last.load(Ordering::SeqCst) == 1);
    Ok(())
}
