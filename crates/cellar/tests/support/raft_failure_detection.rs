// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real TLS connectivity, joined failed boot, unchanged catalog authority.
use super::*;

async fn wait_for(
    f: &RepairFleet,
    service: &FleetMovementService,
    ready: impl Fn(&FleetMovementServiceStatus) -> bool,
) -> Result<()> {
    f.s.f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
            loop {
                let status = service.status();
                if ready(&status) {
                    return Ok::<_, anyhow::Error>(());
                }
                ensure!(status.running, "failure monitor stopped: {status:?}");
                cellar::asyncrt::sleep(Duration::from_millis(20)).await;
            }
        }))
        .await??
        .with_context(|| format!("failure monitor: {:?}", service.status()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_detection_classifies_closed_boot_without_isolation() -> Result<()> {
    let f = Box::pin(RepairFleet::with_isolation(false, false)).await?;
    let inputs = f.evidence().await?;
    let before = f.s.f.catalog.read(TIMEOUT).await?;
    let service =
        f.s.f
            .catalog
            .start_failure_detection_service(
                f.spare.clone(),
                inputs.clone(),
                options(),
                FleetFailureDetectionOptions::default(),
            )
            .await?;
    ensure!(matches!(
        f.s.f
            .catalog
            .start_movement_service(f.spare.clone(), inputs.clone(), options())
            .await,
        Err(FleetCatalogError::Overloaded)
    ));
    wait_for(&f, &service, |s| {
        s.failure_detection.as_ref().is_some_and(|d| {
            d.nodes
                .iter()
                .any(|n| n.boot == f.failed_boot && n.health == FleetNodeHealth::Unavailable)
                && d.nodes
                    .iter()
                    .filter(|n| n.boot != f.failed_boot)
                    .all(|n| n.health == FleetNodeHealth::Healthy)
        })
    })
    .await?;
    let status = service
        .status()
        .failure_detection
        .context("failure observations")?;
    let failed = status
        .nodes
        .iter()
        .find(|n| n.boot == f.failed_boot)
        .context("failed boot")?;
    ensure!(
        failed.consecutive_failures >= 2 && failed.last_error == Some(InspectionError::Unreachable)
    );
    let after = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(after.revision() == before.revision());
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&after, TIMEOUT)
        .await?
        .is_empty());
    ensure!(inputs.calls.load(Ordering::Acquire) == 0);
    service.stop();
    wait_for(&f, &service, |s| !s.running).await?;
    ensure!(service
        .status()
        .failure_detection
        .context("stopped observations")?
        .nodes
        .iter()
        .all(|n| n.health == FleetNodeHealth::Unknown));
    service.shutdown().await;
    f.close().await
}

struct HeldReads(Arc<crate::fault::Store>);

impl Drop for HeldReads {
    fn drop(&mut self) {
        self.0.hold_fleet_reads.store(false, Ordering::Release);
        self.0.fleet_read_hang.store(false, Ordering::Release);
        self.0.fleet_read_resume.notify_waiters();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_detection_metadata_timeout_is_unknown_and_shutdown_joins() -> Result<()> {
    let f = Box::pin(RepairFleet::with_isolation(false, false)).await?;
    let service =
        f.s.f
            .catalog
            .start_failure_detection_service(
                f.spare.clone(),
                f.evidence().await?,
                options(),
                FleetFailureDetectionOptions::default(),
            )
            .await?;
    wait_for(&f, &service, |s| {
        s.failure_detection
            .as_ref()
            .is_some_and(|d| d.nodes.iter().any(|n| n.health == FleetNodeHealth::Healthy))
    })
    .await?;
    let held = HeldReads(f.s.f.store.clone());
    held.0.fleet_read_hang.store(true, Ordering::Release);
    held.0.hold_fleet_reads.store(true, Ordering::Release);
    wait_for(&f, &service, |s| {
        s.last_round_failure.is_some()
            && s.failure_detection.as_ref().is_some_and(|d| {
                !d.nodes.is_empty()
                    && d.nodes.iter().all(|n| {
                        n.health == FleetNodeHealth::Unknown && n.consecutive_failures == 0
                    })
            })
    })
    .await?;
    service.shutdown().await;
    drop(held);
    f.close().await
}
