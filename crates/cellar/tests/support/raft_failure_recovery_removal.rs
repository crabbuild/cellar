// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Automatic recovery must complete the existing terminal proof boundary.
use super::*;

async fn failed_node(f: &RepairFleet) -> Result<serde_json::Value> {
    // Poll through an independent adapter. The running nodes use the fixture's
    // original client for trust refresh and actual membership observations.
    let catalog = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    let bytes = catalog
        .get(
            &cut,
            FleetTable::Nodes,
            &hex::encode(f.failed.bytes()),
            TIMEOUT,
        )
        .await?
        .context("retained failed node")?;
    Ok(serde_json::from_slice(&bytes[13..bytes.len() - 32])?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_recovery_completes_terminal_removal_and_releases_disruption() -> Result<()> {
    Box::pin(removes()).await
}

async fn removes() -> Result<()> {
    let mut f = Box::pin(RepairFleet::with_admission(false, None, 1))
        .await
        .context("create removal fleet")?;
    let platform = Platform::new(&f)?;
    platform.mode.store(2, Ordering::Release);
    let service = start(&f, &f.spare, f.evidence().await?, platform.clone())
        .await
        .context("start removal service")?;
    f.wait(&service, |s| s.settled_movements == 1).await?;
    let result =
        f.s.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
                loop {
                    if failed_node(&f)
                        .await
                        .context("poll failed node during automatic removal")?["state"]
                        == "Removed"
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                }
            }))
            .await?;
    let status = service.status();
    service.shutdown().await;
    result
        .with_context(|| format!("automatic removal did not complete: {status:?}"))?
        .with_context(|| format!("automatic removal observation failed: {status:?}"))?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .get(&cut, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&cut, TIMEOUT)
        .await?
        .is_empty());
    ensure!(platform.scratch.usage()?.generations == 0);
    let context = platform
        .contexts
        .lock()
        .unwrap()
        .last()
        .cloned()
        .context("failure operation")?;
    let receipt =
        f.s.f
            .catalog
            .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
            .await?
            .context("terminal removal receipt")?;
    ensure!(receipt.node() == f.failed.bytes() && receipt.applied_revision() <= cut.revision());
    ensure!(matches!(
        f.s.f
            .catalog
            .removal_receipt(&cut, context.operation(), TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    f.verify(0).await.context("SQL after terminal removal")?;
    retained_engine(&f.s.f, f.old_engine.clone()).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_recovery_resumes_finalizing_after_lost_replies_and_controller_loss(
) -> Result<()> {
    Box::pin(resumes_finalizing()).await
}

async fn resumes_finalizing() -> Result<()> {
    let mut f = Box::pin(RepairFleet::with_admission(false, None, 2)).await?;
    let platform = Platform::new(&f)?;
    platform.mode.store(2, Ordering::Release);
    platform.hold_finalizing.store(true, Ordering::Release);
    platform
        .lose_finalization_reply
        .store(true, Ordering::Release);
    let service = start(&f, &f.spare, f.evidence().await?, platform.clone()).await?;
    f.wait(&service, |_| {
        platform.finalization_waiters.load(Ordering::Acquire) > 0
    })
    .await?;
    ensure!(failed_node(&f).await?["state"] == "Finalizing");
    ensure!(!platform.lose_finalization_reply.load(Ordering::Acquire));
    ensure!(!f.s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let context = platform
        .contexts
        .lock()
        .unwrap()
        .last()
        .cloned()
        .context("failure operation")?;
    service.shutdown().await;
    ensure!(platform.active.load(Ordering::Acquire) == 0);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .get(&cut, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_some());
    let fence =
        f.s.f
            .catalog
            .finalization_receipt(&cut, context.operation(), TIMEOUT)
            .await?
            .context("durable finalization")?;
    ensure!(fence.node()? == f.failed && fence.process()?.bytes() == f.failed_boot.process);
    ensure!(fence.isolation_request().is_some());
    ensure!(f
        .s
        .f
        .catalog
        .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
        .await?
        .is_none());
    // A different controller must renew runtime proofs. Losing the terminal
    // CAS reply must still converge via its canonical journal, without another
    // isolation effect, engine deletion or a second membership change.
    let isolation_calls = platform.calls.load(Ordering::Acquire);
    let successor = Platform::new(&f)?;
    successor.mode.store(2, Ordering::Release);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let service = start(&f, &f.s.target, f.evidence().await?, successor.clone()).await?;
    f.wait(&service, |s| {
        s.failure_recovery.as_ref().is_some_and(|r| {
            r.phase == FleetFailureRecoveryPhase::Removed && r.removed_revision.is_some()
        })
    })
    .await?;
    ensure!(service.status().settled_movements == 0);
    service.shutdown().await;
    ensure!(platform.calls.load(Ordering::Acquire) == isolation_calls);
    ensure!(successor.calls.load(Ordering::Acquire) == 0);
    ensure!(!f.s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let receipt =
        f.s.f
            .catalog
            .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
            .await?
            .context("terminal receipt")?;
    ensure!(receipt.node() == f.failed.bytes());
    ensure!(failed_node(&f).await?["state"] == "Removed");
    ensure!(f
        .s
        .f
        .catalog
        .get(&cut, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    for index in 0..2 {
        f.verify(index).await?;
    }
    retained_engine(&f.s.f, f.old_engine.clone()).await?;
    // A historical query after a new operator admission must preserve the new
    // disruption and never reinterpret isolation as a signed joined closure.
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let next = FleetRequestIdentity {
        id: [229; 16],
        operation: [230; 16],
        principal: "fixture-next-operation".into(),
        expected_revision: cut.revision(),
    };
    let next_node = f.s.nodes[0].spec.node;
    f.s.f
        .catalog
        .begin_evacuation(
            &cut,
            &next,
            next_node,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await?;
    let current = f.s.f.catalog.read(TIMEOUT).await?;
    let slot =
        f.s.f
            .catalog
            .get(&current, FleetTable::Policy, "active-disruption", TIMEOUT)
            .await?
            .context("next disruption")?;
    let historical =
        f.s.f
            .catalog
            .isolated_removal_receipt(&current, context.operation(), TIMEOUT)
            .await?
            .context("historical receipt")?;
    ensure!(historical.applied_revision() == receipt.applied_revision());
    ensure!(
        f.s.f
            .catalog
            .get(&current, FleetTable::Policy, "active-disruption", TIMEOUT)
            .await?
            == Some(slot)
    );
    ensure!(platform.scratch.usage()?.generations == 0);
    ensure!(successor.scratch.usage()?.generations == 0);
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_recovery_archive_failure_retains_node_and_disruption_until_replay(
) -> Result<()> {
    Box::pin(archive_failure()).await
}

async fn archive_failure() -> Result<()> {
    let mut f = Box::pin(RepairFleet::with_admission(false, None, 1)).await?;
    let platform = Platform::new(&f)?;
    platform.mode.store(2, Ordering::Release);
    platform
        .block_removal_archives
        .store(true, Ordering::Release);
    let service = start(&f, &f.spare, f.evidence().await?, platform.clone()).await?;
    f.wait(&service, |s| {
        s.consecutive_round_failures >= 2
            && s.failure_recovery
                .as_ref()
                .is_some_and(|r| r.phase == FleetFailureRecoveryPhase::VerifyingFinalization)
    })
    .await?;
    ensure!(!platform.block_removal_archives.load(Ordering::Acquire));
    ensure!(f.s.f.store.archive_unavailable.load(Ordering::Acquire));
    ensure!(failed_node(&f).await?["state"] == "Draining");
    let catalog = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    let context = platform
        .contexts
        .lock()
        .unwrap()
        .last()
        .cloned()
        .context("failure operation")?;
    ensure!(catalog.pending_movements(&cut, TIMEOUT).await?.is_empty());
    ensure!(catalog
        .get(&cut, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_some());
    ensure!(catalog
        .finalization_receipt(&cut, context.operation(), TIMEOUT)
        .await?
        .is_none());
    ensure!(catalog
        .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
        .await?
        .is_none());
    f.s.f
        .store
        .archive_unavailable
        .store(false, Ordering::Release);
    f.wait(&service, |s| {
        s.failure_recovery
            .as_ref()
            .is_some_and(|r| r.phase == FleetFailureRecoveryPhase::Removed)
    })
    .await?;
    service.shutdown().await;
    ensure!(platform.scratch.usage()?.generations == 0);
    f.verify(0).await?;
    retained_engine(&f.s.f, f.old_engine.clone()).await?;
    f.close().await
}
