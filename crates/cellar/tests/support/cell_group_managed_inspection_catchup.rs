// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A new Fleet revision must not require waiting for a voter's background poll.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inspection_catches_up_trust_with_one_original_request() -> Result<()> {
    let first = Fixture::new(false).await?;
    let mut second = managed_fixture::peer_fixture(&first, 5).await?;
    second.catalog = Fixture::open_catalog(
        &second.domain,
        second.store.clone(),
        second.declaration.namespace.cluster,
    )?;
    let mut nodes = Vec::new();
    let mut hosts = Vec::new();
    let mut roots = Vec::new();
    for (fixture, generation) in [(&first, 24), (&second, 25)] {
        let root = tempfile::tempdir()?;
        let node = start_node(
            root.path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let (config, services) = fixture
            .config(&node, LogOpenMode::Create, generation)
            .await?;
        hosts.push(node.start_raft_cells(config, services).await?);
        nodes.push(node);
        roots.push(root);
    }
    let result = async {
        let cut = first.catalog.read(TIMEOUT).await?;
        managed_fixture::inspect_current(&hosts[0], second.spec.node, cut.revision()).await?;
        managed_fixture::inspect_current(&hosts[1], first.spec.node, cut.revision()).await?;

        // Stall only the already-entered directory read. An independent,
        // admitted inspection reader can still fetch the real current root.
        second.store.fleet_read_hang.store(true, Ordering::Release);
        second
            .store
            .hold_next_fleet_read
            .store(true, Ordering::Release);
        tokio::time::timeout(TIMEOUT, async {
            while second.store.active_fleet_reads.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("hold the target's background trust refresh")?;

        let mut spare = first.spec.clone();
        spare.node = NodeId::new([9; 16])?;
        spare.disk = DiskLineage::new([19; 16])?;
        spare.physical_host = IncarnationId::new([99; 16])?;
        spare.label = "managed-discovery-spare".into();
        let cut = first.catalog.read(TIMEOUT).await?;
        let cut = first
            .catalog
            .register_node(&cut, &request(9, cut.revision()), &spare, TIMEOUT)
            .await?
            .snapshot;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let view = hosts[0].observe_node(TIMEOUT).await?;
                if view
                    .fleet_trust_revision
                    .is_some_and(|r| r >= cut.revision())
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        ensure!(hosts[1]
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_some_and(|r| r < cut.revision()));

        // Exactly one request, with the unchanged deadline and revision floor.
        let observed = hosts[0]
            .inspect_peer_node(second.spec.node, cut.revision(), TIMEOUT)
            .await
            .context("inspection must catch up without a caller retry")?;
        ensure!(observed.view.node.identity.node == second.spec.node);
        ensure!(observed
            .view
            .fleet_trust_revision
            .is_some_and(|r| r >= cut.revision()));
        ensure!(second.store.active_fleet_reads.load(Ordering::Acquire) == 1);

        // Advance the root again while the same background read is held. The
        // local fast path must catch up too; it cannot use the previous RPC's
        // installed cut or rely on another periodic refresh.
        spare.node = NodeId::new([10; 16])?;
        spare.disk = DiskLineage::new([20; 16])?;
        spare.physical_host = IncarnationId::new([100; 16])?;
        spare.label = "managed-local-inspection-spare".into();
        let current = first.catalog.read(TIMEOUT).await?;
        let current = first
            .catalog
            .register_node(&current, &request(10, current.revision()), &spare, TIMEOUT)
            .await?
            .snapshot;
        ensure!(hosts[1]
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_some_and(|r| r < current.revision()));
        let local = hosts[1]
            .inspect_peer_node(second.spec.node, current.revision(), TIMEOUT)
            .await
            .context("self-inspection must catch up without a caller retry")?;
        ensure!(local.view.node.identity.node == second.spec.node);
        ensure!(local
            .view
            .fleet_trust_revision
            .is_some_and(|r| r >= current.revision()));
        ensure!(second.store.active_fleet_reads.load(Ordering::Acquire) == 1);
        // Keep the sender's background read behind a newer root too. The
        // receiver is caught up independently so one remote physical request
        // must install the sender's complete trust cut before selecting a route.
        let mutator = Fixture::open_catalog(
            &first.domain,
            first.store.clone(),
            first.declaration.namespace.cluster,
        )?;
        first.store.fleet_read_hang.store(true, Ordering::Release);
        first
            .store
            .hold_next_fleet_read
            .store(true, Ordering::Release);
        tokio::time::timeout(TIMEOUT, async {
            while first.store.active_fleet_reads.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("hold the sender's background trust refresh")?;
        spare.node = NodeId::new([11; 16])?;
        spare.disk = DiskLineage::new([21; 16])?;
        spare.physical_host = IncarnationId::new([101; 16])?;
        spare.label = "managed-sender-inspection-spare".into();
        let newest = mutator.read(TIMEOUT).await?;
        let newest = mutator
            .register_node(&newest, &request(11, newest.revision()), &spare, TIMEOUT)
            .await?
            .snapshot;
        hosts[1]
            .inspect_peer_node(second.spec.node, newest.revision(), TIMEOUT)
            .await?;
        ensure!(hosts[0]
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_some_and(|r| r < newest.revision()));
        let pending =
            hosts[0].inspect_peer_physical_inventory(second.spec.node, newest.revision(), TIMEOUT);
        // Discovery starts at invocation while the response remains unpolled.
        // Observing local state cannot refresh transport trust on its own.
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if hosts[0]
                    .observe_node(TIMEOUT)
                    .await?
                    .fleet_trust_revision
                    .is_some_and(|r| r >= newest.revision())
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("unpolled inspection did not start sender discovery")??;
        let remote = pending
            .await
            .context("sender inspection must catch up without a caller retry")?;
        ensure!(remote.view.node.identity.node == second.spec.node && remote.inventory.is_some());
        ensure!(remote
            .view
            .fleet_trust_revision
            .is_some_and(|r| r >= newest.revision()));
        ensure!(hosts[0]
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_some_and(|r| r >= newest.revision()));
        ensure!(first.store.active_fleet_reads.load(Ordering::Acquire) == 1);
        ensure!(second.store.active_fleet_reads.load(Ordering::Acquire) == 1);

        // Dropping an unpolled response must cancel its owned discovery and
        // release admission, even while the real catalog read remains held.
        spare.node = NodeId::new([12; 16])?;
        spare.disk = DiskLineage::new([22; 16])?;
        spare.physical_host = IncarnationId::new([102; 16])?;
        spare.label = "managed-cancelled-inspection-spare".into();
        let cancelled_cut = mutator.read(TIMEOUT).await?;
        let cancelled_cut = mutator
            .register_node(
                &cancelled_cut,
                &request(12, cancelled_cut.revision()),
                &spare,
                TIMEOUT,
            )
            .await?
            .snapshot;
        hosts[1]
            .inspect_peer_node(second.spec.node, cancelled_cut.revision(), TIMEOUT)
            .await?;
        ensure!(hosts[0]
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_some_and(|r| r < cancelled_cut.revision()));
        let before = hosts[0].resource_usage().control;
        first
            .store
            .hold_next_fleet_read
            .store(true, Ordering::Release);
        let cancelled = hosts[0].inspect_peer_physical_inventory(
            second.spec.node,
            cancelled_cut.revision(),
            // Expiry must not satisfy the five-second cancellation assertion.
            Duration::from_secs(30),
        );
        tokio::time::timeout(TIMEOUT, async {
            while first.store.active_fleet_reads.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("hold the sender's owned inspection discovery")?;
        let charged = hosts[0].resource_usage().control;
        ensure!(charged.items > before.items && charged.bytes > before.bytes);
        drop(cancelled);
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let usage = hosts[0].resource_usage().control;
                if first.store.active_fleet_reads.load(Ordering::Acquire) == 1
                    && usage.items == before.items
                    && usage.bytes == before.bytes
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("cancelled inspection retained discovery or admission")?;
        ensure!(hosts[0]
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_some_and(|r| r < cancelled_cut.revision()));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    second.store.fleet_read_hang.store(false, Ordering::Release);
    second.store.fleet_read_resume.notify_waiters();
    first.store.fleet_read_hang.store(false, Ordering::Release);
    first.store.fleet_read_resume.notify_waiters();
    let mut joined = Ok(());
    for (node, host) in nodes.into_iter().zip(&hosts) {
        let result = stopped(node, host).await;
        if joined.is_ok() {
            joined = result;
        }
    }
    first.domain.shutdown().await;
    result?;
    joined?;
    drop(roots);
    Ok(())
}
