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
        Ok::<_, anyhow::Error>(())
    }
    .await;
    second.store.fleet_read_hang.store(false, Ordering::Release);
    second.store.fleet_read_resume.notify_waiters();
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
