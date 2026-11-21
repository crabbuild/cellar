// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Current Fleet trust does not imply discovery of a newly booted caller's view.
use super::*;

async fn start(
    fixture: &Fixture,
    generation: u8,
    nodes: &mut Vec<Node>,
    hosts: &mut Vec<RaftCells>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let root = tempfile::tempdir()?;
    let node = start_node(
        root.path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    roots.push(root);
    nodes.push(node);
    let node = nodes.last().context("started inspection node")?;
    let (config, services) = fixture
        .config(node, LogOpenMode::Create, generation)
        .await?;
    hosts.push(node.start_raft_cells(config, services).await?);
    Ok(())
}

fn endpoint_owners(host: &RaftCells) -> Result<u64> {
    let sample = host.performance();
    ensure!(sample.available && !sample.saturated);
    Ok(sample
        .counters
        .iter()
        .find(|counter| counter.name == "cell_endpoint_discovery_owners")
        .context("inspection endpoint discovery counter")?
        .value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inspection_discovers_current_peer_view_with_one_original_request() -> Result<()> {
    let first = Fixture::new(false).await?;
    let mut receiver = managed_fixture::peer_fixture(&first, 5).await?;
    receiver.catalog = Fixture::open_catalog(
        &receiver.domain,
        receiver.store.clone(),
        receiver.declaration.namespace.cluster,
    )?;
    let mut nodes = Vec::new();
    let mut hosts = Vec::new();
    let mut roots = Vec::new();
    let result = async {
        start(&first, 24, &mut nodes, &mut hosts, &mut roots).await?;
        start(&receiver, 25, &mut nodes, &mut hosts, &mut roots).await?;
        let cut = first.catalog.read(TIMEOUT).await?;
        managed_fixture::inspect_current(&hosts[0], receiver.spec.node, cut.revision()).await?;
        managed_fixture::inspect_current(&hosts[1], first.spec.node, cut.revision()).await?;

        // This receiver cannot have a cached view for a node that has not yet
        // been registered or booted. Its background view reads remain held
        // until the original request starts targeted endpoint discovery.
        receiver
            .store
            .hold_view_reads
            .store(true, Ordering::Release);
        tokio::time::timeout(TIMEOUT, async {
            while receiver.store.active_view_reads.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("hold receiver background process-view reads")?;
        let source = managed_fixture::peer_fixture(&first, 7).await?;
        start(&source, 26, &mut nodes, &mut hosts, &mut roots).await?;
        let cut = first.catalog.read(TIMEOUT).await?;

        // Catch up Fleet trust independently. An unchanged revision floor is
        // insufficient to repair the receiver's missing caller process view.
        let local = hosts[1]
            .inspect_peer_node(receiver.spec.node, cut.revision(), TIMEOUT)
            .await?;
        ensure!(local
            .view
            .fleet_trust_revision
            .is_some_and(|revision| revision >= cut.revision()));
        ensure!(receiver.store.active_view_reads.load(Ordering::Acquire) > 0);
        let before = endpoint_owners(&hosts[1])?;
        let request =
            hosts[2].inspect_peer_physical_inventory(receiver.spec.node, cut.revision(), TIMEOUT);
        tokio::pin!(request);
        let discovery = async {
            while endpoint_owners(&hosts[1])? == before {
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        };
        tokio::time::timeout(TIMEOUT, async {
            cellar::asyncrt::select_biased! { "premature inspection completion precedes discovery readiness";
                result = &mut request => anyhow::bail!(
                    "inspection completed before discovering the current caller view: {:?}",
                    result.map(|_| ())
                ),
                result = discovery => result,
            }
        })
        .await
        .context("original inspection did not request endpoint discovery")??;
        receiver
            .store
            .hold_view_reads
            .store(false, Ordering::Release);
        receiver.store.view_reads_resume.notify_waiters();
        let observed = request
            .await
            .context("one original inspection must discover the enrolled caller view")?;
        ensure!(observed.view.node.identity.node == receiver.spec.node);
        ensure!(observed
            .inventory
            .context("complete physical inventory")?
            .histories()
            .is_empty());
        ensure!(endpoint_owners(&hosts[1])? == before + 1);
        let cached = hosts[2]
            .inspect_peer_physical_inventory(receiver.spec.node, cut.revision(), TIMEOUT)
            .await?;
        ensure!(cached.view.node.identity.node == receiver.spec.node && cached.inventory.is_some());
        ensure!(endpoint_owners(&hosts[1])? == before + 1);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    receiver
        .store
        .hold_view_reads
        .store(false, Ordering::Release);
    receiver.store.view_reads_resume.notify_waiters();
    let mut cleanup = Vec::new();
    for (index, node) in nodes.into_iter().enumerate() {
        let joined = async {
            if let Some(host) = hosts.get(index) {
                stopped(node, host).await
            } else {
                ensure!(matches!(
                    node.shutdown(ShutdownMode::Preserve).await?,
                    NodeOutcome::Stopped(_)
                ));
                Ok(())
            }
        }
        .await;
        if let Err(error) = joined {
            cleanup.push(format!("node {index}: {error:#}"));
        }
    }
    first.domain.shutdown().await;
    result.with_context(|| format!("inspection endpoint cleanup: {cleanup:?}"))?;
    ensure!(
        cleanup.is_empty(),
        "inspection endpoint cleanup: {cleanup:?}"
    );
    drop(roots);
    Ok(())
}
