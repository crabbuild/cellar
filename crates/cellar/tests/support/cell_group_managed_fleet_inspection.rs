// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One original group-inspection request owns trust discovery and cancellation.
use super::*;
use managed_fixture::{ArchiveSetup, FirstGroupSetup, ManagedCluster};
use std::sync::atomic::AtomicUsize;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_inspection_catches_up_and_cancels_owned_discovery() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(rooted)).await?;
    }
    Ok(())
}

async fn qualify(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let class = managed_fixture::counter_class_with_callback(move || {
        counted.fetch_add(1, Ordering::AcqRel);
    })?;
    let mut first = Fixture::with_class(rooted, class).await?;
    let store = Arc::new(fault::Store::around(first.store.clone()));
    first.catalog = Fixture::open_catalog(
        &first.domain,
        store.clone(),
        first.declaration.namespace.cluster,
    )?;
    first.host_store = Some(store.clone());
    first.settle_periodic_purges = false;
    let cluster = ManagedCluster::start_with_applications(
        first,
        &[],
        ArchiveSetup::Disabled,
        true,
        None,
        FirstGroupSetup::Normal,
    )
    .await?;
    let result = async {
        managed_fixture::leader(&cluster.hosts, cluster.history).await?;
        let source = &cluster.hosts[0];
        let catalog = Fixture::open_catalog(
            &cluster.fixtures[0].domain,
            cluster.fixtures[0].store.clone(),
            cluster.history.cluster,
        )?;
        // Hold the actual periodic reader, leaving a separate demand reader
        // able to obtain and install a complete authenticated successor cut.
        store.fleet_read_hang.store(true, Ordering::Release);
        store.hold_next_fleet_read.store(true, Ordering::Release);
        tokio::time::timeout(TIMEOUT, async {
            while store.active_fleet_reads.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("hold group observer's periodic trust refresh")?;
        for step in 0..3_u8 {
            let id = 9 + step;
            let mut spare = cluster.fixtures[0].spec.clone();
            spare.node = NodeId::new([id; 16])?;
            spare.disk = DiskLineage::new([id + 10; 16])?;
            spare.physical_host = IncarnationId::new([id + 90; 16])?;
            spare.label = format!("group-inspection-spare-{id}");
            let cut = catalog.read(TIMEOUT).await?;
            let cut = catalog
                .register_node(&cut, &request(id, cut.revision()), &spare, TIMEOUT)
                .await?
                .snapshot;
            let revision = cut.revision();
            let authorization = catalog
                .authorize_group(&cut, &cluster.history, TIMEOUT)
                .await?;
            ensure!(source
                .observe_node(TIMEOUT)
                .await?
                .fleet_trust_revision
                .is_some_and(|r| r < revision));
            if step == 0 {
                let observed = source
                    .observe_peer_fleet_membership(
                        source.identity().node,
                        authorization,
                        revision,
                        TIMEOUT,
                    )
                    .await
                    .context("one local group inspection must catch up")?;
                ensure!(
                    observed.progress().history == cluster.history
                        && observed
                            .node()
                            .fleet_trust_revision
                            .is_some_and(|r| r >= revision)
                );
            } else if step == 1 {
                let pending = source.observe_peer_fleet_membership(
                    cluster.fixtures[1].spec.node,
                    authorization,
                    revision,
                    TIMEOUT,
                );
                // Invocation starts the owned remote discovery even before the
                // caller polls its response. A physical observation cannot
                // advance installed trust by itself.
                tokio::time::timeout(TIMEOUT, async {
                    loop {
                        if source
                            .observe_node(TIMEOUT)
                            .await?
                            .fleet_trust_revision
                            .is_some_and(|r| r >= revision)
                        {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .context("unpolled group inspection did not start discovery")??;
                let observed = pending
                    .await
                    .context("one remote group inspection must catch up")?;
                ensure!(
                    observed.progress().history == cluster.history
                        && observed
                            .node()
                            .fleet_trust_revision
                            .is_some_and(|r| r >= revision)
                );
            } else {
                let before = source.resource_usage().control;
                store.hold_next_fleet_read.store(true, Ordering::Release);
                let cancelled = source.observe_peer_fleet_membership(
                    cluster.fixtures[1].spec.node,
                    authorization,
                    revision,
                    Duration::from_secs(30),
                );
                tokio::time::timeout(TIMEOUT, async {
                    while store.active_fleet_reads.load(Ordering::Acquire) != 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .context("hold owned group-inspection discovery")?;
                let charged = source.resource_usage().control;
                ensure!(charged.items > before.items && charged.bytes > before.bytes);
                drop(cancelled);
                // The 30-second request cannot expire inside this five-second
                // oracle. Only cancellation can release this owned discovery.
                tokio::time::timeout(TIMEOUT, async {
                    loop {
                        let usage = source.resource_usage().control;
                        if store.active_fleet_reads.load(Ordering::Acquire) == 1
                            && usage.items == before.items
                            && usage.bytes == before.bytes
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .context("cancelled group inspection retained discovery or quota")?;
                ensure!(source
                    .observe_node(TIMEOUT)
                    .await?
                    .fleet_trust_revision
                    .is_some_and(|r| r < revision));
            }
            ensure!(store.active_fleet_reads.load(Ordering::Acquire) == 1);
        }
        ensure!(
            calls.load(Ordering::Acquire) == 0,
            "metadata inspection entered application code"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    store.fleet_read_hang.store(false, Ordering::Release);
    store.fleet_read_resume.notify_waiters();
    let mut joined = Ok(());
    for (node, host) in cluster.nodes.into_iter().zip(&cluster.hosts) {
        let stopped = async {
            ensure!(matches!(
                node.shutdown(ShutdownMode::Preserve).await?,
                NodeOutcome::Stopped(_)
            ));
            let report = host.close().await?;
            ensure!(
                report.groups_total == 1
                    && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if joined.is_ok() {
            joined = stopped;
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    if result.is_err() || joined.is_err() {
        for root in cluster.roots {
            eprintln!(
                "retained failed group-inspection disk: {}",
                root.keep().display()
            );
        }
    }
    result.with_context(|| format!("group inspection, extents={rooted}; joined={joined:?}"))?;
    joined
}
