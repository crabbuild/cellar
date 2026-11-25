// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Removing a voter must not confuse joined shutdown with completed reclamation.
use super::*;
use managed_fixture::{leader, movement, seed_cells, ManagedCluster};

const STOP_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_removed_member_joins_held_archive() -> Result<()> {
    qualify(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rooted_shutdown_removed_member_joins_held_archive() -> Result<()> {
    qualify(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_shutdown_removed_member_joins_held_archive() -> Result<()> {
    qualify_store(
        false,
        Arc::new(fault::Store::around(rustfs::store(
            "retirement-shutdown",
            false,
        )?)),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_rooted_shutdown_removed_member_joins_held_archive() -> Result<()> {
    qualify_store(
        true,
        Arc::new(fault::Store::around(rustfs::store(
            "retirement-shutdown",
            true,
        )?)),
    )
    .await
}

struct Release(Arc<fault::Store>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.group_archive_writes.release();
    }
}

async fn qualify(rooted: bool) -> Result<()> {
    qualify_store(rooted, Arc::new(fault::Store::default())).await
}

async fn qualify_store(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        managed_fixture::ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let result = Box::pin(exercise(&mut cluster)).await;
    let joined = movement::close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if result.is_err() || joined.is_err() {
        for root in cluster.roots {
            eprintln!(
                "retained retirement shutdown disk: {}",
                root.keep().display()
            );
        }
    }
    result?;
    joined
}

async fn transfer(cluster: &ManagedCluster, target: usize) -> Result<()> {
    let source = leader(&cluster.hosts[..3], cluster.history).await?;
    if source == target {
        return Ok(());
    }
    let group = cluster.hosts[source].group(cluster.history)?;
    let member = MemberId::new(target as u64 + 1)?;
    let progress = tokio::time::timeout(TIMEOUT, async {
        loop {
            let progress = group.replication_status(TIMEOUT).await?;
            if progress.members.iter().any(|peer| {
                peer.member == member && peer.recent_active && peer.matched >= progress.committed
            }) {
                return Ok::<_, anyhow::Error>(progress);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("retirement test transfer target did not catch up")??;
    group
        .transfer_leadership(
            LeadershipTransfer {
                target: member,
                expected_term: progress.term,
                expected_revision: progress.membership_revision,
            },
            TIMEOUT,
        )
        .await?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if leader(&cluster.hosts[..3], cluster.history).await? == target {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("retirement test leadership transfer")??;
    Ok(())
}

async fn exercise(cluster: &mut ManagedCluster) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    // Peer 1 has an independent fault wrapper; holding it does not block the
    // other replicas' archive stores or any Fleet/endpoint reads.
    let obsolete = 1;
    transfer(cluster, obsolete).await?;
    let service = cluster.hosts[obsolete].clone();
    let group = service.group(cluster.history)?;
    let cell = service.cell(seed.cells[0].clone())?;
    let store = cluster.fixtures[obsolete].store.clone();
    let _release = Release(store.clone());
    store.group_archive_writes.arm();
    let second = CommandId::new(seed.receipts[0].id.client, 2)?;
    let written = cell
        .command(
            &cell.contract("Add")?,
            2i64.to_be_bytes().to_vec(),
            CommandOptions::new(second, seed.retry),
        )
        .await?;
    ensure!(written.durability == Durability::Quorum);
    tokio::time::timeout(TIMEOUT, store.group_archive_writes.entered.notified())
        .await
        .context("obsolete voter never entered its held archive PUT")?;
    transfer(cluster, 0).await?;
    movement::discover(cluster).await?;
    let (_, execution) = movement::admit_replacement(
        cluster,
        movement::ReplacementTarget {
            source: 0,
            obsolete,
            spare: 3,
        },
        [242, 243],
        false,
    )
    .await?;
    let mut settled = false;
    for _ in 0..8 {
        movement::discover(cluster).await?;
        let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
        match cluster.fixtures[0]
            .catalog
            .reconcile_movement(&cut, &execution, &cluster.hosts[0], TIMEOUT)
            .await
        {
            Ok(progress) if progress.settled => {
                ensure!(progress.phase == FleetMovementPhase::Complete);
                settled = true;
                break;
            }
            Ok(_) | Err(FleetCatalogError::Unconfirmed) => (),
            Err(error) => return Err(error.into()),
        }
    }
    ensure!(
        settled,
        "membership replacement did not settle its original identity"
    );
    // Only the controller's verified removal path detaches this runtime owner.
    // Its archive PUT remains held, so controller retirement cannot finish yet.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match group.status().await {
                Err(RuntimeError::NotFound) => return Ok::<_, anyhow::Error>(()),
                Ok(_) => tokio::task::yield_now().await,
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await
    .context("removed runtime did not quiesce while its archive was held")??;
    ensure!(service.resource_usage().host.controller.groups == 1);
    ensure!(group
        .archive_status()?
        .is_some_and(|status| !status.stopped));

    let node = cluster.nodes.remove(obsolete);
    cluster.hosts.remove(obsolete);
    let mut stop = tokio::spawn(node.shutdown(ShutdownMode::Preserve));
    let sealed = tokio::time::timeout(TIMEOUT, async {
        while !service.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let early = tokio::time::timeout(Duration::from_millis(100), &mut stop).await;
    store.group_archive_writes.release();
    let (waited, outcome) = match early {
        Ok(outcome) => (false, outcome?),
        Err(_) => (true, tokio::time::timeout(STOP_TIMEOUT, stop).await??),
    };
    sealed.context("shutdown did not seal public admission")?;
    ensure!(
        waited,
        "shutdown returned before its admitted archive PUT joined"
    );
    let NodeOutcome::Stopped(report) = outcome? else {
        anyhow::bail!("removed voter did not stop");
    };
    ensure!(report.application_work_drained);
    let closed = report
        .raft
        .context("removed voter Raft close report")?
        .result?;
    ensure!(
        closed.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers,
        "physically joined removed voter lacks producer proof: {closed:?}; resources={:?}",
        service.resource_usage()
    );
    ensure!(closed.groups_total == 1 && closed.retained_calls_at_shutdown == 0);
    ensure!(closed
        .groups
        .iter()
        .all(|g| !g.failed && g.applied.index == g.committed));
    ensure!(service.close().await? == closed);
    ensure!(cluster.roots[obsolete].path().join("raft").is_dir());
    // Joining a removed replica neither erases its physical debt nor prevents
    // the fresh quorum from acknowledging new writes.
    let survivor = cluster.hosts[0].cell(seed.cells[0].clone())?;
    let fresh = survivor
        .command(
            &survivor.contract("Add")?,
            1i64.to_be_bytes().to_vec(),
            CommandOptions::new(CommandId::new(second.client, 3)?, seed.retry),
        )
        .await?;
    ensure!(fresh.result == 10i64.to_be_bytes() && fresh.durability == Durability::Quorum);
    Ok(())
}
