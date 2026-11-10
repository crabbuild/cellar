// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One lost metadata reply must not consume a whole movement turn.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_movement_retries_one_lost_leader_observation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(lost_observation(rooted, Arc::new(fault::Store::default()))).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_movement_retries_one_lost_leader_observation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(lost_observation(
            rooted,
            Arc::new(fault::Store::around(rustfs::store(
                "movement-observation",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

async fn lost_observation(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_catalog_clients(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
        true,
    ))
    .await?;
    let work = async {
        // The controller uses catalog client0. Member2's foreground observer
        // owns a distinct client over the same backend, so its one lost read
        // cannot block initial CAS checks, trust refresh or other peers.
        maintenance::handoff_to(&cluster, 3, 1).await?;
        let (_, source, _, _, execution) = Box::pin(reserve_replacement(&cluster, true)).await?;
        ensure!(source == 1);
        let catalog = &cluster.fixtures[0].catalog;
        let snapshot = catalog.read(TIMEOUT).await?;
        let lost = Arc::new(fault::RegistryGate::default());
        lost.armed.store(true, Ordering::Release);
        let result = fault::gate_store_read(
            cluster.fixtures[source].store.as_ref(),
            "/fleet/head".into(),
            lost.clone(),
            catalog.reconcile_movement(&snapshot, &execution, &cluster.hosts[source], TIMEOUT),
        )
        .await;
        ensure!(
            !lost.armed.load(Ordering::Acquire),
            "leader observation fault was not entered"
        );
        match result {
            Ok(progress) => {
                ensure!(progress.phase == FleetMovementPhase::Reserved && !progress.settled)
            }
            Err(FleetCatalogError::Unconfirmed) => (),
            Err(error) => return Err(error.into()),
        }
        // Unknown action delivery is not rollback proof. A fresh quorum must
        // show that this single reconciliation actually reached durable Begin;
        // another mutation request is never issued to rescue the attempt.
        let observed = cluster.hosts[source]
            .group(cluster.history)?
            .quorum_membership(TIMEOUT)
            .await?;
        ensure!(
            observed
                .membership()
                .fleet_status()
                .is_some_and(|s| s.phase == Some(FleetPermitPhase::Seeding)),
            "one lost leader observation prevented movement Begin"
        );
        ensure!(
            catalog
                .pending_reservations(&catalog.read(TIMEOUT).await?, TIMEOUT)
                .await?
                .len()
                == 1,
            "beginning a movement released its physical debit"
        );
        ensure!(
            cluster.hosts[3].resource_usage().host.controller.groups == 0,
            "discovery initialized a learner without activation"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained failed movement observation disk: {}",
                root.keep().display()
            );
        }
    }
    work?;
    joined
}
