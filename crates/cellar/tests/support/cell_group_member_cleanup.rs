// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Targeted durable cleanup after two moves, including an interrupted root CAS.
use super::*;
use cellar::application::consensus::verification::{FleetChange, FleetMutation, FleetTable};

fn options() -> FleetMovementServiceOptions {
    FleetMovementServiceOptions {
        reclaim_removed_members: false,
        interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(200),
        turn_timeout: TIMEOUT,
    }
}

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    evidence: std::sync::Arc<dyn FleetMovementEvidence>,
) -> Result<()> {
    let catalog = &cluster.fixtures[0].catalog;
    for source in [0, 1] {
        let host = &cluster.hosts[source];
        let node = host.identity().node;
        let key = format!("member-cleanup/work/{}", hex::encode(node.bytes()));
        let before = catalog.read(TIMEOUT).await?;
        ensure!(host.observe_node(TIMEOUT).await?.node.groups == 1);
        ensure!(
            catalog
                .get(&before, FleetTable::Operations, &key, TIMEOUT)
                .await?
                .is_some(),
            "completion did not persist targeted cleanup"
        );
        let forged = FleetMutation {
            id: [220 + source as u8; 16],
            operation: [220 + source as u8; 16],
            principal: "fixture".into(),
            expected_revision: before.revision(),
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: key.clone(),
                value: None,
            }],
        };
        ensure!(
            matches!(
                catalog.commit(&before, &forged, TIMEOUT).await,
                Err(FleetCatalogError::Invalid(_))
            ),
            "generic queue deletion allowed"
        );
        if source == 0 {
            // This is the FIRST removed member, after a later completed move.
            // Pause its actual settlement CAS after physical GC, then stop the
            // worker. The replacement must discover the unchanged durable work.
            let store = &cluster.fixtures[0].store;
            store.fleet_cas_hang.store(true, Ordering::Release);
            store.hold_next_fleet_cas.store(true, Ordering::Release);
            let mut worker = Some(
                catalog
                    .start_member_cleanup_service(host.clone(), options())
                    .await?,
            );
            let paused = async {
                tokio::time::timeout(TIMEOUT, async {
                    loop {
                        if store.fleet_cas_waiting.load(Ordering::Acquire) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .context("cleanup did not reach actual root CAS")?;
                ensure!(
                    host.observe_node(TIMEOUT).await?.node.groups == 0,
                    "root CAS preceded physical GC"
                );
                ensure!(catalog.read(TIMEOUT).await?.revision() == before.revision());
                tokio::time::timeout(TIMEOUT, worker.take().context("cleanup worker")?.shutdown())
                    .await
                    .context("cleanup shutdown did not join")?;
                ensure!(
                    !store.fleet_cas_waiting.load(Ordering::Acquire),
                    "cancelled CAS callback outlived worker"
                );
                ensure!(
                    catalog.read(TIMEOUT).await?.revision() == before.revision(),
                    "cancelled CAS committed"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            store.fleet_cas_hang.store(false, Ordering::Release);
            store.fleet_cas_resume.notify_waiters();
            if let Some(worker) = worker.take() {
                worker.shutdown().await;
            }
            paused?;
        }
        let worker = if source == 0 {
            catalog
                .start_member_cleanup_service(host.clone(), options())
                .await?
        } else {
            catalog
                .start_movement_service(
                    host.clone(),
                    evidence.clone(),
                    FleetMovementServiceOptions {
                        reclaim_removed_members: true,
                        ..options()
                    },
                )
                .await?
        };
        let result = tokio::time::timeout(TIMEOUT, async {
            loop {
                let status = worker.status();
                if status.reclaimed_members == 1 && status.pending_member_cleanups == 0 {
                    ensure!(status.member_cleanup_attempts >= 1);
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("replacement cleanup worker did not settle saved work");
        worker.shutdown().await;
        result??;
        let settled = catalog.read(TIMEOUT).await?;
        ensure!(
            settled.revision() == before.revision() + 1,
            "cleanup refunded more than once"
        );
        ensure!(host.observe_node(TIMEOUT).await?.node.groups == 0);
        // A fresh worker sees an empty queue and makes no new root mutation.
        let worker = catalog
            .start_member_cleanup_service(host.clone(), options())
            .await?;
        let result = tokio::time::timeout(TIMEOUT, async {
            loop {
                let status = worker.status();
                if status.poll_rounds > 0 && status.last_revision == Some(settled.revision()) {
                    ensure!(status.pending_member_cleanups == 0 && status.reclaimed_members == 0);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        worker.shutdown().await;
        result??;
        ensure!(catalog.read(TIMEOUT).await?.revision() == settled.revision());
        eprintln!("CG11 targeted cleanup source={source}; original queue recovered, one settlement, empty restart stable");
    }
    Ok(())
}
