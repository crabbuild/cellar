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
    cluster: &mut ManagedCluster,
    evidence: std::sync::Arc<dyn FleetMovementEvidence>,
    retained: bool,
) -> Result<()> {
    // Observe the paused writer through an independent catalog turn.
    let catalog = managed_fixture::Fixture::open_catalog(
        &cluster.fixtures[0].domain,
        cluster.fixtures[0].store.clone(),
        cluster.history.cluster,
    )?;
    for source in [0, 1] {
        let mut host = cluster.hosts[source].clone();
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
        let mut mutation_id = [219; 16];
        mutation_id[15] = source as u8;
        let forged = FleetMutation {
            id: mutation_id,
            operation: mutation_id,
            principal: "fixture".into(),
            expected_revision: before.revision(),
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: key.clone(),
                value: None,
            }],
        };
        let refused = catalog.commit(&before, &forged, TIMEOUT).await.err();
        ensure!(
            matches!(&refused, Some(FleetCatalogError::Invalid(_))),
            "generic queue deletion did not return the expected refusal: {refused:?}"
        );
        let reservation = queued_reservation(&catalog, node).await?;
        let mut expected_revision = before.revision() + 1;
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
        if retained {
            let restarted = restart_and_qualify(
                cluster,
                source,
                170 + source as u8,
                180 + 2 * source as u8,
                source == 0,
                reservation,
            )
            .await?;
            host = restarted.0;
            expected_revision = restarted.1 + 1;
        }
        let worker: Option<FleetMovementService> = if retained && source == 0 {
            match catalog
                .start_member_cleanup_service(host.clone(), options())
                .await
            {
                Err(FleetCatalogError::Overloaded) => (),
                Ok(worker) => {
                    worker.shutdown().await;
                    anyhow::bail!("startup did not retain exclusive cleanup ownership");
                }
                Err(error) => return Err(error.into()),
            }
            None
        } else if source == 0 {
            catalog
                .start_member_cleanup_service(host.clone(), options())
                .await?
                .into()
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
                .into()
        };
        let result = tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(worker) = &worker {
                    let status = worker.status();
                    if status.reclaimed_members == 1 && status.pending_member_cleanups == 0 {
                        ensure!(status.member_cleanup_attempts >= 1);
                        return Ok::<_, anyhow::Error>(());
                    }
                } else if queue_is_empty(&catalog, node).await? {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("replacement cleanup worker did not settle saved work");
        if let Some(worker) = worker {
            worker.shutdown().await;
        }
        result??;
        let settled = catalog.read(TIMEOUT).await?;
        ensure!(
            settled.revision() == expected_revision,
            "cleanup refunded more than once"
        );
        ensure!(host.observe_node(TIMEOUT).await?.node.groups == 0);
        if retained && source == 0 {
            // A second retained boot must resolve an already committed receipt
            // without changing its floor or resurrecting the old member.
            let (new_host, settled_revision) =
                restart_and_qualify(cluster, source, 190, 184, false, reservation).await?;
            ensure!(
                new_host
                    .reclaim_removed_member(reservation, TIMEOUT)
                    .await?
            );
            ensure!(catalog.read(TIMEOUT).await?.revision() == settled_revision);
            continue;
        }
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

async fn queue_body(catalog: &FleetCatalog, node: NodeId) -> Result<serde_json::Value> {
    let cut = catalog.read(TIMEOUT).await?;
    let bytes = catalog
        .get(
            &cut,
            FleetTable::Operations,
            &format!("member-cleanup/work/{}", hex::encode(node.bytes())),
            TIMEOUT,
        )
        .await?
        .context("cleanup queue")?;
    ensure!(bytes.len() >= 45 && &bytes[..8] == b"CLRCAT01" && bytes[8] == 238);
    let len = u32::from_be_bytes(bytes[9..13].try_into()?) as usize;
    ensure!(len + 45 == bytes.len());
    use sha2::Digest as _;
    ensure!(sha2::Sha256::digest(&bytes[..13 + len]).as_slice() == &bytes[13 + len..]);
    Ok(serde_json::from_slice(&bytes[13..13 + len])?)
}

async fn queued_reservation(catalog: &FleetCatalog, node: NodeId) -> Result<[u8; 16]> {
    let body = queue_body(catalog, node).await?;
    let rows = body["reservations"].as_array().context("cleanup rows")?;
    ensure!(rows.len() == 1);
    Ok(serde_json::from_value(rows[0].clone())?)
}

async fn queue_is_empty(catalog: &FleetCatalog, node: NodeId) -> Result<bool> {
    Ok(queue_body(catalog, node).await?["reservations"]
        .as_array()
        .context("cleanup rows")?
        .is_empty())
}

async fn restart_and_qualify(
    cluster: &mut ManagedCluster,
    index: usize,
    generation: u8,
    capacity_id: u8,
    automatic: bool,
    reservation: [u8; 16],
) -> Result<(RaftCells, u64)> {
    use cellar::sdk::{NodeOutcome, ShutdownMode};
    let previous = cluster.hosts[index].clone();
    let original = previous
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("original boot")?;
    let node = cluster.nodes.remove(index);
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    ensure!(
        previous.close().await?.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    let fixture = &cluster.fixtures[index];
    let node = managed_fixture::start_node(
        cluster.roots[index].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let (mut config, services) = fixture
        .config(&node, LogOpenMode::Resume, generation)
        .await?;
    managed_fixture::archives::configure(&mut config);
    config.host.member_cleanup = automatic.then(options);
    let peer = config.peers[0].clone();
    let started = node.start_raft_cells(config, services).await;
    let host = match started {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!("retained cleanup startup, stopped={stopped:?}")));
        }
    };
    cluster.nodes.insert(index, node);
    cluster.peers[index] = peer;
    cluster.hosts[index] = host.clone();
    let observed = host.observe_node(TIMEOUT).await?;
    let boot = observed.fleet_boot.context("retained boot")?;
    ensure!(
        boot.engine == original.engine
            && boot.disk == original.disk
            && boot.process != original.process
            && boot.boot != original.boot
            && boot.join_key != original.join_key
            && boot.epoch > original.epoch
    );
    let catalog = managed_fixture::Fixture::open_catalog(
        &fixture.domain,
        fixture.store.clone(),
        cluster.history.cluster,
    )?;
    let before = catalog.read(TIMEOUT).await?;
    ensure!(
        previous
            .reclaim_removed_member(reservation, TIMEOUT)
            .await
            .is_err(),
        "old process retained cleanup authority"
    );
    let (unqualified, reads) =
        catalog_fault::count_reads(host.reclaim_removed_member(reservation, TIMEOUT)).await;
    ensure!(
        unqualified.is_err(),
        "stale capacity authorized retained cleanup"
    );
    ensure!(host.observe_node(TIMEOUT).await?.node.groups == observed.node.groups);
    ensure!(catalog.read(TIMEOUT).await?.revision() == before.revision());
    managed_fixture::admit_capacity(fixture, &host, &cluster.directory, capacity_id).await?;
    if !automatic && observed.node.groups != 0 {
        let witness = reads
            .keys()
            .find(|key| key.contains("/movement-source-boots/"))
            .context("original source boot witness read")?
            .clone();
        let refused =
            catalog_fault::missing_read(witness, host.reclaim_removed_member(reservation, TIMEOUT))
                .await;
        ensure!(
            refused.is_err(),
            "missing original witness authorized cleanup"
        );
        ensure!(host.observe_node(TIMEOUT).await?.node.groups == observed.node.groups);
    }
    let settled_revision = if automatic {
        before.revision() + 2
    } else {
        catalog.read(TIMEOUT).await?.revision()
    };
    Ok((host, settled_revision))
}
