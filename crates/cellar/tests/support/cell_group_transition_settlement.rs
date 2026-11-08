// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted refunds follow actual source destruction and fund a second transition.
use super::*;
use cellar::application::consensus::verification::{cell_groups::GroupRoute, FleetTable};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

async fn operations(cluster: &ManagedCluster) -> Result<BTreeMap<String, Vec<u8>>> {
    let catalog = &cluster.fixtures[0].catalog;
    let snapshot = catalog.read(TIMEOUT).await?;
    let mut cursor = None;
    let mut values = BTreeMap::new();
    loop {
        let page = catalog
            .page(
                &snapshot,
                FleetTable::Operations,
                cursor.as_ref(),
                32,
                TIMEOUT,
            )
            .await?;
        for entry in page.entries {
            ensure!(values.insert(entry.key, entry.value).is_none());
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    Ok(values)
}

fn record(bytes: &[u8], kind: u8) -> Result<serde_json::Value> {
    ensure!(bytes.len() >= 45 && &bytes[..8] == b"CLRCAT01" && bytes[8] == kind);
    let length = u32::from_be_bytes(bytes[9..13].try_into()?) as usize;
    ensure!(length + 45 == bytes.len());
    ensure!(Sha256::digest(&bytes[..13 + length]).as_slice() == &bytes[13 + length..]);
    Ok(serde_json::from_slice(&bytes[13..13 + length])?)
}

pub(super) async fn assert_refunded(
    cluster: &ManagedCluster,
    control: &RaftFleetControl,
    _children: &[GroupRoute],
) -> Result<()> {
    let values = operations(cluster).await?;
    let cleanup = record(
        values
            .get(&format!("source-cleanup/{}", hex::encode([231; 16])))
            .context("rooted source cleanup")?,
        236,
    )?;
    let nodes = cleanup["nodes"].as_array().context("cleanup node owners")?;
    ensure!(nodes.len() == 3);
    for host in &cluster.hosts[..3] {
        let sample = host
            .inspect_peer_physical_inventory(host.identity().node, 1, TIMEOUT)
            .await?;
        ensure!(
            sample.view.node.groups == 1
                && sample.view.registered_histories == 1
                && sample.view.unattached_engine_histories == 0
        );
        ensure!(sample
            .inventory
            .context("post-settlement inventory")?
            .group_history(&cluster.history)
            .is_none());
        let node = serde_json::to_value(host.identity().node.bytes())?;
        let row = nodes
            .iter()
            .find(|n| n["node"] == node)
            .context("settled node")?;
        ensure!(
            row["before"] == 2
                && row["floor"] == 2
                && row["removed"] == 1
                && row["settled"] == true
        );
        let floor = record(
            values
                .get(&format!(
                    "resource-floor/{}",
                    hex::encode(host.identity().node.bytes())
                ))
                .context("refunded floor")?,
            64,
        )?;
        ensure!(
            floor["physical"]["engine_slots"] == 1,
            "slot floor: {floor}"
        );
    }
    let status = control
        .transition_status(
            &hex::encode(cluster.history.namespace.bytes()),
            &hex::encode([231; 16]),
            TIMEOUT,
        )
        .await?;
    ensure!(!status.fleet_slot_held && status.parent_holds_released);
    Ok(())
}

pub(super) async fn repeat_merge(
    cluster: &ManagedCluster,
    control: &RaftFleetControl,
    children: &[GroupRoute],
    workers: &[Option<RaftFleetTransitionService>],
    seed: &managed_fixture::SeededCells,
    latest: &[CommandReceipt],
) -> Result<()> {
    let namespace = hex::encode(cluster.history.namespace.bytes());
    let basis = control.transition_basis(&namespace, TIMEOUT).await?;
    let merged = GroupHistory {
        group: GroupId::new([206; 16])?,
        incarnation: IncarnationId::new([206; 16])?,
        ..cluster.history
    };
    let request = RaftFleetTransitionAdmissionRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: namespace.clone(),
        operation_id: hex::encode([202; 16]),
        request_id: hex::encode([203; 16]),
        expected_fleet_revision: basis.fleet_revision,
        expected_directory_digest: basis.directory_digest,
        class: "Counter".into(),
        partition: RaftFleetTransitionPartition::Merge {
            left: group(children[0].history),
            right: group(children[1].history),
        },
        children: vec![RaftFleetTransitionChild {
            group: group(merged),
            request_id: hex::encode([204; 16]),
            creation_id: hex::encode([205; 16]),
            voters: std::array::from_fn(|i| RaftFleetFirstGroupVoter {
                node_id: hex::encode(cluster.fixtures[i].spec.node.bytes()),
                member_id: i as u64 + 1,
            }),
        }],
    };
    let admitted = Box::pin(control.admit_transition(
        "automatic-test",
        &namespace,
        &request.operation_id,
        &request,
        TIMEOUT,
    ))
    .await?;
    let current = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    control
        .authorize_transition_execution(
            "automatic-test",
            &RaftFleetTransitionExecutionRequest {
                catalog_id: control.catalog_id().into(),
                namespace_id: namespace.clone(),
                operation_id: request.operation_id.clone(),
                request_id: hex::encode([208; 16]),
                expected_revision: current.revision(),
                plan_digest: admitted.plan_digest.clone(),
            },
            TIMEOUT,
        )
        .await?;
    wait_phase(
        control,
        &namespace,
        &request.operation_id,
        workers,
        TransitionPhase::Retired,
    )
    .await?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for worker in workers.iter().flatten() {
                ensure!(!worker.status().needs_operator, "{:?}", worker.status());
            }
            if !control
                .transition_status(&namespace, &request.operation_id, TIMEOUT)
                .await?
                .fleet_slot_held
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let elected = leader(&cluster.hosts[..3], merged).await?;
    for (i, cell) in seed.cells.iter().enumerate() {
        let handle = cluster.hosts[elected].cell(cell.clone())?;
        let duplicate = handle
            .command(
                &handle.contract("Add")?,
                [7i64, 11][i].to_be_bytes().to_vec(),
                CommandOptions::new(seed.receipts[i].id, seed.retry),
            )
            .await?;
        ensure!(
            duplicate.id == seed.receipts[i].id
                && duplicate.result == seed.receipts[i].result
                && duplicate.version == latest[i].version
                && duplicate.witness == latest[i].witness
                && duplicate.version.covers(&seed.receipts[i].version) == Ok(true),
            "original receipt with exact pre-merge covering version: {duplicate:?}"
        );
        let read = handle
            .query(
                ProposalId::new([211 + i as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(seed.receipts[i].version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            read.result == latest[i].result
                && read.version == latest[i].version
                && read.witness == latest[i].witness
                && read.served_by_node == cluster.hosts[elected].identity()
        );
        // This is a new mutation after both moves, with its own original
        // 60-second retry window. The duplicate above keeps the seed deadline.
        let retry = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?
        .checked_add(60_000)
        .context("new post-merge command retry deadline")?;
        let latest = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions {
                    durability: Durability::S3,
                    ..CommandOptions::new(CommandId::new(ClientId::new([207; 16])?, 1)?, retry)
                },
            )
            .await?;
        ensure!(
            latest.result == [10i64, 14][i].to_be_bytes()
                && latest.witness.group == merged
                && latest.durability == Durability::S3
        );
    }
    let values = operations(cluster).await?;
    for (i, host) in cluster.hosts.iter().enumerate() {
        let sample = host
            .inspect_peer_physical_inventory(host.identity().node, 1, TIMEOUT)
            .await?;
        ensure!(sample.view.node.groups == u32::from(i < 3));
        let floor = record(
            values
                .get(&format!(
                    "resource-floor/{}",
                    hex::encode(host.identity().node.bytes())
                ))
                .context("second-cycle floor")?,
            64,
        )?;
        ensure!(floor["physical"]["engine_slots"] == u32::from(i < 3));
    }
    Ok(())
}

pub(super) async fn resume_final_participant(
    cluster: &ManagedCluster,
    rooted: bool,
    roots: &mut Vec<tempfile::TempDir>,
    workers: &mut [Option<RaftFleetTransitionService>],
    coordinator: &RaftFleetControl,
) -> Result<()> {
    let operation = hex::encode([231; 16]);
    let namespace = hex::encode(cluster.history.namespace.bytes());
    // One source has an external cleanup owner. The rooted intent must prevent
    // completion after other members have already erased their engine histories.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let values = operations(cluster).await?;
            if let Some(bytes) = values.get(&format!("source-cleanup/{operation}")) {
                let cleanup = record(bytes, 236)?;
                let nodes = cleanup["nodes"]
                    .as_array()
                    .context("pending cleanup nodes")?;
                if nodes.iter().filter(|n| n["settled"] == true).count() == 2 {
                    ensure!(nodes.len() == 3);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let catalog = &cluster.fixtures[0].catalog;
    let snapshot = catalog.read(TIMEOUT).await?;
    let transition = catalog
        .reopen_managed_transition(&cluster.directory, ProposalId::new([231; 16])?, TIMEOUT)
        .await?
        .context("selected cleanup transition")?;
    let mut complete = managed_fixture::request(209, snapshot.revision());
    complete.operation = [231; 16];
    complete.principal = "automatic-test".into();
    let blocked = catalog
        .complete_managed_transfer(
            &snapshot,
            &complete,
            &transition,
            &cluster.directory,
            TIMEOUT,
        )
        .await;
    ensure!(
        matches!(
            blocked,
            Err(cellar::application::consensus::verification::FleetCatalogError::Unconfirmed)
        ),
        "pending cleanup must retain transition ownership"
    );
    let forged = cellar::application::consensus::verification::FleetMutation {
        id: [210; 16],
        operation: [231; 16],
        principal: "automatic-test".into(),
        expected_revision: snapshot.revision(),
        changes: vec![cellar::application::consensus::verification::FleetChange {
            table: FleetTable::Operations,
            key: format!("source-cleanup/{operation}"),
            value: Some(vec![1]),
        }],
    };
    ensure!(matches!(
        catalog.commit(&snapshot, &forged, TIMEOUT).await,
        Err(cellar::application::consensus::verification::FleetCatalogError::Invalid(_))
    ));
    ensure!(catalog.read(TIMEOUT).await?.revision() == snapshot.revision());
    ensure!(
        coordinator
            .transition_status(&namespace, &operation, TIMEOUT)
            .await?
            .fleet_slot_held
    );
    workers[3]
        .take()
        .context("first cleanup coordinator")?
        .shutdown()
        .await;
    workers[2]
        .take()
        .context("external source owner")?
        .shutdown()
        .await;
    // Only the resumed local participant can write the next Fleet root. Lose
    // its response after the actual settlement CAS, including confirmation.
    cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let local = Box::pin(control(cluster, rooted, roots)).await?;
    workers[2] = Some(
        local
            .start_transition_service(&cluster.hosts[2], options(false, true, true), TIMEOUT)
            .await?,
    );
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let values = operations(cluster).await?;
            let cleanup = record(
                values
                    .get(&format!("source-cleanup/{operation}"))
                    .context("durable cleanup")?,
                236,
            )?;
            let all = cleanup["nodes"]
                .as_array()
                .context("cleanup owners")?
                .iter()
                .all(|n| n["settled"] == true);
            if all
                && !cluster.fixtures[0]
                    .store
                    .fail_fleet_confirmation
                    .load(Ordering::Acquire)
                && !cluster.fixtures[0]
                    .store
                    .fail_fleet_read
                    .load(Ordering::Acquire)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        coordinator
            .transition_status(&namespace, &operation, TIMEOUT)
            .await?
            .fleet_slot_held
    );
    workers[2]
        .take()
        .context("lost-response source owner")?
        .shutdown()
        .await;
    let local = Box::pin(control(cluster, rooted, roots)).await?;
    workers[2] = Some(
        local
            .start_transition_service(&cluster.hosts[2], options(false, true, true), TIMEOUT)
            .await?,
    );
    let replacement = Box::pin(control(cluster, rooted, roots)).await?;
    workers[3] = Some(
        replacement
            .start_transition_service(&cluster.hosts[3], options(true, true, true), TIMEOUT)
            .await?,
    );
    Ok(())
}
