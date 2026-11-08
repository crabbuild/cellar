// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configured RF3 placement drives actual SQL, archives, split and merge.
use super::*;
use cellar::application::consensus::verification::FleetTable;
use cellar_logic::cell_group::CellId;

fn configuration(reserve: u32) -> RaftFleetAutomaticPartitionOptions {
    let mut sizing = selection::policy().limits();
    sizing.split_ceiling.cells = 8;
    sizing.split_ceiling.writes_per_second = 4;
    sizing.merge_ceiling.cells = 4;
    sizing.merge_ceiling.writes_per_second = 2;
    RaftFleetAutomaticPartitionOptions {
        placement: RaftFleetPartitionPlacementPolicy {
            pool: "test".into(),
            distinct_regions: false,
            distinct_zones: false,
            recovery_slots: reserve,
        },
        sizing: PartitionPolicy::new(sizing).expect("explicit automatic sizing policy"),
        service: RaftFleetPartitionServiceOptions {
            principal: "automatic-test".into(),
            max_groups: 2,
            execution: RaftFleetPartitionExecutionPolicy::Complete,
            interval: Duration::from_millis(50),
            retry_max_interval: Duration::from_millis(250),
            turn_timeout: TIMEOUT,
            transition: options(true, true, true),
        },
        recovery: CellGroupRecoveryLimits {
            max_retained_bytes: 32 * 1024 * 1024,
            max_cells: 32,
            max_image_bytes: 32 * 1024 * 1024,
            max_restore_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_partitions_split_merge_and_resume() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            3,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_placement_preserves_recovery_reserve() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            4,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_automatic_partitions_split_merge_and_resume() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            3,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "automatic-partitions",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_automatic_placement_preserves_recovery_reserve() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            4,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "automatic-reserve",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

async fn qualify(rooted: bool, reserve: u32, store: Arc<catalog_fault::Store>) -> Result<()> {
    let cluster = Box::pin(ManagedCluster::start_with_partitions(
        rooted,
        &[7, 8, 9],
        store,
        configuration(reserve),
    ))
    .await?;
    let result = cluster.fixtures[0]
        .domain
        .run(Box::pin(exercise(&cluster, reserve)))
        .await;
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    ensure!(
        cluster
            .hosts
            .iter()
            .all(|h| h.partition_service_status().is_none_or(|s| !s.running)),
        "node close did not join automatic controllers"
    );
    cluster.fixtures[0].domain.shutdown().await;
    result
        .context("automatic verification domain")?
        .with_context(|| {
            format!("automatic partitions, extents={rooted}, reserve={reserve}; joined={joined:?}")
        })?;
    joined?;
    Ok(())
}

fn statuses(cluster: &ManagedCluster) -> Vec<Option<RaftFleetPartitionServiceStatus>> {
    cluster
        .hosts
        .iter()
        .map(|h| h.partition_service_status())
        .collect()
}

async fn active(cluster: &ManagedCluster) -> Result<Option<Vec<u8>>> {
    let f = &cluster.fixtures[0];
    let cut = f.catalog.read(TIMEOUT).await?;
    Ok(f.catalog
        .get(
            &cut,
            FleetTable::Operations,
            "partition-work/active",
            TIMEOUT,
        )
        .await?)
}

async fn exercise(cluster: &ManagedCluster, reserve: u32) -> Result<()> {
    // Pause execution, leaving automatic sizing/placement on the source voters.
    // No test callback supplies a child identity, destination or operator plan.
    ensure!(statuses(cluster).iter().all(Option::is_some));
    cluster.hosts[3].suspend_automatic_partitions().await;
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let initial = cluster.directory.observe().await?;
    let first = leader(&cluster.hosts[..3], cluster.history).await?;
    let handle = cluster.hosts[first].cell(seed.cells[0].clone())?;
    let mut acknowledged = Vec::new();
    let client = ClientId::new([212; 16])?;
    let mut sequence = 1;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = statuses(cluster);
            ensure!(
                status.iter().flatten().all(|s| !s.needs_operator),
                "{status:?}"
            );
            let done = if reserve == 3 {
                active(cluster).await?.is_some()
            } else {
                status.iter().flatten().any(|s| {
                    s.last_failure
                        .as_ref()
                        .is_some_and(|e| e.message.contains("No RF3 placement"))
                })
            };
            if done {
                return Ok::<_, anyhow::Error>(());
            }
            let receipt = handle
                .command(
                    &handle.contract("Add")?,
                    1i64.to_be_bytes().to_vec(),
                    CommandOptions::new(CommandId::new(client, sequence)?, seed.retry),
                )
                .await?;
            acknowledged.push(receipt);
            sequence += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("automatic selection: {:?}", statuses(cluster)))??;
    ensure!(!acknowledged.is_empty());
    for host in &cluster.hosts {
        host.suspend_automatic_partitions().await;
    }
    if reserve == 4 {
        ensure!(
            active(cluster).await?.is_none(),
            "reserve refusal admitted work"
        );
        ensure!(cluster.directory.observe().await?.digest() == initial.digest());
        let read = handle
            .query(
                ProposalId::new([213; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(
            read.result
                == acknowledged
                    .last()
                    .context("last acknowledged write")?
                    .result
        );
        return Ok(());
    }
    let selected = active(cluster)
        .await?
        .context("durable automatic selection")?;
    ensure!(cluster.directory.observe().await?.groups() == 1);
    // Rebuild every configured controller with fresh volatile state. Neither
    // request IDs nor private verifier stores are passed to the resume API.
    cluster.hosts[0]
        .resume_automatic_partitions(TIMEOUT)
        .await?;
    ensure!(active(cluster).await?.as_ref() == Some(&selected));
    for host in cluster.hosts.iter().skip(1) {
        host.resume_automatic_partitions(TIMEOUT).await?;
    }
    let split = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = statuses(cluster);
            ensure!(
                status.iter().flatten().all(|s| !s.needs_operator),
                "{status:?}"
            );
            let view = cluster.directory.observe().await?;
            if view.groups() == 2 {
                return Ok::<_, anyhow::Error>(view);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("configured automatic split")??;
    let mut children = BTreeSet::new();
    for cell in &seed.cells {
        children.insert(
            cluster
                .directory
                .lookup(&split, &CellId::new(cell.cell.as_str())?)
                .await?
                .history,
        );
    }
    ensure!(children.len() == 2 && !children.contains(&cluster.history));
    // Load has stopped. The same configured owner merges quiet adjacent groups
    // after its normal cooldown/hysteresis and rooted source cleanup complete.
    let merged = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = statuses(cluster);
            ensure!(
                status.iter().flatten().all(|s| !s.needs_operator),
                "{status:?}"
            );
            let view = cluster.directory.observe().await?;
            if view.groups() == 1
                && view.revision() > split.revision()
                && active(cluster).await?.is_none()
            {
                return Ok::<_, anyhow::Error>(view);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("configured quiet merge: {:?}", statuses(cluster)))??;
    for host in &cluster.hosts {
        host.suspend_automatic_partitions().await;
    }
    let merged_history = cluster
        .directory
        .lookup(&merged, &CellId::new(seed.cells[0].cell.as_str())?)
        .await?
        .history;
    ensure!(merged_history != cluster.history && !children.contains(&merged_history));
    ensure!(seed
        .cells
        .iter()
        .all(|cell| cell.namespace == merged_history.namespace));
    for (i, cell) in seed.cells.iter().enumerate() {
        let handle = cluster.hosts[0].cell(cell.clone())?;
        let original = &seed.receipts[i];
        let replay = handle
            .command(
                &handle.contract("Add")?,
                [7i64, 11][i].to_be_bytes().to_vec(),
                CommandOptions::new(original.id, seed.retry),
            )
            .await?;
        ensure!(replay.result == original.result && replay.version == original.version);
        if i == 0 {
            for original in &acknowledged {
                let replay = handle
                    .command(
                        &handle.contract("Add")?,
                        1i64.to_be_bytes().to_vec(),
                        CommandOptions::new(original.id, seed.retry),
                    )
                    .await?;
                ensure!(replay.result == original.result && replay.version == original.version);
            }
        }
        let expected = if i == 0 {
            acknowledged.last().context("last write")?
        } else {
            original
        };
        let read = handle
            .query(
                ProposalId::new([214 + i as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(read.result == expected.result && read.version == expected.version);
        let write = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(
                    CommandId::new(ClientId::new([215; 16])?, i as u64 + 1)?,
                    seed.retry,
                ),
            )
            .await?;
        ensure!(write.version.sequence.get() == expected.version.sequence.get() + 1);
    }
    Ok(())
}
