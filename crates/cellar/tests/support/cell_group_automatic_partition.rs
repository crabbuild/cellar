// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configured RF3 placement drives actual SQL, archives, split and merge.
use super::*;
use cellar::application::consensus::verification::FleetTable;
use cellar_logic::cell_group::CellId;

fn configuration(reserve: u32) -> RaftFleetAutomaticPartitionOptions {
    let mut sizing = selection::policy().limits();
    sizing.split_ceiling.cells = 8;
    sizing.split_ceiling.writes_per_second = 1;
    sizing.merge_ceiling.cells = 4;
    sizing.merge_ceiling.writes_per_second = 0;
    sizing.sample_interval_ms = 1_000;
    sizing.cooldown_ms = 5_000;
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
            // Two live cells; leave room for the recovery pool's three extent buffers.
            max_cells: 16,
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
    let handles = seed
        .cells
        .iter()
        .map(|c| cluster.hosts[first].cell(c.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut acknowledged = [Vec::new(), Vec::new()];
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
                    && status.iter().flatten().any(|s| s.operation_id.is_some())
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
            // Each cell admits one mutation owner. Load both cells in parallel
            // within the installed two-command bound and retain every receipt.
            let writes = handles
                .iter()
                .enumerate()
                .map(|(slot, handle)| {
                    Ok(handle.command(
                        &handle.contract("Add")?,
                        1i64.to_be_bytes().to_vec(),
                        CommandOptions::new(
                            CommandId::new(ClientId::new([212 + slot as u8; 16])?, sequence)?,
                            seed.retry,
                        ),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            for (slot, result) in futures_util::future::join_all(writes)
                .await
                .into_iter()
                .enumerate()
            {
                acknowledged[slot].push(
                    result.with_context(|| format!("automatic load: {:?}", statuses(cluster)))?,
                );
            }
            sequence += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("automatic selection: {:?}", statuses(cluster)))??;
    ensure!(acknowledged.iter().all(|receipts| !receipts.is_empty()));
    let original_operation = statuses(cluster)
        .into_iter()
        .flatten()
        .find_map(|s| s.operation_id);
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
                == acknowledged[0]
                    .last()
                    .context("last acknowledged write")?
                    .result
        );
        let next = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(
                    CommandId::new(ClientId::new([212; 16])?, sequence)?,
                    seed.retry,
                ),
            )
            .await?;
        let last = acknowledged[0].last().context("last acknowledged write")?;
        ensure!(next.version.sequence.get() == last.version.sequence.get() + 1);
        return Ok(());
    }
    let selected = active(cluster)
        .await?
        .context("durable automatic selection")?;
    let original_operation = original_operation.context("selected operation status")?;
    let f = &cluster.fixtures[0];
    let control = RaftFleetControl::new(
        &f.domain,
        f.catalog.clone(),
        vec![cluster.directory.clone()],
    )?;
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
            if view.groups() == 2
                && control
                    .transition_status(
                        &hex::encode(cluster.history.namespace.bytes()),
                        &original_operation,
                        TIMEOUT,
                    )
                    .await?
                    .phase
                    == Some(TransitionPhase::Retired)
            {
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
    for host in &cluster.hosts {
        host.suspend_automatic_partitions().await;
    }
    ensure!(
        active(cluster)
            .await?
            .as_ref()
            .is_none_or(|work| work == &selected),
        "merge selected before leader separation"
    );
    let histories: Vec<_> = children.iter().copied().collect();
    let left = current_leader(cluster, histories[0]).await?;
    let right = current_leader(cluster, histories[1]).await?;
    if left == right {
        let cut = f.catalog.read(TIMEOUT).await?;
        let placement = f
            .catalog
            .history(&cut, &histories[1], TIMEOUT)
            .await?
            .initial_placement()?;
        let target = placement
            .voters
            .into_iter()
            .find(|v| v.node != cluster.fixtures[left].spec.node)
            .context("separate child leader")?;
        let expected = cluster
            .fixtures
            .iter()
            .position(|f| f.spec.node == target.node)
            .context("target node")?;
        let group = cluster.hosts[right].group(histories[1])?;
        let progress = group.replication_status(TIMEOUT).await?;
        group
            .transfer_leadership(
                LeadershipTransfer {
                    target: target.member,
                    expected_term: progress.term,
                    expected_revision: progress.membership_revision,
                },
                TIMEOUT,
            )
            .await?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if current_leader(cluster, histories[1]).await? == expected {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("separate child leaders")??;
    }
    ensure!(
        current_leader(cluster, histories[0]).await?
            != current_leader(cluster, histories[1]).await?,
        "merge must work across different leaders"
    );
    for host in &cluster.hosts {
        host.resume_automatic_partitions(TIMEOUT).await?;
    }
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
    ensure!(
        statuses(cluster)
            .iter()
            .flatten()
            .any(|s| s.remote_samples > 0),
        "merge did not consume remote leader observations"
    );
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
        for original in &acknowledged[i] {
            let replay = handle
                .command(
                    &handle.contract("Add")?,
                    1i64.to_be_bytes().to_vec(),
                    CommandOptions::new(original.id, seed.retry),
                )
                .await?;
            ensure!(replay.result == original.result && replay.version == original.version);
        }
        let expected = acknowledged[i].last().context("last write")?;
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

// Child member numbers belong to their own fresh RF3 placement, not the
// fixture's global host-vector indices. Resolve that actual persisted map.
async fn current_leader(cluster: &ManagedCluster, history: GroupHistory) -> Result<usize> {
    let f = &cluster.fixtures[0];
    let cut = f.catalog.read(TIMEOUT).await?;
    let placement = f
        .catalog
        .history(&cut, &history, TIMEOUT)
        .await?
        .initial_placement()?;
    let members = placement
        .voters
        .into_iter()
        .map(|v| {
            let index = cluster
                .fixtures
                .iter()
                .position(|f| f.spec.node == v.node)
                .context("mapped voter node")?;
            Ok((index, v.member))
        })
        .collect::<Result<Vec<_>>>()?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for (index, member) in &members {
                let status = cluster.hosts[*index].group(history)?.status().await?;
                if status.ready && status.leader == Some(member.get()) {
                    return Ok::<_, anyhow::Error>(*index);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("automatic child leader")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sampling_retains_fractional_write_pressure() -> Result<()> {
    use cellar::application::consensus::CellGroupPartitionContext;
    for rooted in [false, true] {
        let cluster = Box::pin(ManagedCluster::start(rooted, &[])).await?;
        let result = async {
            let _seed = seed_cells(&cluster.hosts, cluster.history).await?;
            let first = leader(&cluster.hosts, cluster.history).await?;
            let mut current = cluster.hosts[first]
                .group(cluster.history)?
                .observe_partition(TIMEOUT)
                .await?;
            ensure!(current.stable_leader);
            let mut earlier = current.clone();
            earlier.workload.sampled_at_ms = 1_000;
            earlier.workload.admitted_writes = 2;
            earlier.workload.applied_writes = 2;
            current.workload.sampled_at_ms = 2_500;
            current.workload.admitted_writes = 4;
            current.workload.applied_writes = 4;
            let context = CellGroupPartitionContext {
                directory_digest: [1; 32],
                topology_changed_at_ms: 0,
                durability: Durability::Quorum,
                placement_class: [2; 32],
                leader_cpu_bps: 0,
                failure_domain_load_bps: 0,
                active_and_stable: true,
            };
            let sample = current
                .sample_since(&earlier, context)
                .context("same owner sample")?;
            ensure!(sample.load.writes_per_second == 2 && sample.applies_per_second == 2);
            earlier = current.clone();
            current.workload.sampled_at_ms += 1_500;
            let quiet = current
                .sample_since(&earlier, context)
                .context("quiet sample")?;
            ensure!(quiet.load.writes_per_second == 0 && quiet.applies_per_second == 0);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result?;
        joined?;
    }
    Ok(())
}
