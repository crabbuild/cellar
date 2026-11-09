// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configured RF3 placement drives actual SQL, archives, split and merge.
use super::*;
use cellar::application::consensus::verification::{FleetCatalog, FleetTable};
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
async fn automatic_disjoint_partitions_merge_and_resume() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify_placement(
            rooted,
            3,
            Arc::new(catalog_fault::Store::default()),
            true,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_sampling_interval_bounds_metadata() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify_sampling(
            rooted,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_partition_sampling_interval_bounds_metadata() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify_sampling(
            rooted,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "partition-sampling",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

async fn qualify_sampling(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let f = &cluster.fixtures[0];
    let result = f
        .domain
        .run(Box::pin(async {
            let _seed = seed_cells(&cluster.hosts, cluster.history).await?;
            let elected = leader(&cluster.hosts[..3], cluster.history).await?;
            let control = RaftFleetControl::new(
                &f.domain,
                managed_fixture::Fixture::open_catalog(
                    &f.domain,
                    f.store.clone(),
                    cluster.history.cluster,
                )?,
                vec![cluster.directory.clone()],
            )?;
            // Test the sampling boundary on a stable group, without transition
            // ownership or newly activated leader discovery in the assertion.
            for node in [elected, 3] {
                let mut sampling = RaftFleetPartitionScheduler::new(
                    control
                        .clone()
                        .with_node_observer(cluster.hosts[node].clone()),
                    configuration(3).sizing,
                    1,
                )?;
                let pressure = RaftFleetPartitionPressure {
                    placement_class: [231; 32],
                    leader_cpu_bps: 0,
                    failure_domain_load_bps: 0,
                };
                ensure!(sampling
                    .observe(cluster.history, pressure, TIMEOUT)
                    .await?
                    .is_none());
                let samples = sampling.remote_samples();
                ensure!(samples == u64::from(node == 3));
                let (early, reads) = catalog_fault::count_reads(sampling.observe(
                    cluster.history,
                    pressure,
                    TIMEOUT,
                ))
                .await;
                ensure!(
                    early?.is_none() && reads.is_empty() && sampling.remote_samples() == samples,
                    "early policy sampling performed catalog or peer work: {reads:?}"
                );
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await;
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    f.domain.shutdown().await;
    result
        .context("sampling execution domain")?
        .with_context(|| format!("sampling interval, extents={rooted}"))?;
    joined?;
    eprintln!(
        "sampling interval: extents={rooted}, local and remote repeat perform no metadata work"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_automatic_disjoint_partitions_merge_and_resume() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify_placement(
            rooted,
            3,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "automatic-disjoint-partitions",
                rooted,
            )?)),
            true,
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
    Box::pin(qualify_placement(rooted, reserve, store, false)).await
}

async fn qualify_placement(
    rooted: bool,
    reserve: u32,
    store: Arc<catalog_fault::Store>,
    disjoint: bool,
) -> Result<()> {
    // Six empty destinations force the standard joint planner to spread the
    // children over disjoint RF3 sets. Each node keeps the normal two slots,
    // image/byte limits and recovery reserve; no callback selects placement.
    let spares: &[u8] = if disjoint {
        &[7, 8, 9, 10, 11, 12]
    } else {
        &[7, 8, 9]
    };
    let cluster = Box::pin(ManagedCluster::start_with_partitions(
        rooted,
        spares,
        store,
        configuration(reserve),
    ))
    .await?;
    eprintln!("automatic RF3: extents={rooted}, reserve={reserve}, startup complete");
    let result = cluster.fixtures[0]
        .domain
        .run(Box::pin(exercise(&cluster, reserve, disjoint)))
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

async fn active(catalog: &FleetCatalog) -> Result<Option<Vec<u8>>> {
    let cut = catalog.read(TIMEOUT).await?;
    Ok(catalog
        .get(
            &cut,
            FleetTable::Operations,
            "partition-work/active",
            TIMEOUT,
        )
        .await?)
}

async fn exercise(cluster: &ManagedCluster, reserve: u32, disjoint: bool) -> Result<()> {
    let f = &cluster.fixtures[0];
    // Status reads own a client; they must not compete for a voter's single
    // catalog turn with its trust and authenticated membership requests.
    let observer = managed_fixture::Fixture::open_catalog(
        &f.domain,
        f.store.clone(),
        cluster.history.cluster,
    )?;
    // Node zero is the fixed public ingress, including when it is a follower
    // or has no local child. Membership grants no application-origin rights.
    let cut = observer.read(TIMEOUT).await?;
    let origin = cluster.peers[0].clone();
    for host in cluster.hosts.iter().skip(1) {
        managed_fixture::inspect_current(host, origin.node, cut.revision()).await?;
        host.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: origin.clone(),
        })?;
        host.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: origin.clone(),
        })?;
    }
    drop(cut);
    // Pause execution, leaving automatic sizing/placement on the source voters.
    // No test callback supplies a child identity, destination or operator plan.
    ensure!(statuses(cluster).iter().all(Option::is_some));
    cluster.hosts[3].suspend_automatic_partitions().await;
    let seed = seed_cells(&cluster.hosts, cluster.history)
        .await
        .context("seed original cells")?;
    eprintln!("automatic RF3: original cells seeded");
    let initial = cluster.directory.observe().await?;
    let handle = cluster.hosts[0].cell(seed.cells[0].clone())?;
    let handles = seed
        .cells
        .iter()
        .map(|c| cluster.hosts[0].cell(c.clone()))
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
                active(&observer).await?.is_some()
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
    eprintln!("automatic RF3: load reached selection or reserve refusal");
    let original_operation = statuses(cluster)
        .into_iter()
        .flatten()
        .find_map(|s| s.operation_id);
    for host in &cluster.hosts {
        host.suspend_automatic_partitions().await;
    }
    if reserve == 4 {
        ensure!(
            active(&observer).await?.is_none(),
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
    let selected = active(&observer)
        .await?
        .context("durable automatic selection")?;
    let original_operation = original_operation.context("selected operation status")?;
    let control =
        RaftFleetControl::new(&f.domain, observer.clone(), vec![cluster.directory.clone()])?;
    ensure!(cluster.directory.observe().await?.groups() == 1);
    // Rebuild every configured controller with fresh volatile state. Neither
    // request IDs nor private verifier stores are passed to the resume API.
    cluster.hosts[0]
        .resume_automatic_partitions(TIMEOUT)
        .await?;
    ensure!(active(&observer).await?.as_ref() == Some(&selected));
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
        active(&observer)
            .await?
            .as_ref()
            .is_none_or(|work| work == &selected),
        "merge selected before leader separation"
    );
    eprintln!("automatic RF3: split retired after controller replacement");
    let histories: Vec<_> = children.iter().copied().collect();
    if disjoint {
        let cut = observer.read(TIMEOUT).await?;
        let mut placements = Vec::new();
        for history in &histories {
            let voters = observer
                .history(&cut, history, TIMEOUT)
                .await?
                .initial_placement()?;
            placements.push(
                voters
                    .voters
                    .into_iter()
                    .map(|v| v.node)
                    .collect::<BTreeSet<_>>(),
            );
        }
        ensure!(
            placements[0].len() == 3 && placements[0].is_disjoint(&placements[1]),
            "automatic split did not produce disjoint RF3 source sets: {placements:?}"
        );
        ensure!(
            cluster.hosts.iter().all(|host| {
                !(host.group(histories[0]).is_ok() && host.group(histories[1]).is_ok())
            }),
            "a controller has both merge parents locally"
        );
        eprintln!("automatic RF3: disjoint source voters confirmed");
    }
    let left = current_leader(cluster, histories[0], &observer).await?;
    let right = current_leader(cluster, histories[1], &observer).await?;
    if left == right {
        let cut = observer.read(TIMEOUT).await?;
        let placement = observer
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
                if current_leader(cluster, histories[1], &observer).await? == expected {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("separate child leaders")??;
    }
    ensure!(
        current_leader(cluster, histories[0], &observer).await?
            != current_leader(cluster, histories[1], &observer).await?,
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
                && active(&observer).await?.is_none()
            {
                return Ok::<_, anyhow::Error>(view);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("configured quiet merge: {:?}", statuses(cluster)))??;
    eprintln!("automatic RF3: different-leader merge retired");
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
    let elected = current_leader(cluster, merged_history, &observer).await?;
    cluster.hosts[elected]
        .group(merged_history)?
        .quorum_membership(TIMEOUT)
        .await?;
    ensure!(seed
        .cells
        .iter()
        .all(|cell| cell.namespace == merged_history.namespace));
    for (i, cell) in seed.cells.iter().enumerate() {
        let handle = cluster.hosts[0].cell(cell.clone())?;
        let expected = acknowledged[i].last().context("last write")?;
        replay_acknowledged(
            &handle,
            &seed.receipts[i],
            [7i64, 11][i],
            seed.retry,
            merged_history,
            expected,
        )
        .await?;
        for original in &acknowledged[i] {
            replay_acknowledged(&handle, original, 1, seed.retry, merged_history, expected).await?;
        }
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
                    CommandId::new(ClientId::new([215; 16])?, 1)?,
                    retry_horizon()?,
                ),
            )
            .await?;
        ensure!(write.version.sequence.get() == expected.version.sequence.get() + 1);
        let previous = i64::from_be_bytes(expected.result.as_slice().try_into()?);
        ensure!(write.result == (previous + 1).to_be_bytes());
    }
    eprintln!("automatic RF3: full receipt ledger, SQL state and fresh quorum writes verified");
    Ok(())
}

// Child member numbers belong to their own fresh RF3 placement, not the
// fixture's global host-vector indices. Resolve that actual persisted map.
async fn current_leader(
    cluster: &ManagedCluster,
    history: GroupHistory,
    catalog: &FleetCatalog,
) -> Result<usize> {
    let cut = catalog.read(TIMEOUT).await?;
    let placement = catalog
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

// A late new identity owns a fresh retry horizon. Replaying a stored command
// below always retains that original command's horizon and input.
fn retry_horizon() -> Result<u64> {
    u64::try_from(cellar::asyncrt::wall_ms())?
        .checked_add(60_000)
        .context("new command retry horizon")
}

async fn replay_acknowledged(
    handle: &RaftCellHandle,
    original: &CommandReceipt,
    delta: i64,
    retry: u64,
    history: GroupHistory,
    latest: &CommandReceipt,
) -> Result<()> {
    use cellar::application::consensus::verification::TransportError;
    use cellar::application::consensus::{CellDispatchError, CellReceiptRequest};
    let started = std::time::Instant::now();
    let remaining = || {
        TIMEOUT
            .checked_sub(started.elapsed())
            .filter(|d| !d.is_zero())
            .context("receipt replay deadline")
    };
    let contract = handle.contract("Add")?;
    let input = delta.to_be_bytes().to_vec();
    let mut options = CommandOptions::new(original.id, retry);
    options.timeout = remaining()?;
    let replay = match handle
        .command(&contract, input.clone(), options.clone())
        .await
    {
        Ok(receipt) => receipt,
        Err(error)
            if error.ambiguous
                && matches!(
                    error.cause.as_ref(),
                    CellDispatchError::Transport(TransportError::Io(_))
                ) =>
        {
            // The SDK never repeats a transmitted mutation automatically. This
            // caller knows its original acknowledgement and first proves the
            // exact stored result through the current group's quorum path.
            ensure!(error.command == original.id);
            let observed = handle
                .receipt(
                    CellReceiptRequest {
                        id: original.id,
                        contract: contract.clone(),
                        input: input.clone(),
                        durability: Durability::Quorum,
                    },
                    remaining()?,
                )
                .await?;
            let stored = observed
                .receipt()
                .context("acknowledged receipt missing after handoff")?;
            ensure!(
                stored.command() == original.id && stored.result() == original.result.as_slice()
            );
            ensure!(
                observed.durability() == Durability::Quorum
                    && observed.observation().version == latest.version
                    && observed.observation().witness == latest.witness,
                "receipt durability={:?}; observation={:?}; latest={:?}; group={:?}",
                observed.durability(),
                observed.observation(),
                latest.version,
                history
            );
            eprintln!(
                "quorum reconciled acknowledged command {:?} after connection handoff",
                original.id
            );
            drop(observed);
            options.timeout = remaining()?;
            handle.command(&contract, input, options).await?
        }
        Err(error) => return Err(error.into()),
    };
    ensure!(replay.id == original.id && replay.result == original.result);
    ensure!(
        replay.version == latest.version && replay.version.covers(&original.version) == Ok(true)
    );
    // Decision locators remain historical across transfer. The merged owner
    // independently supplies fresh quorum before this ledger verification.
    ensure!(replay.witness == latest.witness && replay.durability == Durability::Quorum);
    Ok(())
}
