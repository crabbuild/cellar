// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded control work over actual groups created by the standard planner.
use super::*;
use cellar::application::consensus::verification::{FleetCatalog, FleetTable};
use cellar_logic::cell_group::{CellId, GroupRoute};
use std::sync::Arc;

fn configuration() -> RaftFleetAutomaticPartitionOptions {
    let mut limits = policy(64 * 1024 * 1024).limits();
    limits.split_ceiling.cells = 16;
    limits.merge_ceiling.cells = 0;
    limits.merge_ceiling.writes_per_second = 0;
    limits.sample_interval_ms = 1_000;
    limits.cooldown_ms = 1_000;
    RaftFleetAutomaticPartitionOptions {
        placement: RaftFleetPartitionPlacementPolicy {
            pool: "test".into(),
            distinct_regions: false,
            distinct_zones: false,
            recovery_slots: 3,
        },
        sizing: PartitionPolicy::new(limits).expect("explicit fanout sizing policy"),
        service: RaftFleetPartitionServiceOptions {
            principal: "fanout-test".into(),
            max_groups: 2,
            execution: RaftFleetPartitionExecutionPolicy::Complete,
            interval: Duration::from_millis(50),
            retry_max_interval: Duration::from_millis(250),
            turn_timeout: TIMEOUT,
            transition: RaftFleetTransitionServiceOptions {
                interval: Duration::from_millis(50),
                retry_max_interval: Duration::from_millis(250),
                turn_timeout: TIMEOUT,
                ..Default::default()
            },
        },
        recovery: CellGroupRecoveryLimits {
            // The 64-cell campaign uses the existing 32-cell checkpoint count.
            // Byte limits and physical per-node admission remain unchanged.
            max_cells: 32,
            max_retained_bytes: 32 * 1024 * 1024,
            max_image_bytes: 32 * 1024 * 1024,
            max_restore_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
    }
}

pub(super) async fn qualify(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    // Fifteen nodes retain the existing two slots and physical attestation.
    // The standard planner must preserve three recovery slots while splitting.
    let spares: Vec<_> = (7..=18).collect();
    let cluster = Box::pin(ManagedCluster::start_with_partitions(
        rooted,
        &spares,
        store,
        configuration(),
    ))
    .await?;
    let result = cluster.fixtures[0]
        .domain
        .run(Box::pin(exercise(&cluster)))
        .await;
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result
        .context("fanout execution domain")?
        .with_context(|| format!("bounded fanout, extents={rooted}; joined={joined:?}"))?;
    joined?;
    Ok(())
}

async fn routes(cluster: &ManagedCluster) -> Result<Vec<GroupRoute>> {
    let view = cluster.directory.observe().await?;
    let mut after = None;
    let mut complete = Vec::new();
    loop {
        let page = cluster
            .directory
            .group_page(&view, after.as_ref(), 2)
            .await?;
        ensure!(
            page.routes.len() <= 2,
            "directory exceeded the requested page bound"
        );
        complete.extend(page.routes);
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    ensure!(complete.len() == view.groups() as usize);
    Ok(complete)
}

async fn current_leader(
    cluster: &ManagedCluster,
    route: &GroupRoute,
    catalog: &FleetCatalog,
) -> Result<usize> {
    let cut = catalog.read(TIMEOUT).await?;
    let voters = catalog
        .history(&cut, &route.history, TIMEOUT)
        .await?
        .initial_placement()?
        .voters;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for voter in &voters {
                let node = cluster
                    .hosts
                    .iter()
                    .position(|h| h.identity().node == voter.node)
                    .context("voter endpoint")?;
                let status = cluster.hosts[node].group(route.history)?.status().await?;
                if status.ready && status.leader == Some(voter.member.get()) {
                    return Ok::<_, anyhow::Error>(node);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("fanout group leader")?
}

async fn settled(cluster: &ManagedCluster, catalog: &FleetCatalog) -> Result<Vec<GroupRoute>> {
    // Each batch must complete through the same 60-second transition envelope;
    // it cannot keep adding cells to an unbounded source awaiting a split.
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let states: Vec<_> = cluster
                .hosts
                .iter()
                .filter_map(RaftCells::partition_service_status)
                .collect();
            ensure!(
                states.len() == cluster.hosts.len() && states.iter().all(|s| !s.needs_operator),
                "{states:?}"
            );
            ensure!(
                states.iter().all(|s| s.local_groups <= 2
                    && s.tracked_groups <= 4
                    && s.inventory_pages <= s.rounds.saturating_add(1)),
                "unbounded scheduler inventory: {states:?}"
            );
            let cut = catalog.read(TIMEOUT).await?;
            let active = catalog
                .get(
                    &cut,
                    FleetTable::Operations,
                    "partition-work/active",
                    TIMEOUT,
                )
                .await?;
            if active.is_none() {
                let current = routes(cluster).await?;
                let mut ready = true;
                for route in &current {
                    let node = current_leader(cluster, route, catalog).await?;
                    let sample = cluster.hosts[node]
                        .group(route.history)?
                        .observe_partition(TIMEOUT)
                        .await?;
                    ready &= sample.stable_leader && sample.cells <= 16 && sample.route == *route;
                }
                if ready {
                    return Ok::<_, anyhow::Error>(current);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "fanout batch settlement: {:?}",
            cluster
                .hosts
                .iter()
                .map(RaftCells::partition_service_status)
                .collect::<Vec<_>>()
        )
    })?
}

async fn exercise(cluster: &ManagedCluster) -> Result<()> {
    let f = &cluster.fixtures[0];
    let catalog = managed_fixture::Fixture::open_catalog(
        &f.domain,
        f.store.clone(),
        cluster.history.cluster,
    )?;
    let cut = catalog.read(TIMEOUT).await?;
    for host in cluster.hosts.iter().skip(1) {
        managed_fixture::inspect_current(host, cluster.peers[0].node, cut.revision()).await?;
        host.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: cluster.peers[0].clone(),
        })?;
        host.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: cluster.peers[0].clone(),
        })?;
    }
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(600_000)
    .context("receipt retention horizon")?;
    let mut ledger = Vec::new();
    for batch in 0..8 {
        for offset in 0..8 {
            let index = batch * 8 + offset;
            let history = CellHistory {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                cell: CellId::new(format!("Counter:fanout-{index}"))?,
                incarnation: IncarnationId::new([96; 16])?,
            };
            let creation = ProposalId::new((50_000u128 + index as u128).to_be_bytes())?;
            let handle = cluster.hosts[0].cell(history.clone())?;
            let enrolled = handle
                .enroll(
                    CellEnrollmentRequest {
                        creation,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            ensure!(enrolled.durability() == Durability::Quorum);
            drop(enrolled);
            let delta = i64::from(index + 1);
            let receipt = handle
                .command(
                    &handle.contract("Add")?,
                    delta.to_be_bytes().to_vec(),
                    CommandOptions::new(CommandId::new(ClientId::new([220; 16])?, 1)?, retry),
                )
                .await?;
            ensure!(
                receipt.result == delta.to_be_bytes() && receipt.durability == Durability::Quorum
            );
            ledger.push((history, creation, receipt, delta));
        }
        let current = settled(cluster, &catalog).await?;
        eprintln!(
            "fanout batch={} cells={} groups={}",
            batch + 1,
            ledger.len(),
            current.len()
        );
    }
    let current = settled(cluster, &catalog).await?;
    ensure!(
        current.len() >= 4 && current.iter().all(|r| r.history != cluster.history),
        "64 cells did not form multiple shared groups"
    );
    for host in &cluster.hosts {
        host.suspend_automatic_partitions().await;
    }
    let stable = catalog.read(TIMEOUT).await?;
    for host in &cluster.hosts {
        managed_fixture::inspect_current(host, host.identity().node, stable.revision()).await?;
    }
    let mut physical = 0u32;
    for host in &cluster.hosts {
        let node = host.observe_node(TIMEOUT).await?;
        ensure!(
            node.node.groups <= 2,
            "node exceeded its original engine slots"
        );
        physical += node.node.groups;
    }
    ensure!(
        physical + 3 <= (cluster.hosts.len() * 2) as u32,
        "recovery reserve was consumed"
    );
    let expected = current
        .iter()
        .map(|r| r.history)
        .collect::<std::collections::BTreeSet<_>>();
    let control =
        RaftFleetControl::new(&f.domain, catalog.clone(), vec![cluster.directory.clone()])?;
    let mut sampled = std::collections::BTreeSet::new();
    let before = catalog.read(TIMEOUT).await?.revision();
    for host in &cluster.hosts {
        let local = expected
            .iter()
            .filter(|history| host.group(**history).is_ok())
            .copied()
            .collect::<Vec<_>>();
        let mut scheduler = RaftFleetPartitionScheduler::new(
            control.clone().with_node_observer(host.clone()),
            configuration().sizing,
            2,
        )?;
        for _ in 0..local.len().max(1) {
            let poll = scheduler.poll(pressure(), TIMEOUT).await?;
            if let Some(group) = poll.sampled {
                ensure!(local.contains(&group));
                sampled.insert(group);
            }
        }
        ensure!(scheduler.local_groups() == local.len() && scheduler.inventory_pages() == 1);
        let pages = scheduler.inventory_pages();
        for _ in 0..4 {
            scheduler.poll(pressure(), TIMEOUT).await?;
        }
        ensure!(
            scheduler.inventory_pages() == pages && scheduler.tracked_groups() <= 4,
            "stable inventory was recopied or exceeded tracker admission"
        );
        if local.len() == 2 {
            let mut limited = RaftFleetPartitionScheduler::new(
                control.clone().with_node_observer(host.clone()),
                configuration().sizing,
                1,
            )?;
            let refused = limited.poll(pressure(), TIMEOUT).await;
            ensure!(matches!(refused, Err(error) if error.code == RaftFleetFailureCode::Limit));
            ensure!(
                limited.tracked_groups() == 0 && limited.remote_samples() == 0,
                "overflow sampled a favored prefix"
            );
        }
    }
    ensure!(
        sampled == expected && catalog.read(TIMEOUT).await?.revision() == before,
        "inventory starved a group or mutated the topology journal"
    );
    let mut capped = RaftFleetPartitionScheduler::new(
        control.with_node_observer(cluster.hosts[0].clone()),
        configuration().sizing,
        1,
    )?;
    capped
        .observe(current[0].history, pressure(), TIMEOUT)
        .await?;
    let remote = capped.remote_samples();
    let (refused, reads) =
        catalog_fault::count_reads(capped.observe(current[1].history, pressure(), TIMEOUT)).await;
    ensure!(matches!(refused, Err(error) if error.code == RaftFleetFailureCode::Limit));
    ensure!(
        reads.is_empty() && capped.remote_samples() == remote && capped.tracked_groups() == 1,
        "tracker overflow performed metadata work or admitted another group"
    );
    drop(capped);
    for (history, creation, original, delta) in &ledger {
        let handle = cluster.hosts[0].cell(history.clone())?;
        handle
            .enroll(
                CellEnrollmentRequest {
                    creation: *creation,
                    durability: Durability::Quorum,
                },
                TIMEOUT,
            )
            .await?;
        let replay = handle
            .command(
                &handle.contract("Add")?,
                delta.to_be_bytes().to_vec(),
                CommandOptions::new(original.id, retry),
            )
            .await?;
        ensure!(
            replay.id == original.id
                && replay.result == original.result
                && replay.version == original.version
                && replay.durability == Durability::Quorum
        );
        let read = handle
            .query(
                ProposalId::new(
                    (60_000u128 + sampled.len() as u128 + delta.unsigned_abs() as u128)
                        .to_be_bytes(),
                )?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(
            read.result == delta.to_be_bytes() && read.version == original.version,
            "enrollment or command replay changed a cell across splits"
        );
    }
    eprintln!(
        "bounded fanout: cells={} groups={} voters={} SQL/receipts/reserve verified",
        ledger.len(),
        current.len(),
        cluster.hosts.len()
    );
    Ok(())
}
