// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A retained ingress returns with no local group after all birth voters retire.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_cold_router_returns_after_all_birth_voters_retire() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(cold_return(rooted, Arc::new(fault::Store::default()))).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_cold_router_returns_after_all_birth_voters_retire() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store("cold-router", rooted)?));
        Box::pin(cold_return(rooted, store)).await?;
    }
    Ok(())
}

async fn cold_return(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let disk = tempfile::tempdir()?;
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let work = Box::pin(exercise(&mut cluster, disk.path())).await;
    let joined = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if work.is_err() || joined.is_err() {
        for disk in std::iter::once(disk).chain(std::mem::take(&mut cluster.roots)) {
            eprintln!("retained cold router disk: {}", disk.keep().display());
        }
    }
    work?;
    joined
}

async fn exercise(cluster: &mut ManagedCluster, disk: &Path) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let mut live = [0, 1, 2];
    let router = peer_fixture(&cluster.fixtures[0], 10).await?;
    let (node, ingress, peer) =
        open_router(cluster.history, &router, disk, LogOpenMode::Create, 63).await?;
    let initial = async {
        grant(cluster, &cluster.hosts[..3], &live, &router, &peer).await?;
        let handle = ingress.cell(seed.cells[0].clone())?;
        let read = read_at(&handle, MemberId::new(1)?, &seed.receipts[0], 213).await?;
        ensure!(read.served_by_node.node == cluster.fixtures[0].spec.node);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    close_router(node, ingress).await?;
    initial?;

    for (spare, ids) in [(3, [220, 230]), (4, [221, 231]), (5, [222, 232])] {
        Box::pin(replace_birth_voter(cluster, &mut live, spare, ids)).await?;
    }
    live.sort_unstable();
    ensure!(live == [3, 4, 5]);

    // No old voter process survives the cold ingress restart. Keep its disk
    // and its fleet accounting records so the test never grants debt release.
    let survivors = cluster.nodes.split_off(3);
    let retired = std::mem::replace(&mut cluster.nodes, survivors);
    let retired_hosts = cluster.hosts.drain(..3).collect::<Vec<_>>();
    close(retired, &retired_hosts).await?;

    let (node, ingress, peer) =
        open_router(cluster.history, &router, disk, LogOpenMode::Resume, 64).await?;
    let returned = async {
        let survivors = [3, 4, 5];
        grant(cluster, &cluster.hosts, &survivors, &router, &peer).await?;
        let handle = ingress.cell(seed.cells[0].clone())?;
        let member = MemberId::new(4)?;
        let original = &seed.receipts[0];
        let read = read_at(&handle, member, original, 214).await?;
        ensure!(read.served_by_node.node == cluster.fixtures[3].spec.node);
        let add = handle.contract("Add")?;
        let replay = handle
            .command(
                &add,
                7i64.to_be_bytes().to_vec(),
                CommandOptions::new(original.id, seed.retry),
            )
            .await?;
        ensure!(replay.id == original.id && replay.result == original.result);
        let next = CommandId::new(original.id.client, 2)?;
        let written = handle
            .command(
                &add,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(next, seed.retry),
            )
            .await?;
        ensure!(
            written.id == next
                && written.result == 8i64.to_be_bytes()
                && written.durability == Durability::Quorum
        );
        let verified = read_at(&handle, member, &written, 215).await?;
        ensure!(verified.served_by_node.node == cluster.fixtures[3].spec.node);
        let other = ingress.cell(seed.cells[1].clone())?;
        let unchanged = read_at(&other, member, &seed.receipts[1], 216).await?;
        ensure!(unchanged.result == 11i64.to_be_bytes());
        ensure!(ingress.resource_usage().host.controller.groups == 0);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    close_router(node, ingress).await?;
    returned
}

async fn replace_birth_voter(
    cluster: &ManagedCluster,
    live: &mut [usize],
    spare: usize,
    ids: [u8; 2],
) -> Result<()> {
    let mut source = live_leader(cluster, live).await?;
    let obsolete = *live
        .iter()
        .filter(|i| **i < 3)
        .find(|i| **i != source)
        .or_else(|| live.iter().find(|i| **i < 3))
        .context("retained birth voter")?;
    if source == obsolete {
        let target = *live
            .iter()
            .find(|i| **i != obsolete)
            .context("joined survivor")?;
        let group = cluster.hosts[source].group(cluster.history)?;
        let progress = group.replication_status(TIMEOUT).await?;
        let target_member = MemberId::new(target as u64 + 1)?;
        group
            .transfer_leadership(
                LeadershipTransfer {
                    target: target_member,
                    expected_term: progress.term,
                    expected_revision: progress.membership_revision,
                },
                TIMEOUT,
            )
            .await?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let status = cluster.hosts[target]
                    .group(cluster.history)?
                    .status()
                    .await?;
                if status.ready && status.leader == Some(target_member.get()) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("handoff from the final birth voter")??;
        source = target;
    }

    discover(cluster).await?;
    let (_, execution) = admit_replacement(
        cluster,
        ReplacementTarget {
            source,
            obsolete,
            spare,
        },
        ids,
        false,
    )
    .await?;
    let catalog = &cluster.fixtures[0].catalog;
    let mut settled = false;
    for turn in 0..8 {
        discover(cluster).await?;
        let cut = catalog.read(TIMEOUT).await?;
        let result = catalog
            .reconcile_movement(&cut, &execution, &cluster.hosts[source], TIMEOUT)
            .await;
        match result {
            Ok(progress) if progress.settled => {
                ensure!(progress.phase == FleetMovementPhase::Complete);
                settled = true;
                break;
            }
            Ok(_) => {
                let current = catalog.read(TIMEOUT).await?;
                let pending = catalog.pending_reservations(&current, TIMEOUT).await?;
                ensure!(pending.len() == 1 && pending[0].reservation == execution.reservation);
            }
            Err(FleetCatalogError::Unconfirmed) => {
                let current = catalog.read(TIMEOUT).await?;
                let pending = catalog.pending_reservations(&current, TIMEOUT).await?;
                ensure!(
                    pending.is_empty()
                        || (pending.len() == 1 && pending[0].reservation == execution.reservation)
                );
                // An absent debit can follow a lost terminal reply. The next
                // exact reconciliation still has to return the terminal receipt.
            }
            Err(error) => {
                return Err(error).with_context(|| format!("cold router move {spare}, turn {turn}"))
            }
        }
    }
    ensure!(
        settled,
        "cold router move for voter {} did not settle",
        spare + 1
    );
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog
        .pending_reservations(&cut, TIMEOUT)
        .await?
        .is_empty());
    ensure!(catalog.pending_movements(&cut, TIMEOUT).await?.is_empty());
    discover(cluster).await?;
    let inventory = cluster.hosts[source]
        .inspect_peer_physical_inventory(
            cluster.fixtures[obsolete].spec.node,
            cut.revision(),
            TIMEOUT,
        )
        .await?
        .inventory
        .context("retired birth voter inventory")?;
    let history = inventory
        .group_history(&cluster.history)
        .context("retained birth voter root")?;
    ensure!(!history.in_configuration() && history.member().get() == obsolete as u64 + 1);
    let retired = cluster.hosts[obsolete].group(cluster.history)?;
    let read = tokio::time::timeout(
        Duration::from_millis(250),
        retired.quorum_membership(TIMEOUT),
    )
    .await
    .context("retired voter held a quorum read until its deadline")?;
    ensure!(matches!(read, Err(RuntimeError::NotFound)));
    *live
        .iter_mut()
        .find(|i| **i == obsolete)
        .context("replace live birth voter")? = spare;
    let leader = live_leader(cluster, live).await?;
    let quorum = cluster.hosts[leader]
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let mut actual = quorum.configuration().voters.clone();
    let mut expected = live.iter().map(|i| *i as u64 + 1).collect::<Vec<_>>();
    actual.sort_unstable();
    expected.sort_unstable();
    ensure!(actual == expected && quorum.configuration().voters_outgoing.is_empty());
    ensure!(quorum.configuration().learners.is_empty());
    Ok(())
}

async fn live_leader(cluster: &ManagedCluster, live: &[usize]) -> Result<usize> {
    let hosts = live
        .iter()
        .map(|i| cluster.hosts[*i].clone())
        .collect::<Vec<_>>();
    let members = live.iter().map(|i| *i as u64 + 1).collect::<Vec<_>>();
    let index = current_leader(&hosts, &members, cluster.history).await?;
    Ok(live[index])
}

async fn open_router(
    history: GroupHistory,
    fixture: &Fixture,
    disk: &Path,
    mode: LogOpenMode,
    process: u8,
) -> Result<(Node, RaftCells, RaftPeerNode)> {
    let node = start_node(disk, fixture.application.clone(), &fixture.spec.label).await?;
    let started = async {
        let (mut config, services) = fixture.config(&node, mode, process).await?;
        config.groups.push(RaftGroupAssignment {
            history,
            placement: RaftGroupPlacement::QueryRouter,
        });
        let peer = config.peers[0].clone();
        let ingress = node.start_raft_cells(config, services).await?;
        ensure!(ingress.observe_node(TIMEOUT).await?.node.groups == 0);
        Ok::<_, anyhow::Error>((ingress, peer))
    }
    .await;
    match started {
        Ok((ingress, peer)) => Ok((node, ingress, peer)),
        Err(error) => {
            node.shutdown(ShutdownMode::Preserve).await?;
            Err(error)
        }
    }
}

async fn close_router(node: Node, ingress: RaftCells) -> Result<()> {
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = ingress.close().await?;
    ensure!(
        report.groups_total == 0
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    Ok(())
}

async fn grant(
    cluster: &ManagedCluster,
    hosts: &[RaftCells],
    members: &[usize],
    router: &Fixture,
    peer: &RaftPeerNode,
) -> Result<()> {
    let cut = router.catalog.read(TIMEOUT).await?;
    ensure!(hosts.len() == members.len());
    for (host, member) in hosts.iter().zip(members) {
        ensure!(host.identity().node == cluster.fixtures[*member].spec.node);
        inspect_current(host, router.spec.node, cut.revision()).await?;
        host.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: 1,
            peer: peer.clone(),
        })?;
        host.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: 1,
            peer: peer.clone(),
        })?;
    }
    Ok(())
}

async fn read_at(
    handle: &RaftCellHandle,
    member: MemberId,
    receipt: &CommandReceipt,
    invocation: u8,
) -> Result<QueryResult> {
    let contract = handle.contract("Get")?;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let mut last = None;
    let result = tokio::time::timeout_at(deadline, async {
        loop {
            let budget = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .min(Duration::from_millis(500));
            match handle
                .query(
                    ProposalId::new([invocation; 16])?,
                    &contract,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                        timeout: budget,
                        routing: QueryRouting::Replicas {
                            members: vec![member],
                            attempt_timeout: budget,
                            leader_fallback: false,
                        },
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(read) => {
                    ensure!(read.served_by == member && read.result == receipt.result);
                    return Ok::<_, anyhow::Error>(read);
                }
                Err(error) => last = Some(error),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    result.with_context(|| format!("cold ingress did not route to voter {member:?}: {last:?}"))?
}
