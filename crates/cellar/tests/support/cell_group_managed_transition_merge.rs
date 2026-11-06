// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Merge the live children of a closed split and retire both sources.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::{
        CellGroupArchiveHold, CellGroupArchiveTailLimits, CellGroupArchiveTailPool, CellGroupBirth,
        CellGroupCheckpointPool, CellGroupTransitionPreparationInputs, CellGroupTransitionSource,
        GroupRoute,
    },
    FleetHistoryState,
};
use cellar::sdk::fleet_operations::{RaftFleetControl, RaftFleetFailureCode};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_split_then_merge_replays_late_writes() -> Result<()> {
    Box::pin(run_plan(
        false,
        Arc::new(fault::Store::default()),
        Decision::SplitMerge,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_split_merge() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-split-merge",
            rooted,
        )?));
        Box::pin(run_plan(rooted, store, Decision::SplitMerge))
            .await
            .with_context(|| format!("managed split and merge, extent images={rooted}"))?;
    }
    Ok(())
}

struct ArchiveInputs {
    domain: Arc<ExecutionDomain>,
    pool: CellGroupArchivePool,
    tails: CellGroupArchiveTailPool,
    scratch: tempfile::TempDir,
    rooted: bool,
}

impl ArchiveInputs {
    async fn sources(
        &self,
        stage: &str,
        cuts: &[(CellGroupBirth, CellGroupArchiveHold)],
    ) -> Result<Vec<CellGroupTransitionSource>> {
        let mut sources = Vec::with_capacity(cuts.len());
        for (index, (birth, hold)) in cuts.iter().enumerate() {
            let base = self.scratch.path().join(stage).join(index.to_string());
            let route = birth.route().clone();
            let rooted = self.rooted;
            let target = self
                .domain
                .blocking(move || preparation::store(base, route, rooted))?
                .await??;
            sources.push(CellGroupTransitionSource {
                birth: birth.clone(),
                hold: hold.clone(),
                receiver: CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
                    target.store,
                    target.scratch,
                    target.directory,
                )?,
            });
        }
        Ok(sources)
    }

    async fn partition(
        &self,
        stage: &str,
        cuts: &[(CellGroupBirth, CellGroupArchiveHold)],
        child: &GroupRoute,
    ) -> Result<CellGroupTransitionPreparationInputs> {
        let base = self.scratch.path().join(stage).join("child");
        let route = child.clone();
        let rooted = self.rooted;
        let target = self
            .domain
            .blocking(move || preparation::store(base, route, rooted))?
            .await??;
        Ok(CellGroupTransitionPreparationInputs {
            archives: self.pool.clone(),
            tails: self.tails.clone(),
            restore: preparation::recovery_pool()?,
            sources: self.sources(stage, cuts).await?,
            targets: vec![target],
        })
    }
}

pub(super) async fn run(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    cells: Vec<CellHistory>,
    rooted: bool,
) -> Result<GroupHistory> {
    let fixture = &cluster.fixtures[0];
    let fleet = fixture.catalog.clone();
    let control = RaftFleetControl::new(&fixture.domain, fleet.clone(), vec![directory.clone()])?;
    let archives = ArchiveInputs {
        domain: fixture.domain.clone(),
        pool: CellGroupArchivePool::new(Default::default())?,
        // Both independently verified source cuts stay pinned through staging.
        tails: CellGroupArchiveTailPool::new(CellGroupArchiveTailLimits {
            max_operations: 2,
            max_retained_bytes: 2 * CellGroupArchiveTailLimits::default().max_retained_bytes,
            ..Default::default()
        })?,
        scratch: tempfile::tempdir()?,
        rooted,
    };
    let domain = &archives.domain;
    let view = domain.run(directory.observe()).await??;
    let page = domain.run(directory.group_page(&view, None, 3)).await??;
    ensure!(page.next.is_none());
    let parents: [GroupRoute; 2] = page
        .routes
        .try_into()
        .map_err(|_| anyhow::anyhow!("merge requires exactly two split children"))?;
    let mut routed = Vec::with_capacity(parents.len());
    for parent in &parents {
        let cell = cells
            .iter()
            .enumerate()
            .find(|(_, cell)| {
                parent
                    .range
                    .contains(&cell_routing_key(parent.history.namespace, &cell.cell))
            })
            .context("each split child must hold one cell")?;
        routed.push((cell.1.clone(), [12i64, 13][cell.0]));
    }
    for (index, (fixture, host)) in cluster.fixtures[..3]
        .iter()
        .zip(&cluster.hosts[..3])
        .enumerate()
    {
        admit_capacity(fixture, host, directory, 130 + 2 * index as u8).await?;
    }
    let operation = ProposalId::new([244; 16])?;
    let identity = |id, revision| FleetRequestIdentity {
        operation: operation.bytes(),
        ..request(id, revision)
    };
    let snapshot = fleet.read(TIMEOUT).await?;
    let reservation = fleet
        .reserve_birth(
            &snapshot,
            &identity(243, snapshot.revision()),
            directory,
            CellGroupBirthSpec {
                history: GroupHistory {
                    group: GroupId::new([243; 16])?,
                    incarnation: IncarnationId::new([243; 16])?,
                    ..cluster.history
                },
                range: parents[0].range.merge(&parents[1].range)?,
                epoch: RouteEpoch::new(3)?,
                creation: ProposalId::new([243; 16])?,
                voters: cluster.fixtures[..3]
                    .iter()
                    .enumerate()
                    .map(|(index, fixture)| {
                        Ok(InitialMember {
                            member: MemberId::new(index as u64 + 1)?,
                            node: fixture.spec.node,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                learners: vec![],
            },
            "Counter",
            TIMEOUT,
        )
        .await?;
    let child = reservation.authorization.route().clone();
    let mut reused = child.clone();
    reused.history.group = parents[0].history.group;
    ensure!(directory
        .prepare_merge(&view, operation, &parents, &reused)
        .is_err());
    let mut incompatible = parents.clone();
    incompatible[1].schema_digest = [7; 32];
    ensure!(directory
        .prepare_merge(&view, operation, &incompatible, &child)
        .is_err());
    let mut gap = parents.clone();
    let gap_key = cell_routing_key(child.history.namespace, &CellId::new("Counter:gap-21")?);
    gap[1].range = gap[1]
        .range
        .split(gap_key)
        .context("gap fixture boundary")?
        .1;
    ensure!(directory
        .prepare_merge(&view, operation, &gap, &child)
        .is_err());
    let plan = directory.prepare_merge(&view, operation, &parents, &child)?;
    let cut = fleet.read(TIMEOUT).await?;
    let request = identity(245, cut.revision());
    ensure!(
        fleet
            .plan_managed_transition(&request, directory, &plan, TIMEOUT)
            .await?
            .phase()
            == TransitionPhase::Planned
    );

    let mut registries = Vec::with_capacity(2);
    let mut precopy = Vec::with_capacity(2);
    for route in &parents {
        let birth = domain
            .run(directory.materialize_selected_history(route.history))
            .await??;
        let registry = directory
            .open_group_archive(domain.as_ref(), &archives.pool, &birth)
            .await?;
        let hold = preparation::acquire_hold(domain, &registry).await?;
        precopy.push((birth, hold));
        registries.push(registry);
    }
    let prepared = fleet
        .prepare_managed_transition(
            &request,
            directory,
            &plan,
            archives.partition("precopy", &precopy, &child).await?,
            TIMEOUT,
        )
        .await
        .context("prepare both merge source archives")?;
    ensure!(prepared.journal().phase() == TransitionPhase::Prepared);
    ensure!(prepared.children().len() == 1 && prepared.children()[0].root().cells() == 2);
    drop(prepared);
    fleet
        .authorize_managed_transition_seal(&request, directory, &plan, TIMEOUT)
        .await?;
    let transition = Arc::new(
        fleet
            .reopen_managed_transition(directory, operation, TIMEOUT)
            .await?
            .context("reopened authorized merge")?,
    );
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(60_000)
    .context("merge retry deadline")?;
    let mut receipts = Vec::with_capacity(2);
    let mut sealed = Vec::with_capacity(2);
    for (index, (parent, (cell, expected))) in parents.iter().zip(&routed).enumerate() {
        let voters = &cluster.hosts[3..];
        let voter = leader(voters, parent.history).await?;
        let handle = voters[voter].cell(cell.clone())?;
        let receipt = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([202; 16])?, 1)?, retry),
            )
            .await?;
        ensure!(
            receipt.result == (expected + 1).to_be_bytes()
                && receipt.witness.group == parent.history
        );
        receipts.push(receipt);
        let seal = Box::pin(commit_source_seal(voters, &transition, parent.history)).await?;
        preparation::wait_transfer_applied(voters, &[(parent.history, seal.position().index)])
            .await?;
        wait_archive(voters, parent.history, &seal).await?;
        let hold = preparation::acquire_hold(domain, &registries[index]).await?;
        ensure!(hold.marker().source_seal() == Some(&seal));
        sealed.push((precopy[index].0.clone(), hold));
        if index == 0 {
            let active = Some(parents[1].history);
            Box::pin(restart_voter(cluster, 3, 120, active, 2))
                .await
                .context("restart a voter with one sealed merge parent")?;
            let fifth = Box::pin(stop_voter(cluster, 5, active)).await?;
            let fourth = Box::pin(stop_voter(cluster, 4, active)).await?;
            let survivor = &cluster.hosts[3];
            Box::pin(verify_minority(
                survivor,
                &transition,
                &seal,
                &parents,
                &routed,
                retry,
            ))
            .await?;
            ensure!(matches!(
                fleet
                    .verify_managed_sealed_sources(
                        &transition,
                        directory,
                        &archives.pool,
                        &archives.tails,
                        archives.sources("partial", &sealed).await?,
                        TIMEOUT,
                    )
                    .await,
                Err(FleetCatalogError::Invalid(_) | FleetCatalogError::Conflict)
            ));
            let early = fleet
                .publish_managed_transfer(&transition, directory, &archives.pool, TIMEOUT)
                .await;
            ensure!(
                matches!(early, Err(FleetCatalogError::Unconfirmed)),
                "merge publication without both parents and a child quorum: {early:?}"
            );
            let unchanged = domain.run(directory.observe()).await??;
            ensure!(unchanged.revision() == view.revision() && unchanged.groups() == 2);
            let namespace = hex::encode(child.history.namespace.bytes());
            let operation_id = hex::encode(operation.bytes());
            let status = control
                .transition_status(&namespace, &operation_id, TIMEOUT)
                .await?;
            ensure!(
                status.namespace_id == namespace
                    && status.operation_id == operation_id
                    && status.phase == Some(TransitionPhase::SealAuthorized)
                    && status.plan_digest == hex::encode(plan.digest())
                    && status.fleet_slot_held
                    && !status.child_archives_verified
                    && !status.parent_holds_released
            );
            let missing = hex::encode([249; 16]);
            ensure!(
                matches!(control.transition_status(&namespace, &missing, TIMEOUT).await,
                    Err(failure) if failure.code == RaftFleetFailureCode::NotFound)
            );
            let candidate = &cluster.hosts[0];
            let attached = candidate
                .attach_managed_transfer_child(transition.clone(), child.clone(), TIMEOUT)
                .await;
            ensure!(
                matches!(attached, Err(CellGroupControllerError::Catalog(ref cause))
                    if matches!(cause.as_ref(), CatalogError::Unconfirmed)),
                "uninstalled merge child must refuse attachment: {attached:?}"
            );
            ensure!(matches!(
                candidate.group(child.history),
                Err(RuntimeError::NotFound)
            ));

            for (index, generation, previous) in [(4, 125, fourth), (5, 126, fifth)] {
                Box::pin(start_voter(cluster, index, generation, previous, 2)).await?;
            }
            let revision = fleet.read(TIMEOUT).await?.revision();
            for host in &cluster.hosts[4..] {
                inspect_current(&cluster.hosts[3], host.identity().node, revision).await?;
            }
            leader(&cluster.hosts[3..], parents[1].history)
                .await
                .context("unsealed merge parent did not regain quorum")?;
            verify_durable_seal(&cluster.hosts[3], &transition, parent.history, &seal).await?;
        }
    }
    let sources = fleet
        .verify_managed_sealed_sources(
            &transition,
            directory,
            &archives.pool,
            &archives.tails,
            archives.sources("sealed", &sealed).await?,
            TIMEOUT,
        )
        .await
        .context("verify both sealed merge source archives")?;
    ensure!(sources.journal().phase() == TransitionPhase::Sealed);
    let transition = Arc::new(
        fleet
            .reopen_managed_transition(directory, operation, TIMEOUT)
            .await?
            .context("reopened sealed merge")?,
    );
    let recovery = preparation::recovery_pool()?;
    for host in &cluster.hosts[..3] {
        host.install_managed_transfer_child(
            transition.clone(),
            sources.clone(),
            child.history,
            recovery.clone(),
            TIMEOUT,
        )
        .await
        .context("install merged child")?;
    }
    drop(sources);
    let voters = &cluster.hosts[..3];
    for host in voters {
        host.attach_managed_transfer_child(transition.clone(), child.clone(), TIMEOUT)
            .await
            .context("attach merged child")?;
    }
    let proof = preparation::wait_pending_child(voters, child.history).await?;
    ensure!(
        fleet
            .record_managed_transfer_child_quorums(&transition, directory, vec![proof], TIMEOUT,)
            .await?
            .phase()
            == TransitionPhase::Installed
    );
    let archived = fleet
        .verify_managed_transfer_archive(
            &transition,
            directory,
            archives.partition("final", &sealed, &child).await?,
            TIMEOUT,
        )
        .await
        .context("verify merged child transfer archive")?;
    let published = fleet
        .publish_managed_transfer(&transition, directory, &archives.pool, TIMEOUT)
        .await
        .context("publish merged child after both seals")?;
    ensure!(published.transfer_archive() == archived.transfer_archive());
    ensure!(published.revision() == view.revision() + 1);
    let after = domain.run(directory.observe()).await??;
    let page = domain.run(directory.group_page(&after, None, 2)).await??;
    ensure!(page.next.is_none() && page.routes == vec![child.clone()]);
    let ack = preparation::wait_managed_activation(voters, &transition, child.history).await?;
    ensure!(
        fleet
            .record_managed_transfer_activations(&transition, directory, vec![ack], TIMEOUT,)
            .await?
            .phase()
            == TransitionPhase::Active
    );
    Box::pin(verify_merged_sql(
        voters,
        child.history,
        &routed,
        receipts,
        retry,
    ))
    .await?;
    Box::pin(finish_merge(
        cluster,
        directory,
        &transition,
        &parents,
        child.history,
        &archives,
        &control,
    ))
    .await?;
    let voters = &cluster.hosts[..3];
    let host = &voters[leader(voters, child.history).await?];
    for (index, (cell, expected)) in routed.into_iter().enumerate() {
        let handle = host.cell(cell)?;
        let query = handle
            .query(
                ProposalId::new([218 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(query.result == (expected + 2).to_be_bytes());
    }
    Ok(child.history)
}

async fn verify_minority(
    survivor: &RaftCells,
    transition: &ReopenedManagedCellGroupTransition,
    seal: &GroupSourceSeal,
    parents: &[GroupRoute; 2],
    cells: &[(CellHistory, i64)],
    retry: u64,
) -> Result<()> {
    let unsealed = parents[1].history;
    let group = survivor.group(unsealed)?;
    let committed = tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = group.status().await?;
            if !status.ready && status.leader.is_none() {
                return Ok::<_, anyhow::Error>(status.committed);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("unsealed merge parent did not lose quorum")??;
    let handle = survivor.cell(cells[1].0.clone())?;
    let get = handle.contract("Get")?;
    let historical = handle
        .query(
            ProposalId::new([220; 16])?,
            &get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Stale,
                stale_policy: StaleReadPolicy::DisconnectedHistorical,
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        historical.result == cells[1].1.to_be_bytes()
            && historical.witness.group == unsealed
            && historical.served_by_node == survivor.identity()
    );
    let fresh = handle
        .query(
            ProposalId::new([221; 16])?,
            &get,
            vec![],
            QueryOptions {
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await;
    ensure!(
        matches!(
            fresh.as_ref().err().map(Arc::as_ref),
            Some(CellDispatchError::Runtime(
                RuntimeError::NotLeader | RuntimeError::NotReady | RuntimeError::DeadlineExceeded
            ))
        ),
        "minority cannot serve a fresh read: {fresh:?}"
    );
    let refused = group.seal_managed_source(transition, TIMEOUT).await.err();
    ensure!(
        matches!(
            refused,
            Some(CellGroupControllerError::Runtime(RuntimeError::NotLeader))
        ),
        "minority cannot seal the other parent: {refused:?}"
    );
    ensure!(group.status().await?.committed == committed);

    let sealed = parents[0].history;
    verify_durable_seal(survivor, transition, sealed, seal).await?;
    let handle = survivor.cell(cells[0].0.clone())?;
    let id = CommandId::new(ClientId::new([222; 16])?, 1)?;
    let refused = handle
        .command(
            &handle.contract("Add")?,
            64i64.to_be_bytes().to_vec(),
            CommandOptions::new(id, retry),
        )
        .await;
    ensure!(
        matches!(refused, Err(ref failure) if failure.command == id && !failure.ambiguous),
        "sealed parent admitted a new command: {refused:?}"
    );
    verify_durable_seal(survivor, transition, sealed, seal).await
}

async fn verify_durable_seal(
    host: &RaftCells,
    transition: &ReopenedManagedCellGroupTransition,
    history: GroupHistory,
    seal: &GroupSourceSeal,
) -> Result<()> {
    let source = host.group(history)?;
    let observed = source.seal_managed_source(transition, TIMEOUT).await?;
    ensure!(matches!(observed, CellGroupSealOutcome::Applied(ref applied) if applied == seal));
    ensure!(!source.status().await?.ready);
    Ok(())
}

async fn finish_merge(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parents: &[GroupRoute; 2],
    child: GroupHistory,
    archives: &ArchiveInputs,
    control: &RaftFleetControl,
) -> Result<()> {
    let fleet = cluster.fixtures[0].catalog.clone();
    let domain = &archives.domain;
    let publication = domain
        .run(directory.publication(&transition.plan))
        .await??
        .context("merge publication")?;
    let mut tombstones = Vec::with_capacity(parents.len());
    for parent in parents {
        let ack =
            retirement::commit_on_all(&cluster.hosts[3..], parent.history, transition).await?;
        ensure!(ack.tombstone().publication() == publication.journal_digest());
        tombstones.push(ack);
    }
    preparation::initialize_transfer_archive(
        &fleet,
        directory,
        transition,
        child,
        &archives.pool,
        &archives.scratch.path().join("merge.intent"),
        domain,
    )
    .await?;
    Box::pin(restart_voter(cluster, 0, 121, Some(child), 1)).await?;
    let applied = preparation::wait_active_archive(&cluster.hosts[0], child).await?;
    let birth = domain
        .run(directory.materialize_selected_history(child))
        .await??;
    let registry = directory
        .open_group_archive(domain.as_ref(), &archives.pool, &birth)
        .await?;
    let hold = preparation::acquire_hold(domain, &registry).await?;
    ensure!(hold.marker().position().index >= applied);
    let covered = fleet
        .verify_managed_transfer_child_archives(
            transition,
            directory,
            &archives.pool,
            &archives.tails,
            archives.sources("merge-baseline", &[(birth, hold)]).await?,
            TIMEOUT,
        )
        .await?;
    ensure!(covered.phase() == TransitionPhase::Active && covered.child_archives().is_some());
    ensure!(matches!(
        fleet
            .settle_managed_transfer_retirement(
                transition,
                directory,
                &archives.pool,
                tombstones[..1].to_vec(),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    let retired = fleet
        .settle_managed_transfer_retirement(
            transition,
            directory,
            &archives.pool,
            tombstones,
            TIMEOUT,
        )
        .await?;
    ensure!(retired.phase() == TransitionPhase::Retired && !retired.parent_holds_released());
    let cut = fleet.read(TIMEOUT).await?;
    let confirmation = FleetRequestIdentity {
        operation: transition.request.operation,
        ..request(246, cut.revision())
    };
    let confirmed = fleet
        .confirm_managed_transfer_children(
            &cut,
            &confirmation,
            transition,
            directory,
            &archives.pool,
            TIMEOUT,
        )
        .await?;
    ensure!(
        *fleet
            .history(&confirmed.snapshot, &child, TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Live
    );

    Box::pin(restart_voter(cluster, 3, 122, None, 0)).await?;
    let cut = fleet.read(TIMEOUT).await?;
    let observer = &cluster.hosts[0];
    for source in &cluster.hosts[3..] {
        inspect_current(observer, source.identity().node, cut.revision()).await?;
    }
    ensure!(matches!(
        fleet
            .release_managed_transfer_parent_holds(
                transition,
                directory,
                &archives.pool,
                observer,
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    for index in 4..6 {
        Box::pin(restart_voter(cluster, index, 119 + index as u8, None, 0)).await?;
    }
    let cut = fleet.read(TIMEOUT).await?;
    let observer = &cluster.hosts[0];
    for source in &cluster.hosts[3..] {
        let node = source.identity().node;
        inspect_current(observer, node, cut.revision()).await?;
        let parked = observer
            .inspect_peer_physical_inventory(node, cut.revision(), TIMEOUT)
            .await?;
        ensure!(
            parked.view.unattached_engine_histories == 2
                && parked.view.node.reserved_bytes == 2 * parked.view.node.profile.bytes_per_group
        );
        let inventory = parked.inventory.context("two parked merge parents")?;
        for parent in parents {
            ensure!(inventory
                .group_history(&parent.history)
                .context("parked merge parent")?
                .parked_retired_source());
        }
    }
    let released = fleet
        .release_managed_transfer_parent_holds(
            transition,
            directory,
            &archives.pool,
            observer,
            TIMEOUT,
        )
        .await?;
    ensure!(
        released.parent_holds_released()
            && domain.run(directory.active_transition()).await??.is_none()
    );
    let namespace = hex::encode(child.namespace.bytes());
    let operation = hex::encode(transition.request.operation);
    let cleaning = control
        .transition_status(&namespace, &operation, TIMEOUT)
        .await?;
    ensure!(
        cleaning.phase == Some(TransitionPhase::Retired)
            && cleaning.child_archives_verified
            && cleaning.parent_holds_released
            && cleaning.fleet_slot_held
            && cleaning.observed_fleet_revision == cut.revision()
    );
    let completion = FleetRequestIdentity {
        operation: transition.request.operation,
        ..request(247, cut.revision())
    };
    let completed = fleet
        .complete_managed_transfer(&cut, &completion, transition, directory, TIMEOUT)
        .await?;
    let settled = &completed.snapshot;
    let terminal = control
        .transition_status(&namespace, &operation, TIMEOUT)
        .await?;
    ensure!(
        terminal.observed_fleet_revision == settled.revision()
            && terminal.phase == cleaning.phase
            && terminal.plan_digest == cleaning.plan_digest
            && terminal.parent_holds_released
            && !terminal.fleet_slot_held
    );
    crate::managed_fixture::operator::transition_status(
        &cluster.fixtures[0],
        directory,
        &transition.request.principal,
        &terminal,
    )
    .await?;
    let retirement = released
        .retired_parents()
        .context("merge parent retirement digest")?;
    for parent in parents {
        ensure!(
            *fleet
                .history(settled, &parent.history, TIMEOUT)
                .await?
                .state()
                == FleetHistoryState::Retired {
                    operation: transition.request.operation,
                    retirement
                }
        );
    }
    let inventory = fleet
        .check_inventory(settled, std::slice::from_ref(directory), TIMEOUT)
        .await?;
    ensure!(inventory.histories() == 4 && inventory.namespaces() == 1);
    Ok(())
}

async fn wait_archive(
    voters: &[RaftCells],
    history: GroupHistory,
    seal: &GroupSourceSeal,
) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for host in voters {
                if host
                    .group(history)?
                    .archive_status()?
                    .and_then(|status| status.published)
                    .is_some_and(|marker| marker.source_seal() == Some(seal))
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("merge parent did not publish its sealed cut")?
}

async fn restart_voter(
    cluster: &mut ManagedCluster,
    index: usize,
    generation: u8,
    active: Option<GroupHistory>,
    registered: u32,
) -> Result<()> {
    let previous = stop_voter(cluster, index, active).await?;
    start_voter(cluster, index, generation, previous, registered).await
}

async fn stop_voter(
    cluster: &mut ManagedCluster,
    index: usize,
    active: Option<GroupHistory>,
) -> Result<NodeIdentity> {
    let node = cluster.nodes.remove(index);
    let old = cluster.hosts.remove(index);
    let identity = old.identity();
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = old.close().await?;
    ensure!(
        report.groups_total == 2
            && report.omitted_groups == 0
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
            && report
                .groups
                .iter()
                .all(|group| group.active == (active == Some(group.history))
                    && !group.failed
                    && group.applied.index == group.committed),
        "merge voter shutdown inventory: {report:?}"
    );
    Ok(identity)
}

async fn start_voter(
    cluster: &mut ManagedCluster,
    index: usize,
    generation: u8,
    previous: NodeIdentity,
    registered: u32,
) -> Result<()> {
    let fixture = &cluster.fixtures[index];
    let node = start_node(
        cluster.roots[index].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let startup = async {
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, generation)
            .await?;
        crate::managed_fixture::archives::configure(&mut config);
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match startup {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!("restart merge voter {index}, shutdown={stopped:?}")));
        }
    };
    cluster.nodes.insert(index, node);
    cluster.hosts.insert(index, host);
    let host = &cluster.hosts[index];
    ensure!(host.identity().disk == previous.disk && host.identity().process != previous.process);
    let view = host.observe_node(TIMEOUT).await?;
    ensure!(view.node.groups == 2 && view.registered_histories == registered);
    Ok(())
}

async fn verify_merged_sql(
    voters: &[RaftCells],
    history: GroupHistory,
    cells: &[(CellHistory, i64)],
    receipts: Vec<CommandReceipt>,
    retry: u64,
) -> Result<()> {
    let host = &voters[leader(voters, history).await?];
    let mut positions = Vec::with_capacity(cells.len());
    for (index, ((cell, expected), receipt)) in cells.iter().zip(receipts).enumerate() {
        let handle = host.cell(cell.clone())?;
        let query = handle
            .query(
                ProposalId::new([214 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(query.result == (expected + 1).to_be_bytes());
        let add = handle.contract("Add")?;
        let retry_receipt = handle
            .command(
                &add,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(receipt.id, retry),
            )
            .await?;
        ensure!(
            retry_receipt.id == receipt.id
                && retry_receipt.result == receipt.result
                && retry_receipt.version == receipt.version
        );
        let write = handle
            .command(
                &add,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(receipt.id.client, 2)?, retry),
            )
            .await?;
        ensure!(write.result == (expected + 2).to_be_bytes() && write.witness.group == history);
        positions.push((history, write.witness.position.index));
    }
    preparation::wait_transfer_applied(voters, &positions).await
}

pub(super) async fn close(
    nodes: Vec<Node>,
    hosts: &[RaftCells],
    merged: Option<GroupHistory>,
) -> Result<()> {
    let reports = futures_util::future::join_all(nodes.into_iter().zip(hosts).map(
        |(node, host)| async move {
            ensure!(matches!(
                node.shutdown(ShutdownMode::Preserve).await?,
                NodeOutcome::Stopped(_)
            ));
            Ok::<_, anyhow::Error>(host.close().await?)
        },
    ))
    .await;
    for (index, report) in reports.into_iter().enumerate() {
        let report = report?;
        if let Some(merged) = merged {
            ensure!(
                report.groups_total == 2
                    && report.groups.len() == 2
                    && report.omitted_groups == 0
                    && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
                    && report.groups.iter().all(|group| !group.failed
                        && group.active == (index < 3 && group.history == merged)
                        && group.applied.index == group.committed),
                "merged shutdown inventory: {report:?}"
            );
        }
    }
    Ok(())
}
