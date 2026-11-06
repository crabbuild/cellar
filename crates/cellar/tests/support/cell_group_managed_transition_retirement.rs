// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A retained sealed parent reopens for retirement after the route switch.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::TransitionJournal, FleetHistoryState,
};
use std::time::Instant;

pub(super) async fn reopen_and_retire(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
) -> Result<CellGroupSourceRetirementAck> {
    let source = cluster.history;
    let fixture = &cluster.fixtures[0];
    let publication = fixture
        .domain
        .run(directory.publication(&transition.plan))
        .await??
        .context("selected transfer publication")?;
    let selected = fixture
        .catalog
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("selected managed transfer")?;
    ensure!(
        selected
            .journal
            .as_ref()
            .context("transfer journal")?
            .phase()
            == TransitionPhase::Active
    );

    restart_source(cluster, 0, 112).await?;
    let fixture = &cluster.fixtures[0];
    let reopened = &cluster.hosts[0];
    ensure!(reopened.observe_node(TIMEOUT).await?.node.groups == 1);
    ensure!(!reopened.group(source)?.status().await?.ready);
    ensure!(reopened.group(source)?.archive_status()?.is_none());
    ensure!(fixture
        .domain
        .run(directory.materialize_selected_history(source))
        .await?
        .is_err());

    let voters = &cluster.hosts[..3];
    let tombstone = commit(voters, source, &selected).await?;
    ensure!(tombstone.tombstone().publication() == publication.journal_digest());
    for voter in voters {
        let local = commit(std::slice::from_ref(voter), source, &selected).await?;
        ensure!(local.tombstone() == tombstone.tombstone());
    }
    ensure!(!reopened.group(source)?.status().await?.ready);
    let retained = fixture
        .catalog
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("managed retirement remains owned")?;
    let journal = retained.journal.context("active transition journal")?;
    ensure!(journal.phase() == TransitionPhase::Active && !journal.parent_holds_released());
    Ok(tombstone)
}

pub(super) async fn park_and_release_parent(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
) -> Result<()> {
    let archives = CellGroupArchivePool::new(Default::default())?;
    let fleet = cluster.fixtures[0].catalog.clone();
    let domain = cluster.fixtures[0].domain.clone();
    Box::pin(reopen_parked_source(cluster, 0)).await?;
    let observer = &cluster.hosts[3];
    let revision = fleet.read(TIMEOUT).await?.revision();
    for source in &cluster.hosts[..3] {
        inspect_current(observer, source.identity().node, revision).await?;
    }
    ensure!(matches!(
        fleet
            .release_managed_transfer_parent_holds(
                transition, directory, &archives, observer, TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(!domain
        .run(directory.active_transition())
        .await??
        .context("an attached parent keeps topology owned")?
        .1
        .parent_holds_released());
    let cut = fleet.read(TIMEOUT).await?;
    let premature = FleetRequestIdentity {
        operation: transition.request.operation,
        ..request(239, cut.revision())
    };
    ensure!(matches!(
        fleet
            .complete_managed_transfer(&cut, &premature, transition, directory, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    for index in 1..3 {
        Box::pin(reopen_parked_source(cluster, index)).await?;
    }
    let observer = &cluster.hosts[3];
    let revision = fleet.read(TIMEOUT).await?.revision();
    for source in &cluster.hosts[..3] {
        inspect_current(observer, source.identity().node, revision).await?;
    }
    let store = &cluster.fixtures[0].store;
    store.deny_registry_writes.store(true, Ordering::Release);
    let denied = fleet
        .release_managed_transfer_parent_holds(transition, directory, &archives, observer, TIMEOUT)
        .await;
    store.deny_registry_writes.store(false, Ordering::Release);
    ensure!(matches!(denied, Err(FleetCatalogError::Unconfirmed)));
    ensure!(!domain
        .run(directory.active_transition())
        .await??
        .context("archive outage keeps topology owned")?
        .1
        .parent_holds_released());
    store.fail_head_confirmation.store(true, Ordering::Release);
    let completed = fleet
        .release_managed_transfer_parent_holds(transition, directory, &archives, observer, TIMEOUT)
        .await?;
    ensure!(!store.fail_head_confirmation.load(Ordering::Acquire));
    ensure!(completed.phase() == TransitionPhase::Retired && completed.parent_holds_released());
    ensure!(domain.run(directory.active_transition()).await??.is_none());
    ensure!(
        fleet
            .release_managed_transfer_parent_holds(
                transition, directory, &archives, observer, TIMEOUT,
            )
            .await?
            == completed
    );
    let cut = fleet.read(TIMEOUT).await?;
    ensure!(cut.revision() == revision);
    Box::pin(complete_fleet_transfer(
        cluster, directory, transition, completed,
    ))
    .await?;
    Ok(())
}

async fn complete_fleet_transfer(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    released: TransitionJournal,
) -> Result<()> {
    let fleet = cluster.fixtures[0].catalog.clone();
    let domain = cluster.fixtures[0].domain.clone();
    let cut = fleet.read(TIMEOUT).await?;
    let confirmation = FleetRequestIdentity {
        operation: transition.request.operation,
        ..request(240, cut.revision())
    };
    let parent = cluster.history;
    let live = fleet.history(&cut, &parent, TIMEOUT).await?;
    ensure!(*live.state() == FleetHistoryState::Live);
    let store = &cluster.fixtures[0].store;
    store.fail_fleet_confirmation.store(true, Ordering::Release);
    ensure!(matches!(
        fleet
            .complete_managed_transfer(&cut, &confirmation, transition, directory, TIMEOUT)
            .await,
        Err(FleetCatalogError::Storage(_))
    ));
    ensure!(!store.fail_fleet_confirmation.load(Ordering::Acquire));
    let settled = fleet.read(TIMEOUT).await?;
    let retired = FleetHistoryState::Retired {
        operation: transition.request.operation,
        retirement: released
            .retired_parents()
            .context("released retirement digest")?,
    };
    ensure!(settled.revision() == cut.revision() + 1);
    ensure!(*fleet.history(&settled, &parent, TIMEOUT).await?.state() == retired);
    ensure!(matches!(
        fleet.authorize_group(&settled, &parent, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(
        fleet
            .complete_managed_transfer(&settled, &confirmation, transition, directory, TIMEOUT)
            .await?
            .applied_revision
            == settled.revision()
    );
    ensure!(
        fleet
            .plan_managed_transition(&transition.request, directory, &transition.plan, TIMEOUT,)
            .await?
            == released
    );
    let view = domain.run(directory.observe()).await??;
    let children = domain.run(directory.group_page(&view, None, 3)).await??;
    ensure!(children.next.is_none() && children.routes.len() == 2);
    for child in children.routes {
        fleet
            .authorize_group(&settled, &child.history, TIMEOUT)
            .await?;
    }
    let inventory = fleet
        .check_inventory(&settled, std::slice::from_ref(directory), TIMEOUT)
        .await?;
    ensure!(inventory.histories() == 3 && inventory.namespaces() == 1);

    let revival = request(241, settled.revision());
    let revival = FleetMutation {
        id: revival.id,
        operation: revival.operation,
        expected_revision: revival.expected_revision,
        principal: revival.principal,
        changes: vec![FleetChange {
            table: FleetTable::Histories,
            key: live
                .binding()
                .key()
                .map_err(|_| anyhow::anyhow!("retired history key"))?,
            value: Some(live.encode()?),
        }],
    };
    ensure!(matches!(
        fleet.commit(&settled, &revival, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let admitted = request(242, settled.revision());
    let unrelated = FleetMutation {
        id: admitted.id,
        operation: admitted.operation,
        expected_revision: admitted.expected_revision,
        principal: admitted.principal,
        changes: vec![FleetChange {
            table: FleetTable::Admissions,
            key: "test-membership".into(),
            value: None,
        }],
    };
    ensure!(
        fleet
            .commit(&settled, &unrelated, TIMEOUT)
            .await?
            .applied_revision
            == settled.revision() + 1
    );
    restart_source(cluster, 0, 119).await?;
    let revision = fleet.read(TIMEOUT).await?.revision();
    let observer = &cluster.hosts[3];
    let node = cluster.hosts[0].identity().node;
    inspect_current(observer, node, revision).await?;
    let retained = observer
        .inspect_peer_physical_inventory(node, revision, TIMEOUT)
        .await?;
    ensure!(
        retained.view.node.groups == 1
            && retained.view.node.reserved_bytes == retained.view.node.profile.bytes_per_group
    );
    ensure!(retained
        .inventory
        .context("closed parent inventory")?
        .group_history(&parent)
        .context("closed parent slot")?
        .parked_retired_source());
    Ok(())
}

async fn reopen_parked_source(cluster: &mut ManagedCluster, index: usize) -> Result<()> {
    let source = cluster.history;
    let host = &cluster.hosts[index];
    let identity = host.identity();
    let attached = host
        .inspect_peer_physical_inventory(identity.node, 1, TIMEOUT)
        .await?;
    ensure!(attached.view.registered_histories == 1);
    ensure!(!attached
        .inventory
        .context("attached source inventory")?
        .group_history(&source)
        .context("attached source")?
        .parked_retired_source());

    restart_source(cluster, index, 116 + index as u8).await?;
    let observer = &cluster.hosts[3];
    let revision = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
    inspect_current(observer, identity.node, revision).await?;
    let parked = observer
        .inspect_peer_physical_inventory(identity.node, revision, TIMEOUT)
        .await?;
    let boot = parked
        .view
        .fleet_boot
        .as_ref()
        .context("parked source boot")?;
    let old_boot = attached
        .view
        .fleet_boot
        .as_ref()
        .context("attached source boot")?;
    ensure!(
        boot.engine == old_boot.engine
            && boot.disk == old_boot.disk
            && boot.process != old_boot.process
    );
    ensure!(
        parked.view.node.groups == 1
            && parked.view.registered_histories == 0
            && parked.view.unattached_engine_histories == 1
            && parked.view.node.reserved_bytes == parked.view.node.profile.bytes_per_group
    );
    let inventory = parked.inventory.context("parked source inventory")?;
    let row = inventory
        .group_history(&source)
        .context("retained retired source")?;
    ensure!(row.parked_retired_source() && row.in_configuration());
    ensure!(matches!(
        cluster.hosts[index].group(source)?.status().await,
        Err(RuntimeError::NotFound)
    ));
    Ok(())
}

async fn restart_source(cluster: &mut ManagedCluster, index: usize, generation: u8) -> Result<()> {
    let fixture = &cluster.fixtures[index];
    let old = cluster.nodes.remove(index);
    let previous = cluster.hosts.remove(index);
    let identity = previous.identity();
    ensure!(matches!(
        old.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = previous.close().await?;
    ensure!(
        report.groups_total == 1
            && report.groups.len() == 1
            && report.omitted_groups == 0
            && report
                .groups
                .iter()
                .all(|group| !group.active && !group.failed)
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers,
        "sealed source shutdown inventory: {report:?}"
    );
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
            return Err(error.context(format!("retained source startup, shutdown={stopped:?}")));
        }
    };
    ensure!(host.identity().process != identity.process);
    cluster.nodes.insert(index, node);
    cluster.hosts.insert(index, host);
    Ok(())
}

async fn commit(
    voters: &[RaftCells],
    source: GroupHistory,
    transition: &ReopenedManagedCellGroupTransition,
) -> Result<CellGroupSourceRetirementAck> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        for voter in voters {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(!remaining.is_zero(), "sealed source did not retire");
            match voter
                .group(source)?
                .retire_managed_source(transition, remaining)
                .await
            {
                Ok(CellGroupSourceRetirement::Applied(ack)) => return Ok(*ack),
                Ok(CellGroupSourceRetirement::Accepted(_))
                | Err(CellGroupControllerError::Runtime(
                    RuntimeError::NotLeader | RuntimeError::NotReady,
                )) => (),
                Err(error) => return Err(error.into()),
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
