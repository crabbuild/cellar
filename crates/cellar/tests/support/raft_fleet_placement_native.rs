//! Rooted separation applies to actual managed repair, not only birth counts.
//! Declared labels are trusted fixture inputs, not deployed host qualification.
use super::retirement::{admission, discover, reserve, step, stopped};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_physical_host() -> Result<()> {
    Box::pin(physical_hosts()).await?;
    Box::pin(explicit_colocation()).await
}

async fn physical_hosts() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (proposal, authorization) = birth(&f).await?;
    let cells = registered(&nodes, &authorization).await?;
    activate(&cells, &authorization).await?;
    write_read(&f, &cells, 1, 7, 7).await?;
    let p = primary(&f, &cells).await?;
    let source = &cells[p];
    let mut spec = f.peer_spec(7)?;
    spec.physical_host = nodes[p].spec.physical_host;
    let (bad_root, bad_node, bad_host) = f.peer_host_with_spec(spec.clone()).await?;
    f.state(spec.node, FleetNodeState::Active, 150).await?;
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &bad_host],
    )
    .await?;
    let bad_cell = bad_host
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    let (plan, permit) = reserve(
        &f,
        source,
        &proposal.scope,
        1,
        160,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: spec.node,
            disk: spec.disk,
        },
    )
    .await?;
    let key = f.catalog.group_binding(&proposal.scope)?.key()?;
    let root = f.catalog.read(TIMEOUT).await?;
    let reserved = f
        .catalog
        .get(&root, FleetTable::Admissions, &key, TIMEOUT)
        .await?;
    ensure!(reserved.is_some());
    ensure!(matches!(
        source
            .begin_managed_repair(permit.clone(), plan, TIMEOUT)
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    let proof = source
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(proof.status().consumed_sequence == 0 && proof.configuration().learners().is_empty());
    ensure!(source.status().await?.member_high_water == 3);
    ensure!(bad_cell.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    ensure!(bad_host.observe_node(TIMEOUT).await?.node.groups == 0);
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .get(&root, FleetTable::Admissions, &key, TIMEOUT)
            .await?
            == reserved
    );
    let fenced = source.fence_fleet_permit(permit, TIMEOUT).await?;
    ensure!(fenced.consumed_sequence == 1 && fenced.phase == Some(FleetPermitPhase::Fenced));
    let proof = source
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(proof.status() == &fenced);
    admission(&f, &proposal.scope, 162, None).await?;

    let (good_root, good_node, good_host) = f.peer_host(8).await?;
    f.state(NodeId::new([8; 16])?, FleetNodeState::Active, 151)
        .await?;
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &good_host],
    )
    .await?;
    let target = good_host
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    let (plan, permit) = reserve(
        &f,
        source,
        &proposal.scope,
        2,
        170,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(5)?,
            node: NodeId::new([8; 16])?,
            disk: DiskLineage::new([18; 16])?,
        },
    )
    .await?;
    source
        .begin_managed_repair(permit.clone(), plan, TIMEOUT)
        .await?;
    target
        .activate_managed_learner(
            permit.clone(),
            LearnerActivation {
                member: MemberId::new(5)?,
                source: MemberId::new(p as u64 + 1)?,
                source_node: nodes[p].spec.node,
                from_s3: false,
                timeout: TIMEOUT,
            },
        )
        .await?;
    ensure!(
        step(&f, source, &permit, RepairStep::PromoteAndRemove)
            .await?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&f, source, &permit, RepairStep::FinishJoint)
            .await?
            .phase
            == RepairPhase::Complete
    );
    admission(&f, &proposal.scope, 172, None).await?;
    write_read(
        &f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone(), target],
        2,
        5,
        12,
    )
    .await?;
    for node in nodes {
        stopped(node.node).await?;
    }
    stopped(bad_node).await?;
    stopped(good_node).await?;
    drop((bad_root, good_root));
    f.domain.shutdown().await;
    Ok(())
}

async fn explicit_colocation() -> Result<()> {
    let f = Fixture::with_separation(true, false, FleetReplicaSeparation::Nodes).await?;
    let nodes = fleet(&f).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(root.replica_separation() == FleetReplicaSeparation::Nodes);
    ensure!(matches!(
        f.catalog.initialize(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let (proposal, authorization) = birth(&f).await?;
    let cells = registered(&nodes, &authorization).await?;
    activate(&cells, &authorization).await?;
    write_read(&f, &cells, 1, 7, 7).await?;
    let p = primary(&f, &cells).await?;
    let source = &cells[p];
    let (target_root, target_node, target_host) = f.peer_host(7).await?;
    f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 150)
        .await?;
    let target_view = target_host.observe_node(TIMEOUT).await?;
    ensure!(
        target_view
            .current_enrollment
            .context("target enrollment")?
            .physical_host
            == nodes[p].spec.physical_host
    );
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &target_host],
    )
    .await?;
    let target = target_host
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    let (plan, permit) = reserve(
        &f,
        source,
        &proposal.scope,
        1,
        160,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: DiskLineage::new([17; 16])?,
        },
    )
    .await?;
    source
        .begin_managed_repair(permit.clone(), plan, TIMEOUT)
        .await?;
    target
        .activate_managed_learner(
            permit.clone(),
            LearnerActivation {
                member: MemberId::new(4)?,
                source: MemberId::new(p as u64 + 1)?,
                source_node: nodes[p].spec.node,
                from_s3: false,
                timeout: TIMEOUT,
            },
        )
        .await?;
    ensure!(
        step(&f, source, &permit, RepairStep::PromoteAndRemove)
            .await?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&f, source, &permit, RepairStep::FinishJoint)
            .await?
            .phase
            == RepairPhase::Complete
    );
    admission(&f, &proposal.scope, 162, None).await?;
    write_read(
        &f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone(), target],
        2,
        5,
        12,
    )
    .await?;
    for node in nodes {
        stopped(node.node).await?;
    }
    stopped(target_node).await?;
    drop(target_root);
    f.domain.shutdown().await;
    Ok(())
}
