//! Rooted separation applies to actual managed repair, not only birth counts.
//! Declared labels are trusted fixture inputs, not deployed host qualification.
use super::retirement::{discover, step, stopped};
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
    let key = f.catalog.group_binding(&proposal.scope)?.key()?;
    let refused = reserve_capacity(
        &f,
        &nodes[p].host,
        &bad_host,
        &proposal.scope,
        180,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: spec.node,
            disk: spec.disk,
        },
    )
    .await;
    ensure!(
        matches!(&refused, Err(error) if matches!(error.downcast_ref::<FleetCatalogError>(), Some(FleetCatalogError::Conflict))),
        "physical host admission: {refused:?}"
    );
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .pending_reservations(&root, TIMEOUT)
        .await?
        .is_empty());
    ensure!(f
        .catalog
        .get(&root, FleetTable::Admissions, &key, TIMEOUT)
        .await?
        .is_none());
    let proof = source
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(proof.status().consumed_sequence == 0 && proof.configuration().learners().is_empty());
    ensure!(source.status().await?.member_high_water == 3);
    ensure!(bad_cell.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    ensure!(bad_host.observe_node(TIMEOUT).await?.node.groups == 0);

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
    let (plan, permit) = reserve_capacity(
        &f,
        &nodes[p].host,
        &good_host,
        &proposal.scope,
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
        .await
        .context("begin separated repair")?;
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
        .await
        .context("activate separated learner")?;
    ensure!(
        step(&f, source, &permit, RepairStep::PromoteAndRemove)
            .await
            .context("promote separated learner")?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&f, source, &permit, RepairStep::FinishJoint)
            .await
            .context("finish separated repair")?
            .phase
            == RepairPhase::Complete
    );

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
    let (plan, permit) = reserve_capacity(
        &f,
        &nodes[p].host,
        &target_host,
        &proposal.scope,
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
        .await
        .context("begin separated repair")?;
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
        .await
        .context("activate separated learner")?;
    ensure!(
        step(&f, source, &permit, RepairStep::PromoteAndRemove)
            .await
            .context("promote separated learner")?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&f, source, &permit, RepairStep::FinishJoint)
            .await
            .context("finish separated repair")?
            .phase
            == RepairPhase::Complete
    );

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

// Exercise managed placement with a real boot-bound capacity debit. Physical
// host labels and resource measurements remain explicit fixture attestations.
pub(super) async fn reserve_capacity(
    f: &Fixture,
    origin: &RaftCells,
    target: &RaftCells,
    scope: &GroupScope,
    id: u8,
    obsolete: MemberId,
    replacement: JoiningMember,
) -> Result<(RepairPlan, FleetPermitAuthorization)> {
    qualify_capacity(f, origin, target, id + 1).await?;
    reserve_qualified(f, origin, target, scope, id, obsolete, replacement).await
}

pub(super) async fn qualify_capacity(
    f: &Fixture,
    origin: &RaftCells,
    target: &RaftCells,
    id: u8,
) -> Result<()> {
    let (namespace, _) = initial_birth(f).await?;
    let evidence = super::super::capacity_tests::attestation(f, origin, target).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .admit_node_capacity(
            &root,
            &FleetCapacityRequest {
                identity: Fixture::request(id, root.revision()),
                attestation: evidence,
                action: FleetCapacityAction::Activate,
            },
            origin,
            std::slice::from_ref(&namespace),
            TIMEOUT,
        )
        .await
        .context("qualify placement destination")?;
    let root = f.catalog.read(TIMEOUT).await?;
    let node = target.observe_node(TIMEOUT).await?.node.identity.node;
    f.remote(origin, node, root.revision()).await?;
    Ok(())
}

pub(super) async fn reserve_qualified(
    f: &Fixture,
    origin: &RaftCells,
    target: &RaftCells,
    scope: &GroupScope,
    id: u8,
    obsolete: MemberId,
    replacement: JoiningMember,
) -> Result<(RepairPlan, FleetPermitAuthorization)> {
    let (namespace, _) = initial_birth(f).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    f.remote(origin, replacement.node, root.revision()).await?;
    let local = target.observe_node(TIMEOUT).await?;
    let source = origin.cell(scope.namespace, &scope.cell)?.status().await?;
    let plan = RepairPlan {
        id: ProposalId::new([id; 16])?,
        policy_revision: 1,
        expected_revision: source.membership_revision,
        obsolete,
        replacement,
    };
    let request = FleetMovementRequest {
        resources: FleetResourceRequest {
            identity: Fixture::request(id, root.revision()),
            group: scope.clone(),
            attestation: FleetResourceAttestation {
                boot: local.fleet_boot.context("placement target boot")?,
                profile_revision: 1,
                used: FleetResourceBounds {
                    engine_slots: local.node.groups,
                    durable_bytes: 1024 * 1024,
                    memory_bytes: 64 * 1024 * 1024,
                },
                evidence_digest: [71; 32],
                evidence_reference: "fixture-only/placement-occupancy".into(),
            },
            peak: FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: local.node.profile.bytes_per_group,
                memory_bytes: 8 * 1024 * 1024,
            },
        },
        source: origin.observe_node(TIMEOUT).await?.node.identity.node,
        plan: plan.clone(),
    };
    let admitted = f
        .catalog
        .reserve_movement(&root, &request, origin, &[namespace], TIMEOUT)
        .await
        .context("reserve placement movement")?;
    Ok((plan, admitted.authorization()))
}
