// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact managed repair can seed a cold configuration without a user write.
//! Actual capacity and exact-boot permits precede learner initialization.
//! Occupancy remains fixture attestation, not kernel qualification.
use super::super::retirement::{discover, step};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_empty_archive_seed_retains_guard_and_completes_repair() -> Result<()> {
    let s = Box::pin(Setup::with_storage(
        true,
        8,
        1,
        RaftImageStorage::Materialized,
    ))
    .await?;
    Box::pin(cold_repair(s)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_extent_empty_archive_seed_retains_guard_and_completes_repair() -> Result<()> {
    let s = Box::pin(Setup::with_storage(true, 8, 1, RaftImageStorage::Extents)).await?;
    Box::pin(cold_repair(s)).await
}

async fn cold_repair(s: Setup) -> Result<()> {
    let f = &s.f;
    let nodes = &s.nodes;
    let proposal = &s.proposals[0];
    let root = f.catalog.read(TIMEOUT).await?;
    let auth = f
        .catalog
        .authorize_group(&root, &proposal.scope, TIMEOUT)
        .await?;
    let mut cells = registered(nodes, &auth)
        .await
        .context("register managed cold voters")?;
    activate(&cells, &auth)
        .await
        .context("activate managed cold voters")?;
    let p = primary(f, &cells)
        .await
        .context("select initial managed cold primary")?;
    let obsolete = (p + 1) % 3;
    let source = cells[p].clone();
    discover(
        f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &s.target],
    )
    .await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let resources = s.request(0, 160, root.revision()).await?;
    let plan = RepairPlan {
        id: ProposalId::new([161; 16])?,
        policy_revision: 1,
        expected_revision: source.status().await?.membership_revision,
        obsolete: MemberId::new(obsolete as u64 + 1)?,
        replacement: JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: f.peer_spec(7)?.disk,
        },
    };
    let accepted = f
        .catalog
        .reserve_movement(
            &root,
            &FleetMovementRequest {
                resources,
                source: nodes[p].spec.node,
                plan: plan.clone(),
            },
            &nodes[p].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await
        .context("reserve exact-boot cold movement")?;
    let permit = accepted.authorization();
    let target = s
        .target
        .register_managed_history(auth.clone(), TIMEOUT)
        .await?;
    discover(
        f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &s.target],
    )
    .await?;
    source
        .begin_managed_repair(permit.clone(), plan, TIMEOUT)
        .await
        .context("begin managed cold repair on initial source")?;
    target
        .activate_managed_learner(
            permit.clone(),
            LearnerActivation {
                member: MemberId::new(4)?,
                source: MemberId::new(p as u64 + 1)?,
                source_node: nodes[p].spec.node,
                from_s3: true,
                timeout: TIMEOUT,
            },
        )
        .await
        .context("activate managed learner from cold archive")?;
    ensure!(target.status().await?.version.is_none());
    let proof = target
        .observe_fleet_membership(auth.clone(), TIMEOUT)
        .await
        .context("observe cold learner fleet guard")?;
    ensure!(proof.status().consumed_sequence == 1);
    ensure!(proof
        .configuration()
        .learners()
        .contains(&MemberId::new(4)?));
    ensure!(proof.status().binding == f.catalog.group_binding(&proposal.scope)?);
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    ensure!(
        step(f, &source, &permit, RepairStep::PromoteAndRemove)
            .await
            .context("promote managed cold learner and remove obsolete voter")?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(f, &source, &permit, RepairStep::FinishJoint)
            .await
            .context("finish managed cold joint configuration")?
            .phase
            == RepairPhase::Complete
    );
    cells[obsolete] = target;
    write_read(f, &cells, 1, 13, 13)
        .await
        .context("first managed SQL write after cold replacement")?;
    ensure!(f.calls.load(Ordering::Acquire) == 1);
    s.close().await
}
