//! Exact managed repair can seed a cold configuration without a user write.
//! Privileged rooted fixture inputs do not qualify the pending reconciler.
use super::retirement::{discover, reserve, step, stopped};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_empty_archive_seed_retains_guard_and_completes_repair() -> Result<()> {
    let f = Fixture::with_domains(true, true)
        .await
        .context("construct managed cold fixture")?;
    let nodes = fleet(&f).await.context("start managed cold voters")?;
    let (proposal, auth) = birth(&f).await.context("authorize managed cold birth")?;
    let mut cells = registered(&nodes, &auth)
        .await
        .context("register managed cold voters")?;
    activate(&cells, &auth)
        .await
        .context("activate managed cold voters")?;
    let p = primary(&f, &cells)
        .await
        .context("select initial managed cold primary")?;
    let obsolete = (p + 1) % 3;
    let source = cells[p].clone();
    let (_root, target_node, host) = f.peer_host(7).await.context("start managed cold spare")?;
    f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 150)
        .await
        .context("activate fixture cold spare metadata")?;
    discover(&f, &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &host])
        .await
        .context("discover managed cold endpoints")?;
    let target = host
        .register_managed_history(auth.clone(), TIMEOUT)
        .await
        .context("register cold spare history")?;
    let (plan, permit) = reserve(
        &f,
        &source,
        &proposal.scope,
        1,
        160,
        MemberId::new(obsolete as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: DiskLineage::new([17; 16])?,
        },
    )
    .await
    .context("reserve immutable managed cold repair permit")?;
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
        step(&f, &source, &permit, RepairStep::PromoteAndRemove)
            .await
            .context("promote managed cold learner and remove obsolete voter")?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&f, &source, &permit, RepairStep::FinishJoint)
            .await
            .context("finish managed cold joint configuration")?
            .phase
            == RepairPhase::Complete
    );
    cells[obsolete] = target;
    write_read(&f, &cells, 1, 13, 13)
        .await
        .context("first managed SQL write after cold replacement")?;
    ensure!(f.calls.load(Ordering::Acquire) == 1);
    stopped(target_node)
        .await
        .context("stop cold replacement node")?;
    stop(nodes).await.context("stop original cold voters")?;
    f.domain.shutdown().await;
    Ok(())
}
