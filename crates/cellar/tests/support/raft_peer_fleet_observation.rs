//! Empty coordinators obtain actual remote safe ReadIndex, never status proofs.
use super::*;

async fn discover(f: &Fixture, nodes: &[Running], source: &RaftCells) -> Result<u64> {
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    for node in nodes {
        f.remote(source, node.spec.node, revision).await?;
        f.remote(&node.host, NodeId::new([7; 16])?, revision)
            .await?;
    }
    Ok(revision)
}

async fn idle(f: &Fixture, host: &RaftCells) -> Result<()> {
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let usage = host.resource_usage();
                ensure!(usage.query_pins.in_use == 0);
                ensure!(usage.control.bytes <= usage.control.max_bytes);
                if usage.control.bytes == 0 && usage.control.items == 0 {
                    return Ok(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_peer_observation_fresh_catalog_and_owned_io() -> Result<()> {
    use object_store::{path::Path, ObjectStore};
    let f = Fixture::with_domains(true, true).await?;
    let nodes = fleet(&f).await?;
    let (_source_root, source_node, source) = f.peer_host(7).await?;
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    let p = primary(&f, &cells).await?;
    discover(&f, &nodes, &source).await?;
    let key = Path::from(format!(
        "tenant/hosts/raft/{}/fleet/head",
        hex::encode(proposal.scope.cluster.bytes())
    ));
    let stale = f.store.inner.get(&key).await?.bytes().await?;
    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .commit(
            &root,
            &FleetMutation {
                id: [220; 16],
                operation: [221; 16],
                principal: "quorum-catalog-cut".into(),
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Policy,
                    key: "quorum-observation-cut".into(),
                    value: Some(vec![1]),
                }],
            },
            TIMEOUT,
        )
        .await?;
    let revision = discover(&f, &nodes, &source).await?;
    let current = f.store.inner.get(&key).await?.bytes().await?;
    // Fault injection emulates an old root read after both processes installed
    // the requested cut. Installed trust cannot replace this operation's read.
    f.store.inner.put(&key, stale.into()).await?;
    let refused = source
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await;
    f.store.inner.put(&key, current.into()).await?;
    ensure!(
        matches!(refused, Err(InspectionError::Unavailable)),
        "old root: {refused:?}"
    );
    source
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await?;

    f.store.fleet_read_hang.store(true, Ordering::Release);
    f.store.hold_fleet_reads.store(true, Ordering::Release);
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while f.store.active_fleet_reads.load(Ordering::Acquire) < 4 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    let pending = source.observe_peer_fleet_membership(
        nodes[p].spec.node,
        auth,
        revision,
        Duration::from_secs(60),
    );
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while f.store.active_fleet_reads.load(Ordering::Acquire) < 5 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    write_read(&f, &cells, 1, 4, 4).await?;
    drop(pending);
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while f.store.active_fleet_reads.load(Ordering::Acquire) != 4 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    f.store.hold_fleet_reads.store(false, Ordering::Release);
    f.store.fleet_read_hang.store(false, Ordering::Release);
    f.store.fleet_read_resume.notify_waiters();
    idle(&f, &source).await?;
    idle(&f, &nodes[p].host).await?;
    source_node.shutdown(ShutdownMode::Preserve).await?;
    stop(nodes).await?;
    ensure!(f.store.active_fleet_reads.load(Ordering::Acquire) == 0);
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_peer_observation_empty_coordinator() -> Result<()> {
    let f = Fixture::with_domains(true, true).await?;
    let nodes = fleet(&f).await?;
    let (_source_root, source_node, source) = f.peer_host(7).await?;
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    primary(&f, &cells).await?;
    let revision = discover(&f, &nodes, &source).await?;
    for (i, node) in nodes.iter().enumerate() {
        ensure!(cells[i].status().await?.version.is_none());
        let observed = source
            .observe_peer_fleet_membership(node.spec.node, auth.clone(), revision, TIMEOUT)
            .await?;
        let proof = observed.membership();
        ensure!(proof.replica().scope == proposal.scope);
        ensure!(proof.replica().node == node.spec.node);
        ensure!(proof.replica().disk == node.spec.disk);
        ensure!(proof.replica().process == observed.node().node.identity.process);
        ensure!(proof.barrier().get() > 0 && proof.applied().index >= proof.barrier());
        ensure!(proof.configuration().voters().len() == 3);
        ensure!(proof.configuration().outgoing_voters().is_empty());
        ensure!(proof.status().binding == f.catalog.group_binding(&proposal.scope)?);
        ensure!(observed
            .progress()
            .member
            .as_ref()
            .unwrap()
            .published
            .is_none());
        ensure!(
            observed
                .node()
                .current_enrollment
                .as_ref()
                .unwrap()
                .revision
                >= revision
        );
    }
    ensure!(source.observe_node(TIMEOUT).await?.node.groups == 0);
    ensure!(source.observe_node(TIMEOUT).await?.registered_histories == 0);
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    write_read(&f, &cells, 1, 4, 4).await?;
    let p = primary(&f, &cells).await?;
    let version = cells[p].status().await?.version.unwrap();
    let pending = source.observe_peer_fleet_membership(nodes[p].spec.node, auth, revision, TIMEOUT);
    f.domain
        .run(async { cellar::asyncrt::sleep(Duration::from_millis(150)).await })
        .await?;
    let observed = pending.await?;
    ensure!(observed.sample_age_ms() >= 150);
    ensure!(observed.progress().member.as_ref().unwrap().sample_age_ms >= 150);
    ensure!(observed.membership().barrier() >= version.position.index);
    ensure!(
        observed
            .progress()
            .member
            .as_ref()
            .unwrap()
            .published
            .unwrap()
            .index
            >= version.position.index
    );
    idle(&f, &source).await?;
    source_node.shutdown(ShutdownMode::Preserve).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_peer_observation_requires_resident_history() -> Result<()> {
    let f = Fixture::with_domains(true, true).await?;
    let nodes = fleet(&f).await?;
    let (_source_root, source_node, source) = f.peer_host(7).await?;
    let (_proposal, auth) = birth(&f).await?;
    let _cells = registered(&nodes, &auth).await?;
    let revision = discover(&f, &nodes, &source).await?;
    for node in &nodes {
        ensure!(matches!(
            source
                .observe_peer_fleet_membership(node.spec.node, auth.clone(), revision, TIMEOUT,)
                .await,
            Err(InspectionError::Refused)
        ));
        ensure!(node.host.observe_node(TIMEOUT).await?.node.groups == 0);
    }
    ensure!(matches!(
        source
            .observe_peer_fleet_membership(nodes[0].spec.node, auth.clone(), 0, TIMEOUT,)
            .await,
        Err(InspectionError::Refused)
    ));
    ensure!(matches!(
        source
            .observe_peer_fleet_membership(
                nodes[0].spec.node,
                auth,
                revision,
                Duration::from_secs(61),
            )
            .await,
        Err(InspectionError::Refused)
    ));
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    source_node.shutdown(ShutdownMode::Preserve).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_peer_observation_repair_roles() -> Result<()> {
    use super::retirement::{discover as discover_all, reserve, step};
    let f = Fixture::with_domains(true, true).await?;
    let nodes = fleet(&f).await?;
    let (_source_root, source_node, source) = f.peer_host(7).await?;
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    write_read(&f, &cells, 1, 4, 4).await?;
    let p = primary(&f, &cells).await?;
    let (_target_root, target_node, target_host) = f.peer_host(8).await?;
    f.state(NodeId::new([8; 16])?, FleetNodeState::Active, 150)
        .await?;
    let participants = [
        &nodes[0].host,
        &nodes[1].host,
        &nodes[2].host,
        &source,
        &target_host,
    ];
    discover_all(&f, &participants).await?;
    let target = target_host
        .register_managed_history(auth.clone(), TIMEOUT)
        .await?;
    let (plan, permit) = reserve(
        &f,
        &cells[p],
        &proposal.scope,
        1,
        160,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([8; 16])?,
            disk: DiskLineage::new([18; 16])?,
        },
    )
    .await?;
    discover_all(&f, &participants).await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    cells[p]
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
    let learner = source
        .observe_peer_fleet_membership(NodeId::new([8; 16])?, auth.clone(), revision, TIMEOUT)
        .await?;
    ensure!(learner.membership().configuration().learners() == [MemberId::new(4)?]);
    let (binding, _) = learner
        .membership()
        .joined_member(MemberId::new(4)?)?
        .context("joined learner binding")?;
    ensure!(binding.node == NodeId::new([8; 16])? && binding.disk == DiskLineage::new([18; 16])?);
    ensure!(learner.membership().status().phase == Some(FleetPermitPhase::Seeding));
    ensure!(
        step(&f, &cells[p], &permit, RepairStep::PromoteAndRemove)
            .await?
            .phase
            == RepairPhase::Joint
    );
    let joint = source
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await?;
    ensure!(joint.membership().configuration().voters().len() == 3);
    ensure!(joint.membership().configuration().outgoing_voters().len() == 3);
    ensure!(joint.membership().status().phase == Some(FleetPermitPhase::Joint));
    ensure!(
        step(&f, &cells[p], &permit, RepairStep::FinishJoint)
            .await?
            .phase
            == RepairPhase::Complete
    );
    let done = source
        .observe_peer_fleet_membership(NodeId::new([8; 16])?, auth, revision, TIMEOUT)
        .await?;
    ensure!(done
        .membership()
        .configuration()
        .outgoing_voters()
        .is_empty());
    ensure!(done.membership().configuration().learners().is_empty());
    ensure!(done.membership().status().phase == Some(FleetPermitPhase::Complete));
    write_read(&f, &[cells[p].clone(), target], 2, 5, 9).await?;
    ensure!(source.observe_node(TIMEOUT).await?.node.groups == 0);
    source_node.shutdown(ShutdownMode::Preserve).await?;
    target_node.shutdown(ShutdownMode::Preserve).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_peer_observation_minority_cancel_and_shutdown() -> Result<()> {
    let f = Fixture::with_domains(true, true).await?;
    let mut nodes = fleet(&f).await?;
    let (_source_root, source_node, source) = f.peer_host(7).await?;
    let (_proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    let p = primary(&f, &cells).await?;
    let revision = discover(&f, &nodes, &source).await?;
    source
        .observe_peer_fleet_membership(nodes[p].spec.node, auth.clone(), revision, TIMEOUT)
        .await?;
    let target = nodes.remove(p);
    stop(nodes).await?;
    f.remote(&source, target.spec.node, revision).await?;
    let result = source
        .observe_peer_fleet_membership(
            target.spec.node,
            auth.clone(),
            revision,
            Duration::from_millis(350),
        )
        .await;
    ensure!(
        matches!(
            result,
            Err(InspectionError::Unavailable | InspectionError::Overloaded)
        ),
        "minority: {result:?}"
    );
    let pending = source.observe_peer_fleet_membership(
        target.spec.node,
        auth.clone(),
        revision,
        Duration::from_secs(60),
    );
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if target.host.resource_usage().control.items >= 2 {
                    break;
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    drop(pending);
    idle(&f, &source).await?;
    idle(&f, &target.host).await?;
    // A retained, unpolled SDK waiter cannot keep either host alive on close.
    let retained = source.observe_peer_fleet_membership(
        target.spec.node,
        auth,
        revision,
        Duration::from_secs(60),
    );
    source_node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(retained.await.is_err());
    idle(&f, &target.host).await?;
    target.node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(source.resource_usage().control.bytes == 0);
    ensure!(target.host.resource_usage().control.bytes == 0);
    f.domain.shutdown().await;
    Ok(())
}
