//! Real superseded connections, retained-engine restart and rooted final revoke.
//! Catalog lifecycle edits are fixture inputs, not operator removal certificates.
use super::retirement::{admission, discover, reserve, step, stopped};
use super::*;
use object_store::{path::Path, ObjectStore};

#[path = "raft_revocation_endpoint.rs"]
mod endpoint;

async fn installed(f: &Fixture, host: &RaftCells, revision: u64) -> Result<()> {
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if host
                    .observe_node(TIMEOUT)
                    .await?
                    .fleet_trust_revision
                    .is_some_and(|r| r >= revision)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await???;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_route_revocation() -> Result<()> {
    Box::pin(super::trust::verify_move_trust_acknowledgements()).await?;
    Box::pin(verify_route_revocation()).await
}

async fn verify_route_revocation() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (proposal, authorization) = birth(&f).await?;
    let cells = registered(&nodes, &authorization).await?;
    activate(&cells, &authorization).await?;
    write_read(&f, &cells, 1, 7, 7).await?;
    let p = primary(&f, &cells).await?;
    let source = &cells[p];
    let source_member = MemberId::new(p as u64 + 1)?;
    let source_node = nodes[p].spec.node;
    let (target_root, target_node, target_host) = f.peer_host(7).await?;
    f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 150)
        .await?;
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
                source: source_member,
                source_node,
                from_s3: false,
                timeout: TIMEOUT,
            },
        )
        .await?;
    let instance = target_host
        .observe_node(TIMEOUT)
        .await?
        .node
        .storage_instance;
    stopped(target_node).await?;
    let revision = f
        .state(NodeId::new([7; 16])?, FleetNodeState::RestartPending, 151)
        .await?;
    let mut old = endpoint::Endpoint::open(&f, target_root.path(), 7, 99).await?;
    let pending = old.block(&f, &nodes[p].host, revision).await?;
    ensure!(
        nodes[p]
            .host
            .resource_usage()
            .transport
            .context("transport unavailable")?
            .inspection_metadata
            .bytes
            > 0
    );
    old.log.close().await?;
    let spec = f.peer_spec(7)?;
    let root = f.catalog.read(TIMEOUT).await?;
    let auth = f.catalog.authorize_host(&root, spec.node, TIMEOUT).await?;
    let resumed_node = public_node::node_named(
        target_root.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&resumed_node, auth, &spec).await?;
    let process = ProcessGeneration::new([128; 16])?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = process;
    config.peers[0].process = process;
    config.cells = vec![RaftCellRegistration {
        placement: RaftCellPlacement::Joining {
            member: MemberId::new(4)?,
            source: source_member,
            source_node,
        },
        namespace: f.declaration.namespace.clone(),
        class: "Counter".into(),
        birth: proposal.clone(),
    }];
    let resumed = resumed_node.start_raft_cells(config, services).await?;
    let view = resumed.observe_node(TIMEOUT).await?;
    ensure!(
        view.node.storage_instance == instance
            && view.node.identity.process == process
            && view
                .startup_enrollment
                .context("missing restart enrollment")?
                .state
                == FleetNodeState::RestartPending
    );
    for node in &nodes {
        let view = f.remote(&node.host, spec.node, revision).await?;
        ensure!(view.view.node.identity.process == process);
    }
    let result = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, pending))
        .await??;
    ensure!(
        matches!(
            result,
            Err(InspectionError::StalePeer | InspectionError::Refused)
        ),
        "superseded call returned {result:?}"
    );
    old.joined(&f).await?;
    let key = Path::from(format!(
        "host-enrollment/raft/{}/nodes/{}/view",
        hex::encode(proposal.scope.cluster.bytes()),
        hex::encode(spec.node.bytes())
    ));
    let current = f.store.get(&key).await?.bytes().await?;
    ensure!(old.publication.publish(TIMEOUT).await.is_err());
    ensure!(f.store.get(&key).await?.bytes().await? == current);
    old.close(&f).await?;
    let restarted = resumed.cell(proposal.scope.namespace, &proposal.scope.cell)?;
    let receipt = source
        .command(
            f.definition.increment.clone(),
            5_i64.to_be_bytes().to_vec(),
            public_node::archived_options(2)?,
        )
        .await?;
    let read = restarted
        .query(
            f.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(receipt.version_token),
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        i64::from_be_bytes(read.result.as_slice().try_into()?) == 12
            && read.version.covers(&receipt.version)?
    );
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
    f.state(spec.node, FleetNodeState::Active, 152).await?;

    let (empty_root, empty_node, empty) = f.peer_host(8).await?;
    let empty_instance = empty.observe_node(TIMEOUT).await?.node.storage_instance;
    stopped(empty_node).await?;
    let mut removed = endpoint::Endpoint::open(&f, empty_root.path(), 8, 129).await?;
    ensure!(removed.log.storage_instance()? == empty_instance);
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    let mut pending = removed.block(&f, &nodes[p].host, revision).await?;
    for (ordinal, state) in [
        FleetNodeState::RestartPending,
        FleetNodeState::ResizePending,
        FleetNodeState::Draining,
        FleetNodeState::Evacuated,
    ]
    .into_iter()
    .enumerate()
    {
        let revision = f
            .state(removed.peer.node, state, 180 + ordinal as u8)
            .await?;
        installed(&f, &nodes[p].host, revision).await?;
        f.domain.run(async {
            cellar::asyncrt::select! {
                result = &mut pending => anyhow::bail!("exclusion revoked unresolved transport: {result:?}"),
                _ = cellar::asyncrt::sleep(Duration::from_millis(20)) => Ok::<_,anyhow::Error>(()),
            }
        }).await??;
        ensure!(removed.counts.metadata_closed.load(Ordering::Acquire) == 0);
    }
    let revision = f
        .state(removed.peer.node, FleetNodeState::Removed, 184)
        .await?;
    installed(&f, &nodes[p].host, revision).await?;
    removed.joined(&f).await?;
    ensure!(
        nodes[p]
            .host
            .resource_usage()
            .transport
            .context("transport unavailable")?
            .inspection_metadata
            .bytes
            == 0
    );
    let result = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, pending))
        .await??;
    ensure!(
        matches!(
            result,
            Err(InspectionError::StalePeer | InspectionError::Refused)
        ),
        "revoked call returned {result:?}"
    );
    let key = Path::from(format!(
        "host-enrollment/raft/{}/nodes/{}/view",
        hex::encode(proposal.scope.cluster.bytes()),
        hex::encode(removed.peer.node.bytes())
    ));
    f.store.delete(&key).await?;
    removed.publication.publish(TIMEOUT).await?;
    let started = removed.counts.started.load(Ordering::Acquire);
    ensure!(matches!(
        nodes[p]
            .host
            .inspect_peer_node(removed.peer.node, revision, TIMEOUT)
            .await,
        Err(InspectionError::Refused)
    ));
    ensure!(removed.counts.started.load(Ordering::Acquire) == started);
    removed.close(&f).await?;
    let current = [cells[p].clone(), cells[(p + 2) % 3].clone(), restarted];
    write_read(&f, &current, 3, 1, 13).await?;
    let proof = source
        .observe_fleet_membership(authorization, TIMEOUT)
        .await?;
    ensure!(
        proof.configuration().voters().contains(&MemberId::new(4)?)
            && proof.status().phase == Some(FleetPermitPhase::Complete)
    );
    for node in nodes {
        stopped(node.node).await?;
    }
    stopped(resumed_node).await?;
    drop((target_root, empty_root));
    f.domain.shutdown().await;
    Ok(())
}
