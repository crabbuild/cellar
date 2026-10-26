//! Actual guarded retirement with a deliberately stale, intact old engine.
//! Catalog permit edits stand in for the pending fleet reconciler; this test
//! grants no node evacuation/removal or infrastructure-stop certificate.
use super::*;
use cellar::application::consensus::verification::LogError;
use object_store::{path::Path, ObjectStore};
use sha2::{Digest, Sha256};

pub(super) async fn admission(
    f: &Fixture,
    scope: &GroupScope,
    id: u8,
    value: Option<Vec<u8>>,
) -> Result<()> {
    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .commit(
            &root,
            &FleetMutation {
                id: [id; 16],
                operation: [230; 16],
                principal: "retirement-fixture".into(),
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Admissions,
                    key: f.catalog.group_binding(scope)?.key()?,
                    value,
                }],
            },
            TIMEOUT,
        )
        .await?;
    Ok(())
}

pub(super) async fn reserve(
    f: &Fixture,
    source: &RaftCellHandle,
    scope: &GroupScope,
    sequence: u64,
    id: u8,
    obsolete: MemberId,
    replacement: JoiningMember,
) -> Result<(RepairPlan, FleetPermitAuthorization)> {
    let plan = RepairPlan {
        id: ProposalId::new([id; 16])?,
        policy_revision: 1,
        expected_revision: source.status().await?.membership_revision,
        obsolete,
        replacement,
    };
    let permit = FleetMembershipPermit::new(
        f.catalog.group_binding(scope)?,
        sequence,
        ProposalId::new([id + 1; 16])?,
        ProposalId::new([230; 16])?,
        &plan,
    )?;
    admission(f, scope, id + 1, Some(permit.encode()?)).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let authorization = f
        .catalog
        .authorize_membership(&root, scope, TIMEOUT)
        .await?;
    Ok((plan, authorization))
}

pub(super) async fn step(
    f: &Fixture,
    source: &RaftCellHandle,
    permit: &FleetPermitAuthorization,
    action: RepairStep,
) -> Result<RepairStatus> {
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let expected = source.status().await?.membership_revision;
                match source
                    .advance_managed_repair(
                        permit.clone(),
                        action,
                        expected,
                        Duration::from_secs(2),
                    )
                    .await
                {
                    Ok(status) => return Ok(status),
                    Err(CommandFailure::Unavailable | CommandFailure::Overloaded) => {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }))
        .await?
        .context("managed retirement step timed out")?
}

pub(super) async fn discover(f: &Fixture, hosts: &[&RaftCells]) -> Result<()> {
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    for origin in hosts {
        let own = origin.observe_node(TIMEOUT).await?.node.identity.node;
        for target in hosts {
            let node = target.observe_node(TIMEOUT).await?.node.identity.node;
            if node != own {
                f.remote(origin, node, revision).await?;
            }
        }
    }
    Ok(())
}

pub(super) async fn stopped(node: cellar::sdk::Node) -> Result<()> {
    let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await? else {
        anyhow::bail!("managed retirement fixture did not stop");
    };
    let raft = report
        .raft
        .context("missing physical Raft shutdown report")?;
    ensure!(!raft.startup_incomplete && raft.result?.unfinished.is_empty());
    Ok(())
}

async fn no_claim(f: &Fixture, scope: &GroupScope, member: u64) -> Result<()> {
    let path = Path::from(format!(
        "host-enrollment/namespaces/{}/cells/{}/claims/{member:016x}",
        hex::encode(scope.namespace.bytes()),
        hex::encode(Sha256::digest(scope.cell.as_bytes()))
    ));
    ensure!(
        matches!(
            f.store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "retained engine created a fresh member claim"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_retired_binding() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let mut nodes = fleet(&f).await?;
    let (proposal, authorization) = birth(&f).await?;
    let cells = registered(&nodes, &authorization).await?;
    activate(&cells, &authorization).await?;
    write_read(&f, &cells, 1, 7, 7)
        .await
        .context("initial managed write")?;
    let primary = primary(&f, &cells).await?;
    let obsolete = (primary + 1) % 3;
    let obsolete_member = MemberId::new(obsolete as u64 + 1)?;
    let source_member = MemberId::new(primary as u64 + 1)?;
    let source_node = nodes[primary].spec.node;
    let source = cells[primary].clone();
    let (target_root, target_node, target_host) = f.peer_host(7).await?;
    f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 150)
        .await?;
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &target_host],
    )
    .await
    .context("discover first learner host")?;
    let target = target_host
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(target.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    let (plan, permit) = reserve(
        &f,
        &source,
        &proposal.scope,
        1,
        160,
        obsolete_member,
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
        .context("begin peer-seeded replacement")?;
    let observed = target_host.observe_node(TIMEOUT).await?;
    ensure!(
        observed.startup_enrollment.as_ref().map(|e| e.state) == Some(FleetNodeState::Registered)
            && observed.current_enrollment.as_ref().map(|e| e.state)
                == Some(FleetNodeState::Active)
    );
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
        .await
        .context("peer seed into fresh managed host")?;
    ensure!(
        step(&f, &source, &permit, RepairStep::PromoteAndRemove)
            .await?
            .phase
            == RepairPhase::Joint
    );
    let old_proof = cells[obsolete]
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(
        old_proof.status().phase == Some(FleetPermitPhase::Joint)
            && old_proof
                .configuration()
                .outgoing_voters()
                .contains(&obsolete_member)
    );
    let removed = nodes.remove(obsolete);
    let old_instance = removed
        .host
        .observe_node(TIMEOUT)
        .await?
        .node
        .storage_instance;
    let log_path = removed.node.info().data_dir.join("raft");
    let old_spec = removed.spec.clone();
    stopped(removed.node)
        .await
        .context("stop outgoing voter in joint state")?;
    ensure!(
        step(&f, &source, &permit, RepairStep::FinishJoint)
            .await?
            .phase
            == RepairPhase::Complete
    );
    let proof = source
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(
        proof.status().consumed_sequence == 1
            && proof.status().phase == Some(FleetPermitPhase::Complete)
            && proof.configuration().voters().len() == 3
            && proof.configuration().outgoing_voters().is_empty()
            && !proof.configuration().voters().contains(&obsolete_member)
    );
    admission(&f, &proposal.scope, 162, None).await?;

    // The physical engine did not see final removal. It must not be reset just
    // because the current quorum no longer contains its original member.
    let root = f.catalog.read(TIMEOUT).await?;
    let host_authorization = f
        .catalog
        .authorize_host(&root, old_spec.node, TIMEOUT)
        .await?;
    ensure!(host_authorization.state() == FleetNodeState::Active);
    let node = public_node::node_named(
        removed.root.path(),
        f.definition.application.clone(),
        &old_spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&node, host_authorization, &old_spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    let process = ProcessGeneration::new([old_spec.node.bytes()[0] + 100; 16])?;
    config.infrastructure.log.process = process;
    config.peers[0].process = process;
    let old_host = node.start_raft_cells(config, services).await?;
    let retained = old_host.observe_node(TIMEOUT).await?;
    ensure!(
        retained.node.storage_instance == old_instance
            && retained.node.groups == 1
            && retained.unattached_engine_histories == 1
            && retained.registered_histories == 0
    );
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &target_host, &old_host],
    )
    .await
    .context("discover reopened retired host")?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let physical = nodes[0]
        .host
        .inspect_peer_physical_inventory(old_spec.node, cut.revision(), TIMEOUT)
        .await?;
    let inventory = physical
        .inventory
        .as_ref()
        .context("missing retained physical inventory")?;
    let occupied = inventory
        .history(&proposal.scope)?
        .context("unregistered retired history was omitted")?;
    let (namespace, _) = initial_birth(&f).await?;
    let expected_birth = namespace
        .birth(&proposal.scope.cell)
        .await?
        .context("retained canonical birth")?;
    ensure!(
        occupied.member() == obsolete_member && occupied.birth_digest() == expected_birth.digest()
    );
    ensure!(physical.view.unattached_engine_histories == 1 && inventory.histories().len() == 1);
    let (plan, stale_target) = reserve(
        &f,
        &source,
        &proposal.scope,
        2,
        170,
        MemberId::new(4)?,
        JoiningMember {
            member: MemberId::new(5)?,
            node: old_spec.node,
            disk: old_spec.disk,
        },
    )
    .await?;
    source
        .begin_managed_repair(stale_target.clone(), plan, TIMEOUT)
        .await
        .context("begin replacement on retired engine")?;
    let before = f.calls.load(Ordering::Acquire);
    ensure!(
        matches!(
            old_host
                .register_managed_history(authorization.clone(), TIMEOUT)
                .await,
            Err(CommandFailure::CommandConflict)
        ),
        "retired slot accepted a fresh metadata binding"
    );
    ensure!(old_host
        .cell(proposal.scope.namespace, &proposal.scope.cell)
        .is_err());
    no_claim(&f, &proposal.scope, 5).await?;
    ensure!(f.calls.load(Ordering::Acquire) == before);
    ensure!(
        old_host
            .observe_node(TIMEOUT)
            .await?
            .unattached_engine_histories
            == 1
    );
    ensure!(
        step(&f, &source, &stale_target, RepairStep::AbortLearner)
            .await?
            .phase
            == RepairPhase::Aborted
    );
    let proof = source
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(
        proof.status().consumed_sequence == 2
            && proof.status().phase == Some(FleetPermitPhase::Aborted)
            && proof.configuration().learners().is_empty()
    );
    admission(&f, &proposal.scope, 172, None).await?;
    stopped(node).await?;
    let log = f
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                log_path,
                old_spec.node,
                old_spec.disk,
                ProcessGeneration::new([old_spec.node.bytes()[0] + 150; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    ensure!(log.storage_instance()? == old_instance);
    let group = log.group(&proposal.scope)?;
    ensure!(
        group.identity().member == obsolete_member
            && group.membership()?.fleet_status().as_ref() == Some(old_proof.status())
            && group.membership()?.high_water() == 4
    );
    let (namespace, _) = initial_birth(&f).await?;
    let actual_birth = namespace
        .birth(&proposal.scope.cell)
        .await?
        .context("managed birth missing")?;
    let rebinding = GroupBootstrap {
        scope: proposal.scope.clone(),
        member: MemberId::new(5)?,
        birth_digest: actual_birth.digest(),
        voters: proposal.voters.iter().map(|m| m.member).collect(),
        learners: vec![MemberId::new(5)?],
    };
    ensure!(
        matches!(
            f.domain.run(log.create_group(rebinding)).await?,
            Err(LogError::WrongIdentity)
        ),
        "stale physical history attached under a fresh member ID"
    );
    ensure!(
        log.group(&proposal.scope)?.identity().member == obsolete_member
            && log
                .group(&proposal.scope)?
                .membership()?
                .fleet_status()
                .as_ref()
                == Some(old_proof.status())
    );
    no_claim(&f, &proposal.scope, 5).await?;
    log.close().await?;

    // A different enrolled engine is usable; the consumed failed member ID is
    // retained, and a newer ID receives an actual archive seed and native SQL.
    let (fresh_root, fresh_node, original_host) = f.peer_host(8).await?;
    let fresh_instance = original_host
        .observe_node(TIMEOUT)
        .await?
        .node
        .storage_instance;
    f.state(NodeId::new([8; 16])?, FleetNodeState::Active, 151)
        .await?;
    stopped(fresh_node).await?;
    let fresh_spec = f.peer_spec(8)?;
    let fresh_node = public_node::node_named(
        fresh_root.path(),
        f.definition.application.clone(),
        &fresh_spec.label,
    )
    .await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let fresh_authorization = f
        .catalog
        .authorize_host(&root, fresh_spec.node, TIMEOUT)
        .await?;
    let (mut config, services) = f
        .config_for(&fresh_node, fresh_authorization, &fresh_spec)
        .await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    let process = ProcessGeneration::new([128; 16])?;
    config.infrastructure.log.process = process;
    config.peers[0].process = process;
    let fresh_host = fresh_node.start_raft_cells(config, services).await?;
    let observed = fresh_host.observe_node(TIMEOUT).await?;
    ensure!(
        observed.startup_enrollment.unwrap().state == FleetNodeState::Active
            && observed.node.storage_instance == fresh_instance
    );
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &target_host, &fresh_host],
    )
    .await
    .context("discover resumed fresh host")?;
    let target_fresh = fresh_host
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(target_fresh.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    let mut current = vec![
        cells[primary].clone(),
        cells[(primary + 2) % 3].clone(),
        target.clone(),
    ];
    write_read(&f, &current, 2, 5, 12)
        .await
        .context("write before archive replacement")?;
    let (plan, fresh_permit) = reserve(
        &f,
        &source,
        &proposal.scope,
        3,
        180,
        MemberId::new(4)?,
        JoiningMember {
            member: MemberId::new(6)?,
            node: NodeId::new([8; 16])?,
            disk: DiskLineage::new([18; 16])?,
        },
    )
    .await?;
    source
        .begin_managed_repair(fresh_permit.clone(), plan, TIMEOUT)
        .await
        .context("begin archive-seeded replacement")?;
    let receipt = source
        .command(
            f.definition.increment.clone(),
            9_i64.to_be_bytes().to_vec(),
            public_node::archived_options(3)?,
        )
        .await
        .context("write archive replacement seed")?;
    ensure!(receipt.durability == Durability::S3);
    f.state(fresh_spec.node, FleetNodeState::Draining, 154)
        .await?;
    ensure!(
        matches!(
            target_fresh
                .activate_managed_learner(
                    fresh_permit.clone(),
                    LearnerActivation {
                        member: MemberId::new(6)?,
                        source: source_member,
                        source_node,
                        from_s3: true,
                        timeout: TIMEOUT,
                    }
                )
                .await,
            Err(CommandFailure::Unavailable)
        ),
        "startup Active bypassed current target exclusion"
    );
    no_claim(&f, &proposal.scope, 6).await?;
    ensure!(
        target_fresh.inspect().await?.occupancy == ReplicaOccupancy::Standby
            && fresh_host.observe_node(TIMEOUT).await?.node.groups == 0
    );
    f.state(fresh_spec.node, FleetNodeState::Active, 155)
        .await?;
    target_fresh
        .activate_managed_learner(
            fresh_permit.clone(),
            LearnerActivation {
                member: MemberId::new(6)?,
                source: source_member,
                source_node,
                from_s3: true,
                timeout: TIMEOUT,
            },
        )
        .await
        .context("archive seed into fresh managed host")?;
    let read = target_fresh
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
        i64::from_be_bytes(read.result.as_slice().try_into()?) == 21
            && read.version.covers(&receipt.version)?
    );
    ensure!(
        step(&f, &source, &fresh_permit, RepairStep::PromoteAndRemove)
            .await?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&f, &source, &fresh_permit, RepairStep::FinishJoint)
            .await?
            .phase
            == RepairPhase::Complete
    );
    let proof = source
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(
        proof.status().consumed_sequence == 3
            && proof.configuration().voters().contains(&MemberId::new(6)?)
            && !proof.configuration().voters().contains(&MemberId::new(4)?)
            && proof.configuration().outgoing_voters().is_empty()
    );
    admission(&f, &proposal.scope, 182, None).await?;
    current[2] = target_fresh;
    write_read(&f, &current, 4, 1, 22)
        .await
        .context("write after archive replacement")?;
    ensure!(source.status().await?.member_high_water == 6);
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .check_inventory(&root, std::slice::from_ref(&namespace), TIMEOUT)
            .await?
            .histories()
            == 1
    );
    stop(nodes).await?;
    stopped(target_node).await?;
    stopped(fresh_node).await?;
    drop((target_root, fresh_root, removed.root));
    f.domain.shutdown().await;
    Ok(())
}
