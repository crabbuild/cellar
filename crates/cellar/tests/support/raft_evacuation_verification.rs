// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Candidate verification owns actual quorum RPC and exact physical replay.
//! No candidate grants terminal removal, stop or disruption-release authority.
use super::*;
use cellar::application::consensus::verification::{
    FleetCatalogError, FleetEvacuationArchive, FleetEvacuationVerification,
};

#[path = "raft_finalization_fence.rs"]
mod finalization_fence;

async fn begin(s: &Started) -> Result<(NamespaceCatalog, ProposalId, FleetEvacuationVerification)> {
    s.f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 190)
        .await?;
    let (namespace, _) = initial_birth(&s.f).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let request = FleetRequestIdentity {
        id: [200; 16],
        operation: [201; 16],
        principal: "candidate-verification".into(),
        expected_revision: root.revision(),
    };
    let admitted =
        s.f.catalog
            .begin_evacuation(
                &root,
                &request,
                NodeId::new([7; 16])?,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    let operation = ProposalId::new(request.operation)?;
    let verification =
        s.f.catalog
            .prepare_evacuation_verification(
                &admitted.snapshot,
                operation,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.observer,
        ],
    )
    .await?;
    Ok((namespace, operation, verification))
}

fn archive<'a>(s: &'a Started, reference: &'a ArchiveReference) -> FleetEvacuationArchive<'a> {
    FleetEvacuationArchive {
        archive: &s.archive,
        reference,
        verifier: &s.verifier,
    }
}

fn survivors(s: &Started) -> [NodeId; 3] {
    [
        s.nodes[0].spec.node,
        s.nodes[1].spec.node,
        s.nodes[2].spec.node,
    ]
}

async fn refresh_trust(s: &Started) -> Result<()> {
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.observer,
        ],
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_candidate_complete_inventory() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    let (namespace, operation, mut verification) = begin(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        verification.publish_candidate(TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    let history = verification
        .next_history(TIMEOUT)
        .await?
        .context("pending history")?;
    ensure!(history.binding() == &s.f.catalog.group_binding(&s.proposal.scope)?);
    ensure!(verification.next_history(TIMEOUT).await?.unwrap().binding() == history.binding());
    ensure!(matches!(
        verification.verify_cancelled(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let cut = s.cut().await?;
    ensure!(cut.image().is_none());
    let reference = s.reference(&cut).await?;
    verification
        .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT)
        .await?;
    ensure!(verification.next_history(TIMEOUT).await?.is_none());
    let candidate = verification.publish_candidate(TIMEOUT).await?;
    let retry = verification.publish_candidate(TIMEOUT).await?;
    ensure!(candidate.digest() == retry.digest() && candidate.digest() != [0; 32]);
    ensure!(candidate.operation() == operation.bytes() && candidate.histories() == 1);
    ensure!(candidate.inventory_revision() == root.revision());
    // Candidate PUT and retries do not mutate root, release the durable slot,
    // certify zero physical occupancy, or stop the departing coordinator.
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    let observed = s.observer.observe_node(TIMEOUT).await?;
    ensure!(observed.current_enrollment.unwrap().state == FleetNodeState::Draining);
    let conflict = FleetRequestIdentity {
        id: [202; 16],
        operation: [203; 16],
        principal: "second-evacuation".into(),
        expected_revision: root.revision(),
    };
    ensure!(matches!(
        s.f.catalog
            .begin_evacuation(
                &root,
                &conflict,
                s.nodes[0].spec.node,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::DisruptionInProgress { .. })
    ));
    write_read(&s.f, &s.cells, 1, 9, 9).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_candidate_replay_failure_and_root_change() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    write_read(&s.f, &s.cells, 1, 13, 13).await?;
    let (namespace, operation, mut verification) = begin(&s).await?;
    let cut = s.cut().await?;
    let reference = s.reference(&cut).await?;
    s.f.store
        .refuse_archive_payload_reads
        .store(true, Ordering::Release);
    let failed = verification
        .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT)
        .await;
    s.f.store
        .refuse_archive_payload_reads
        .store(false, Ordering::Release);
    ensure!(
        matches!(failed, Err(FleetCatalogError::Storage(_))),
        "failed replay: {failed:?}"
    );
    ensure!(matches!(
        verification.publish_candidate(TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(verification.next_history(TIMEOUT).await?.is_some());
    verification
        .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT)
        .await?;
    let candidate = verification.publish_candidate(TIMEOUT).await?;
    ensure!(candidate.histories() == 1);
    // A later permit cannot be omitted merely because it has not reached Raft.
    let p = primary(&s.f, &s.cells).await?;
    reserve(
        &s.f,
        &s.cells[p],
        &s.proposal.scope,
        1,
        210,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: s.f.peer_spec(7)?.disk,
        },
    )
    .await?;
    ensure!(matches!(
        verification.publish_candidate(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let root = s.f.catalog.read(TIMEOUT).await?;
    refresh_trust(&s).await?;
    let mut renewed =
        s.f.catalog
            .prepare_evacuation_verification(
                &root,
                operation,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(matches!(
        renewed
            .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT,)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    // The old candidate remains historical material and is not terminal state.
    ensure!(candidate.histories() == 1);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_candidate_reserved_cancelled_and_missing_import() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    let (namespace, mut proposal) = initial_birth(&s.f).await?;
    proposal.scope.cell = "Counter:verification-pending".into();
    proposal.scope.group = GroupId::new([85; 16])?;
    proposal.scope.incarnation = IncarnationId::new([86; 16])?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let reservation =
        s.f.catalog
            .reserve_birth(
                &root,
                &Fixture::request(218, root.revision()),
                &namespace,
                proposal,
                TIMEOUT,
            )
            .await?;
    let (_, operation, mut verification) = begin(&s).await?;
    let cut = s.cut().await?;
    let reference = s.reference(&cut).await?;
    loop {
        let history = verification
            .next_history(TIMEOUT)
            .await?
            .context("reserved inventory")?;
        if *history.state() == FleetHistoryState::Reserved {
            ensure!(matches!(
                verification.verify_cancelled(TIMEOUT).await,
                Err(FleetCatalogError::Conflict)
            ));
            ensure!(matches!(
                verification
                    .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT,)
                    .await,
                Err(FleetCatalogError::Unconfirmed)
            ));
            break;
        }
        verification
            .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT)
            .await?;
    }
    ensure!(matches!(
        verification.publish_candidate(TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    s.f.domain
        .run(namespace.cancel_managed_birth(
            &reservation.authorization,
            ProposalId::new([220; 16])?,
            TIMEOUT,
        ))
        .await??;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .confirm_birth(
            &root,
            &Fixture::request(221, root.revision()),
            &namespace,
            &reservation.authorization,
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        verification.next_history(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let root = s.f.catalog.read(TIMEOUT).await?;
    refresh_trust(&s).await?;
    let mut verification =
        s.f.catalog
            .prepare_evacuation_verification(
                &root,
                operation,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    let mut cancelled = 0;
    while let Some(history) = verification.next_history(TIMEOUT).await? {
        match history.state() {
            FleetHistoryState::Live => {
                verification
                    .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT)
                    .await?
            }
            FleetHistoryState::Cancelled { .. } => {
                verification.verify_cancelled(TIMEOUT).await?;
                cancelled += 1;
            }
            _ => anyhow::bail!("reserved history unexpectedly survived reconciliation"),
        }
    }
    ensure!(cancelled == 1 && verification.publish_candidate(TIMEOUT).await?.histories() == 2);

    let mut config = namespace.config().clone();
    config.namespace = NamespaceId::new([87; 16])?;
    config.managed_fleet = None;
    let extra = Catalog::new(
        s.f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(config)
    .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .declare_namespace(
            &root,
            &Fixture::request(222, root.revision()),
            &extra,
            "Counter",
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        verification.publish_candidate(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let root = s.f.catalog.read(TIMEOUT).await?;
    for catalogs in [vec![namespace.clone()], vec![namespace.clone(), extra]] {
        ensure!(matches!(
            s.f.catalog
                .prepare_evacuation_verification(&root, operation, &catalogs, TIMEOUT,)
                .await,
            Err(FleetCatalogError::InventoryIncomplete)
        ));
    }
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_candidate_fixed_cut_under_writes() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    let (_, operation, mut verification) = begin(&s).await?;
    let fixed = verification
        .freeze_live(&s.observer, survivors(&s), TIMEOUT)
        .await?;
    ensure!(fixed.image().is_none());
    write_read(&s.f, &s.cells, 1, 17, 17).await?;
    write_read(&s.f, &s.cells, 2, 8, 25).await?;
    let repeated = verification
        .freeze_live(&s.observer, survivors(&s), TIMEOUT)
        .await?;
    ensure!(repeated.applied() == fixed.applied() && repeated.image().is_none());
    let later = s.cut().await?;
    ensure!(later.applied().index > fixed.applied().index && later.image().is_some());
    let reference = s.reference(&later).await?;
    verification
        .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT)
        .await?;
    let candidate = verification.publish_candidate(TIMEOUT).await?;
    ensure!(candidate.histories() == 1);
    write_read(&s.f, &s.cells, 3, 6, 31).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .commit(
            &root,
            &FleetMutation {
                id: [224; 16],
                operation: [225; 16],
                principal: "unrelated-progress".into(),
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Operations,
                    key: "unrelated-progress".into(),
                    value: Some(vec![1]),
                }],
            },
            TIMEOUT,
        )
        .await?;
    ensure!(verification.publish_candidate(TIMEOUT).await?.digest() == candidate.digest());
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .commit(
            &root,
            &FleetMutation {
                id: [226; 16],
                operation: [227; 16],
                principal: "corrupt-active-operation".into(),
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Operations,
                    key: hex::encode(operation.bytes()),
                    value: None,
                }],
            },
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        verification.publish_candidate(TIMEOUT).await,
        Err(FleetCatalogError::Corrupt(_))
    ));
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_evacuation_candidate_after_voter_replacement() -> Result<()> {
    use super::super::retirement::{step, stopped};
    let mut s = Box::pin(Started::new()).await?;
    write_read(&s.f, &s.cells, 1, 7, 7).await?;
    let p = primary(&s.f, &s.cells).await?;
    let obsolete = (p + 1) % 3;
    s.f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 190)
        .await?;
    refresh_trust(&s).await?;
    let target = s
        .observer
        .register_managed_history(s.authorization.clone(), TIMEOUT)
        .await?;
    let source = &s.cells[p];
    let (plan, permit) = reserve(
        &s.f,
        source,
        &s.proposal.scope,
        1,
        210,
        MemberId::new(obsolete as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: s.f.peer_spec(7)?.disk,
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
                source_node: s.nodes[p].spec.node,
                from_s3: true,
                timeout: TIMEOUT,
            },
        )
        .await?;
    ensure!(
        step(&s.f, source, &permit, RepairStep::PromoteAndRemove)
            .await?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&s.f, source, &permit, RepairStep::FinishJoint)
            .await?
            .phase
            == RepairPhase::Complete
    );
    let (namespace, _) = initial_birth(&s.f).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = FleetRequestIdentity {
        id: [200; 16],
        operation: [201; 16],
        principal: "evacuate-replaced-voter".into(),
        expected_revision: root.revision(),
    };
    let admitted =
        s.f.catalog
            .begin_evacuation(
                &root,
                &identity,
                s.nodes[obsolete].spec.node,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    refresh_trust(&s).await?;
    let mut verification =
        s.f.catalog
            .prepare_evacuation_verification(
                &admitted.snapshot,
                ProposalId::new(identity.operation)?,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    // The departing process is the observer; all three targets are survivors.
    // Its old physical history stays charged even after membership replacement.
    let observer = &s.nodes[obsolete].host;
    let peers = [
        s.nodes[(obsolete + 1) % 3].spec.node,
        s.nodes[(obsolete + 2) % 3].spec.node,
        NodeId::new([7; 16])?,
    ];
    let fixed = verification.freeze_live(observer, peers, TIMEOUT).await?;
    let reference = s.reference(&fixed).await?;
    verification
        .verify_live(observer, peers, archive(&s, &reference), TIMEOUT)
        .await?;
    let candidate = verification.publish_candidate(TIMEOUT).await?;
    ensure!(candidate.histories() == 1);
    let retained = observer.observe_node(TIMEOUT).await?;
    ensure!(retained.node.groups > 0 || retained.unattached_engine_histories > 0);
    let mut active = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != obsolete)
        .map(|(_, cell)| cell.clone())
        .collect::<Vec<_>>();
    active.push(target);
    let root = s.f.catalog.read(TIMEOUT).await?;
    let operation = ProposalId::new(identity.operation)?;
    let finalizing = candidate
        .begin_finalization(
            &root,
            &FleetRequestIdentity {
                id: [230; 16],
                operation: operation.bytes(),
                principal: "retired-voter-stop".into(),
                expected_revision: root.revision(),
            },
            &s.observer,
            std::slice::from_ref(&namespace),
            TIMEOUT,
        )
        .await?;
    let minimum = finalizing.applied_revision();
    s.f.remote(&s.observer, s.nodes[obsolete].spec.node, minimum)
        .await?;
    s.f.remote(&s.nodes[obsolete].host, NodeId::new([7; 16])?, minimum)
        .await?;
    let stop_id = ProposalId::new([238; 16])?;
    let stopping =
        s.observer
            .request_peer_fleet_stop(finalizing.stop_request(stop_id)?, minimum, TIMEOUT);
    let (stop_result, write_result) =
        futures_util::join!(stopping, write_read(&s.f, &active, 2, 5, 12));
    write_result?;
    ensure!(matches!(
        stop_result,
        Ok(_) | Err(FleetCatalogError::Unconfirmed)
    ));
    let mut departed = s.nodes.remove(obsolete);
    let outcome =
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, departed.node.finished()))
            .await???;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("retired voter did not finish its scoped stop");
    };
    let raft = report.raft.context("retired voter physical shutdown")?;
    ensure!(!raft.startup_incomplete);
    let closed = raft.result?;
    ensure!(closed.groups_total > 0 && closed.unfinished.is_empty());
    let root = s.f.catalog.read(TIMEOUT).await?;
    let closure =
        s.f.catalog
            .verify_physical_closure(&root, operation, stop_id, TIMEOUT)
            .await?;
    ensure!(
        closure.node() == departed.spec.node.bytes()
            && closure.process() == retained.node.identity.process.bytes()
    );
    write_read(&s.f, &active, 3, 6, 18).await?;
    // This fixture promoted the original empty observer into the replacement
    // voter, so require its actual occupied state and join that owner explicitly.
    ensure!(s.scratch.usage()?.generations == 0);
    ensure!(s.observer.observe_node(TIMEOUT).await?.node.groups == 1);
    stopped(s.observer_node).await?;
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}
