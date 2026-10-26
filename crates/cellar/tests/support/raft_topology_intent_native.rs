// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent intent -> same-CAS indexed admission -> owned execution -> receipt.
use super::*;

#[path = "raft_topology_service_native.rs"]
mod service_cases;

fn identity(id: u8, operation: u8, revision: u64) -> FleetRequestIdentity {
    let mut request = Fixture::request(id, revision);
    request.operation = [operation; 16];
    request
}

async fn desired(
    f: &Scenario,
    operation: u8,
    exclude: Option<NodeId>,
) -> Result<FleetTopologyIntentRequest> {
    let request = f.request().await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    Ok(FleetTopologyIntentRequest {
        identity: identity(160, operation, cut.revision()),
        keep: request
            .keep
            .into_iter()
            .filter(|n| Some(*n) != exclude)
            .collect(),
        policy: request.policy,
    })
}

async fn preview(
    f: &Scenario,
    intent: &FleetTopologyIntentRequest,
) -> Result<FleetTopologyPreview> {
    discover(&f.s.f, &f.hosts()).await?;
    let mut request = f.request().await?;
    request.keep = intent.keep.clone();
    request.policy = intent.policy.clone();
    let mut walk = f.collection(&request).await?;
    while walk.verify_next(TIMEOUT).await? {}
    walk.preview(TIMEOUT).await.map_err(Into::into)
}

fn destination(f: &Scenario, movement: &FleetMovementRequest) -> Result<RaftCells> {
    match movement.plan.replacement.node.bytes()[0] {
        7 => Ok(f.s.target.clone()),
        8 => Ok(f.spare.clone()),
        _ => anyhow::bail!("unexpected generated target"),
    }
}

async fn apply_move(
    f: &mut Scenario,
    movement: &FleetMovementRequest,
    target: &RaftCells,
) -> Result<()> {
    settle_with_service(f, movement, target).await?;
    let index =
        f.s.proposals
            .iter()
            .position(|p| p.scope == movement.resources.group)
            .unwrap();
    let source = f.live[index]
        .iter_mut()
        .find(|(n, _)| *n == movement.source)
        .unwrap();
    *source = (
        movement.plan.replacement.node,
        target.cell(
            movement.resources.group.namespace,
            &movement.resources.group.cell,
        )?,
    );
    write_read(
        &f.s.f,
        &f.live[index]
            .iter()
            .map(|(_, h)| h.clone())
            .collect::<Vec<_>>(),
        2,
        5,
        12,
    )
    .await
}

async fn protected(f: &Scenario, operation: u8) -> Result<()> {
    for (i, (table, key)) in [
        (FleetTable::Policy, "topology-placement".into()),
        (FleetTable::Policy, "active-topology".into()),
        (
            FleetTable::Operations,
            format!("topology/{}", hex::encode([operation; 16])),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let cut = f.s.f.catalog.read(TIMEOUT).await?;
        for value in [None, Some(vec![1])] {
            let attempted =
                f.s.f
                    .catalog
                    .commit(
                        &cut,
                        &FleetMutation {
                            id: [240 + i as u8; 16],
                            operation: [operation; 16],
                            expected_revision: cut.revision(),
                            principal: "fixture-topology-tamper".into(),
                            changes: vec![FleetChange {
                                table,
                                key: key.clone(),
                                value,
                            }],
                        },
                        TIMEOUT,
                    )
                    .await;
            ensure!(matches!(
                attempted,
                Err(FleetCatalogError::Invalid("topology mutation authority"))
            ));
        }
        ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_intent_executes_indexed_moves_and_completes() -> Result<()> {
    Box::pin(converge()).await
}

async fn converge() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let intent = desired(&f, 191, None).await?;
    let other = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let other_cut = other.read(TIMEOUT).await?;
    let (a, b) = tokio::join!(
        f.s.f
            .catalog
            .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT),
        other.begin_topology(
            &other_cut,
            &intent,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT
        ),
    );
    ensure!(a.is_ok() || b.is_ok());
    let now = other.read(TIMEOUT).await?;
    let retry = other.begin_topology(&now, &intent, &[], TIMEOUT).await?;
    ensure!(retry.phase == FleetTopologyPhase::Running && retry.admitted_movements == 0);
    ensure!(now.revision() == cut.revision() + 1);
    protected(&f, 191).await?;
    let first = preview(&f, &intent).await?;
    ensure!(!first.plan().moves.is_empty());
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .complete_topology(&cut, &identity(161, 191, cut.revision()), &first, TIMEOUT)
        .await
        .is_err());
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    for round in 0..2 {
        let planned = preview(&f, &intent).await?;
        let request = identity(164 + round * 2, 191, planned.inventory_revision());
        let movement = planned.first_movement(request.clone())?.unwrap();
        let cut = f.s.f.catalog.read(TIMEOUT).await?;
        ensure!(matches!(
            f.s.f
                .catalog
                .reserve_movement(
                    &cut,
                    &movement,
                    &f.s.nodes[0].host,
                    std::slice::from_ref(&f.s.namespace),
                    TIMEOUT
                )
                .await,
            Err(FleetCatalogError::DisruptionInProgress { .. })
        ));
        let admitted =
            f.s.f
                .catalog
                .reserve_topology_movement(
                    &planned,
                    request,
                    &f.s.nodes[0].host,
                    std::slice::from_ref(&f.s.namespace),
                    TIMEOUT,
                )
                .await?
                .unwrap();
        let now = other.read(TIMEOUT).await?;
        let retry = other
            .reserve_movement(&now, &movement, &f.s.nodes[0].host, &[], TIMEOUT)
            .await?;
        ensure!(retry.resources().applied_revision() == admitted.resources().applied_revision());
        let status = other
            .topology_operation(&now, ProposalId::new([191; 16])?, TIMEOUT)
            .await?;
        ensure!(status.admitted_movements == u64::from(round) + 1);
        let page = other
            .topology_movements(&now, ProposalId::new([191; 16])?, None, 128, TIMEOUT)
            .await?;
        ensure!(page.entries.len() == round as usize + 1);
        ensure!(page
            .entries
            .iter()
            .any(|e| e.reservation == movement.resources.identity.id));
        let target = destination(&f, &movement)?;
        apply_move(&mut f, &movement, &target).await?;
    }
    let planned = preview(&f, &intent).await?;
    ensure!(planned.plan().moves.is_empty());
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let complete = identity(172, 191, cut.revision());
    let done =
        f.s.f
            .catalog
            .complete_topology(&cut, &complete, &planned, TIMEOUT)
            .await?;
    ensure!(done.phase == FleetTopologyPhase::Complete && done.admitted_movements == 2);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .active_topology_operation(&cut, TIMEOUT)
        .await?
        .is_none());
    let page =
        f.s.f
            .catalog
            .topology_movements(&cut, ProposalId::new([191; 16])?, None, 1, TIMEOUT)
            .await?;
    ensure!(page.entries.len() == 1 && page.next.is_some());
    let rest =
        f.s.f
            .catalog
            .topology_movements(
                &cut,
                ProposalId::new([191; 16])?,
                page.next.as_ref(),
                1,
                TIMEOUT,
            )
            .await?;
    ensure!(rest.entries.len() == 1 && rest.next.is_some());
    ensure!(rest.entries[0].reservation != page.entries[0].reservation);
    let exhausted =
        f.s.f
            .catalog
            .topology_movements(
                &cut,
                ProposalId::new([191; 16])?,
                rest.next.as_ref(),
                1,
                TIMEOUT,
            )
            .await?;
    ensure!(exhausted.entries.is_empty() && exhausted.next.is_none());
    let previous_policy =
        f.s.f
            .catalog
            .get(&cut, FleetTable::Policy, "topology-placement", TIMEOUT)
            .await?;
    let mut next = intent.clone();
    next.identity = identity(174, 192, cut.revision());
    f.s.f
        .catalog
        .begin_topology(&cut, &next, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let retry =
        f.s.f
            .catalog
            .complete_topology(&cut, &complete, &first, TIMEOUT)
            .await?;
    ensure!(retry.phase == FleetTopologyPhase::Complete);
    ensure!(
        f.s.f
            .catalog
            .active_topology_operation(&cut, TIMEOUT)
            .await?
            .unwrap()
            .identity
            .operation
            == [192; 16]
    );
    let cancelling =
        f.s.f
            .catalog
            .cancel_topology(&cut, &identity(176, 192, cut.revision()), TIMEOUT)
            .await?;
    ensure!(cancelling.phase == FleetTopologyPhase::Cancelling);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .complete_topology_cancellation(&cut, &identity(178, 192, cut.revision()), TIMEOUT)
        .await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.s.f
            .catalog
            .get(&cut, FleetTable::Policy, "topology-placement", TIMEOUT)
            .await?
            == previous_policy
    );
    for live in &f.live {
        write_read(
            &f.s.f,
            &live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>(),
            3,
            9,
            21,
        )
        .await?;
    }
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_intent_cancel_settles_admitted_work_and_restores_policy() -> Result<()> {
    Box::pin(cancel()).await
}

async fn cancel() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let excluded = f.s.nodes[2].spec.node;
    let intent = desired(&f, 193, Some(excluded)).await?;
    let before = f.request().await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let after = f.request().await?;
    for (a, b) in before.nodes.iter().zip(&after.nodes) {
        ensure!(a.attestation.boot == b.attestation.boot);
    }
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let mut birth = f.s.proposals[0].clone();
    birth.scope.cell = "Counter:excluded-birth".into();
    birth.scope.group = GroupId::new([199; 16])?;
    ensure!(matches!(
        f.s.f
            .catalog
            .reserve_birth(
                &cut,
                &identity(162, 194, cut.revision()),
                &f.s.namespace,
                birth.clone(),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid("excluded birth target"))
    ));
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let planned = preview(&f, &intent).await?;
    let request = identity(164, 193, planned.inventory_revision());
    let movement = planned.first_movement(request.clone())?.unwrap();
    let target = destination(&f, &movement)?;
    f.s.f
        .catalog
        .reserve_topology_movement(
            &planned,
            request,
            &f.s.nodes[0].host,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await?
        .unwrap();
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let cancellation = identity(166, 193, cut.revision());
    f.s.f
        .catalog
        .cancel_topology(&cut, &cancellation, TIMEOUT)
        .await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.s.f
            .catalog
            .complete_topology_cancellation(&cut, &identity(168, 193, cut.revision()), TIMEOUT)
            .await,
        Err(FleetCatalogError::Resource(LedgerError::MovementLimit))
    ));
    let other = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let other_cut = other.read(TIMEOUT).await?;
    other
        .reserve_movement(&other_cut, &movement, &f.s.nodes[0].host, &[], TIMEOUT)
        .await?;
    apply_move(&mut f, &movement, &target).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let completion = identity(170, 193, cut.revision());
    let done =
        f.s.f
            .catalog
            .complete_topology_cancellation(&cut, &completion, TIMEOUT)
            .await?;
    ensure!(done.phase == FleetTopologyPhase::Cancelled && done.admitted_movements == 1);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let history =
        f.s.f
            .catalog
            .begin_topology(&cut, &intent, &[], TIMEOUT)
            .await?;
    ensure!(history.phase == FleetTopologyPhase::Cancelled && history.admitted_movements == 1);
    ensure!(f
        .s
        .f
        .catalog
        .get(&cut, FleetTable::Policy, "topology-placement", TIMEOUT)
        .await?
        .is_none());
    let pending =
        f.s.f
            .catalog
            .reserve_birth(
                &cut,
                &identity(172, 194, cut.revision()),
                &f.s.namespace,
                birth,
                TIMEOUT,
            )
            .await?;
    f.s.f
        .domain
        .run(
            f.s.namespace
                .resolve_managed_birth(&pending.authorization, TIMEOUT),
        )
        .await??;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .confirm_birth(
            &cut,
            &identity(174, 194, cut.revision()),
            &f.s.namespace,
            &pending.authorization,
            TIMEOUT,
        )
        .await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.s.f
            .catalog
            .cancel_topology(&cut, &cancellation, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelled
    );
    ensure!(
        f.s.f
            .catalog
            .complete_topology_cancellation(&cut, &completion, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelled
    );
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_intent_fence_races_birth_and_preserves_prior_admission() -> Result<()> {
    Box::pin(birth_race()).await
}

async fn birth_race() -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    let mut prior = f.s.proposals[0].clone();
    prior.scope.cell = "Counter:prior-topology-birth".into();
    prior.scope.group = GroupId::new([196; 16])?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let prior =
        f.s.f
            .catalog
            .reserve_birth(
                &cut,
                &identity(154, 197, cut.revision()),
                &f.s.namespace,
                prior,
                TIMEOUT,
            )
            .await?;
    let mut delayed = f.s.proposals[0].clone();
    delayed.scope.cell = "Counter:delayed-topology-birth".into();
    delayed.scope.group = GroupId::new([197; 16])?;
    let delayed_scope = delayed.scope.clone();
    let intent = desired(&f, 195, Some(f.s.nodes[2].spec.node)).await?;
    let writer = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let winner = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let old_cut = writer.read(TIMEOUT).await?;
    let winner_cut = winner.read(TIMEOUT).await?;
    let namespace = f.s.namespace.clone();
    let old_request = identity(156, 197, old_cut.revision());
    f.s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    f.s.f
        .store
        .hold_next_fleet_cas
        .store(true, Ordering::Release);
    let pending = f.s.f.domain.spawn(async move {
        writer
            .reserve_birth(&old_cut, &old_request, &namespace, delayed, TIMEOUT)
            .await
    })?;
    f.s.waiting().await?;
    let result = winner
        .begin_topology(
            &winner_cut,
            &intent,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await;
    f.s.release();
    let delayed = pending.await?;
    ensure!(result?.phase == FleetTopologyPhase::Running);
    ensure!(matches!(delayed, Err(FleetCatalogError::Conflict)));
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.s.f.catalog.history(&cut, &delayed_scope, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    // The earlier birth reference remains authoritative even though one of its
    // original voters is now outside the desired keep-set.
    f.s.f
        .domain
        .run(
            f.s.namespace
                .resolve_managed_birth(&prior.authorization, TIMEOUT),
        )
        .await??;
    f.s.f
        .catalog
        .confirm_birth(
            &cut,
            &identity(162, 197, cut.revision()),
            &f.s.namespace,
            &prior.authorization,
            TIMEOUT,
        )
        .await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let authorization =
        f.s.f
            .catalog
            .authorize_group(&cut, prior.authorization.scope(), TIMEOUT)
            .await?;
    let cells = registered(&f.s.nodes, &authorization).await?;
    activate(&cells, &authorization).await?;
    write_read(&f.s.f, &cells, 1, 7, 7).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let refused =
        f.s.f
            .catalog
            .begin_evacuation(
                &cut,
                &identity(164, 198, cut.revision()),
                f.s.nodes[2].spec.node,
                std::slice::from_ref(&f.s.namespace),
                TIMEOUT,
            )
            .await;
    ensure!(
        matches!(refused, Err(FleetCatalogError::DisruptionInProgress { operation }) if operation == [195; 16])
    );
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    f.s.f
        .catalog
        .cancel_topology(&cut, &identity(166, 195, cut.revision()), TIMEOUT)
        .await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .complete_topology_cancellation(&cut, &identity(168, 195, cut.revision()), TIMEOUT)
        .await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .begin_evacuation(
            &cut,
            &identity(170, 199, cut.revision()),
            f.s.nodes[2].spec.node,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await?;
    let mut next = intent.clone();
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    next.identity = identity(172, 196, cut.revision());
    ensure!(matches!(
        f.s.f
            .catalog
            .begin_topology(&cut, &next, std::slice::from_ref(&f.s.namespace), TIMEOUT)
            .await,
        Err(FleetCatalogError::Invalid("active node disruption"))
    ));
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    write_read(&f.s.f, &cells, 2, 5, 12).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_intent_lost_replies_reconcile_after_later_operation() -> Result<()> {
    Box::pin(lost_replies()).await
}

async fn lost_replies() -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    let intent = desired(&f, 187, None).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .s
        .f
        .catalog
        .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await
        .is_err());
    let reopened = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let cut = reopened.read(TIMEOUT).await?;
    ensure!(
        reopened
            .begin_topology(&cut, &intent, &[], TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Running
    );
    let mut changed = intent.clone();
    changed.keep.pop();
    ensure!(matches!(
        reopened.begin_topology(&cut, &changed, &[], TIMEOUT).await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let cancellation = identity(162, 187, cut.revision());
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(reopened
        .cancel_topology(&cut, &cancellation, TIMEOUT)
        .await
        .is_err());
    let cut = reopened.read(TIMEOUT).await?;
    ensure!(
        reopened
            .cancel_topology(&cut, &cancellation, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelling
    );
    let completion = identity(164, 187, cut.revision());
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(reopened
        .complete_topology_cancellation(&cut, &completion, TIMEOUT)
        .await
        .is_err());
    let cut = reopened.read(TIMEOUT).await?;
    ensure!(
        reopened
            .complete_topology_cancellation(&cut, &completion, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelled
    );
    let mut next = intent.clone();
    next.identity = identity(166, 188, cut.revision());
    reopened
        .begin_topology(&cut, &next, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let cut = reopened.read(TIMEOUT).await?;
    let revision = cut.revision();
    ensure!(
        reopened
            .begin_topology(&cut, &intent, &[], TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelled
    );
    ensure!(
        reopened
            .cancel_topology(&cut, &cancellation, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelled
    );
    ensure!(
        reopened
            .complete_topology_cancellation(&cut, &completion, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelled
    );
    ensure!(
        reopened
            .active_topology_operation(&cut, TIMEOUT)
            .await?
            .unwrap()
            .identity
            .operation
            == [188; 16]
    );
    ensure!(reopened.read(TIMEOUT).await?.revision() == revision);
    for live in &f.live {
        write_read(
            &f.s.f,
            &live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>(),
            2,
            5,
            12,
        )
        .await?;
    }
    f.close().await
}
