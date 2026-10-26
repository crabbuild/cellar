// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual resource-backed permits, source quorum, native seed and joint finish.
use super::super::retirement::{discover, step};
use super::*;

async fn running(s: &Setup) -> Result<(Vec<RaftCellHandle>, FleetGroupAuthorization)> {
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let authorization =
        s.f.catalog
            .authorize_group(&cut, &s.proposals[0].scope, TIMEOUT)
            .await?;
    let cells = registered(&s.nodes, &authorization).await?;
    activate(&cells, &authorization).await?;
    write_read(&s.f, &cells, 1, 7, 7).await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.target,
        ],
    )
    .await?;
    Ok((cells, authorization))
}

async fn movement(
    s: &Setup,
    cells: &[RaftCellHandle],
    id: u8,
) -> Result<(usize, FleetMovementRequest)> {
    let p = primary(&s.f, cells).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let resources = s.request(0, id, cut.revision()).await?;
    let plan = RepairPlan {
        id: ProposalId::new([id + 1; 16])?,
        policy_revision: 1,
        expected_revision: cells[p].status().await?.membership_revision,
        obsolete: MemberId::new(((p + 1) % 3) as u64 + 1)?,
        replacement: JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: s.f.peer_spec(7)?.disk,
        },
    };
    Ok((
        p,
        FleetMovementRequest {
            resources,
            source: s.nodes[p].spec.node,
            plan,
        },
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_atomic_permit_drives_native_replacement() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, authorization) = running(&s).await?;
    let (p, request) = movement(&s, &cells, 210).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let accepted =
        s.f.catalog
            .reserve_movement(
                &cut,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    let permit = accepted.authorization();
    let current = s.f.catalog.read(TIMEOUT).await?;
    ensure!(current.revision() == cut.revision() + 1);
    ensure!(accepted.resources().applied_revision() == current.revision());
    let key = s.f.catalog.group_binding(&request.resources.group)?.key()?;
    let rooted =
        s.f.catalog
            .authorize_membership(&current, &request.resources.group, TIMEOUT)
            .await?;
    // The capability is opaque; compare actual committed group status below.
    let target = s
        .target
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.target,
        ],
    )
    .await?;
    cells[p]
        .begin_managed_repair(permit.clone(), request.plan.clone(), TIMEOUT)
        .await?;
    target
        .activate_managed_learner(
            rooted,
            LearnerActivation {
                member: request.plan.replacement.member,
                source: MemberId::new(p as u64 + 1)?,
                source_node: s.nodes[p].spec.node,
                from_s3: true,
                timeout: TIMEOUT,
            },
        )
        .await?;
    ensure!(
        step(&s.f, &cells[p], &permit, RepairStep::PromoteAndRemove)
            .await?
            .phase
            == RepairPhase::Joint
    );
    ensure!(
        step(&s.f, &cells[p], &permit, RepairStep::FinishJoint)
            .await?
            .phase
            == RepairPhase::Complete
    );
    let state = cells[p]
        .observe_fleet_membership(authorization, TIMEOUT)
        .await?;
    ensure!(
        state.status().consumed_sequence == 1
            && state.status().phase == Some(FleetPermitPhase::Complete)
    );
    ensure!(state
        .status()
        .permit
        .as_ref()
        .is_some_and(|p| p.id() == request.resources.identity.id));
    write_read(
        &s.f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone(), target],
        2,
        5,
        12,
    )
    .await?;
    // Completion of Raft membership alone cannot erase the active resource debit.
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        s.f.catalog
            .commit(
                &root,
                &FleetMutation {
                    id: [220; 16],
                    operation: [220; 16],
                    principal: "forged-release".into(),
                    expected_revision: root.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Admissions,
                        key,
                        value: None
                    }],
                },
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid(
            "resource-linked permit replacement"
        ))
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_lost_reply_reconciles_exact_permit() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (_, request) = movement(&s, &cells, 210).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .reserve_movement(
            &cut,
            &request,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    let applied = cut.revision() + 1;
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == applied);
    joined(s.target_node).await?;
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = catalog.read(TIMEOUT).await?;
    let accepted = catalog
        .reserve_movement(&root, &request, &s.nodes[0].host, &[], TIMEOUT)
        .await?;
    ensure!(accepted.resources().applied_revision() == applied);
    let mut changed = request.clone();
    changed.plan.policy_revision += 1;
    ensure!(matches!(
        catalog
            .reserve_movement(&root, &changed, &s.nodes[0].host, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(catalog.read(TIMEOUT).await?.revision() == applied);
    for node in s.nodes {
        joined(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_minority_cannot_reserve_capacity_or_permit() -> Result<()> {
    let mut s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (_, mut request) = movement(&s, &cells, 210).await?;
    request.source = s.nodes[0].spec.node;
    joined(s.nodes.remove(2).node).await?;
    joined(s.nodes.remove(1).node).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let result =
        s.f.catalog
            .reserve_movement(
                &root,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                Duration::from_millis(500),
            )
            .await;
    ensure!(matches!(
        result,
        Err(FleetCatalogError::Observation(_) | FleetCatalogError::Unconfirmed)
    ));
    let current = s.f.catalog.read(TIMEOUT).await?;
    ensure!(current.revision() == root.revision());
    let key = s.f.catalog.group_binding(&request.resources.group)?.key()?;
    ensure!(s
        .f
        .catalog
        .get(&current, FleetTable::Admissions, &key, TIMEOUT)
        .await?
        .is_none());
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_concurrent_last_slot_is_atomic() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (first_cells, _) = running(&s).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let authorization =
        s.f.catalog
            .authorize_group(&cut, &s.proposals[1].scope, TIMEOUT)
            .await?;
    let second_cells = registered(&s.nodes, &authorization).await?;
    activate(&second_cells, &authorization).await?;
    write_read(&s.f, &second_cells, 1, 7, 7).await?;
    let (_, first) = movement(&s, &first_cells, 210).await?;
    let (p, mut second) = movement(&s, &second_cells, 212).await?;
    // Writer turns are independent of the hosts serving fresh control proofs.
    let writer = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let other = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = writer.read(TIMEOUT).await?;
    let other_cut = other.read(TIMEOUT).await?;
    ensure!(other_cut.revision() == cut.revision());
    second.resources = s.request(1, 212, cut.revision()).await?;
    second.plan.expected_revision = second_cells[p].status().await?.membership_revision;
    let first_key = s.f.catalog.group_binding(&first.resources.group)?.key()?;
    let second_key = s.f.catalog.group_binding(&second.resources.group)?.key()?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let (catalog, observer, namespace, first_cut) = (
        writer,
        s.nodes[0].host.clone(),
        s.namespace.clone(),
        cut.clone(),
    );
    let pending = s.f.domain.spawn(async move {
        catalog
            .reserve_movement(&first_cut, &first, &observer, &[namespace], TIMEOUT)
            .await
    })?;
    s.waiting().await?;
    let winner = other
        .reserve_movement(
            &other_cut,
            &second,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await;
    s.release();
    let loser = pending.await?;
    let winner = winner?;
    ensure!(matches!(loser, Err(FleetCatalogError::Conflict)));
    let current = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        current.revision() == cut.revision() + 1
            && winner.resources().applied_revision() == current.revision()
    );
    ensure!(s
        .f
        .catalog
        .get(&current, FleetTable::Admissions, &first_key, TIMEOUT)
        .await?
        .is_none());
    ensure!(s
        .f
        .catalog
        .get(&current, FleetTable::Admissions, &second_key, TIMEOUT)
        .await?
        .is_some());
    ensure!(s
        .f
        .catalog
        .get(
            &current,
            FleetTable::Operations,
            &format!("resource/{}", hex::encode([210; 16])),
            TIMEOUT
        )
        .await?
        .is_none());
    ensure!(s
        .f
        .catalog
        .get(
            &current,
            FleetTable::Operations,
            &format!("resource/{}", hex::encode([212; 16])),
            TIMEOUT
        )
        .await?
        .is_some());
    // Install the winning cut before requesting a fresh capacity decision.
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.target,
        ],
    )
    .await?;
    let mut retry = s.request(0, 214, current.revision()).await?;
    retry.identity.expected_revision = current.revision();
    let (_, mut request) = movement(&s, &first_cells, 214).await?;
    request.resources = retry;
    let refused =
        s.f.catalog
            .reserve_movement(
                &current,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await;
    ensure!(
        matches!(
            &refused,
            Err(FleetCatalogError::Resource(LedgerError::CapacityExceeded(
                ResourceKind::EngineSlots
            )))
        ),
        "last-slot refusal: {:?}",
        refused.as_ref().err()
    );
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == current.revision());
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    s.close().await
}

#[path = "raft_movement_completion.rs"]
mod completion_tests;
