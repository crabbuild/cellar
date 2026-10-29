// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Previously admitted work remains executable through restart preparation.
use super::*;
use cellar::application::consensus::verification::FleetRestartRequest;

use cellar::application::consensus::verification::{
    FleetRestartBirthResolution, FleetRestartGateRequest, FleetRestartSettlementBlocker,
    FleetRestartSettlementProgress,
};

async fn gate(s: &Setup, intent: &FleetRestartRequest) -> Result<ProposalId> {
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = Fixture::request(246, root.revision());
    identity.operation = intent.identity.operation;
    s.f.catalog
        .admit_restart_gate(
            &root,
            &FleetRestartGateRequest {
                identity,
                node: intent.node,
                expected_process: intent.expected_process,
                gate: ProposalId::new([248; 16])?,
            },
            TIMEOUT,
        )
        .await?;
    Ok(ProposalId::new(intent.identity.operation)?)
}

struct Evidence {
    request: FleetMovementExecutionRequest,
    target: RaftCells,
    wrong: bool,
}

impl FleetMovementEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<FleetMovementExecutionRequest, FleetCatalogError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let view = self
                .target
                .observe_node(timeout)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            let mut request = self.request.clone();
            request.reservation = if self.wrong {
                [250; 16]
            } else {
                movement.reservation
            };
            request.attestation.boot = view.fleet_boot.ok_or(FleetCatalogError::Unconfirmed)?;
            request.attestation.used.engine_slots = view.node.groups;
            Ok(request)
        })
    }
}

async fn prepare(s: &Setup, node: NodeId) -> Result<FleetRestartRequest> {
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let host = if node == NodeId::new([7; 16])? {
        &s.target
    } else {
        &s.nodes
            .iter()
            .find(|running| running.spec.node == node)
            .context("restart host")?
            .host
    };
    let boot = host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("restart boot")?;
    let intent = FleetRestartRequest {
        identity: Fixture::request(245, cut.revision()),
        node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    s.f.catalog
        .begin_restart_maintenance(&cut, &intent, std::slice::from_ref(&s.namespace), TIMEOUT)
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
    Ok(intent)
}

async fn restart_move(target: bool) -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let (cells, group) = running(&s).await?;
    let (p, movement, permit) = reserved(&s, &cells).await?;
    if !target {
        let learner = s.target.register_managed_history(group, TIMEOUT).await?;
        cells[p]
            .begin_managed_repair(permit.clone(), movement.plan.clone(), TIMEOUT)
            .await?;
        learner
            .activate_managed_learner(
                permit.clone(),
                LearnerActivation {
                    member: movement.plan.replacement.member,
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
    }
    // Peer observation requires a different process. Keep the observer outside
    // this group's electorate, including after leadership changes.
    let (_observer_root, observer_node, observer) = s.f.peer_host(8).await?;
    s.f.state(NodeId::new([8; 16])?, FleetNodeState::Active, 244)
        .await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.target,
            &observer,
        ],
    )
    .await?;
    let execution = execution(&s, &movement).await?;
    let node = if target {
        movement.plan.replacement.node
    } else {
        s.nodes[p].spec.node
    };
    let intent = prepare(&s, node).await?;
    discover(&s.f, &[&observer]).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let host = s.f.catalog.authorize_host(&cut, node, TIMEOUT).await?;
    ensure!(host.state() == FleetNodeState::RestartPending);
    ensure!(!host.eligibility().receive_replicas && !host.eligibility().receive_leadership);
    let pending =
        s.f.catalog
            .pending_restart_work(
                &cut,
                ProposalId::new(intent.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(pending.pending_births == 0 && pending.next_birth.is_none());
    ensure!(
        pending.movements.len() == 1 && pending.movements[0].reservation == execution.reservation
    );
    ensure!(pending.reservations.len() == 1 && pending.reservations[0].executable_movement);
    if target {
        let unrelated =
            s.f.catalog
                .authorize_group(&cut, &s.proposals[1].scope, TIMEOUT)
                .await?;
        ensure!(matches!(
            s.target.register_managed_history(unrelated, TIMEOUT).await,
            Err(CommandFailure::CommandConflict)
        ));
        let next = s.request(1, 247, cut.revision()).await?;
        ensure!(matches!(
            s.f.catalog
                .reserve_resources(
                    &cut,
                    &next,
                    &observer,
                    std::slice::from_ref(&s.namespace),
                    TIMEOUT
                )
                .await,
            Err(FleetCatalogError::Conflict)
        ));
    }
    let phases = finish(&s, &execution, &observer).await?;
    ensure!(phases.last() == Some(&FleetMovementPhase::Complete));
    ensure!(phases.contains(&if target {
        FleetMovementPhase::Reserved
    } else {
        FleetMovementPhase::Joint
    }));
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(s
        .f
        .catalog
        .pending_reservations(&cut, TIMEOUT)
        .await?
        .is_empty());
    let pending =
        s.f.catalog
            .pending_restart_work(
                &cut,
                ProposalId::new(intent.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(
        pending.observed_revision == cut.revision()
            && pending.movements.is_empty()
            && pending.reservations.is_empty()
    );
    ensure!(
        s.f.catalog
            .restart_maintenance_admission(
                &cut,
                ProposalId::new(intent.identity.operation)?,
                TIMEOUT
            )
            .await?
            .boot()?
            .node
            == node.bytes()
    );
    let replacement = s.target.cell(
        movement.resources.group.namespace,
        &movement.resources.group.cell,
    )?;
    write_read(
        &s.f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone(), replacement],
        2,
        5,
        12,
    )
    .await?;
    joined(s.target_node).await?;
    joined(observer_node).await?;
    for running in s.nodes {
        joined(running.node).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settlement_finishes_an_admitted_learner() -> Result<()> {
    Box::pin(restart_move(true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settlement_finishes_joint_with_pending_leader() -> Result<()> {
    Box::pin(restart_move(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settlement_retains_resource_only_obligations() -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let resource = s.request(0, 210, cut.revision()).await?;
    s.f.catalog
        .reserve_resources(
            &cut,
            &resource,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await
        .context("reserve resource-only obligation")?;
    // The destination differs: an unresolved debit for this voter's history
    // still prevents treating an empty movement list as settled maintenance.
    let intent = prepare(&s, s.nodes[0].spec.node).await?;
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = reopened.read(TIMEOUT).await?;
    let pending = reopened
        .pending_restart_work(
            &cut,
            ProposalId::new(intent.identity.operation)?,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    ensure!(pending.movements.is_empty() && pending.reservations.len() == 1);
    ensure!(
        pending.reservations[0].reservation == resource.identity.id
            && !pending.reservations[0].executable_movement
            && pending.reservations[0].peak == resource.peak
    );
    ensure!(pending.reservations[0].target == NodeId::new([7; 16])?);
    ensure!(reopened.pending_reservations(&cut, TIMEOUT).await?.len() == 1);
    joined(s.target_node).await?;
    for running in s.nodes {
        joined(running.node).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settler_executes_admitted_movement_and_reports_missing_services(
) -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let (cells, _) = running(&s).await?;
    let (p, movement, _) = reserved(&s, &cells).await?;
    let request = execution(&s, &movement).await?;
    let (_observer_root, observer_node, observer) = s.f.peer_host(8).await?;
    s.f.state(NodeId::new([8; 16])?, FleetNodeState::Active, 244)
        .await?;
    let intent = prepare(&s, movement.plan.replacement.node).await?;
    let operation = gate(&s, &intent).await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.target,
            &observer,
        ],
    )
    .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        matches!(s.f.catalog.reconcile_restart_work(operation, FleetRestartBirthResolution::Cancel,std::slice::from_ref(&s.namespace),None,None,TIMEOUT).await?,FleetRestartSettlementProgress::Blocked(FleetRestartSettlementBlocker::MovementServicesUnavailable {reservation,..}) if reservation==request.reservation)
    );
    let wrong = Evidence {
        request: request.clone(),
        target: s.target.clone(),
        wrong: true,
    };
    ensure!(matches!(
        s.f.catalog
            .reconcile_restart_work(
                operation,
                FleetRestartBirthResolution::Cancel,
                std::slice::from_ref(&s.namespace),
                Some(&observer),
                Some(&wrong),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    let evidence = Evidence {
        request: request.clone(),
        target: s.target.clone(),
        wrong: false,
    };
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
            let mut phases = Vec::new();
            for _ in 0..64 {
                let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
                match catalog
                    .reconcile_restart_work(
                        operation,
                        FleetRestartBirthResolution::Cancel,
                        std::slice::from_ref(&s.namespace),
                        Some(&observer),
                        Some(&evidence),
                        TIMEOUT,
                    )
                    .await
                {
                    Ok(FleetRestartSettlementProgress::Movement(progress)) => {
                        phases.push(progress.phase);
                        if progress.settled {
                            ensure!(
                                phases.contains(&FleetMovementPhase::Reserved)
                                    && phases.contains(&FleetMovementPhase::Seeding)
                                    && phases.contains(&FleetMovementPhase::Joint)
                                    && progress.phase == FleetMovementPhase::Complete
                            );
                            return Ok::<_, anyhow::Error>(());
                        }
                    }
                    Err(
                        FleetCatalogError::Conflict
                        | FleetCatalogError::Unconfirmed
                        | FleetCatalogError::Overloaded,
                    ) => (),
                    other => anyhow::bail!("unexpected settlement result: {other:?}"),
                }
                cellar::asyncrt::sleep(Duration::from_millis(20)).await;
            }
            anyhow::bail!("settlement turns exhausted: {phases:?}")
        }))
        .await???;
    ensure!(matches!(
        s.f.catalog
            .reconcile_restart_work(
                operation,
                FleetRestartBirthResolution::Cancel,
                std::slice::from_ref(&s.namespace),
                Some(&observer),
                Some(&evidence),
                TIMEOUT
            )
            .await?,
        FleetRestartSettlementProgress::CatalogClear { .. }
    ));
    let replacement = s.target.cell(
        movement.resources.group.namespace,
        &movement.resources.group.cell,
    )?;
    write_read(
        &s.f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone(), replacement],
        2,
        5,
        12,
    )
    .await?;
    joined(s.target_node).await?;
    joined(observer_node).await?;
    for running in s.nodes {
        joined(running.node).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settler_reports_resource_only_blocker() -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let resource = s.request(0, 210, root.revision()).await?;
    s.f.catalog
        .reserve_resources(
            &root,
            &resource,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    let intent = prepare(&s, s.nodes[0].spec.node).await?;
    let operation = gate(&s, &intent).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        matches!(s.f.catalog.reconcile_restart_work(operation, FleetRestartBirthResolution::Cancel,std::slice::from_ref(&s.namespace),None,None,TIMEOUT).await?,FleetRestartSettlementProgress::Blocked(FleetRestartSettlementBlocker::ResourceOnly {reservation,..}) if reservation==resource.identity.id)
    );
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    let pending = s.f.catalog.pending_reservations(&root, TIMEOUT).await?;
    ensure!(pending.len() == 1 && pending[0].peak == resource.peak);
    joined(s.target_node).await?;
    for running in s.nodes {
        joined(running.node).await?;
    }
    Ok(())
}
