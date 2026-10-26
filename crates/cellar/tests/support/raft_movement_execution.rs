// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Composed remote execution. No application traffic drives membership progress.
use super::*;

async fn reserved(
    s: &Setup,
    cells: &[RaftCellHandle],
) -> Result<(usize, FleetMovementRequest, FleetPermitAuthorization)> {
    let (p, request) = movement(s, cells, 210).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let admitted =
        s.f.catalog
            .reserve_movement(
                &cut,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
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
    Ok((p, request, admitted.authorization()))
}

async fn execution(
    s: &Setup,
    movement: &FleetMovementRequest,
) -> Result<FleetMovementExecutionRequest> {
    let cut = s.f.catalog.read(TIMEOUT).await?;
    Ok(FleetMovementExecutionRequest {
        reservation: movement.resources.identity.id,
        attestation: s.request(0, 230, cut.revision()).await?.attestation,
        from_s3: true,
    })
}

async fn finish(
    s: &Setup,
    request: &FleetMovementExecutionRequest,
    observer: &RaftCells,
) -> Result<Vec<FleetMovementPhase>> {
    // Four distinct remote/configuration/settlement turns each retain the
    // existing five-second operation bound. The cohort has one overall bound.
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
            let mut phases = Vec::new();
            for _ in 0..64 {
                let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
                let cut = match catalog.read(TIMEOUT).await {
                    Ok(cut) => cut,
                    Err(
                        FleetCatalogError::Storage(_)
                        | FleetCatalogError::Unconfirmed
                        | FleetCatalogError::Overloaded,
                    ) => {
                        cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                match catalog
                    .reconcile_movement(&cut, request, observer, TIMEOUT)
                    .await
                {
                    Ok(progress) => {
                        ensure!(progress.reservation == request.reservation);
                        phases.push(progress.phase);
                        if progress.settled {
                            return Ok::<_, anyhow::Error>(phases);
                        }
                    }
                    Err(
                        FleetCatalogError::Overloaded
                        | FleetCatalogError::Unconfirmed
                        | FleetCatalogError::Conflict
                        | FleetCatalogError::Storage(_),
                    ) => (),
                    Err(error) => return Err(error.into()),
                }
                cellar::asyncrt::sleep(Duration::from_millis(20)).await;
            }
            anyhow::bail!("bounded movement turns exhausted: {phases:?}")
        }))
        .await??
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_executor_remote_s3_move_recovers_lost_settlement() -> Result<()> {
    // Keep composed fixture startup off the standard native harness stack.
    Box::pin(remote_s3_case()).await
}

async fn remote_s3_case() -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let (cells, _) = running(&s).await?;
    let (p, movement, _) = reserved(&s, &cells).await?;
    let (_root, observer_node, observer) = s.f.peer_host(8).await?;
    s.f.state(NodeId::new([8; 16])?, FleetNodeState::Active, 240)
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
    let request = execution(&s, &movement).await?;
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    let pending = catalog.pending_movements(&cut, TIMEOUT).await?;
    ensure!(
        pending.len() == 1
            && pending[0].reservation == request.reservation
            && pending[0].group == movement.resources.group
            && pending[0].plan == movement.plan
    );
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let phases = finish(&s, &request, &observer).await?;
    ensure!(
        phases.contains(&FleetMovementPhase::Reserved)
            && phases.contains(&FleetMovementPhase::Seeding)
            && phases.contains(&FleetMovementPhase::Joint)
            && phases.last() == Some(&FleetMovementPhase::Complete)
    );
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    ensure!(observer.observe_node(TIMEOUT).await?.node.groups == 0);
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 1);
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog.pending_movements(&cut, TIMEOUT).await?.is_empty());
    let other = s.request(1, 231, cut.revision()).await?;
    catalog
        .reserve_resources(
            &cut,
            &other,
            &observer,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    let next = catalog.read(TIMEOUT).await?;
    joined(s.target_node).await?;
    let retry = catalog
        .reconcile_movement(&next, &request, &observer, TIMEOUT)
        .await?;
    ensure!(retry.settled && retry.phase == FleetMovementPhase::Complete);
    ensure!(catalog.read(TIMEOUT).await?.revision() == next.revision());
    ensure!(catalog
        .get(
            &next,
            FleetTable::Operations,
            &format!("resource/{}", hex::encode(other.identity.id)),
            TIMEOUT
        )
        .await?
        .is_some());
    write_read(
        &s.f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone()],
        2,
        5,
        12,
    )
    .await?;
    joined(observer_node).await?;
    for node in s.nodes {
        joined(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_executor_duplicate_turns_consume_one_permit() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (p, movement, _) = reserved(&s, &cells).await?;
    let request = execution(&s, &movement).await?;
    let a = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let b = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let ac = a.read(TIMEOUT).await?;
    let bc = b.read(TIMEOUT).await?;
    let (first, second) = tokio::join!(
        a.reconcile_movement(&ac, &request, &s.target, TIMEOUT),
        b.reconcile_movement(&bc, &request, &s.target, TIMEOUT),
    );
    ensure!(
        first.is_ok() || second.is_ok(),
        "duplicate attempts: {first:?}, {second:?}"
    );
    let status = cells[p].fleet_membership_status().await?.unwrap();
    ensure!(
        status.consumed_sequence == 1
            && matches!(
                status.phase,
                Some(FleetPermitPhase::Seeding | FleetPermitPhase::Joint)
            )
    );
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == ac.revision());
    ensure!(s.f.catalog.pending_movements(&cut, TIMEOUT).await?.len() == 1);
    let phases = finish(&s, &request, &s.target).await?;
    ensure!(phases.last() == Some(&FleetMovementPhase::Complete));
    let target = s.target.cell(
        movement.resources.group.namespace,
        &movement.resources.group.cell,
    )?;
    write_read(
        &s.f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone(), target],
        2,
        5,
        12,
    )
    .await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_executor_catalog_outage_retains_intent() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (_, movement, _) = reserved(&s, &cells).await?;
    let request = execution(&s, &movement).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.store.fleet_read_hang.store(true, Ordering::Release);
    s.f.store.hold_fleet_reads.store(true, Ordering::Release);
    let result =
        s.f.catalog
            .reconcile_movement(&cut, &request, &s.target, Duration::from_millis(100))
            .await;
    s.f.store.hold_fleet_reads.store(false, Ordering::Release);
    s.f.store.fleet_read_hang.store(false, Ordering::Release);
    s.f.store.fleet_read_resume.notify_waiters();
    ensure!(result.is_err());
    let current = s.f.catalog.read(TIMEOUT).await?;
    ensure!(current.revision() == cut.revision());
    ensure!(
        s.f.catalog
            .pending_movements(&current, TIMEOUT)
            .await?
            .len()
            == 1
    );
    for cell in &cells {
        ensure!(
            cell.fleet_membership_status()
                .await?
                .unwrap()
                .consumed_sequence
                == 0
        );
    }
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    write_read(&s.f, &cells, 2, 5, 12).await?;
    s.close().await
}
