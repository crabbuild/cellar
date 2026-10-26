// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Committed cancellation, physical absence and exact terminal reconciliation.
use super::*;

async fn reserve(
    s: &Setup,
    cells: &[RaftCellHandle],
) -> Result<(usize, FleetMovementRequest, FleetPermitAuthorization)> {
    let (p, request) = movement(s, cells, 210).await?;
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
    Ok((p, request, accepted.authorization()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_fenced_movement_settles_and_reconciles_lost_reply() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, authorization) = running(&s).await?;
    let (p, movement, permit) = reserve(&s, &cells).await?;
    let target = s
        .target
        .register_managed_history(authorization, TIMEOUT)
        .await?;
    let (cut, request) = completion(&s, &movement, 230).await?;
    ensure!(s
        .f
        .catalog
        .settle_fenced_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let fenced = cells[p].fence_fleet_permit(permit.clone(), TIMEOUT).await?;
    ensure!(fenced.phase == Some(FleetPermitPhase::Fenced) && fenced.consumed_sequence == 1);
    ensure!(matches!(
        cells[p]
            .begin_managed_repair(permit.clone(), movement.plan.clone(), TIMEOUT)
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(target
        .activate_managed_learner(
            permit,
            LearnerActivation {
                member: movement.plan.replacement.member,
                source: MemberId::new(p as u64 + 1)?,
                source_node: s.nodes[p].spec.node,
                from_s3: true,
                timeout: TIMEOUT,
            }
        )
        .await
        .is_err());
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .settle_fenced_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    let current = s.f.catalog.read(TIMEOUT).await?;
    ensure!(current.revision() == cut.revision() + 1);
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let current = catalog.read(TIMEOUT).await?;
    let receipt = catalog
        .settle_fenced_movement(&current, &request, &s.nodes[p].host, TIMEOUT)
        .await?;
    ensure!(
        receipt.reservation() == movement.resources.identity.id
            && receipt.applied_revision() == current.revision()
            && receipt.observed_revision() == current.revision()
    );
    ensure!(matches!(
        catalog
            .complete_movement(&current, &request, &s.target, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let mut changed = request.clone();
    changed.attestation.used.memory_bytes += 1;
    ensure!(matches!(
        catalog
            .settle_fenced_movement(&current, &changed, &s.target, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
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
    let other = s.request(1, 231, current.revision()).await?;
    catalog
        .reserve_resources(
            &current,
            &other,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    let next = catalog.read(TIMEOUT).await?;
    ensure!(next.revision() == current.revision() + 1);
    // Historical retry cannot remove a subsequent reservation, and does not
    // require the old destination process to remain reachable.
    joined(s.target_node).await?;
    let retry = catalog
        .settle_fenced_movement(&next, &request, &s.nodes[p].host, TIMEOUT)
        .await?;
    ensure!(
        retry.applied_revision() == current.revision()
            && retry.observed_revision() == next.revision()
    );
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
    ensure!(catalog
        .get(
            &next,
            FleetTable::Operations,
            &format!("resource/{}", hex::encode(movement.resources.identity.id)),
            TIMEOUT
        )
        .await?
        .is_some());
    ensure!(catalog
        .get(
            &next,
            FleetTable::Operations,
            &format!("resource-floor/{}", hex::encode([7; 16])),
            TIMEOUT
        )
        .await?
        .is_some());
    write_read(&s.f, &cells, 2, 5, 12).await?;
    for node in s.nodes {
        joined(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_fenced_movement_refuses_seeding_and_aborted_work() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (p, movement, permit) = reserve(&s, &cells).await?;
    cells[p]
        .begin_managed_repair(permit.clone(), movement.plan.clone(), TIMEOUT)
        .await?;
    let (cut, request) = completion(&s, &movement, 230).await?;
    ensure!(s
        .f
        .catalog
        .settle_fenced_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(
        step(&s.f, &cells[p], &permit, RepairStep::AbortLearner)
            .await?
            .phase
            == RepairPhase::Aborted
    );
    ensure!(s
        .f
        .catalog
        .settle_fenced_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let other = s.request(1, 231, cut.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &other,
                &s.target,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Resource(_))
    ));
    write_read(&s.f, &cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_fenced_movement_unreachable_owner_retains_debt() -> Result<()> {
    let mut s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (p, movement, permit) = reserve(&s, &cells).await?;
    cells[p].fence_fleet_permit(permit, TIMEOUT).await?;
    let (cut, request) = completion(&s, &movement, 230).await?;
    let failed = s.nodes.remove((p + 1) % 3);
    joined(failed.node).await?;
    ensure!(s
        .f
        .catalog
        .settle_fenced_movement(&cut, &request, &s.target, Duration::from_millis(500))
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let other = s.request(1, 231, cut.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &other,
                &s.target,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Resource(_))
    ));
    write_read(
        &s.f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone()],
        2,
        5,
        12,
    )
    .await?;
    s.close().await
}
