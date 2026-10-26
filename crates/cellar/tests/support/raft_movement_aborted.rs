// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Aborted learners retain physical state while exact terminal proof frees work.
use super::*;
use anyhow::Context;
use std::{future::Future, pin::Pin};

struct Started {
    cells: Vec<RaftCellHandle>,
    target: RaftCellHandle,
    source: usize,
    movement: FleetMovementRequest,
    permit: FleetPermitAuthorization,
}

async fn start(s: &Setup, seeded: bool) -> Result<Started> {
    let (cells, authorization) = running(s).await?;
    let (source, movement, permit) = fenced::reserve(s, &cells).await?;
    let target = s
        .target
        .register_managed_history(authorization, TIMEOUT)
        .await?;
    cells[source]
        .begin_managed_repair(permit.clone(), movement.plan.clone(), TIMEOUT)
        .await?;
    if seeded {
        target
            .activate_managed_learner(permit.clone(), activation(s, &movement, source, TIMEOUT))
            .await?;
    }
    Ok(Started {
        cells,
        target,
        source,
        movement,
        permit,
    })
}

fn activation(
    s: &Setup,
    movement: &FleetMovementRequest,
    source: usize,
    timeout: Duration,
) -> LearnerActivation {
    LearnerActivation {
        member: movement.plan.replacement.member,
        source: MemberId::new(source as u64 + 1).unwrap(),
        source_node: s.nodes[source].spec.node,
        from_s3: true,
        timeout,
    }
}

async fn abort(s: &Setup, started: &Started) -> Result<()> {
    ensure!(
        step(
            &s.f,
            &started.cells[started.source],
            &started.permit,
            RepairStep::AbortLearner
        )
        .await?
        .phase
            == RepairPhase::Aborted
    );
    Ok(())
}

async fn warm_owners(s: &Setup, started: &Started) -> Result<()> {
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let authorization =
        s.f.catalog
            .authorize_group(&cut, &started.movement.resources.group, TIMEOUT)
            .await?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let mut ready = true;
                for node in &s.nodes {
                    let sample = s.nodes[started.source]
                        .host
                        .observe_peer_fleet_membership(
                            node.spec.node,
                            authorization.clone(),
                            cut.revision(),
                            TIMEOUT,
                        )
                        .await?;
                    ready &= sample.membership().status().phase == Some(FleetPermitPhase::Aborted);
                }
                if ready {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await???;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_aborted_movement_retains_seeded_history_and_recovers_receipt() -> Result<()> {
    Box::pin(retained_case()).await
}

async fn retained_case() -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let started = start(&s, true).await?;
    abort(&s, &started).await?;
    warm_owners(&s, &started).await?;
    write_read(&s.f, &started.cells, 2, 5, 12).await?;
    let (cut, request) = completion(&s, &started.movement, 230).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .settle_aborted_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision() + 1);
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let current = catalog.read(TIMEOUT).await?;
    let receipt = catalog
        .settle_aborted_movement(&current, &request, &s.nodes[0].host, TIMEOUT)
        .await?;
    ensure!(
        receipt.retained_target()
            && receipt.reservation() == request.reservation
            && receipt.applied_revision() == current.revision()
            && receipt.observed_revision() == current.revision()
    );
    ensure!(catalog
        .pending_movements(&current, TIMEOUT)
        .await?
        .is_empty());
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 1);
    for wrong in [
        catalog
            .complete_movement(&current, &request, &s.target, TIMEOUT)
            .await
            .map(|_| ()),
        catalog
            .settle_fenced_movement(&current, &request, &s.target, TIMEOUT)
            .await
            .map(|_| ()),
    ] {
        ensure!(matches!(wrong, Err(FleetCatalogError::PayloadConflict)));
    }
    let mut changed = request.clone();
    changed.attestation.used.memory_bytes += 1;
    ensure!(matches!(
        catalog
            .settle_aborted_movement(&current, &changed, &s.target, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(started.cells[started.source]
        .begin_managed_repair(
            started.permit.clone(),
            started.movement.plan.clone(),
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(started.cells[started.source]
        .advance_managed_repair(
            started.permit.clone(),
            RepairStep::PromoteAndRemove,
            started.cells[started.source]
                .status()
                .await?
                .membership_revision,
            TIMEOUT
        )
        .await
        .is_err());
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
    let execution = FleetMovementExecutionRequest {
        reservation: request.reservation,
        attestation: request.attestation.clone(),
        from_s3: true,
    };
    joined(s.target_node).await?;
    let retry = catalog
        .settle_aborted_movement(&next, &request, &s.nodes[0].host, TIMEOUT)
        .await?;
    ensure!(
        retry.retained_target()
            && retry.applied_revision() == current.revision()
            && retry.observed_revision() == next.revision()
    );
    let progress = catalog
        .reconcile_movement(&next, &execution, &s.nodes[0].host, TIMEOUT)
        .await?;
    ensure!(progress.settled && progress.phase == FleetMovementPhase::Aborted);
    ensure!(catalog.read(TIMEOUT).await?.revision() == next.revision());
    for key in [
        format!("resource/{}", hex::encode(other.identity.id)),
        format!("resource/{}", hex::encode(request.reservation)),
        format!("resource-floor/{}", hex::encode([7; 16])),
    ] {
        ensure!(catalog
            .get(&next, FleetTable::Operations, &key, TIMEOUT)
            .await?
            .is_some());
    }
    write_read(&s.f, &started.cells, 3, 9, 21).await?;
    for node in s.nodes {
        joined(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}

struct Evidence {
    request: FleetMovementExecutionRequest,
}

impl FleetMovementEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        _timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<FleetMovementExecutionRequest, FleetCatalogError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if movement.reservation != self.request.reservation {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(self.request.clone())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_aborted_service_settles_empty_target_and_fences_late_activation() -> Result<()> {
    Box::pin(empty_service_case()).await
}

async fn empty_service_case() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let started = start(&s, false).await?;
    abort(&s, &started).await?;
    warm_owners(&s, &started).await?;
    let (cut, request) = completion(&s, &started.movement, 230).await?;
    let execution = FleetMovementExecutionRequest {
        reservation: request.reservation,
        attestation: request.attestation.clone(),
        from_s3: true,
    };
    let service =
        s.f.catalog
            .start_movement_service(
                s.nodes[0].host.clone(),
                Arc::new(Evidence {
                    request: execution.clone(),
                }),
                FleetMovementServiceOptions {
                    interval: Duration::from_millis(50),
                    retry_max_interval: Duration::from_millis(500),
                    turn_timeout: TIMEOUT,
                },
            )
            .await?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
            while service.status().settled_movements != 1 {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??;
    service.shutdown().await;
    let current = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        current.revision() == cut.revision() + 1
            && s.f
                .catalog
                .pending_movements(&current, TIMEOUT)
                .await?
                .is_empty()
    );
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    ensure!(started
        .target
        .activate_managed_learner(
            started.permit.clone(),
            activation(&s, &started.movement, started.source, TIMEOUT)
        )
        .await
        .is_err());
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    let progress =
        s.f.catalog
            .reconcile_movement(&current, &execution, &s.nodes[0].host, TIMEOUT)
            .await?;
    ensure!(progress.settled && progress.phase == FleetMovementPhase::Aborted);
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
    s.f.catalog
        .reserve_resources(
            &current,
            &other,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    write_read(&s.f, &started.cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_aborted_movement_refuses_active_and_joint_work() -> Result<()> {
    Box::pin(active_case()).await
}

async fn active_case() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let started = start(&s, true).await?;
    let (cut, request) = completion(&s, &started.movement, 230).await?;
    ensure!(s
        .f
        .catalog
        .settle_aborted_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(
        step(
            &s.f,
            &started.cells[started.source],
            &started.permit,
            RepairStep::PromoteAndRemove
        )
        .await?
        .phase
            == RepairPhase::Joint
    );
    ensure!(s
        .f
        .catalog
        .settle_aborted_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(s.f.catalog.pending_movements(&cut, TIMEOUT).await?.len() == 1);
    ensure!(
        step(
            &s.f,
            &started.cells[started.source],
            &started.permit,
            RepairStep::FinishJoint
        )
        .await?
        .phase
            == RepairPhase::Complete
    );
    let live = [
        started.cells[started.source].clone(),
        started.cells[(started.source + 2) % 3].clone(),
        started.target,
    ];
    write_read(&s.f, &live, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_aborted_movement_inflight_seed_retains_debt() -> Result<()> {
    Box::pin(inflight_case()).await
}

async fn inflight_case() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let started = start(&s, false).await?;
    let seed = (started.source + 2) % 3;
    s.f.store.archive_hang.store(true, Ordering::Release);
    let pending = started.target.activate_managed_learner(
        started.permit.clone(),
        activation(&s, &started.movement, seed, Duration::from_millis(500)),
    );
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if s.nodes[seed].host.observe_node(TIMEOUT).await?.seed_workers > 0 {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await???;
    drop(pending);
    abort(&s, &started).await?;
    warm_owners(&s, &started).await?;
    s.f.domain
        .run(async {
            cellar::asyncrt::sleep(Duration::from_millis(600)).await;
        })
        .await?;
    ensure!(s.nodes[seed].host.observe_node(TIMEOUT).await?.seed_workers > 0);
    let (cut, request) = completion(&s, &started.movement, 230).await?;
    ensure!(s
        .f
        .catalog
        .settle_aborted_movement(&cut, &request, &s.nodes[0].host, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    s.f.store.archive_hang.store(false, Ordering::Release);
    s.f.store.archive_resume.notify_waiters();
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while s.nodes[seed].host.observe_node(TIMEOUT).await?.seed_workers != 0 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await???;
    let receipt =
        s.f.catalog
            .settle_aborted_movement(&cut, &request, &s.nodes[0].host, TIMEOUT)
            .await
            .context("settle after actual seed exit")?;
    ensure!(!receipt.retained_target());
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    write_read(&s.f, &started.cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_aborted_movement_unavailable_owner_retains_debt() -> Result<()> {
    Box::pin(unavailable_case()).await
}

async fn unavailable_case() -> Result<()> {
    let mut s = Box::pin(Setup::new(true)).await?;
    let started = start(&s, false).await?;
    abort(&s, &started).await?;
    warm_owners(&s, &started).await?;
    let (cut, request) = completion(&s, &started.movement, 230).await?;
    joined(s.nodes.remove((started.source + 1) % 3).node).await?;
    ensure!(s
        .f
        .catalog
        .settle_aborted_movement(&cut, &request, &s.target, Duration::from_millis(500))
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(s.f.catalog.pending_movements(&cut, TIMEOUT).await?.len() == 1);
    write_read(
        &s.f,
        &[
            started.cells[started.source].clone(),
            started.cells[(started.source + 2) % 3].clone(),
        ],
        2,
        5,
        12,
    )
    .await?;
    s.close().await
}
