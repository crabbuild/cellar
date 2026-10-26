// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual terminal membership, retained floors and exact ambiguous completion.
use super::*;
use cellar::application::consensus::verification::FleetSnapshot;

struct Replaced {
    cells: Vec<RaftCellHandle>,
    target: RaftCellHandle,
    source: usize,
    request: FleetMovementRequest,
}

async fn replace(s: &Setup, from_s3: bool) -> Result<Replaced> {
    let (cells, authorization) = running(s).await?;
    let (p, request) = movement(s, &cells, 210).await?;
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
    let target = s
        .target
        .register_managed_history(authorization, TIMEOUT)
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
            permit.clone(),
            LearnerActivation {
                member: request.plan.replacement.member,
                source: MemberId::new(if from_s3 { p } else { (p + 2) % 3 } as u64 + 1)?,
                source_node: s.nodes[if from_s3 { p } else { (p + 2) % 3 }].spec.node,
                from_s3,
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
    write_read(
        &s.f,
        &[cells[p].clone(), cells[(p + 2) % 3].clone(), target.clone()],
        2,
        5,
        12,
    )
    .await?;
    // Warm only actual physical observations. The terminal API must independently
    // obtain its own fresh quorum and complete physical probes afterward.
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let cut = s.f.catalog.read(TIMEOUT).await?;
                let mut complete = true;
                for node in &s.nodes {
                    let observed = node
                        .host
                        .inspect_peer_physical_inventory(node.spec.node, cut.revision(), TIMEOUT)
                        .await?;
                    complete &= observed.view.seed_workers == 0
                        && observed
                            .inventory
                            .as_ref()
                            .unwrap()
                            .history(&request.resources.group)?
                            .is_some_and(|h| h.completed_sequence() == 1);
                }
                if complete {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await???;
    Ok(Replaced {
        cells,
        target,
        source: p,
        request,
    })
}

async fn completion(
    s: &Setup,
    reservation: &FleetMovementRequest,
    id: u8,
) -> Result<(FleetSnapshot, FleetMovementCompletionRequest)> {
    let cut = s.f.catalog.read(TIMEOUT).await?;
    Ok((
        cut.clone(),
        FleetMovementCompletionRequest {
            identity: Fixture::request(id, cut.revision()),
            reservation: reservation.resources.identity.id,
            attestation: s.request(0, id, cut.revision()).await?.attestation,
        },
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_completion_reuses_slot_preserves_floor() -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let r = replace(&s, true).await?;
    let (cut, request) = completion(&s, &r.request, 230).await?;
    let receipt =
        s.f.catalog
            .complete_movement(&cut, &request, &s.nodes[r.source].host, TIMEOUT)
            .await?;
    ensure!(
        receipt.reservation() == r.request.resources.identity.id
            && receipt.applied_revision() == cut.revision() + 1
    );
    let root = s.f.catalog.read(TIMEOUT).await?;
    for (id, key) in [
        (231, format!("resource-floor/{}", hex::encode([7; 16]))),
        (
            232,
            format!("resource-completion/{}", hex::encode(request.reservation)),
        ),
    ] {
        ensure!(matches!(
            s.f.catalog
                .commit(
                    &root,
                    &FleetMutation {
                        id: [id; 16],
                        operation: [100; 16],
                        principal: "forged-release".into(),
                        expected_revision: root.revision(),
                        changes: vec![FleetChange {
                            table: FleetTable::Operations,
                            key,
                            value: None
                        }]
                    },
                    TIMEOUT
                )
                .await,
            Err(FleetCatalogError::Invalid(_))
        ));
    }
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
    let second = s.request(1, 233, root.revision()).await?;
    let debit =
        s.f.catalog
            .reserve_resources(
                &root,
                &second,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(debit.applied_revision() == root.revision() + 1);
    let root = s.f.catalog.read(TIMEOUT).await?;
    let historical =
        s.f.catalog
            .complete_movement(&root, &request, &s.nodes[0].host, TIMEOUT)
            .await?;
    ensure!(historical.applied_revision() == receipt.applied_revision());
    let key = format!("resource-floor/{}", hex::encode([7; 16]));
    let bytes =
        s.f.catalog
            .get(&root, FleetTable::Operations, &key, TIMEOUT)
            .await?
            .context("missing retained floor")?;
    let floor: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    ensure!(floor["physical"]["engine_slots"] == 1);
    write_read(
        &s.f,
        &[
            r.cells[r.source].clone(),
            r.cells[(r.source + 2) % 3].clone(),
            r.target,
        ],
        3,
        1,
        13,
    )
    .await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_completion_lost_reply_survives_target_shutdown() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let r = replace(&s, false).await?;
    let (cut, request) = completion(&s, &r.request, 230).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .complete_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == cut.revision() + 1);
    joined(s.target_node).await?;
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let current = catalog.read(TIMEOUT).await?;
    let receipt = catalog
        .complete_movement(&current, &request, &s.nodes[0].host, TIMEOUT)
        .await?;
    ensure!(receipt.applied_revision() == cut.revision() + 1);
    let mut changed = request;
    changed.attestation.used.memory_bytes += 1;
    ensure!(matches!(
        catalog
            .complete_movement(&current, &changed, &s.nodes[0].host, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    for node in s.nodes {
        joined(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_completion_unstarted_retains_debt() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (_, movement) = movement(&s, &cells, 210).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .reserve_movement(
            &cut,
            &movement,
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
    let (cut, request) = completion(&s, &movement, 230).await?;
    ensure!(s
        .f
        .catalog
        .complete_movement(&cut, &request, &s.target, Duration::from_millis(500))
        .await
        .is_err());
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == cut.revision());
    ensure!(s
        .f
        .catalog
        .get(
            &root,
            FleetTable::Operations,
            &format!("resource-completion/{}", hex::encode(request.reservation)),
            TIMEOUT
        )
        .await?
        .is_none());
    let other = s.request(1, 231, root.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &root,
                &other,
                &s.nodes[0].host,
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
async fn fleet_movement_completion_unreachable_source_retains_debt() -> Result<()> {
    let mut s = Box::pin(Setup::new(true)).await?;
    let r = replace(&s, true).await?;
    joined(s.nodes.remove(r.source).node).await?;
    let (cut, request) = completion(&s, &r.request, 230).await?;
    let authorization =
        s.f.catalog
            .authorize_group(&cut, &r.request.resources.group, TIMEOUT)
            .await?;
    let surviving = s
        .target
        .observe_peer_fleet_membership(
            s.target.observe_node(TIMEOUT).await?.node.identity.node,
            authorization,
            cut.revision(),
            TIMEOUT,
        )
        .await?;
    ensure!(surviving.membership().status().phase == Some(FleetPermitPhase::Complete));
    ensure!(s
        .f
        .catalog
        .complete_movement(&cut, &request, &s.target, Duration::from_millis(500))
        .await
        .is_err());
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == cut.revision());
    ensure!(s
        .f
        .catalog
        .get(
            &root,
            FleetTable::Operations,
            &format!("resource-completion/{}", hex::encode(request.reservation)),
            TIMEOUT
        )
        .await?
        .is_none());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_seed_waiter_loss_retains_physical_worker() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, authorization) = running(&s).await?;
    let (p, request) = movement(&s, &cells, 210).await?;
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
    let permit = admitted.authorization();
    let target = s
        .target
        .register_managed_history(authorization, TIMEOUT)
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
    let seed = (p + 2) % 3;
    s.f.store.archive_hang.store(true, Ordering::Release);
    let pending = target.activate_managed_learner(
        permit,
        LearnerActivation {
            member: request.plan.replacement.member,
            source: MemberId::new(seed as u64 + 1)?,
            source_node: s.nodes[seed].spec.node,
            from_s3: true,
            timeout: Duration::from_millis(500),
        },
    );
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if s.nodes[seed].host.observe_node(TIMEOUT).await?.seed_workers > 0 {
                    break;
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await???;
    drop(pending);
    // Exceed the seed waiter's original deadline while its actual metadata
    // future remains held by the object-store gate.
    s.f.domain
        .run(async {
            cellar::asyncrt::sleep(Duration::from_millis(600)).await;
        })
        .await?;
    ensure!(s.nodes[seed].host.observe_node(TIMEOUT).await?.seed_workers > 0);
    let (cut, completion) = completion(&s, &request, 230).await?;
    ensure!(s
        .f
        .catalog
        .complete_movement(
            &cut,
            &completion,
            &s.nodes[p].host,
            Duration::from_millis(500)
        )
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    s.f.store.archive_hang.store(false, Ordering::Release);
    s.f.store.archive_resume.notify_waiters();
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if s.nodes[seed].host.observe_node(TIMEOUT).await?.seed_workers == 0 {
                    break;
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await???;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_completion_unreachable_other_seed_owner_retains_debt() -> Result<()> {
    let mut s = Box::pin(Setup::new(true)).await?;
    let r = replace(&s, true).await?;
    // This retired voter differs from the admission's selected source. It
    // could still own an old seed transfer even after the new RF3 commits.
    joined(s.nodes.remove((r.source + 1) % 3).node).await?;
    let (cut, request) = completion(&s, &r.request, 230).await?;
    let authorization =
        s.f.catalog
            .authorize_group(&cut, &r.request.resources.group, TIMEOUT)
            .await?;
    ensure!(
        s.target
            .observe_peer_fleet_membership(
                s.target.observe_node(TIMEOUT).await?.node.identity.node,
                authorization,
                cut.revision(),
                TIMEOUT
            )
            .await?
            .membership()
            .status()
            .phase
            == Some(FleetPermitPhase::Complete)
    );
    ensure!(s
        .f
        .catalog
        .complete_movement(&cut, &request, &s.target, Duration::from_millis(500))
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    s.close().await
}

#[path = "raft_movement_isolation.rs"]
mod isolation_tests;
