// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual receiver-owned proof, stop, autonomous join and durable offline fact.
use super::*;
#[path = "raft_restart_return_native.rs"]
mod rejoin;
use cellar::application::consensus::verification::{
    FleetChange, FleetMutation, FleetRestartOfflineRequest, FleetRestartReadyRequest,
    FleetRestartStopRequest, FleetSnapshot, FleetTable,
};

async fn consent(
    s: &Setup,
    p: usize,
) -> Result<(
    FleetRestartGateRequest,
    FleetRestartStopRequest,
    FleetSnapshot,
)> {
    let (gate, mut walk) = prepare(s, p).await?;
    consume(s, &mut walk, &s.proposal.scope).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = request(240, root.revision());
    identity.operation = gate.identity.operation;
    let ready = FleetRestartReadyRequest {
        identity,
        node: gate.node,
        expected_process: gate.expected_process,
    };
    walk.publish_ready(&ready, TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = request(242, root.revision());
    identity.operation = gate.identity.operation;
    Ok((
        gate.clone(),
        FleetRestartStopRequest {
            identity,
            ready: ready.identity,
            node: gate.node,
            expected_process: gate.expected_process,
            stop: ProposalId::new([244; 16])?,
        },
        root,
    ))
}

async fn trust(s: &Setup) -> Result<()> {
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.spare,
        ],
    )
    .await
}

async fn joined(node: &mut cellar::sdk::Node, f: &Fixture) -> Result<()> {
    let outcome = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, node.finished()))
        .await???;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("maintenance did not autonomously stop");
    };
    let raft = report.raft.context("joined Raft report")?;
    ensure!(!raft.startup_incomplete && raft.result?.unfinished.is_empty());
    Ok(())
}

async fn stop_and_confirm(primary_target: bool, lose_replies: bool) -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let leader = primary(&s.f, &s.cells).await?;
    let p = if primary_target {
        leader
    } else {
        (leader + 1) % 3
    };
    let (gate, request, root) = consent(&s, p).await?;
    if lose_replies {
        s.f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
    }
    let admission =
        s.f.catalog
            .admit_restart_stop(&root, &request, TIMEOUT)
            .await;
    if lose_replies {
        ensure!(admission.is_err());
        ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    } else {
        admission.context("root stop intent")?;
    }
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    let admission = catalog.admit_restart_stop(&cut, &request, TIMEOUT).await?;
    ensure!(admission.applied_revision() == root.revision() + 1 && !s.nodes[p].node.is_closed());
    ensure!(matches!(
        catalog.restart_stop_closure(&request, TIMEOUT).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    let mut identity = super::super::super::request(246, cut.revision());
    identity.operation = gate.identity.operation;
    let premature = FleetRestartOfflineRequest {
        identity,
        stop: request.clone(),
    };
    ensure!(matches!(
        catalog
            .confirm_restart_offline(&cut, &premature, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(catalog.read(TIMEOUT).await?.revision() == cut.revision());
    trust(&s).await?;
    let outcome = catalog
        .stop_restart_maintenance(&cut, &request, &s.spare, TIMEOUT)
        .await;
    ensure!(
        outcome.is_ok()
            || matches!(
                outcome,
                Err(FleetCatalogError::Closed | FleetCatalogError::Unconfirmed)
            ),
        "unexpected stop refusal"
    );
    let mut target = s.nodes.remove(p);
    joined(&mut target.node, &s.f).await?;
    let closure = catalog.restart_stop_closure(&request, TIMEOUT).await?;
    ensure!(
        closure.node() == gate.node.bytes()
            && closure.process() == gate.expected_process.bytes()
            && closure.request() == request.stop.bytes()
            && closure.operation() == request.identity.operation
    );
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(
        catalog
            .authorize_host(&cut, gate.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(catalog
        .authorize_capacity(&cut, gate.node, TIMEOUT)
        .await
        .is_err());
    let survivors = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != p)
        .map(|(_, cell)| cell.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &survivors, 2, 5, 12).await?;
    let mut identity = super::super::super::request(248, cut.revision());
    identity.operation = gate.identity.operation;
    let offline = FleetRestartOfflineRequest {
        identity,
        stop: request.clone(),
    };
    if lose_replies {
        s.f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
    }
    let committed = catalog
        .confirm_restart_offline(&cut, &offline, TIMEOUT)
        .await;
    if lose_replies {
        ensure!(committed.is_err());
        ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    } else {
        ensure!(committed?.closure_digest() == closure.digest());
    }
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = reopened.read(TIMEOUT).await?;
    let recovered = reopened
        .confirm_restart_offline(&cut, &offline, TIMEOUT)
        .await?;
    ensure!(recovered.closure_digest() == closure.digest());
    ensure!(
        reopened
            .restart_offline(&cut, ProposalId::new(gate.identity.operation)?, TIMEOUT)
            .await?
            .closure_digest()
            == closure.digest()
    );
    let mut changed = offline.clone();
    changed.identity.principal.push_str("-changed");
    ensure!(matches!(
        reopened
            .confirm_restart_offline(&cut, &changed, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let mut changed = request.clone();
    changed.stop = ProposalId::new([249; 16])?;
    ensure!(matches!(
        reopened.admit_restart_stop(&cut, &changed, TIMEOUT).await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(reopened
        .begin_evacuation(
            &cut,
            &super::super::super::request(250, cut.revision()),
            s.nodes[0].spec.node,
            std::slice::from_ref(&s.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(reopened.read(TIMEOUT).await?.revision() == cut.revision());
    drop(target);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_primary_joins_and_records_offline_without_releasing_slot() -> Result<()>
{
    Box::pin(stop_and_confirm(true, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_follower_recovers_lost_consent_and_offline_confirmations() -> Result<()>
{
    Box::pin(stop_and_confirm(false, true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_renews_expired_readiness_against_current_sql() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, request, root) = consent(&s, p).await?;
    s.f.catalog
        .admit_restart_stop(&root, &request, TIMEOUT)
        .await?;
    trust(&s).await?;
    let survivors = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != p)
        .map(|(_, cell)| cell.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &survivors, 2, 5, 12).await?;
    s.f.domain
        .run(async { cellar::asyncrt::sleep(Duration::from_millis(5001)).await })
        .await?;
    let result =
        s.f.catalog
            .stop_restart_maintenance(&root, &request, &s.spare, TIMEOUT)
            .await;
    ensure!(
        result.is_ok()
            || matches!(
                result,
                Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Closed)
            )
    );
    let mut target = s.nodes.remove(p);
    joined(&mut target.node, &s.f).await?;
    s.f.catalog.restart_stop_closure(&request, TIMEOUT).await?;
    write_read(&s.f, &survivors, 3, 2, 14).await?;
    drop(target);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_missing_second_survivor_refuses_shutdown() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, request, root) = consent(&s, p).await?;
    s.f.catalog
        .admit_restart_stop(&root, &request, TIMEOUT)
        .await?;
    trust(&s).await?;
    let victim = (p + 1) % 3;
    let node = s.nodes.remove(victim);
    stopped(node.node).await?;
    let target = if victim < p { p - 1 } else { p };
    ensure!(s
        .f
        .catalog
        .stop_restart_maintenance(&root, &request, &s.spare, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.nodes[target].node.is_closed());
    ensure!(matches!(
        s.f.catalog.restart_stop_closure(&request, TIMEOUT).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let cancel = cancel(&gate, cut.revision(), 246)?;
    s.f.catalog
        .cancel_restart_gate(&cut, &cancel, Some(&s.spare), TIMEOUT)
        .await?;
    ensure!(!s.nodes[target].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_changed_basis_preserves_unknown_outcome_and_slot() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, request, root) = consent(&s, p).await?;
    s.f.catalog
        .admit_restart_stop(&root, &request, TIMEOUT)
        .await?;
    s.declare_extra(&s.f.catalog).await?;
    trust(&s).await?;
    ensure!(s
        .f
        .catalog
        .stop_restart_maintenance(&root, &request, &s.spare, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.nodes[p].node.is_closed());
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&cut, gate.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(matches!(
        s.f.catalog.restart_stop_closure(&request, TIMEOUT).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_synced_cancel_fence_defeats_delayed_stop() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, request, root) = consent(&s, p).await?;
    s.f.catalog
        .admit_restart_stop(&root, &request, TIMEOUT)
        .await?;
    trust(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let cancelled = cancel(&gate, root.revision(), 246)?;
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    let observer = s.spare.clone();
    let intent = cancelled.clone();
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let pending = s.f.domain.spawn(async move {
        catalog
            .cancel_restart_gate(&cut, &intent, Some(&observer), TIMEOUT)
            .await
    })?;
    let pending = match futures_util::future::select(Box::pin(s.waiting()), Box::pin(pending)).await
    {
        futures_util::future::Either::Left((ready, pending)) => {
            ready?;
            pending
        }
        futures_util::future::Either::Right((result, _)) => {
            let result = result.map(|r| r.map(|_| ()));
            anyhow::bail!("cancel failed before held CAS: {result:?}");
        }
    };
    ensure!(s
        .f
        .catalog
        .stop_restart_maintenance(&root, &request, &s.spare, TIMEOUT)
        .await
        .is_err());
    ensure!(
        s.f.catalog.read(TIMEOUT).await?.revision() == root.revision()
            && !s.nodes[p].node.is_closed()
    );
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    pending.await??;
    ensure!(s
        .f
        .catalog
        .stop_restart_maintenance(&root, &request, &s.spare, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.nodes[p].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_generic_projection_and_rebound_request_refuse() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, request, root) = consent(&s, p).await?;
    for (i, prefix) in ["restart-stop/", "restart-offline/"].iter().enumerate() {
        let identity = super::super::super::request(246 + i as u8, root.revision());
        let mutation = FleetMutation {
            id: identity.id,
            operation: request.identity.operation,
            principal: identity.principal,
            expected_revision: root.revision(),
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: format!("{prefix}{}", hex::encode(request.identity.operation)),
                value: Some(vec![1]),
            }],
        };
        ensure!(matches!(
            s.f.catalog.commit(&root, &mutation, TIMEOUT).await,
            Err(FleetCatalogError::Invalid("restart stop authority"))
        ));
    }
    let admission =
        s.f.catalog
            .admit_restart_stop(&root, &request, TIMEOUT)
            .await?;
    trust(&s).await?;
    let mut forged = serde_json::to_value(admission.stop_request()?)?;
    forged["selector"]["finalization"] = serde_json::json!(vec![249; 16]);
    ensure!(s
        .spare
        .request_peer_fleet_stop(
            serde_json::from_value(forged)?,
            admission.applied_revision(),
            TIMEOUT
        )
        .await
        .is_err());
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let mut changed = request.clone();
    changed.ready.principal.push_str("-changed");
    ensure!(matches!(
        s.f.catalog
            .admit_restart_stop(&cut, &changed, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(
        s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision()
            && !s.nodes[p].node.is_closed()
    );
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_empty_node_joins_without_a_dummy_group() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let boot = s
        .spare
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("empty boot")?;
    let node = NodeId::new(boot.node)?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let preparation = FleetRestartRequest {
        identity: request(210, root.revision()),
        node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    let root =
        s.f.catalog
            .begin_restart_maintenance(
                &root,
                &preparation,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?
            .snapshot;
    let mut identity = request(212, root.revision());
    identity.operation = preparation.identity.operation;
    let gate = FleetRestartGateRequest {
        identity,
        node,
        expected_process: preparation.expected_process,
        gate: ProposalId::new([213; 16])?,
    };
    s.f.catalog
        .admit_restart_gate(&root, &gate, TIMEOUT)
        .await?;
    trust(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut walk =
        s.f.catalog
            .prepare_restart_verification(
                &root,
                ProposalId::new(gate.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                &s.nodes[0].host,
                TIMEOUT,
            )
            .await?;
    ensure!(matches!(
        walk.reconcile_next(TIMEOUT).await?,
        Some(FleetRestartRuntimeProgress::Verified(
            FleetRestartHistoryProgress::Unrelated { .. }
        ))
    ));
    let mut identity = request(240, root.revision());
    identity.operation = gate.identity.operation;
    let ready = FleetRestartReadyRequest {
        identity,
        node,
        expected_process: preparation.expected_process,
    };
    walk.publish_ready(&ready, TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = request(242, root.revision());
    identity.operation = gate.identity.operation;
    let stop = FleetRestartStopRequest {
        identity,
        ready: ready.identity,
        node,
        expected_process: preparation.expected_process,
        stop: ProposalId::new([244; 16])?,
    };
    s.f.catalog
        .admit_restart_stop(&root, &stop, TIMEOUT)
        .await?;
    trust(&s).await?;
    let result =
        s.f.catalog
            .stop_restart_maintenance(&root, &stop, &s.nodes[0].host, TIMEOUT)
            .await;
    ensure!(
        result.is_ok()
            || matches!(
                result,
                Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Closed)
            )
    );
    joined(&mut s.spare_node, &s.f).await?;
    let closure = s.f.catalog.restart_stop_closure(&stop, TIMEOUT).await?;
    ensure!(closure.node() == node.bytes() && closure.process() == boot.process);
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = request(248, root.revision());
    identity.operation = gate.identity.operation;
    let offline = FleetRestartOfflineRequest { identity, stop };
    ensure!(
        s.f.catalog
            .confirm_restart_offline(&root, &offline, TIMEOUT)
            .await?
            .closure_digest()
            == closure.digest()
    );
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    for running in s.nodes.drain(..) {
        stopped(running.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}
