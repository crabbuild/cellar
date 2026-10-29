// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Readiness is an owned point-in-time proof, distinct from stop consent.
use super::*;
use cellar::application::consensus::verification::{
    FleetChange, FleetMutation, FleetRestartReadyRequest, FleetTable,
};

fn ready(gate: &FleetRestartGateRequest, revision: u64) -> FleetRestartReadyRequest {
    let mut identity = request(240, revision);
    identity.operation = gate.identity.operation;
    FleetRestartReadyRequest {
        identity,
        node: gate.node,
        expected_process: gate.expected_process,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_empty_target_requires_complete_unrelated_inventory() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let boot = s
        .spare
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("empty boot")?;
    let node = NodeId::new(boot.node)?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let preparing = FleetRestartRequest {
        identity: request(210, root.revision()),
        node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    let committed =
        s.f.catalog
            .begin_restart_maintenance(
                &root,
                &preparing,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    let mut identity = request(212, committed.snapshot.revision());
    identity.operation = preparing.identity.operation;
    let gate = FleetRestartGateRequest {
        identity,
        node,
        expected_process: preparing.expected_process,
        gate: ProposalId::new([213; 16])?,
    };
    s.f.catalog
        .admit_restart_gate(&committed.snapshot, &gate, TIMEOUT)
        .await?;
    s.f.remote(
        &s.nodes[0].host,
        node,
        s.f.catalog.read(TIMEOUT).await?.revision(),
    )
    .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut walk =
        s.f.catalog
            .prepare_restart_verification(
                &root,
                ProposalId::new(preparing.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                &s.nodes[0].host,
                TIMEOUT,
            )
            .await?;
    let request = ready(&gate, root.revision());
    ensure!(matches!(
        walk.publish_ready(&request, TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(matches!(
        walk.reconcile_next(TIMEOUT).await?,
        Some(FleetRestartRuntimeProgress::Verified(
            FleetRestartHistoryProgress::Unrelated { .. }
        ))
    ));
    let receipt = walk.publish_ready(&request, TIMEOUT).await?;
    let candidate = receipt.candidate();
    ensure!(
        candidate.histories() == 1
            && candidate.physical_histories() == 0
            && candidate.voting_histories() == 0
            && candidate.matched_histories() == 0
    );
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, node, TIMEOUT)
        .await
        .is_err());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_primary_keeps_slot_and_recovers_after_cancellation() -> Result<()> {
    let s = Box::pin(Setup::new())
        .await
        .context("ready primary setup")?;
    let p = primary(&s.f, &s.cells)
        .await
        .context("ready primary discovery")?;
    let (gate, mut walk) = prepare(&s, p).await.context("ready primary preparation")?;
    ensure!(
        consume(&s, &mut walk, &s.proposal.scope)
            .await
            .context("ready primary handoff")?
            == 1
    );
    let root = s.f.catalog.read(TIMEOUT).await?;
    let request = ready(&gate, root.revision());
    let receipt = walk
        .publish_ready(&request, TIMEOUT)
        .await
        .context("publish primary readiness")?;
    ensure!(receipt.identity() == &request.identity);
    ensure!(receipt.applied_revision() == root.revision() + 1);
    ensure!(receipt.boot().process == gate.expected_process.bytes());
    ensure!(receipt.candidate().matched_histories() == 1);
    ensure!(receipt.candidate().physical_histories() == 1);
    let digest = receipt.candidate().digest();
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, gate.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, gate.node, TIMEOUT)
        .await
        .is_err());
    ensure!(matches!(s
        .f
        .catalog
        .begin_evacuation(
            &root,
            &super::super::super::request(245, root.revision()),
            s.nodes[(p + 1) % 3].spec.node,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await, Err(FleetCatalogError::DisruptionInProgress { operation }) if operation == gate.identity.operation));
    let q = survivor_leader(&s, p).await?;
    let r = (0..3)
        .find(|i| *i != p && *i != q)
        .context("second survivor")?;
    write_read(&s.f, &[s.cells[q].clone(), s.cells[r].clone()], 2, 5, 12)
        .await
        .context("ready survivor SQL")?;
    // Root publication precedes asynchronous route/trust installation. Own
    // the authenticated revision barrier before issuing exact stop control.
    s.f.remote(&s.spare, gate.node, root.revision()).await?;
    let intent = cancel(&gate, root.revision(), 242)?;
    s.f.catalog
        .cancel_restart_gate(&root, &intent, Some(&s.spare), TIMEOUT)
        .await
        .context("cancel published primary readiness")?;
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let historical = reopened
        .restart_readiness(&request, TIMEOUT)
        .await?
        .context("receipt")?;
    ensure!(historical.candidate().digest() == digest);
    ensure!(historical.observed_revision() > historical.applied_revision());
    // An exact retry confirms history even though live readiness is now revoked.
    ensure!(
        walk.publish_ready(&request, TIMEOUT)
            .await?
            .candidate()
            .digest()
            == digest
    );
    let mut changed = request.clone();
    changed.expected_process = ProcessGeneration::new([249; 16])?;
    ensure!(matches!(
        reopened.restart_readiness(&changed, TIMEOUT).await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_lost_confirmation_reopens_exact_receipt() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, mut walk) = prepare(&s, p).await?;
    ensure!(consume(&s, &mut walk, &s.proposal.scope).await? == 0);
    let request = ready(&gate, s.f.catalog.read(TIMEOUT).await?.revision());
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(walk.publish_ready(&request, TIMEOUT).await.is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    drop(walk);
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let receipt = reopened
        .restart_readiness(&request, TIMEOUT)
        .await?
        .context("lost receipt")?;
    ensure!(receipt.applied_revision() == request.identity.expected_revision + 1);
    ensure!(receipt.candidate().matched_histories() == 1);
    let root = reopened.read(TIMEOUT).await?;
    let mut changed = request.clone();
    changed.identity.principal.push_str("-different");
    ensure!(matches!(
        reopened.restart_readiness(&changed, TIMEOUT).await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(reopened.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_inspection_cannot_substitute_for_matched_handoff() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let (gate, mut walk) = prepare(&s, p).await?;
    ensure!(walk.verify_next(TIMEOUT).await?.is_some());
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        walk.publish_ready(&ready(&gate, root.revision()), TIMEOUT)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_expired_walk_refuses_unchanged_root() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, mut walk) = prepare(&s, p).await?;
    consume(&s, &mut walk, &s.proposal.scope).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    // Age actual evidence past the unchanged production five-second envelope.
    s.f.domain
        .run(async { cellar::asyncrt::sleep(Duration::from_millis(5001)).await })
        .await?;
    let request = ready(&gate, root.revision());
    ensure!(matches!(
        walk.publish_ready(&request, TIMEOUT).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(s
        .f
        .catalog
        .restart_readiness(&request, TIMEOUT)
        .await?
        .is_none());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_relevant_root_drift_blocks_publication() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, mut walk) = prepare(&s, p).await?;
    consume(&s, &mut walk, &s.proposal.scope).await?;
    s.declare_extra(&s.f.catalog).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let request = ready(&gate, root.revision());
    ensure!(matches!(
        walk.publish_ready(&request, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_generic_writes_cannot_forge_or_delete_proof() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, mut walk) = prepare(&s, p).await?;
    consume(&s, &mut walk, &s.proposal.scope).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let request = ready(&gate, root.revision());
    walk.publish_ready(&request, TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    for (offset, value) in [Some(vec![1]), None].into_iter().enumerate() {
        let identity = super::super::super::request(246 + offset as u8, root.revision());
        let mutation = FleetMutation {
            id: identity.id,
            operation: gate.identity.operation,
            principal: identity.principal,
            expected_revision: root.revision(),
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: format!("restart-ready/{}", hex::encode(gate.identity.operation)),
                value,
            }],
        };
        ensure!(matches!(
            s.f.catalog.commit(&root, &mutation, TIMEOUT).await,
            Err(FleetCatalogError::Invalid("restart readiness authority"))
        ));
    }
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(s
        .f
        .catalog
        .restart_readiness(&request, TIMEOUT)
        .await?
        .is_some());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_ready_synced_cancellation_fence_blocks_unchanged_root() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, mut walk) = prepare(&s, p).await?;
    consume(&s, &mut walk, &s.proposal.scope).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let intent = cancel(&gate, root.revision(), 242)?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    // Independent controllers have independent bounded adapter turns.
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == root.revision());
    let request = intent.clone();
    let observer = s.spare.clone();
    let pending = s.f.domain.spawn(async move {
        catalog
            .cancel_restart_gate(&cut, &request, Some(&observer), TIMEOUT)
            .await
    })?;
    let pending = match futures_util::future::select(Box::pin(s.waiting()), Box::pin(pending)).await
    {
        futures_util::future::Either::Left((ready, pending)) => {
            ready?;
            pending
        }
        futures_util::future::Either::Right((result, _)) => {
            let result = result.map(|result| result.map(|_| ()));
            anyhow::bail!("cancellation completed before held Root CAS: {result:?}");
        }
    };
    // While the Fence is synced but Root is unchanged, a fresh final physical
    // cut cannot replay the quiesce selector. Do not wait for the CAS timeout.
    let request = ready(&gate, root.revision());
    ensure!(walk.publish_ready(&request, TIMEOUT).await.is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    pending.await??;
    ensure!(s
        .f
        .catalog
        .restart_readiness(&request, TIMEOUT)
        .await?
        .is_none());
    s.close().await
}
