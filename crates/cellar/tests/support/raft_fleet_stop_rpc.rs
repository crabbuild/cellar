// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real TLS between empty managed nodes, with no business-cell identity or
//! Actor lease in the stop path. Root intent and local fsync still authorize it.
use super::*;

async fn discover(s: &Started, origin: &RaftCells, minimum: u64) -> Result<()> {
    s.f.remote(origin, NodeId::new([7; 16])?, minimum).await?;
    s.f.remote(&s.observer, NodeId::new([8; 16])?, minimum)
        .await?;
    ensure!(origin.observe_node(TIMEOUT).await?.registered_histories == 0);
    ensure!(s.observer.observe_node(TIMEOUT).await?.registered_histories == 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_rpc_empty_nodes_and_fence() -> Result<()> {
    let (s, _gate) = Box::pin(guarded()).await?;
    let (_origin_root, origin_node, origin) = s.f.peer_host(8).await?;
    let (operation, admission) = finalizing(&s).await?;
    let minimum = admission.applied_revision();
    discover(&s, &origin, minimum).await?;
    let id = ProposalId::new([239; 16])?;
    let fence = admission.stop_fence_request(id)?;
    let before = s.f.catalog.read(TIMEOUT).await?.revision();
    ensure!(matches!(
        origin
            .request_peer_fleet_stop(fence.clone(), u64::MAX - 3, TIMEOUT)
            .await,
        Err(FleetCatalogError::Observation(InspectionError::StalePeer))
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == before);
    ensure!(!s.observer_node.is_closed());
    let mut wrong = serde_json::to_value(&fence)?;
    wrong["selector"]["boot"]["boot"] = serde_json::json!(vec![138; 16]);
    ensure!(matches!(
        origin
            .request_peer_fleet_stop(serde_json::from_value(wrong)?, minimum, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict | FleetCatalogError::PayloadConflict)
    ));
    for _ in 0..2 {
        let accepted = origin
            .request_peer_fleet_stop(fence.clone(), minimum, TIMEOUT)
            .await?;
        ensure!(accepted.action() == FleetStopAction::Fence && accepted.request() == id.bytes());
    }
    let stop_id = ProposalId::new([238; 16])?;
    ensure!(matches!(
        origin
            .request_peer_fleet_stop(admission.stop_request(stop_id)?, minimum, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(!s.observer_node.is_closed());
    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    stopped(s.observer_node).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, stop_id, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    stopped(origin_node).await?;
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_rpc_waiter_loss_still_joins() -> Result<()> {
    let (s, gate) = Box::pin(guarded()).await?;
    let (_origin_root, origin_node, origin) = s.f.peer_host(8).await?;
    let (operation, admission) = finalizing(&s).await?;
    let minimum = admission.applied_revision();
    discover(&s, &origin, minimum).await?;
    let id = ProposalId::new([238; 16])?;
    let release = gate.arm(fault::HOLD_SYNC);
    let mut pending =
        Box::pin(origin.request_peer_fleet_stop(admission.stop_request(id)?, minimum, TIMEOUT));
    ensure!(futures_util::poll!(pending.as_mut()).is_pending());
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified()))
        .await??;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    drop(pending);
    drop(release);
    finished(s.observer_node, &s.f).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let proof =
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await?;
    ensure!(
        proof.node() == [7; 16] && proof.process() == [137; 16] && proof.request() == id.bytes()
    );
    write_read(&s.f, &s.cells, 1, 5, 5).await?;
    ensure!(s.scratch.usage()?.generations == 0);
    stopped(origin_node).await?;
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_rpc_expired_metadata_wait_has_no_effect() -> Result<()> {
    let (s, _gate) = Box::pin(guarded()).await?;
    let (_origin_root, origin_node, origin) = s.f.peer_host(8).await?;
    let (_operation, admission) = finalizing(&s).await?;
    let minimum = admission.applied_revision();
    discover(&s, &origin, minimum).await?;
    s.f.store.fleet_read_hang.store(true, Ordering::Release);
    s.f.store.hold_fleet_reads.store(true, Ordering::Release);
    let id = ProposalId::new([238; 16])?;
    let mut pending = Box::pin(origin.request_peer_fleet_stop(
        admission.stop_request(id)?,
        minimum,
        Duration::from_millis(500),
    ));
    ensure!(futures_util::poll!(pending.as_mut()).is_pending());
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while s.f.store.active_fleet_reads.load(Ordering::Acquire) == 0 {
                cellar::asyncrt::sleep(Duration::from_millis(1)).await;
            }
        }))
        .await??;
    let outcome = pending.await;
    s.f.domain
        .run(async { cellar::asyncrt::sleep(Duration::from_millis(75)).await })
        .await?;
    s.f.store.hold_fleet_reads.store(false, Ordering::Release);
    s.f.store.fleet_read_hang.store(false, Ordering::Release);
    s.f.store.fleet_read_resume.notify_waiters();
    ensure!(matches!(outcome, Err(FleetCatalogError::Unconfirmed)));
    let fenced = origin
        .request_peer_fleet_stop(
            admission.stop_fence_request(ProposalId::new([239; 16])?)?,
            minimum,
            TIMEOUT,
        )
        .await?;
    ensure!(fenced.action() == FleetStopAction::Fence);
    ensure!(!s.observer_node.is_closed());
    stopped(s.observer_node).await?;
    stopped(origin_node).await?;
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_rpc_removed_origin_cannot_admit_work() -> Result<()> {
    let (s, _gate) = Box::pin(guarded()).await?;
    let (_origin_root, origin_node, origin) = s.f.peer_host(8).await?;
    let (_operation, admission) = finalizing(&s).await?;
    let minimum = admission.applied_revision();
    discover(&s, &origin, minimum).await?;
    s.f.state(NodeId::new([8; 16])?, FleetNodeState::Removed, 243)
        .await?;
    let result = origin
        .request_peer_fleet_stop(
            admission.stop_request(ProposalId::new([238; 16])?)?,
            minimum,
            TIMEOUT,
        )
        .await;
    ensure!(matches!(
        result,
        Err(FleetCatalogError::Conflict
            | FleetCatalogError::Invalid(_)
            | FleetCatalogError::Unconfirmed)
    ));
    let fresh = s.f.catalog.read(TIMEOUT).await?.revision();
    s.f.remote(&s.nodes[0].host, NodeId::new([7; 16])?, fresh)
        .await?;
    let fenced = s.nodes[0]
        .host
        .request_peer_fleet_stop(
            admission.stop_fence_request(ProposalId::new([239; 16])?)?,
            fresh,
            TIMEOUT,
        )
        .await?;
    ensure!(fenced.action() == FleetStopAction::Fence && !s.observer_node.is_closed());
    stopped(s.observer_node).await?;
    stopped(origin_node).await?;
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}
