// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real node-owned polling; no caller loop advances a membership operation.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize},
};

struct Evidence {
    request: FleetMovementExecutionRequest,
    target: RaftCells,
    hold: AtomicBool,
    active: Arc<AtomicUsize>,
    calls: AtomicUsize,
    wrong_reservation: bool,
}

struct Active(Arc<AtomicUsize>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl FleetMovementEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<FleetMovementExecutionRequest, FleetCatalogError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.active.fetch_add(1, Ordering::AcqRel);
            let _active = Active(self.active.clone());
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.hold.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            let observed = self
                .target
                .observe_node(timeout)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            let mut request = self.request.clone();
            request.reservation = if self.wrong_reservation {
                [9; 16]
            } else {
                movement.reservation
            };
            request.attestation.boot = observed.fleet_boot.ok_or(FleetCatalogError::Closed)?;
            request.attestation.used.engine_slots = observed.node.groups;
            Ok(request)
        })
    }
}

fn options() -> FleetMovementServiceOptions {
    FleetMovementServiceOptions {
        interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(500),
        turn_timeout: TIMEOUT,
    }
}

async fn wait(s: &Setup, mut ready: impl FnMut() -> bool) -> Result<()> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
            while !ready() {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_service_restarts_and_settles_remote_s3_work() -> Result<()> {
    Box::pin(restart_case()).await
}

async fn restart_case() -> Result<()> {
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
    let evidence = Arc::new(Evidence {
        request: execution(&s, &movement).await?,
        target: s.target.clone(),
        hold: AtomicBool::new(true),
        active: Arc::new(AtomicUsize::new(0)),
        calls: AtomicUsize::new(0),
        wrong_reservation: false,
    });
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let service = catalog
        .start_movement_service(observer.clone(), evidence.clone(), options())
        .await?;
    wait(&s, || evidence.active.load(Ordering::Acquire) == 1).await?;
    let other = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    ensure!(matches!(
        other
            .start_movement_service(observer.clone(), evidence.clone(), options())
            .await,
        Err(FleetCatalogError::Overloaded)
    ));
    service.shutdown().await;
    ensure!(evidence.active.load(Ordering::Acquire) == 0);
    let cut = other.read(TIMEOUT).await?;
    ensure!(other.pending_movements(&cut, TIMEOUT).await?.len() == 1);
    ensure!(
        cells[primary(&s.f, &cells).await?]
            .fleet_membership_status()
            .await?
            .context("missing fleet membership")?
            .consumed_sequence
            == 0
    );
    evidence.hold.store(false, Ordering::Release);
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let service = other
        .start_movement_service(observer.clone(), evidence.clone(), options())
        .await?;
    wait(&s, || service.status().settled_movements == 1).await?;
    let status = service.status();
    ensure!(status.running && status.work.is_empty() && status.execution_attempts >= 4);
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    service.shutdown().await;
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog.pending_movements(&cut, TIMEOUT).await?.is_empty());
    ensure!(observer.observe_node(TIMEOUT).await?.node.groups == 0);
    let target = s.target.cell(
        movement.resources.group.namespace,
        &movement.resources.group.cell,
    )?;
    let live = [cells[p].clone(), cells[(p + 2) % 3].clone(), target];
    write_read(&s.f, &live, 2, 5, 12).await?;
    joined(observer_node).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_service_node_shutdown_joins_blocked_evidence() -> Result<()> {
    Box::pin(shutdown_case()).await
}

async fn shutdown_case() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (_, movement, _) = reserved(&s, &cells).await?;
    let (_root, observer_node, observer) = s.f.peer_host(8).await?;
    let evidence = Arc::new(Evidence {
        request: execution(&s, &movement).await?,
        target: s.target.clone(),
        hold: AtomicBool::new(true),
        active: Arc::new(AtomicUsize::new(0)),
        calls: AtomicUsize::new(0),
        wrong_reservation: false,
    });
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let service = catalog
        .start_movement_service(observer.clone(), evidence.clone(), options())
        .await?;
    wait(&s, || evidence.active.load(Ordering::Acquire) == 1).await?;
    joined(observer_node).await?;
    ensure!(!service.status().running && evidence.active.load(Ordering::Acquire) == 0);
    service.shutdown().await;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog.pending_movements(&cut, TIMEOUT).await?.len() == 1);
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    write_read(&s.f, &cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_service_rejects_rebound_evidence_without_consuming_permit() -> Result<()> {
    Box::pin(rebound_case()).await
}

async fn rebound_case() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let (cells, _) = running(&s).await?;
    let (_, movement, _) = reserved(&s, &cells).await?;
    let evidence = Arc::new(Evidence {
        request: execution(&s, &movement).await?,
        target: s.target.clone(),
        hold: AtomicBool::new(false),
        active: Arc::new(AtomicUsize::new(0)),
        calls: AtomicUsize::new(0),
        wrong_reservation: true,
    });
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let before = catalog.read(TIMEOUT).await?.revision();
    let service = catalog
        .start_movement_service(s.nodes[0].host.clone(), evidence, options())
        .await?;
    wait(&s, || {
        service.status().work.first().is_some_and(|work| {
            work.needs_operator && work.last_failure == Some(FleetMovementServiceFailure::Rejected)
        })
    })
    .await?;
    service.shutdown().await;
    ensure!(catalog.read(TIMEOUT).await?.revision() == before);
    ensure!(
        cells[primary(&s.f, &cells).await?]
            .fleet_membership_status()
            .await?
            .context("missing fleet membership")?
            .consumed_sequence
            == 0
    );
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    s.close().await
}
