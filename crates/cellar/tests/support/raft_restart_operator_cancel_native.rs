// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cancellation survives ambiguity and fences the exact admitted physical work.
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartOperatorCancelRequest, FleetRestartReadyAdmission, FleetRestartStopRequest,
};

pub(super) fn cancel_request(
    original: &FleetRestartOperatorRequest,
    revision: u64,
) -> Result<FleetRestartOperatorCancelRequest> {
    let mut identity = request(200, revision);
    identity.operation = original.identity.operation;
    Ok(FleetRestartOperatorCancelRequest {
        identity,
        fence: ProposalId::new([202; 16])?,
    })
}

pub(super) async fn begin(s: &Setup, p: usize) -> Result<FleetRestartOperatorRequest> {
    let request = intent(s, p, 188).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .begin_restart_operator(&root, &request, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    Ok(request)
}

async fn ready(
    s: &Setup,
    controller: &FleetRestartOperatorDriver,
) -> Result<FleetRestartReadyAdmission> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                routes(s).await?;
                match controller.reconcile(TIMEOUT).await {
                    Ok(Some(progress)) => {
                        if let Some(ready) = progress.ready {
                            return Ok::<_, anyhow::Error>(ready);
                        }
                    }
                    Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Overloaded)
                    | Err(FleetCatalogError::Observation(
                        InspectionError::Unavailable | InspectionError::Overloaded,
                    )) => (),
                    Err(error) => return Err(error.into()),
                    Ok(None) => anyhow::bail!("missing maintenance before readiness"),
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??
}

async fn cancelled(
    s: &Setup,
    controller: &FleetRestartOperatorDriver,
    original: &FleetRestartOperatorRequest,
) -> Result<()> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                routes(s).await?;
                match controller.reconcile(TIMEOUT).await {
                    Ok(Some(progress)) if progress.cancelled.is_some() => {
                        ensure!(
                            progress.phase == FleetRestartOperatorPhase::Cancelled
                                && progress.ready.is_none()
                        );
                        return Ok::<_, anyhow::Error>(());
                    }
                    Ok(Some(_)) => (),
                    Ok(None) => {
                        let root = s.f.catalog.read(TIMEOUT).await?;
                        ensure!(s
                            .f
                            .catalog
                            .restart_operator_cancelled(
                                &root,
                                ProposalId::new(original.identity.operation)?,
                                TIMEOUT
                            )
                            .await?
                            .is_some());
                        return Ok(());
                    }
                    Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Overloaded)
                    | Err(FleetCatalogError::Observation(
                        InspectionError::Unavailable | InspectionError::Overloaded,
                    )) => (),
                    Err(error) => return Err(error.into()),
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await???;
    Ok(())
}

async fn retained(
    s: &Setup,
    original: &FleetRestartOperatorRequest,
    cancel: &FleetRestartOperatorCancelRequest,
    next: usize,
) -> Result<()> {
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = catalog.read(TIMEOUT).await?;
    ensure!(catalog
        .active_restart_operator(&root, TIMEOUT)
        .await?
        .is_none());
    let terminal = catalog
        .restart_operator_cancelled(
            &root,
            ProposalId::new(original.identity.operation)?,
            TIMEOUT,
        )
        .await?
        .context("rooted terminal cancellation")?;
    ensure!(
        terminal.cancellation_request() == cancel.identity.id
            && terminal.observed_revision() == root.revision()
    );
    let retry = catalog
        .cancel_restart_operator(&root, cancel, TIMEOUT)
        .await?;
    ensure!(retry.identity() == &cancel.identity);
    let mut wrong = cancel.clone();
    wrong.fence = ProposalId::new([203; 16])?;
    ensure!(matches!(
        catalog
            .cancel_restart_operator(&root, &wrong, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(catalog.read(TIMEOUT).await?.revision() == root.revision());
    let request = intent(s, next, 204).await?;
    catalog
        .begin_restart_operator(&root, &request, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    let current = catalog.read(TIMEOUT).await?;
    ensure!(
        catalog
            .cancel_restart_operator(&current, cancel, TIMEOUT)
            .await?
            .identity()
            == &cancel.identity
    );
    ensure!(
        catalog
            .active_restart_operator(&current, TIMEOUT)
            .await?
            .context("next operator")?
            .identity()
            == &request.identity
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_cancel_unexecuted_reconciles_lost_confirmations() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let request = begin(&s, 0).await?;
    let controller = driver(&s)?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let cancel = cancel_request(&request, root.revision())?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .cancel_restart_operator(&root, &cancel, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let recreated = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = recreated.read(TIMEOUT).await?;
    recreated
        .cancel_restart_operator(&root, &cancel, TIMEOUT)
        .await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(controller.reconcile(TIMEOUT).await.is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    cancelled(&s, &driver(&s)?, &request).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, request.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    retained(&s, &request, &cancel, 1).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_cancel_ready_fences_old_stop_and_releases_only_operator_slot(
) -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let original = begin(&s, p).await?;
    let controller = driver(&s)?;
    let ready = ready(&s, &controller).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = request(206, root.revision());
    identity.operation = ready.identity().operation;
    let stop = FleetRestartStopRequest {
        identity,
        ready: ready.identity().clone(),
        node: original.node,
        expected_process: original.expected_process,
        stop: ProposalId::new([208; 16])?,
    };
    s.f.catalog
        .admit_restart_stop(&root, &stop, TIMEOUT)
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let cancel = cancel_request(&original, root.revision())?;
    s.f.catalog
        .cancel_restart_operator(&root, &cancel, TIMEOUT)
        .await?;
    // Persist the exact SDK cancellation child, then lose its successful Root
    // confirmation. A recreated controller must reconcile it while Node=Ready.
    ensure!(
        driver(&s)?
            .reconcile(TIMEOUT)
            .await?
            .context("cancel child")?
            .phase
            == FleetRestartOperatorPhase::CancelRequested
    );
    routes(&s).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(driver(&s)?.reconcile(TIMEOUT).await.is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    cancelled(&s, &driver(&s)?, &original).await?;
    routes(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, original.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Ready
    );
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, original.node, TIMEOUT)
        .await
        .is_err());
    ensure!(s
        .f
        .catalog
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    ensure!(s
        .f
        .catalog
        .stop_restart_maintenance(&root, &stop, &s.spare, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.nodes[p].node.is_closed());
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    retained(&s, &original, &cancel, (p + 1) % 3).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_cancel_fences_held_unadmitted_preparation() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let request = begin(&s, 0).await?;
    let controller = driver(&s)?;
    ensure!(controller.reconcile(TIMEOUT).await?.is_some());
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let pending = Box::pin(controller.reconcile(TIMEOUT));
    let pending = match futures_util::future::select(Box::pin(s.waiting()), pending).await {
        futures_util::future::Either::Left((ready, pending)) => {
            ready?;
            pending
        }
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("preparation completed before held CAS")
        }
    };
    let independent = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = independent.read(TIMEOUT).await?;
    let cancel = cancel_request(&request, root.revision())?;
    independent
        .cancel_restart_operator(&root, &cancel, TIMEOUT)
        .await?;
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    ensure!(matches!(pending.await, Err(FleetCatalogError::Conflict)));
    cancelled(&s, &driver(&s)?, &request).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, request.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    ensure!(s
        .f
        .catalog
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    retained(&s, &request, &cancel, 1).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_cancel_admitted_preparation_before_gate() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let original = begin(&s, 0).await?;
    let controller = driver(&s)?;
    controller
        .reconcile(TIMEOUT)
        .await?
        .context("prepare child")?;
    controller
        .reconcile(TIMEOUT)
        .await?
        .context("admitted M1")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, original.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(
        !s.nodes[0]
            .host
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
    );
    let cancel = cancel_request(&original, root.revision())?;
    s.f.catalog
        .cancel_restart_operator(&root, &cancel, TIMEOUT)
        .await?;
    cancelled(&s, &driver(&s)?, &original).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, original.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Ready
    );
    ensure!(s
        .f
        .catalog
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    retained(&s, &original, &cancel, 1).await?;
    s.close().await
}
