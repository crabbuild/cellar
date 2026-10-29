// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cancel wins before stop CAS, or actual signed closure refuses it afterwards.
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartOperatorAction, FleetRestartOperatorActionRequest,
};

async fn stopping(
    s: &Setup,
    p: usize,
    controller: &FleetRestartOperatorDriver,
) -> Result<FleetRestartOperatorRequest> {
    let original = cancellation_cases::begin(s, p).await?;
    cancellation_cases::ready(s, controller).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = request(204, root.revision());
    identity.operation = original.identity.operation;
    s.f.catalog
        .admit_restart_operator_action(
            &root,
            &FleetRestartOperatorActionRequest {
                identity,
                node: original.node,
                action: FleetRestartOperatorAction::Stop {
                    process: original.expected_process.bytes(),
                    stop: [206; 16],
                },
            },
            TIMEOUT,
        )
        .await?;
    controller.reconcile(TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let status =
        s.f.catalog
            .restart_operator_status(
                &root,
                ProposalId::new(original.identity.operation)?,
                TIMEOUT,
            )
            .await?
            .context("stop command")?;
    ensure!(status.phase == FleetRestartOperatorPhase::Stopping && status.stop.is_none());
    Ok(original)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_cancel_fences_held_stop_consent_before_physical_effect(
) -> Result<()> {
    Box::pin(held()).await
}

async fn held() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let controller = driver(&s)?;
    let original = stopping(&s, 0, &controller).await?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let independent = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cancel = async {
        s.waiting().await?;
        let root = independent.read(TIMEOUT).await?;
        let cancel = cancellation_cases::cancel_request(&original, root.revision())?;
        independent
            .cancel_restart_operator(&root, &cancel, TIMEOUT)
            .await?;
        s.f.store.fleet_cas_hang.store(false, Ordering::Release);
        s.f.store.fleet_cas_resume.notify_waiters();
        Ok::<_, anyhow::Error>(())
    };
    let (stopped, cancel) = tokio::join!(controller.reconcile(TIMEOUT), cancel);
    cancel?;
    ensure!(matches!(stopped, Err(FleetCatalogError::Conflict)));
    cancellation_cases::cancelled(&s, &controller, &original).await?;
    let root = independent.read(TIMEOUT).await?;
    let status = independent
        .restart_operator_status(
            &root,
            ProposalId::new(original.identity.operation)?,
            TIMEOUT,
        )
        .await?
        .context("cancelled operation")?;
    ensure!(
        status.phase == FleetRestartOperatorPhase::Cancelled
            && status.stop.is_none()
            && status.offline.is_none()
            && !status.cancellation_refused
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
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_joined_stop_refuses_cancel_and_confirms_offline_without_slot_release(
) -> Result<()> {
    Box::pin(joined_stop()).await
}

async fn joined_stop() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let controller = driver(&s)?;
    let original = stopping(&s, 0, &controller).await?;
    controller.reconcile(TIMEOUT).await?;
    routes(&s).await?;
    let result = controller.reconcile(TIMEOUT).await;
    ensure!(
        result.is_ok()
            || matches!(
                result,
                Err(FleetCatalogError::Closed | FleetCatalogError::Unconfirmed)
            )
    );
    let target = s.nodes.remove(0);
    stopped(target.node).await?;
    let independent = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = independent.read(TIMEOUT).await?;
    let status = independent
        .restart_operator_status(
            &root,
            ProposalId::new(original.identity.operation)?,
            TIMEOUT,
        )
        .await?
        .context("stopping operation")?;
    ensure!(status.stop.is_some() && status.offline.is_none());
    independent
        .cancel_restart_operator(
            &root,
            &cancellation_cases::cancel_request(&original, root.revision())?,
            TIMEOUT,
        )
        .await?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            let mut return_granted = false;
            loop {
                match controller.reconcile(TIMEOUT).await {
                    Ok(_) => (),
                    Err(error) => return Err(anyhow::Error::from(error)),
                }
                let root = independent.read(TIMEOUT).await?;
                let status = independent
                    .restart_operator_status(
                        &root,
                        ProposalId::new(original.identity.operation)?,
                        TIMEOUT,
                    )
                    .await?
                    .context("joined operation")?;
                if status.offline.is_some() && !return_granted {
                    ensure!(status.phase == FleetRestartOperatorPhase::CancelRequested);
                    let mut identity = request(208, root.revision());
                    identity.operation = original.identity.operation;
                    independent
                        .admit_restart_operator_action(
                            &root,
                            &FleetRestartOperatorActionRequest {
                                identity,
                                node: original.node,
                                action: FleetRestartOperatorAction::Return { process: [209; 16] },
                            },
                            TIMEOUT,
                        )
                        .await?;
                    return_granted = true;
                }
                if status.phase == FleetRestartOperatorPhase::Offline && status.cancellation_refused
                {
                    ensure!(
                        status.offline.is_some()
                            && status.cancelled.is_none()
                            && status.completed.is_none()
                            && status.return_grant.is_some()
                    );
                    ensure!(independent
                        .active_restart_operator(&root, TIMEOUT)
                        .await?
                        .is_some());
                    ensure!(independent
                        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
                        .await?
                        .is_some());
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await???;
    s.close().await
}
