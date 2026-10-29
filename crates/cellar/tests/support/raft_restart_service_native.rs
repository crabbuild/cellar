// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real preparation ownership, requester loss and Node cancellation/join.
use super::*;
use cellar::sdk::consensus::{FleetRestartService, FleetRestartServiceOptions};

fn options() -> FleetRestartServiceOptions {
    FleetRestartServiceOptions {
        interval: Duration::from_millis(50),
        ..Default::default()
    }
}

async fn phase(
    s: &Setup,
    service: &FleetRestartService,
    operation: [u8; 16],
    expected: FleetRestartOperatorPhase,
) -> Result<()> {
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let revision = catalog.read(TIMEOUT).await?.revision();
                let hosts = [
                    &s.nodes[0].host,
                    &s.nodes[1].host,
                    &s.nodes[2].host,
                    &s.spare,
                ];
                for origin in hosts {
                    let own = origin.observe_node(TIMEOUT).await?.node.identity.node;
                    for target in hosts {
                        let node = target.observe_node(TIMEOUT).await?.node.identity.node;
                        if node != own {
                            s.f.remote(origin, node, revision).await?;
                        }
                    }
                }
                // Model an independent management client, without borrowing
                // the receiving host's serialized catalog turn.
                let root = catalog.read(TIMEOUT).await?;
                let status = catalog
                    .restart_operator_status(&root, ProposalId::new(operation)?, TIMEOUT)
                    .await
                    .context("independent service status")?
                    .context("service operation")?;
                if status.phase == expected {
                    return Ok::<_, anyhow::Error>(());
                }
                ensure!(
                    service.status().running,
                    "service stopped: {:?}",
                    service.status()
                );
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_service_survives_requester_loss_recreates_and_cancels() -> Result<()> {
    Box::pin(requester_loss()).await
}

async fn requester_loss() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let request = cancellation_cases::begin(&s, p).await?;
    let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
    let catalog = Fixture::open_catalog(&owner, s.f.store.clone())?;
    let service = catalog
        .start_restart_service(s.spare.clone(), vec![s.namespace.clone()], None, options())
        .await?;
    owner.shutdown().await;
    let other = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    ensure!(matches!(
        other
            .start_restart_service(s.spare.clone(), vec![s.namespace.clone()], None, options())
            .await,
        Err(FleetCatalogError::Overloaded)
    ));
    ensure!(matches!(
        catalog
            .start_restart_service(s.spare.clone(), vec![], None, options())
            .await,
        Err(FleetCatalogError::Closed)
    ));
    phase(
        &s,
        &service,
        request.identity.operation,
        FleetRestartOperatorPhase::Ready,
    )
    .await?;
    ensure!(
        s.nodes[p]
            .host
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
    );
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    service.shutdown().await;
    let root = other.read(TIMEOUT).await?;
    let cancel = cancellation_cases::cancel_request(&request, root.revision())?;
    other
        .cancel_restart_operator(&root, &cancel, TIMEOUT)
        .await?;
    let service = other
        .start_restart_service(s.spare.clone(), vec![s.namespace.clone()], None, options())
        .await?;
    phase(
        &s,
        &service,
        request.identity.operation,
        FleetRestartOperatorPhase::Cancelled,
    )
    .await?;
    service.shutdown().await;
    let root = other.read(TIMEOUT).await?;
    ensure!(other
        .active_restart_operator(&root, TIMEOUT)
        .await?
        .is_none());
    ensure!(other
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    ensure!(
        other
            .authorize_host(&root, request.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Ready
    );
    write_read(&s.f, &s.cells, 3, 2, 14).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_service_node_shutdown_joins_held_root_attempt() -> Result<()> {
    Box::pin(node_shutdown()).await
}

async fn node_shutdown() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let request = cancellation_cases::begin(&s, 0).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let service =
        s.f.catalog
            .start_restart_service(s.spare.clone(), vec![s.namespace.clone()], None, options())
            .await?;
    s.waiting().await?;
    stopped(s.spare_node).await?;
    ensure!(!service.status().running && !s.f.store.fleet_cas_waiting.load(Ordering::Acquire));
    service.shutdown().await;
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    let current = s.f.catalog.read(TIMEOUT).await?;
    ensure!(current.revision() == root.revision());
    let status =
        s.f.catalog
            .restart_operator_status(
                &current,
                ProposalId::new(request.identity.operation)?,
                TIMEOUT,
            )
            .await?
            .context("retained intent")?;
    ensure!(
        status.phase == FleetRestartOperatorPhase::Requested
            && status.execution_operation.is_none()
    );
    for node in s.nodes {
        stopped(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}
