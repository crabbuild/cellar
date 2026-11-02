// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shipped CLI -> authenticated audit -> Root intent -> Node-owned execution.
use super::*;
#[path = "raft_operator_restart_native.rs"]
mod restart_cases;

fn operation(server: &Server, request: u8, revision: u64) -> RaftFleetNodeOperation {
    RaftFleetNodeOperation {
        catalog_id: server.control.catalog_id().into(),
        operation_id: hex::encode([210; 16]),
        request_id: hex::encode([request; 16]),
        expected_revision: revision,
    }
}

fn args(node: &str, verb: &str, operation: &RaftFleetNodeOperation) -> Vec<String> {
    vec![
        "fleet".into(),
        "nodes".into(),
        verb.into(),
        node.into(),
        "--operation-id".into(),
        operation.operation_id.clone(),
        "--request-id".into(),
        operation.request_id.clone(),
        "--expected-revision".into(),
        operation.expected_revision.to_string(),
    ]
}

async fn prepare(
    f: &Scenario,
    server: &Server,
) -> Result<(String, Vec<String>, RaftFleetNodeOperation)> {
    let node = hex::encode(f.s.nodes[0].spec.node.bytes());
    let process = f.s.nodes[0]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("maintenance boot")?
        .process;
    let operation = operation(server, 211, f.s.f.catalog.read(TIMEOUT).await?.revision());
    let mut args = args(&node, "prepare-maintenance", &operation);
    args.extend([
        "--expected-process".into(),
        hex::encode(process),
        "--grace-seconds".into(),
        "120".into(),
        "--birth-resolution".into(),
        "cancel".into(),
    ]);
    Ok((node, args, operation))
}

async fn refresh_routes_once(f: &Scenario, catalog: &FleetCatalog) -> Result<u64> {
    let hosts =
        f.s.nodes
            .iter()
            .map(|n| &n.host)
            .chain([&f.s.target, &f.spare])
            .collect::<Vec<_>>();
    let revision = catalog.read(TIMEOUT).await?.revision();
    for origin in &hosts {
        let own = origin.observe_node(TIMEOUT).await?.node.identity.node;
        for target in &hosts {
            let node = target.observe_node(TIMEOUT).await?.node.identity.node;
            if node != own {
                f.s.f.remote(origin, node, revision).await?;
            }
        }
    }
    Ok(revision)
}

async fn refresh_routes(f: &Scenario, catalog: &FleetCatalog) -> Result<()> {
    let mut refreshed = None;
    let mut next_refresh = std::time::Instant::now();
    loop {
        let revision = catalog.read(TIMEOUT).await?.revision();
        // Runtime admission/profile can change without a Root mutation.
        // Match normal directory pacing instead of flooding unchanged peers.
        if refreshed != Some(revision) || std::time::Instant::now() >= next_refresh {
            refreshed = Some(refresh_routes_once(f, catalog).await?);
            next_refresh = std::time::Instant::now() + Duration::from_millis(250);
        }
        cellar::asyncrt::sleep(Duration::from_millis(5)).await;
    }
}

async fn routed<F: Future>(f: &Scenario, future: F) -> Result<F::Output> {
    tokio::pin!(future);
    let catalog = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    f.s.f
        .domain
        // A CLI wait observes several separately budgeted protocol steps. It
        // must return its own typed timeout before the fixture stops routing.
        // Individual probes, catalog operations and service turns keep TIMEOUT.
        .run(cellar::asyncrt::timeout(Duration::from_secs(30), async {
            // A peer-probe retry must not prevent polling a completed CLI result.
            // Both branches share this original deadline; no task is detached.
            cellar::asyncrt::select! {
                value = &mut future => Ok(value),
                result = refresh_routes(f, &catalog) => {
                    result?;
                    Err(anyhow::anyhow!("route refresher stopped"))
                },
            }
        }))
        .await??
}

async fn view(server: &Server, operation: &str) -> Result<RaftFleetMaintenanceStatus> {
    let args = vec![
        "fleet".into(),
        "nodes".into(),
        "maintenance-status".into(),
        operation.into(),
    ];
    let (exit, value) = server.cli(&args, "reader").await?;
    ensure!(exit == 0, "maintenance status {exit}: {value}");
    let status: RaftFleetMaintenanceStatus = serde_json::from_value(value)?;
    status.validate()?;
    Ok(status)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_maintenance_cli_audits_grants_replays_unknown_and_reads_without_execution(
) -> Result<()> {
    Box::pin(queued()).await
}

async fn queued() -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    let server = Server::configured(&f.s.f, &f.s.namespace, None).await?;
    let (node, command, request) = prepare(&f, &server).await?;
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    for role in [
        "reader",
        "canceller",
        "capacity",
        "stopper",
        "evacuator",
        "remover",
        "registrar",
    ] {
        let (exit, value) = server.cli(&command, role).await?;
        ensure!(
            exit == 4 && value["error"]["code"] == "forbidden",
            "grant {role}: {exit}: {value}"
        );
    }
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, value) = server.cli(&command, "maintainer").await?;
    ensure!(
        exit == 5 && value["error"]["outcome"] == "unknown",
        "lost maintenance confirmation {exit}: {value}"
    );
    let status = view(&server, &request.operation_id).await?;
    ensure!(
        status.phase == RaftFleetMaintenancePhase::Requested
            && status.principal == "maintainer"
            && status.execution_operation.is_none()
            && status.node_id == node
    );
    let after = f.s.f.catalog.read(TIMEOUT).await?;
    let (exit, value) = server.cli(&command, "maintainer").await?;
    ensure!(
        exit == 0 && value["reconciled"] == true,
        "maintenance replay {exit}: {value}"
    );
    let admission: RaftFleetMaintenanceAcceptance = serde_json::from_value(value)?;
    admission.validate()?;
    let mut changed = command.clone();
    *changed.last_mut().context("birth choice")? = "finish".into();
    let (exit, value) = server.cli(&changed, "maintainer").await?;
    ensure!(exit == 3 && value["error"]["code"] == "payload_conflict");
    view(&server, &request.operation_id).await?;
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == after.revision());
    ensure!(
        !f.s.nodes[0]
            .host
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
    );
    ensure!(f
        .s
        .f
        .catalog
        .get(&after, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    let cancel = operation(&server, 212, after.revision());
    let mut command = args(&node, "cancel-maintenance", &cancel);
    command.extend(["--fence-id".into(), hex::encode([213; 16])]);
    let (exit, value) = server.cli(&command, "maintainer").await?;
    ensure!(exit == 4 && value["error"]["code"] == "forbidden");
    let (exit, value) = server.cli(&command, "canceller").await?;
    ensure!(exit == 0, "queued cancellation {exit}: {value}");
    ensure!(
        view(&server, &cancel.operation_id).await?.phase
            == RaftFleetMaintenancePhase::CancelRequested
    );
    server.close().await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_maintenance_cli_wait_prepares_and_cancels_after_server_loss() -> Result<()>
{
    Box::pin(owned()).await
}

async fn owned() -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    let server = Server::configured(&f.s.f, &f.s.namespace, Some(f.spare.clone())).await?;
    let service = server
        .control
        .start_maintenance_service(
            None,
            FleetRestartServiceOptions {
                interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .await?;
    let (node, mut command, request) = prepare(&f, &server).await?;
    command.extend(["--wait".into(), "--timeout-seconds".into(), "5".into()]);
    let (exit, value) = routed(&f, server.cli(&command, "maintainer")).await??;
    ensure!(exit == 0, "maintenance prepare wait {exit}: {value}");
    let ready: RaftFleetMaintenanceStatus = serde_json::from_value(value)?;
    ready.validate()?;
    ensure!(ready.phase == RaftFleetMaintenancePhase::Ready && ready.readiness_request.is_some());
    server.close().await?;
    ensure!(service.status().running);
    let server = Server::configured(&f.s.f, &f.s.namespace, None).await?;
    let before = f.s.f.catalog.read(TIMEOUT).await?;
    let status = view(&server, &request.operation_id).await?;
    ensure!(
        status.phase == RaftFleetMaintenancePhase::Ready
            && f.s.f.catalog.read(TIMEOUT).await?.revision() == before.revision()
    );
    let cancel = operation(&server, 212, before.revision());
    let mut command = args(&node, "cancel-maintenance", &cancel);
    command.extend([
        "--fence-id".into(),
        hex::encode([213; 16]),
        "--wait".into(),
        "--timeout-seconds".into(),
        "5".into(),
    ]);
    let (exit, value) = routed(&f, server.cli(&command, "canceller")).await??;
    ensure!(exit == 0, "maintenance cancellation wait {exit}: {value}");
    let status: RaftFleetMaintenanceStatus = serde_json::from_value(value)?;
    status.validate()?;
    ensure!(
        status.phase == RaftFleetMaintenancePhase::Cancelled && status.cancelled_revision.is_some()
    );
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .active_restart_operator(&root, TIMEOUT)
        .await?
        .is_none());
    ensure!(f
        .s
        .f
        .catalog
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    ensure!(
        f.s.f
            .catalog
            .authorize_host(&root, f.s.nodes[0].spec.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Ready
    );
    service.shutdown().await;
    server.close().await?;
    f.close().await
}
