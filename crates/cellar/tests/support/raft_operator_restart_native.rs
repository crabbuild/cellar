// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Separate human CLI grants, actual joined stop, retained boot and M9 capacity.
use super::*;
use cellar::application::consensus::verification::LogOpenMode;
use cellar::application::consensus::{
    fleet_capacity_profile_digest, FleetBootBinding, FleetRestartService,
};
use cellar_logic::consensus::ProcessGeneration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_maintenance_full_restart_preserves_data_and_releases_only_terminal_capacity(
) -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    Box::pin(restart(f)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_extent_maintenance_full_restart_preserves_data_and_terminal_capacity(
) -> Result<()> {
    let f = Box::pin(Scenario::with_storage(true, 2, RaftImageStorage::Extents)).await?;
    Box::pin(restart(f)).await
}

async fn revision(server: &Server) -> Result<u64> {
    Ok(server.control.status(TIMEOUT).await?.observed_revision)
}

struct Stopped {
    root: tempfile::TempDir,
    spec: FleetNodeSpec,
    position: usize,
    boot: FleetBootBinding,
}

async fn restart(mut f: Scenario) -> Result<()> {
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
    let stopped = Box::pin(stop_primary(&mut f, server, service)).await?;
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
    let host = Box::pin(return_primary(&mut f, &server, stopped)).await?;
    Box::pin(resume_primary(&f, server, service, host)).await?;
    f.close().await
}

async fn stop_primary(
    f: &mut Scenario,
    server: Server,
    service: FleetRestartService,
) -> Result<Stopped> {
    let handles = f.live[0].iter().map(|(_, h)| h.clone()).collect::<Vec<_>>();
    let primary = primary(&f.s.f, &handles).await?;
    let target_id = f.live[0][primary].0;
    let p =
        f.s.nodes
            .iter()
            .position(|n| n.spec.node == target_id)
            .context("primary node")?;
    let node = hex::encode(target_id.bytes());
    let old_boot = f.s.nodes[p]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("original boot")?;
    let request = operation(&server, 211, revision(&server).await?);
    let mut command = args(&node, "prepare-maintenance", &request);
    command.extend([
        "--expected-process".into(),
        hex::encode(old_boot.process),
        "--grace-seconds".into(),
        "120".into(),
        "--birth-resolution".into(),
        "cancel".into(),
        "--wait".into(),
        "--timeout-seconds".into(),
        "5".into(),
    ]);
    let (exit, value) = routed(f, server.cli(&command, "maintainer"))
        .await
        .with_context(|| format!("prepare wait; service {:?}", service.status()))??;
    ensure!(
        exit == 0 && value["phase"] == "ready",
        "prepare {exit}: {value}"
    );
    let request = operation(&server, 214, revision(&server).await?);
    let mut command = args(&node, "stop-maintenance", &request);
    command.extend([
        "--expected-process".into(),
        hex::encode(old_boot.process),
        "--stop-id".into(),
        hex::encode([215; 16]),
    ]);
    for role in ["maintainer", "capacity", "reader", "canceller"] {
        let (exit, value) = server.cli(&command, role).await?;
        ensure!(
            exit == 4 && value["error"]["code"] == "forbidden",
            "stop grant {role} {exit}: {value}"
        );
    }
    // Closed target handles are retained for join, and excluded from the live
    // discovery helper once the independent worker can accept actual shutdown.
    let target = f.s.nodes.remove(p);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, value) = server.cli(&command, "stopper").await?;
    ensure!(
        exit == 5 && value["error"]["outcome"] == "unknown",
        "stop consent {exit}: {value}"
    );
    command.extend(["--wait".into(), "--timeout-seconds".into(), "5".into()]);
    let (exit, value) = routed(f, server.cli(&command, "stopper"))
        .await
        .with_context(|| format!("stop wait; service {:?}", service.status()))??;
    ensure!(
        exit == 0 && value["offline"].is_object(),
        "offline {exit}: {value}"
    );
    joined(target.node).await?;
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .authorize_capacity(&root, target_id, TIMEOUT)
        .await
        .is_err());
    ensure!(f
        .s
        .f
        .catalog
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_some());
    for members in &f.live {
        let survivors = members
            .iter()
            .filter(|(id, _)| *id != target_id)
            .map(|(_, h)| h.clone())
            .collect::<Vec<_>>();
        write_read(&f.s.f, &survivors, 2, 5, 12).await?;
    }
    // A joined stop cannot be cancelled back to Ready. The durable refusal
    // must allow return/resume to continue while retaining the real slot.
    let cancel = operation(&server, 212, revision(&server).await?);
    let mut command = args(&node, "cancel-maintenance", &cancel);
    command.extend([
        "--fence-id".into(),
        hex::encode([213; 16]),
        "--wait".into(),
        "--timeout-seconds".into(),
        "5".into(),
    ]);
    let (exit, value) = routed(f, server.cli(&command, "canceller"))
        .await
        .with_context(|| format!("cancel after join wait; service {:?}", service.status()))??;
    ensure!(
        exit == 4 && value["error"]["code"] == "blocked",
        "joined cancellation {exit}: {value}"
    );
    let status = view(&server, &request.operation_id).await?;
    ensure!(
        status.cancellation_refused
            && status.offline.is_some()
            && status.cancelled_revision.is_none()
    );
    service.shutdown().await;
    server.close().await?;
    Ok(Stopped {
        root: target.root,
        spec: target.spec,
        position: p,
        boot: old_boot,
    })
}

async fn return_primary(f: &mut Scenario, server: &Server, stopped: Stopped) -> Result<RaftCells> {
    let target_id = stopped.spec.node;
    let node = hex::encode(target_id.bytes());
    let old_boot = stopped.boot;
    let returning = operation(server, 216, revision(server).await?);
    let mut command = args(&node, "return-maintenance", &returning);
    let next = ProcessGeneration::new([217; 16])?;
    command.extend([
        "--next-process".into(),
        hex::encode(next.bytes()),
        "--wait".into(),
        "--timeout-seconds".into(),
        "5".into(),
    ]);
    let (exit, value) = server.cli(&command, "stopper").await?;
    ensure!(exit == 4 && value["error"]["code"] == "forbidden");
    let (exit, value) = routed(f, server.cli(&command, "maintainer"))
        .await
        .context("retained return CLI routing")??;
    ensure!(
        exit == 0 && value["returning"].is_object(),
        "return {exit}: {value}"
    );
    let reader = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let root = reader.read(TIMEOUT).await?;
    let context = reader
        .restart_operator_return_request(&root, ProposalId::new([210; 16])?, TIMEOUT)
        .await?
        .context("canonical return context")?;
    ensure!(context.next_process == next && context.expected_process.bytes() == old_boot.process);
    let new_node = public_node::node_named(
        stopped.root.path(),
        f.s.f.definition.application.clone(),
        &stopped.spec.label,
    )
    .await?;
    let auth = reader.authorize_host(&root, target_id, TIMEOUT).await?;
    let (mut config, services) = f.s.f.config_for(&new_node, auth, &stopped.spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = next;
    config.peers[0].process = next;
    config.cells =
        f.s.proposals
            .iter()
            .map(|birth| RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: f.s.f.declaration.namespace.clone(),
                class: "Counter".into(),
                birth: birth.clone(),
            })
            .collect();
    let host = Box::pin(new_node.start_raft_cells(config, services)).await?;
    ensure!(host.observe_node(TIMEOUT).await?.node.profile.draining);
    for members in &mut f.live {
        for (id, cell) in members.iter_mut().filter(|(id, _)| *id == target_id) {
            ensure!(*id == target_id);
            let scope = cell.scope();
            *cell = host.cell(scope.namespace, &scope.cell)?;
        }
    }
    f.s.nodes.insert(
        stopped.position,
        Running {
            root: stopped.root,
            spec: stopped.spec,
            node: new_node,
            host: host.clone(),
        },
    );
    Ok(host)
}

async fn resume_primary(
    f: &Scenario,
    server: Server,
    service: FleetRestartService,
    host: RaftCells,
) -> Result<()> {
    let target_id = host.observe_node(TIMEOUT).await?.node.identity.node;
    let node = hex::encode(target_id.bytes());
    let reader = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    let mut attestation = attestation(&f.s.f, &f.spare, &host).await?;
    let mut profile = host.observe_node(TIMEOUT).await?.node.profile;
    profile.draining = false;
    attestation.profile_digest = fleet_capacity_profile_digest(&profile)?;
    // A new intended profile uses a new capacity revision. Reusing the old
    // revision would correctly refuse before reaching the actual Node check.
    attestation.profile_revision = attestation
        .profile_revision
        .checked_add(1)
        .context("resume profile revision")?;
    attestation.evidence_reference = "r".repeat(256);
    // Before an SDK consent exists, a human can correct an unexecuted profile
    // grant. Once admitted, the intended scope is fixed by the actual SDK row.
    service.shutdown().await;
    let path = server.root.path().join("resume-capacity.json");
    let mut wrong = attestation.clone();
    wrong.profile_digest = [1; 32];
    std::fs::write(&path, serde_json::to_vec(&wrong)?)?;
    let bad = operation(&server, 218, revision(&server).await?);
    let mut command = args(&node, "resume-maintenance", &bad);
    command.extend([
        "--attestation".into(),
        path.to_string_lossy().into_owned(),
        "--resume-id".into(),
        hex::encode([219; 16]),
    ]);
    for role in ["maintainer", "capacity", "stopper", "reader"] {
        let (exit, value) = server.cli(&command, role).await?;
        ensure!(
            exit == 4 && value["error"]["code"] == "forbidden",
            "resume grant {role} {exit}: {value}"
        );
    }
    let (exit, value) = server.cli(&command, "owner").await?;
    ensure!(exit == 0, "unexecuted profile grant {exit}: {value}");
    let status = view(&server, &bad.operation_id).await?;
    ensure!(
        status.resume_consent.is_none() && host.observe_node(TIMEOUT).await?.node.profile.draining
    );
    let controller = cellar::application::consensus::verification::FleetRestartOperatorDriver::new(
        reader.clone(),
        vec![f.s.namespace.clone()],
        f.spare.clone(),
        None,
    )?;
    controller.reconcile(TIMEOUT).await?;
    let routes = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
    f.s.f
        .domain
        .run(cellar::asyncrt::timeout(
            TIMEOUT,
            refresh_routes_once(f, &routes),
        ))
        .await???;
    let before = reader.read(TIMEOUT).await?.revision();
    let rejected = controller.reconcile(TIMEOUT).await;
    let error = rejected
        .err()
        .context("incorrect live profile was accepted")?;
    ensure!(
        matches!(error, FleetCatalogError::Conflict),
        "incorrect profile: {error}"
    );
    ensure!(reader.read(TIMEOUT).await?.revision() == before);
    std::fs::write(&path, serde_json::to_vec(&attestation)?)?;
    let resume = operation(&server, 220, revision(&server).await?);
    let mut command = args(&node, "resume-maintenance", &resume);
    command.extend([
        "--attestation".into(),
        path.to_string_lossy().into_owned(),
        "--resume-id".into(),
        hex::encode([219; 16]),
    ]);
    let (exit, value) = server.cli(&command, "owner").await?;
    ensure!(exit == 0, "corrected profile grant {exit}: {value}");
    let seen = std::sync::Mutex::new((0u64, String::new()));
    f.s.f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            let mut refreshed = None;
            loop {
                let revision = routes.read(TIMEOUT).await?.revision();
                if refreshed != Some(revision) {
                    refreshed = Some(refresh_routes_once(f, &routes).await?);
                }
                let progress = controller
                    .reconcile(TIMEOUT)
                    .await?
                    .context("owned resume operation")?;
                {
                    let mut seen = seen.lock().map_err(|_| anyhow::anyhow!("progress mutex"))?;
                    seen.0 += 1;
                    seen.1 = format!(
                        "phase {:?}, histories {}/{}, root {}",
                        progress.phase,
                        progress.checked_histories,
                        progress.total_histories,
                        progress.admission.observed_revision()
                    );
                }
                if progress.phase == RaftFleetMaintenancePhase::VerifyingResume
                    && progress.checked_histories == progress.total_histories
                {
                    f.s.f
                        .store
                        .fail_fleet_confirmation
                        .store(true, Ordering::Release);
                    ensure!(controller.reconcile(TIMEOUT).await.is_err());
                    ensure!(!f.s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
                    return Ok::<_, anyhow::Error>(());
                }
            }
        }))
        .await?
        .with_context(|| format!("resume consent walk: {:?}", seen.lock()))??;
    drop(controller);
    let paused = view(&server, &resume.operation_id).await?;
    let execution = paused
        .execution_operation
        .context("SDK execution operation")?;
    let root = reader.read(TIMEOUT).await?;
    ensure!(reader
        .get(
            &root,
            FleetTable::Operations,
            &format!("restart-resume/{execution}"),
            TIMEOUT
        )
        .await?
        .is_some());
    ensure!(host.observe_node(TIMEOUT).await?.node.profile.draining);
    ensure!(reader
        .authorize_capacity(&root, target_id, TIMEOUT)
        .await
        .is_err());
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
    command.extend(["--wait".into(), "--timeout-seconds".into(), "5".into()]);
    let (exit, value) = routed(f, server.cli(&command, "owner"))
        .await
        .with_context(|| format!("terminal resume wait; service {:?}", service.status()))??;
    ensure!(
        exit == 0 && value["phase"] == "complete" && value["completed_revision"].is_number(),
        "resume {exit}: {value}"
    );
    let root = reader.read(TIMEOUT).await?;
    ensure!(reader
        .active_restart_operator(&root, TIMEOUT)
        .await?
        .is_none());
    ensure!(reader
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    ensure!(reader
        .authorize_capacity(&root, target_id, TIMEOUT)
        .await
        .is_ok());
    ensure!(!host.observe_node(TIMEOUT).await?.node.profile.draining);
    for members in &f.live {
        write_read(
            &f.s.f,
            &members.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>(),
            3,
            2,
            14,
        )
        .await?;
    }
    let before = root.revision();
    view(&server, &resume.operation_id).await?;
    ensure!(reader.read(TIMEOUT).await?.revision() == before);
    service.shutdown().await;
    server.close().await?;
    Ok(())
}
