// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained physical startup catches up behind a closed application gate.
use super::*;
#[path = "raft_restart_resume_native.rs"]
mod resume;
use cellar::application::consensus::verification::FleetRestartReturnRequest;

async fn offline(s: &mut Setup, p: usize) -> Result<(Running, FleetRestartReturnRequest)> {
    let (gate, request, root) = consent(s, p).await?;
    s.f.catalog
        .admit_restart_stop(&root, &request, TIMEOUT)
        .await?;
    trust(s).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let result =
        s.f.catalog
            .stop_restart_maintenance(&cut, &request, &s.spare, TIMEOUT)
            .await;
    ensure!(
        result.is_ok()
            || matches!(
                result,
                Err(FleetCatalogError::Closed | FleetCatalogError::Unconfirmed)
            )
    );
    let mut target = s.nodes.remove(p);
    joined(&mut target.node, &s.f).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut identity = request_identity(180, root.revision(), gate.identity.operation);
    let offline = FleetRestartOfflineRequest {
        identity: identity.clone(),
        stop: request,
    };
    s.f.catalog
        .confirm_restart_offline(&root, &offline, TIMEOUT)
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    identity = request_identity(182, root.revision(), gate.identity.operation);
    Ok((
        target,
        FleetRestartReturnRequest {
            identity,
            offline: offline.identity,
            node: gate.node,
            expected_process: gate.expected_process,
            next_process: ProcessGeneration::new([184; 16])?,
        },
    ))
}

fn request_identity(id: u8, revision: u64, operation: [u8; 16]) -> FleetRequestIdentity {
    let mut r = request(id, revision);
    r.operation = operation;
    r
}

async fn configuration(
    s: &Setup,
    target: &Running,
    node: &cellar::sdk::Node,
    request: &FleetRestartReturnRequest,
) -> Result<(RaftCellsConfig, RaftCellsServices)> {
    let root = s.f.catalog.read(TIMEOUT).await?;
    let auth =
        s.f.catalog
            .authorize_host(&root, target.spec.node, TIMEOUT)
            .await?;
    let (mut config, services) = s.f.config_for(node, auth, &target.spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = request.next_process;
    config.peers[0].process = request.next_process;
    config.cells = vec![RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: s.f.declaration.namespace.clone(),
        class: "Counter".into(),
        birth: s.proposal.clone(),
    }];
    Ok((config, services))
}

async fn returned(primary_target: bool, lose_confirmations: bool) -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let leader = primary(&s.f, &s.cells).await?;
    let p = if primary_target {
        leader
    } else {
        (leader + 1) % 3
    };
    let (mut target, request) = offline(&mut s, p).await?;
    let old = target.host.observe_node(TIMEOUT).await;
    ensure!(old.is_err());
    let root = s.f.catalog.read(TIMEOUT).await?;
    if lose_confirmations {
        s.f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
    }
    let result =
        s.f.catalog
            .admit_restart_return(&root, &request, TIMEOUT)
            .await;
    if lose_confirmations {
        ensure!(result.is_err());
        ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    } else {
        result?;
    }
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = reopened.read(TIMEOUT).await?;
    let admitted = reopened
        .admit_restart_return(&cut, &request, TIMEOUT)
        .await?;
    ensure!(
        admitted.next_process()? == request.next_process
            && admitted.applied_revision() == root.revision() + 1
    );
    let survivors = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != p)
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &survivors, 2, 5, 12).await?;
    let node = public_node::node_named(
        target.root.path(),
        s.f.definition.application.clone(),
        &target.spec.label,
    )
    .await?;
    if lose_confirmations {
        let (config, services) = configuration(&s, &target, &node, &request).await?;
        s.f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        ensure!(node.start_raft_cells(config, services).await.is_err());
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(TIMEOUT)
            .build()?;
        ensure!(
            client
                .get(format!(
                    "http://{}/do/Counter:probe/fetch",
                    node.info().public_address
                ))
                .send()
                .await?
                .status()
                == 503
        );

        ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    }
    let (config, services) = configuration(&s, &target, &node, &request).await?;
    let host = node
        .start_raft_cells(config, services)
        .await
        .context("retained engine return")?;
    let view = host.observe_node(TIMEOUT).await?;
    let boot = view.fleet_boot.as_ref().context("returned physical boot")?;
    ensure!(
        boot.process == request.next_process.bytes()
            && boot.process != request.expected_process.bytes()
    );
    ensure!(boot.engine == view.node.storage_instance.bytes() && view.node.profile.draining);
    ensure!(view
        .current_enrollment
        .as_ref()
        .is_some_and(|e| e.state == FleetNodeState::RestartPending));
    let root = reopened.read(TIMEOUT).await?;
    ensure!(reopened
        .authorize_capacity(&root, request.node, TIMEOUT)
        .await
        .is_err());
    ensure!(
        reopened
            .admit_restart_return(&root, &request, TIMEOUT)
            .await?
            .applied_revision()
            == admitted.applied_revision()
    );
    let cell = host.cell(s.proposal.scope.namespace, &s.proposal.scope.cell)?;
    ensure!(matches!(
        cell.command(
            s.f.definition.increment.clone(),
            1i64.to_be_bytes().to_vec(),
            public_node::options(3)?
        )
        .await,
        Err(CommandFailure::Unavailable)
    ));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()?;
    ensure!(
        client
            .get(format!(
                "http://{}/do/Counter:probe/fetch",
                node.info().public_address
            ))
            .send()
            .await?
            .status()
            == 503
    );
    let revision = root.revision();
    for survivor in &s.nodes {
        s.f.remote(&survivor.host, request.node, revision).await?;
        s.f.remote(&host, survivor.spec.node, revision).await?;
    }
    write_read(&s.f, &survivors, 3, 2, 14).await?;
    let wanted = survivors[0]
        .inspect()
        .await?
        .member
        .context("survivor progress")?
        .applied;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if cell
                    .inspect()
                    .await?
                    .member
                    .is_some_and(|m| m.applied.index > wanted.index || m.applied == wanted)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await???;
    ensure!(host.observe_node(TIMEOUT).await?.node.profile.draining);
    ensure!(reopened
        .begin_evacuation(
            &root,
            &request_identity(188, root.revision(), [189; 16]),
            s.nodes[0].spec.node,
            std::slice::from_ref(&s.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    target.node = node;
    target.host = host;
    s.nodes.push(target);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_return_primary_retains_engine_and_catches_up_behind_gate() -> Result<()> {
    Box::pin(returned(true, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_return_recovers_lost_consent_and_startup_confirmations() -> Result<()> {
    Box::pin(returned(false, true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_return_requires_joined_offline_and_fresh_process() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, stop, root) = consent(&s, p).await?;
    s.f.catalog
        .admit_restart_stop(&root, &stop, TIMEOUT)
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let request = FleetRestartReturnRequest {
        identity: request_identity(180, root.revision(), gate.identity.operation),
        offline: request_identity(178, root.revision() - 1, gate.identity.operation),
        node: gate.node,
        expected_process: gate.expected_process,
        next_process: ProcessGeneration::new([184; 16])?,
    };
    ensure!(s
        .f
        .catalog
        .admit_restart_return(&root, &request, TIMEOUT)
        .await
        .is_err());
    ensure!(
        s.f.catalog.read(TIMEOUT).await?.revision() == root.revision()
            && !s.nodes[p].node.is_closed()
    );
    trust(&s).await?;
    s.f.catalog
        .cancel_restart_gate(
            &root,
            &cancel(&gate, root.revision(), 190)?,
            Some(&s.spare),
            TIMEOUT,
        )
        .await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_return_wrong_process_replacement_engine_and_forgery_refuse() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (target, request) = offline(&mut s, p).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut reused = request.clone();
    reused.next_process = reused.expected_process;
    ensure!(matches!(
        s.f.catalog
            .admit_restart_return(&root, &reused, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    s.f.catalog
        .admit_restart_return(&root, &request, TIMEOUT)
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let key = format!("restart-return/{}", hex::encode(request.identity.operation));
    let bytes =
        s.f.catalog
            .get(&root, FleetTable::Operations, &key, TIMEOUT)
            .await?
            .context("consent")?;
    let forged = FleetMutation {
        id: [190; 16],
        operation: request.identity.operation,
        principal: request.identity.principal.clone(),
        expected_revision: root.revision(),
        changes: vec![FleetChange {
            table: FleetTable::Operations,
            key,
            value: Some(bytes),
        }],
    };
    ensure!(matches!(
        s.f.catalog.commit(&root, &forged, TIMEOUT).await,
        Err(FleetCatalogError::Invalid("restart return authority"))
    ));
    // A valid encoded Node row cannot bypass the private boot transition.
    let node_key = hex::encode(request.node.bytes());
    let bytes =
        s.f.catalog
            .get(&root, FleetTable::Nodes, &node_key, TIMEOUT)
            .await?
            .context("pending node")?;
    let payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?;
    let changed = payload.replacen(
        &format!(
            "\"process\":{}",
            serde_json::to_string(&request.expected_process.bytes())?
        ),
        &format!(
            "\"process\":{}",
            serde_json::to_string(&request.next_process.bytes())?
        ),
        1,
    );
    ensure!(changed != payload);
    let mut value = bytes[..9].to_vec();
    value.extend_from_slice(&(changed.len() as u32).to_be_bytes());
    value.extend_from_slice(changed.as_bytes());
    use sha2::{Digest, Sha256};
    value.extend_from_slice(&Sha256::digest(&value));
    let forged_boot = FleetMutation {
        id: [192; 16],
        operation: request.identity.operation,
        principal: request.identity.principal.clone(),
        expected_revision: root.revision(),
        changes: vec![FleetChange {
            table: FleetTable::Nodes,
            key: node_key,
            value: Some(value),
        }],
    };
    ensure!(matches!(
        s.f.catalog.commit(&root, &forged_boot, TIMEOUT).await,
        Err(FleetCatalogError::Invalid("restart return boot authority"))
    ));
    let node = public_node::node_named(
        target.root.path(),
        s.f.definition.application.clone(),
        &target.spec.label,
    )
    .await?;
    let (mut config, services) = configuration(&s, &target, &node, &request).await?;
    config.infrastructure.log.process = ProcessGeneration::new([185; 16])?;
    config.peers[0].process = config.infrastructure.log.process;
    ensure!(node.start_raft_cells(config, services).await.is_err());
    stopped_failed_startup(node).await?;
    let retained =
        s.f.domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    target.root.path().join("raft"),
                    target.spec.node,
                    target.spec.disk,
                    request.next_process,
                    LogOpenMode::Resume,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
    ensure!(
        Some(retained.storage_instance()?)
            == s.f
                .catalog
                .authorize_host(&root, request.node, TIMEOUT)
                .await?
                .storage_instance()
    );
    retained.close().await?;
    let replacement = tempfile::tempdir()?;
    let node = public_node::node_named(
        replacement.path(),
        s.f.definition.application.clone(),
        &target.spec.label,
    )
    .await?;
    let (mut config, services) = configuration(&s, &target, &node, &request).await?;
    config.infrastructure.log.mode = LogOpenMode::Create;
    ensure!(node.start_raft_cells(config, services).await.is_err());
    stopped_failed_startup(node).await?;
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

async fn stopped_failed_startup(node: cellar::sdk::Node) -> Result<()> {
    let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await? else {
        anyhow::bail!("failed startup node did not join");
    };
    ensure!(
        report.raft.is_none(),
        "failed startup fabricated a completed Raft host"
    );
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.pending_application_events == 0
            && !report.drain_timed_out
            && !report.deadline_elapsed
    );
    Ok(())
}
