// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual CLI -> authenticated admin -> complete proof walks -> joined stop.
use super::*;

impl Server {
    fn departure_args(
        &self,
        node: &str,
        verb: &str,
        request: &RaftFleetNodeOperation,
    ) -> Vec<String> {
        vec![
            "fleet".into(),
            "nodes".into(),
            verb.into(),
            node.into(),
            "--operation-id".into(),
            request.operation_id.clone(),
            "--request-id".into(),
            request.request_id.clone(),
            "--expected-revision".into(),
            request.expected_revision.to_string(),
        ]
    }

    async fn evacuation_view(&self, operation: &str) -> Result<RaftFleetEvacuationStatus> {
        let args = vec![
            "fleet".into(),
            "nodes".into(),
            "evacuation-status".into(),
            operation.into(),
        ];
        let (exit, value) = self.cli(&args, "reader").await?;
        ensure!(exit == 0, "evacuation status {exit}: {value}");
        let status: RaftFleetEvacuationStatus = serde_json::from_value(value)?;
        status.validate()?;
        Ok(status)
    }
}

fn accepted(value: Value) -> Result<RaftFleetEvacuationProgress> {
    let result: RaftFleetEvacuationAcceptance = serde_json::from_value(value)?;
    result.progress.validate()?;
    Ok(result.progress)
}

async fn server(f: &Scenario) -> Result<Server> {
    Server::configured_runtime(
        &f.s.f,
        &f.s.namespace,
        Some(f.s.nodes[1].host.clone()),
        true,
    )
    .await
}

fn operation(server: &Server, request: u8, revision: u64) -> RaftFleetNodeOperation {
    RaftFleetNodeOperation {
        catalog_id: server.control.catalog_id().into(),
        operation_id: hex::encode([166; 16]),
        request_id: hex::encode([request; 16]),
        expected_revision: revision,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_departure_redistributes_verifies_joins_and_removes() -> Result<()> {
    Box::pin(departure()).await
}

async fn departure() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let mut admin = server(&f).await?;
    let node = hex::encode([4; 16]);
    let revision = f.s.f.catalog.read(TIMEOUT).await?.revision();
    let refused = operation(&admin, 165, revision);
    let (exit, value) = admin
        .cli(
            &admin.departure_args(&node, "evacuate", &refused),
            "evacuator",
        )
        .await?;
    ensure!(
        exit != 0,
        "departure before completed keep-set exclusion: {value}"
    );
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision);

    let mut begin = request(&f, 164).await?;
    begin.keep.retain(|n| n != &node);
    let (exit, value) = admin
        .cli(&admin.begin_args(&begin, "departure-keep.json")?, "owner")
        .await?;
    ensure!(exit == 0, "redistribution admission {exit}: {value}");
    let evidence = Arc::new(Inputs::new(&f).await?);
    let service =
        f.s.f
            .catalog
            .start_topology_service_with_collection(
                f.s.nodes[1].host.clone(),
                evidence.clone(),
                evidence,
                options(),
                FleetTopologyCollectionOptions::default(),
            )
            .await?;
    let mut args = op_args("wait", &begin.operation_id);
    args.extend(["--timeout-seconds".into(), "40".into()]);
    let (exit, value) = admin.cli(&args, "reader").await?;
    ensure!(
        exit == 0 && value["converged"] == true,
        "redistribution {exit}: {value}"
    );
    service.shutdown().await;
    completed_sql(&mut f, 164).await?;
    ensure!(f
        .live
        .iter()
        .all(|members| members.iter().all(|(n, _)| n.bytes() != [4; 16])));

    let exclusion = operation(&admin, 167, f.s.f.catalog.read(TIMEOUT).await?.revision());
    let args = admin.departure_args(&node, "evacuate", &exclusion);
    let (exit, value) = admin.cli(&args, "capacity").await?;
    ensure!(exit == 4 && value["error"]["code"] == "forbidden");
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, value) = admin.cli(&args, "evacuator").await?;
    ensure!(
        exit == 5 && value["error"]["outcome"] == "unknown",
        "lost exclusion {exit}: {value}"
    );
    let (exit, value) = admin.cli(&args, "evacuator").await?;
    ensure!(exit == 0, "reconciled exclusion {exit}: {value}");
    let excluded = accepted(value)?;
    ensure!(excluded.phase == RaftFleetEvacuationPhase::Excluded && excluded.total_histories == 2);
    let status = admin
        .evacuation_view(&exclusion.operation_id)
        .await
        .context("evacuation phase status")?;
    ensure!(status.state == RaftFleetNodeState::Draining && status.pending_movements.is_empty());
    ensure!(!f.s.nodes[0].node.is_closed());

    let forged = reqwest::Client::new().post(format!("{}/api/v1/fleets/test/raft/nodes/{}/prepare-removal", admin.url, node))
        .bearer_auth(REMOVER).json(&json!({"catalog_id":exclusion.catalog_id,"operation_id":exclusion.operation_id,
            "request_id":hex::encode([176;16]),"expected_revision":status.observed_revision,"proof":"forged","terminal":true})).send().await?;
    ensure!(forged.status() == reqwest::StatusCode::BAD_REQUEST);
    let preparation = operation(&admin, 168, status.observed_revision);
    let args = admin.departure_args(&node, "prepare-removal", &preparation);
    let (exit, value) = admin.cli(&args, "evacuator").await?;
    ensure!(exit == 4 && value["error"]["code"] == "forbidden");
    f.s.f
        .store
        .refuse_archive_payload_reads
        .store(true, Ordering::Release);
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit != 0, "physical replay failure admitted fence: {value}");
    f.s.f
        .store
        .refuse_archive_payload_reads
        .store(false, Ordering::Release);
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == status.observed_revision);
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit == 0, "first history {exit}: {value}");
    let partial = accepted(value)?;
    ensure!(
        partial.phase == RaftFleetEvacuationPhase::VerifyingBeforeStop
            && partial.checked_histories == 1
            && partial.total_histories == 2
    );
    admin.close().await?;
    admin = server(&f).await?;
    // Public SDK use has no caller-provided execution-domain scope.
    let repeated = admin
        .control
        .prepare_node_removal("remover", &node, preparation.clone(), TIMEOUT)
        .await?;
    ensure!(
        repeated.checked_histories == 1,
        "restart must repeat ephemeral proof"
    );
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit == 0 && accepted(value)?.checked_histories == 2);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(
        exit == 5 && value["error"]["outcome"] == "unknown",
        "lost finalization {exit}: {value}"
    );
    admin.close().await?;
    admin = server(&f).await?;
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit == 0, "historical fence {exit}: {value}");
    let fence = accepted(value)?;
    ensure!(fence.phase == RaftFleetEvacuationPhase::Finalizing && fence.checked_histories == 2);
    ensure!(
        !f.s.nodes[0].node.is_closed(),
        "preparation cannot stop process"
    );
    ensure!(admin.control.node(&node, TIMEOUT).await?.state == RaftFleetNodeState::Finalizing);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.s.f
            .catalog
            .authorize_host(&cut, NodeId::new([4; 16])?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let revision = fence.observed_revision;
    let process = fence.process.context("fenced process")?;
    let stop = operation(&admin, 169, revision);
    let mut stop_args = admin.departure_args(&node, "stop", &stop);
    stop_args.extend(["--expected-process".into(), process.clone()]);
    let (exit, value) = admin.cli(&stop_args, "remover").await?;
    ensure!(exit == 4 && value["error"]["code"] == "forbidden");
    let premature = RaftFleetNodeRemove {
        operation: operation(&admin, 170, revision),
        stop_id: stop.request_id.clone(),
    };
    let mut removal_args = admin.departure_args(&node, "remove", &premature.operation);
    removal_args.extend(["--stop-id".into(), premature.stop_id.clone()]);
    let (exit, value) = admin.cli(&removal_args, "remover").await?;
    ensure!(exit != 0, "removal without signed join: {value}");
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision);
    let wrong = RaftFleetNodeStop {
        operation: operation(&admin, 171, revision),
        expected_process: hex::encode([92; 16]),
    };
    ensure!(admin
        .control
        .stop_evacuated_node("stopper", &node, wrong, TIMEOUT)
        .await
        .is_err());

    // Install current metadata on the departing process through the existing
    // authenticated discovery path before the exact stop RPC.
    f.s.f
        .remote(&f.s.nodes[1].host, NodeId::new([4; 16])?, revision)
        .await?;
    let (exit, value) = admin.cli(&stop_args, "stopper").await?;
    ensure!(
        (exit == 0 && accepted(value.clone())?.phase == RaftFleetEvacuationPhase::StopRequested)
            || (exit == 5 && value["error"]["outcome"] == "unknown"),
        "stop acceptance {exit}: {value}"
    );
    let mut departing = f.s.nodes.remove(0);
    let outcome =
        f.s.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT, departing.node.finished()))
            .await???;
    let cellar::sdk::NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("departing process did not stop")
    };
    let joined = report.raft.context("joined Raft report")?;
    ensure!(!joined.startup_incomplete && joined.result?.unfinished.is_empty());
    let (exit, value) = admin.cli(&stop_args, "stopper").await?;
    ensure!(exit == 0 && accepted(value)?.phase == RaftFleetEvacuationPhase::Joined);
    let mut changed = RaftFleetNodeStop {
        operation: stop.clone(),
        expected_process: process.clone(),
    };
    changed.operation.expected_revision -= 1;
    ensure!(matches!(
        admin
            .control
            .stop_evacuated_node("stopper", &node, changed, TIMEOUT)
            .await,
        Err(RaftFleetFailure {
            code: RaftFleetFailureCode::PayloadConflict,
            ..
        })
    ));

    let (exit, value) = admin.cli(&removal_args, "remover").await?;
    ensure!(exit == 0, "post-stop history {exit}: {value}");
    ensure!(accepted(value)?.phase == RaftFleetEvacuationPhase::VerifyingRemoval);
    let (exit, value) = admin.cli(&removal_args, "remover").await?;
    ensure!(exit == 0 && accepted(value)?.checked_histories == 2);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let mut removal_wait = removal_args.clone();
    removal_wait.push("--wait".into());
    let (exit, value) = admin.cli(&removal_wait, "remover").await?;
    ensure!(
        exit == 5 && value["error"]["outcome"] == "unknown",
        "lost removal {exit}: {value}"
    );
    admin.close().await?;
    // A terminal retry does not need a replay cache or even an observer.
    admin = Server::configured(&f.s.f, &f.s.namespace, None).await?;
    let (exit, value) = admin.cli(&removal_args, "remover").await?;
    ensure!(exit == 0 && accepted(value)?.phase == RaftFleetEvacuationPhase::Removed);
    let (exit, value) = admin.cli(&stop_args, "stopper").await?;
    ensure!(exit == 0 && accepted(value)?.phase == RaftFleetEvacuationPhase::Joined);
    let status = admin
        .evacuation_view(&exclusion.operation_id)
        .await
        .context("terminal removal status")?;
    ensure!(
        status.state == RaftFleetNodeState::Removed
            && status.removal.is_some()
            && status.finalization.is_some()
            && status.pending_movements.is_empty()
    );
    ensure!(admin.control.node(&node, TIMEOUT).await?.state == RaftFleetNodeState::Removed);
    let current = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .get(&current, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    ensure!(
        departing.node.info().data_dir.join("raft").exists(),
        "local durable state must remain retained"
    );
    for live in &f.live {
        let cells = live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>();
        write_read(&f.s.f, &cells, 4, 7, 28).await?;
    }
    admin.close().await?;
    drop(departing);
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_departure_status_retains_resource_only_debt() -> Result<()> {
    Box::pin(retained_debt()).await
}

async fn retained_debt() -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    let admin = server(&f).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let request = operation(&admin, 173, cut.revision());
    // Exercise descriptive operator status for an existing low-level admission.
    // The graceful operator guard is independently exercised by departure().
    let admitted =
        f.s.f
            .catalog
            .begin_evacuation(
                &cut,
                &FleetRequestIdentity {
                    id: [173; 16],
                    operation: [166; 16],
                    principal: "evacuator".into(),
                    expected_revision: cut.revision(),
                },
                NodeId::new([8; 16])?,
                std::slice::from_ref(&f.s.namespace),
                TIMEOUT,
            )
            .await?;
    discover(&f.s.f, &f.hosts()).await?;
    let resource = f.s.request(0, 174, admitted.snapshot.revision()).await?;
    f.s.f
        .catalog
        .reserve_resources(
            &admitted.snapshot,
            &resource,
            &f.s.nodes[0].host,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await?;
    let status = admin.evacuation_view(&request.operation_id).await?;
    ensure!(status.pending_movements.len() == 1);
    let debt = &status.pending_movements[0];
    ensure!(
        !debt.executable_movement
            && debt.reservation == hex::encode(resource.identity.id)
            && debt.target == hex::encode([7; 16])
            && debt.boot == hex::encode(resource.attestation.boot.boot)
            && debt.peak_engine_slots == resource.peak.engine_slots
            && debt.peak_durable_bytes == resource.peak.durable_bytes
            && debt.peak_memory_bytes == resource.peak.memory_bytes
    );
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&root, TIMEOUT)
        .await?
        .is_empty());
    let prepare = operation(&admin, 175, root.revision());
    let (exit, value) = admin
        .cli(
            &admin.departure_args(&hex::encode([8; 16]), "prepare-removal", &prepare),
            "remover",
        )
        .await?;
    ensure!(
        exit == 0,
        "partial verification with unrelated retained debt {exit}: {value}"
    );
    ensure!(accepted(value)?.phase == RaftFleetEvacuationPhase::VerifyingBeforeStop);
    // Unrelated resource-only debt is not an absence proof or storage credit.
    // A partial proof walk cannot authorize terminal removal or release it.
    let mut args = admin.departure_args(
        &hex::encode([8; 16]),
        "remove",
        &operation(&admin, 177, root.revision()),
    );
    args.extend(["--stop-id".into(), hex::encode([178; 16])]);
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(
        exit != 0,
        "partial verification cannot authorize removal: {value}"
    );
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(!f.spare_node.is_closed());
    ensure!(
        serde_json::to_value(
            admin
                .evacuation_view(&request.operation_id)
                .await?
                .pending_movements
        )? == serde_json::to_value(status.pending_movements)?
    );
    admin.close().await?;
    f.close().await
}
