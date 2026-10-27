// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resume departure after inventory churn by cancelling its fenced epoch.
use super::*;

fn id(tag: u8) -> [u8; 16] {
    let mut id = [231; 16];
    id[0] = tag;
    id
}

fn mutation(admin: &Server, tag: u8, op: u8, revision: u64) -> RaftFleetNodeOperation {
    RaftFleetNodeOperation {
        catalog_id: admin.control.catalog_id().into(),
        request_id: hex::encode(id(tag)),
        operation_id: hex::encode(id(op)),
        expected_revision: revision,
    }
}

fn cancel_args(admin: &Server, node: &str, request: &RaftFleetNodeCancel) -> Vec<String> {
    let mut args = admin.departure_args(node, "cancel-evacuation", &request.operation);
    if let (Some(process), Some(fence)) = (&request.expected_process, &request.fence_id) {
        args.extend([
            "--expected-process".into(),
            process.clone(),
            "--fence-id".into(),
            fence.clone(),
        ]);
    }
    args
}

async fn cancelled_birth(f: &Scenario, tag: u8) -> Result<()> {
    let mut proposal = f.s.proposals[0].clone();
    proposal.scope.group = GroupId::new(id(tag))?;
    proposal.scope.cell = format!("Counter:cancelled-{tag}");
    proposal.voters = [5, 6, 7]
        .into_iter()
        .enumerate()
        .map(|(i, n)| {
            let spec = f.s.f.peer_spec(n)?;
            Ok(InitialMember {
                member: MemberId::new(i as u64 + 1)?,
                node: spec.node,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    let reserve =
        f.s.f
            .catalog
            .reserve_birth(
                &root,
                &FleetRequestIdentity {
                    id: id(tag + 1),
                    operation: id(tag + 1),
                    principal: "birth-during-departure".into(),
                    expected_revision: root.revision(),
                },
                &f.s.namespace,
                proposal,
                TIMEOUT,
            )
            .await?;
    f.s.f
        .domain
        .run(f.s.namespace.cancel_managed_birth(
            &reserve.authorization,
            ProposalId::new(id(tag + 2))?,
            TIMEOUT,
        ))
        .await??;
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .confirm_birth(
            &root,
            &FleetRequestIdentity {
                id: id(tag + 3),
                operation: id(tag + 3),
                principal: "confirm-cancelled-birth".into(),
                expected_revision: root.revision(),
            },
            &f.s.namespace,
            &reserve.authorization,
            TIMEOUT,
        )
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_cancels_stale_departure_and_restarts_with_fresh_proof() -> Result<()> {
    Box::pin(cancel_and_restart()).await
}

async fn cancel_and_restart() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let mut admin = server(&f).await?;
    let node = hex::encode([4; 16]);
    redistribute(&mut f, &admin, &node).await?;
    let exclude = mutation(
        &admin,
        10,
        20,
        f.s.f.catalog.read(TIMEOUT).await?.revision(),
    );
    let (exit, value) = admin
        .cli(
            &admin.departure_args(&node, "evacuate", &exclude),
            "evacuator",
        )
        .await?;
    ensure!(exit == 0, "exclusion {exit}: {value}");
    let prepare = mutation(
        &admin,
        11,
        20,
        f.s.f.catalog.read(TIMEOUT).await?.revision(),
    );
    let mut args = admin.departure_args(&node, "prepare-removal", &prepare);
    args.push("--wait".into());
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit == 0, "finalization {exit}: {value}");
    let first = accepted(value)?;
    let process = first.process.context("first process")?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let original =
        f.s.f
            .catalog
            .finalization_receipt(&cut, ProposalId::new(id(20))?, TIMEOUT)
            .await?
            .context("original fence")?;
    let delayed_stop = original.stop_request(ProposalId::new(id(13))?)?;
    let fence_id = id(14);
    let delayed_fence = original.stop_fence_request(ProposalId::new(fence_id)?)?;
    cancelled_birth(&f, 100).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .remote(&f.s.nodes[1].host, NodeId::new([4; 16])?, cut.revision())
        .await?;
    ensure!(f.s.nodes[1]
        .host
        .request_peer_fleet_stop(delayed_stop.clone(), cut.revision(), TIMEOUT)
        .await
        .is_err());
    ensure!(!f.s.nodes[0].node.is_closed());
    let cancel = RaftFleetNodeCancel {
        operation: mutation(&admin, 15, 20, cut.revision()),
        expected_process: Some(process.clone()),
        fence_id: Some(hex::encode(fence_id)),
    };
    let args = cancel_args(&admin, &node, &cancel);
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit == 4 && value["error"]["code"] == "forbidden");
    let forged=reqwest::Client::new().post(format!("{}/api/v1/fleets/test/raft/nodes/{}/cancel-evacuation",admin.url,node))
        .bearer_auth(CANCELLER).json(&json!({"operation":cancel.operation,"expected_process":process,"fence_id":cancel.fence_id,"fence_ack":true})).send().await?;
    ensure!(forged.status() == reqwest::StatusCode::BAD_REQUEST);

    // Hold the root CAS only after the real target has synced the narrowing
    // fence. Another adapter then admits and confirms a new cancelled history.
    f.s.f
        .store
        .hold_next_fleet_cas
        .store(true, Ordering::Release);
    f.s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    let control = admin.control.clone();
    let pending_request = cancel.clone();
    let pending_node = node.clone();
    let pending = f.s.f.domain.spawn(async move {
        control
            .cancel_node_evacuation("canceller", &pending_node, pending_request, TIMEOUT)
            .await
    })?;
    f.s.waiting().await?;
    cancelled_birth(&f, 110).await?;
    f.s.release();
    ensure!(pending.await?.is_err());
    let current = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .evacuation_cancellation(&current, ProposalId::new(id(20))?, TIMEOUT)
        .await?
        .is_none());
    ensure!(admin.control.node(&node, TIMEOUT).await?.state == RaftFleetNodeState::Finalizing);
    let mut rebound = cancel.clone();
    rebound.operation.expected_revision = current.revision();
    ensure!(matches!(
        admin
            .control
            .cancel_node_evacuation("canceller", &node, rebound, TIMEOUT)
            .await,
        Err(RaftFleetFailure {
            code: RaftFleetFailureCode::PayloadConflict,
            ..
        })
    ));
    f.s.f
        .remote(
            &f.s.nodes[1].host,
            NodeId::new([4; 16])?,
            current.revision(),
        )
        .await?;
    let mut retry = cancel.clone();
    retry.operation = mutation(&admin, 16, 20, current.revision());
    let args = cancel_args(&admin, &node, &retry);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let (exit, value) = admin.cli(&args, "canceller").await?;
    ensure!(
        exit == 5 && value["error"]["outcome"] == "unknown",
        "lost cancellation {exit}: {value}"
    );
    admin.close().await?;
    admin = Server::configured(&f.s.f, &f.s.namespace, None).await?;
    let (exit, value) = admin.cli(&args, "canceller").await?;
    ensure!(exit == 0, "reconciled cancellation {exit}: {value}");
    let receipt = accepted(value)?;
    ensure!(
        receipt.phase == RaftFleetEvacuationPhase::Cancelled
            && receipt.terminal
            && receipt.fence_id == retry.fence_id
    );
    let view = admin.control.node(&node, TIMEOUT).await?;
    ensure!(view.state == RaftFleetNodeState::Ready && view.capacity.is_none());
    let status = admin.evacuation_view(&exclude.operation_id).await?;
    ensure!(status.cancellation.is_some() && status.state == RaftFleetNodeState::Ready);
    let mut wrong = retry.clone();
    wrong.fence_id = Some(hex::encode(id(17)));
    ensure!(matches!(
        admin
            .control
            .cancel_node_evacuation("canceller", &node, wrong, TIMEOUT)
            .await,
        Err(RaftFleetFailure {
            code: RaftFleetFailureCode::PayloadConflict,
            ..
        })
    ));
    ensure!(!f.s.nodes[0].node.is_closed());
    admin.close().await?;
    admin = server(&f).await?;
    discover(&f.s.f, &f.hosts()).await?;
    // Ready grants no capacity. Explicit fresh attestation and live checks
    // requalify this exact boot; they do not resurrect its retired bindings.
    let evidence = attestation(&f.s.f, &f.s.nodes[1].host, &f.s.nodes[0].host).await?;
    let capacity = RaftFleetNodeCapacity {
        catalog_id: admin.control.catalog_id().into(),
        request_id: hex::encode(id(18)),
        expected_revision: f.s.f.catalog.read(TIMEOUT).await?.revision(),
        attestation: evidence,
    };
    admin
        .control
        .activate_node("capacity", &node, capacity, TIMEOUT)
        .await?;
    let exclude_new = mutation(
        &admin,
        21,
        22,
        f.s.f.catalog.read(TIMEOUT).await?.revision(),
    );
    let (exit, value) = admin
        .cli(
            &admin.departure_args(&node, "evacuate", &exclude_new),
            "evacuator",
        )
        .await?;
    ensure!(exit == 0, "new exclusion {exit}: {value}");
    let prepare_new = mutation(
        &admin,
        23,
        22,
        f.s.f.catalog.read(TIMEOUT).await?.revision(),
    );
    let mut args = admin.departure_args(&node, "prepare-removal", &prepare_new);
    args.push("--wait".into());
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit == 0, "fresh complete proof {exit}: {value}");
    let fresh = accepted(value)?;
    ensure!(fresh.checked_histories == 4 && fresh.total_histories == 4);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .remote(&f.s.nodes[1].host, NodeId::new([4; 16])?, cut.revision())
        .await?;
    for old in [delayed_stop, delayed_fence] {
        ensure!(f.s.nodes[1]
            .host
            .request_peer_fleet_stop(old, cut.revision(), TIMEOUT)
            .await
            .is_err());
    }
    ensure!(!f.s.nodes[0].node.is_closed());
    let stop = mutation(&admin, 24, 22, cut.revision());
    let mut args = admin.departure_args(&node, "stop", &stop);
    args.extend(["--expected-process".into(), process]);
    let (exit, value) = admin.cli(&args, "stopper").await?;
    ensure!(
        exit == 0 || (exit == 5 && value["error"]["outcome"] == "unknown"),
        "fresh stop {exit}: {value}"
    );
    let mut departing = f.s.nodes.remove(0);
    let outcome =
        f.s.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT, departing.node.finished()))
            .await???;
    let cellar::sdk::NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("fresh stop did not join")
    };
    let raft = report.raft.context("joined report")?;
    ensure!(!raft.startup_incomplete && raft.result?.unfinished.is_empty());
    let remove = mutation(&admin, 25, 22, cut.revision());
    let mut args = admin.departure_args(&node, "remove", &remove);
    args.extend(["--stop-id".into(), stop.request_id, "--wait".into()]);
    let (exit, value) = admin.cli(&args, "remover").await?;
    ensure!(exit == 0 && accepted(value)?.phase == RaftFleetEvacuationPhase::Removed);
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    // A historical cancellation neither restores Ready nor releases another
    // operation's slot, and remains readable after later terminal removal.
    let receipt = admin
        .control
        .cancel_node_evacuation("canceller", &node, retry, TIMEOUT)
        .await?;
    ensure!(
        receipt.phase == RaftFleetEvacuationPhase::Cancelled
            && receipt.observed_revision == root.revision()
    );
    ensure!(admin.control.node(&node, TIMEOUT).await?.state == RaftFleetNodeState::Removed);
    ensure!(admin
        .evacuation_view(&exclude.operation_id)
        .await?
        .cancellation
        .is_some());
    for live in &f.live {
        write_read(
            &f.s.f,
            &live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>(),
            4,
            7,
            28,
        )
        .await?;
    }
    ensure!(departing.node.info().data_dir.join("raft").exists());
    admin.close().await?;
    drop(departing);
    f.close().await
}
