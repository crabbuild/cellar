// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public maintenance consent and paused retained startup across daemon processes.
use super::*;

pub(super) const OPERATION: [u8; 16] = [130; 16];
const NEXT_PROCESS: [u8; 16] = [135; 16];
const CAPACITY: &str = "maintenance-return-capacity.json";
const PAUSED: &str = "maintenance retained voter is paused";
const HANDOFF: &str = "/fixture/maintenance-handoff";

#[derive(serde::Serialize, serde::Deserialize)]
struct Handoff {
    member: u64,
    term: u64,
}

pub(super) fn with_handoff(router: axum::Router, group: RaftGroupHandle) -> axum::Router {
    router.route(
        HANDOFF,
        axum::routing::post(
            move |headers: axum::http::HeaderMap, axum::Json(request): axum::Json<Handoff>| {
                let group = group.clone();
                async move {
                    use axum::http::{header, StatusCode};
                    if headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.strip_prefix("Bearer "))
                        != Some(SECRET)
                    {
                        return Err(StatusCode::UNAUTHORIZED);
                    }
                    let target =
                        MemberId::new(request.member).map_err(|_| StatusCode::BAD_REQUEST)?;
                    let start = Instant::now();
                    let progress = group
                        .replication_status(TIMEOUT)
                        .await
                        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
                    if progress.term.get() != request.term {
                        return Err(StatusCode::SERVICE_UNAVAILABLE);
                    }
                    group
                        .transfer_leadership(
                            LeadershipTransfer {
                                target,
                                expected_term: progress.term,
                                expected_revision: progress.membership_revision,
                            },
                            TIMEOUT.saturating_sub(start.elapsed()),
                        )
                        .await
                        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
                    Ok(StatusCode::NO_CONTENT)
                }
            },
        ),
    )
}

pub(super) async fn handoff_to(
    client: &reqwest::Client,
    addresses: &[ManagedIngress],
    members: &[u64; 3],
    target: usize,
    receipt: &Value,
) -> Result<u64> {
    tokio::time::timeout(TIMEOUT, async {
        let mut minimum_term = 1;
        loop {
            let (leader, term, _) =
                recovery::observe_members(client, addresses, members, None, minimum_term, receipt)
                    .await?;
            if leader == target {
                return Ok(term);
            }
            let response = client
                .post(format!("{}{HANDOFF}", addresses[leader].axum))
                .bearer_auth(SECRET)
                .json(&Handoff {
                    member: members[target],
                    term,
                })
                .send()
                .await?;
            if response.status().is_success() {
                minimum_term = term
                    .checked_add(1)
                    .context("maintenance handoff term overflow")?;
            } else {
                ensure!(
                    response.status() == 503,
                    "maintenance handoff refused: {}",
                    response.status()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    })
    .await
    .context("returned voter did not become leader")?
}

pub(super) async fn wait_for_resume(service: &RaftCells, root: &Path, index: u8) -> Result<()> {
    let observed = service.observe_node(TIMEOUT).await?;
    let boot = observed.fleet_boot.context("returned maintenance boot")?;
    ensure!(
        boot.node == node(index)?.bytes()
            && boot.process == NEXT_PROCESS
            && observed.node.groups == 1
            && observed.node.profile.draining
            && service.resource_usage().retained_manifest_cells == Some(2),
        "retained startup did not keep the exact voting history paused"
    );
    let mut profile = observed.node.profile;
    profile.draining = false;
    let mut attestation: FleetCapacityAttestation =
        serde_json::from_slice(&std::fs::read(root.join(format!("capacity-{index}.json")))?)?;
    ensure!(attestation.boot.disk == boot.disk && attestation.boot.engine == boot.engine);
    attestation.boot = boot;
    attestation.profile_revision += 1;
    attestation.profile_digest = fleet_capacity_profile_digest(&profile)?;
    persist(root, CAPACITY, &serde_json::to_vec(&attestation)?)?;
    cellar::note!("{PAUSED}");

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if !service.observe_node(TIMEOUT).await?.node.profile.draining {
                return Ok::<_, anyhow::Error>(());
            }
            cellar::asyncrt::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("returned voter still paused at startup deadline")??;
    Ok(())
}

async fn node_status(cli: &OperatorCli, node: &str) -> Result<RaftFleetNodeStatus> {
    Ok(serde_json::from_value(
        cli.call(&["nodes", "status", node], OWNER)
            .await?
            .success()?,
    )?)
}

async fn action(
    cli: &OperatorCli,
    verb: &str,
    node: &str,
    request: u8,
    options: &[&str],
) -> Result<RaftFleetMaintenanceStatus> {
    let operation = hex::encode(OPERATION);
    let request = hex::encode([request; 16]);
    let revision = cli.revision().await?.to_string();
    let mut args = vec![
        "nodes",
        verb,
        node,
        "--operation-id",
        &operation,
        "--request-id",
        &request,
        "--expected-revision",
        &revision,
        "--wait",
        "--timeout-seconds",
        "20",
    ];
    args.extend_from_slice(options);
    if verb == "prepare-maintenance" {
        let refused = cli.call(&args, cli::READER).await?;
        ensure!(refused.code == 4 && refused.value["error"]["code"] == "forbidden");
    }
    let value = cli
        .call_with_timeout(&args, OWNER, Duration::from_secs(22))
        .await?
        .success()
        .with_context(|| format!("public {verb}"))?;
    let status: RaftFleetMaintenanceStatus = serde_json::from_value(value)?;
    ensure!(status.node_id == node && status.operation_id == operation);
    Ok(status)
}

pub(crate) async fn qualify(
    root: &Path,
    client: &reqwest::Client,
    processes: &mut [Process],
    addresses: &mut [ManagedIngress],
    original: &recovery::Acknowledged,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    let operator = 0;
    let (leader, term, _) =
        recovery::observe_leader(client, addresses, None, 1, &original.receipt).await?;
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let catalog = Operator::open(domain.clone(), root)
        .and_then(|operator| Ok(hex::encode(operator.fleet.catalog_id()?)));
    domain.shutdown().await;
    let catalog = catalog?;
    let cli = OperatorCli::connect(addresses[operator].axum.clone(), catalog);
    // The hard-killed voter can be serving again without capacity for its new
    // boot. Choose a qualified target while keeping the controller live.
    let targets = if leader == 2 { [2, 1] } else { [1, 2] };
    let mut candidate = None;
    for target in targets {
        let id = hex::encode(node(u8::try_from(target)?)?.bytes());
        let before = node_status(&cli, &id).await?;
        if let Some(capacity) = before.capacity {
            candidate = Some((target, id, before.descriptor.storage_instance, capacity));
            break;
        }
    }
    let (target, node, retained_engine, original_capacity) =
        candidate.context("maintenance needs an independently qualified target")?;
    let process = hex::encode(original_capacity.boot.process);
    let ready = action(
        &cli,
        "prepare-maintenance",
        &node,
        131,
        &[
            "--expected-process",
            &process,
            "--grace-seconds",
            "120",
            "--birth-resolution",
            "cancel",
        ],
    )
    .await?;
    ensure!(ready.phase == RaftFleetMaintenancePhase::Ready && ready.readiness_request.is_some());
    let old_pid = processes[target].pid()?;
    ledger.record(serde_json::json!({
        "phase":"maintenance_ready", "operation":ready.operation_id, "member":target + 1,
        "leader":leader + 1, "term":term, "pid":old_pid,
        "engine":hex::encode(original_capacity.boot.engine),
    }))?;
    let offline = action(
        &cli,
        "stop-maintenance",
        &node,
        132,
        &[
            "--expected-process",
            &process,
            "--stop-id",
            &hex::encode([133; 16]),
        ],
    )
    .await?;
    // The worker can record the offline proof before advancing the phase.
    ensure!(
        offline.offline.is_some(),
        "stop wait returned without durable offline proof"
    );
    let logs = processes[target].exited(true).await?;
    ensure!(
        logs.contains("\"owners_closed\":true"),
        "maintenance stop lacked joined ownership\n{logs}"
    );
    let stopped = node_status(&cli, &node).await?;
    ensure!(stopped.capacity.is_none() && stopped.descriptor.storage_instance == retained_engine);
    let (successor, successor_term, _) =
        recovery::observe_leader(client, addresses, Some(target), term, &original.receipt).await?;
    ensure!(successor != target && (leader != target || successor_term > term));
    ledger.record(serde_json::json!({
        "phase":"maintenance_offline", "operation":offline.operation_id, "member":target + 1,
        "pid":old_pid, "successor":successor + 1, "term":successor_term,
        "observed_phase":offline.phase,
    }))?;
    let command = format!("{}:1", hex::encode([137; 16]));
    let retry = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        + 120_000)
        .to_string();
    let written = verify_sql(
        client,
        &addresses[successor],
        Ingress::Native,
        &command,
        &retry,
        "quorum",
    )
    .await?;
    ledger.record(serde_json::json!({
        "phase":"maintenance_offline_sql", "member":successor + 1,
        "ingress":"native", "command":command, "receipt":written,
    }))?;

    let returning = action(
        &cli,
        "return-maintenance",
        &node,
        134,
        &["--next-process", &hex::encode(NEXT_PROCESS)],
    )
    .await?;
    ensure!(
        returning.returning.context("return consent")?.next_process == hex::encode(NEXT_PROCESS)
    );
    let disk = docker::disk(root, target);
    processes[target] = Process::spawn(
        root,
        &format!("cluster-maintenance-{target}"),
        &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()],
        false,
    )?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let logs = processes[target].logs()?;
        ensure!(
            processes[target].child.try_wait()?.is_none() && Instant::now() < deadline,
            "retained return did not start paused\n{logs}"
        );
        ensure!(
            !logs.contains("cellar listening on ") && !logs.contains("cellar axum listening on "),
            "paused voter opened public ingress\n{logs}"
        );
        if logs.contains(PAUSED) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let path = root.join(CAPACITY);
    let attestation: FleetCapacityAttestation = serde_json::from_slice(&std::fs::read(&path)?)?;
    ensure!(
        attestation.boot.process == NEXT_PROCESS
            && attestation.boot.engine == original_capacity.boot.engine
    );
    ensure!(node_status(&cli, &node).await?.capacity.is_none());
    ledger.record(serde_json::json!({
        "phase":"maintenance_return_paused", "operation":returning.operation_id,
        "member":target + 1, "engine":hex::encode(attestation.boot.engine),
    }))?;
    let complete = action(
        &cli,
        "resume-maintenance",
        &node,
        136,
        &[
            "--attestation",
            path.to_str().context("resume attestation path")?,
            "--resume-id",
            &hex::encode([138; 16]),
        ],
    )
    .await?;
    ensure!(
        complete.phase == RaftFleetMaintenancePhase::Complete
            && complete.completed_revision.is_some()
    );
    addresses[target] = processes[target].managed_addresses().await?;
    let new_pid = processes[target].pid()?;
    let after = node_status(&cli, &node).await?;
    ensure!(
        new_pid != old_pid
            && after.state == FleetNodeState::Active
            && after.capacity == Some(attestation)
    );
    ledger.record(serde_json::json!({
        "phase":"maintenance_complete", "operation":complete.operation_id,
        "member":target + 1, "pid":new_pid, "revision":complete.completed_revision,
    }))?;
    // Membership does not grant namespace forwarding. Exercise the returned
    // process as leader, using a term- and revision-guarded transfer.
    let resumed_term = handoff_to(client, addresses, &[1, 2, 3], target, &written).await?;
    ledger.record(serde_json::json!({
        "phase":"maintenance_returned_leader", "member":target + 1, "term":resumed_term,
    }))?;
    for durability in ["quorum", "s3"] {
        for ingress in Ingress::ALL {
            let reply = verify_sql(
                client,
                &addresses[target],
                ingress,
                &command,
                &retry,
                durability,
            )
            .await?;
            recovery::matching_receipt(&reply, &written)?;
            ledger.record(serde_json::json!({
                "phase":"maintenance_resumed_sql", "member":target + 1,
                "ingress":ingress.label(), "command":command, "receipt":reply,
            }))?;
        }
    }
    Ok(())
}

pub(super) async fn verify_sql(
    client: &reqwest::Client,
    address: &ManagedIngress,
    ingress: Ingress,
    command: &str,
    retry: &str,
    durability: &str,
) -> Result<Value> {
    let public = ingress.endpoint(address);
    let response = client
        .post(format!("{public}/counters/one?delta=0"))
        .bearer_auth(SECRET)
        .header("x-command-id", command)
        .header("x-retry-until-ms", retry)
        .header("x-write-durability", durability)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status.is_success(),
        "maintenance {} {durability} receipt refused {status}: {body}",
        ingress.label()
    );
    let receipt: Value = serde_json::from_str(&body)?;
    let read: Value = client
        .get(format!("{public}/counters/one"))
        .bearer_auth(SECRET)
        .header("x-read-consistency", "linearizable")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        receipt["value"] == 23
            && receipt["durability"] == durability
            && read["value"] == 23
            && read["group"] == receipt["group"]
            && read["incarnation"] == receipt["incarnation"]
            && read["index"].as_u64().context("maintenance read index")?
                >= receipt["index"]
                    .as_u64()
                    .context("maintenance receipt index")?,
        "maintenance changed SQL history: {receipt} / {read}"
    );
    Ok(receipt)
}
