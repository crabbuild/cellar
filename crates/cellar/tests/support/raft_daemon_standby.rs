// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A separately owned repair destination with public capacity admission.
use super::*;

pub(crate) const INDEX: u8 = 3;
pub(crate) const ENABLED: &str = "CELLAR_RAFT_DAEMON_STANDBY_GATE";
const ATTESTATION: &str = "repair-standby-capacity.json";
const WAITING: &str = "repair standby waiting for its local replica";

pub(super) async fn wait_for_attachment(service: &RaftCells, root: &Path) -> Result<()> {
    let view = service.observe_node(TIMEOUT).await?;
    ensure!(
        view.node.groups == 0 && view.node.identity.node == node(INDEX)?,
        "repair standby already owns a group"
    );
    let attestation = capacity_attestation(view)?;
    persist(root, ATTESTATION, &serde_json::to_vec(&attestation)?)?;
    cellar::note!("{WAITING}");
    tokio::time::timeout(TIMEOUT * 16, async {
        loop {
            if service.resource_usage().retained_manifest_cells == Some(2) {
                let group = service.group(history()?)?;
                let promoted = group.quorum_membership(TIMEOUT).await.is_ok_and(|proof| {
                    proof.replica().member.get() == u64::from(INDEX) + 1
                        && proof
                            .configuration()
                            .voters
                            .contains(&proof.replica().member.get())
                        && proof.configuration().voters_outgoing.is_empty()
                });
                if !promoted {
                    cellar::asyncrt::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                let view = service.observe_node(TIMEOUT).await?;
                ensure!(
                    view.node.groups == 1 && view.fleet_boot.as_ref() == Some(&attestation.boot),
                    "repair changed the selected standby boot"
                );
                return Ok::<_, anyhow::Error>(());
            }
            cellar::asyncrt::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("repair standby did not attach a retained replica")??;
    Ok(())
}

pub(super) async fn capacity(
    cli: &OperatorCli,
    node: &str,
    path: &str,
    verb: &str,
    id: u8,
    expected: FleetNodeState,
) -> Result<()> {
    let operation = hex::encode([id; 16]);
    let revision = cli.revision().await?.to_string();
    let args = [
        "nodes",
        verb,
        node,
        "--attestation",
        path,
        "--operation-id",
        &operation,
        "--expected-revision",
        &revision,
    ];
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), "standby {verb} confirmation deadline");
        let reply = cli.call_with_timeout(&args, OWNER, remaining).await?;
        if reply.code == 5
            && matches!(
                reply.value["error"]["code"].as_str(),
                Some("unavailable" | "busy")
            )
        {
            // Directory refresh can trail the boot CAS. Preserve the same
            // revision, attestation and request while reconciling admission.
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        }
        let admitted: RaftFleetNodeAcceptance = serde_json::from_value(reply.success()?)?;
        ensure!(admitted.admission.admitted_state == expected);
        return Ok(());
    }
}

async fn bind_container(operator: &Operator, voter: &docker::Voter) -> Result<()> {
    let (owner, container) = voter.fleet_identity()?;
    let selected = operator
        .domain
        .run(
            operator
                .adapter
                .existing_namespace(operator.declaration()?.namespace),
        )
        .await??;
    let directory = RangeDirectory::new(selected, configuration::limits().directory)?;
    let isolation = FleetDockerIsolation::new(
        operator.fleet.clone(),
        cellar::docker::Docker::new("/var/run/docker.sock"),
        &owner,
        vec![directory],
    )?;
    isolation
        .bind_node(node(INDEX)?, &container, TIMEOUT)
        .await?;
    Ok(())
}

pub(super) async fn launch(
    operator: &Operator,
    root: &Path,
    admin: &ManagedIngress,
    ledger: &mut recovery::Ledger,
) -> Result<Process> {
    let cli = OperatorCli::connect(
        admin.axum.clone(),
        hex::encode(operator.fleet.catalog_id()?),
    );
    let spec = node_spec(
        INDEX,
        RaftManagedHostOptions::capability_digest(&[operator.declaration()?])?,
    )?;
    let node = hex::encode(spec.node.bytes());
    let descriptor = RaftFleetNodeDescriptor {
        node_id: node.clone(),
        disk_id: hex::encode(spec.disk.bytes()),
        physical_host: hex::encode(spec.physical_host.bytes()),
        label: spec.label,
        server_name: spec.server_name,
        capability_digest: hex::encode(spec.capability_digest),
        storage_instance: None,
    };
    let filename = "repair-standby-descriptor.json";
    persist(root, filename, &serde_json::to_vec(&descriptor)?)?;
    let descriptor_path = root.join(filename);
    let args = [
        "nodes",
        "add",
        "--descriptor",
        descriptor_path
            .to_str()
            .context("standby descriptor path")?,
        "--operation-id",
        &hex::encode([141; 16]),
        "--expected-revision",
        &cli.revision().await?.to_string(),
    ];
    let registered: RaftFleetNodeAcceptance =
        serde_json::from_value(cli.call(&args, OWNER).await?.success()?)?;
    ensure!(registered.admission.admitted_state == FleetNodeState::Registered);
    let disk = docker::disk(root, usize::from(INDEX));
    let mut process = Process::spawn(
        root,
        &format!("cluster-{INDEX}"),
        &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()],
        false,
    )?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let logs = process.logs()?;
        ensure!(
            process.child.try_wait()?.is_none() && Instant::now() < deadline,
            "standby did not publish its startup attestation\n{logs}"
        );
        ensure!(
            !logs.contains("cellar listening on ") && !logs.contains("cellar axum listening on "),
            "standby opened SQL ingress without a replica\n{logs}"
        );
        if logs.contains(WAITING) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let path = root.join(ATTESTATION);
    let attestation: FleetCapacityAttestation = serde_json::from_slice(&std::fs::read(&path)?)?;
    let path = path.to_str().context("standby attestation path")?;
    ensure!(
        attestation.boot.node == spec.node.bytes() && attestation.boot.disk == spec.disk.bytes()
    );
    capacity(&cli, &node, path, "qualify", 142, FleetNodeState::Ready).await?;
    if let Some(voter) = &process.voter {
        voter.running()?;
        bind_container(operator, voter)
            .await
            .context("bind Ready standby to its immutable Docker container")?;
    }
    capacity(&cli, &node, path, "activate", 143, FleetNodeState::Active).await?;
    let status: RaftFleetNodeStatus = serde_json::from_value(
        cli.call(&["nodes", "status", &node], OWNER)
            .await?
            .success()?,
    )?;
    ensure!(status.state == FleetNodeState::Active && status.capacity == Some(attestation.clone()));
    let logs = process.logs()?;
    ensure!(
        !logs.contains("cellar listening on ") && !logs.contains("cellar axum listening on "),
        "capacity activation opened SQL ingress without a replica\n{logs}"
    );
    ledger.record(serde_json::json!({
        "phase":"repair_standby_ready", "node":node, "process":hex::encode(attestation.boot.process),
        "engine":hex::encode(attestation.boot.engine), "pid":process.pid()?,
        "capacity_revision":status.observed_revision, "docker_bound":process.voter.is_some(),
    }))?;
    Ok(process)
}

pub(crate) async fn qualify(
    root: &Path,
    admin: &ManagedIngress,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = domain
        .run(async {
            let operator = Arc::new(Operator::open(domain.clone(), root)?);
            let mut process = launch(&operator, root, admin, ledger).await?;
            let (controller, observer) = Box::pin(repair::start_observer(&operator, root)).await?;
            let inspected = async {
                let own = observer.observe_node(TIMEOUT).await?;
                ensure!(own.node.groups == 0 && own.fleet_boot.is_some());
                let cut = operator.fleet.read(TIMEOUT).await?;
                let remote = inspect_current(&observer, node(INDEX)?, cut.revision()).await?;
                ensure!(remote.node.groups == 0 && remote.node.identity.node == node(INDEX)?);
                ledger.record(serde_json::json!({
                    "phase":"repair_observer_ready", "local_groups":own.node.groups,
                    "standby_groups":remote.node.groups,"revision":cut.revision(),
                }))?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let joined = controller.shutdown(ShutdownMode::Preserve).await;
            ensure!(
                matches!(joined, Ok(NodeOutcome::Stopped(_))),
                "repair observer join: {joined:?}"
            );
            inspected?;
            if let Some(voter) = &process.voter {
                voter.terminate()?;
            } else {
                ensure!(Command::new("/bin/kill")
                    .args(["-TERM", &process.child.id().to_string()])
                    .status()?
                    .success());
            }
            let logs = process.exited(false).await?;
            ensure!(
                logs.contains("startup interrupted by SIGTERM")
                    && !logs.contains("cellar listening on "),
                "standby cancellation did not join startup\n{logs}"
            );
            ledger.record(
                serde_json::json!({"phase":"repair_standby_joined", "startup_cancelled":true}),
            )?;
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}
