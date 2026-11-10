// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Failed voter replacement owned by a surviving Active voter in the lab.
use super::*;

pub(crate) const ENABLED: &str = "CELLAR_RAFT_DAEMON_REPAIR_GATE";
pub(crate) const REPAIRED: [u64; 3] = [4, 2, 3];
const OBSERVER: u8 = 4;
const FAILED: usize = 0;
pub(crate) const WORKER: u8 = 1;
const REQUEST: &str = "repair-worker-request.json";
const READY: &str = "repair-worker-ready.json";
const TERMINAL: &str = "repair-worker-terminal.json";

#[derive(serde::Serialize, serde::Deserialize)]
struct WorkerRequest {
    owner: String,
    failed: FleetBootBinding,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WorkerReady {
    origin: FleetBootBinding,
    revision: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Removed {
    operation: [u8; 16],
    revision: u64,
}

fn publish<T: serde::Serialize>(root: &Path, file: &str, value: &T) -> Result<()> {
    let staged = format!("{file}.pending");
    persist(root, &staged, &serde_json::to_vec(value)?)?;
    std::fs::rename(root.join(staged), root.join(file))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn read<T: serde::de::DeserializeOwned>(root: &Path, file: &str) -> Result<Option<T>> {
    match std::fs::read(root.join(file)) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn await_worker<T: serde::de::DeserializeOwned>(
    root: &Path,
    file: &str,
    timeout: Duration,
) -> Result<T> {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(value) = read(root, file)? {
                return Ok(value);
            }
            if let Some(Err(error)) = read::<Result<Removed, String>>(root, TERMINAL)? {
                anyhow::bail!("Active repair worker: {error}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| format!("Active repair worker did not publish {file}"))?
}

const MIB: u64 = 1024 * 1024;
const DURABLE_OVERHEAD: u64 = 128 * MIB;
const DURABLE_SEED: u64 = 256 * MIB;
const DURABLE_RESERVE: u64 = 64 * MIB;
pub(super) const STANDBY_DURABLE_HEADROOM: u64 = DURABLE_OVERHEAD + DURABLE_SEED + DURABLE_RESERVE;

pub(super) async fn start_observer(
    operator: &Arc<Operator>,
    root: &Path,
) -> Result<(Node, RaftCells)> {
    let declaration = operator.declaration()?;
    let spec = node_spec(
        OBSERVER,
        RaftManagedHostOptions::capability_digest(&[declaration])?,
    )?;
    let snapshot = operator.fleet.read(TIMEOUT).await?;
    operator
        .fleet
        .register_node(
            &snapshot,
            &request(144, snapshot.revision()),
            &spec,
            TIMEOUT,
        )
        .await?;
    let disk = root.join("repair-observer");
    std::fs::create_dir(&disk)?;
    let history = history()?;
    let class = counter::definition()?;
    let application = counter::application(
        counter::CounterApi::new(SECRET.into(), history.cluster, history.namespace, &class)?,
        class,
    )?;
    // Load the exact deployment's peer key from the store that owns its leases.
    let deployment = open_deployment_store(root)?;
    let peer_key = cellar::peer_auth::load_existing(&deployment).await?;
    let node =
        public_node::node_with_peer_key(&disk, application.clone(), &spec.label, peer_key).await?;
    let started = async {
        let prepared = operator
            .clone()
            .prepare(
                NativeRaftNode {
                    node: spec.label.clone(),
                    data_dir: node.info().data_dir.clone(),
                },
                OBSERVER,
                false,
                false,
                operator.bucket.raft_store(),
            )
            .await?;
        node.start_raft_cells(
            prepared.config,
            RaftCellsServices {
                listeners: prepared.listeners,
                tls: prepared.tls,
                store: prepared.store,
                application,
                versions: prepared.versions,
                recovery: None,
            },
        )
        .await
    }
    .await;
    match started {
        Ok(cells) => Ok((node, cells)),
        Err(error) => {
            let joined = node.shutdown(ShutdownMode::Preserve).await;
            Err(error).context(format!("repair observer startup cleanup: {joined:?}"))
        }
    }
}

async fn bind_live_voters(
    operator: &Operator,
    observer: &RaftCells,
    root: &Path,
    admin: &ManagedIngress,
    processes: &[Process],
    directory: RangeDirectory,
) -> Result<WorkerRequest> {
    let cli = OperatorCli::connect(
        admin.axum.clone(),
        hex::encode(operator.fleet.catalog_id()?),
    );
    let voter = processes[FAILED]
        .voter
        .as_ref()
        .context("repair requires owned Docker voters")?;
    let (owner, _) = voter.fleet_identity()?;
    let isolation = FleetDockerIsolation::new(
        operator.fleet.clone(),
        cellar::docker::Docker::new("/var/run/docker.sock"),
        &owner,
        vec![directory],
    )?;
    let mut failed = None;
    for (index, process) in processes.iter().enumerate() {
        let id = node(u8::try_from(index)?)?;
        let snapshot = operator.fleet.read(TIMEOUT).await?;
        let observed = inspect_current(observer, id, snapshot.revision()).await?;
        let attestation = capacity_attestation(observed)?;
        match operator
            .fleet
            .authorize_capacity(&snapshot, id, TIMEOUT)
            .await
        {
            Ok(capacity) => ensure!(capacity.attestation().boot == attestation.boot),
            Err(FleetCatalogError::Conflict) => {
                // Retained crash startup roots a new boot before its old
                // attestation can be reused. Qualify the exact new boot.
                let filename = format!("repair-current-capacity-{index}.json");
                persist(root, &filename, &serde_json::to_vec(&attestation)?)?;
                standby::capacity(
                    &cli,
                    &hex::encode(id.bytes()),
                    root.join(filename)
                        .to_str()
                        .context("current capacity attestation path")?,
                    "activate",
                    150 + u8::try_from(index)?,
                    FleetNodeState::Active,
                )
                .await?;
            }
            Err(error) => return Err(error.into()),
        }
        let (owner_check, container) = process
            .voter
            .as_ref()
            .context("unowned repair voter")?
            .fleet_identity()?;
        ensure!(
            owner_check == owner,
            "repair voter belongs to another deployment"
        );
        isolation.bind_node(id, &container, TIMEOUT).await?;
        if index == FAILED {
            failed = Some(attestation.boot);
        }
    }
    Ok(WorkerRequest {
        owner,
        failed: failed.context("failed voter boot binding")?,
    })
}

async fn wait_for_recovery(
    worker: &FleetMovementService,
    ready: impl Fn(&FleetMovementServiceStatus) -> bool,
    timeout: Duration,
) -> Result<FleetMovementServiceStatus> {
    tokio::time::timeout(timeout, async {
        loop {
            let status = worker.status();
            if ready(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| format!("public failure recovery deadline: {:?}", worker.status()))
}

pub(crate) async fn qualify(
    root: &Path,
    client: &reqwest::Client,
    processes: &mut [Process],
    addresses: &[ManagedIngress],
    original: &Value,
    ledger: &mut recovery::Ledger,
) -> Result<(Process, ManagedIngress)> {
    ensure!(
        cfg!(target_os = "linux")
            && docker::enabled()
            && std::env::var_os(RUSTFS_ENDPOINT).is_some(),
        "repair gate requires the dedicated RustFS Docker lab"
    );
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let work = domain
        .run(async {
            let operator = Arc::new(Operator::open(domain.clone(), root)?);
            let mut spare = standby::launch(&operator, root, &addresses[FAILED], ledger).await?;
            let (node, observer) = Box::pin(start_observer(&operator, root)).await?;
            let result = Box::pin(repair(
                &operator, &observer, root, client, processes, addresses, original, &mut spare,
                ledger,
            ))
            .await;
            let joined = node.shutdown(ShutdownMode::Preserve).await;
            ensure!(
                matches!(joined, Ok(NodeOutcome::Stopped(_))),
                "repair observer join: {joined:?}"
            );
            Ok::<_, anyhow::Error>((spare, result?))
        })
        .await;
    domain.shutdown().await;
    work?
}

#[allow(clippy::too_many_arguments)]
async fn repair(
    operator: &Operator,
    observer: &RaftCells,
    root: &Path,
    client: &reqwest::Client,
    processes: &mut [Process],
    addresses: &[ManagedIngress],
    original: &Value,
    spare: &mut Process,
    ledger: &mut recovery::Ledger,
) -> Result<ManagedIngress> {
    let view = observer.observe_node(TIMEOUT).await?;
    ensure!(view.node.groups == 0, "inspection observer owns a voter");
    let selected = operator
        .domain
        .run(
            operator
                .adapter
                .existing_namespace(operator.declaration()?.namespace),
        )
        .await??;
    let directory = RangeDirectory::new(selected, configuration::limits().directory)?;
    let request = bind_live_voters(
        operator,
        observer,
        root,
        &addresses[FAILED],
        processes,
        directory,
    )
    .await?;
    let (leader, _, _) = recovery::observe_leader(client, addresses, None, 1, original).await?;
    let retry = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        + 300_000)
        .to_string();
    let command = format!("{}:1", hex::encode([155; 16]));
    let archived = maintenance::verify_sql(
        client,
        &addresses[leader],
        Ingress::Native,
        &command,
        &retry,
        "s3",
    )
    .await?;
    publish(root, REQUEST, &request)?;
    let ready: WorkerReady = await_worker(root, READY, TIMEOUT * 3).await?;
    let cut = operator.fleet.read(TIMEOUT).await?;
    let worker_node = node(WORKER)?;
    let authorized = operator
        .fleet
        .authorize_host(&cut, worker_node, TIMEOUT)
        .await?;
    let capacity = operator
        .fleet
        .authorize_capacity(&cut, worker_node, TIMEOUT)
        .await?;
    ensure!(
        authorized.state() == FleetNodeState::Active
            && capacity.attestation().boot == ready.origin
            && cut.revision() >= ready.revision
    );
    ledger.record(serde_json::json!({"phase":"repair_worker_ready",
        "member":u64::from(WORKER) + 1, "boot":hex::encode(ready.origin.boot),
        "revision":ready.revision}))?;
    let failed = request.failed;
    let standby = node(standby::INDEX)?;
    let failed_process = &mut processes[FAILED];
    let old_pid = failed_process.pid()?;
    failed_process
        .voter
        .as_ref()
        .context("owned failed voter")?
        .kill_if_running()?;
    failed_process.exited(false).await?;
    failed_process
        .voter
        .as_ref()
        .context("joined failed voter")?
        .killed()?;
    let old_disk = docker::disk(root, FAILED).join("raft");
    let retained = retained_disk::disk_digest(&old_disk)?;
    ledger.record(
        serde_json::json!({"phase":"repair_failure_joined", "member":FAILED + 1,
        "pid":old_pid,"boot":hex::encode(failed.boot),"receipt":archived,"command":command}),
    )?;
    let terminal: Result<Removed, String> = await_worker(root, TERMINAL, TIMEOUT * 17).await?;
    let terminal = terminal.map_err(|error| anyhow::anyhow!("Active repair worker: {error}"))?;
    let cut = operator.fleet.read(TIMEOUT).await?;
    let removed = operator
        .fleet
        .isolated_removal_receipt(&cut, ProposalId::new(terminal.operation)?, TIMEOUT)
        .await?
        .context("rooted isolated removal receipt")?;
    ensure!(
        removed.node() == failed.node
            && removed.applied_revision() == terminal.revision
            && removed.isolation_digest() != [0; 32]
            && removed.candidate_digest() != [0; 32]
    );
    ensure!(
        operator
            .fleet
            .authorize_host(&cut, node(u8::try_from(FAILED)?)?, TIMEOUT)
            .await
            .is_err(),
        "terminally removed voter can still obtain host authority"
    );
    ensure!(
        operator
            .fleet
            .pending_reservations(&cut, TIMEOUT)
            .await?
            .is_empty()
            && operator
                .fleet
                .pending_movements(&cut, TIMEOUT)
                .await?
                .is_empty()
    );
    ensure!(
        retained_disk::disk_digest(&old_disk)? == retained,
        "failed voter engine was reclaimed"
    );
    inspect_current(observer, standby, cut.revision()).await?;
    let replacement = observer
        .observe_peer_fleet_membership(
            standby,
            operator
                .fleet
                .authorize_group(&cut, &history()?, TIMEOUT)
                .await?,
            cut.revision(),
            TIMEOUT,
        )
        .await?;
    let proof = replacement.membership();
    let config = proof.configuration();
    let voters = config
        .voters()
        .iter()
        .map(|member| member.get())
        .collect::<Vec<_>>();
    ensure!(
        proof.replica().member.get() == 4
            && voters == [2, 3, 4]
            && config.learners().is_empty()
            && config.next_learners().is_empty()
            && config.outgoing_voters().is_empty()
    );
    ledger.record(serde_json::json!({"phase":"repair_removed", "member":FAILED + 1,
        "replacement":4,"voters":voters,"revision":removed.applied_revision(),"retained_files":retained.len(),
        "isolation_digest":hex::encode(removed.isolation_digest())}))?;
    let ingress = spare.managed_addresses().await?;
    let replacement_routes = [ingress, addresses[1].clone(), addresses[2].clone()];
    let term =
        maintenance::handoff_to(client, &replacement_routes, &REPAIRED, 0, &archived).await?;
    for adapter in Ingress::ALL {
        let replay = maintenance::verify_sql(
            client,
            &replacement_routes[0],
            adapter,
            &command,
            &retry,
            "s3",
        )
        .await?;
        recovery::matching_receipt(&replay, &archived)?;
        let fresh = format!("{}:2", hex::encode([155; 16]));
        let written = maintenance::verify_sql(
            client,
            &replacement_routes[0],
            adapter,
            &fresh,
            &retry,
            "quorum",
        )
        .await?;
        ensure!(
            written["group"] == archived["group"]
                && written["group_incarnation"] == archived["group_incarnation"]
                && written["sequence"]
                    .as_u64()
                    .context("replacement sequence")?
                    == archived["sequence"]
                        .as_u64()
                        .and_then(|sequence| sequence.checked_add(1))
                        .context("archived sequence")?,
            "replacement lost command history"
        );
        ledger.record(
            serde_json::json!({"phase":"repair_replacement_sql","member":4,"term":term,
            "ingress":adapter.label(),"replayed":replay,"written":written}),
        )?;
    }
    let [ingress, _, _] = replacement_routes;
    Ok(ingress)
}

pub(super) async fn run_worker(
    operator: Arc<Operator>,
    service: RaftCells,
    root: PathBuf,
    directory: RangeDirectory,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let request = cellar::asyncrt::select! {
        _ = stop.changed() => return Ok(()),
        request = async {
            loop {
                if let Some(request) = read(&root, REQUEST)? {
                    return Ok::<WorkerRequest, anyhow::Error>(request);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        } => request?,
    };
    let domain = operator.domain.clone();
    let result = domain
        .run(Box::pin(recover_on_voter(
            &operator, &service, &root, directory, request, stop,
        )))
        .await?;
    let terminal = match result {
        Ok(Some(removed)) => Ok(removed),
        Ok(None) => return Ok(()),
        Err(error) => Err(format!("{error:#}")),
    };
    publish(&root, TERMINAL, &terminal)
}

async fn recover_on_voter(
    operator: &Operator,
    observer: &RaftCells,
    root: &Path,
    directory: RangeDirectory,
    request: WorkerRequest,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<Option<Removed>> {
    let cut = operator.fleet.read(TIMEOUT).await?;
    let origin = node(WORKER)?;
    let authorized = operator.fleet.authorize_host(&cut, origin, TIMEOUT).await?;
    let capacity = operator
        .fleet
        .authorize_capacity(&cut, origin, TIMEOUT)
        .await?;
    let origin = capacity.attestation().boot.clone();
    let view = inspect_current(observer, authorized.node(), cut.revision()).await?;
    ensure!(
        authorized.state() == FleetNodeState::Active
            && view.node.groups == 1
            && view.fleet_boot.as_ref() == Some(&origin),
        "repair worker must own the exact live Active voter boot"
    );
    let bytes = view.node.profile.bytes_per_group;
    let failed = request.failed;
    let platform = FleetDockerIsolation::new(
        operator.fleet.clone(),
        cellar::docker::Docker::new("/run/docker.sock"),
        &request.owner,
        vec![directory],
    )?;
    let policy = FleetDockerResourcePolicy {
        placement: FleetTopologyPolicy {
            revision: 1,
            distinct_zones: false,
            distinct_regions: false,
            max_work: 1_000_000,
        },
        durable_overhead_bytes: DURABLE_OVERHEAD,
        resident: FleetResourceBounds {
            engine_slots: 1,
            durable_bytes: bytes,
            memory_bytes: 128 * MIB,
        },
        peak: FleetResourceBounds {
            engine_slots: 1,
            durable_bytes: bytes + DURABLE_SEED,
            memory_bytes: 256 * MIB,
        },
        reserve: FleetResourceBounds {
            engine_slots: 0,
            durable_bytes: DURABLE_RESERVE,
            memory_bytes: 128 * MIB,
        },
    };
    let resources = Arc::new(FleetDockerResources::new(
        platform.clone(),
        observer.clone(),
        policy.clone(),
    )?);
    let standby = node(standby::INDEX)?;
    // Measure the current receiving target before injecting a fault. The same
    // provider resamples against the current Root when reserving the movement.
    let cut = operator.fleet.read(TIMEOUT).await?;
    let standby_view = inspect_current(observer, standby, cut.revision()).await?;
    let usage = resources.observe(standby, TIMEOUT).await?;
    let qualified = operator
        .fleet
        .authorize_capacity(&cut, standby, TIMEOUT)
        .await?;
    let durable_capacity = standby_view
        .node
        .profile
        .byte_capacity
        .min(qualified.attestation().durable_bytes);
    let durable_required = usage
        .used
        .durable_bytes
        .checked_add(policy.peak.durable_bytes)
        .and_then(|total| total.checked_add(policy.reserve.durable_bytes))
        .context("standby repair durable budget overflow")?;
    ensure!(
        durable_capacity >= durable_required,
        "standby disk budget {durable_capacity} cannot cover repair charge {durable_required}"
    );
    let verifier = open_archive_verifier(operator, &root.join("repair-replay")).await?;
    let worker = operator
        .fleet
        .start_failure_recovery_service(
            observer.clone(),
            resources.clone(),
            resources,
            Arc::new(platform),
            FleetMovementServiceOptions {
                reclaim_removed_members: false,
                interval: Duration::from_millis(50),
                retry_max_interval: Duration::from_millis(500),
                turn_timeout: TIMEOUT,
            },
            FleetFailureRecoveryOptions {
                detection: FleetFailureDetectionOptions::default(),
                archive: verifier,
            },
        )
        .await?;

    let outcome = cellar::asyncrt::select! {
        _ = stop.changed() => Ok(None),
        result = async {
            wait_for_recovery(&worker, |status| status.failure_detection.as_ref().is_some_and(|view|
                view.nodes.iter().any(|item| item.boot == failed && item.health == FleetNodeHealth::Healthy)),
                TIMEOUT * 3).await?;
            publish(root, READY, &WorkerReady { origin, revision: cut.revision() })?;
            let terminal = wait_for_recovery(&worker, |status| status.failure_recovery.as_ref().is_some_and(|view|
                view.phase == FleetFailureRecoveryPhase::Removed), TIMEOUT * 16).await?;
            let recovery = terminal.failure_recovery.context("terminal recovery evidence")?;
            ensure!(recovery.context.boot() == &failed
                && recovery.verified_histories == 1 && recovery.total_histories == 1);
            Ok(Some(Removed {
                operation: recovery.context.operation().bytes(),
                revision: recovery.removed_revision.context("rooted removal revision")?,
            }))
        } => result,
    };
    worker.shutdown().await;
    outcome
}
