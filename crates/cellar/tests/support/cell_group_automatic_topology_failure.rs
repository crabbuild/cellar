// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An automatically admitted movement keeps its debt through fenced recovery.
use super::*;
use cellar::application::consensus::verification::cell_groups::RangeDirectory;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Mutex,
    },
};

#[path = "cell_group_automatic_topology_traffic.rs"]
mod traffic;

struct JoinedBoot {
    boot: FleetBootBinding,
    host: RaftCells,
}

struct Callback(Arc<AtomicUsize>);

impl Drop for Callback {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

// This fixture proves joined local shutdown and rooted restart exclusion, not
// independent-host launch revocation. The failure target is selected only after
// the default planner admits its movement; the test never supplies a move ID.
struct Platform {
    catalog: FleetCatalog,
    directory: RangeDirectory,
    failed: Mutex<Option<JoinedBoot>>,
    hold: AtomicBool,
    active: Arc<AtomicUsize>,
    entered: tokio::sync::Notify,
    contexts: Mutex<Vec<FleetFailureRecoveryContext>>,
}

impl FleetFailureRecoveryPlatform for Platform {
    fn namespaces<'a>(
        &'a self,
        _: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<Vec<RangeDirectory>, FleetCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { Ok(vec![self.directory.clone()]) })
    }

    fn isolate<'a>(
        &'a self,
        context: &'a FleetFailureRecoveryContext,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<FleetIsolationAttestation, FleetCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            {
                let failed = self.failed.lock().unwrap();
                let failed = failed.as_ref().ok_or(FleetCatalogError::Unconfirmed)?;
                if context.boot() != &failed.boot || !failed.host.is_closed() {
                    return Err(FleetCatalogError::Invalid("fixture boot is not joined"));
                }
            }
            let cut = self.catalog.read(timeout).await?;
            let exclusion = self
                .catalog
                .evacuation_admission(&cut, context.operation(), timeout)
                .await?;
            if exclusion.node().bytes() != context.boot().node {
                return Err(FleetCatalogError::Conflict);
            }
            {
                let mut contexts = self.contexts.lock().unwrap();
                if contexts.last().is_some_and(|old| old != context) {
                    return Err(FleetCatalogError::Conflict);
                }
                contexts.push(context.clone());
            }
            self.active.fetch_add(1, Ordering::AcqRel);
            let _callback = Callback(self.active.clone());
            self.entered.notify_one();
            while self.hold.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok(FleetIsolationAttestation {
                boot: context.boot().clone(),
                evidence_digest: [94; 32],
                evidence_reference: "fixture-only/joined-boot-and-rooted-restart-refusal".into(),
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_balance_yields_to_repair_across_controller_restart() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(rooted, Arc::new(catalog_fault::Store::default()))).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_admitted_balance_yields_to_repair_across_controller_restart() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "automatic-admitted-handoff",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

async fn qualify(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let mut first =
        managed_fixture::Fixture::with_store(rooted, managed_fixture::counter_class()?, store)
            .await?;
    // Exactly the existing positive profile, paired with the unchanged refusal
    // case in the parent: two slots, same disk/image/memory and time bounds.
    first.logical_byte_capacity = Some(1024 * 1024 * 1024);
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        true,
        None,
        managed_fixture::FirstGroupSetup::Normal,
    ))
    .await?;
    let mut nodes = std::mem::take(&mut cluster.nodes)
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    let mut workers = Vec::new();
    let mut roots = Vec::new();
    let replay = tempfile::tempdir()?;
    let domain = cluster.fixtures[0].domain.clone();
    let result = domain
        .run(Box::pin(exercise(
            &mut cluster,
            &mut nodes,
            &mut workers,
            &mut roots,
            replay.path(),
            rooted,
        )))
        .await;
    for worker in workers {
        worker.shutdown().await;
    }
    let mut failures = Vec::new();
    for (index, node) in nodes.into_iter().enumerate() {
        if let Some(node) = node {
            if let Err(error) =
                movement::close(vec![node], std::slice::from_ref(&cluster.hosts[index])).await
            {
                failures.push(error.to_string());
            }
        }
    }
    domain.shutdown().await;
    result
        .context("automatic admitted handoff execution domain")?
        .with_context(|| {
            format!("automatic admitted handoff, extents={rooted}; joined={failures:?}")
        })?;
    ensure!(failures.is_empty(), "handoff shutdown: {failures:?}");
    Ok(())
}

async fn stop(cluster: &ManagedCluster, nodes: &mut [Option<Node>], index: usize) -> Result<()> {
    let node = nodes[index].take().context("running voter")?;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    let joined = cluster.hosts[index].close().await;
    ensure!(matches!(stopped?, NodeOutcome::Stopped(_)));
    ensure!(joined?.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
    Ok(())
}

async fn start_controller(
    cluster: &mut ManagedCluster,
    nodes: &mut [Option<Node>],
    index: usize,
    topology: &FleetAutomaticTopologyOptions,
    stopped: Option<usize>,
    nonce: u8,
) -> Result<RaftCells> {
    stop(cluster, nodes, index).await?;
    let fixture = &cluster.fixtures[index];
    let node = managed_fixture::start_node(
        cluster.roots[index].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    // Retain ownership even if configuring or starting the group host fails.
    nodes[index] = Some(node);
    let node = nodes[index].as_ref().context("retained controller")?;
    let (mut config, services) = fixture.config(node, LogOpenMode::Resume, nonce).await?;
    managed_fixture::archives::configure(&mut config);
    config.host.automatic_topology = Some(topology.clone());
    cluster.peers[index] = config.peers[0].clone();
    let host = node.start_raft_cells(config, services).await?;
    cluster.hosts[index] = host.clone();
    managed_fixture::admit_capacity(fixture, &host, &cluster.directory, nonce).await?;
    movement::discover_survivors(cluster, stopped).await?;
    Ok(host)
}

async fn original_debit(
    catalog: &FleetCatalog,
    movement: &FleetPendingMovement,
    debit: &FleetPendingReservation,
) -> Result<()> {
    let cut = catalog.read(TIMEOUT).await?;
    let moves = catalog.pending_movements(&cut, TIMEOUT).await?;
    let debts = catalog.pending_reservations(&cut, TIMEOUT).await?;
    ensure!(
        moves.len() == 1
            && moves[0].reservation == movement.reservation
            && moves[0].group == movement.group
            && moves[0].plan == movement.plan,
        "automatic handoff replaced admitted movement"
    );
    ensure!(
        debts.len() == 1
            && debts[0].reservation == debit.reservation
            && debts[0].boot == debit.boot
            && debts[0].peak == debit.peak
            && debts[0].executable_movement,
        "automatic handoff lost original resource debit"
    );
    Ok(())
}

async fn exercise(
    cluster: &mut ManagedCluster,
    nodes: &mut [Option<Node>],
    workers: &mut Vec<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
    replay: &Path,
    rooted: bool,
) -> Result<()> {
    let PreparedGroups {
        seed,
        histories,
        observer,
    } = prepare_groups(cluster, rooted, workers, roots).await?;
    let directory = cluster.directory.observe().await?;
    let platform = Arc::new(Platform {
        catalog: observer.clone(),
        directory: cluster.directory.clone(),
        failed: Mutex::new(None),
        hold: AtomicBool::new(true),
        active: Arc::new(AtomicUsize::new(0)),
        entered: tokio::sync::Notify::new(),
        contexts: Mutex::new(Vec::new()),
    });
    let verifier =
        managed_fixture::verifier::open(&cluster.fixtures[0], replay, rooted, Default::default())
            .await?;
    let profile = cluster.hosts[3].observe_node(TIMEOUT).await?.node.profile;
    let mut topology = configuration(profile.bytes_per_group);
    topology.recovery = Some(FleetAutomaticRecoveryOptions {
        platform: platform.clone(),
        recovery: FleetFailureRecoveryOptions {
            detection: Default::default(),
            archive: verifier,
        },
    });
    let store = cluster.fixtures[0].store.clone();
    store
        .deny_group_payload_reads
        .store(true, Ordering::Release);
    let mut controller = 3;
    let host = start_controller(cluster, nodes, controller, &topology, None, 180).await?;
    let (movement, debit, operation, obsolete) =
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let cut = observer.read(TIMEOUT).await?;
                let pending = observer.pending_movements(&cut, TIMEOUT).await?;
                let debts = observer.pending_reservations(&cut, TIMEOUT).await?;
                if pending.len() == 1
                    && debts.len() == 1
                    && store.denied_group_payload_reads.load(Ordering::Acquire) > 0
                {
                    let movement = &pending[0];
                    let birth = observer
                        .history(&cut, &movement.group, TIMEOUT)
                        .await?
                        .initial_placement()?;
                    let binding = birth
                        .voters
                        .iter()
                        .find(|voter| voter.member == movement.plan.obsolete)
                        .context("obsolete birth voter")?;
                    let obsolete = (3..6)
                        .find(|index| cluster.fixtures[*index].spec.node == binding.node)
                        .context("obsolete source voter")?;
                    let source = (3..6)
                        .find(|index| *index != obsolete)
                        .context("surviving source")?;
                    let proof = cluster.hosts[source]
                        .group(movement.group)?
                        .quorum_membership(TIMEOUT)
                        .await?;
                    let configuration = proof.configuration();
                    if configuration.voters.contains(&movement.plan.obsolete.get())
                        && configuration
                            .learners
                            .contains(&movement.plan.replacement.member.get())
                        && configuration.voters_outgoing.is_empty()
                    {
                        let operation = observer
                            .active_topology_operation(&cut, TIMEOUT)
                            .await?
                            .context("automatic intent")?;
                        ensure!(
                            operation.phase == FleetTopologyPhase::Running
                                && operation.admitted_movements == 1
                        );
                        ensure!(
                            debts[0].reservation == movement.reservation
                                && debts[0].executable_movement
                        );
                        return Ok::<_, anyhow::Error>((
                            movement.clone(),
                            debts[0].clone(),
                            operation.identity.operation,
                            obsolete,
                        ));
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("automatic admission and actual blocked learner seeding")??;
    // The controller must remain a source voter, never a restarted Seeding
    // learner. Move controller ownership before failing it if the planner chose
    // that source as obsolete; the generated operation is still the same one.
    if obsolete == controller {
        host.suspend_automatic_topology().await;
        controller = 4;
        start_controller(cluster, nodes, controller, &topology, None, 184).await?;
    }
    let ingress = (3..6)
        .find(|index| *index != obsolete && *index != controller)
        .context("stable source ingress")?;
    authorize_ingress(cluster, ingress, None).await?;
    original_debit(&observer, &movement, &debit).await?;
    let load = traffic::Traffic::default();
    let ingress_host = cluster.hosts[ingress].clone();
    let (drive, requests) = tokio::join!(
        async {
            let result = drive_failure(
                cluster, nodes, controller, obsolete, &topology, &platform, &observer, &movement,
                &debit, operation, &load,
            )
            .await;
            load.stop.store(true, Ordering::Release);
            result
        },
        async {
            let result = load.run(ingress_host, &seed).await;
            if result.is_err() {
                load.stop.store(true, Ordering::Release);
            }
            result
        }
    );
    let receipts = match (drive, requests) {
        (Ok(()), Ok(receipts)) => receipts,
        (Err(drive), Err(requests)) => {
            anyhow::bail!("fault drive: {drive:#}; foreground requests: {requests:#}")
        }
        (Err(error), _) | (_, Err(error)) => return Err(error),
    };
    load.audit(&cluster.hosts[ingress], &seed, &receipts)
        .await?;
    let host = cluster.hosts[controller].clone();
    host.suspend_automatic_topology().await;
    ensure!(platform.active.load(Ordering::Acquire) == 0);
    let cut = observer.read(TIMEOUT).await?;
    let operation = observer
        .topology_operation(&cut, ProposalId::new(operation)?, TIMEOUT)
        .await?;
    ensure!(operation.phase == FleetTopologyPhase::Cancelled && operation.admitted_movements == 1);
    ensure!(
        observer.pending_movements(&cut, TIMEOUT).await?.is_empty()
            && observer
                .pending_reservations(&cut, TIMEOUT)
                .await?
                .is_empty()
    );
    ensure!(cluster.directory.observe().await?.digest() == directory.digest());
    for history in histories {
        // The ingress may itself have moved out during the ordinary balance.
        // SDK reads/writes above must still work there; physical membership is
        // independently observed on an actual remaining replica.
        let mut observed = None;
        for owner in cluster.hosts.iter().filter(|host| !host.is_closed()) {
            let Ok(group) = owner.group(history) else {
                continue;
            };
            if let Ok(proof) = group.quorum_membership(TIMEOUT).await {
                observed = Some(proof);
                break;
            }
        }
        let proof = observed.context("no fresh quorum after automatic repair and balance")?;
        let configuration = proof.configuration();
        ensure!(
            configuration.voters.len() == 3
                && configuration.voters_outgoing.is_empty()
                && configuration.learners.is_empty()
        );
        let birth = observer
            .history(&cut, &history, TIMEOUT)
            .await?
            .initial_placement()?;
        let mut voters = BTreeSet::new();
        for member in &configuration.voters {
            let member = MemberId::new(*member)?;
            let node = match proof.membership().joined(member)? {
                Some((joined, _)) => joined.node,
                None => {
                    birth
                        .voters
                        .iter()
                        .find(|v| v.member == member)
                        .context("birth binding")?
                        .node
                }
            };
            ensure!(node != cluster.fixtures[obsolete].spec.node);
            ensure!(voters.insert(node), "RF3 voters share a physical node");
        }
        if history == movement.group {
            ensure!(
                configuration
                    .voters
                    .contains(&movement.plan.replacement.member.get())
                    && voters.contains(&movement.plan.replacement.node),
                "original admitted learner was replaced"
            );
        }
    }
    eprintln!("automatic handoff: extents={rooted}, admitted learner/debit retained across voter loss and controller reboot, exact-boot fence replayed, both groups RF3, mixed SQL/receipts preserved");
    Ok(())
}

async fn authorize_ingress(
    cluster: &ManagedCluster,
    ingress: usize,
    stopped: Option<usize>,
) -> Result<()> {
    let revision = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
    for (index, destination) in cluster.hosts.iter().enumerate() {
        if stopped == Some(index) || index == ingress {
            continue;
        }
        managed_fixture::inspect_current(destination, cluster.peers[ingress].node, revision)
            .await?;
        destination.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision,
            peer: cluster.peers[ingress].clone(),
        })?;
        destination.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision,
            peer: cluster.peers[ingress].clone(),
        })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn drive_failure(
    cluster: &mut ManagedCluster,
    nodes: &mut [Option<Node>],
    controller: usize,
    obsolete: usize,
    topology: &FleetAutomaticTopologyOptions,
    platform: &Platform,
    observer: &FleetCatalog,
    movement: &FleetPendingMovement,
    debit: &FleetPendingReservation,
    operation: [u8; 16],
    load: &traffic::Traffic,
) -> Result<()> {
    load.wait_phase(0).await?;
    let boot = cluster.hosts[obsolete]
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("obsolete boot")?;
    stop(cluster, nodes, obsolete).await?;
    let disk = cluster.roots[obsolete].path().join("raft");
    let retained = disk_digest(&disk)?;
    *platform.failed.lock().unwrap() = Some(JoinedBoot {
        boot: boot.clone(),
        host: cluster.hosts[obsolete].clone(),
    });
    load.phase.store(1, Ordering::Release);
    tokio::time::timeout(TIMEOUT * 16, platform.entered.notified())
        .await
        .context("automatic isolation of admitted-move voter")?;
    original_debit(observer, movement, debit).await?;
    let cut = observer.read(TIMEOUT).await?;
    ensure!(
        observer
            .topology_operation(&cut, ProposalId::new(operation)?, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelling
    );
    load.wait_phase(1).await?;
    cluster.hosts[controller].suspend_automatic_topology().await;
    ensure!(
        platform.active.load(Ordering::Acquire) == 0,
        "suspend did not join platform callback"
    );
    let context = platform
        .contexts
        .lock()
        .unwrap()
        .last()
        .cloned()
        .context("durable isolation context")?;
    ensure!(context.boot() == &boot);
    let calls = platform.contexts.lock().unwrap().len();
    let old_controller_boot = cluster.hosts[controller]
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("controller boot before restart")?;
    let ingress = (3..6)
        .find(|index| *index != obsolete && *index != controller)
        .context("surviving ingress")?;
    let host = start_controller(cluster, nodes, controller, topology, Some(obsolete), 188).await?;
    ensure!(
        host.observe_node(TIMEOUT)
            .await?
            .fleet_boot
            .context("restarted controller boot")?
            != old_controller_boot
    );
    authorize_ingress(cluster, ingress, Some(obsolete)).await?;
    tokio::time::timeout(TIMEOUT * 16, async {
        while platform.contexts.lock().unwrap().len() <= calls {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("replay isolation after controller reboot")?;
    ensure!(
        platform
            .contexts
            .lock()
            .unwrap()
            .iter()
            .all(|seen| seen == &context),
        "controller reboot changed isolation identity"
    );
    original_debit(observer, movement, debit).await?;
    load.phase.store(2, Ordering::Release);
    load.wait_phase(2).await?;
    cluster.fixtures[0]
        .store
        .deny_group_payload_reads
        .store(false, Ordering::Release);
    platform.hold.store(false, Ordering::Release);
    tokio::time::timeout(TIMEOUT * 16, async {
        loop {
            let status = host
                .automatic_topology_status()
                .context("managed recovery owner")?;
            if status.failure_recovery.as_ref().is_some_and(|recovery| {
                recovery.phase == FleetFailureRecoveryPhase::Removed
                    && recovery.removed_revision.is_some()
            }) && status.work.is_empty()
                && status.automatic_topology.as_ref().is_some_and(|automatic| {
                    automatic.wait == FleetAutomaticTopologyWait::Unchanged
                })
            {
                // Removing a failed node can select another ordinary balance.
                // Let the configured owner settle that work too, then join its
                // idle state before checking the complete resource book.
                host.suspend_automatic_topology().await;
                let cut = observer.read(TIMEOUT).await?;
                let receipt = observer
                    .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
                    .await?
                    .context("terminal failed-boot removal")?;
                ensure!(
                    receipt.node() == boot.node
                        && observer
                            .authorize_host(&cut, cluster.fixtures[obsolete].spec.node, TIMEOUT)
                            .await
                            .is_err()
                );
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "finish original movement and managed repair: {:?}",
            host.automatic_topology_status()
        )
    })??;
    ensure!(
        disk_digest(&disk)? == retained,
        "failed voter disk was reclaimed"
    );
    load.phase.store(3, Ordering::Release);
    load.wait_phase(3).await?;
    Ok(())
}

fn disk_digest(root: &Path) -> Result<[u8; 32]> {
    fn walk(root: &Path, here: &Path, hash: &mut Sha256) -> Result<()> {
        let mut paths = std::fs::read_dir(here)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.sort();
        for path in paths {
            if path.is_dir() {
                walk(root, &path, hash)?;
            } else {
                hash.update(path.strip_prefix(root)?.as_os_str().as_encoded_bytes());
                hash.update(std::fs::read(path)?);
            }
        }
        Ok(())
    }
    let mut hash = Sha256::new();
    walk(root, root, &mut hash)?;
    Ok(hash.finalize().into())
}
