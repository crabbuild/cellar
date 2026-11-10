// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configured repair uses declared domains and real RF3/S3 owners. The joined
//! fixture fence is not an independent-host or Docker launch-revocation proof.
use super::*;
use cellar::application::consensus::verification::cell_groups::RangeDirectory;
use cellar::sdk::{Node, NodeOutcome, ShutdownMode};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, Mutex,
    },
};

type CatalogResult<T> = std::result::Result<T, FleetCatalogError>;

struct Callback(Arc<AtomicUsize>);

impl Drop for Callback {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Platform {
    catalog: FleetCatalog,
    directory: RangeDirectory,
    boot: FleetBootBinding,
    stopped: RaftCells,
    hold: AtomicBool,
    active: Arc<AtomicUsize>,
    entered: tokio::sync::Notify,
    contexts: Mutex<Vec<FleetFailureRecoveryContext>>,
}

impl FleetFailureRecoveryPlatform for Platform {
    fn namespaces<'a>(
        &'a self,
        _: Duration,
    ) -> Pin<Box<dyn Future<Output = CatalogResult<Vec<RangeDirectory>>> + Send + 'a>> {
        Box::pin(async move { Ok(vec![self.directory.clone()]) })
    }

    fn isolate<'a>(
        &'a self,
        context: &'a FleetFailureRecoveryContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = CatalogResult<FleetIsolationAttestation>> + Send + 'a>> {
        Box::pin(async move {
            if context.boot() != &self.boot || !self.stopped.is_closed() {
                return Err(FleetCatalogError::Invalid("fixture boot is not joined"));
            }
            let cut = self.catalog.read(timeout).await?;
            let exclusion = self
                .catalog
                .evacuation_admission(&cut, context.operation(), timeout)
                .await?;
            if exclusion.node().bytes() != self.boot.node {
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
                boot: self.boot.clone(),
                evidence_digest: [93; 32],
                evidence_reference: "fixture-only/joined-boot-and-rooted-restart-refusal".into(),
            })
        })
    }
}

pub async fn qualify(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            "off,cellar::raft::fleet_catalog::resources::execution::service::worker=debug",
        )
        .with_ansi(false)
        .with_test_writer()
        .try_init();
    let mut first =
        managed_fixture::Fixture::with_store(rooted, managed_fixture::counter_class()?, store)
            .await?;
    // Spares 7 and 8 share the surviving voters' domains. Only 9 can restore
    // separation after node 4 fails. Physical limits remain the normal fixture.
    first.placement_domains = Some(
        [4u8, 5, 6, 7, 8, 9]
            .into_iter()
            .map(|id| {
                let domain = match id {
                    7 => 5,
                    8 => 6,
                    _ => id,
                };
                Ok((
                    NodeId::new([id; 16])?,
                    (format!("region-{domain}"), format!("zone-{domain}")),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?,
    );
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[7, 8, 9],
        managed_fixture::ArchiveSetup::Initialized,
        true,
        None,
        managed_fixture::FirstGroupSetup::Normal,
    ))
    .await?;
    let mut nodes = std::mem::take(&mut cluster.nodes)
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    let replay = tempfile::tempdir()?;
    let domain = cluster.fixtures[0].domain.clone();
    let result = domain
        .run(Box::pin(exercise(
            &mut cluster,
            &mut nodes,
            replay.path(),
            rooted,
        )))
        .await;
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
    result.context("automatic repair execution domain")??;
    ensure!(failures.is_empty(), "repair shutdown: {failures:?}");
    Ok(())
}

async fn stop(cluster: &ManagedCluster, nodes: &mut [Option<Node>], index: usize) -> Result<()> {
    let node = nodes[index].take().context("running node")?;
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = cluster.hosts[index].close().await?;
    ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
    Ok(())
}

async fn wait(
    host: &RaftCells,
    ready: impl Fn(&FleetMovementServiceStatus) -> bool,
) -> Result<FleetMovementServiceStatus> {
    tokio::time::timeout(TIMEOUT * 16, async {
        loop {
            let status = host
                .automatic_topology_status()
                .context("configured repair owner")?;
            ensure!(status.running, "repair owner stopped: {status:?}");
            if ready(&status) {
                return Ok::<_, anyhow::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "configured repair deadline: {:?}",
            host.automatic_topology_status()
        )
    })?
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

async fn exercise(
    cluster: &mut ManagedCluster,
    nodes: &mut [Option<Node>],
    replay: &Path,
    rooted: bool,
) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    movement::discover(cluster).await?;
    let directory = cluster.directory.observe().await?;
    let seed_owner = leader(&cluster.hosts[..3], cluster.history).await?;
    for (i, original) in seed.receipts.iter().enumerate() {
        let handle = cluster.hosts[seed_owner].cell(seed.cells[i].clone())?;
        let mut options = CommandOptions::new(original.id, seed.retry);
        options.durability = Durability::S3;
        let receipt = handle
            .command(
                &handle.contract("Add")?,
                [7i64, 11][i].to_be_bytes().to_vec(),
                options,
            )
            .await
            .context("establish archived receipt coverage on the current leader")?;
        ensure!(
            receipt.result == original.result
                && receipt.version == original.version
                && receipt.durability == Durability::S3
        );
    }
    let failed_boot = cluster.hosts[0]
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("failed boot")?;
    let platform = Arc::new(Platform {
        catalog: cluster.fixtures[0].catalog.clone(),
        directory: cluster.directory.clone(),
        boot: failed_boot.clone(),
        stopped: cluster.hosts[0].clone(),
        hold: AtomicBool::new(true),
        active: Arc::new(AtomicUsize::new(0)),
        entered: tokio::sync::Notify::new(),
        contexts: Mutex::new(Vec::new()),
    });
    let verifier =
        managed_fixture::verifier::open(&cluster.fixtures[0], replay, rooted, Default::default())
            .await?;
    let profile = cluster.hosts[5].observe_node(TIMEOUT).await?.node.profile;
    let bound = FleetResourceBounds {
        engine_slots: 1,
        durable_bytes: profile.bytes_per_group,
        memory_bytes: 8 * 1024 * 1024,
    };
    let reserve = FleetResourceBounds {
        engine_slots: 1,
        durable_bytes: 32 * 1024 * 1024,
        memory_bytes: 8 * 1024 * 1024,
    };
    stop(cluster, nodes, 5).await?;
    let fixture = &cluster.fixtures[5];
    let node = managed_fixture::start_node(
        cluster.roots[5].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 91).await?;
    managed_fixture::archives::configure(&mut config);
    config.host.automatic_topology = Some(FleetAutomaticTopologyOptions {
        principal: "automatic-repair-test".into(),
        policy: FleetTopologyPolicy {
            revision: 1,
            distinct_zones: true,
            distinct_regions: true,
            max_work: 12,
        },
        resident: bound,
        peak: bound,
        reserve,
        movement: FleetMovementServiceOptions {
            reclaim_removed_members: true,
            interval: Duration::from_millis(50),
            retry_max_interval: Duration::from_millis(250),
            turn_timeout: TIMEOUT,
        },
        collection: FleetTopologyCollectionOptions {
            interval: Duration::from_millis(50),
            max_histories_per_round: 2,
            quantum: Duration::from_millis(100),
        },
        recovery: Some(FleetAutomaticRecoveryOptions {
            platform: platform.clone(),
            recovery: FleetFailureRecoveryOptions {
                detection: Default::default(),
                archive: verifier,
            },
        }),
    });
    cluster.peers[5] = config.peers[0].clone();
    let host = node.start_raft_cells(config, services).await?;
    cluster.hosts[5] = host.clone();
    nodes[5] = Some(node);
    managed_fixture::admit_capacity(fixture, &host, &cluster.directory, 170).await?;
    let status = wait(&host, |status| {
        status.topology.as_ref().is_some_and(|topology| {
            topology.phase == FleetTopologyPhase::Running && topology.admitted_movements == 0
        }) && status.last_round_failure == Some(FleetMovementServiceFailure::Rejected)
    })
    .await?;
    let balancing = status
        .topology
        .context("bounded automatic intent")?
        .operation;
    stop(cluster, nodes, 0).await?;
    let disk = cluster.roots[0].path().join("raft");
    let retained = disk_digest(&disk)?;
    tokio::time::timeout(TIMEOUT * 16, platform.entered.notified())
        .await
        .context("automatic exact-boot isolation")?;
    host.suspend_automatic_topology().await;
    ensure!(
        platform.active.load(Ordering::Acquire) == 0,
        "suspend did not join isolation"
    );
    let context = platform
        .contexts
        .lock()
        .unwrap()
        .last()
        .cloned()
        .context("persisted failure context")?;
    ensure!(context.boot() == &failed_boot);
    let catalog = &cluster.fixtures[0].catalog;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(
        catalog
            .topology_operation(&cut, ProposalId::new(balancing)?, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Cancelled
    );
    ensure!(catalog
        .active_topology_operation(&cut, TIMEOUT)
        .await?
        .is_none());
    ensure!(
        catalog.pending_movements(&cut, TIMEOUT).await?.is_empty(),
        "repair ran before isolation proof"
    );
    // A second lost domain before repair leaves one original voter. A timeout
    // is ambiguous; retain and reconcile this exact zero-delta mutation later.
    stop(cluster, nodes, 1).await?;
    platform.hold.store(false, Ordering::Release);
    host.resume_automatic_topology().await?;
    wait(&host, |status| {
        status
            .failure_recovery
            .as_ref()
            .is_some_and(|recovery| recovery.phase == FleetFailureRecoveryPhase::Repairing)
            && status.isolated_repair.as_ref().is_some_and(|repair| {
                repair.blocked_histories > 0 && repair.last_blocked_group == Some(cluster.history)
            })
    })
    .await?;
    let minority = cluster.hosts[2].cell(seed.cells[0].clone())?;
    let command = CommandId::new(ClientId::new([242; 16])?, 1)?;
    let refused = minority
        .command(
            &minority.contract("Add")?,
            0i64.to_be_bytes().to_vec(),
            CommandOptions::new(command, seed.retry),
        )
        .await;
    ensure!(
        matches!(refused, Err(error) if error.command == command),
        "second-domain loss acknowledged a quorum write"
    );
    let failed_read = minority
        .query(
            ProposalId::new([243; 16])?,
            &minority.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await;
    ensure!(failed_read.is_err(), "minority served a linearizable read");
    host.suspend_automatic_topology().await;
    let isolation_calls = platform.contexts.lock().unwrap().len();
    ensure!(platform.active.load(Ordering::Acquire) == 0);
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(
        catalog.pending_movements(&cut, TIMEOUT).await?.is_empty()
            && catalog
                .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
                .await?
                .is_none()
            && host.observe_node(TIMEOUT).await?.node.groups == 0,
        "active repair fabricated a replacement without quorum"
    );
    eprintln!("automatic repair: extents={rooted}, active controller refused replacement and strong requests without quorum");
    let fixture = &cluster.fixtures[1];
    let node = managed_fixture::start_node(
        cluster.roots[1].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 92).await?;
    managed_fixture::archives::configure(&mut config);
    cluster.peers[1] = config.peers[0].clone();
    cluster.hosts[1] = node.start_raft_cells(config, services).await?;
    nodes[1] = Some(node);
    managed_fixture::admit_capacity(fixture, &cluster.hosts[1], &cluster.directory, 175).await?;
    movement::discover_survivors(cluster, Some(0)).await?;
    let revision = catalog.read(TIMEOUT).await?.revision();
    for index in [1, 5] {
        let target = &cluster.hosts[index];
        managed_fixture::inspect_current(target, cluster.peers[2].node, revision).await?;
        target.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision,
            peer: cluster.peers[2].clone(),
        })?;
        target.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision,
            peer: cluster.peers[2].clone(),
        })?;
    }
    let original_membership = cluster.hosts[2]
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    ensure!(
        original_membership.configuration().voters.len() == 3
            && original_membership.configuration().voters.contains(&1),
        "endpoint omission changed the durable voting configuration"
    );
    // The proof owns a runtime read pin. Settlement must observe its release,
    // rather than retaining this test observation throughout the repair.
    drop(original_membership);
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let recovered = tokio::time::timeout_at(deadline, async {
        loop {
            let mut options = CommandOptions::new(command, seed.retry);
            options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            match minority
                .command(
                    &minority.contract("Add")?,
                    0i64.to_be_bytes().to_vec(),
                    options,
                )
                .await
            {
                Err(error)
                    if matches!(
                        error.cause.as_ref(),
                        CellDispatchError::Execution(
                            cellar::application::consensus::verification::ExecutionError::Busy
                        )
                    ) =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                result => return Ok::<_, anyhow::Error>(result?),
            }
        }
    })
    .await
    .context("reconcile original minority command")??;
    ensure!(recovered.id == command && recovered.result == seed.receipts[0].result);
    host.resume_automatic_topology().await?;
    let terminal = wait(&host, |status| {
        status.failure_recovery.as_ref().is_some_and(|recovery| {
            recovery.phase == FleetFailureRecoveryPhase::Removed
                && recovery.removed_revision.is_some()
        })
    })
    .await;
    if terminal.is_err() {
        let cut = catalog.read(TIMEOUT).await?;
        for index in [1, 2, 5] {
            let physical = host
                .inspect_peer_physical_inventory(
                    cluster.fixtures[index].spec.node,
                    cut.revision(),
                    TIMEOUT,
                )
                .await
                .map(|sample| {
                    let inventory = sample.inventory.as_ref();
                    let group = inventory.and_then(|i| i.group_history(&cluster.history));
                    format!(
                        "trust={:?} seed={} pending={} initializing={:?} group={:?}",
                        sample.view.fleet_trust_revision,
                        sample.view.seed_workers,
                        sample.view.pending_history_registrations,
                        inventory.and_then(|i| i.initializing()),
                        group.map(|g| (g.member(), g.completed_sequence(), g.in_configuration())),
                    )
                });
            eprintln!("automatic repair physical node {index}: {physical:?}");
        }
    }
    let terminal = terminal?;
    ensure!(
        terminal
            .failure_recovery
            .context("terminal repair")?
            .context
            == context
    );
    host.suspend_automatic_topology().await;
    ensure!(platform.active.load(Ordering::Acquire) == 0);
    ensure!(
        platform.contexts.lock().unwrap().len() == isolation_calls,
        "controller replacement repeated committed platform isolation"
    );
    ensure!(platform
        .contexts
        .lock()
        .unwrap()
        .iter()
        .all(|seen| seen == &context));
    let cut = catalog.read(TIMEOUT).await?;
    let removed = catalog
        .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
        .await?
        .context("terminal removal receipt")?;
    ensure!(
        removed.node() == failed_boot.node
            && catalog
                .authorize_host(&cut, cluster.fixtures[0].spec.node, TIMEOUT)
                .await
                .is_err()
    );
    ensure!(
        catalog.pending_movements(&cut, TIMEOUT).await?.is_empty()
            && catalog
                .pending_reservations(&cut, TIMEOUT)
                .await?
                .is_empty()
    );
    ensure!(
        disk_digest(&disk)? == retained,
        "repair reclaimed the fenced disk"
    );
    ensure!(cluster.directory.observe().await?.digest() == directory.digest());
    let proof = host
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let configuration = proof.configuration();
    ensure!(
        configuration.voters.len() == 3
            && configuration.voters_outgoing.is_empty()
            && configuration.learners.is_empty()
    );
    ensure!(
        !configuration.voters.contains(&1)
            && configuration.voters.contains(&2)
            && configuration.voters.contains(&3)
    );
    for index in [3, 4] {
        ensure!(
            cluster.hosts[index]
                .observe_node(TIMEOUT)
                .await?
                .node
                .groups
                == 0,
            "repair selected a co-located spare"
        );
    }
    let installed = host.observe_node(TIMEOUT).await?;
    ensure!(
        installed.node.groups == 1
            && installed.node.profile.group_capacity == 2
            && installed.node.profile.byte_capacity == profile.byte_capacity
    );
    ensure!(installed.node.profile.group_capacity - installed.node.groups >= reserve.engine_slots);
    ensure!(
        installed
            .node
            .reserved_bytes
            .checked_add(reserve.durable_bytes)
            .context("remaining byte reserve")?
            <= installed.node.profile.byte_capacity
    );
    for (i, cell) in seed.cells.iter().enumerate() {
        let cell = cluster.hosts[2].cell(cell.clone())?;
        let replay = cell
            .command(
                &cell.contract("Add")?,
                [7i64, 11][i].to_be_bytes().to_vec(),
                CommandOptions::new(seed.receipts[i].id, seed.retry),
            )
            .await?;
        ensure!(
            replay.result == seed.receipts[i].result
                && replay.id == seed.receipts[i].id
                && replay.version.covers(&seed.receipts[i].version) == Ok(true)
        );
        let read = cell
            .query(
                ProposalId::new([244 + i as u8; 16])?,
                &cell.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == seed.receipts[i].result);
        let fresh = cell
            .command(
                &cell.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([245; 16])?, 1)?, seed.retry),
            )
            .await?;
        ensure!(
            fresh.durability == Durability::Quorum
                && fresh.result == ([7i64, 11][i] + 1).to_be_bytes()
        );
    }
    eprintln!("automatic repair: extents={rooted}, cancelled bounded balance, exact-boot fence resumed, safe RF3 restored with reserve, minority refused, SQL/receipts/quorum preserved");
    Ok(())
}
