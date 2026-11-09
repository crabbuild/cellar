// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configured placement moves existing shared groups, retaining every cell.
use super::*;
use cellar::application::consensus::verification::FleetTable;
use cellar_logic::consensus::{MemberId, ProposalId};
use managed_fixture::SeededCells;

#[path = "cell_group_automatic_topology_failure.rs"]
mod failure;

#[derive(Clone, Copy)]
enum CapacityProfile {
    Original,
    RecoveryRoom,
}

fn configuration(durable_bytes: u64) -> FleetAutomaticTopologyOptions {
    let resident = FleetResourceBounds {
        engine_slots: 1,
        durable_bytes,
        memory_bytes: 8 * 1024 * 1024,
    };
    FleetAutomaticTopologyOptions {
        principal: "automatic-topology-test".into(),
        policy: FleetTopologyPolicy {
            revision: 1,
            distinct_zones: false,
            distinct_regions: false,
            max_work: 1_000_000,
        },
        resident,
        peak: resident,
        // The existing two-slot fixture has no additional per-node slack once
        // two groups are balanced over six nodes. The unchanged planner still
        // proves a full RF3 restoration after every kept-node loss.
        reserve: FleetResourceBounds::default(),
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
        recovery: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_topology_balances_groups_and_resumes() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            CapacityProfile::RecoveryRoom,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_configured_topology_balances_groups_and_resumes() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "automatic-topology",
                rooted,
            )?)),
            CapacityProfile::RecoveryRoom,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_topology_preserves_recovery_room() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            CapacityProfile::Original,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_configured_topology_preserves_recovery_room() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "automatic-topology-reserve",
                rooted,
            )?)),
            CapacityProfile::Original,
        ))
        .await?;
    }
    Ok(())
}

async fn qualify(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
    profile: CapacityProfile,
) -> Result<()> {
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
    if matches!(profile, CapacityProfile::RecoveryRoom) {
        // Declare recovery headroom within the SAME existing 1-GiB deployment
        // attestation, two engine slots and image/memory bounds. Original 512-MiB
        // logical admission is separately required to refuse the same topology.
        first.logical_byte_capacity = Some(1024 * 1024 * 1024);
    }
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        true,
        None,
        managed_fixture::FirstGroupSetup::Normal,
    ))
    .await?;
    let mut workers: Vec<RaftFleetTransitionService> = Vec::new();
    let mut roots = Vec::new();
    let domain = cluster.fixtures[0].domain.clone();
    let result = domain
        .run(Box::pin(exercise(
            &mut cluster,
            rooted,
            profile,
            &mut workers,
            &mut roots,
        )))
        .await;
    for worker in workers {
        worker.shutdown().await;
    }
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result
        .context("scenario execution domain")?
        .with_context(|| {
            format!("configured whole-group balance, extents={rooted}; joined={joined:?}")
        })?;
    joined?;
    Ok(())
}

struct PreparedGroups {
    seed: SeededCells,
    histories: BTreeSet<GroupHistory>,
    observer: FleetCatalog,
}

async fn prepare_groups(
    cluster: &ManagedCluster,
    rooted: bool,
    workers: &mut Vec<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<PreparedGroups> {
    let seed = seed_cells(&cluster.hosts, cluster.history)
        .await
        .context("seed cells before balance")?;
    let endpoint = Box::pin(control(cluster, rooted, roots)).await?;
    let (request, admitted) = admit_split(cluster, &endpoint, [3, 3])
        .await
        .context("admit preparation split")?;
    for (i, host) in cluster.hosts.iter().enumerate() {
        let worker = Box::pin(control(cluster, rooted, roots)).await?;
        workers.push(
            worker
                .start_transition_service(host, options(i == 3, true, true), TIMEOUT)
                .await
                .with_context(|| format!("start setup worker {i}"))?,
        );
    }
    let observer = managed_fixture::Fixture::open_catalog(
        &cluster.fixtures[0].domain,
        cluster.fixtures[0].store.clone(),
        cluster.history.cluster,
    )?;
    let cut = observer.read(TIMEOUT).await?;
    endpoint
        .authorize_transition_execution(
            "automatic-test",
            &RaftFleetTransitionExecutionRequest {
                catalog_id: endpoint.catalog_id().into(),
                namespace_id: request.namespace_id.clone(),
                operation_id: request.operation_id.clone(),
                request_id: hex::encode([236; 16]),
                expected_revision: cut.revision(),
                plan_digest: admitted.plan_digest,
            },
            TIMEOUT,
        )
        .await
        .context("authorize preparation split")?;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = endpoint
                .transition_status(&request.namespace_id, &request.operation_id, TIMEOUT)
                .await?;
            if status.phase == Some(TransitionPhase::Retired) && !status.fleet_slot_held {
                return Ok::<_, anyhow::Error>(());
            }
            ensure!(
                workers.iter().all(|w| !w.status().needs_operator),
                "transition worker needs operator"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("prepare two occupied groups")??;
    for worker in workers.drain(..) {
        worker.shutdown().await;
    }
    let directory = cluster.directory.observe().await?;
    ensure!(directory.groups() == 2);
    let mut histories = BTreeSet::new();
    for cell in &seed.cells {
        histories.insert(
            cluster
                .directory
                .lookup(
                    &directory,
                    &cellar_logic::cell_group::CellId::new(cell.cell.as_str())?,
                )
                .await?
                .history,
        );
    }
    ensure!(histories.len() == 2);
    let cut = observer.read(TIMEOUT).await?;
    for owner in &cluster.hosts {
        let profile = owner.observe_node(TIMEOUT).await?.node.profile;
        let floor = observer
            .get(
                &cut,
                FleetTable::Operations,
                &format!(
                    "resource-floor/{}",
                    hex::encode(owner.identity().node.bytes())
                ),
                TIMEOUT,
            )
            .await?;
        let floor = floor
            .map(|bytes| settlement::record(&bytes, 64))
            .transpose()?;
        eprintln!(
            "automatic topology capacity: slots={}, bytes={}, floor={:?}",
            profile.group_capacity,
            profile.byte_capacity,
            floor.as_ref().map(|v| &v["physical"])
        );
    }
    drop(cut);

    Ok(PreparedGroups {
        seed,
        histories,
        observer,
    })
}

async fn exercise(
    cluster: &mut ManagedCluster,
    rooted: bool,
    profile: CapacityProfile,
    workers: &mut Vec<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let PreparedGroups {
        seed,
        histories,
        observer,
    } = prepare_groups(cluster, rooted, workers, roots).await?;
    let directory = cluster.directory.observe().await?;

    // Retained restart with no local group exercises normal configured startup,
    // including fresh boot qualification before automatic placement can run.
    let node_profile = cluster.hosts[0].observe_node(TIMEOUT).await?.node.profile;
    let topology = configuration(node_profile.bytes_per_group);
    let node = cluster.nodes.remove(0);
    let old = cluster.hosts.remove(0);
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    old.close().await?;
    let fixture = &cluster.fixtures[0];
    let node = managed_fixture::start_node(
        cluster.roots[0].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 91).await?;
    managed_fixture::archives::configure(&mut config);
    config.host.automatic_topology = Some(topology.clone());
    cluster.peers[0] = config.peers[0].clone();
    let host = node.start_raft_cells(config, services).await?;
    cluster.nodes.insert(0, node);
    cluster.hosts.insert(0, host.clone());
    // Until explicit activation, the configured controller must not append
    // topology intent or compete with the foreground qualification CAS.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = host.automatic_topology_status().context("startup status")?;
            if let Some(status) = status.automatic_topology {
                ensure!(
                    status.selected_operations == 0,
                    "unqualified origin selected work"
                );
                if status.wait == FleetAutomaticTopologyWait::OriginUnqualified {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("automatic startup waits for qualification")??;
    let cut = observer.read(TIMEOUT).await?;
    ensure!(observer
        .active_topology_operation(&cut, TIMEOUT)
        .await?
        .is_none());
    managed_fixture::admit_capacity(fixture, &host, &cluster.directory, 180)
        .await
        .context("activate restarted automatic topology origin")?;
    ensure!(host.automatic_topology_status().is_some());
    eprintln!("automatic topology: extents={rooted}, configured startup on retained empty voter");

    let cut = observer.read(TIMEOUT).await?;
    for destination in cluster.hosts.iter().skip(1) {
        managed_fixture::inspect_current(destination, cluster.peers[0].node, cut.revision())
            .await?;
        destination.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: cluster.peers[0].clone(),
        })?;
        destination.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: cluster.peers[0].clone(),
        })?;
    }
    drop(cut);
    // Replace the controller after actual durable intent selection. The next
    // owner must discover that same operation without application-supplied IDs.
    let operation = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let cut = observer.read(TIMEOUT).await?;
            let operation = observer.active_topology_operation(&cut, TIMEOUT).await?;
            if let Some(operation) = operation {
                return Ok::<_, anyhow::Error>(operation.identity.operation);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("automatically select whole-group intent")??;
    host.suspend_automatic_topology().await;
    ensure!(host.automatic_topology_status().is_none());
    let cut = observer.read(TIMEOUT).await?;
    ensure!(
        observer
            .topology_operation(&cut, ProposalId::new(operation)?, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Running
    );
    host.resume_automatic_topology().await?;
    if matches!(profile, CapacityProfile::Original) {
        // The positive case uses the identical path with declared recovery
        // room. This paired case must retain all debt and refuse every move.
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let cut = observer.read(TIMEOUT).await?;
                let operation = observer
                    .topology_operation(&cut, ProposalId::new(operation)?, TIMEOUT)
                    .await?;
                ensure!(
                    operation.phase == FleetTopologyPhase::Running
                        && operation.admitted_movements == 0
                );
                let status = host.automatic_topology_status().context("reserve status")?;
                if status.last_round_failure == Some(FleetMovementServiceFailure::Rejected)
                    && status.consecutive_round_failures >= 3
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("refuse balance without recovery room")??;
        ensure!(cluster.directory.observe().await?.digest() == directory.digest());
        verify_cells(&host, &seed).await?;
        eprintln!("automatic topology: extents={rooted}, original 512-MiB logical profile refused all moves; SQL/receipts/quorum continue");
        return Ok(());
    }
    let completed = tokio::time::timeout(Duration::from_secs(60), async {
        let mut previous = Vec::new();
        loop {
            let progress = host
                .automatic_topology_status()
                .context("balance worker status")?;
            let phases = progress
                .work
                .iter()
                .map(|w| (w.reservation, w.phase, w.last_failure))
                .collect::<Vec<_>>();
            if phases != previous {
                eprintln!("automatic balance progress: {progress:?}");
                previous = phases;
            }
            let cut = observer.read(TIMEOUT).await?;
            let status = observer
                .topology_operation(&cut, ProposalId::new(operation)?, TIMEOUT)
                .await?;
            if status.phase == FleetTopologyPhase::Complete {
                return Ok::<_, anyhow::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if completed.is_err() {
        let progress = host.automatic_topology_status();
        host.suspend_automatic_topology().await;
        let diagnostic = diagnose_learner(cluster, &observer).await;
        anyhow::bail!("automatic balance timed out; worker={progress:?}; learner={diagnostic:?}");
    }
    let completed = completed.context("resume and complete automatic balance")??;
    ensure!(completed.admitted_movements == 3 && completed.keep.len() == 6);
    ensure!(
        cluster.directory.observe().await?.digest() == directory.digest(),
        "replica moves changed cell routing"
    );
    let mut copies = vec![0; 6];
    for history in &histories {
        let cut = observer.read(TIMEOUT).await?;
        let birth = observer
            .history(&cut, history, TIMEOUT)
            .await?
            .initial_placement()?;
        let mut observed = false;
        for owner in &cluster.hosts {
            let Ok(group) = owner.group(*history) else {
                continue;
            };
            let Ok(proof) = group.quorum_membership(TIMEOUT).await else {
                continue;
            };
            ensure!(
                proof.configuration().voters.len() == 3
                    && proof.configuration().voters_outgoing.is_empty()
                    && proof.configuration().learners.is_empty()
            );
            for member in &proof.configuration().voters {
                let member = MemberId::new(*member)?;
                let node = if let Some((joined, _)) = proof.membership().joined(member)? {
                    joined.node
                } else {
                    birth
                        .voters
                        .iter()
                        .find(|v| v.member == member)
                        .context("birth voter binding")?
                        .node
                };
                let index = cluster
                    .fixtures
                    .iter()
                    .position(|f| f.spec.node == node)
                    .context("current voter node")?;
                copies[index] += 1;
            }
            observed = true;
            break;
        }
        ensure!(observed, "no fresh quorum for balanced history {history:?}");
    }
    ensure!(
        copies == vec![1; 6],
        "whole-group copies are not balanced: {copies:?}"
    );
    verify_cells(&host, &seed).await?;
    eprintln!("automatic topology: extents={rooted}, three persisted moves, same cells/receipts, fresh quorum writes");
    // An unchanged completed topology must not append another empty operation.
    let operations = host
        .automatic_topology_status()
        .context("configured status")?
        .automatic_topology
        .context("automatic status")?
        .selected_operations;
    tokio::time::sleep(Duration::from_millis(150)).await;
    ensure!(
        host.automatic_topology_status()
            .context("idle status")?
            .automatic_topology
            .context("automatic idle status")?
            .selected_operations
            == operations
    );
    // A deliberate operator policy remains authoritative even when it uses
    // this controller's principal. Only generated immutable identities are
    // automatic work; a label alone never authorizes replacing this intent.
    let cut = observer.read(TIMEOUT).await?;
    let explicit = ProposalId::new([224; 16])?;
    observer
        .begin_topology(
            &cut,
            &FleetTopologyIntentRequest {
                identity: FleetRequestIdentity {
                    id: [223; 16],
                    operation: explicit.bytes(),
                    principal: topology.principal.clone(),
                    expected_revision: cut.revision(),
                },
                keep: completed.keep.clone(),
                policy: FleetTopologyPolicy {
                    revision: 2,
                    ..topology.policy.clone()
                },
            },
            std::slice::from_ref(&cluster.directory),
            TIMEOUT,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let cut = observer.read(TIMEOUT).await?;
            let explicit = observer.topology_operation(&cut, explicit, TIMEOUT).await?;
            ensure!(
                !matches!(
                    explicit.phase,
                    FleetTopologyPhase::Cancelled | FleetTopologyPhase::Cancelling
                ),
                "operator policy was cancelled"
            );
            let waiting = host
                .automatic_topology_status()
                .and_then(|s| s.automatic_topology)
                .is_some_and(|s| s.wait == FleetAutomaticTopologyWait::OperatorPolicy);
            if explicit.phase == FleetTopologyPhase::Complete && waiting {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("explicit operator policy precedence")??;
    let cut = observer.read(TIMEOUT).await?;
    let automatic = topology.intent(observer.catalog_id()?, cut.revision(), completed.keep)?;
    observer
        .begin_topology(
            &cut,
            &automatic,
            std::slice::from_ref(&cluster.directory),
            TIMEOUT,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let cut = observer.read(TIMEOUT).await?;
            let status = observer
                .topology_operation(
                    &cut,
                    ProposalId::new(automatic.identity.operation)?,
                    TIMEOUT,
                )
                .await?;
            let waiting = host
                .automatic_topology_status()
                .and_then(|s| s.automatic_topology)
                .is_some_and(|s| s.wait == FleetAutomaticTopologyWait::Unchanged);
            if status.phase == FleetTopologyPhase::Complete && waiting {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("explicit handover to automatic policy")??;
    Ok(())
}

async fn verify_cells(host: &RaftCells, seed: &SeededCells) -> Result<()> {
    for (i, cell) in seed.cells.iter().enumerate() {
        let cell = host.cell(cell.clone())?;
        let replay = cell
            .command(
                &cell.contract("Add")?,
                [7i64, 11][i].to_be_bytes().to_vec(),
                CommandOptions::new(seed.receipts[i].id, seed.retry),
            )
            .await?;
        ensure!(replay.result == seed.receipts[i].result && replay.id == seed.receipts[i].id);
        ensure!(replay.version.covers(&seed.receipts[i].version) == Ok(true));
        let query = cell
            .query(
                ProposalId::new([215 + i as u8; 16])?,
                &cell.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(query.result == seed.receipts[i].result && query.version == replay.version);
        let retry = u64::try_from(cellar::asyncrt::wall_ms())?
            .checked_add(60_000)
            .context("fresh retry horizon")?;
        let write = cell
            .command(
                &cell.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([219; 16])?, 1)?, retry),
            )
            .await?;
        ensure!(
            write.version.sequence.get() == replay.version.sequence.get() + 1
                && write.durability == Durability::Quorum
        );
        ensure!(write.result == ([7i64, 11][i] + 1).to_be_bytes());
    }
    Ok(())
}

// A failed test stays failed. Probe the exact retained admission only after
// joining the worker, so an opaque movement retry retains its typed SDK cause.
async fn diagnose_learner(cluster: &ManagedCluster, observer: &FleetCatalog) -> Result<String> {
    let cut = observer.read(TIMEOUT).await?;
    let pending = observer.pending_movements(&cut, TIMEOUT).await?;
    let pending = pending.first().context("no pending movement")?;
    let birth = observer
        .history(&cut, &pending.group, TIMEOUT)
        .await?
        .initial_placement()?;
    for voter in birth.voters {
        let Some(source) = cluster
            .hosts
            .iter()
            .find(|h| h.identity().node == voter.node)
        else {
            continue;
        };
        let group = source.group(pending.group)?;
        let status = group.status().await?;
        if !status.ready || status.leader != Some(voter.member.get()) {
            continue;
        }
        let target = cluster
            .hosts
            .iter()
            .find(|h| h.identity().node == pending.plan.replacement.node)
            .context("movement target")?;
        let history = observer
            .authorize_group(&cut, &pending.group, TIMEOUT)
            .await?;
        let authorization = observer
            .authorize_membership(&cut, &pending.group, TIMEOUT)
            .await?;
        let result = target
            .activate_managed_learner_group(
                history,
                authorization,
                LearnerActivation {
                    member: pending.plan.replacement.member,
                    source: voter.member,
                    source_node: voter.node,
                    from_s3: true,
                    timeout: TIMEOUT,
                },
            )
            .await;
        return Ok(format!("{result:?}"));
    }
    anyhow::bail!("no ready birth-voter leader for diagnostic")
}
