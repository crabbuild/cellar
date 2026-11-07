// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Automatic split execution uses rooted consent and real voter owners.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;
#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
#[path = "support/cell_group_rustfs.rs"]
pub mod rustfs;

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::cell_groups::{
    cell_routing_key, GroupHistory, TransitionPhase,
};
use cellar::sdk::{consensus::*, fleet_operations::*, *};
use managed_fixture::{leader, movement, seed_cells, ArchiveSetup, ManagedCluster, TIMEOUT};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consented_split_resumes_with_owned_workers() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(rooted, Arc::new(catalog_fault::Store::default()))).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_consented_split_resumes_with_owned_workers() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "transition-service",
            rooted,
        )?));
        Box::pin(qualify(rooted, store)).await?;
    }
    Ok(())
}

fn options(coordinate: bool) -> RaftFleetTransitionServiceOptions {
    RaftFleetTransitionServiceOptions {
        coordinate,
        interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(250),
        turn_timeout: TIMEOUT,
    }
}

async fn control(
    cluster: &ManagedCluster,
    rooted: bool,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<RaftFleetControl> {
    let root = tempfile::tempdir()?;
    let f = &cluster.fixtures[0];
    let verifier =
        managed_fixture::verifier::open(f, root.path(), rooted, Default::default()).await?;
    roots.push(root);
    Ok(RaftFleetControl::new(
        &f.domain,
        managed_fixture::Fixture::open_catalog(
            &f.domain,
            f.store.clone(),
            cluster.history.cluster,
        )?,
        vec![cluster.directory.clone()],
    )?
    .with_transition_preparer(verifier, CellGroupRecoveryPool::new(Default::default())?))
}

async fn qualify(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_catalog_clients(
        rooted,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        store,
        true,
    ))
    .await?;
    let mut workers: Vec<Option<RaftFleetTransitionService>> = (0..6).map(|_| None).collect();
    let mut roots = Vec::new();
    let result = Box::pin(exercise(&mut cluster, rooted, &mut workers, &mut roots)).await;
    cluster.fixtures[0]
        .store
        .deny_registry_writes
        .store(false, Ordering::Release);
    for worker in workers.into_iter().flatten() {
        worker.shutdown().await;
    }
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result.with_context(|| format!("automatic transition, extents={rooted}; joined={joined:?}"))?;
    joined?;
    Ok(())
}

async fn exercise(
    cluster: &mut ManagedCluster,
    rooted: bool,
    workers: &mut [Option<RaftFleetTransitionService>],
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let coordinator = Box::pin(control(cluster, rooted, roots)).await?;
    let namespace = hex::encode(cluster.history.namespace.bytes());
    let basis = coordinator.transition_basis(&namespace, TIMEOUT).await?;
    let first = leader(&cluster.hosts[..3], cluster.history).await?;
    let observed = cluster.hosts[first]
        .group(cluster.history)?
        .observe_partition(TIMEOUT)
        .await?;
    let pivot = observed
        .balanced_pivot
        .context("occupied whole-cell split boundary")?;
    let request = RaftFleetTransitionAdmissionRequest {
        catalog_id: basis.catalog_id,
        namespace_id: namespace.clone(),
        operation_id: hex::encode([231; 16]),
        request_id: hex::encode([234; 16]),
        expected_fleet_revision: basis.fleet_revision,
        expected_directory_digest: basis.directory_digest,
        class: "Counter".into(),
        partition: RaftFleetTransitionPartition::Split {
            parent: group(cluster.history),
            pivot_cell: pivot.cell().as_str().into(),
        },
        children: [232u8, 233]
            .into_iter()
            .map(|id| RaftFleetTransitionChild {
                group: RaftFleetTransitionGroup {
                    group_id: hex::encode([id; 16]),
                    incarnation_id: hex::encode([id; 16]),
                },
                request_id: hex::encode([id; 16]),
                creation_id: hex::encode([id; 16]),
                voters: std::array::from_fn(|i| RaftFleetFirstGroupVoter {
                    node_id: hex::encode(cluster.fixtures[3 + i].spec.node.bytes()),
                    member_id: i as u64 + 1,
                }),
            })
            .collect(),
    };
    let admitted = Box::pin(coordinator.admit_transition(
        "automatic-test",
        &namespace,
        &request.operation_id,
        &request,
        TIMEOUT,
    ))
    .await?;
    ensure!(admitted.phase == TransitionPhase::Planned);
    let mut controls = Vec::new();
    for (i, host) in cluster.hosts.iter().enumerate() {
        let endpoint = Box::pin(control(cluster, rooted, roots)).await?;
        workers[i] = Some(
            endpoint
                .start_transition_service(host, options(i == 3), TIMEOUT)
                .await
                .with_context(|| format!("start transition worker {i}"))?,
        );
        controls.push(endpoint);
    }
    // A duplicate service cannot create another worker or private admission bank.
    let duplicate = controls[3]
        .start_transition_service(&cluster.hosts[3], options(true), TIMEOUT)
        .await;
    ensure!(matches!(duplicate, Err(error) if error.code == RaftFleetFailureCode::Busy));
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if workers
                .iter()
                .all(|w| w.as_ref().is_some_and(|w| w.status().rounds >= 2))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    ensure!(
        coordinator
            .transition_status(&namespace, &request.operation_id, TIMEOUT)
            .await?
            .phase
            == Some(TransitionPhase::Planned)
    );
    ensure!(cluster.hosts[3..]
        .iter()
        .all(|host| host.resource_usage().host.controller.groups == 0));
    let revision = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
    let consent = RaftFleetTransitionExecutionRequest {
        catalog_id: coordinator.catalog_id().into(),
        namespace_id: namespace.clone(),
        operation_id: request.operation_id.clone(),
        request_id: hex::encode([236; 16]),
        expected_revision: revision,
        plan_digest: admitted.plan_digest.clone(),
    };
    let forbidden = coordinator
        .authorize_transition_execution("other-principal", &consent, TIMEOUT)
        .await;
    ensure!(matches!(forbidden, Err(error) if error.code == RaftFleetFailureCode::Forbidden));
    // The first controller reaches an operation-owned preparation but cannot
    // finish archive hold publication. Its replacement must resume that work.
    let denied_before = cluster.fixtures[0]
        .store
        .denied_registry_writes
        .load(Ordering::Acquire);
    cluster.fixtures[0]
        .store
        .deny_registry_writes
        .store(true, Ordering::Release);
    coordinator
        .authorize_transition_execution("automatic-test", &consent, TIMEOUT)
        .await
        .context("persist/reopen execution consent")?;
    coordinator
        .authorize_transition_execution("automatic-test", &consent, TIMEOUT)
        .await
        .context("persist/reopen execution consent")?;
    let mut changed = consent.clone();
    changed.request_id = hex::encode([237; 16]);
    let refused = coordinator
        .authorize_transition_execution("automatic-test", &changed, TIMEOUT)
        .await;
    ensure!(matches!(refused, Err(error) if error.code == RaftFleetFailureCode::PayloadConflict));
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = workers[3].as_ref().context("coordinator worker")?.status();
            ensure!(!status.needs_operator, "{status:?}");
            if status
                .last_failure
                .as_ref()
                .is_some_and(|failure| failure.code == RaftFleetFailureCode::Unavailable)
                && status.phase == Some(TransitionPhase::Planned)
                && cluster.fixtures[0]
                    .store
                    .denied_registry_writes
                    .load(Ordering::Acquire)
                    > denied_before
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    workers[3]
        .take()
        .context("first coordinator")?
        .shutdown()
        .await;
    let before = coordinator
        .transition_status(&namespace, &request.operation_id, TIMEOUT)
        .await?;
    ensure!(before.phase == Some(TransitionPhase::Planned) && before.fleet_slot_held);
    cluster.fixtures[0]
        .store
        .deny_registry_writes
        .store(false, Ordering::Release);
    let endpoint = Box::pin(control(cluster, rooted, roots)).await?;
    workers[3] = Some(
        endpoint
            .start_transition_service(&cluster.hosts[3], options(true), TIMEOUT)
            .await?,
    );
    controls[3] = endpoint;

    wait_phase(
        &coordinator,
        &namespace,
        &request.operation_id,
        workers,
        TransitionPhase::Retired,
    )
    .await?;
    let operation = ProposalId::new([231; 16])?;
    let selected = cluster.fixtures[0]
        .catalog
        .reopen_managed_transition(&cluster.directory, operation, TIMEOUT)
        .await?
        .context("retired transition")?;
    let mut children = Vec::new();
    for id in [232, 233] {
        let history = GroupHistory {
            group: GroupId::new([id; 16])?,
            incarnation: IncarnationId::new([id; 16])?,
            ..cluster.history
        };
        let birth = cluster.fixtures[0]
            .domain
            .run(cluster.directory.materialize_selected_history(history))
            .await??;
        children.push(birth.route().clone());
    }
    ensure!(selected.plan.operation() == operation);
    ensure!(children[0].range.merge(&children[1].range)? == CellRange::all());
    let current = coordinator
        .transition_status(&namespace, &request.operation_id, TIMEOUT)
        .await?;
    ensure!(
        current.child_archives_verified
            && current.fleet_slot_held
            && !current.parent_holds_released
    );
    let mut retired_through = LogIndex::new(0);
    for host in &cluster.hosts[..3] {
        retired_through =
            retired_through.max(host.group(cluster.history)?.status().await?.applied.index);
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let mut caught_up = true;
            for host in &cluster.hosts[..3] {
                caught_up &=
                    host.group(cluster.history)?.status().await?.applied.index >= retired_through;
            }
            if caught_up {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("all retained source voters applied retirement")??;
    // Automatic execution must preserve logical histories, original results and
    // floors. Retry through a destination, then write and read new state.
    let mut latest = Vec::new();
    for (i, (cell, original)) in seed.cells.iter().zip(&seed.receipts).enumerate() {
        let route = children
            .iter()
            .find(|r| {
                r.range
                    .contains(&cell_routing_key(cell.namespace, &cell.cell))
            })
            .context("cell range")?;
        let elected = leader(&cluster.hosts[3..], route.history).await? + 3;
        let handle = cluster.hosts[elected].cell(cell.clone())?;
        let duplicate = handle
            .command(
                &handle.contract("Add")?,
                [7i64, 11][i].to_be_bytes().to_vec(),
                CommandOptions::new(original.id, seed.retry),
            )
            .await?;
        ensure!(
            duplicate.id == original.id
                && duplicate.result == original.result
                && duplicate.version.history == original.version.history
        );
        let read = handle
            .query(
                ProposalId::new([240 + i as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(original.version_token.clone()),
                    routing: QueryRouting::Local,
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        // A transferred version preserves its original mutation witness.
        // Prove physical response ownership independently of that provenance.
        ensure!(
            read.result == original.result
                && read.version == original.version
                && read.witness == original.witness
        );
        ensure!(read.served_by_node == cluster.hosts[elected].identity());
        let updated = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([247; 16])?, 1)?, seed.retry),
            )
            .await?;
        ensure!(
            updated.result == ([8i64, 12][i]).to_be_bytes()
                && updated.witness.group == route.history
        );
        latest.push((cell.clone(), updated));
    }

    // Existing physical-release proof requires retained parent retirement.
    // Reboot only the owned parent nodes; the controller must retain its slot
    // through every partial return and finish once all physical cuts verify.
    for (i, worker) in workers.iter_mut().enumerate().take(3) {
        worker.take().context("source worker")?.shutdown().await;
        let node = cluster.nodes.remove(i);
        let old = cluster.hosts.remove(i);
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        ensure!(
            old.close().await?.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
        );
        let fixture = &cluster.fixtures[i];
        let node = managed_fixture::start_node(
            cluster.roots[i].path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, 71 + i as u8)
            .await?;
        managed_fixture::archives::configure(&mut config);
        cluster.peers[i] = config.peers[0].clone();
        let host = node.start_raft_cells(config, services).await?;
        cluster.nodes.insert(i, node);
        cluster.hosts.insert(i, host);
        if i < 2 {
            ensure!(
                coordinator
                    .transition_status(&namespace, &request.operation_id, TIMEOUT)
                    .await?
                    .fleet_slot_held
            );
        }
    }
    movement::discover(cluster).await?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = coordinator
                .transition_status(&namespace, &request.operation_id, TIMEOUT)
                .await?;
            if !status.fleet_slot_held {
                ensure!(
                    status.phase == Some(TransitionPhase::Retired) && status.parent_holds_released
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    // Namespace discovery never grants application I/O. Reinstall the trusted
    // ingress grants for its new boot before forwarding application requests.
    for host in &cluster.hosts[3..] {
        host.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: 1,
            peer: cluster.peers[0].clone(),
        })?;
        host.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: 1,
            peer: cluster.peers[0].clone(),
        })?;
    }
    for (i, (cell, receipt)) in latest.iter().enumerate() {
        let ingress = cluster.hosts[0].cell(cell.clone())?;
        let read = ingress
            .query(
                ProposalId::new([250 + i as u8; 16])?,
                &ingress.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == receipt.result && read.version == receipt.version);
        ensure!(cluster.hosts[3..]
            .iter()
            .any(|host| host.identity() == read.served_by_node));
        let archived = ingress
            .command(
                &ingress.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions {
                    durability: Durability::S3,
                    ..CommandOptions::new(CommandId::new(ClientId::new([248; 16])?, 1)?, seed.retry)
                },
            )
            .await?;
        ensure!(
            archived.result == ([9i64, 13][i]).to_be_bytes()
                && archived.durability == Durability::S3
        );
    }
    Ok(())
}

async fn wait_phase(
    control: &RaftFleetControl,
    namespace: &str,
    operation: &str,
    workers: &[Option<RaftFleetTransitionService>],
    phase: TransitionPhase,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let observed = control
                .transition_status(namespace, operation, TIMEOUT)
                .await?;
            if observed.phase == Some(phase) {
                return Ok::<_, anyhow::Error>(());
            }
            for worker in workers.iter().flatten() {
                ensure!(!worker.status().needs_operator, "{:?}", worker.status());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "transition progress: {:?}",
            workers
                .iter()
                .flatten()
                .map(|w| w.status())
                .collect::<Vec<_>>()
        )
    })??;
    Ok(())
}

fn group(history: GroupHistory) -> RaftFleetTransitionGroup {
    RaftFleetTransitionGroup {
        group_id: hex::encode(history.group.bytes()),
        incarnation_id: hex::encode(history.incarnation.bytes()),
    }
}
