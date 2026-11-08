// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Adaptive admission against real RF3 owners, SQLite cells and TLS peers.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;
#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
#[path = "support/cell_group_rustfs.rs"]
pub mod rustfs;

#[path = "support/cell_group_member_cleanup.rs"]
mod cleanup;

#[path = "support/cell_group_scheduler_topology.rs"]
mod topology;

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::cell_groups::TransitionPhase;
use cellar::sdk::{consensus::*, fleet_operations::*};
use managed_fixture::{
    leader, movement, request, seed_cells, ManagedCluster, SeededCells, TIMEOUT,
};
use std::{sync::atomic::Ordering, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_member_cleanup_resumes_on_qualified_boot() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start(rooted, &[7, 8, 9])).await?;
        let result = Box::pin(topology::exercise_retained_cleanup(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result
            .with_context(|| format!("retained cleanup and startup ownership, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_retained_member_cleanup_resumes_on_qualified_boot() -> Result<()> {
    for rooted in [false, true] {
        let store = std::sync::Arc::new(catalog_fault::Store::around(rustfs::store(
            "retained-member-cleanup",
            rooted,
        )?));
        let mut cluster = Box::pin(ManagedCluster::start_with_store(
            rooted,
            &[7, 8, 9],
            managed_fixture::ArchiveSetup::Initialized,
            store,
        ))
        .await?;
        let result = Box::pin(topology::exercise_retained_cleanup(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| {
            format!("RustFS retained cleanup and startup ownership, extents={rooted}")
        })?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_member_cleanup_resumes_after_worker_restart() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start(rooted, &[7, 8, 9])).await?;
        let result = Box::pin(topology::exercise_cleanup_service(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("managed cleanup restart, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_member_cleanup_resumes_after_worker_restart() -> Result<()> {
    for rooted in [false, true] {
        let store = std::sync::Arc::new(catalog_fault::Store::around(rustfs::store(
            "member-cleanup-service",
            rooted,
        )?));
        let mut cluster = Box::pin(ManagedCluster::start_with_store(
            rooted,
            &[7, 8, 9],
            managed_fixture::ArchiveSetup::Initialized,
            store,
        ))
        .await?;
        let result = Box::pin(topology::exercise_cleanup_service(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("RustFS managed cleanup restart, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_member_reclaimed_and_returns_with_fresh_id() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start(rooted, &[7, 8, 9])).await?;
        let result = Box::pin(topology::exercise_member_return(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("removed member GC and return, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_removed_member_reclaimed_and_returns_with_fresh_id() -> Result<()> {
    for rooted in [false, true] {
        let store = std::sync::Arc::new(catalog_fault::Store::around(rustfs::store(
            "member-reclamation",
            rooted,
        )?));
        let mut cluster = Box::pin(ManagedCluster::start_with_store(
            rooted,
            &[7, 8, 9],
            managed_fixture::ArchiveSetup::Initialized,
            store,
        ))
        .await?;
        let result = Box::pin(topology::exercise_member_return(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("RustFS removed member GC and return, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_group_scale_out_in() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start(rooted, &[7, 8, 9])).await?;
        let result = Box::pin(topology::exercise(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("whole-group desired placement, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_whole_group_scale_out_in() -> Result<()> {
    for rooted in [false, true] {
        let store = std::sync::Arc::new(catalog_fault::Store::around(rustfs::store(
            "whole-group-placement",
            rooted,
        )?));
        let mut cluster = Box::pin(ManagedCluster::start_with_store(
            rooted,
            &[7, 8, 9],
            managed_fixture::ArchiveSetup::Initialized,
            store,
        ))
        .await?;
        let result = Box::pin(topology::exercise(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result
            .with_context(|| format!("RustFS whole-group desired placement, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_scheduler_hysteresis_and_hot_isolation() -> Result<()> {
    for rooted in [false, true] {
        let store = std::sync::Arc::new(catalog_fault::Store::around(rustfs::store(
            "scheduler-admission",
            rooted,
        )?));
        let cluster = Box::pin(ManagedCluster::start_with_store(
            rooted,
            &[7, 8, 9],
            managed_fixture::ArchiveSetup::Initialized,
            store,
        ))
        .await?;
        let result = Box::pin(exercise_admission(&cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("RustFS adaptive hot-cell admission, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_scheduler_cannot_grant_authority() -> Result<()> {
    for rooted in [false, true] {
        let store = std::sync::Arc::new(catalog_fault::Store::around(rustfs::store(
            "scheduler-quorum",
            rooted,
        )?));
        let mut cluster = Box::pin(ManagedCluster::start_with_store(
            rooted,
            &[7, 8, 9],
            managed_fixture::ArchiveSetup::Initialized,
            store,
        ))
        .await?;
        let result = Box::pin(exercise_quorum_refusal(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("RustFS adaptive quorum refusal, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduler_hysteresis_and_hot_isolation() -> Result<()> {
    for rooted in [false, true] {
        let cluster = Box::pin(ManagedCluster::start(rooted, &[7, 8, 9])).await?;
        let result = Box::pin(exercise_admission(&cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("adaptive hot-cell admission, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduler_cannot_grant_authority() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start(rooted, &[7, 8, 9])).await?;
        let result = Box::pin(exercise_quorum_refusal(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("adaptive quorum refusal, extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

fn pressure() -> RaftFleetPartitionPressure {
    RaftFleetPartitionPressure {
        placement_class: [1; 32],
        leader_cpu_bps: 0,
        failure_domain_load_bps: 0,
    }
}

fn policy(hot_bytes: u64) -> PartitionPolicy {
    PartitionPolicy::new(PartitionLimits {
        split_ceiling: PartitionLoad {
            cells: 32,
            logical_bytes: 64 * 1024 * 1024,
            writes_per_second: 1_000_000,
        },
        merge_ceiling: PartitionLoad {
            cells: 16,
            logical_bytes: 32 * 1024 * 1024,
            writes_per_second: 500_000,
        },
        split_cpu_bps: 9_000,
        merge_cpu_bps: 1_000,
        max_queue_age_ms: 1_000,
        max_archive_lag_entries: 1_024,
        max_failure_domain_load_bps: 9_000,
        hot_cell_bytes: hot_bytes,
        hot_cell_writes_per_second: 1_000_000,
        split_samples: 2,
        merge_samples: 3,
        sample_interval_ms: 20,
        max_sample_age_ms: 5_000,
        cooldown_ms: 100,
        max_active_operations: 1,
    })
    .expect("explicit valid qualification policy")
}

async fn ready(
    cluster: &ManagedCluster,
) -> Result<(
    RaftFleetControl,
    RaftFleetPartitionScheduler,
    RaftFleetTransitionAdmissionRequest,
    SeededCells,
    usize,
)> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let index = leader(&cluster.hosts[..3], cluster.history).await?;
    let local = cluster.hosts[index]
        .group(cluster.history)?
        .observe_partition(TIMEOUT)
        .await?;
    let largest = local.largest_cell.context("real largest SQLite cell")?;
    ensure!(local.cells == 2 && largest.logical_bytes > 1);
    let f = &cluster.fixtures[0];
    let control = RaftFleetControl::new(
        &f.domain,
        f.catalog.clone(),
        vec![cluster.directory.clone()],
    )?
    .with_node_observer(cluster.hosts[index].clone());
    let mut scheduler =
        RaftFleetPartitionScheduler::new(control.clone(), policy(largest.logical_bytes - 1), 1)?;
    ensure!(scheduler
        .observe(cluster.history, pressure(), TIMEOUT)
        .await?
        .is_none());
    ensure!(scheduler.tracked_groups() == 1);
    for _ in 0..3 {
        ensure!(
            scheduler.recommend_split(cluster.history)?
                == RaftFleetPartitionRecommendation::Wait(WaitReason::NoFreshSample)
        );
    }
    let unrelated = GroupHistory {
        group: GroupId::new([252; 16])?,
        ..cluster.history
    };
    let full = scheduler.observe(unrelated, pressure(), TIMEOUT).await;
    ensure!(matches!(full, Err(error) if error.code == RaftFleetFailureCode::Limit));
    ensure!(scheduler.tracked_groups() == 1);

    let pivot = tokio::time::timeout(TIMEOUT, async {
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            scheduler
                .observe(cluster.history, pressure(), TIMEOUT)
                .await?;
            match scheduler.recommend_split(cluster.history)? {
                RaftFleetPartitionRecommendation::Split { pivot, reason, .. } => {
                    ensure!(reason == SplitReason::IsolateHotCell);
                    // Two occupied cells are separated at a canonical whole-key
                    // boundary, including when the largest cell is leftmost.
                    let expected = if largest.has_predecessor {
                        largest.key.clone()
                    } else {
                        largest.next.clone().context("next occupied cell")?
                    };
                    ensure!(pivot == expected);
                    return Ok::<_, anyhow::Error>(pivot);
                }
                RaftFleetPartitionRecommendation::Wait(_) => (),
                other => anyhow::bail!("unexpected split observation: {other:?}"),
            }
        }
    })
    .await??;
    let namespace = hex::encode(cluster.history.namespace.bytes());
    let basis = control.transition_basis(&namespace, TIMEOUT).await?;
    let request = RaftFleetTransitionAdmissionRequest {
        catalog_id: basis.catalog_id,
        namespace_id: namespace,
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
                voters: std::array::from_fn(|member| RaftFleetFirstGroupVoter {
                    node_id: hex::encode(cluster.fixtures[3 + member].spec.node.bytes()),
                    member_id: member as u64 + 1,
                }),
            })
            .collect(),
    };
    Ok((control, scheduler, request, seed, index))
}

fn group(history: GroupHistory) -> RaftFleetTransitionGroup {
    RaftFleetTransitionGroup {
        group_id: hex::encode(history.group.bytes()),
        incarnation_id: hex::encode(history.incarnation.bytes()),
    }
}

async fn exercise_admission(cluster: &ManagedCluster) -> Result<()> {
    let (control, mut scheduler, original, seed, index) = Box::pin(ready(cluster)).await?;
    let f = &cluster.fixtures[0];
    let before = f.catalog.read(TIMEOUT).await?;
    let invalid = scheduler.admit("", &original, TIMEOUT).await;
    ensure!(matches!(invalid, Err(error) if error.code == RaftFleetFailureCode::InvalidInput));
    ensure!(scheduler.pending_request().is_none());

    // A real CAS writes the first child reservation but loses confirmation.
    // The scheduling owner must not issue another operation or choose new IDs.
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = Box::pin(scheduler.admit("scheduler", &original, TIMEOUT)).await;
    ensure!(
        matches!(&lost, Err(error) if error.code == RaftFleetFailureCode::Unavailable),
        "{lost:?}"
    );
    ensure!(scheduler.pending_request() == Some(&original));
    let partial = f.catalog.read(TIMEOUT).await?;
    ensure!(partial.revision() == before.revision() + 1);
    let mut different = original.clone();
    different.operation_id = hex::encode([250; 16]);
    let refused = scheduler.admit("scheduler", &different, TIMEOUT).await;
    ensure!(
        matches!(refused, Err(error) if error.code == RaftFleetFailureCode::Busy && error.outcome == RaftFleetOutcome::NotSubmitted)
    );
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == partial.revision());
    scheduler.forget(cluster.history);
    ensure!(scheduler.tracked_groups() == 0 && scheduler.pending_request() == Some(&original));
    let admitted = Box::pin(scheduler.admit("scheduler", &original, TIMEOUT)).await?;
    ensure!(admitted.phase == TransitionPhase::Planned);
    ensure!(!Box::pin(scheduler.settle(TIMEOUT)).await?);
    let selected = f
        .catalog
        .reopen_managed_transition(&cluster.directory, ProposalId::new([231; 16])?, TIMEOUT)
        .await?
        .context("rooted scheduled split")?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let cancellation = FleetRequestIdentity {
        operation: [231; 16],
        principal: "scheduler".into(),
        ..request(235, cut.revision())
    };
    f.catalog
        .cancel_managed_transition(
            &cut,
            &cancellation,
            &cluster.directory,
            &selected.plan,
            TIMEOUT,
        )
        .await?;
    ensure!(Box::pin(scheduler.settle(TIMEOUT)).await?);
    ensure!(scheduler.pending_request().is_none());
    ensure!(
        scheduler.recommend_split(cluster.history)?
            == RaftFleetPartitionRecommendation::Wait(WaitReason::NoFreshSample)
    );
    ensure!(
        control
            .transition_status(&original.namespace_id, &original.operation_id, TIMEOUT)
            .await?
            .phase
            == Some(TransitionPhase::Cancelled)
    );
    verify_original_cells(cluster, &seed, index).await?;
    Ok(())
}

async fn exercise_quorum_refusal(cluster: &mut ManagedCluster) -> Result<()> {
    let (_, mut scheduler, original, seed, index) = Box::pin(ready(cluster)).await?;
    let survivor = cluster.hosts[index].clone();
    let before = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    for peer in (0..3).rev().filter(|peer| *peer != index) {
        let node = cluster.nodes.remove(peer);
        let host = cluster.hosts.remove(peer);
        ensure!(matches!(
            node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?,
            cellar::sdk::NodeOutcome::Stopped(_)
        ));
        let report = host.close().await?;
        ensure!(
            report.groups_total == 1
                && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
        );
    }
    let f = &cluster.fixtures[0];
    let refused = Box::pin(scheduler.admit("scheduler", &original, TIMEOUT)).await;
    ensure!(
        refused.is_err(),
        "a sizing observation fabricated quorum: {refused:?}"
    );
    ensure!(scheduler.pending_request() == Some(&original));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == before.revision());
    ensure!(f
        .catalog
        .reopen_managed_transition(&cluster.directory, ProposalId::new([231; 16])?, TIMEOUT,)
        .await?
        .is_none());
    ensure!(!Box::pin(scheduler.settle(TIMEOUT)).await?);
    let write = survivor.cell(seed.cells[0].clone())?;
    let command = CommandId::new(ClientId::new([249; 16])?, 1)?;
    let refused_write = write
        .command(
            &write.contract("Add")?,
            1i64.to_be_bytes().to_vec(),
            CommandOptions::new(command, seed.retry),
        )
        .await;
    ensure!(
        refused_write.is_err(),
        "minority scheduler path acknowledged a write"
    );
    // A timeout is not proof that the command failed. Restore the same disks
    // and reconcile the exact identity before asking shutdown to prove that
    // every local producer has resolved. Never discard the retained proposal.
    for peer in (0..3).filter(|peer| *peer != index) {
        let fixture = &cluster.fixtures[peer];
        let node = managed_fixture::start_node(
            cluster.roots[peer].path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, 51 + peer as u8)
            .await?;
        managed_fixture::archives::configure(&mut config);
        cluster.peers[peer] = config.peers[0].clone();
        let host = node.start_raft_cells(config, services).await?;
        cluster.nodes.insert(peer, node);
        cluster.hosts.insert(peer, host);
    }
    movement::discover(cluster).await?;
    let current = leader(&cluster.hosts[..3], cluster.history).await?;
    let handle = cluster.hosts[current].cell(seed.cells[0].clone())?;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let recovered = tokio::time::timeout_at(deadline, async {
        cluster.hosts[current]
            .group(cluster.history)?
            .quorum_membership(TIMEOUT)
            .await?;
        let mut busy = 0;
        loop {
            let mut options = CommandOptions::new(command, seed.retry);
            options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            match handle
                .command(
                    &handle.contract("Add")?,
                    1i64.to_be_bytes().to_vec(),
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
                    busy += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => {
                    eprintln!("CG11 retained-quorum reconciliation busy observations: {busy}");
                    return Ok::<_, anyhow::Error>(result?);
                }
            }
        }
    })
    .await
    .context("original retained-quorum reconciliation budget")??;
    ensure!(recovered.id == command && recovered.result == 8i64.to_be_bytes());
    let duplicate = handle
        .command(
            &handle.contract("Add")?,
            1i64.to_be_bytes().to_vec(),
            CommandOptions::new(command, seed.retry),
        )
        .await?;
    ensure!(
        duplicate.id == recovered.id
            && duplicate.result == recovered.result
            && duplicate.version == recovered.version
            && duplicate.witness == recovered.witness
            && duplicate.durability == recovered.durability,
        "minority retry changed its committed receipt"
    );
    // Token issuance time can differ. Authenticate both against the same
    // immutable committed observation rather than comparing signatures.
    let keys = managed_fixture::Fixture::version_keys()?;
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    for receipt in [&recovered, &duplicate] {
        let verified = keys.verify_cell(
            &receipt.version_token,
            &recovered.version.history,
            cluster.directory.namespace_config().schema_digest,
            now,
        )?;
        ensure!(verified.version == recovered.version && verified.witness == recovered.witness);
    }
    Ok(())
}

async fn verify_original_cells(
    cluster: &ManagedCluster,
    seed: &SeededCells,
    index: usize,
) -> Result<()> {
    for (i, (cell, receipt)) in seed.cells.iter().zip(&seed.receipts).enumerate() {
        let handle = cluster.hosts[index].cell(cell.clone())?;
        let read = handle
            .query(
                ProposalId::new([240 + i as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == receipt.result);
    }
    Ok(())
}
