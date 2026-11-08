// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Automatic discovery and rooted admission recovery feed the actual worker.
use super::*;
use cellar::application::consensus::verification::{
    FleetChange, FleetHistoryState, FleetMutation, FleetTable,
};
use sha2::Digest as _;

fn policy() -> PartitionPolicy {
    PartitionPolicy::new(PartitionLimits {
        split_ceiling: PartitionLoad {
            cells: 1,
            logical_bytes: 64 * 1024 * 1024,
            writes_per_second: 1_000_000,
        },
        merge_ceiling: PartitionLoad {
            cells: 0,
            logical_bytes: 32 * 1024 * 1024,
            writes_per_second: 500_000,
        },
        split_cpu_bps: 9_000,
        merge_cpu_bps: 1_000,
        max_queue_age_ms: 1_000,
        max_archive_lag_entries: 1_024,
        max_failure_domain_load_bps: 9_000,
        hot_cell_bytes: 64 * 1024 * 1024,
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

fn pressure() -> RaftFleetPartitionPressure {
    RaftFleetPartitionPressure {
        placement_class: [1; 32],
        leader_cpu_bps: 0,
        failure_domain_load_bps: 0,
    }
}

fn cold_scheduler(
    cluster: &ManagedCluster,
    observer: &RaftCells,
) -> Result<RaftFleetPartitionScheduler> {
    let control = RaftFleetControl::new(
        &cluster.fixtures[0].domain,
        managed_fixture::Fixture::open_catalog(
            &cluster.fixtures[0].domain,
            cluster.fixtures[0].store.clone(),
            cluster.history.cluster,
        )?,
        vec![cluster.directory.clone()],
    )?
    .with_node_observer(observer.clone());
    Ok(RaftFleetPartitionScheduler::new(control, policy(), 1)?)
}

pub(super) async fn admit(
    cluster: &ManagedCluster,
    coordinator: &RaftFleetControl,
    destinations: [usize; 2],
) -> Result<(
    RaftFleetTransitionAdmissionRequest,
    RaftFleetTransitionAdmission,
)> {
    let first = leader(&cluster.hosts[..3], cluster.history).await?;
    let observer = cluster.hosts[first].clone();
    let endpoint = coordinator.clone().with_node_observer(observer.clone());
    let mut scheduler = RaftFleetPartitionScheduler::new(endpoint, policy(), 1)?;
    let before = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    let pivot = tokio::time::timeout(TIMEOUT, async {
        loop {
            let poll = scheduler.poll(pressure(), TIMEOUT).await?;
            ensure!(poll.sampled == Some(cluster.history));
            if let Some(RaftFleetPartitionRecommendation::Split {
                basis,
                pivot,
                reason,
            }) = poll.recommendation
            {
                ensure!(basis.route.history == cluster.history && reason == SplitReason::GroupLoad);
                return Ok::<_, anyhow::Error>(pivot);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    ensure!(scheduler.tracked_groups() == 1);
    ensure!(cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == before.revision());
    let request = split_request(cluster, coordinator, destinations, pivot).await?;
    let mut replacement = cold_scheduler(cluster, &observer)?;
    let absent = Box::pin(replacement.resume("automatic-test", &request, TIMEOUT)).await;
    ensure!(matches!(absent, Err(error) if error.code == RaftFleetFailureCode::Conflict));
    ensure!(replacement.pending_request().is_none());

    // Persist the operator plan but fail the first Fleet CAS before mutation.
    // The unchanged Fleet revision makes the missing-plan retry a real test:
    // a fresh fallback could otherwise start another admission at that root.
    cluster.fixtures[0]
        .store
        .fail_fleet_before
        .store(true, Ordering::Release);
    let (unconfirmed, admission_reads) = catalog_fault::count_reads(Box::pin(scheduler.admit(
        "automatic-test",
        &request,
        TIMEOUT,
    )))
    .await;
    ensure!(
        matches!(&unconfirmed, Err(error) if error.code == RaftFleetFailureCode::Unavailable),
        "{unconfirmed:?}"
    );
    ensure!(scheduler.pending_request() == Some(&request));
    ensure!(cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == before.revision());
    let head = admission_reads
        .keys()
        .find(|path| path.ends_with("/fleet/head"))
        .context("observed Fleet root read")?
        .clone();
    let suffix = format!("/fleet/transition-operator-plans/{}", request.operation_id);
    let plan = admission_reads
        .keys()
        .find(|path| path.ends_with(&suffix))
        .context("observed operator plan read")?
        .clone();
    let interrupted = catalog_fault::missing_read(
        head,
        Box::pin(replacement.resume("automatic-test", &request, TIMEOUT)),
    )
    .await;
    ensure!(interrupted.is_err(), "{interrupted:?}");
    ensure!(replacement.pending_request() == Some(&request));
    let (missing, reads) = catalog_fault::count_reads(catalog_fault::missing_read(
        plan.clone(),
        Box::pin(replacement.resume("automatic-test", &request, TIMEOUT)),
    ))
    .await;
    ensure!(matches!(missing, Err(error) if error.code == RaftFleetFailureCode::Conflict));
    ensure!(reads.get(&plan) == Some(&1));
    ensure!(
        reads.keys().all(|path| path == &plan
            || path.ends_with("/fleet/pin")
            || path.ends_with("/fleet/head")),
        "missing-plan replay entered fresh admission: {reads:?}"
    );
    ensure!(replacement.pending_request() == Some(&request));
    ensure!(cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == before.revision());
    drop(replacement);

    // Select the full request durably before reserving either child, then lose
    // the actual journal CAS response. A cold poll discovers the Fleet's
    // work and never samples a replacement operation.
    cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost_journal = Box::pin(scheduler.stage("automatic-test", &request, TIMEOUT)).await;
    ensure!(matches!(lost_journal, Err(error) if error.code == RaftFleetFailureCode::Unavailable));
    let selected = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    ensure!(selected.revision() == before.revision() + 1);
    ensure!(
        cluster.fixtures[0]
            .catalog
            .pending_reservations(&selected, TIMEOUT)
            .await?
            .is_empty(),
        "scheduler journal created child reservations"
    );
    for child in &request.children {
        ensure!(
            matches!(
                cluster.fixtures[0]
                    .catalog
                    .history(&selected, &child_history(cluster.history, child)?, TIMEOUT)
                    .await,
                Err(FleetCatalogError::Conflict)
            ),
            "staging must not reserve a child history"
        );
    }
    let key = "partition-work/active".to_string();
    ensure!(cluster.fixtures[0]
        .catalog
        .get(&selected, FleetTable::Operations, &key, TIMEOUT)
        .await?
        .is_some());
    let mut id = [0; 16];
    id.copy_from_slice(&sha2::Sha256::digest(b"partition-work-test-forged-delete")[..16]);
    let forged = FleetMutation {
        id,
        operation: id,
        principal: "automatic-test".into(),
        expected_revision: selected.revision(),
        changes: vec![FleetChange {
            table: FleetTable::Operations,
            key,
            value: None,
        }],
    };
    let refusal = cluster.fixtures[0]
        .catalog
        .commit(&selected, &forged, TIMEOUT)
        .await
        .err();
    ensure!(
        matches!(refusal, Some(FleetCatalogError::Invalid(_))),
        "generic work deletion: {refusal:?}"
    );
    let mut cold = cold_scheduler(cluster, &cluster.hosts[4])?;
    let (missing, discovery_reads) = catalog_fault::count_reads(catalog_fault::missing_read(
        plan.clone(),
        Box::pin(cold.resume_pending(TIMEOUT)),
    ))
    .await;
    ensure!(matches!(missing, Err(error) if error.code == RaftFleetFailureCode::Conflict));
    ensure!(cold.pending_request().is_none());
    let body = discovery_reads
        .keys()
        .find(|key| key.contains("/partition-work-requests/"))
        .context("immutable scheduled request read")?
        .clone();
    ensure!(discovery_reads.get(&body) == Some(&1));
    let missing = catalog_fault::missing_read(body, Box::pin(cold.resume_pending(TIMEOUT))).await;
    ensure!(matches!(missing, Err(error) if error.code == RaftFleetFailureCode::Unavailable));
    ensure!(cold.pending_request().is_none());
    let poll = cold.poll(pressure(), TIMEOUT).await?;
    ensure!(poll.sampled.is_none() && poll.recommendation.is_none());
    ensure!(cold.pending_request() == Some(&request) && cold.tracked_groups() == 0);
    drop(cold);
    ensure!(cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == selected.revision());
    // Preserve the original child-reservation fault oracle at its own boundary.
    let before = selected;

    // Now lose confirmation after a real reservation CAS. Another cold owner
    // must reconcile the original partial reservations without fresh samples.
    cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = Box::pin(scheduler.admit("automatic-test", &request, TIMEOUT)).await;
    ensure!(
        matches!(&lost, Err(error) if error.code == RaftFleetFailureCode::Unavailable),
        "{lost:?}"
    );
    ensure!(scheduler.pending_request() == Some(&request));
    let partial = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    ensure!(partial.revision() == before.revision() + 1);
    let first = child_history(cluster.history, &request.children[0])?;
    let second = child_history(cluster.history, &request.children[1])?;
    ensure!(
        *cluster.fixtures[0]
            .catalog
            .history(&partial, &first, TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Reserved
    );
    ensure!(
        matches!(
            cluster.fixtures[0]
                .catalog
                .history(&partial, &second, TIMEOUT)
                .await,
            Err(FleetCatalogError::Conflict)
        ),
        "the second child must remain unreserved"
    );
    let (poll, reads) = catalog_fault::count_reads(scheduler.poll(pressure(), TIMEOUT)).await;
    let poll = poll?;
    ensure!(poll.sampled.is_none() && poll.recommendation.is_none() && reads.is_empty());
    ensure!(scheduler.pending_request() == Some(&request));
    let mut changed = request.clone();
    changed.children[0].creation_id = hex::encode([251; 16]);
    let busy = Box::pin(scheduler.admit("automatic-test", &changed, TIMEOUT)).await;
    ensure!(matches!(busy, Err(error) if error.code == RaftFleetFailureCode::Busy));
    drop(scheduler);
    let mut replacement = cold_scheduler(cluster, &cluster.hosts[3])?;
    let changed_plan = Box::pin(replacement.resume("automatic-test", &changed, TIMEOUT)).await;
    ensure!(
        matches!(changed_plan, Err(error) if error.code == RaftFleetFailureCode::PayloadConflict)
    );
    ensure!(replacement.pending_request().is_none());
    ensure!(cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == partial.revision());
    let admitted = Box::pin(replacement.resume_pending(TIMEOUT))
        .await?
        .context("discovered scheduled admission")?;
    ensure!(replacement.tracked_groups() == 0 && replacement.pending_request() == Some(&request));
    ensure!(admitted.phase == TransitionPhase::Planned);
    Ok((request, admitted))
}

pub(super) async fn verify_discovery(
    cluster: &ManagedCluster,
    coordinator: &RaftFleetControl,
    children: &[cellar::application::consensus::verification::cell_groups::GroupRoute],
) -> Result<()> {
    Box::pin(settle_cursor(cluster)).await?;
    let revision = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
    let target = coordinator
        .clone()
        .with_node_observer(cluster.hosts[3].clone());
    let mut limited = RaftFleetPartitionScheduler::new(target.clone(), policy(), 1)?;
    let refused = limited.poll(pressure(), TIMEOUT).await;
    ensure!(matches!(refused, Err(error) if error.code == RaftFleetFailureCode::Limit));
    ensure!(limited.tracked_groups() == 0 && limited.pending_request().is_none());
    drop(limited);
    let mut sampler = RaftFleetPartitionScheduler::new(target, policy(), 2)?;
    let first = sampler.poll(pressure(), TIMEOUT).await?;
    let second = sampler.poll(pressure(), TIMEOUT).await?;
    ensure!(first.recommendation.is_none() && second.recommendation.is_none());
    let selected = [
        first.sampled.context("first child discovery")?,
        second.sampled.context("second child discovery")?,
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    ensure!(selected == children.iter().map(|route| route.history).collect());
    ensure!(sampler.tracked_groups() == 2);
    let wrapped = sampler.poll(pressure(), TIMEOUT).await?;
    ensure!(wrapped.sampled == first.sampled);
    drop(sampler);
    let mut retired = RaftFleetPartitionScheduler::new(
        coordinator
            .clone()
            .with_node_observer(cluster.hosts[0].clone()),
        policy(),
        1,
    )?;
    let empty = retired.poll(pressure(), TIMEOUT).await?;
    ensure!(
        empty.sampled.is_none() && empty.recommendation.is_none() && retired.tracked_groups() == 0
    );
    ensure!(cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == revision);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_selection_resumes_partial_admission_and_executes() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify_with_selection(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            true,
            false,
            true,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_automatic_selection_resumes_partial_admission_and_executes() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "automatic-selection",
            rooted,
        )?));
        Box::pin(qualify_with_selection(rooted, store, true, false, true)).await?;
    }
    Ok(())
}

async fn settle_cursor(cluster: &ManagedCluster) -> Result<()> {
    let catalog = &cluster.fixtures[0].catalog;
    let key = "partition-work/active";
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog
        .get(&cut, FleetTable::Operations, key, TIMEOUT)
        .await?
        .is_some());
    // The original coordinator was a source voter. Another node owns this
    // continuation using only the rooted request, without caller-supplied IDs.
    let mut cleaner = cold_scheduler(cluster, &cluster.hosts[3])?;
    cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = cleaner.settle(TIMEOUT).await;
    ensure!(matches!(lost, Err(error) if error.code == RaftFleetFailureCode::Unavailable));
    let request = cleaner
        .pending_request()
        .context("pending terminal work")?
        .clone();
    let completed = catalog.read(TIMEOUT).await?;
    ensure!(completed.revision() == cut.revision() + 1);
    ensure!(catalog
        .get(&completed, FleetTable::Operations, key, TIMEOUT)
        .await?
        .is_none());
    cleaner.stage("automatic-test", &request, TIMEOUT).await?;
    ensure!(catalog.read(TIMEOUT).await?.revision() == completed.revision());
    ensure!(cleaner.settle(TIMEOUT).await? && cleaner.pending_request().is_none());
    ensure!(catalog.read(TIMEOUT).await?.revision() == completed.revision());
    drop(cleaner);
    let mut empty = cold_scheduler(cluster, &cluster.hosts[4])?;
    ensure!(empty.resume_pending(TIMEOUT).await?.is_none());
    ensure!(catalog.read(TIMEOUT).await?.revision() == completed.revision());
    Ok(())
}

fn child_history(scope: GroupHistory, child: &RaftFleetTransitionChild) -> Result<GroupHistory> {
    Ok(GroupHistory {
        group: cellar_logic::consensus::GroupId::new(
            hex::decode(&child.group.group_id)?.as_slice().try_into()?,
        )?,
        incarnation: cellar_logic::consensus::IncarnationId::new(
            hex::decode(&child.group.incarnation_id)?
                .as_slice()
                .try_into()?,
        )?,
        ..scope
    })
}
