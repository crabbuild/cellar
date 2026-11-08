// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned automatic selection, cold recovery and joined cancellation.
use super::*;
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct Planner {
    template: RaftFleetTransitionAdmissionRequest,
    pressures: AtomicUsize,
    plans: AtomicUsize,
    refuse: AtomicBool,
    block: AtomicBool,
    active: AtomicUsize,
}

struct Call<'a>(&'a AtomicUsize);

impl Drop for Call<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RaftFleetPartitionPlanner for Planner {
    fn pressure(
        &self,
        _: Duration,
    ) -> BoxFuture<'_, std::result::Result<RaftFleetPartitionPressure, RaftFleetFailure>> {
        Box::pin(async {
            self.pressures.fetch_add(1, Ordering::AcqRel);
            if self.refuse.load(Ordering::Acquire) {
                return Err(RaftFleetFailure::invalid("recovery called pressure"));
            }
            self.active.fetch_add(1, Ordering::AcqRel);
            let _call = Call(&self.active);
            if self.block.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            Ok(RaftFleetPartitionPressure {
                placement_class: [1; 32],
                leader_cpu_bps: 0,
                failure_domain_load_bps: 0,
            })
        })
    }

    fn plan(
        &self,
        context: RaftFleetPartitionPlanContext,
        _: Duration,
    ) -> BoxFuture<'_, std::result::Result<RaftFleetTransitionAdmissionRequest, RaftFleetFailure>>
    {
        Box::pin(async move {
            if self.refuse.load(Ordering::Acquire) || self.plans.fetch_add(1, Ordering::AcqRel) != 0
            {
                return Err(RaftFleetFailure::invalid("replacement plan requested"));
            }
            let RaftFleetPartitionRecommendation::Split { pivot, .. } = context.recommendation
            else {
                return Err(RaftFleetFailure::invalid("expected split"));
            };
            let mut request = self.template.clone();
            request.catalog_id = context.basis.catalog_id;
            request.namespace_id = context.basis.namespace_id;
            request.expected_fleet_revision = context.basis.fleet_revision;
            request.expected_directory_digest = context.basis.directory_digest;
            let RaftFleetTransitionPartition::Split { pivot_cell, .. } = &mut request.partition
            else {
                return Err(RaftFleetFailure::invalid("split template"));
            };
            *pivot_cell = pivot.cell().as_str().into();
            Ok(request)
        })
    }
}

fn service_options(
    principal: &str,
    execute: bool,
    coordinate: bool,
) -> RaftFleetPartitionServiceOptions {
    RaftFleetPartitionServiceOptions {
        principal: principal.into(),
        max_groups: 2,
        execution: if execute {
            RaftFleetPartitionExecutionPolicy::Complete
        } else {
            RaftFleetPartitionExecutionPolicy::default()
        },
        interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(250),
        turn_timeout: TIMEOUT,
        transition: options(coordinate, true, false),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_partition_service_recovers_and_executes() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(rooted, Arc::new(catalog_fault::Store::default()))).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_owned_partition_service_recovers_and_executes() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "partition-service",
            rooted,
        )?));
        Box::pin(qualify(rooted, store)).await?;
    }
    Ok(())
}

async fn qualify(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let cluster = Box::pin(ManagedCluster::start_with_catalog_clients(
        rooted,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        store,
        true,
    ))
    .await?;
    let mut workers: Vec<Option<RaftFleetPartitionService>> = (0..6).map(|_| None).collect();
    let mut roots = Vec::new();
    let result = Box::pin(exercise(&cluster, rooted, &mut workers, &mut roots)).await;
    for worker in workers.into_iter().flatten() {
        worker.shutdown().await;
    }
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result
        .with_context(|| format!("owned partition service, extents={rooted}; joined={joined:?}"))?;
    joined?;
    Ok(())
}

async fn wait(
    workers: &[Option<RaftFleetPartitionService>],
    predicate: impl Fn() -> bool,
) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for worker in workers.iter().flatten() {
                ensure!(!worker.status().needs_operator, "{:?}", worker.status());
            }
            if predicate() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "partition owners: {:?}",
            workers
                .iter()
                .flatten()
                .map(|w| w.status())
                .collect::<Vec<_>>()
        )
    })??;
    Ok(())
}

async fn exercise(
    cluster: &ManagedCluster,
    rooted: bool,
    workers: &mut [Option<RaftFleetPartitionService>],
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let coordinator = Box::pin(control(cluster, rooted, roots)).await?;
    let first = leader(&cluster.hosts[..3], cluster.history).await?;
    let pivot = cluster.hosts[first]
        .group(cluster.history)?
        .observe_partition(TIMEOUT)
        .await?
        .balanced_pivot
        .context("split pivot")?;
    let template = split_request(cluster, &coordinator, [3, 3], pivot).await?;
    let planner = Arc::new(Planner {
        template: template.clone(),
        pressures: AtomicUsize::new(0),
        plans: AtomicUsize::new(0),
        refuse: AtomicBool::new(false),
        block: AtomicBool::new(false),
        active: AtomicUsize::new(0),
    });
    // Cancellation must drop an actually entered callback before releasing the
    // node's service slots. Restarting below proves both slots were joined.
    planner.block.store(true, Ordering::Release);
    workers[first] = Some(
        coordinator
            .start_partition_service(
                &cluster.hosts[first],
                planner.clone(),
                selection::policy(),
                service_options("automatic-test", false, false),
                TIMEOUT,
            )
            .await?,
    );
    wait(workers, || planner.active.load(Ordering::Acquire) == 1).await?;
    workers[first]
        .take()
        .context("blocked owner")?
        .shutdown()
        .await;
    ensure!(planner.active.load(Ordering::Acquire) == 0);
    planner.block.store(false, Ordering::Release);
    // Only the scheduler's selection can write Fleet here. Lose the real Root
    // CAS confirmation before it can enter either child's birth admission.
    cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    workers[first] = Some(
        coordinator
            .start_partition_service(
                &cluster.hosts[first],
                planner.clone(),
                selection::policy(),
                service_options("automatic-test", false, false),
                TIMEOUT,
            )
            .await?,
    );
    let duplicate = coordinator
        .start_partition_service(
            &cluster.hosts[first],
            planner.clone(),
            selection::policy(),
            service_options("automatic-test", false, false),
            TIMEOUT,
        )
        .await;
    ensure!(matches!(duplicate, Err(error) if error.code == RaftFleetFailureCode::Busy));
    wait(workers, || {
        workers[first].as_ref().is_some_and(|w| {
            w.status()
                .last_failure
                .as_ref()
                .is_some_and(|e| e.code == RaftFleetFailureCode::Unavailable)
        })
    })
    .await?;
    workers[first]
        .take()
        .context("first owner")?
        .shutdown()
        .await;
    ensure!(planner.plans.load(Ordering::Acquire) == 1);
    ensure!(!cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .load(Ordering::Acquire));
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    for child in &template.children {
        let history = GroupHistory {
            group: GroupId::new(
                hex::decode(&child.group.group_id)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("group ID"))?,
            )?,
            incarnation: IncarnationId::new(
                hex::decode(&child.group.incarnation_id)?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("incarnation ID"))?,
            )?,
            ..cluster.history
        };
        ensure!(
            matches!(
                cluster.fixtures[0]
                    .catalog
                    .history(&cut, &history, TIMEOUT)
                    .await,
                Err(FleetCatalogError::Conflict)
            ),
            "selection must precede child reservations"
        );
    }
    let pressures = planner.pressures.load(Ordering::Acquire);
    planner.refuse.store(true, Ordering::Release);
    // Different node, cold service and no retained operation argument. It must
    // discover the original selection and admit without either callback.
    workers[3] = Some(
        coordinator
            .start_partition_service(
                &cluster.hosts[3],
                planner.clone(),
                selection::policy(),
                service_options("automatic-test", false, true),
                TIMEOUT,
            )
            .await?,
    );
    wait(workers, || {
        workers[3]
            .as_ref()
            .is_some_and(|w| w.status().last_action == Some("await-consent"))
    })
    .await?;
    ensure!(
        planner.pressures.load(Ordering::Acquire) == pressures
            && planner.plans.load(Ordering::Acquire) == 1
    );
    let status = coordinator
        .transition_status(&template.namespace_id, &template.operation_id, TIMEOUT)
        .await?;
    ensure!(status.phase == Some(TransitionPhase::Planned));
    ensure!(cluster.hosts[3..]
        .iter()
        .all(|host| host.resource_usage().host.controller.groups == 0));
    workers[3]
        .take()
        .context("admission owner")?
        .shutdown()
        .await;
    // Complete never consents to another principal's selected work.
    workers[3] = Some(
        coordinator
            .start_partition_service(
                &cluster.hosts[3],
                planner.clone(),
                selection::policy(),
                service_options("other-principal", true, true),
                TIMEOUT,
            )
            .await?,
    );
    tokio::time::timeout(TIMEOUT, async {
        while workers[3]
            .as_ref()
            .is_some_and(|w| w.status().running || w.status().rounds == 0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let refused = workers[3].as_ref().context("foreign owner")?.status();
    ensure!(
        refused.needs_operator
            && refused
                .last_failure
                .is_some_and(|e| e.code == RaftFleetFailureCode::Forbidden)
    );
    workers[3].take().context("foreign owner")?.shutdown().await;
    ensure!(
        coordinator
            .transition_status(&template.namespace_id, &template.operation_id, TIMEOUT)
            .await?
            .phase
            == Some(TransitionPhase::Planned)
    );
    planner.refuse.store(false, Ordering::Release);
    planner.block.store(true, Ordering::Release);
    for (i, host) in cluster.hosts.iter().enumerate() {
        let endpoint = Box::pin(control(cluster, rooted, roots)).await?;
        workers[i] = Some(
            endpoint
                .start_partition_service(
                    host,
                    planner.clone(),
                    selection::policy(),
                    service_options("automatic-test", i == 3, i == 3),
                    TIMEOUT,
                )
                .await?,
        );
    }
    // Wait for the actual plan and child archives, with the original 60s gate.
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            for worker in workers.iter().flatten() {
                ensure!(!worker.status().needs_operator, "{:?}", worker.status());
            }
            let status = coordinator
                .transition_status(&template.namespace_id, &template.operation_id, TIMEOUT)
                .await?;
            if status.phase == Some(TransitionPhase::Retired) && status.child_archives_verified {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("automatic split completion")??;
    // Stop before terminal cursor settlement permits another observation. The
    // service must preserve original identities throughout physical execution.
    for worker in workers.iter_mut().filter_map(Option::take) {
        worker.shutdown().await;
    }
    ensure!(
        planner.active.load(Ordering::Acquire) == 0 && planner.plans.load(Ordering::Acquire) == 1
    );
    for (i, (cell, original)) in seed.cells.iter().zip(&seed.receipts).enumerate() {
        let key = cell_routing_key(cell.namespace, &cell.cell);
        let mut selected = None;
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
            if birth.route().range.contains(&key) {
                selected = Some(history);
            }
        }
        let history = selected.context("cell's fresh history")?;
        let elected = leader(&cluster.hosts[3..], history).await? + 3;
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
                && duplicate.version == original.version
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
        ensure!(
            read.result == original.result
                && read.version == original.version
                && read.served_by_node == cluster.hosts[elected].identity()
        );
        let archived = archive_increment(&handle, seed.retry).await?;
        ensure!(
            archived.result == ([8i64, 12][i]).to_be_bytes()
                && archived.durability == Durability::S3
                && archived.witness.group == history
        );
    }
    Ok(())
}
