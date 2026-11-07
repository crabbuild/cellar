// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Capacity refusal precedes sealing, retains reservations and leaves SQL live.
use super::*;
use cellar::application::consensus::verification::FleetHistoryState;
use cellar_logic::raft_fleet::{LedgerError, ResourceKind};

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Parent,
    Reservation,
    PlatformStorage,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_capacity_keeps_parent_writable() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            Scenario::Parent,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_capacity_counts_unmaterialized_births() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            Scenario::Reservation,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_split_capacity_keeps_parent_writable() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "capacity-parent",
            rooted,
        )?));
        Box::pin(qualify(rooted, store, Scenario::Parent)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_split_capacity_counts_unmaterialized_births() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "capacity-pending",
            rooted,
        )?));
        Box::pin(qualify(rooted, store, Scenario::Reservation)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_capacity_respects_platform_storage() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            Scenario::PlatformStorage,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_split_capacity_respects_platform_storage() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "capacity-storage",
            rooted,
        )?));
        Box::pin(qualify(rooted, store, Scenario::PlatformStorage)).await?;
    }
    Ok(())
}

async fn qualify(rooted: bool, store: Arc<catalog_fault::Store>, scenario: Scenario) -> Result<()> {
    let mut first =
        managed_fixture::Fixture::with_store(rooted, managed_fixture::counter_class()?, store)
            .await?;
    if matches!(scenario, Scenario::PlatformStorage) {
        first.image_bytes = 512 * 1024 * 1024;
    }
    let cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        true,
        None,
        managed_fixture::FirstGroupSetup::Normal,
    ))
    .await?;
    let mut worker = None;
    let mut roots = Vec::new();
    let result = Box::pin(exercise(
        &cluster,
        rooted,
        scenario,
        &mut worker,
        &mut roots,
    ))
    .await;
    if let Some(worker) = worker {
        worker.shutdown().await;
    }
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result.with_context(|| {
        format!("pre-seal capacity, extents={rooted}, scenario={scenario:?}; joined={joined:?}")
    })?;
    joined?;
    Ok(())
}

async fn exercise(
    cluster: &ManagedCluster,
    rooted: bool,
    scenario: Scenario,
    worker: &mut Option<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let f = &cluster.fixtures[0];
    let reserved_debt = matches!(scenario, Scenario::Reservation);
    let expected_kind = if matches!(scenario, Scenario::PlatformStorage) {
        ResourceKind::DurableBytes
    } else {
        ResourceKind::EngineSlots
    };
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let debt = if reserved_debt {
        let snapshot = f.catalog.read(TIMEOUT).await?;
        let reserved = f
            .catalog
            .reserve_birth(
                &snapshot,
                &FleetRequestIdentity {
                    id: [219; 16],
                    operation: [220; 16],
                    principal: "prior-controller".into(),
                    expected_revision: snapshot.revision(),
                },
                &cluster.directory,
                CellGroupBirthSpec {
                    history: GroupHistory {
                        group: GroupId::new([219; 16])?,
                        incarnation: IncarnationId::new([219; 16])?,
                        ..cluster.history
                    },
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(2)?,
                    creation: ProposalId::new([219; 16])?,
                    voters: cluster.fixtures[3..]
                        .iter()
                        .enumerate()
                        .map(|(i, fixture)| {
                            Ok(InitialMember {
                                node: fixture.spec.node,
                                member: MemberId::new(i as u64 + 1)?,
                            })
                        })
                        .collect::<Result<_>>()?,
                    learners: vec![],
                },
                "Counter",
                TIMEOUT,
            )
            .await?;
        Some(reserved.authorization.route().history)
    } else {
        None
    };
    let coordinator = Box::pin(control(cluster, rooted, roots)).await?;
    let (request, admitted) = admit_split(
        cluster,
        &coordinator,
        if matches!(scenario, Scenario::Parent) {
            [0, 0]
        } else {
            [3, 3]
        },
    )
    .await?;
    let consent = RaftFleetTransitionExecutionRequest {
        catalog_id: coordinator.catalog_id().into(),
        namespace_id: request.namespace_id.clone(),
        operation_id: request.operation_id.clone(),
        request_id: hex::encode([236; 16]),
        expected_revision: f.catalog.read(TIMEOUT).await?.revision(),
        plan_digest: admitted.plan_digest.clone(),
    };
    coordinator
        .authorize_transition_execution("automatic-test", &consent, TIMEOUT)
        .await?;
    *worker = Some(
        coordinator
            .start_transition_service(&cluster.hosts[3], options(true, true), TIMEOUT)
            .await?,
    );
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let status = worker.as_ref().context("coordinator")?.status();
            ensure!(!status.needs_operator, "{status:?}");
            if status.phase == Some(TransitionPhase::Prepared)
                && status
                    .last_failure
                    .as_ref()
                    .is_some_and(|error| error.code == RaftFleetFailureCode::Blocked)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| {
        format!(
            "capacity must refuse before source sealing: {:?}",
            worker.as_ref().map(|worker| worker.status())
        )
    })??;
    worker.take().context("coordinator")?.shutdown().await;

    // A replacement management client loses no original operation identity.
    let reopened = Box::pin(control(cluster, rooted, roots)).await?;
    let before = reopened
        .transition_status(&request.namespace_id, &request.operation_id, TIMEOUT)
        .await?;
    ensure!(
        before.phase == Some(TransitionPhase::Prepared)
            && before.fleet_slot_held
            && !before.parent_holds_released
    );
    let seal = RaftFleetTransitionSealRequest {
        catalog_id: coordinator.catalog_id().into(),
        namespace_id: request.namespace_id.clone(),
        operation_id: request.operation_id.clone(),
        request_id: hex::encode([237; 16]),
        plan_digest: admitted.plan_digest,
        preparation_digest: before
            .preparation_digest
            .clone()
            .context("durable preparation")?,
    };
    let refused = reopened
        .authorize_transition_seal(
            "automatic-test",
            &request.namespace_id,
            &request.operation_id,
            &seal,
            TIMEOUT,
        )
        .await;
    ensure!(
        matches!(&refused, Err(error) if error.code == RaftFleetFailureCode::Blocked),
        "{refused:?}"
    );
    let selected = f
        .catalog
        .reopen_managed_transition(&cluster.directory, ProposalId::new([231; 16])?, TIMEOUT)
        .await?
        .context("rooted original plan")?;
    let refused = f
        .catalog
        .authorize_managed_transition_seal(
            &selected.request,
            &cluster.directory,
            &selected.plan,
            &cluster.hosts[0],
            TIMEOUT,
        )
        .await;
    ensure!(
        matches!(
            &refused,
            Err(FleetCatalogError::Resource(LedgerError::CapacityExceeded(kind))) if *kind == expected_kind
        ),
        "capacity refusal: {refused:?}"
    );
    ensure!(
        reopened
            .transition_status(&request.namespace_id, &request.operation_id, TIMEOUT)
            .await?
            == before
    );
    for (i, host) in cluster.hosts.iter().enumerate() {
        let sample = host
            .inspect_peer_physical_inventory(host.identity().node, 1, TIMEOUT)
            .await?;
        ensure!(sample.view.node.groups == u32::from(i < 3));
        let inventory = sample.inventory.context("complete physical inventory")?;
        ensure!(inventory.histories().len() == usize::from(i < 3));
        if i < 3 {
            let row = inventory
                .group_history(&cluster.history)
                .context("unsealed parent")?;
            ensure!(!row.source_retirement_applied() && !row.parked_retired_source());
        }
        if let Some(debt) = debt {
            ensure!(inventory.group_history(&debt).is_none());
        }
    }
    let leader = leader(&cluster.hosts[..3], cluster.history).await?;
    let handle = cluster.hosts[leader].cell(seed.cells[0].clone())?;
    let receipt = handle
        .command(
            &handle.contract("Add")?,
            1i64.to_be_bytes().to_vec(),
            CommandOptions::new(CommandId::new(ClientId::new([251; 16])?, 1)?, seed.retry),
        )
        .await?;
    ensure!(
        receipt.result == 8i64.to_be_bytes()
            && receipt.durability == Durability::Quorum
            && receipt.witness.group == cluster.history
    );
    let read = handle
        .query(
            ProposalId::new([252; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                routing: QueryRouting::Local,
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await?;
    ensure!(read.result == receipt.result && read.version == receipt.version);
    // No source seal was selected: the original plan remains cancellable.
    let snapshot = f.catalog.read(TIMEOUT).await?;
    let cancel = FleetRequestIdentity {
        id: [253; 16],
        expected_revision: snapshot.revision(),
        ..selected.request.clone()
    };
    f.catalog
        .cancel_managed_transition(
            &snapshot,
            &cancel,
            &cluster.directory,
            &selected.plan,
            TIMEOUT,
        )
        .await?;
    let cancelled = reopened
        .transition_status(&request.namespace_id, &request.operation_id, TIMEOUT)
        .await?;
    ensure!(cancelled.phase == Some(TransitionPhase::Cancelled) && !cancelled.fleet_slot_held);
    if let Some(debt) = debt {
        let snapshot = f.catalog.read(TIMEOUT).await?;
        let original = f.catalog.history(&snapshot, &debt, TIMEOUT).await?;
        ensure!(*original.state() == FleetHistoryState::Reserved);
    }
    Ok(())
}
