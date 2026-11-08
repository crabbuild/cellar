// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root cancellation competes with actual delayed child reservation CAS writes.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::CellGroupBirthSpec, InitialMember,
};
use cellar::application::consensus::verification::{
    FleetChange, FleetHistoryState, FleetMutation, FleetTable,
};
use cellar_logic::{
    cell_group::{CellRange, RouteEpoch},
    consensus::{GroupId, IncarnationId, MemberId},
};

struct Resume<'a>(&'a catalog_fault::Store);

impl Drop for Resume<'_> {
    fn drop(&mut self) {
        self.0.fleet_cas_hang.store(false, Ordering::Release);
        self.0.fleet_cas_resume.notify_waiters();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_partition_cancellation_fences_delayed_admission() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            0,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_partition_cancellation_recovers_birth_and_settlement() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            1,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_cancellation_fences_transition_slot_admission() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            2,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_staged_cancellation_fences_transition_slot_admission() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            2,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "slot-cancellation",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_staged_partition_cancellation_fences_delayed_admission() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            0,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "staged-cancellation",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_partial_partition_cancellation_recovers_birth_and_settlement() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(qualify(
            rooted,
            1,
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "partial-cancellation",
                rooted,
            )?)),
        ))
        .await?;
    }
    Ok(())
}

async fn qualify(
    rooted: bool,
    reservations: usize,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let cluster = Box::pin(ManagedCluster::start_with_catalog_clients(
        rooted,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        store,
        true,
    ))
    .await?;
    let mut roots = Vec::new();
    let result = Box::pin(exercise(&cluster, rooted, reservations, &mut roots)).await;
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result.with_context(|| {
        format!(
            "staged cancellation extents={rooted}, reservations={reservations}, joined={joined:?}"
        )
    })?;
    joined
}

async fn recommend(
    cluster: &ManagedCluster,
    control: &RaftFleetControl,
) -> Result<(
    RaftFleetPartitionScheduler,
    RaftFleetTransitionAdmissionRequest,
)> {
    let first = leader(&cluster.hosts[..3], cluster.history).await?;
    let mut scheduler = RaftFleetPartitionScheduler::new(
        control
            .clone()
            .with_node_observer(cluster.hosts[first].clone()),
        selection::policy(),
        1,
    )?;
    let pivot = tokio::time::timeout(TIMEOUT, async {
        loop {
            let poll = scheduler
                .poll(
                    RaftFleetPartitionPressure {
                        placement_class: [1; 32],
                        leader_cpu_bps: 0,
                        failure_domain_load_bps: 0,
                    },
                    TIMEOUT,
                )
                .await?;
            if let Some(RaftFleetPartitionRecommendation::Split { pivot, .. }) = poll.recommendation
            {
                return Ok::<_, anyhow::Error>(pivot);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    let request = split_request(cluster, control, [3, 3], pivot).await?;
    Ok((scheduler, request))
}

fn history(scope: GroupHistory, child: &RaftFleetTransitionChild) -> Result<GroupHistory> {
    Ok(GroupHistory {
        group: GroupId::new(hex::decode(&child.group.group_id)?.as_slice().try_into()?)?,
        incarnation: IncarnationId::new(
            hex::decode(&child.group.incarnation_id)?
                .as_slice()
                .try_into()?,
        )?,
        ..scope
    })
}

async fn exercise(
    cluster: &ManagedCluster,
    rooted: bool,
    reservations: usize,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let partial = reservations > 0;
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let endpoint = Box::pin(control(cluster, rooted, roots)).await?;
    let delayed_endpoint = Box::pin(control(cluster, rooted, roots)).await?;
    let (mut scheduler, request) = recommend(cluster, &endpoint).await?;
    scheduler.stage("automatic-test", &request, TIMEOUT).await?;
    let f = &cluster.fixtures[0];
    let first = history(cluster.history, &request.children[0])?;
    let second = history(cluster.history, &request.children[1])?;
    for _ in 0..reservations {
        f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        let lost = Box::pin(scheduler.admit("automatic-test", &request, TIMEOUT)).await;
        ensure!(matches!(lost, Err(error) if error.code == RaftFleetFailureCode::Unavailable));
    }
    let authorization = if partial {
        let cut = f.catalog.read(TIMEOUT).await?;
        ensure!(
            *f.catalog.history(&cut, &first, TIMEOUT).await?.state() == FleetHistoryState::Reserved
        );
        Some(
            f.catalog
                .recover_birth_reservation(&cut, &cluster.directory, &first, TIMEOUT)
                .await?,
        )
    } else {
        None
    };
    let before = f.catalog.read(TIMEOUT).await?;
    let waiting = if partial { second } else { first };
    if reservations < 2 {
        ensure!(matches!(
            f.catalog.history(&before, &waiting, TIMEOUT).await,
            Err(FleetCatalogError::Conflict)
        ));
    } else {
        ensure!(
            *f.catalog.history(&before, &second, TIMEOUT).await?.state()
                == FleetHistoryState::Reserved
        );
    }
    f.store.fleet_cas_hang.store(true, Ordering::Release);
    f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let resume = Resume(&f.store);
    let mut delayed = Box::pin(delayed_endpoint.admit_transition(
        "automatic-test",
        &request.namespace_id,
        &request.operation_id,
        &request,
        TIMEOUT,
    ));
    f.domain.run(async {
        tokio::time::timeout(TIMEOUT, async {
            cellar::asyncrt::select! {
                result = &mut delayed => anyhow::bail!("admission returned before actual CAS: {result:?}"),
                _ = async { while !f.store.fleet_cas_waiting.load(Ordering::Acquire) { tokio::time::sleep(Duration::from_millis(5)).await; } } => Ok(()),
            }
        }).await
    }).await???;
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == before.revision());
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = Box::pin(scheduler.cancel_pending("automatic-test", TIMEOUT)).await;
    ensure!(
        matches!(&lost, Err(error) if error.code == RaftFleetFailureCode::Unavailable),
        "{lost:?}"
    );
    ensure!(scheduler.pending_request() == Some(&request));
    let fenced = f.catalog.read(TIMEOUT).await?;
    ensure!(fenced.revision() == before.revision() + 1);
    ensure!(f
        .catalog
        .get(
            &fenced,
            FleetTable::Operations,
            &format!("partition-work/cancellation/{}", request.operation_id),
            TIMEOUT
        )
        .await?
        .is_some());
    drop(resume);
    let refused = delayed.await;
    ensure!(
        matches!(&refused, Err(error) if error.code == RaftFleetFailureCode::Conflict),
        "{refused:?}"
    );
    // A fresh root cannot bypass the permanent fence after the delayed CAS
    // lost. Use direct public admission, independent of scheduler prechecks.
    let fresh = Box::pin(delayed_endpoint.admit_transition(
        "automatic-test",
        &request.namespace_id,
        &request.operation_id,
        &request,
        TIMEOUT,
    ))
    .await;
    ensure!(matches!(fresh, Err(error) if error.code == RaftFleetFailureCode::Conflict));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == fenced.revision());
    if reservations < 2 {
        ensure!(matches!(
            f.catalog.history(&fenced, &waiting, TIMEOUT).await,
            Err(FleetCatalogError::Conflict)
        ));
    } else {
        ensure!(
            *f.catalog.history(&fenced, &second, TIMEOUT).await?.state()
                == FleetHistoryState::Reserved
        );
    }
    let foreign = Box::pin(endpoint.cancel_staged_partition("foreign", &request, TIMEOUT)).await;
    ensure!(matches!(foreign, Err(error) if error.code == RaftFleetFailureCode::Forbidden));
    let mut changed = request.clone();
    changed.children[0].creation_id = hex::encode([250; 16]);
    let foreign =
        Box::pin(endpoint.cancel_staged_partition("automatic-test", &changed, TIMEOUT)).await;
    ensure!(matches!(foreign, Err(error) if error.code == RaftFleetFailureCode::PayloadConflict));
    let forged = FleetMutation {
        id: [247; 16],
        operation: [247; 16],
        principal: "automatic-test".into(),
        expected_revision: fenced.revision(),
        changes: vec![FleetChange {
            table: FleetTable::Operations,
            key: format!("partition-work/cancellation/{}", request.operation_id),
            value: None,
        }],
    };
    ensure!(matches!(
        f.catalog.commit(&fenced, &forged, TIMEOUT).await,
        Err(FleetCatalogError::Invalid(_))
    ));
    if partial {
        let forged = FleetMutation {
            id: [246; 16],
            operation: [246; 16],
            principal: "automatic-test".into(),
            expected_revision: fenced.revision(),
            changes: vec![FleetChange {
                table: FleetTable::Histories,
                key: f.catalog.group_binding(&first)?.key()?,
                value: None,
            }],
        };
        ensure!(matches!(
            f.catalog.commit(&fenced, &forged, TIMEOUT).await,
            Err(FleetCatalogError::Conflict)
        ));
        let cold = Box::pin(control(cluster, rooted, roots)).await?;
        let mut cold = RaftFleetPartitionScheduler::new(cold, selection::policy(), 1)?;
        let (lost, delivered) =
            catalog_fault::lose_group_birth_confirmation(Box::pin(cold.resume_pending(TIMEOUT)))
                .await;
        ensure!(
            delivered
                && matches!(&lost, Err(error) if error.code == RaftFleetFailureCode::Unavailable),
            "birth confirmation: {lost:?}, delivered={delivered}"
        );
        ensure!(cold.pending_request() == Some(&request));
        ensure!(f
            .catalog
            .get(
                &f.catalog.read(TIMEOUT).await?,
                FleetTable::Operations,
                "partition-work/active",
                TIMEOUT
            )
            .await?
            .is_some());
        ensure!(matches!(
            cluster
                .directory
                .materialize_fleet_group_birth(authorization.as_ref().context("reserved child")?)
                .await,
            Err(cellar::application::consensus::verification::CatalogError::BirthCancelled)
        ));
    }
    let cold = Box::pin(control(cluster, rooted, roots)).await?;
    let mut cold = RaftFleetPartitionScheduler::new(cold, selection::policy(), 1)?;
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = Box::pin(cold.resume_pending(TIMEOUT)).await;
    ensure!(
        matches!(&lost, Err(error) if error.code == RaftFleetFailureCode::Unavailable),
        "settlement confirmation: {lost:?}"
    );
    let completed = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .get(
            &completed,
            FleetTable::Operations,
            "partition-work/active",
            TIMEOUT
        )
        .await?
        .is_none());
    ensure!(f
        .catalog
        .get(
            &completed,
            FleetTable::Operations,
            &format!("partition-work/finished/{}", request.operation_id),
            TIMEOUT
        )
        .await?
        .is_some());
    ensure!(
        Box::pin(cold.resume_pending(TIMEOUT)).await?.is_none() && cold.pending_request().is_none()
    );
    if partial {
        ensure!(
            *f.catalog
                .history(&completed, &first, TIMEOUT)
                .await?
                .state()
                == FleetHistoryState::Reserved,
            "cancellation refunded or rewrote original birth debt"
        );
        ensure!(matches!(
            cluster
                .directory
                .materialize_fleet_group_birth(authorization.as_ref().context("reserved child")?)
                .await,
            Err(cellar::application::consensus::verification::CatalogError::BirthCancelled)
        ));
        let proposal = f
            .catalog
            .history(&completed, &first, TIMEOUT)
            .await?
            .initial_placement()?;
        ensure!(matches!(
            f.catalog
                .reserve_birth(
                    &completed,
                    &FleetRequestIdentity {
                        id: [245; 16],
                        operation: [245; 16],
                        principal: "automatic-test".into(),
                        expected_revision: completed.revision()
                    },
                    &cluster.directory,
                    proposal,
                    "Counter",
                    TIMEOUT
                )
                .await,
            Err(FleetCatalogError::Conflict)
        ));
    }
    ensure!(
        hex::encode(cluster.directory.observe().await?.digest())
            == request.expected_directory_digest
    );
    if reservations == 2 {
        ensure!(
            *f.catalog
                .history(&completed, &second, TIMEOUT)
                .await?
                .state()
                == FleetHistoryState::Reserved
        );
        let authorization = f
            .catalog
            .recover_birth_reservation(&completed, &cluster.directory, &second, TIMEOUT)
            .await?;
        ensure!(matches!(
            cluster
                .directory
                .materialize_fleet_group_birth(&authorization)
                .await,
            Err(cellar::application::consensus::verification::CatalogError::BirthCancelled)
        ));
    }
    let current = leader(&cluster.hosts[..3], cluster.history).await?;
    for (cell, original) in seed.cells.iter().zip(&seed.receipts) {
        let handle = cluster.hosts[current].cell(cell.clone())?;
        let replay = handle
            .command(
                &handle.contract("Add")?,
                original.result.clone(),
                CommandOptions::new(CommandId::new(ClientId::new([200; 16])?, 1)?, seed.retry),
            )
            .await?;
        ensure!(replay.result == original.result && replay.version == original.version);
        let fresh = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([200; 16])?, 2)?, seed.retry),
            )
            .await?;
        ensure!(fresh.version.sequence.get() == original.version.sequence.get() + 1);
        let read = handle
            .query(
                ProposalId::new([240; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(read.result == fresh.result && read.version == fresh.version);
    }
    // A proposed identity is not ownership. A later selected plan may be
    // permanently inadmissible because another operation owns a child name.
    // Cancelling that plan must leave the other operation and its debt intact.
    let cut = f.catalog.read(TIMEOUT).await?;
    let unrelated = f
        .catalog
        .reserve_birth(
            &cut,
            &FleetRequestIdentity {
                id: [249; 16],
                operation: [249; 16],
                principal: "other-owner".into(),
                expected_revision: cut.revision(),
            },
            &cluster.directory,
            CellGroupBirthSpec {
                history: GroupHistory {
                    group: GroupId::new([243; 16])?,
                    incarnation: IncarnationId::new([243; 16])?,
                    ..cluster.history
                },
                range: CellRange::all(),
                epoch: RouteEpoch::new(2)?,
                creation: ProposalId::new([243; 16])?,
                voters: cluster.fixtures[..3]
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
    let (mut next, mut new_request) = recommend(cluster, &endpoint).await?;
    new_request.operation_id = hex::encode([241; 16]);
    new_request.request_id = hex::encode([242; 16]);
    for (i, child) in new_request.children.iter_mut().enumerate() {
        let id = 243 + i as u8;
        child.group.group_id = hex::encode([id; 16]);
        child.group.incarnation_id = hex::encode([id; 16]);
        child.creation_id = hex::encode([id; 16]);
        child.request_id = hex::encode([id; 16]);
    }
    next.stage("automatic-test", &new_request, TIMEOUT).await?;
    let selected = f.catalog.read(TIMEOUT).await?;
    Box::pin(endpoint.cancel_staged_partition("automatic-test", &request, TIMEOUT)).await?;
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == selected.revision());
    let restored = Box::pin(control(cluster, rooted, roots)).await?;
    let mut restored = RaftFleetPartitionScheduler::new(restored, selection::policy(), 1)?;
    restored
        .poll(
            RaftFleetPartitionPressure {
                placement_class: [1; 32],
                leader_cpu_bps: 0,
                failure_domain_load_bps: 0,
            },
            TIMEOUT,
        )
        .await?;
    ensure!(
        restored.pending_request() == Some(&new_request),
        "historical cancellation erased newer work"
    );
    Box::pin(restored.cancel_pending("automatic-test", TIMEOUT)).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(
        *f.catalog
            .history(&cut, unrelated.authorization.scope(), TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Reserved
    );
    cluster
        .directory
        .materialize_fleet_group_birth(&unrelated.authorization)
        .await?;
    let confirmed = f
        .catalog
        .confirm_birth(
            &cut,
            &FleetRequestIdentity {
                id: [250; 16],
                operation: [249; 16],
                principal: "other-owner".into(),
                expected_revision: cut.revision(),
            },
            &cluster.directory,
            &unrelated.authorization,
            TIMEOUT,
        )
        .await?;
    ensure!(
        *f.catalog
            .history(
                &confirmed.snapshot,
                unrelated.authorization.scope(),
                TIMEOUT
            )
            .await?
            .state()
            == FleetHistoryState::Live,
        "cancellation fenced another operation's child"
    );
    Ok(())
}
