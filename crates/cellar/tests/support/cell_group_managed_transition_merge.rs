// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Merge the live children of a closed split and retire both sources.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::{
        CellGroupArchiveHold, CellGroupArchiveTailLimits, CellGroupArchiveTailPool, CellGroupBirth,
        CellGroupCheckpointPool, CellGroupTransitionSource, GroupRoute,
    },
    FleetHistoryState, FleetSnapshot,
};
use cellar::sdk::fleet_operations::{
    RaftFleetControl, RaftFleetFailureCode, RaftFleetFirstGroupVoter, RaftFleetOutcome,
    RaftFleetTransitionAdmissionRequest, RaftFleetTransitionChild, RaftFleetTransitionGroup,
    RaftFleetTransitionPartition, RaftFleetTransitionPublishRequest, RaftFleetTransitionResume,
    RaftFleetTransitionResumeStep,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_operator_split_reserves_children_in_key_order() -> Result<()> {
    let cluster = Box::pin(ManagedCluster::start(false, &[7, 8, 9])).await?;
    let result = Box::pin(check_operator_split_admission(&cluster)).await;
    let joined = close(cluster.nodes, &cluster.hosts, None).await;
    cluster.fixtures[0].domain.shutdown().await;
    result?;
    joined
}

async fn check_operator_split_admission(cluster: &ManagedCluster) -> Result<()> {
    let fixture = &cluster.fixtures[0];
    let directory = &cluster.directory;
    let control = RaftFleetControl::new(
        &fixture.domain,
        fixture.catalog.clone(),
        vec![directory.clone()],
    )?;
    let namespace = hex::encode(cluster.history.namespace.bytes());
    let basis = control.transition_basis(&namespace, TIMEOUT).await?;
    let principal = request(234, basis.fleet_revision).principal;
    let parent = fixture
        .domain
        .run(directory.materialize_selected_history(cluster.history))
        .await??;
    let pivot_cell = "Counter:operator-split";
    let pivot = cell_routing_key(cluster.history.namespace, &CellId::new(pivot_cell)?);
    let (left, right) = parent.route().range.split(pivot)?;
    let operation = ProposalId::new([231; 16])?;
    let histories = [232, 233].map(|id| -> Result<_> {
        Ok(GroupHistory {
            group: GroupId::new([id; 16])?,
            incarnation: IncarnationId::new([id; 16])?,
            ..cluster.history
        })
    });
    let [left_history, right_history] = histories;
    let histories = [left_history?, right_history?];
    let admission = RaftFleetTransitionAdmissionRequest {
        catalog_id: basis.catalog_id,
        namespace_id: namespace.clone(),
        operation_id: hex::encode(operation.bytes()),
        request_id: hex::encode([234; 16]),
        expected_fleet_revision: basis.fleet_revision,
        expected_directory_digest: basis.directory_digest,
        class: "Counter".into(),
        partition: RaftFleetTransitionPartition::Split {
            parent: transition_group(cluster.history),
            pivot_cell: pivot_cell.into(),
        },
        children: histories
            .iter()
            .enumerate()
            .map(|(index, history)| RaftFleetTransitionChild {
                group: transition_group(*history),
                request_id: hex::encode([232 + index as u8; 16]),
                creation_id: hex::encode([232 + index as u8; 16]),
                voters: std::array::from_fn(|member| RaftFleetFirstGroupVoter {
                    node_id: hex::encode(cluster.fixtures[3 + member].spec.node.bytes()),
                    member_id: member as u64 + 1,
                }),
            })
            .collect(),
    };
    let invalid = control
        .admit_transition("", &namespace, &admission.operation_id, &admission, TIMEOUT)
        .await;
    ensure!(matches!(invalid, Err(failure) if failure.code == RaftFleetFailureCode::InvalidInput));
    ensure!(fixture.catalog.read(TIMEOUT).await?.revision() == basis.fleet_revision);
    let admitted = crate::managed_fixture::operator::admit_transition(
        fixture, directory, &principal, &admission,
    )
    .await?;
    let cut = fixture.catalog.read(TIMEOUT).await?;
    ensure!(
        admitted.phase == TransitionPhase::Planned && cut.revision() == basis.fleet_revision + 3
    );
    for (history, range) in histories.iter().zip([left, right]) {
        ensure!(
            *fixture
                .catalog
                .history(&cut, history, TIMEOUT)
                .await?
                .state()
                == FleetHistoryState::Reserved
        );
        let birth = fixture
            .catalog
            .recover_birth_reservation(&cut, directory, history, TIMEOUT)
            .await?;
        ensure!(birth.route().range == range && birth.route().epoch == RouteEpoch::new(2)?);
    }
    let view = fixture.domain.run(directory.observe()).await??;
    ensure!(view.revision() == basis.directory_revision && view.groups() == 1);
    let selected = fixture
        .catalog
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("admitted split plan")?;
    let cancellation = FleetRequestIdentity {
        operation: operation.bytes(),
        ..request(235, cut.revision())
    };
    fixture
        .catalog
        .cancel_managed_transition(&cut, &cancellation, directory, &selected.plan, TIMEOUT)
        .await?;
    let cancelled = control
        .admit_transition(
            &principal,
            &namespace,
            &admission.operation_id,
            &admission,
            TIMEOUT,
        )
        .await?;
    ensure!(
        cancelled.phase == TransitionPhase::Cancelled
            && cancelled.plan_digest == admitted.plan_digest
    );
    Ok(())
}

fn transition_group(history: GroupHistory) -> RaftFleetTransitionGroup {
    RaftFleetTransitionGroup {
        group_id: hex::encode(history.group.bytes()),
        incarnation_id: hex::encode(history.incarnation.bytes()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_split_then_merge_replays_late_writes() -> Result<()> {
    Box::pin(run_plan(
        false,
        Arc::new(fault::Store::default()),
        Decision::SplitMerge,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_split_merge() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-split-merge",
            rooted,
        )?));
        Box::pin(run_plan(rooted, store, Decision::SplitMerge))
            .await
            .with_context(|| format!("managed split and merge, extent images={rooted}"))?;
    }
    Ok(())
}

struct ArchiveInputs {
    domain: Arc<ExecutionDomain>,
    pool: CellGroupArchivePool,
    tails: CellGroupArchiveTailPool,
    scratch: tempfile::TempDir,
    rooted: bool,
}

impl ArchiveInputs {
    async fn sources(
        &self,
        stage: &str,
        cuts: &[(CellGroupBirth, CellGroupArchiveHold)],
    ) -> Result<Vec<CellGroupTransitionSource>> {
        let mut sources = Vec::with_capacity(cuts.len());
        for (index, (birth, hold)) in cuts.iter().enumerate() {
            let base = self.scratch.path().join(stage).join(index.to_string());
            let route = birth.route().clone();
            let rooted = self.rooted;
            let target = self
                .domain
                .blocking(move || preparation::store(base, route, rooted))?
                .await??;
            sources.push(CellGroupTransitionSource {
                birth: birth.clone(),
                hold: hold.clone(),
                receiver: CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
                    target.store,
                    target.scratch,
                    target.directory,
                )?,
            });
        }
        Ok(sources)
    }
}

pub(super) fn run<'a>(
    cluster: &'a mut ManagedCluster,
    directory: &'a RangeDirectory,
    cells: Vec<CellHistory>,
    rooted: bool,
) -> impl std::future::Future<Output = Result<GroupHistory>> + 'a {
    Box::pin(run_inner(cluster, directory, cells, rooted))
}

async fn run_inner(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    cells: Vec<CellHistory>,
    rooted: bool,
) -> Result<GroupHistory> {
    let fixture = &cluster.fixtures[0];
    let fleet = fixture.catalog.clone();
    let control = RaftFleetControl::new(&fixture.domain, fleet.clone(), vec![directory.clone()])?;
    let archives = ArchiveInputs {
        domain: fixture.domain.clone(),
        pool: CellGroupArchivePool::new(Default::default())?,
        // Both independently verified source cuts stay pinned through staging.
        tails: CellGroupArchiveTailPool::new(CellGroupArchiveTailLimits {
            max_operations: 2,
            max_retained_bytes: 2 * CellGroupArchiveTailLimits::default().max_retained_bytes,
            ..Default::default()
        })?,
        scratch: tempfile::tempdir()?,
        rooted,
    };
    let domain = &archives.domain;
    let view = domain.run(directory.observe()).await??;
    let page = domain.run(directory.group_page(&view, None, 3)).await??;
    ensure!(page.next.is_none());
    let parents: [GroupRoute; 2] = page
        .routes
        .try_into()
        .map_err(|_| anyhow::anyhow!("merge requires exactly two split children"))?;
    let mut routed = Vec::with_capacity(parents.len());
    for parent in &parents {
        let cell = cells
            .iter()
            .enumerate()
            .find(|(_, cell)| {
                parent
                    .range
                    .contains(&cell_routing_key(parent.history.namespace, &cell.cell))
            })
            .context("each split child must hold one cell")?;
        routed.push((cell.1.clone(), [12i64, 13][cell.0]));
    }
    for (index, (fixture, host)) in cluster.fixtures[..3]
        .iter()
        .zip(&cluster.hosts[..3])
        .enumerate()
    {
        admit_capacity(fixture, host, directory, 130 + 2 * index as u8).await?;
    }
    let operation = ProposalId::new([244; 16])?;
    let history = GroupHistory {
        group: GroupId::new([243; 16])?,
        incarnation: IncarnationId::new([243; 16])?,
        ..cluster.history
    };
    let (admitted, cut) = Box::pin(admit_merge(
        cluster, directory, &control, &parents, history, operation,
    ))
    .await?;
    let planned = fleet
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("operator admitted merge")?;
    ensure!(
        planned
            .journal
            .as_ref()
            .is_some_and(|journal| journal.phase() == TransitionPhase::Planned)
            && admitted.plan_digest == hex::encode(planned.plan.digest())
    );
    let child = fleet
        .recover_birth_reservation(&cut, directory, &history, TIMEOUT)
        .await?
        .route()
        .clone();
    let mut reused = child.clone();
    reused.history.group = parents[0].history.group;
    ensure!(directory
        .prepare_merge(&view, operation, &parents, &reused)
        .is_err());
    let mut incompatible = parents.clone();
    incompatible[1].schema_digest = [7; 32];
    ensure!(directory
        .prepare_merge(&view, operation, &incompatible, &child)
        .is_err());
    let mut gap = parents.clone();
    let gap_key = cell_routing_key(child.history.namespace, &CellId::new("Counter:gap-21")?);
    gap[1].range = gap[1]
        .range
        .split(gap_key)
        .context("gap fixture boundary")?
        .1;
    ensure!(directory
        .prepare_merge(&view, operation, &gap, &child)
        .is_err());
    let plan = planned.plan;
    let request = planned.request;

    let mut registries = Vec::with_capacity(2);
    for route in &parents {
        let birth = domain
            .run(directory.materialize_selected_history(route.history))
            .await??;
        let registry = directory
            .open_group_archive(domain.as_ref(), &archives.pool, &birth)
            .await?;
        registries.push(registry);
    }
    let staged =
        preparation::allocate_preparation_holds(&fleet, directory, operation, &archives.pool)
            .await?;
    ensure!(staged.phase() == TransitionPhase::Planned && staged.preparation().is_none());
    let (preparer, reopened_directory) = reopen_merge_controller(fixture).await?;
    let holds = preparer
        .staged_managed_preparation_holds(&reopened_directory, operation, TIMEOUT)
        .await?
        .context("selected merge preparation holds")?;
    ensure!(holds.iter().map(|hold| hold.marker().route()).eq(&parents));
    let workspace = tempfile::tempdir()?;
    let verifier = crate::managed_fixture::verifier::open(
        fixture,
        workspace.path(),
        rooted,
        CellGroupArchiveTailLimits {
            max_operations: 2,
            max_retained_bytes: 2 * CellGroupArchiveTailLimits::default().max_retained_bytes,
            ..Default::default()
        },
    )
    .await?;
    let prepared = preparer
        .resume_managed_preparation(
            &reopened_directory,
            operation,
            &verifier,
            preparation::recovery_pool()?,
            TIMEOUT,
        )
        .await
        .context("reopen both merge preparation archives")?;
    ensure!(prepared.journal().phase() == TransitionPhase::Prepared);
    ensure!(prepared.children().len() == 1 && prepared.children()[0].root().cells() == 2);
    drop(prepared);
    drop(verifier);
    drop(workspace);
    fleet
        .authorize_managed_transition_seal(&request, directory, &plan, TIMEOUT)
        .await?;
    let transition = Arc::new(
        fleet
            .reopen_managed_transition(directory, operation, TIMEOUT)
            .await?
            .context("reopened authorized merge")?,
    );
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(60_000)
    .context("merge retry deadline")?;
    let limits = CellGroupArchiveTailLimits {
        max_operations: parents.len(),
        max_retained_bytes: parents.len()
            * CellGroupArchiveTailLimits::default().max_retained_bytes,
        ..Default::default()
    };
    let sealed_workspace = tempfile::tempdir()?;
    let verifier = crate::managed_fixture::verifier::open(
        &cluster.fixtures[0],
        sealed_workspace.path(),
        rooted,
        limits,
    )
    .await?;
    let mut controller = (fleet.clone(), directory.clone());
    let mut receipts = Vec::with_capacity(2);
    for (index, (parent, (cell, expected))) in parents.iter().zip(&routed).enumerate() {
        let voters = &cluster.hosts[3..];
        let voter = leader(voters, parent.history).await?;
        let handle = voters[voter].cell(cell.clone())?;
        let receipt = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([202; 16])?, 1)?, retry),
            )
            .await?;
        ensure!(
            receipt.result == (expected + 1).to_be_bytes()
                && receipt.witness.group == parent.history
        );
        receipts.push(receipt);
        let seal = Box::pin(commit_source_seal(voters, &transition, parent.history)).await?;
        preparation::wait_transfer_applied(voters, &[(parent.history, seal.position().index)])
            .await?;
        wait_archive(voters, parent.history, &seal).await?;
        let hold = preparation::acquire_hold(domain, &registries[index]).await?;
        ensure!(hold.marker().source_seal() == Some(&seal));
        let selected = controller
            .0
            .reopen_managed_transition(&controller.1, operation, TIMEOUT)
            .await?
            .context("current seal controller")?;
        let birth = domain
            .run(controller.1.materialize_selected_history(parent.history))
            .await??;
        let cut = (birth, hold);
        let mut source = archives
            .sources(&format!("stage-{index}"), std::slice::from_ref(&cut))
            .await?;
        controller
            .0
            .stage_managed_sealed_source(
                &selected,
                &controller.1,
                &archives.pool,
                &archives.tails,
                source.pop().context("single sealed parent receiver")?,
                TIMEOUT,
            )
            .await?;
        if index == 0 {
            let active = Some(parents[1].history);
            Box::pin(restart_voter(cluster, 3, 120, active, 2))
                .await
                .context("restart a voter with one sealed merge parent")?;
            let fifth = Box::pin(stop_voter(cluster, 5, active)).await?;
            let fourth = Box::pin(stop_voter(cluster, 4, active)).await?;
            let survivor = &cluster.hosts[3];
            Box::pin(verify_minority(
                survivor,
                &transition,
                &seal,
                &parents,
                &routed,
                retry,
            ))
            .await?;
            ensure!(matches!(
                fleet
                    .verify_managed_sealed_sources(
                        &transition,
                        directory,
                        &archives.pool,
                        &archives.tails,
                        archives
                            .sources("partial", std::slice::from_ref(&cut))
                            .await?,
                        TIMEOUT,
                    )
                    .await,
                Err(FleetCatalogError::Invalid(_) | FleetCatalogError::Conflict)
            ));
            controller = reopen_merge_controller(&cluster.fixtures[0]).await?;
            let observed = controller
                .0
                .staged_managed_sealed_sources(&controller.1, operation, TIMEOUT)
                .await?;
            ensure!(
                observed.len() == 2
                    && observed[0].0 == parents[0]
                    && observed[0].1.as_ref() == Some(&cut.1)
                    && observed[1].0 == parents[1]
                    && observed[1].1.is_none()
            );
            drop(observed);
            drop(cut);
            ensure!(matches!(
                controller
                    .0
                    .replay_managed_sealed_sources(&controller.1, operation, &verifier, TIMEOUT,)
                    .await,
                Err(FleetCatalogError::Unconfirmed)
            ));
            let selected = controller
                .0
                .reopen_managed_transition(&controller.1, operation, TIMEOUT)
                .await?
                .context("reopened partial merge")?;
            let alternate = preparation::acquire_hold(domain, &registries[index]).await?;
            let birth = domain
                .run(controller.1.materialize_selected_history(parent.history))
                .await??;
            let mut replacement = archives
                .sources("competing-hold", &[(birth, alternate.clone())])
                .await?;
            ensure!(matches!(
                controller
                    .0
                    .stage_managed_sealed_source(
                        &selected,
                        &controller.1,
                        &archives.pool,
                        &archives.tails,
                        replacement
                            .pop()
                            .context("competing sealed parent receiver")?,
                        TIMEOUT,
                    )
                    .await,
                Err(FleetCatalogError::Conflict)
            ));
            registries[index]
                .release(domain.as_ref(), &alternate)
                .await?;
            let early = fleet
                .publish_managed_transfer(&transition, directory, &archives.pool, TIMEOUT)
                .await;
            ensure!(
                matches!(early, Err(FleetCatalogError::Unconfirmed)),
                "merge publication without both parents and a child quorum: {early:?}"
            );
            let unchanged = domain.run(directory.observe()).await??;
            ensure!(unchanged.revision() == view.revision() && unchanged.groups() == 2);
            let namespace = hex::encode(child.history.namespace.bytes());
            let operation_id = hex::encode(operation.bytes());
            let status = control
                .transition_status(&namespace, &operation_id, TIMEOUT)
                .await?;
            ensure!(
                status.namespace_id == namespace
                    && status.operation_id == operation_id
                    && status.phase == Some(TransitionPhase::SealAuthorized)
                    && status.plan_digest == hex::encode(plan.digest())
                    && status.fleet_slot_held
                    && !status.child_archives_verified
                    && !status.parent_holds_released
            );
            let missing = hex::encode([249; 16]);
            ensure!(
                matches!(control.transition_status(&namespace, &missing, TIMEOUT).await,
                    Err(failure) if failure.code == RaftFleetFailureCode::NotFound)
            );
            let candidate = &cluster.hosts[0];
            let attached = candidate
                .attach_managed_transfer_child(transition.clone(), child.clone(), TIMEOUT)
                .await;
            ensure!(
                matches!(attached, Err(CellGroupControllerError::Catalog(ref cause))
                    if matches!(cause.as_ref(), CatalogError::Unconfirmed)),
                "uninstalled merge child must refuse attachment: {attached:?}"
            );
            ensure!(matches!(
                candidate.group(child.history),
                Err(RuntimeError::NotFound)
            ));

            for (index, generation, previous) in [(4, 125, fourth), (5, 126, fifth)] {
                Box::pin(start_voter(cluster, index, generation, previous, 2)).await?;
            }
            let revision = fleet.read(TIMEOUT).await?.revision();
            for host in &cluster.hosts[4..] {
                inspect_current(&cluster.hosts[3], host.identity().node, revision).await?;
            }
            leader(&cluster.hosts[3..], parents[1].history)
                .await
                .context("unsealed merge parent did not regain quorum")?;
            verify_durable_seal(&cluster.hosts[3], &transition, parent.history, &seal).await?;
        }
    }
    let fixture = &cluster.fixtures[0];
    let (resumed, reopened) = controller;
    let holds = resumed
        .staged_managed_sealed_sources(&reopened, operation, TIMEOUT)
        .await?;
    ensure!(holds.len() == parents.len() && holds.iter().all(|(_, hold)| hold.is_some()));
    drop(holds);
    let recovery = preparation::recovery_pool()?;
    ensure!(matches!(
        resumed
            .resume_managed_transfer_archive(
                &reopened,
                operation,
                &verifier,
                recovery.clone(),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let store = &fixture.store;
    let denied_before = store.denied_group_payload_reads.load(Ordering::Acquire);
    store
        .deny_group_payload_reads
        .store(true, Ordering::Release);
    let denied = resumed
        .replay_managed_sealed_sources(&reopened, operation, &verifier, TIMEOUT)
        .await;
    store
        .deny_group_payload_reads
        .store(false, Ordering::Release);
    ensure!(
        denied.is_err() && store.denied_group_payload_reads.load(Ordering::Acquire) > denied_before
    );
    let pending = resumed
        .reopen_managed_transition(&reopened, operation, TIMEOUT)
        .await?
        .context("reopened after denied source payload")?;
    ensure!(pending
        .journal
        .is_some_and(|journal| journal.phase() == TransitionPhase::SealAuthorized
            && journal.sealed_sources().is_none()));
    let sources = resumed
        .replay_managed_sealed_sources(&reopened, operation, &verifier, TIMEOUT)
        .await
        .context("select both staged cuts on a replacement controller")?;
    ensure!(sources.journal().phase() == TransitionPhase::Sealed);
    let sealed_digest = sources
        .journal()
        .sealed_sources()
        .context("sealed source digest")?;
    let transition = Arc::new(
        resumed
            .reopen_managed_transition(&reopened, operation, TIMEOUT)
            .await?
            .context("reopened sealed merge")?,
    );
    ensure!(
        transition
            .journal
            .as_ref()
            .and_then(|journal| journal.sealed_sources())
            == Some(sealed_digest)
    );
    for host in &cluster.hosts[..3] {
        host.install_managed_transfer_child(
            transition.clone(),
            sources.clone(),
            child.history,
            recovery.clone(),
            TIMEOUT,
        )
        .await
        .context("install merged child")?;
    }
    drop(sources);
    drop(verifier);
    drop(sealed_workspace);
    let voters = &cluster.hosts[..3];
    for host in voters {
        host.attach_managed_transfer_child(transition.clone(), child.clone(), TIMEOUT)
            .await
            .context("attach merged child")?;
    }
    let proof = preparation::wait_pending_child(voters, child.history).await?;
    ensure!(
        fleet
            .record_managed_transfer_child_quorums(&transition, directory, vec![proof], TIMEOUT,)
            .await?
            .phase()
            == TransitionPhase::Installed
    );
    let archive_workspace = tempfile::tempdir()?;
    let verifier =
        crate::managed_fixture::verifier::open(fixture, archive_workspace.path(), rooted, limits)
            .await?;
    let archived = resumed
        .resume_managed_transfer_archive(&reopened, operation, &verifier, recovery, TIMEOUT)
        .await
        .context("rebuild the installed child archive on a replacement controller")?;
    ensure!(
        archived.phase() == TransitionPhase::Archived
            && archived.sealed_sources() == Some(sealed_digest)
    );
    let namespace = hex::encode(child.history.namespace.bytes());
    let operation_id = hex::encode(operation.bytes());
    let principal = &transition.request.principal;
    let status = control
        .transition_status(&namespace, &operation_id, TIMEOUT)
        .await?;
    ensure!(status.phase == Some(TransitionPhase::Archived));
    let archive = archived
        .transfer_archive()
        .context("merged archive closure")?;
    let publish = RaftFleetTransitionPublishRequest {
        catalog_id: status.catalog_id,
        namespace_id: namespace.clone(),
        operation_id: operation_id.clone(),
        request_id: hex::encode([218; 16]),
        plan_digest: status.plan_digest,
        archive_digest: status
            .transfer_archive_digest
            .context("archived merge cut")?,
    };
    ensure!(publish.archive_digest == hex::encode(archive));
    let published = control
        .publish_transition(principal, &namespace, &operation_id, &publish, TIMEOUT)
        .await
        .context("publish merged child after both seals")?;
    ensure!(published.directory_revision == view.revision() + 1);
    let after = domain.run(directory.observe()).await??;
    let page = domain.run(directory.group_page(&after, None, 2)).await??;
    ensure!(page.next.is_none() && page.routes == vec![child.clone()]);
    let ack = preparation::wait_managed_activation(voters, &transition, child.history).await?;
    ensure!(
        fleet
            .record_managed_transfer_activations(&transition, directory, vec![ack], TIMEOUT,)
            .await?
            .phase()
            == TransitionPhase::Active
    );
    Box::pin(verify_merged_sql(
        voters,
        child.history,
        &routed,
        receipts,
        retry,
    ))
    .await?;
    finish_merge(
        cluster,
        directory,
        &transition,
        &parents,
        child.history,
        &archives,
        &control,
    )
    .await?;
    let historical = control
        .publish_transition(principal, &namespace, &operation_id, &publish, TIMEOUT)
        .await?;
    ensure!(historical == published);
    let voters = &cluster.hosts[..3];
    let host = &voters[leader(voters, child.history).await?];
    for (index, (cell, expected)) in routed.into_iter().enumerate() {
        let handle = host.cell(cell)?;
        let query = handle
            .query(
                ProposalId::new([218 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(query.result == (expected + 2).to_be_bytes());
    }
    Ok(child.history)
}

async fn admit_merge(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
    control: &RaftFleetControl,
    parents: &[GroupRoute; 2],
    child: GroupHistory,
    operation: ProposalId,
) -> Result<(
    cellar::sdk::fleet_operations::RaftFleetTransitionAdmission,
    FleetSnapshot,
)> {
    let fixture = &cluster.fixtures[0];
    let fleet = &fixture.catalog;
    let basis = control
        .transition_basis(&hex::encode(child.namespace.bytes()), TIMEOUT)
        .await?;
    let principal = request(245, basis.fleet_revision).principal;
    let admission = RaftFleetTransitionAdmissionRequest {
        catalog_id: basis.catalog_id,
        namespace_id: basis.namespace_id,
        operation_id: hex::encode(operation.bytes()),
        request_id: hex::encode([245; 16]),
        expected_fleet_revision: basis.fleet_revision,
        expected_directory_digest: basis.directory_digest,
        class: "Counter".into(),
        partition: RaftFleetTransitionPartition::Merge {
            left: transition_group(parents[0].history),
            right: transition_group(parents[1].history),
        },
        children: vec![RaftFleetTransitionChild {
            group: transition_group(child),
            request_id: hex::encode([243; 16]),
            creation_id: hex::encode([243; 16]),
            voters: std::array::from_fn(|index| RaftFleetFirstGroupVoter {
                node_id: hex::encode(cluster.fixtures[index].spec.node.bytes()),
                member_id: index as u64 + 1,
            }),
        }],
    };
    let namespace = &admission.namespace_id;
    let id = &admission.operation_id;
    let stale = RaftFleetTransitionAdmissionRequest {
        expected_directory_digest: hex::encode([67; 32]),
        ..admission.clone()
    };
    let refused = control
        .admit_transition(&principal, namespace, id, &stale, TIMEOUT)
        .await;
    ensure!(matches!(refused, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
    ensure!(fleet.read(TIMEOUT).await?.revision() == basis.fleet_revision);

    fixture
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = control
        .admit_transition(&principal, namespace, id, &admission, TIMEOUT)
        .await;
    ensure!(matches!(lost, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable));
    let reserved = fleet.read(TIMEOUT).await?;
    ensure!(reserved.revision() == basis.fleet_revision + 1);
    ensure!(
        *fleet.history(&reserved, &child, TIMEOUT).await?.state() == FleetHistoryState::Reserved
    );
    ensure!(fleet
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .is_none());
    let mut conflicting = admission.clone();
    conflicting.children[0].creation_id = hex::encode([242; 16]);
    let refused = control
        .admit_transition(&principal, namespace, id, &conflicting, TIMEOUT)
        .await;
    ensure!(
        matches!(refused, Err(failure) if failure.code == RaftFleetFailureCode::PayloadConflict)
    );

    let admitted = crate::managed_fixture::operator::admit_transition(
        fixture, directory, &principal, &admission,
    )
    .await?;
    ensure!(admitted.phase == TransitionPhase::Planned);
    let exact = control
        .admit_transition(&principal, namespace, id, &admission, TIMEOUT)
        .await?;
    ensure!(admitted == exact);
    let cut = fleet.read(TIMEOUT).await?;
    ensure!(cut.revision() == basis.fleet_revision + 2);
    Ok((admitted, cut))
}

async fn reopen_merge_controller(fixture: &Fixture) -> Result<(FleetCatalog, RangeDirectory)> {
    let fleet = Fixture::open_catalog(
        &fixture.domain,
        fixture.store.clone(),
        fixture.declaration.namespace.cluster,
    )?;
    let catalog = Catalog::new(
        fixture.store.clone(),
        CatalogOptions {
            prefix: "managed/".into(),
            ..Default::default()
        },
    )?;
    let namespace = fixture
        .domain
        .run(catalog.existing_namespace(fixture.declaration.namespace.clone()))
        .await??;
    Ok((
        fleet,
        RangeDirectory::new(namespace, DirectoryLimits::default())?,
    ))
}

async fn verify_minority(
    survivor: &RaftCells,
    transition: &ReopenedManagedCellGroupTransition,
    seal: &GroupSourceSeal,
    parents: &[GroupRoute; 2],
    cells: &[(CellHistory, i64)],
    retry: u64,
) -> Result<()> {
    let unsealed = parents[1].history;
    let group = survivor.group(unsealed)?;
    let committed = tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = group.status().await?;
            if !status.ready && status.leader.is_none() {
                return Ok::<_, anyhow::Error>(status.committed);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("unsealed merge parent did not lose quorum")??;
    let handle = survivor.cell(cells[1].0.clone())?;
    let get = handle.contract("Get")?;
    let historical = handle
        .query(
            ProposalId::new([220; 16])?,
            &get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Stale,
                stale_policy: StaleReadPolicy::DisconnectedHistorical,
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        historical.result == cells[1].1.to_be_bytes()
            && historical.witness.group == unsealed
            && historical.served_by_node == survivor.identity()
    );
    let fresh = handle
        .query(
            ProposalId::new([221; 16])?,
            &get,
            vec![],
            QueryOptions {
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await;
    ensure!(
        matches!(
            fresh.as_ref().err().map(Arc::as_ref),
            Some(CellDispatchError::Runtime(
                RuntimeError::NotLeader | RuntimeError::NotReady | RuntimeError::DeadlineExceeded
            ))
        ),
        "minority cannot serve a fresh read: {fresh:?}"
    );
    let refused = group.seal_managed_source(transition, TIMEOUT).await.err();
    ensure!(
        matches!(
            refused,
            Some(CellGroupControllerError::Runtime(RuntimeError::NotLeader))
        ),
        "minority cannot seal the other parent: {refused:?}"
    );
    ensure!(group.status().await?.committed == committed);

    let sealed = parents[0].history;
    verify_durable_seal(survivor, transition, sealed, seal).await?;
    let handle = survivor.cell(cells[0].0.clone())?;
    let id = CommandId::new(ClientId::new([222; 16])?, 1)?;
    let refused = handle
        .command(
            &handle.contract("Add")?,
            64i64.to_be_bytes().to_vec(),
            CommandOptions::new(id, retry),
        )
        .await;
    ensure!(
        matches!(refused, Err(ref failure) if failure.command == id && !failure.ambiguous),
        "sealed parent admitted a new command: {refused:?}"
    );
    verify_durable_seal(survivor, transition, sealed, seal).await
}

async fn verify_durable_seal(
    host: &RaftCells,
    transition: &ReopenedManagedCellGroupTransition,
    history: GroupHistory,
    seal: &GroupSourceSeal,
) -> Result<()> {
    let source = host.group(history)?;
    let observed = source.seal_managed_source(transition, TIMEOUT).await?;
    ensure!(matches!(observed, CellGroupSealOutcome::Applied(ref applied) if applied == seal));
    ensure!(!source.status().await?.ready);
    Ok(())
}

fn finish_merge<'a>(
    cluster: &'a mut ManagedCluster,
    directory: &'a RangeDirectory,
    transition: &'a ReopenedManagedCellGroupTransition,
    parents: &'a [GroupRoute; 2],
    child: GroupHistory,
    archives: &'a ArchiveInputs,
    control: &'a RaftFleetControl,
) -> impl std::future::Future<Output = Result<()>> + 'a {
    Box::pin(finish_merge_inner(
        cluster, directory, transition, parents, child, archives, control,
    ))
}

async fn finish_merge_inner(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parents: &[GroupRoute; 2],
    child: GroupHistory,
    archives: &ArchiveInputs,
    control: &RaftFleetControl,
) -> Result<()> {
    let fleet = cluster.fixtures[0].catalog.clone();
    let domain = &archives.domain;
    let publication = domain
        .run(directory.publication(&transition.plan))
        .await??
        .context("merge publication")?;
    let mut tombstones = Vec::with_capacity(parents.len());
    for parent in parents {
        let ack =
            retirement::commit_on_all(&cluster.hosts[3..], parent.history, transition).await?;
        ensure!(ack.tombstone().publication() == publication.journal_digest());
        tombstones.push(ack);
    }
    preparation::initialize_transfer_archive(
        &fleet,
        directory,
        transition,
        child,
        &archives.pool,
        &archives.scratch.path().join("merge.intent"),
        domain,
    )
    .await?;
    Box::pin(restart_voter(cluster, 0, 121, Some(child), 1)).await?;
    let applied = preparation::wait_active_archive(&cluster.hosts[0], child).await?;
    let birth = domain
        .run(directory.materialize_selected_history(child))
        .await??;
    let registry = directory
        .open_group_archive(domain.as_ref(), &archives.pool, &birth)
        .await?;
    let hold = preparation::acquire_hold(domain, &registry).await?;
    ensure!(hold.marker().position().index >= applied);
    let covered = fleet
        .verify_managed_transfer_child_archives(
            transition,
            directory,
            &archives.pool,
            &archives.tails,
            archives.sources("merge-baseline", &[(birth, hold)]).await?,
            TIMEOUT,
        )
        .await?;
    ensure!(covered.phase() == TransitionPhase::Active && covered.child_archives().is_some());
    ensure!(matches!(
        fleet
            .settle_managed_transfer_retirement(
                transition,
                directory,
                &archives.pool,
                tombstones[..1].to_vec(),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    let retired = fleet
        .settle_managed_transfer_retirement(
            transition,
            directory,
            &archives.pool,
            tombstones,
            TIMEOUT,
        )
        .await?;
    ensure!(retired.phase() == TransitionPhase::Retired && !retired.parent_holds_released());
    let cut = fleet.read(TIMEOUT).await?;
    let principal = &transition.request.principal;
    let confirmation = RaftFleetTransitionResume {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(child.namespace.bytes()),
        operation_id: hex::encode(transition.request.operation),
        request_id: hex::encode([246; 16]),
        expected_revision: cut.revision(),
        plan_digest: hex::encode(transition.plan.digest()),
        step: RaftFleetTransitionResumeStep::ConfirmChildren,
    };
    let namespace = &confirmation.namespace_id;
    let operation = &confirmation.operation_id;
    let foreign = control
        .resume_transition(
            "another-operator",
            namespace,
            operation,
            &confirmation,
            TIMEOUT,
        )
        .await;
    ensure!(
        matches!(foreign, Err(failure) if failure.code == RaftFleetFailureCode::Forbidden
        && failure.outcome == RaftFleetOutcome::NotSubmitted)
    );
    let confirmed = control
        .resume_transition(principal, namespace, operation, &confirmation, TIMEOUT)
        .await?;
    let cut = fleet.read(TIMEOUT).await?;
    ensure!(
        confirmed.applied_revision == cut.revision()
            && *fleet.history(&cut, &child, TIMEOUT).await?.state() == FleetHistoryState::Live
    );
    let control = control.clone().with_node_observer(cluster.hosts[0].clone());

    Box::pin(restart_voter(cluster, 3, 122, None, 0)).await?;
    let cut = fleet.read(TIMEOUT).await?;
    let observer = &cluster.hosts[0];
    for source in &cluster.hosts[3..] {
        inspect_current(observer, source.identity().node, cut.revision()).await?;
    }
    let premature = RaftFleetTransitionResume {
        request_id: hex::encode([248; 16]),
        expected_revision: cut.revision(),
        step: RaftFleetTransitionResumeStep::Complete,
        ..confirmation.clone()
    };
    let attached = control
        .resume_transition(principal, namespace, operation, &premature, TIMEOUT)
        .await;
    ensure!(
        matches!(attached, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable
            && failure.outcome == RaftFleetOutcome::Unknown)
    );
    let still_held = control
        .transition_status(namespace, operation, TIMEOUT)
        .await?;
    ensure!(still_held.fleet_slot_held && !still_held.parent_holds_released);
    for index in 4..6 {
        Box::pin(restart_voter(cluster, index, 119 + index as u8, None, 0)).await?;
    }
    let cut = fleet.read(TIMEOUT).await?;
    let observer = &cluster.hosts[0];
    for source in &cluster.hosts[3..] {
        let node = source.identity().node;
        inspect_current(observer, node, cut.revision()).await?;
        let parked = observer
            .inspect_peer_physical_inventory(node, cut.revision(), TIMEOUT)
            .await?;
        ensure!(
            parked.view.unattached_engine_histories == 2
                && parked.view.node.reserved_bytes == 2 * parked.view.node.profile.bytes_per_group
        );
        let inventory = parked.inventory.context("two parked merge parents")?;
        for parent in parents {
            ensure!(inventory
                .group_history(&parent.history)
                .context("parked merge parent")?
                .parked_retired_source());
        }
    }
    let stale = control
        .resume_transition(principal, namespace, operation, &premature, TIMEOUT)
        .await;
    ensure!(matches!(stale, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
    let completion = RaftFleetTransitionResume {
        request_id: hex::encode([247; 16]),
        expected_revision: cut.revision(),
        step: RaftFleetTransitionResumeStep::Complete,
        ..confirmation.clone()
    };
    let collision = RaftFleetTransitionResume {
        request_id: confirmation.request_id.clone(),
        ..completion.clone()
    };
    let reused = control
        .resume_transition(principal, namespace, operation, &collision, TIMEOUT)
        .await;
    ensure!(
        matches!(reused, Err(failure) if failure.code == RaftFleetFailureCode::PayloadConflict)
    );
    let still_held = control
        .transition_status(namespace, operation, TIMEOUT)
        .await?;
    ensure!(still_held.fleet_slot_held && !still_held.parent_holds_released);

    let store = &cluster.fixtures[0].store;
    store.fail_fleet_before.store(true, Ordering::Release);
    let interrupted = control
        .resume_transition(principal, namespace, operation, &completion, TIMEOUT)
        .await;
    ensure!(
        matches!(interrupted, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable
            && failure.outcome == RaftFleetOutcome::Unknown)
    );
    ensure!(!store.fail_fleet_before.load(Ordering::Acquire));
    ensure!(domain.run(directory.active_transition()).await??.is_none());
    let cleaning = control
        .transition_status(namespace, operation, TIMEOUT)
        .await?;
    ensure!(
        cleaning.phase == Some(TransitionPhase::Retired)
            && cleaning.child_archives_verified
            && cleaning.parent_holds_released
            && cleaning.fleet_slot_held
            && cleaning.observed_fleet_revision == cut.revision()
    );

    let resumed = crate::managed_fixture::operator::resume_transition(
        &cluster.fixtures[0],
        directory,
        principal,
        observer,
        &completion,
    )
    .await?;
    let replayed = control
        .resume_transition(principal, namespace, operation, &completion, TIMEOUT)
        .await?;
    ensure!(resumed == replayed && resumed.applied_revision == cut.revision() + 1);
    let settled = fleet.read(TIMEOUT).await?;
    let terminal = control
        .transition_status(namespace, operation, TIMEOUT)
        .await?;
    ensure!(
        terminal.observed_fleet_revision == settled.revision()
            && terminal.phase == cleaning.phase
            && terminal.plan_digest == cleaning.plan_digest
            && terminal.parent_holds_released
            && !terminal.fleet_slot_held
    );
    crate::managed_fixture::operator::transition_status(
        &cluster.fixtures[0],
        directory,
        principal,
        &terminal,
    )
    .await?;
    let retirement = retired
        .retired_parents()
        .context("merge parent retirement digest")?;
    for parent in parents {
        ensure!(
            *fleet
                .history(&settled, &parent.history, TIMEOUT)
                .await?
                .state()
                == FleetHistoryState::Retired {
                    operation: transition.request.operation,
                    retirement
                }
        );
    }
    let inventory = fleet
        .check_inventory(&settled, std::slice::from_ref(directory), TIMEOUT)
        .await?;
    ensure!(inventory.histories() == 4 && inventory.namespaces() == 1);
    Ok(())
}

async fn wait_archive(
    voters: &[RaftCells],
    history: GroupHistory,
    seal: &GroupSourceSeal,
) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for host in voters {
                if host
                    .group(history)?
                    .archive_status()?
                    .and_then(|status| status.published)
                    .is_some_and(|marker| marker.source_seal() == Some(seal))
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("merge parent did not publish its sealed cut")?
}

async fn restart_voter(
    cluster: &mut ManagedCluster,
    index: usize,
    generation: u8,
    active: Option<GroupHistory>,
    registered: u32,
) -> Result<()> {
    let previous = stop_voter(cluster, index, active).await?;
    start_voter(cluster, index, generation, previous, registered).await
}

async fn stop_voter(
    cluster: &mut ManagedCluster,
    index: usize,
    active: Option<GroupHistory>,
) -> Result<NodeIdentity> {
    let node = cluster.nodes.remove(index);
    let old = cluster.hosts.remove(index);
    let identity = old.identity();
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = old.close().await?;
    ensure!(
        report.groups_total == 2
            && report.omitted_groups == 0
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
            && report
                .groups
                .iter()
                .all(|group| group.active == (active == Some(group.history))
                    && !group.failed
                    && group.applied.index == group.committed),
        "merge voter shutdown inventory: {report:?}"
    );
    Ok(identity)
}

async fn start_voter(
    cluster: &mut ManagedCluster,
    index: usize,
    generation: u8,
    previous: NodeIdentity,
    registered: u32,
) -> Result<()> {
    let fixture = &cluster.fixtures[index];
    let node = start_node(
        cluster.roots[index].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let startup = async {
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, generation)
            .await?;
        crate::managed_fixture::archives::configure(&mut config);
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match startup {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!("restart merge voter {index}, shutdown={stopped:?}")));
        }
    };
    cluster.nodes.insert(index, node);
    cluster.hosts.insert(index, host);
    let host = &cluster.hosts[index];
    ensure!(host.identity().disk == previous.disk && host.identity().process != previous.process);
    let view = host.observe_node(TIMEOUT).await?;
    ensure!(view.node.groups == 2 && view.registered_histories == registered);
    Ok(())
}

async fn verify_merged_sql(
    voters: &[RaftCells],
    history: GroupHistory,
    cells: &[(CellHistory, i64)],
    receipts: Vec<CommandReceipt>,
    retry: u64,
) -> Result<()> {
    let host = &voters[leader(voters, history).await?];
    let mut positions = Vec::with_capacity(cells.len());
    for (index, ((cell, expected), receipt)) in cells.iter().zip(receipts).enumerate() {
        let handle = host.cell(cell.clone())?;
        let query = handle
            .query(
                ProposalId::new([214 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        ensure!(query.result == (expected + 1).to_be_bytes());
        let add = handle.contract("Add")?;
        let retry_receipt = handle
            .command(
                &add,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(receipt.id, retry),
            )
            .await?;
        ensure!(
            retry_receipt.id == receipt.id
                && retry_receipt.result == receipt.result
                && retry_receipt.version == receipt.version
        );
        let write = handle
            .command(
                &add,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(receipt.id.client, 2)?, retry),
            )
            .await?;
        ensure!(write.result == (expected + 2).to_be_bytes() && write.witness.group == history);
        positions.push((history, write.witness.position.index));
    }
    preparation::wait_transfer_applied(voters, &positions).await
}

pub(super) async fn close(
    nodes: Vec<Node>,
    hosts: &[RaftCells],
    merged: Option<GroupHistory>,
) -> Result<()> {
    let reports = futures_util::future::join_all(nodes.into_iter().zip(hosts).map(
        |(node, host)| async move {
            ensure!(matches!(
                node.shutdown(ShutdownMode::Preserve).await?,
                NodeOutcome::Stopped(_)
            ));
            Ok::<_, anyhow::Error>(host.close().await?)
        },
    ))
    .await;
    for (index, report) in reports.into_iter().enumerate() {
        let report = report?;
        if let Some(merged) = merged {
            ensure!(
                report.groups_total == 2
                    && report.groups.len() == 2
                    && report.omitted_groups == 0
                    && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
                    && report.groups.iter().all(|group| !group.failed
                        && group.active == (index < 3 && group.history == merged)
                        && group.applied.index == group.committed),
                "merged shutdown inventory: {report:?}"
            );
        }
    }
    Ok(())
}
