// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Merge the live children of a closed split and retire both sources.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::{
        CellGroupArchiveHold, CellGroupArchiveTailLimits, CellGroupArchiveTailPool, CellGroupBirth,
        CellGroupCheckpointPool, CellGroupTransitionSource, CellImagePin, GroupRoute,
    },
    FleetChange, FleetHistoryState, FleetMutation, FleetSnapshot, FleetTable,
};
use cellar::application::consensus::{DisasterFenceAuthority, FleetNodeSpec};
use cellar::sdk::fleet_operations::{
    FleetFencedTransitionRecovery, FleetTransitionRecoveryFenceRequest, RaftFleetControl,
    RaftFleetFailureCode, RaftFleetFirstGroupVoter, RaftFleetOutcome,
    RaftFleetTransitionAdmissionRequest, RaftFleetTransitionChild,
    RaftFleetTransitionChildSetupRequest, RaftFleetTransitionChildSetupResult,
    RaftFleetTransitionChildStep, RaftFleetTransitionGroup, RaftFleetTransitionPartition,
    RaftFleetTransitionPrepareRequest, RaftFleetTransitionPublishRequest,
    RaftFleetTransitionResume, RaftFleetTransitionResumeStep, RaftFleetTransitionSealRequest,
    RaftFleetTransitionSourceRetirement, RaftFleetTransitionSourceSealRequest,
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
    let (left, right) = parent.route().range.split(pivot.clone())?;
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
    let voters = &cluster.hosts[..3];
    let leader = &voters[leader(voters, cluster.history).await?];
    let sample = leader
        .group(cluster.history)?
        .observe_partition(TIMEOUT)
        .await?;
    let selected = PartitionBasis {
        route: sample.route,
        directory_digest: hex::decode(&admission.expected_directory_digest)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("directory digest length"))?,
        membership: sample.membership,
        term: sample.status.term,
        durability: directory.namespace_config().minimum_durability,
        placement_class: [1; 32],
    };
    let control = control.with_node_observer(leader.clone());
    let stale = PartitionBasis {
        term: Term::new(selected.term.get() + 1),
        ..selected.clone()
    };
    let rejected = Box::pin(control.admit_recommended_partition(
        &principal,
        &admission,
        PartitionDecision::Split {
            basis: &stale,
            pivot: &pivot,
            reason: SplitReason::GroupLoad,
        },
        TIMEOUT,
    ))
    .await;
    ensure!(matches!(rejected, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
    ensure!(fixture.catalog.read(TIMEOUT).await?.revision() == basis.fleet_revision);
    fixture
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = Box::pin(control.admit_recommended_partition(
        &principal,
        &admission,
        PartitionDecision::Split {
            basis: &selected,
            pivot: &pivot,
            reason: SplitReason::GroupLoad,
        },
        TIMEOUT,
    ))
    .await;
    ensure!(matches!(lost, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable));
    let reserved = fixture.catalog.read(TIMEOUT).await?;
    ensure!(reserved.revision() == basis.fleet_revision + 1);
    ensure!(
        *fixture
            .catalog
            .history(&reserved, &histories[0], TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Reserved
    );
    ensure!(matches!(
        fixture
            .catalog
            .history(&reserved, &histories[1], TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let receipt = request(236, reserved.revision());
    fixture
        .catalog
        .commit(
            &reserved,
            &FleetMutation {
                id: receipt.id,
                operation: receipt.operation,
                principal: receipt.principal,
                expected_revision: receipt.expected_revision,
                changes: vec![FleetChange {
                    table: FleetTable::Admissions,
                    key: "qualification-advance-between-children".into(),
                    value: None,
                }],
            },
            TIMEOUT,
        )
        .await?;
    // The CLI opens a fresh control against the changed Fleet revision.
    let admitted = crate::managed_fixture::operator::admit_transition(
        fixture, directory, &principal, &admission,
    )
    .await?;
    let cut = fixture.catalog.read(TIMEOUT).await?;
    ensure!(
        admitted.phase == TransitionPhase::Planned && cut.revision() == basis.fleet_revision + 4
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
        Decision::SplitMerge {
            recovery_barrier: false,
        },
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
        Box::pin(run_plan(
            rooted,
            store,
            Decision::SplitMerge {
                recovery_barrier: false,
            },
        ))
        .await
        .with_context(|| format!("managed split and merge, extent images={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_recovery_barrier_fences_transfer_and_reopens() -> Result<()> {
    Box::pin(run_plan(
        false,
        Arc::new(fault::Store::default()),
        Decision::SplitMerge {
            recovery_barrier: true,
        },
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_recovery_barrier_fences_transfer_and_reopens() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-recovery-barrier",
            rooted,
        )?));
        Box::pin(run_plan(
            rooted,
            store,
            Decision::SplitMerge {
                recovery_barrier: true,
            },
        ))
        .await
        .with_context(|| format!("managed recovery barrier, extent images={rooted}"))?;
    }
    Ok(())
}

#[derive(Default)]
struct TransitionFence {
    stopped: std::sync::Mutex<Option<[u8; 32]>>,
    fences: std::sync::atomic::AtomicUsize,
    verifications: std::sync::atomic::AtomicUsize,
}

impl DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest> for TransitionFence {
    fn fence<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.fences.fetch_add(1, Ordering::AcqRel);
            let mut stopped = self.stopped.lock().unwrap();
            let digest = request.digest();
            ensure!(
                *stopped.get_or_insert(digest) == digest,
                "different fence scope"
            );
            Ok(digest.to_vec())
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
        evidence: &'a [u8],
    ) -> futures_util::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.verifications.fetch_add(1, Ordering::AcqRel);
            let digest = request.digest();
            ensure!(
                *self.stopped.lock().unwrap() == Some(digest),
                "fence not effective"
            );
            ensure!(evidence == digest, "unbound fence evidence");
            Ok(())
        })
    }
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
    recovery_barrier: bool,
) -> impl std::future::Future<Output = Result<Option<GroupHistory>>> + 'a {
    Box::pin(run_inner(
        cluster,
        directory,
        cells,
        rooted,
        recovery_barrier,
    ))
}

async fn run_inner(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    cells: Vec<CellHistory>,
    rooted: bool,
    recovery_barrier: bool,
) -> Result<Option<GroupHistory>> {
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
    let (preparer, reopened_directory) = reopen_merge_controller(fixture).await?;
    let workspace = tempfile::tempdir()?;
    let tail_limits = CellGroupArchiveTailLimits {
        max_operations: 2,
        max_retained_bytes: 2 * CellGroupArchiveTailLimits::default().max_retained_bytes,
        ..Default::default()
    };
    let verifier = crate::managed_fixture::verifier::open(
        fixture,
        &workspace.path().join("operator"),
        rooted,
        tail_limits,
    )
    .await?;
    let prepare_request = RaftFleetTransitionPrepareRequest {
        catalog_id: hex::encode(fleet.catalog_id()?),
        namespace_id: hex::encode(cluster.history.namespace.bytes()),
        operation_id: hex::encode(operation.bytes()),
        request_id: hex::encode([240; 16]),
        plan_digest: hex::encode(plan.digest()),
    };
    let preparation = crate::managed_fixture::operator::prepare_transition(
        fixture,
        &reopened_directory,
        &request.principal,
        &prepare_request,
        Some((verifier, preparation::recovery_pool()?)),
    )
    .await?;
    ensure!(preparation.phase == TransitionPhase::Prepared);
    let holds = preparer
        .staged_managed_preparation_holds(&reopened_directory, operation, TIMEOUT)
        .await?
        .context("selected merge preparation holds")?;
    ensure!(holds.iter().map(|hold| hold.marker().route()).eq(&parents));
    let verifier = crate::managed_fixture::verifier::open(
        fixture,
        &workspace.path().join("replay"),
        rooted,
        tail_limits,
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
    ensure!(
        prepared.journal().preparation().map(hex::encode).as_deref()
            == Some(preparation.preparation_digest.as_str())
    );
    drop(prepared);
    drop(verifier);
    drop(workspace);
    let authorization = crate::managed_fixture::operator::authorize_transition_seal(
        fixture,
        &reopened_directory,
        &request.principal,
        &RaftFleetTransitionSealRequest {
            catalog_id: prepare_request.catalog_id,
            namespace_id: prepare_request.namespace_id,
            operation_id: prepare_request.operation_id,
            request_id: hex::encode([239; 16]),
            plan_digest: prepare_request.plan_digest,
            preparation_digest: preparation.preparation_digest,
        },
    )
    .await?;
    ensure!(authorization.phase == TransitionPhase::SealAuthorized);
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
        let fixture = &cluster.fixtures[0];
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
        if index == 0 {
            let denied_before = fixture
                .store
                .denied_group_payload_reads
                .load(Ordering::Acquire);
            fixture
                .store
                .deny_group_payload_reads
                .store(true, Ordering::Release);
            let denied = controller
                .0
                .capture_managed_sealed_source(
                    &controller.1,
                    operation,
                    parent.history,
                    &verifier,
                    TIMEOUT,
                )
                .await;
            fixture
                .store
                .deny_group_payload_reads
                .store(false, Ordering::Release);
            ensure!(
                denied.is_err()
                    && fixture
                        .store
                        .denied_group_payload_reads
                        .load(Ordering::Acquire)
                        > denied_before
            );
            let progress = controller
                .0
                .staged_managed_sealed_sources(&controller.1, operation, TIMEOUT)
                .await?;
            ensure!(progress.iter().all(|(_, hold)| hold.is_none()));
            controller = reopen_merge_controller(fixture).await?;
        }
        let capture = RaftFleetTransitionSourceSealRequest {
            catalog_id: control.catalog_id().into(),
            namespace_id: hex::encode(parent.history.namespace.bytes()),
            operation_id: hex::encode(operation.bytes()),
            request_id: hex::encode([237 + index as u8; 16]),
            plan_digest: hex::encode(plan.digest()),
            source: transition_group(parent.history),
        };
        let captured = crate::managed_fixture::operator::capture_transition_source(
            fixture,
            &controller.1,
            &request.principal,
            &capture,
            (verifier.clone(), preparation::recovery_pool()?),
        )
        .await?;
        ensure!(
            captured.request == capture
                && captured.sealed.index == seal.position().index.get()
                && captured.sealed.term == seal.position().term.get()
                && captured.sealed.authorization_digest == hex::encode(seal.authorization())
        );
        let hold = controller
            .0
            .staged_managed_sealed_sources(&controller.1, operation, TIMEOUT)
            .await?
            .into_iter()
            .nth(index)
            .and_then(|(_, hold)| hold)
            .context("captured sealed parent slot")?;
        ensure!(hold.marker().source_seal() == Some(&seal));
        ensure!(
            controller
                .0
                .capture_managed_sealed_source(
                    &controller.1,
                    operation,
                    parent.history,
                    &verifier,
                    TIMEOUT,
                )
                .await?
                == hold
        );
        let birth = domain
            .run(controller.1.materialize_selected_history(parent.history))
            .await??;
        let cut = (birth, hold);
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
            let recovery =
                Box::pin(control.transition_recovery_basis(&namespace, &operation_id, TIMEOUT))
                    .await?;
            let parent_groups: Vec<_> = parents
                .iter()
                .map(|parent| transition_group(parent.history))
                .collect();
            ensure!(
                recovery.phase == TransitionPhase::SealAuthorized
                    && recovery.plan_digest == status.plan_digest
                    && recovery.directory_digest == hex::encode(unchanged.digest())
                    && recovery.publication_digest.is_none()
                    && recovery.archive_sources() == parent_groups
                    && recovery.children == [transition_group(child.history)]
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
    let operator_install = RaftFleetTransitionChildSetupRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(child.history.namespace.bytes()),
        operation_id: hex::encode(operation.bytes()),
        request_id: hex::encode([178; 16]),
        plan_digest: hex::encode(plan.digest()),
        child: transition_group(child.history),
        step: RaftFleetTransitionChildStep::Install,
    };
    let operator_workspace = tempfile::tempdir()?;
    let operator_verifier =
        crate::managed_fixture::verifier::open(fixture, operator_workspace.path(), rooted, limits)
            .await?;
    let operator_voter = &cluster.hosts[0];
    let installed = Box::pin(crate::managed_fixture::operator::setup_transition_child(
        fixture,
        &reopened,
        &request.principal,
        operator_voter,
        &operator_install,
        Some((operator_verifier, recovery.clone())),
    ))
    .await?;
    ensure!(installed.request == operator_install);
    ensure!(matches!(
        installed.result,
        RaftFleetTransitionChildSetupResult::Installed { member: 1, ref node_id, .. }
            if node_id == &hex::encode(operator_voter.identity().node.bytes())
    ));
    ensure!(matches!(
        operator_voter.group(child.history)?.status().await,
        Err(RuntimeError::NotFound)
    ));
    for host in &cluster.hosts[1..3] {
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
    let operator_attach = RaftFleetTransitionChildSetupRequest {
        request_id: hex::encode([179; 16]),
        step: RaftFleetTransitionChildStep::Attach,
        ..operator_install
    };
    let attached = Box::pin(crate::managed_fixture::operator::setup_transition_child(
        fixture,
        &reopened,
        &request.principal,
        operator_voter,
        &operator_attach,
        None,
    ))
    .await?;
    ensure!(
        attached.request == operator_attach
            && attached.result == RaftFleetTransitionChildSetupResult::Attached
    );
    for host in &voters[1..] {
        host.attach_managed_transfer_child(transition.clone(), child.clone(), TIMEOUT)
            .await
            .context("attach merged child")?;
    }
    let (child_leader, _) = preparation::wait_pending_child_witness(voters, child.history).await?;
    let quorum_request = RaftFleetTransitionChildSetupRequest {
        request_id: hex::encode([181; 16]),
        step: RaftFleetTransitionChildStep::Quorum,
        ..operator_attach
    };
    let quorum = Box::pin(crate::managed_fixture::operator::setup_transition_child(
        fixture,
        &reopened,
        &request.principal,
        &voters[child_leader],
        &quorum_request,
        None,
    ))
    .await?;
    ensure!(quorum.request == quorum_request);
    ensure!(matches!(
        quorum.result,
        RaftFleetTransitionChildSetupResult::Quorum {
            phase: TransitionPhase::Installed,
            ..
        }
    ));
    let archive_workspace = tempfile::tempdir()?;
    let verifier =
        crate::managed_fixture::verifier::open(fixture, archive_workspace.path(), rooted, limits)
            .await?;
    let verify_request = RaftFleetTransitionPrepareRequest {
        catalog_id: quorum_request.catalog_id.clone(),
        namespace_id: quorum_request.namespace_id.clone(),
        operation_id: quorum_request.operation_id.clone(),
        request_id: hex::encode([182; 16]),
        plan_digest: quorum_request.plan_digest.clone(),
    };
    let verified = Box::pin(crate::managed_fixture::operator::verify_transition_archive(
        fixture,
        &reopened,
        &request.principal,
        &verify_request,
        (verifier, recovery),
        "verify-archive",
    ))
    .await?;
    ensure!(verified.request == verify_request && verified.phase == TransitionPhase::Archived);
    let archived = resumed
        .reopen_managed_transition(&reopened, operation, TIMEOUT)
        .await?
        .context("reopen verified merge")?
        .journal
        .context("archived merge journal")?;
    ensure!(
        archived.phase() == TransitionPhase::Archived
            && archived.sealed_sources() == Some(sealed_digest)
            && archived.transfer_archive().map(hex::encode).as_deref()
                == Some(verified.archive_digest.as_str())
    );
    let namespace = hex::encode(child.history.namespace.bytes());
    let operation_id = hex::encode(operation.bytes());
    let principal = &transition.request.principal;
    let expected_lineages: Vec<_> = parents
        .iter()
        .flat_map(|parent| {
            cluster.fixtures[3..]
                .iter()
                .enumerate()
                .map(|(index, fixture)| (parent.history, index, fixture.spec.clone()))
        })
        .chain(
            cluster.fixtures[..3]
                .iter()
                .enumerate()
                .map(|(index, fixture)| (child.history, index, fixture.spec.clone())),
        )
        .collect();
    let recovery_workspace = tempfile::tempdir()?;
    let recovery_verifier =
        crate::managed_fixture::verifier::open(fixture, recovery_workspace.path(), rooted, limits)
            .await?;
    let recovery_control = control
        .clone()
        .with_transition_preparer(recovery_verifier.clone(), preparation::recovery_pool()?);
    let parent_basis =
        Box::pin(control.transition_recovery_basis(&namespace, &operation_id, TIMEOUT)).await?;
    let mut parent_holds = Vec::new();
    for registry in &registries {
        parent_holds.push(preparation::acquire_hold(domain, registry).await?);
    }
    ensure!(matches!(
        Box::pin(recovery_control.verify_transition_recovery_archives(
            principal, &parent_basis, vec![parent_holds[0].clone()], TIMEOUT,
        ))
        .await,
        Err(failure) if failure.code == RaftFleetFailureCode::InvalidInput
    ));
    let recovery_operation = hex::encode([207; 16]);
    let parent_intent = {
        let restored = Box::pin(recovery_control.verify_transition_recovery_archives(
            principal,
            &parent_basis,
            parent_holds.clone(),
            TIMEOUT,
        ))
        .await
        .context("verify selected parent archives before publication")?;
        assert_recovery_lineages(&restored, &expected_lineages)?;
        ensure!(
            restored.basis() == &parent_basis
                && restored
                    .archives()
                    .iter()
                    .map(|proof| proof.hold())
                    .eq(&parent_holds)
        );
        fixture
            .store
            .fail_recovery_record_confirmation
            .store(true, Ordering::Release);
        let unknown = Box::pin(recovery_control.stage_transition_recovery_intent(
            principal,
            &recovery_operation,
            &restored,
            TIMEOUT,
        ))
        .await;
        ensure!(
            matches!(unknown, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable)
        );
        ensure!(!fixture
            .store
            .fail_recovery_record_confirmation
            .load(Ordering::Acquire));
        let staged = Box::pin(recovery_control.stage_transition_recovery_intent(
            principal,
            &recovery_operation,
            &restored,
            TIMEOUT,
        ))
        .await?;
        ensure!(staged.operation() == [207; 16] && staged.transition() == operation.bytes());
        ensure!(staged.archive_holds() == parent_holds);
        ensure!(staged.required_voter_lineages() == restored.required_voter_lineages());
        staged
    };
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
    if recovery_barrier {
        let birth = domain
            .run(directory.materialize_selected_history(parents[0].history))
            .await??;
        let wrong = Box::pin(control.select_transition_recovery_barrier(
            principal,
            &namespace,
            &recovery_operation,
            [99; 32],
            TIMEOUT,
        ))
        .await;
        ensure!(
            matches!(wrong, Err(failure) if failure.code == RaftFleetFailureCode::PayloadConflict)
        );
        let work = control.select_transition_recovery_barrier(
            principal,
            &namespace,
            &recovery_operation,
            parent_intent.digest(),
            TIMEOUT,
        );
        let (unknown, delivered) = Box::pin(fault::lose_directory_confirmation(work)).await;
        ensure!(
            delivered
                && matches!(unknown, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable)
        );
        let (fresh_catalog, fresh_directory) = reopen_merge_controller(fixture).await?;
        let fresh = RaftFleetControl::new(domain, fresh_catalog, vec![fresh_directory])?;
        let confirmed = Box::pin(fresh.select_transition_recovery_barrier(
            principal,
            &namespace,
            &recovery_operation,
            parent_intent.digest(),
            TIMEOUT,
        ))
        .await?;
        ensure!(confirmed.intent_digest() == parent_intent.digest());
        let locked = domain.run(directory.observe()).await??;
        ensure!(locked.digest() == confirmed.directory_digest());
        let authority = TransitionFence::default();
        let foreign = Box::pin(recovery_control.fence_transition_recovery(
            "foreign-operator",
            &namespace,
            &recovery_operation,
            confirmed,
            &authority,
            TIMEOUT,
        ))
        .await;
        ensure!(matches!(foreign, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
        ensure!(authority.fences.load(Ordering::Acquire) == 0);
        fixture
            .store
            .fail_recovery_record_confirmation
            .store(true, Ordering::Release);
        let unknown = Box::pin(recovery_control.fence_transition_recovery(
            principal,
            &namespace,
            &recovery_operation,
            confirmed,
            &authority,
            TIMEOUT,
        ))
        .await;
        ensure!(
            matches!(unknown, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable)
        );
        ensure!(!fixture
            .store
            .fail_recovery_record_confirmation
            .load(Ordering::Acquire));
        ensure!(authority.fences.load(Ordering::Acquire) == 1);
        let fresh =
            fresh.with_transition_preparer(recovery_verifier, preparation::recovery_pool()?);
        let fenced = Box::pin(fresh.fence_transition_recovery(
            principal,
            &namespace,
            &recovery_operation,
            confirmed,
            &authority,
            TIMEOUT,
        ))
        .await?;
        let fenced = Arc::new(fenced);
        ensure!(fenced.request().barrier() == confirmed);
        ensure!(fenced.request().intent().digest() == parent_intent.digest());
        ensure!(
            fenced.request().intent().required_voter_lineages()
                == parent_intent.required_voter_lineages()
        );
        ensure!(fenced.receipt_digest() != [0; 32]);
        ensure!(fenced
            .archives()
            .iter()
            .map(|cut| cut.hold())
            .eq(parent_holds.iter()));
        ensure!(authority.fences.load(Ordering::Acquire) == 1);
        ensure!(authority.verifications.load(Ordering::Acquire) == 2);
        ensure!(matches!(
            domain
                .run(directory.require_current_group_birth(&birth))
                .await?,
            Err(cellar::application::consensus::CatalogError::Fenced)
        ));
        let refused = Box::pin(control.publish_transition(
            principal,
            &namespace,
            &operation_id,
            &publish,
            TIMEOUT,
        ))
        .await;
        ensure!(matches!(refused, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
        let observed = control
            .transition_status(&namespace, &operation_id, TIMEOUT)
            .await?;
        ensure!(observed.phase == Some(TransitionPhase::Archived) && observed.fleet_slot_held);
        let page = domain.run(directory.group_page(&locked, None, 2)).await??;
        ensure!(page.routes == parents && page.next.is_none());
        Box::pin(check_recovery_replacements(
            fixture,
            &recovery_control,
            &fresh,
            &namespace,
            &fenced,
            &routed,
            rooted,
        ))
        .await?;
        drop(fenced);
        return Ok(None);
    }
    for (registry, hold) in registries.iter().zip(&parent_holds) {
        registry.release(domain.as_ref(), hold).await?;
    }
    let work = control.publish_transition(principal, &namespace, &operation_id, &publish, TIMEOUT);
    let (attempt, delivered) = Box::pin(fault::lose_directory_confirmation(work)).await;
    ensure!(
        delivered,
        "the publication CAS did not reach the injected confirmation loss"
    );
    if let Err(failure) = attempt {
        ensure!(
            failure.code == RaftFleetFailureCode::Unavailable
                && failure.outcome == RaftFleetOutcome::Unknown
        );
    }
    let after = domain.run(directory.observe()).await??;
    let recovery =
        Box::pin(control.transition_recovery_basis(&namespace, &operation_id, TIMEOUT)).await?;
    ensure!(
        recovery.phase == TransitionPhase::Published
            && recovery.directory_digest == hex::encode(after.digest())
            && recovery.transfer_archive_digest.as_deref() == Some(publish.archive_digest.as_str())
            && recovery.child_archives_digest.is_none()
            && recovery.archive_sources() == [transition_group(child.history)]
            && recovery.parents.len() == 2
    );
    let published = control
        .publish_transition(principal, &namespace, &operation_id, &publish, TIMEOUT)
        .await
        .context("reopen merged publication after losing its first confirmation")?;
    ensure!(
        published.directory_revision == view.revision() + 1
            && recovery.publication_digest.as_deref()
                == Some(published.publication_digest.as_str())
    );
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
    Box::pin(initialize_merge_child_archive(
        cluster,
        directory,
        &transition,
        child.history,
        &control,
    ))
    .await?;
    let old_scope = Box::pin(recovery_control.verify_transition_recovery_archives(
        principal,
        &parent_basis,
        parent_holds,
        TIMEOUT,
    ))
    .await;
    ensure!(matches!(old_scope, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
    let child_birth = domain
        .run(directory.materialize_selected_history(child.history))
        .await??;
    let child_registry = directory
        .open_group_archive(domain.as_ref(), &archives.pool, &child_birth)
        .await?;
    let voters = &cluster.hosts[..3];
    let archive_leader = &voters[leader(voters, child.history).await?];
    preparation::wait_active_archive(archive_leader, child.history).await?;
    let older = preparation::acquire_hold(domain, &child_registry).await?;
    let (cell, expected) = &routed[0];
    let handle = archive_leader.cell(cell.clone())?;
    let s3_receipt = handle
        .command(
            &handle.contract("Add")?,
            0i64.to_be_bytes().to_vec(),
            CommandOptions {
                durability: cellar_logic::consensus::Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(CommandId::new(ClientId::new([211; 16])?, 1)?, retry)
            },
        )
        .await?;
    ensure!(
        s3_receipt.result == (expected + 2).to_be_bytes()
            && s3_receipt.durability == cellar_logic::consensus::Durability::S3
    );
    let applied = preparation::wait_active_archive(archive_leader, child.history).await?;
    let active_basis =
        Box::pin(control.transition_recovery_basis(&namespace, &operation_id, TIMEOUT)).await?;
    ensure!(matches!(
        Box::pin(recovery_control.verify_transition_recovery_archives(
            principal, &active_basis, vec![older.clone()], TIMEOUT,
        ))
        .await,
        Err(failure) if failure.code == RaftFleetFailureCode::Conflict
    ));
    let mut current = preparation::acquire_hold(domain, &child_registry).await?;
    ensure!(
        current.marker().position().index >= applied
            && current.marker().position().index > older.marker().position().index
    );
    {
        // During selection, a producer can replace the archive descriptor
        // for the same Raft cut. Keep the selected logical cut and Fleet
        // basis fixed while reacquiring an exact hold before staging intent.
        let mut replacements = 0;
        let restored = loop {
            let result = Box::pin(recovery_control.verify_transition_recovery_archives(
                principal,
                &active_basis,
                vec![current.clone()],
                TIMEOUT,
            ))
            .await;
            match result {
                Ok(restored) => break restored,
                Err(failure)
                    if failure.code == RaftFleetFailureCode::Conflict && replacements < 3 =>
                {
                    let next = preparation::acquire_hold(domain, &child_registry).await?;
                    ensure!(
                        next.marker() == current.marker(),
                        "the selected child Raft cut changed during verification"
                    );
                    child_registry.release(domain.as_ref(), &current).await?;
                    current = next;
                    replacements += 1;
                }
                Err(error) => {
                    return Err(anyhow::Error::from(error)
                        .context("verify the selected child archive after S3 acknowledgement"));
                }
            }
        };
        assert_recovery_lineages(&restored, &expected_lineages)?;
        ensure!(matches!(
            Box::pin(recovery_control.stage_transition_recovery_intent(
                principal, &recovery_operation, &restored, TIMEOUT,
            )).await,
            Err(failure) if failure.code == RaftFleetFailureCode::PayloadConflict
        ));
        let (fresh_catalog, reopened_directory) =
            reopen_merge_controller(&cluster.fixtures[0]).await?;
        let fresh = RaftFleetControl::new(domain, fresh_catalog, vec![reopened_directory])?;
        let reopened = fresh
            .reopen_transition_recovery_intent(principal, &namespace, &recovery_operation, TIMEOUT)
            .await?
            .context("reopen recovery intent after transfer publication")?;
        ensure!(
            reopened.digest() == parent_intent.digest()
                && reopened.archive_holds() == parent_intent.archive_holds()
                && reopened.required_voter_lineages() == parent_intent.required_voter_lineages()
        );
        let [archive] = restored.archives() else {
            anyhow::bail!("expected exactly one selected child archive");
        };
        ensure!(archive.hold() == &current);
        let manifest = archive.recovery().manifest();
        ensure!(manifest.pinned_images() == routed.len());
        for (cell, _) in &routed {
            let (leaf, _) = manifest
                .pin_cell(&cell.cell)
                .context("restored merged cell")?;
            ensure!(leaf.version.history == *cell);
            if leaf.version.history == s3_receipt.version.history {
                ensure!(leaf.version == s3_receipt.version);
            }
        }
    }
    for hold in [&older, &current] {
        child_registry.release(domain.as_ref(), hold).await?;
    }
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
    ensure!(matches!(
        Box::pin(control.transition_recovery_basis(&namespace, &operation_id, TIMEOUT)).await,
        Err(failure) if failure.code == RaftFleetFailureCode::Conflict
    ));
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
    Ok(Some(child.history))
}

async fn check_recovery_replacements(
    fixture: &Fixture,
    control: &RaftFleetControl,
    fresh: &RaftFleetControl,
    namespace: &str,
    fenced: &Arc<FleetFencedTransitionRecovery>,
    routed: &[(CellHistory, i64)],
    rooted: bool,
) -> Result<()> {
    let epoch = fenced
        .archives()
        .iter()
        .map(|cut| cut.marker().route().epoch.get())
        .max()
        .context("recovery epoch")?;
    let replacements = fenced
        .archives()
        .iter()
        .enumerate()
        .map(|(index, cut)| {
            let route = cut.marker().route();
            let id = 170 + index as u8;
            Ok(CellGroupBirthSpec {
                history: GroupHistory {
                    group: GroupId::new([id; 16])?,
                    incarnation: IncarnationId::new([id + 10; 16])?,
                    ..route.history
                },
                range: route.range.clone(),
                epoch: RouteEpoch::new(epoch + 1)?,
                creation: ProposalId::new([id + 20; 16])?,
                voters: (0..3)
                    .map(|offset| {
                        Ok(InitialMember {
                            node: NodeId::new([70 + offset; 16])?,
                            member: MemberId::new(21 + u64::from(offset))?,
                        })
                    })
                    .collect::<Result<_>>()?,
                learners: vec![],
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut unsafe_roster = replacements.clone();
    unsafe_roster[0].voters[0].node = fenced.request().intent().required_voter_lineages()[0].node;
    let refused = Box::pin(control.stage_transition_recovery_replacement(
        namespace,
        fenced,
        unsafe_roster,
        TIMEOUT,
    ))
    .await;
    ensure!(matches!(refused, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
    fixture
        .store
        .fail_recovery_record_confirmation
        .store(true, Ordering::Release);
    let unknown = Box::pin(control.stage_transition_recovery_replacement(
        namespace,
        fenced,
        replacements.clone(),
        TIMEOUT,
    ))
    .await;
    ensure!(matches!(unknown, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable));
    let plan = Box::pin(fresh.reopen_transition_recovery_replacement(namespace, fenced, TIMEOUT))
        .await?
        .context("reopen selected recovery replacements")?;
    let plan = Arc::new(plan);
    let repeated = Box::pin(control.stage_transition_recovery_replacement(
        namespace,
        fenced,
        replacements.clone(),
        TIMEOUT,
    ))
    .await?;
    ensure!(plan.digest() == repeated.digest());
    ensure!(plan.mutation().operation().bytes() == fenced.request().intent().operation());
    let mut changed = replacements;
    changed[0].creation = ProposalId::new([215; 16])?;
    let conflict = Box::pin(
        control.stage_transition_recovery_replacement(namespace, fenced, changed, TIMEOUT),
    )
    .await;
    ensure!(
        matches!(conflict, Err(failure) if failure.code == RaftFleetFailureCode::PayloadConflict)
    );

    let first = plan.births()[0].route().history;
    let absent =
        Box::pin(fresh.reserve_transition_recovery_range(namespace, fenced, &plan, first, TIMEOUT))
            .await;
    ensure!(matches!(absent, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
    let workspace = tempfile::tempdir()?;
    let mut checked = 0;
    for (index, (birth, cut)) in plan.births().iter().zip(fenced.archives()).enumerate() {
        let base = workspace.path().join(index.to_string());
        let route = birth.route().clone();
        let target = fixture
            .domain
            .blocking(move || preparation::store(base, route, rooted))?
            .await??;
        let restored = Box::pin(
            fresh.prepare_transition_recovery_range(namespace, fenced, &plan, target, TIMEOUT),
        )
        .await?;
        ensure!(restored.plan_digest() == plan.digest() && restored.commitment() != [0; 32]);
        let marker = restored.manifest().marker();
        ensure!(
            marker.route() == birth.route()
                && marker.position().index.get() == 1
                && !marker.is_active()
        );
        for (cell, _) in routed {
            if let Some((old, before)) = cut.recovery().manifest().pin_cell(&cell.cell) {
                let (new, after) = restored
                    .manifest()
                    .pin_cell(&cell.cell)
                    .context("restored cell")?;
                ensure!(new.version.history.incarnation == birth.route().history.incarnation);
                ensure!(old.version.history.incarnation != new.version.history.incarnation);
                ensure!(
                    new.version.sequence.get() == 1 && new.witness.group == birth.route().history
                );
                ensure!(new.version.image.checksum == old.version.image.checksum);
                fixture
                    .domain
                    .blocking(move || check_restored_image(before, after))?
                    .await??;
                checked += 1;
            }
        }
    }
    ensure!(checked == routed.len());
    Box::pin(check_recovery_voter_reservations(
        fixture, control, fresh, namespace, fenced, &plan,
    ))
    .await?;

    Ok(())
}

async fn check_recovery_voter_reservations(
    fixture: &Fixture,
    control: &RaftFleetControl,
    fresh: &RaftFleetControl,
    namespace: &str,
    fenced: &Arc<FleetFencedTransitionRecovery>,
    plan: &Arc<cellar::sdk::fleet_operations::FleetTransitionRecoveryReplacement>,
) -> Result<()> {
    let (_, directory) = reopen_merge_controller(fixture).await?;
    let mut standbys = Vec::new();
    let checked = Box::pin(async {
        let mut boots = Vec::new();
        for (id, capacity_request) in [(70, 190), (71, 192), (72, 194)] {
            let peer = peer_fixture(fixture, id).await?;
            let root = tempfile::tempdir()?;
            let node = start_node(root.path(), peer.application.clone(), &peer.spec.label).await?;
            let (mut config, services) = peer.config(&node, LogOpenMode::Create, id + 100).await?;
            crate::managed_fixture::archives::configure(&mut config);
            let host = Box::pin(node.start_raft_cells(config, services)).await?;
            standbys.push((root, node, host));
            let (_, _, host) = standbys.last().context("started recovery voter")?;
            Box::pin(admit_capacity(&peer, host, &directory, capacity_request)).await?;
            let view = host.observe_node(TIMEOUT).await?;
            ensure!(view.node.groups == 0);
            boots.push(view.fleet_boot.context("new recovery voter boot")?);
        }

        let first = plan.births()[0].route().history;
        fixture
            .store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        let unknown = Box::pin(
            control.reserve_transition_recovery_range(namespace, fenced, plan, first, TIMEOUT),
        )
        .await;
        ensure!(
            matches!(unknown, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable)
        );
        ensure!(!fixture
            .store
            .fail_fleet_confirmation
            .load(Ordering::Acquire));
        let mut previous = 0;
        let mut reserved = Vec::new();
        for birth in plan.births() {
            let proof = Box::pin(fresh.reserve_transition_recovery_range(
                namespace,
                fenced,
                plan,
                birth.route().history,
                TIMEOUT,
            ))
            .await?;
            ensure!(proof.route() == birth.route() && proof.plan_digest() == plan.digest());
            ensure!(proof.voter_boots() == boots.as_slice());
            ensure!(proof.applied_revision() > previous);
            previous = proof.applied_revision();
            reserved.push(Arc::new(proof));
        }
        let replayed = Box::pin(
            control.reserve_transition_recovery_range(namespace, fenced, plan, first, TIMEOUT),
        )
        .await?;
        ensure!(
            replayed.applied_revision() < previous && replayed.voter_boots() == boots.as_slice()
        );
        let revision = fixture.catalog.read(TIMEOUT).await?.revision();
        let recovery = preparation::recovery_pool()?;
        let mut quorum_digest = None;
        for (voter_index, (_, _, host)) in standbys.iter().enumerate() {
            let node = NodeId::new(boots[voter_index].node)?;
            inspect_current(host, node, revision).await?;
            for (range_index, reservation) in reserved.iter().enumerate() {
                let prepared = Box::pin(host.prepare_managed_recovery_seed(
                    fenced,
                    plan,
                    reservation,
                    &recovery,
                    TIMEOUT,
                ))
                .await?;
                if voter_index == 0 && range_index == 0 {
                    fixture
                        .store
                        .fail_recovery_record_confirmation
                        .store(true, Ordering::Release);
                    let lost = Box::pin(host.seed_managed_recovery_range(
                        fenced.clone(),
                        plan.clone(),
                        reservation.clone(),
                        prepared.clone(),
                        TIMEOUT,
                    ))
                    .await;
                    ensure!(lost.is_err());
                    ensure!(!fixture
                        .store
                        .fail_recovery_record_confirmation
                        .load(Ordering::Acquire));
                    ensure!(host.observe_node(TIMEOUT).await?.node.groups == 1);
                }
                let receipt = Box::pin(host.seed_managed_recovery_range(
                    fenced.clone(),
                    plan.clone(),
                    reservation.clone(),
                    prepared.clone(),
                    TIMEOUT,
                ))
                .await?;
                ensure!(
                    receipt.route == *reservation.route() && receipt.plan_digest == plan.digest()
                );
                ensure!(receipt.genesis_commitment == prepared.commitment());
                ensure!(
                    receipt.boot == boots[voter_index]
                        && receipt.member.get() == 21 + voter_index as u64
                );
                if voter_index == 1 && range_index == 0 {
                    let incomplete = Box::pin(control.select_transition_recovery_seed_quorums(
                        namespace, fenced, plan, TIMEOUT,
                    )).await;
                    ensure!(matches!(incomplete, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable));
                }
            }
            if voter_index == 1 {
                fixture.store.fail_recovery_record_confirmation.store(true, Ordering::Release);
                let unknown = Box::pin(control.select_transition_recovery_seed_quorums(
                    namespace, fenced, plan, TIMEOUT,
                )).await;
                ensure!(matches!(unknown, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable));
                ensure!(!fixture.store.fail_recovery_record_confirmation.load(Ordering::Acquire));
                let selected = Box::pin(fresh.select_transition_recovery_seed_quorums(
                    namespace, fenced, plan, TIMEOUT,
                )).await?;
                ensure!(selected.plan_digest() == plan.digest() && selected.ranges().len() == reserved.len());
                ensure!(selected.ranges().iter().zip(plan.births()).all(|(range, birth)| {
                    range.birth_digest() == birth.route().birth_digest
                        && range.members() == [21, 22]
                        && range.genesis_commitment() != [0; 32]
                }));
                quorum_digest = Some(selected.digest());
            }
            let view = host.observe_node(TIMEOUT).await?;
            ensure!(view.node.groups == reserved.len() as u32 && view.registered_histories == 0);
        }
        let reopened = Box::pin(control.select_transition_recovery_seed_quorums(
            namespace, fenced, plan, TIMEOUT,
        )).await?;
        ensure!(Some(reopened.digest()) == quorum_digest);
        ensure!(reopened.ranges().iter().all(|range| range.members() == [21, 22]));
        let locked = fixture.domain.run(directory.observe()).await??;
        let work = control.publish_transition_recovery(namespace, fenced, plan, &reopened, TIMEOUT);
        let (unknown, delivered) = Box::pin(fault::lose_directory_confirmation(work)).await;
        ensure!(delivered && matches!(unknown, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable));
        let operation = hex::encode(fenced.request().intent().operation());
        let principal = "managed-test";
        let expected = (plan.digest(), reopened.digest());
        let publication = Box::pin(fresh.reopen_transition_recovery_publication(
            namespace, &operation, principal, expected, TIMEOUT,
        )).await?.context("reopen recovery publication by durable operation")?;
        let replayed = Box::pin(control.publish_transition_recovery(
            namespace, fenced, plan, &reopened, TIMEOUT,
        )).await?;
        ensure!(replayed == publication);
        let after = fixture.domain.run(directory.observe()).await??;
        ensure!(after.revision() == locked.revision() + 1 && publication.revision() == after.revision());
        let page = fixture.domain.run(directory.group_page(&after, None, 2)).await??;
        ensure!(page.next.is_none() && page.routes.iter().eq(plan.births().iter().map(|birth| birth.route())));
        for birth in plan.births() {
            let denied = fixture.domain.run(directory.materialize_selected_history(birth.route().history)).await?;
            ensure!(matches!(denied, Err(cellar::application::consensus::CatalogError::Fenced)));
        }
        Ok::<(), anyhow::Error>(())
    })
    .await;
    let mut shutdown = Ok(());
    for (_root, node, host) in standbys {
        let joined = async {
            ensure!(matches!(
                node.shutdown(ShutdownMode::Preserve).await?,
                NodeOutcome::Stopped(_)
            ));
            let report = host.close().await?;
            ensure!(report.groups_total == plan.births().len());
            ensure!(
                report.groups.iter().all(|group| !group.active
                    && !group.failed
                    && group.applied.index.get() == 1
                    && group.committed == group.applied.index),
                "managed recovery shutdown must preserve inactive genesis: {report:?}"
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;
        shutdown = shutdown.and(joined);
    }
    checked?;
    shutdown
}

fn check_restored_image(before: CellImagePin, after: CellImagePin) -> Result<()> {
    match (before, after) {
        (CellImagePin::Materialized(before), CellImagePin::Materialized(after)) => {
            ensure!(before.state() == after.state());
            let (mut before, mut after) = (before.open_reader()?, after.open_reader()?);
            let mut offset = 0;
            while offset < before.length() {
                let length = (before.length() - offset).min(64 * 1024) as usize;
                ensure!(
                    before.read_exact_at(offset, length)? == after.read_exact_at(offset, length)?
                );
                offset += length as u64;
            }
        }
        (CellImagePin::Extents(before), CellImagePin::Extents(after)) => {
            let count = before.manifest().geometry().extent_count();
            ensure!(count == after.manifest().geometry().extent_count());
            for index in 0..count {
                let id = before.manifest().extent(index).context("source extent")?;
                ensure!(after.manifest().extent(index) == Some(id));
                ensure!(before.view().read_extent(id)? == after.view().read_extent(id)?);
            }
        }
        _ => anyhow::bail!("expected matching nonempty image representations"),
    }
    Ok(())
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

async fn retire_merge_parent(
    control: &RaftFleetControl,
    voters: &[RaftCells],
    principal: &str,
    request: &RaftFleetTransitionSourceSealRequest,
) -> Result<RaftFleetTransitionSourceRetirement> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for voter in voters {
                let local = control.clone().with_node_observer(voter.clone());
                match local
                    .retire_transition_source(
                        principal,
                        &request.namespace_id,
                        &request.operation_id,
                        request,
                        TIMEOUT,
                    )
                    .await
                {
                    Ok(retired) if retired.applied.is_some() => return Ok(retired),
                    Ok(_) => (),
                    Err(error)
                        if matches!(
                            error.code,
                            RaftFleetFailureCode::Unavailable | RaftFleetFailureCode::Busy
                        ) =>
                    {
                        ()
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("operator parent retirement")?
}

async fn initialize_merge_child_archive(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    child: GroupHistory,
    control: &RaftFleetControl,
) -> Result<()> {
    let archive_request = RaftFleetTransitionChildSetupRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(child.namespace.bytes()),
        operation_id: hex::encode(transition.request.operation),
        request_id: hex::encode([185; 16]),
        plan_digest: hex::encode(transition.plan.digest()),
        child: RaftFleetTransitionGroup {
            group_id: hex::encode(child.group.bytes()),
            incarnation_id: hex::encode(child.incarnation.bytes()),
        },
        step: RaftFleetTransitionChildStep::InitializeArchive,
    };
    for _ in 0..2 {
        let initialized = Box::pin(crate::managed_fixture::operator::setup_transition_child(
            &cluster.fixtures[0],
            directory,
            &transition.request.principal,
            &cluster.hosts[0],
            &archive_request,
            None,
        ))
        .await?;
        ensure!(
            initialized.request == archive_request
                && initialized.result == RaftFleetTransitionChildSetupResult::ArchiveInitialized
        );
    }
    for (index, generation) in [121, 126, 127].into_iter().enumerate() {
        Box::pin(restart_voter(cluster, index, generation, Some(child), 1)).await?;
    }
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
    let applied = preparation::wait_active_archive(&cluster.hosts[0], child).await?;
    let coverage_request = RaftFleetTransitionPrepareRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(child.namespace.bytes()),
        operation_id: hex::encode(transition.request.operation),
        request_id: hex::encode([186; 16]),
        plan_digest: hex::encode(transition.plan.digest()),
    };
    let mut digest = None;
    for _ in 0..2 {
        let verifier_root = tempfile::tempdir()?;
        let verifier = crate::managed_fixture::verifier::open(
            &cluster.fixtures[0],
            verifier_root.path(),
            archives.rooted,
            Default::default(),
        )
        .await?;
        let covered = Box::pin(crate::managed_fixture::operator::verify_transition_archive(
            &cluster.fixtures[0],
            directory,
            &transition.request.principal,
            &coverage_request,
            (verifier, preparation::recovery_pool()?),
            "verify-child-archives",
        ))
        .await?;
        ensure!(covered.request == coverage_request && covered.phase == TransitionPhase::Active);
        ensure!(digest
            .as_ref()
            .is_none_or(|old| old == &covered.archive_digest));
        digest = Some(covered.archive_digest);
    }
    let holds = fleet
        .reopen_managed_transfer_child_holds(transition, directory, TIMEOUT)
        .await?
        .context("selected child archive")?;
    ensure!(holds.len() == 1 && holds[0].marker().position().index >= applied);
    let principal = &transition.request.principal;
    for (index, parent) in parents.iter().enumerate() {
        let retirement = RaftFleetTransitionSourceSealRequest {
            catalog_id: control.catalog_id().into(),
            namespace_id: hex::encode(child.namespace.bytes()),
            operation_id: hex::encode(transition.request.operation),
            request_id: hex::encode([187 + index as u8; 16]),
            plan_digest: hex::encode(transition.plan.digest()),
            source: RaftFleetTransitionGroup {
                group_id: hex::encode(parent.history.group.bytes()),
                incarnation_id: hex::encode(parent.history.incarnation.bytes()),
            },
        };
        let completed = Box::pin(retire_merge_parent(
            control,
            &cluster.hosts[3..],
            principal,
            &retirement,
        ))
        .await?;
        let expected_phase = if index == 0 {
            TransitionPhase::Active
        } else {
            TransitionPhase::Retired
        };
        ensure!(completed.request == retirement && completed.phase == expected_phase);
        let applied = completed
            .applied
            .as_ref()
            .context("durable parent tombstone")?;
        let replay = Box::pin(
            crate::managed_fixture::operator::reopen_transition_retirement(
                &cluster.fixtures[0],
                directory,
                principal,
                &retirement,
            ),
        )
        .await?;
        ensure!(replay == completed);
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let mut applied_on_all = true;
                for voter in &cluster.hosts[3..] {
                    let status = voter.group(parent.history)?.status().await?;
                    applied_on_all &= status.applied.index.get() >= applied.index;
                }
                if applied_on_all {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("retirement applied on retained parent voters")??;
    }
    let retired = fleet
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .and_then(|reopened| reopened.journal)
        .context("rooted merge retirement")?;
    ensure!(retired.phase() == TransitionPhase::Retired && !retired.parent_holds_released());
    let cut = fleet.read(TIMEOUT).await?;
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
    Box::pin(start_voter(
        cluster, index, generation, previous, registered,
    ))
    .await
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

fn assert_recovery_lineages(
    restored: &cellar::sdk::fleet_operations::RaftFleetVerifiedTransitionRecovery,
    expected: &[(GroupHistory, usize, FleetNodeSpec)],
) -> Result<()> {
    let required = restored.required_voter_lineages();
    ensure!(required.len() == expected.len());
    for (group, index, node) in expected {
        ensure!(required.iter().any(|lineage| {
            lineage.group == *group
                && lineage.member.get() == *index as u64 + 1
                && lineage.node == node.node
                && lineage.disk == node.disk
                && lineage.physical_host == node.physical_host
        }));
    }
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
