// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The fleet slot owns the exact pre-seal directory operation.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::{cell_routing_key, TransitionPhase},
    FleetChange, FleetMutation, FleetTable,
};

use cellar::sdk::fleet_operations::{
    RaftFleetControl, RaftFleetFailureCode, RaftFleetTransitionGroup,
    RaftFleetTransitionSealRequest, RaftFleetTransitionSourceSealRequest,
};

#[path = "cell_group_managed_transition_preparation.rs"]
mod preparation;

#[path = "cell_group_managed_transition_retirement.rs"]
mod retirement;

#[path = "cell_group_managed_transition_merge.rs"]
pub(crate) mod merge;

#[path = "cell_group_transition_restart.rs"]
pub(crate) mod restart;

#[path = "cell_group_transition_effects.rs"]
pub(crate) mod effects;

#[path = "cell_group_transition_closure.rs"]
pub(crate) mod closure;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_split_plan_fences_membership_and_replays_cancellation() -> Result<()> {
    Box::pin(run_plan(
        false,
        Arc::new(fault::Store::default()),
        Decision::Cancel,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_split_plan() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-transition",
            rooted,
        )?));
        Box::pin(run_plan(rooted, store, Decision::Cancel)).await?;
    }
    Ok(())
}

pub(crate) async fn child_requires_install_and_publication(rooted: bool) -> Result<()> {
    Box::pin(run_plan(
        rooted,
        Arc::new(fault::Store::default()),
        Decision::Authorize,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_split_authorization() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-transition-authorize",
            rooted,
        )?));
        Box::pin(run_plan(rooted, store, Decision::Authorize))
            .await
            .with_context(|| format!("managed split authorization, extent images={rooted}"))?;
    }
    Ok(())
}

pub(crate) async fn qualify_retirement(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    Box::pin(run_plan(rooted, store, Decision::Retirement)).await
}

enum Decision {
    Authorize,
    Retirement,
    Publication,
    Cancel,
    SplitMerge {
        recovery_barrier: bool,
        check_child_fence: bool,
    },
}

async fn run_plan(rooted: bool, store: Arc<fault::Store>, decision: Decision) -> Result<()> {
    let class = effects::class()?.unwrap_or(counter_class()?);
    let mut first = Fixture::with_store(rooted, class, store).await?;
    if effects::enabled() {
        first.effects = Some(RaftEffectOptions::default());
        // The signed inbox protocol admits requests up to 60 KiB. Keep the
        // ordinary 1 KiB SQL fixture profile unchanged for all other cases.
        first.max_input_bytes = 64 * 1024;
    }
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let merge = matches!(decision, Decision::SplitMerge { .. });
    let publication_only = matches!(decision, Decision::Publication);
    let expected_children = match decision {
        Decision::Authorize
        | Decision::Retirement
        | Decision::Publication
        | Decision::SplitMerge { .. } => 2,
        Decision::Cancel => 0,
    };
    let result = exercise(&mut cluster, rooted, decision).await;
    if merge {
        let shutdown = merge::close(
            cluster.nodes,
            &cluster.hosts,
            result.as_ref().ok().copied().flatten(),
        )
        .await;
        cluster.fixtures[0].domain.shutdown().await;
        result?;
        return shutdown;
    }
    let activated = result.is_ok() && expected_children > 0 && !publication_only;
    let pending_quorums = if result.is_ok() { expected_children } else { 0 };
    let mut nodes = std::mem::take(&mut cluster.nodes);
    let spares = nodes.split_off(3);
    let joined = close_group_nodes(nodes, &cluster.hosts).await;
    // Start all destination drains together. Serial shutdown can elect a
    // successor while the next voter is already draining its local apply.
    let joined_spares =
        futures_util::future::join_all(spares.into_iter().zip(&cluster.hosts[3..]).map(
            |(node, host)| {
                stop_transfer_destination(node, host, expected_children, pending_quorums, activated)
            },
        ))
        .await;
    cluster.fixtures[0].domain.shutdown().await;
    result?;
    joined?;
    for result in joined_spares {
        result?;
    }
    if publication_only {
        closure::erase_and_restore(cluster).await?;
    }
    Ok(())
}

async fn stop_transfer_destination(
    node: Node,
    host: &RaftCells,
    expected: usize,
    pending_quorums: usize,
    active: bool,
) -> Result<()> {
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = host.close().await?;
    ensure!(
        report.groups_total == expected
            && report.groups.len() == expected
            && report.omitted_groups == 0
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers,
        "transfer destination shutdown inventory: {report:?}"
    );
    ensure!(
        report.groups.iter().all(|group| group.active == active
            && !group.failed
            && group.applied.index == group.committed),
        "expected active={active}, shutdown inventory: {report:?}"
    );
    ensure!(
        report
            .groups
            .iter()
            .filter(|group| group.applied.index.get() > 1)
            .count()
            == pending_quorums
    );
    Ok(())
}

// Allocate the fixture's composite future outside the caller's poll frame.
// Real S3 adds HTTP stack below retained startup.
fn exercise(
    cluster: &mut ManagedCluster,
    rooted: bool,
    decision: Decision,
) -> impl std::future::Future<Output = Result<Option<GroupHistory>>> + '_ {
    Box::pin(exercise_inner(cluster, rooted, decision))
}

async fn exercise_inner(
    cluster: &mut ManagedCluster,
    rooted: bool,
    decision: Decision,
) -> Result<Option<GroupHistory>> {
    let fixture = &cluster.fixtures[0];
    let fleet = &fixture.catalog;
    let directory = &cluster.directory;
    let domain = &fixture.domain;
    let scope = cluster.history;
    let merge = matches!(decision, Decision::SplitMerge { .. });
    let pivot = if merge || effects::enabled() {
        let left = cell_routing_key(scope.namespace, &CellId::new("Counter:cold-0")?);
        let right = cell_routing_key(scope.namespace, &CellId::new("Counter:cold-1")?);
        left.max(right)
    } else {
        cell_routing_key(scope.namespace, &CellId::new("managed-split-pivot")?)
    };
    let operation = ProposalId::new([231; 16])?;
    let identity = |id, revision| FleetRequestIdentity {
        operation: operation.bytes(),
        ..request(id, revision)
    };
    let (children, plan, original) = if merge {
        let admitted = Box::pin(merge::admit_operator_split(
            cluster,
            pivot.cell().as_str(),
            operation,
        ))
        .await?;
        (
            admitted.children,
            admitted.selected.plan,
            admitted.selected.request,
        )
    } else {
        let parent = domain
            .run(directory.materialize_selected_history(scope))
            .await??;
        let view = domain.run(directory.observe()).await??;
        let (left, right) = parent.route().range.split(pivot)?;
        let voters = cluster.fixtures[3..]
            .iter()
            .enumerate()
            .map(|(index, fixture)| {
                Ok(InitialMember {
                    member: MemberId::new(index as u64 + 1)?,
                    node: fixture.spec.node,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut children = Vec::new();
        for (id, range) in [(232, left), (233, right)] {
            let snapshot = fleet.read(TIMEOUT).await?;
            let reserved = fleet
                .reserve_birth(
                    &snapshot,
                    &identity(id, snapshot.revision()),
                    directory,
                    CellGroupBirthSpec {
                        history: GroupHistory {
                            group: GroupId::new([id; 16])?,
                            incarnation: IncarnationId::new([id; 16])?,
                            ..scope
                        },
                        range,
                        epoch: RouteEpoch::new(2)?,
                        creation: ProposalId::new([id; 16])?,
                        voters: voters.clone(),
                        learners: vec![],
                    },
                    "Counter",
                    TIMEOUT,
                )
                .await?;
            children.push(reserved.authorization.route().clone());
        }
        let plan = directory.prepare_split(&view, operation, parent.route(), &children)?;
        ensure!(fleet
            .reopen_managed_transition(directory, operation, TIMEOUT)
            .await?
            .is_none());
        let before = fleet.read(TIMEOUT).await?;
        let original = identity(234, before.revision());
        fixture
            .store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        let lost = fleet
            .plan_managed_transition(&original, directory, &plan, TIMEOUT)
            .await;
        ensure!(
            matches!(lost, Err(FleetCatalogError::Storage(_))),
            "lost plan confirmation: {lost:?}"
        );
        ensure!(!fixture
            .store
            .fail_fleet_confirmation
            .load(Ordering::Acquire));
        ensure!(fleet.read(TIMEOUT).await?.revision() == before.revision() + 1);
        ensure!(domain.run(directory.active_transition()).await??.is_none());
        (children, plan, original)
    };
    restart::register(&children);
    let reopened = Fixture::open_catalog(domain, fixture.store.clone(), scope.cluster)?;
    let adapter = Catalog::new(
        fixture.store.clone(),
        CatalogOptions {
            prefix: "managed/".into(),
            ..Default::default()
        },
    )?;
    let namespace = domain
        .run(adapter.existing_namespace(fixture.declaration.namespace.clone()))
        .await??;
    let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let directory = &directory;
    let recovered = reopened
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("reopen the exact admitted split with a fresh controller")?;
    ensure!(
        recovered.journal.as_ref().map(|journal| journal.phase())
            == merge.then_some(TransitionPhase::Planned)
    );
    ensure!(recovered.request == original && recovered.plan.encode() == plan.encode());
    drop(plan);
    let plan = &recovered.plan;
    let original = &recovered.request;
    let parent = domain
        .run(directory.materialize_selected_history(scope))
        .await??;
    let first = reopened
        .plan_managed_transition(original, directory, plan, TIMEOUT)
        .await?;
    ensure!(first.phase() == TransitionPhase::Planned);
    ensure!(
        fleet
            .plan_managed_transition(original, directory, plan, TIMEOUT)
            .await?
            == first
    );
    ensure!(matches!(
        domain
            .run(directory.authorize_transition_seal(plan))
            .await?,
        Err(CatalogError::ManagedAdmissionRequired)
    ));

    ensure!(matches!(
        reopened
            .authorize_managed_transition_seal(original, directory, plan, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));

    let control = RaftFleetControl::new(domain, reopened.clone(), vec![directory.clone()])?;
    let mut seal_request = RaftFleetTransitionSealRequest {
        catalog_id: hex::encode(reopened.catalog_id()?),
        namespace_id: hex::encode(scope.namespace.bytes()),
        operation_id: hex::encode(operation.bytes()),
        request_id: hex::encode([237; 16]),
        plan_digest: hex::encode(plan.digest()),
        preparation_digest: hex::encode([88; 32]),
    };
    let unprepared = control
        .authorize_transition_seal(
            &original.principal,
            &seal_request.namespace_id,
            &seal_request.operation_id,
            &seal_request,
            TIMEOUT,
        )
        .await;
    ensure!(matches!(unprepared, Err(error) if error.code == RaftFleetFailureCode::Conflict));

    let (registry, held, seeded, archived) = Box::pin(preparation::exercise(
        cluster, &reopened, directory, &recovered, &parent, &children, rooted,
    ))
    .await?;

    closure::acknowledged(&archived);
    effects::before_seal(cluster, &seeded.cells).await?;
    let admitted = fleet.read(TIMEOUT).await?;
    let other = identity(235, admitted.revision());
    let mutation = FleetMutation {
        id: other.id,
        operation: other.operation,
        expected_revision: other.expected_revision,
        principal: other.principal,
        changes: vec![FleetChange {
            table: FleetTable::Admissions,
            key: "test-membership".into(),
            value: None,
        }],
    };
    ensure!(matches!(
        fleet.commit(&admitted, &mutation, TIMEOUT).await,
        Err(FleetCatalogError::DisruptionInProgress { operation: actual }) if actual == operation.bytes()
    ));
    let cancel = identity(236, admitted.revision());
    if matches!(
        decision,
        Decision::Authorize
            | Decision::Retirement
            | Decision::Publication
            | Decision::SplitMerge { .. }
    ) {
        ensure!(matches!(
            reopened
                .authorize_managed_transition_seal(&cancel, directory, plan, TIMEOUT)
                .await,
            Err(FleetCatalogError::PayloadConflict)
        ));
        let prepared = control
            .transition_status(
                &seal_request.namespace_id,
                &seal_request.operation_id,
                TIMEOUT,
            )
            .await?;
        ensure!(prepared.phase == Some(TransitionPhase::Prepared));
        let source_request = RaftFleetTransitionSourceSealRequest {
            catalog_id: seal_request.catalog_id.clone(),
            namespace_id: seal_request.namespace_id.clone(),
            operation_id: seal_request.operation_id.clone(),
            request_id: hex::encode([177; 16]),
            plan_digest: seal_request.plan_digest.clone(),
            source: RaftFleetTransitionGroup {
                group_id: hex::encode(scope.group.bytes()),
                incarnation_id: hex::encode(scope.incarnation.bytes()),
            },
        };
        let before_authorization = control
            .seal_transition_source(
                &original.principal,
                &source_request.namespace_id,
                &source_request.operation_id,
                &source_request,
                TIMEOUT,
            )
            .await;
        ensure!(
            matches!(before_authorization, Err(error) if error.code == RaftFleetFailureCode::Conflict)
        );
        let mismatch = control
            .authorize_transition_seal(
                &original.principal,
                &seal_request.namespace_id,
                &seal_request.operation_id,
                &seal_request,
                TIMEOUT,
            )
            .await;
        ensure!(matches!(mismatch, Err(error) if error.code == RaftFleetFailureCode::Conflict));
        seal_request.preparation_digest =
            prepared.preparation_digest.context("prepared archive")?;
        let foreign = control
            .authorize_transition_seal(
                "different-operator",
                &seal_request.namespace_id,
                &seal_request.operation_id,
                &seal_request,
                TIMEOUT,
            )
            .await;
        ensure!(matches!(foreign, Err(error) if error.code == RaftFleetFailureCode::Forbidden));
        let reply = crate::managed_fixture::operator::authorize_transition_seal(
            fixture,
            directory,
            &original.principal,
            &seal_request,
        )
        .await?;
        ensure!(reply.request == seal_request && reply.phase == TransitionPhase::SealAuthorized);
        let replay = control
            .authorize_transition_seal(
                &original.principal,
                &seal_request.namespace_id,
                &seal_request.operation_id,
                &seal_request,
                TIMEOUT,
            )
            .await?;
        ensure!(replay == reply);
        let authorized = reopened
            .authorize_managed_transition_seal(original, directory, plan, TIMEOUT)
            .await?;
        ensure!(authorized.phase() == TransitionPhase::SealAuthorized);
        ensure!(
            authorized.preparation().map(hex::encode).as_deref()
                == Some(seal_request.preparation_digest.as_str())
        );
        ensure!(
            fleet
                .authorize_managed_transition_seal(original, directory, plan, TIMEOUT)
                .await?
                == authorized
        );
        ensure!(matches!(
            fleet
                .cancel_managed_transition(&admitted, &cancel, directory, plan, TIMEOUT)
                .await,
            Err(FleetCatalogError::Conflict)
        ));
        ensure!(fleet.read(TIMEOUT).await?.revision() == admitted.revision());
        ensure!(
            domain
                .run(directory.active_transition())
                .await??
                .context("authorized plan still owns the topology")?
                .1
                == authorized
        );
        ensure!(
            domain
                .run(directory.managed_preparation_holds(plan))
                .await??
                == vec![held.clone()]
        );
        let without_observer = control
            .seal_transition_source(
                &original.principal,
                &source_request.namespace_id,
                &source_request.operation_id,
                &source_request,
                TIMEOUT,
            )
            .await;
        ensure!(
            matches!(without_observer, Err(error) if error.code == RaftFleetFailureCode::Unsupported)
        );
        restart::checkpoint(
            cluster,
            directory,
            &recovered,
            TransitionPhase::SealAuthorized,
        )
        .await?;
        let voter = leader(&cluster.hosts[..3], scope).await?;
        let voter = &cluster.hosts[voter];
        let cut = Box::pin(crate::managed_fixture::operator::seal_transition_source(
            fixture,
            directory,
            &original.principal,
            voter,
            &source_request,
        ))
        .await?;
        let source = voter.group(scope)?;
        let CellGroupSealOutcome::Applied(seal) =
            source.seal_managed_source(&recovered, TIMEOUT).await?
        else {
            anyhow::bail!("operator seal lacks its durable source receipt");
        };
        ensure!(
            cut.index == seal.position().index.get()
                && cut.term == seal.position().term.get()
                && cut.authorization_digest == hex::encode(seal.authorization())
                && !source.status().await?.ready
        );
        Box::pin(preparation::verify_sealed_cut(
            cluster,
            &reopened,
            directory,
            &recovered,
            &parent,
            held.clone(),
            &seal,
        ))
        .await?;
        if matches!(decision, Decision::Publication) {
            return Ok(None);
        }
        let cells = seeded.cells.clone();
        Box::pin(preparation::verify_transferred_sql(
            cluster, directory, seeded,
        ))
        .await?;
        restart::checkpoint(cluster, directory, &recovered, TransitionPhase::Active).await?;
        let tombstone = Box::pin(retirement::reopen_and_retire(
            cluster, directory, &recovered,
        ))
        .await?;
        Box::pin(preparation::verify_child_archive_coverage(
            cluster,
            directory,
            &recovered,
            tombstone,
            cells.clone(),
        ))
        .await?;
        restart::checkpoint(cluster, directory, &recovered, TransitionPhase::Retired).await?;
        effects::resume_children(cluster, directory).await?;
        if matches!(decision, Decision::Retirement) {
            Box::pin(retirement::qualify_parent_cleanup(
                cluster, directory, &recovered, &registry, &held, &cells, archived,
            ))
            .await?;
        } else {
            Box::pin(retirement::park_and_release_parent(
                cluster, directory, &recovered,
            ))
            .await?;
        }
        Box::pin(preparation::verify_released_source_holds(
            cluster, directory, plan, &registry, held,
        ))
        .await?;
        effects::after_cleanup(cluster, directory).await?;
        return match decision {
            Decision::SplitMerge {
                recovery_barrier,
                check_child_fence,
            } => {
                merge::run(
                    cluster,
                    directory,
                    cells,
                    rooted,
                    recovery_barrier,
                    check_child_fence,
                )
                .await
            }
            _ => Ok(None),
        };
    }
    fixture
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = fleet
        .cancel_managed_transition(&admitted, &cancel, directory, plan, TIMEOUT)
        .await;
    ensure!(matches!(lost, Err(FleetCatalogError::Storage(_))));
    ensure!(!fixture
        .store
        .fail_fleet_confirmation
        .load(Ordering::Acquire));
    ensure!(fleet.read(TIMEOUT).await?.revision() == admitted.revision() + 1);
    let cancelled = fleet
        .cancel_managed_transition(&admitted, &cancel, directory, plan, TIMEOUT)
        .await?;
    ensure!(
        fleet
            .cancel_managed_transition(&admitted, &cancel, directory, plan, TIMEOUT)
            .await?
            .applied_revision
            == cancelled.applied_revision
    );
    ensure!(
        fleet
            .plan_managed_transition(original, directory, plan, TIMEOUT)
            .await?
            .phase()
            == TransitionPhase::Cancelled
    );
    ensure!(domain.run(directory.active_transition()).await??.is_none());
    ensure!(matches!(
        reopened
            .authorize_managed_transition_seal(original, directory, plan, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let cancelled_plan = reopened
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("cancelled operation must retain its exact historical receipt")?;
    ensure!(cancelled_plan.request == *original && cancelled_plan.plan.encode() == plan.encode());
    ensure!(
        cancelled_plan
            .journal
            .context("missing cancellation decision")?
            .phase()
            == TransitionPhase::Cancelled
    );
    ensure!(matches!(
        domain
            .run(directory.managed_preparation_holds(plan))
            .await?,
        Err(CatalogError::Fenced)
    ));
    ensure!(
        reopened
            .staged_managed_preparation_holds(directory, operation, TIMEOUT)
            .await?
            == Some(vec![held.clone()])
    );
    let archives = CellGroupArchivePool::new(Default::default())?;
    for _ in 0..2 {
        reopened
            .release_cancelled_preparation_holds(directory, operation, &archives, TIMEOUT)
            .await?;
    }
    preparation::verify_expired_holds(domain, directory, &registry, vec![held]).await?;
    let view = domain.run(directory.observe()).await??;
    ensure!(view.groups() == 1);
    Ok(None)
}

async fn commit_source_seal(
    voters: &[RaftCells],
    transition: &ReopenedManagedCellGroupTransition,
    history: GroupHistory,
) -> Result<GroupSourceSeal> {
    let voter = leader(voters, history).await?;
    let follower = voters[(voter + 1) % voters.len()].group(history)?;
    ensure!(matches!(
        follower.seal_managed_source(transition, TIMEOUT).await,
        Err(CellGroupControllerError::Runtime(RuntimeError::NotLeader))
    ));
    let source = voters[voter].group(history)?;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let applied = tokio::time::timeout_at(deadline, async {
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match source.seal_managed_source(transition, remaining).await {
                Ok(CellGroupSealOutcome::Applied(seal)) => return Ok(seal),
                Ok(CellGroupSealOutcome::Accepted(_))
                | Err(CellGroupControllerError::Runtime(
                    RuntimeError::NotReady | RuntimeError::Overloaded,
                )) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .context("managed parent did not apply its exact source seal")??;
    ensure!(applied.position().index.get() > 0 && !source.status().await?.ready);
    let CellGroupSealOutcome::Applied(replayed) =
        source.seal_managed_source(transition, TIMEOUT).await?
    else {
        anyhow::bail!("managed seal replay attempted another proposal");
    };
    ensure!(replayed == applied);
    Ok(applied)
}

pub(crate) fn qualify_split_merge(
    rooted: bool,
    store: Arc<fault::Store>,
    check_child_fence: bool,
) -> futures_util::future::LocalBoxFuture<'static, Result<()>> {
    Box::pin(run_plan(
        rooted,
        store,
        Decision::SplitMerge {
            recovery_barrier: false,
            check_child_fence,
        },
    ))
}
