// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The fleet slot owns the exact pre-seal directory operation.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::{cell_routing_key, TransitionPhase},
    FleetChange, FleetMutation, FleetTable,
};

#[path = "cell_group_managed_transition_preparation.rs"]
mod preparation;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_split_authorizes_only_the_prepared_plan() -> Result<()> {
    Box::pin(run_plan(
        false,
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
        Box::pin(run_plan(rooted, store, Decision::Authorize)).await?;
    }
    Ok(())
}

enum Decision {
    Authorize,
    Cancel,
}

async fn run_plan(rooted: bool, store: Arc<fault::Store>, decision: Decision) -> Result<()> {
    let first = Fixture::with_store(rooted, counter_class()?, store).await?;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[7, 8, 9],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let expected_children = match decision {
        Decision::Authorize => 2,
        Decision::Cancel => 0,
    };
    let result = Box::pin(exercise(&cluster, rooted, decision)).await;
    let spares = cluster.nodes.split_off(3);
    let joined = close_group_nodes(cluster.nodes, &cluster.hosts).await;
    let joined_spares = async {
        for (node, host) in spares.into_iter().zip(&cluster.hosts[3..]) {
            stop_transfer_destination(node, host, expected_children).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    cluster.fixtures[0].domain.shutdown().await;
    result?;
    joined?;
    joined_spares
}

async fn stop_transfer_destination(node: Node, host: &RaftCells, expected: usize) -> Result<()> {
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
    ensure!(report.groups.iter().all(|group| {
        !group.active
            && !group.failed
            && group.committed.get() == 1
            && group.applied.index.get() == 1
    }));
    Ok(())
}

async fn exercise(cluster: &ManagedCluster, rooted: bool, decision: Decision) -> Result<()> {
    let fixture = &cluster.fixtures[0];
    let fleet = &fixture.catalog;
    let directory = &cluster.directory;
    let domain = &fixture.domain;
    let scope = cluster.history;
    let parent = domain
        .run(directory.materialize_selected_history(scope))
        .await??;
    let view = domain.run(directory.observe()).await??;
    let (left, right) = parent.route().range.split(cell_routing_key(
        scope.namespace,
        &CellId::new("managed-split-pivot")?,
    ))?;
    let operation = ProposalId::new([231; 16])?;
    let identity = |id, revision| FleetRequestIdentity {
        operation: operation.bytes(),
        ..request(id, revision)
    };
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
    let plan = directory.prepare_replacement(
        &view,
        operation,
        std::slice::from_ref(parent.route()),
        &children,
    )?;
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
        .context("admitted fleet operation must reopen without a directory decision")?;
    ensure!(recovered.journal.is_none());
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

    let (registry, held) = Box::pin(preparation::exercise(
        cluster, &reopened, directory, &recovered, &parent, &children, rooted,
    ))
    .await?;

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
    if matches!(decision, Decision::Authorize) {
        ensure!(matches!(
            reopened
                .authorize_managed_transition_seal(&cancel, directory, plan, TIMEOUT)
                .await,
            Err(FleetCatalogError::PayloadConflict)
        ));
        let authorized = reopened
            .authorize_managed_transition_seal(original, directory, plan, TIMEOUT)
            .await?;
        ensure!(authorized.phase() == TransitionPhase::SealAuthorized);
        ensure!(authorized.preparation().is_some());
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
        let seal = Box::pin(commit_source_seal(cluster, &recovered, scope)).await?;
        Box::pin(preparation::verify_sealed_cut(
            cluster, &reopened, directory, &recovered, &parent, held, &seal,
        ))
        .await?;
        return Ok(());
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
    registry.release(domain.as_ref(), &held).await?;
    let view = domain.run(directory.observe()).await??;
    ensure!(view.groups() == 1);
    Ok(())
}

async fn commit_source_seal(
    cluster: &ManagedCluster,
    transition: &ReopenedManagedCellGroupTransition,
    history: GroupHistory,
) -> Result<GroupSourceSeal> {
    let voter = leader(&cluster.hosts[..3], history).await?;
    let follower = cluster.hosts[(voter + 1) % 3].group(history)?;
    ensure!(matches!(
        follower.seal_managed_source(transition, TIMEOUT).await,
        Err(CellGroupControllerError::Runtime(RuntimeError::NotLeader))
    ));
    let source = cluster.hosts[voter].group(history)?;
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
