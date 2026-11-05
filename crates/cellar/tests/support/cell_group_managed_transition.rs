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
    Box::pin(run_plan(false, Arc::new(fault::Store::default()))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_split_plan() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-transition",
            rooted,
        )?));
        Box::pin(run_plan(rooted, store)).await?;
    }
    Ok(())
}

async fn run_plan(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
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
    let result = Box::pin(exercise(&cluster, rooted)).await;
    let spares = cluster.nodes.split_off(3);
    let joined = close_group_nodes(cluster.nodes, &cluster.hosts).await;
    let joined_spares = async {
        for (node, host) in spares.into_iter().zip(&cluster.hosts[3..]) {
            stopped(node, host).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    cluster.fixtures[0].domain.shutdown().await;
    result?;
    joined?;
    joined_spares
}

async fn exercise(cluster: &ManagedCluster, rooted: bool) -> Result<()> {
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
