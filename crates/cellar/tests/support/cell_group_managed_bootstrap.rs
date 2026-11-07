// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cold managed bootstrap must obtain inventory before reserving its first group.
use super::*;
use crate::managed_fixture::{
    admit_capacity, close_group_nodes, counter_class, inspect_current, leader, peer_fixture,
    seed_cells, ArchiveSetup, FirstGroupSetup, ManagedCluster, SeededCells,
};
use cellar::application::consensus::verification::cell_groups::{
    CellGroupArchivePool, RangeDirectory,
};

#[path = "cell_group_managed_first_group_operator.rs"]
mod first_group_operator;

#[path = "cell_group_managed_request_cases.rs"]
mod requests;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_cold_inventory_bootstrap() -> Result<()> {
    let f = Fixture::new(false).await?;
    let namespace = f
        .domain
        .run(
            Catalog::new(
                f.store.clone(),
                CatalogOptions {
                    prefix: "managed/".into(),
                    ..Default::default()
                },
            )?
            .existing_namespace(f.declaration.namespace.clone()),
        )
        .await??;
    let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let initialization = directory.prepare_standby_initialization(ProposalId::new([100; 16])?)?;
    let publication = f
        .domain
        .run(directory.publication(&initialization))
        .await??
        .context("SDK provisioning must publish standby before declaring the namespace")?;
    ensure!(publication.tree_digest().is_none());
    let root = tempfile::tempdir()?;
    let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
    let (config, services) = f.config(&node, LogOpenMode::Create, 24).await?;
    let host = node.start_raft_cells(config, services).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let inventory = f
        .catalog
        .check_inventory(&cut, std::slice::from_ref(&directory), TIMEOUT)
        .await;
    stopped(node, &host).await?;
    f.domain.shutdown().await;
    let inventory = inventory.context("cold namespace inventory before capacity/birth")?;
    ensure!(inventory.namespaces() == 1 && inventory.histories() == 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_namespace_provisioning_replays_lost_confirmation() -> Result<()> {
    let f = Fixture::new(false).await?;
    let result = async {
        let class = counter_class()?;
        let mut config = f.declaration.namespace.clone();
        config.namespace = NamespaceId::new([3; 16])?;
        config.schema_digest = class.schema_digest();
        let adapter = Catalog::new(
            f.store.clone(),
            CatalogOptions {
                prefix: "managed/".into(),
                ..Default::default()
            },
        )?;
        let before = f.catalog.read(TIMEOUT).await?.revision();
        let identity = request(94, before);
        let provision = |namespace| {
            f.catalog.provision_managed_sql_namespace(
                &identity,
                &adapter,
                namespace,
                &class,
                DirectoryLimits::default(),
                TIMEOUT,
            )
        };
        f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        ensure!(matches!(
            provision(config.namespace).await,
            Err(FleetCatalogError::Storage(_))
        ));
        ensure!(!f.store.fail_fleet_confirmation.load(Ordering::Acquire));
        let after = f.catalog.read(TIMEOUT).await?.revision();
        ensure!(after == before + 1);
        let mut changed = config.clone();
        changed.namespace = NamespaceId::new([7; 16])?;
        ensure!(matches!(
            provision(changed.namespace).await,
            Err(FleetCatalogError::PayloadConflict)
        ));
        ensure!(f
            .domain
            .run(adapter.existing_namespace(changed))
            .await?
            .is_err());
        let replay = provision(config.namespace).await?;
        ensure!(replay.class == class.name() && replay.namespace == config);
        ensure!(
            replay.namespace.mode == NamespaceMode::Raft
                && replay.namespace.minimum_durability == Durability::Quorum
                && replay.namespace.voters == VoterPolicy::ThreeVoters
                && replay.namespace.image_storage == RaftImageStorage::Materialized
        );
        ensure!(f.catalog.read(TIMEOUT).await?.revision() == after);
        let opened = f
            .domain
            .run(adapter.existing_namespace(replay.namespace))
            .await??;
        let directory = RangeDirectory::new(opened, DirectoryLimits::default())?;
        let view = f.domain.run(directory.observe()).await??;
        ensure!(view.revision() == 1 && view.groups() == 0);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.domain.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_namespace_provisioning_reuses_standby_after_root_conflict() -> Result<()> {
    let f = Fixture::new(false).await?;
    let result = async {
        let class = counter_class()?;
        let mut config = f.declaration.namespace.clone();
        config.namespace = NamespaceId::new([3; 16])?;
        config.schema_digest = class.schema_digest();
        let adapter = Catalog::new(
            f.store.clone(),
            CatalogOptions {
                prefix: "managed/".into(),
                ..Default::default()
            },
        )?;
        let provisioner = Fixture::open_catalog(&f.domain, f.store.clone(), config.cluster)?;
        let revision = f.catalog.read(TIMEOUT).await?.revision();
        let original = request(94, revision);
        let gate = &f.store.range_genesis_after;
        gate.armed.store(true, Ordering::Release);
        let (submitted, concurrent) = tokio::time::timeout(TIMEOUT, async {
            tokio::join!(
                provisioner.provision_managed_namespace(
                    &original,
                    &adapter,
                    config.clone(),
                    &class,
                    DirectoryLimits::default(),
                    TIMEOUT,
                ),
                async {
                    gate.entered.notified().await;
                    let result = peer_fixture(&f, 8).await;
                    gate.resume.notify_one();
                    result
                }
            )
        })
        .await?;
        concurrent?;
        ensure!(matches!(submitted, Err(FleetCatalogError::Conflict)));
        let advanced = f.catalog.read(TIMEOUT).await?.revision();
        ensure!(advanced == revision + 1);
        let reopened = f
            .domain
            .run(adapter.existing_namespace(config.clone()))
            .await??;
        let directory = RangeDirectory::new(reopened, DirectoryLimits::default())?;
        let standby = f.domain.run(directory.observe()).await??;
        ensure!(standby.revision() == 1 && standby.groups() == 0);
        let retry = request(95, advanced);
        ensure!(retry.operation == original.operation && retry.id != original.id);
        let declaration = provisioner
            .provision_managed_namespace(
                &retry,
                &adapter,
                config.clone(),
                &class,
                DirectoryLimits::default(),
                TIMEOUT,
            )
            .await?;
        ensure!(declaration.namespace == config && declaration.class == class.name());
        ensure!(f.catalog.read(TIMEOUT).await?.revision() == advanced + 1);
        ensure!(f.domain.run(directory.observe()).await??.digest() == standby.digest());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.domain.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_cold_capacity_group_and_retained_restart() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(cold_group(rooted)).await?;
    }
    Ok(())
}

async fn cold_group(rooted: bool) -> Result<()> {
    let first = Fixture::with_class(rooted, counter_class()?).await?;
    let ManagedCluster {
        fixtures,
        roots,
        nodes,
        hosts,
        directory,
        history,
        ..
    } = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::ExerciseReplay,
    ))
    .await?;
    let f = &fixtures[0];
    let proposal = || -> Result<CellGroupBirthSpec> {
        Ok(CellGroupBirthSpec {
            history,
            range: CellRange::all(),
            epoch: RouteEpoch::new(1)?,
            creation: ProposalId::new([150; 16])?,
            voters: fixtures
                .iter()
                .take(3)
                .enumerate()
                .map(|(i, f)| {
                    Ok(InitialMember {
                        member: MemberId::new(i as u64 + 1)?,
                        node: f.spec.node,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        })
    };
    let SeededCells {
        cells,
        receipts,
        retry,
    } = seed_cells(&hosts, history).await?;
    close_group_nodes(nodes, &hosts).await?;
    drop(hosts);
    let mut nodes = Vec::new();
    let mut hosts = Vec::new();
    for (i, fixture) in fixtures.iter().enumerate() {
        let node = start_node(
            roots[i].path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let (config, services) = fixture
            .config(&node, LogOpenMode::Resume, 34 + i as u8)
            .await?;
        hosts.push(node.start_raft_cells(config, services).await?);
        nodes.push(node);
    }
    let index = leader(&hosts, history).await?;
    for (i, (cell, original)) in cells.iter().zip(&receipts).enumerate() {
        let handle = hosts[index].cell(cell.clone())?;
        let delta = [7i64, 11][i];
        let duplicate = handle
            .command(
                &handle.contract("Add")?,
                delta.to_be_bytes().to_vec(),
                CommandOptions::new(original.id, retry),
            )
            .await?;
        ensure!(duplicate.id == original.id && duplicate.result == original.result);
        let observed = handle
            .query(
                ProposalId::new([190 + i as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(original.version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            observed.result == original.result,
            "retained duplicate ran twice"
        );
        let next = CommandId::new(original.id.client, 2)?;
        let updated = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(next, retry),
            )
            .await?;
        ensure!(updated.result == (delta + 1).to_be_bytes());
    }
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .check_inventory(&cut, std::slice::from_ref(&directory), TIMEOUT)
            .await?
            .histories()
            == 1
    );
    for fixture in &fixtures {
        ensure!(
            matches!(
                f.catalog
                    .authorize_capacity(&cut, fixture.spec.node, TIMEOUT)
                    .await,
                Err(FleetCatalogError::Conflict)
            ),
            "old boot capacity survived restart"
        );
    }
    for host in &hosts {
        ensure!(
            host.observe_node(TIMEOUT).await?.node.groups == 1
                && host.resource_usage().retained_manifest_cells == Some(2)
        );
    }
    let next_birth = || -> Result<CellGroupBirthSpec> {
        Ok(CellGroupBirthSpec {
            history: GroupHistory {
                group: GroupId::new([4; 16])?,
                incarnation: IncarnationId::new([8; 16])?,
                ..history
            },
            creation: ProposalId::new([151; 16])?,
            ..proposal()?
        })
    };
    let attempted = f
        .catalog
        .reserve_birth(
            &cut,
            &request(79, cut.revision()),
            &directory,
            next_birth()?,
            "Counter",
            TIMEOUT,
        )
        .await;
    let blocked = matches!(attempted, Err(FleetCatalogError::Conflict));
    let after = f.catalog.read(TIMEOUT).await?.revision();
    if !blocked || after != cut.revision() {
        close_group_nodes(nodes, &hosts).await?;
        f.domain.shutdown().await;
        anyhow::bail!("new boot admitted a birth using stale capacity: blocked={blocked}, before={}, after={after}", cut.revision());
    }
    for (i, fixture) in fixtures.iter().enumerate() {
        admit_capacity(fixture, &hosts[i], &directory, 70 + 2 * i as u8).await?;
    }
    let cut = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .reserve_birth(
            &cut,
            &request(80, cut.revision()),
            &directory,
            next_birth()?,
            "Counter",
            TIMEOUT,
        )
        .await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .check_inventory(&cut, std::slice::from_ref(&directory), TIMEOUT)
            .await?
            .histories()
            == 2
    );
    leader(&hosts, history).await?;
    close_group_nodes(nodes, &hosts).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[path = "cell_group_managed_lifecycle.rs"]
mod lifecycle;

#[path = "cell_group_managed_archives.rs"]
mod archives;
