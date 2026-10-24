//! Actual empty managed hosts, durable genesis, native SQLite and TLS quorum.
//! Active node metadata is fixture input, not capacity-activation qualification.
use super::*;

struct Running {
    root: tempfile::TempDir,
    node: cellar::sdk::Node,
    host: RaftCells,
    spec: FleetNodeSpec,
}

async fn fleet(f: &Fixture) -> Result<Vec<Running>> {
    let (root, node, host) = f.empty_host().await?;
    let mut nodes = vec![Running {
        root,
        node,
        host,
        spec: f.spec.clone(),
    }];
    for id in [5, 6] {
        let (root, node, host) = f.peer_host(id).await?;
        nodes.push(Running {
            root,
            node,
            host,
            spec: f.peer_spec(id)?,
        });
    }
    for (i, node) in nodes.iter().enumerate() {
        f.state(node.spec.node, FleetNodeState::Active, 110 + i as u8)
            .await?;
    }
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    for origin in &nodes {
        for target in &nodes {
            if origin.spec.node != target.spec.node {
                f.remote(&origin.host, target.spec.node, revision).await?;
            }
        }
    }
    Ok(nodes)
}

async fn birth(f: &Fixture) -> Result<(BirthProposal, FleetGroupAuthorization)> {
    let namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(f.declaration.namespace.clone())
    .await?;
    let proposal = BirthProposal {
        scope: GroupScope {
            cluster: f.declaration.namespace.cluster,
            namespace: f.declaration.namespace.namespace,
            cell: "Counter:initial".into(),
            group: GroupId::new([81; 16])?,
            incarnation: IncarnationId::new([82; 16])?,
        },
        voters: [4, 5, 6]
            .into_iter()
            .enumerate()
            .map(|(i, id)| {
                Ok(InitialMember {
                    member: MemberId::new(i as u64 + 1)?,
                    node: NodeId::new([id; 16])?,
                })
            })
            .collect::<Result<_>>()?,
        learners: vec![],
    };
    let root = f.catalog.read(TIMEOUT).await?;
    let reserved = f
        .catalog
        .reserve_birth(
            &root,
            &Fixture::request(130, root.revision()),
            &namespace,
            proposal.clone(),
            TIMEOUT,
        )
        .await?;
    f.domain
        .run(namespace.resolve_managed_birth(&reserved.authorization, TIMEOUT))
        .await??;
    let root = f.catalog.read(TIMEOUT).await?;
    let confirmed = f
        .catalog
        .confirm_birth(
            &root,
            &Fixture::request(131, root.revision()),
            &namespace,
            &reserved.authorization,
            TIMEOUT,
        )
        .await?;
    Ok((
        proposal.clone(),
        f.catalog
            .authorize_group(&confirmed.snapshot, &proposal.scope, TIMEOUT)
            .await?,
    ))
}

async fn registered(
    nodes: &[Running],
    auth: &FleetGroupAuthorization,
) -> Result<Vec<RaftCellHandle>> {
    let mut result = vec![];
    for node in nodes {
        result.push(
            node.host
                .register_managed_history(auth.clone(), TIMEOUT)
                .await?,
        );
    }
    Ok(result)
}

async fn activate(cells: &[RaftCellHandle], auth: &FleetGroupAuthorization) -> Result<()> {
    for cell in cells {
        let guard = cell
            .activate_managed_initial_member(auth.clone(), TIMEOUT)
            .await?;
        ensure!(
            guard.changed_at == LogIndex::new(1)
                && guard.consumed_sequence == 0
                && guard.permit.is_none()
        );
        let again = cell
            .activate_managed_initial_member(auth.clone(), TIMEOUT)
            .await?;
        ensure!(guard == again);
    }
    Ok(())
}

async fn primary(f: &Fixture, cells: &[RaftCellHandle]) -> Result<usize> {
    Ok(f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                for (i, cell) in cells.iter().enumerate() {
                    if cell.status().await.is_ok_and(|s| s.command_ready) {
                        return i;
                    }
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??)
}

async fn write_read(
    f: &Fixture,
    cells: &[RaftCellHandle],
    sequence: u64,
    value: i64,
    expected: i64,
) -> Result<()> {
    let p = primary(f, cells).await?;
    let receipt = cells[p]
        .command(
            f.definition.increment.clone(),
            value.to_be_bytes().to_vec(),
            public_node::archived_options(sequence)?,
        )
        .await?;
    ensure!(receipt.durability == Durability::S3);
    let read = cells[p]
        .query(
            f.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(i64::from_be_bytes(read.result.as_slice().try_into()?) == expected);
    ensure!(read.version.covers(&receipt.version)?);
    ensure!(matches!(
        cells[p]
            .add_learner(
                JoiningMember {
                    member: MemberId::new(4)?,
                    node: NodeId::new([9; 16])?,
                    disk: DiskLineage::new([19; 16])?
                },
                LearnerOptions {
                    expected_revision: cells[p].status().await?.membership_revision,
                    timeout: TIMEOUT
                }
            )
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    Ok(())
}

async fn stop(nodes: Vec<Running>) -> Result<Vec<(tempfile::TempDir, FleetNodeSpec)>> {
    let mut retained = vec![];
    for node in nodes {
        node.node.shutdown(ShutdownMode::Preserve).await?;
        retained.push((node.root, node.spec));
    }
    Ok(retained)
}

async fn restart(
    f: &Fixture,
    retained: Vec<(tempfile::TempDir, FleetNodeSpec)>,
    proposal: &BirthProposal,
) -> Result<Vec<Running>> {
    let mut nodes = vec![];
    for (root, spec) in retained {
        let current = f.catalog.read(TIMEOUT).await?;
        let auth = f
            .catalog
            .authorize_host(&current, spec.node, TIMEOUT)
            .await?;
        let node =
            public_node::node_named(root.path(), f.definition.application.clone(), &spec.label)
                .await?;
        let (mut config, services) = f.config_for(&node, auth, &spec).await?;
        config.infrastructure.log.mode = LogOpenMode::Resume;
        config.infrastructure.log.process =
            ProcessGeneration::new([spec.node.bytes()[0] + 100; 16])?;
        config.peers[0].process = config.infrastructure.log.process;
        ensure!(config.peers.len() == 1);
        config.cells = vec![RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace: f.declaration.namespace.clone(),
            class: "Counter".into(),
            birth: proposal.clone(),
        }];
        let host = node.start_raft_cells(config, services).await?;
        nodes.push(Running {
            root,
            node,
            host,
            spec,
        });
    }
    Ok(nodes)
}

fn handles(nodes: &[Running], proposal: &BirthProposal) -> Result<Vec<RaftCellHandle>> {
    nodes
        .iter()
        .map(|n| {
            Ok(n.host
                .cell(proposal.scope.namespace, &proposal.scope.cell)?)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_genesis_dynamic_replication_and_restart() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    for node in &nodes {
        ensure!(node.host.observe_node(TIMEOUT).await?.node.groups == 0);
    }
    activate(&cells, &auth).await?;
    write_read(&f, &cells, 1, 7, 7).await?;
    let retained = stop(nodes).await?;
    let nodes = restart(&f, retained, &proposal).await?;
    let cells = handles(&nodes, &proposal)?;
    for cell in &cells {
        ensure!(cell.fleet_membership_status().await?.unwrap().changed_at == LogIndex::new(1));
    }
    write_read(&f, &cells, 2, 5, 12).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_genesis_startup_bootstrap() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (proposal, _) = birth(&f).await?;
    let retained = stop(nodes).await?;
    let nodes = restart(&f, retained, &proposal).await?;
    let cells = handles(&nodes, &proposal)?;
    write_read(&f, &cells, 1, 9, 9).await?;
    for cell in &cells {
        let status = cell.fleet_membership_status().await?.unwrap();
        ensure!(status.changed_at == LogIndex::new(1) && status.consumed_sequence == 0);
    }
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_genesis_waiter_cancel_and_retry() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (_, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    f.store.claim_hang.store(true, Ordering::Release);
    f.store.hold_nth_claim.store(1, Ordering::Release);
    let pending = cells[0].activate_managed_initial_member(auth.clone(), TIMEOUT);
    f.domain
        .run(cellar::asyncrt::timeout(
            TIMEOUT,
            f.store.claim_waiting.notified(),
        ))
        .await??;
    let during = nodes[0].host.observe_node(TIMEOUT).await?;
    ensure!(
        during.node.groups == 1
            && during.unattached_engine_histories == 1
            && during.dormant_histories == 1
    );
    ensure!(matches!(
        cells[0].status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    drop(pending);
    ensure!(nodes[0].host.resource_usage().control.bytes >= 4 * 1024 * 1024);
    f.store.claim_hang.store(false, Ordering::Release);
    f.store.claim_resume.notify_waiters();
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !cells[0]
                .fleet_membership_status()
                .await
                .is_ok_and(|state| state.is_some())
            {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??;
    activate(&cells, &auth).await?;
    write_read(&f, &cells, 1, 4, 4).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_genesis_shutdown_during_claim() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    f.store.claim_hang.store(true, Ordering::Release);
    f.store.hold_nth_claim.store(1, Ordering::Release);
    let pending = cells[0].activate_managed_initial_member(auth.clone(), TIMEOUT);
    f.domain
        .run(cellar::asyncrt::timeout(
            TIMEOUT,
            f.store.claim_waiting.notified(),
        ))
        .await??;
    let report = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, nodes[0].host.close()))
        .await???;
    ensure!(report.unfinished.is_empty());
    ensure!(nodes[0].host.resource_usage().control.bytes == 4096);
    ensure!(matches!(pending.await, Err(CommandFailure::Unavailable)));
    ensure!(nodes[0].host.resource_usage().control.bytes == 0);
    f.store.claim_hang.store(false, Ordering::Release);
    f.store.claim_resume.notify_waiters();
    let retained = stop(nodes).await?;
    let nodes = restart(&f, retained, &proposal).await?;
    write_read(&f, &handles(&nodes, &proposal)?, 1, 6, 6).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_genesis_stale_history_refused() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    let binding = FleetGroupBinding::new(IncarnationId::new([80; 16])?, &proposal.scope)?;
    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .commit(
            &root,
            &FleetMutation {
                id: [132; 16],
                operation: [100; 16],
                principal: "initial-test".into(),
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Histories,
                    key: binding.key()?,
                    value: None,
                }],
            },
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        cells[0]
            .activate_managed_initial_member(auth, TIMEOUT)
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(nodes[0].host.observe_node(TIMEOUT).await?.node.groups == 0);
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_genesis_foreign_namespace_owner_refused() -> Result<()> {
    let mut f = Fixture::with_managed_births(true).await?;
    let foreign = FleetCatalog::new(
        &f.domain,
        Bucket::from_clients(
            f.store.clone(),
            f.store.clone(),
            Arc::new(NoListing),
            StorageBackend::S3,
            "foreign".into(),
            "tenant/".into(),
        )?,
        f.declaration.namespace.cluster,
        IncarnationId::new([80; 16])?,
        "foreign/",
    )?;
    let mut config = f.declaration.namespace.clone();
    config.managed_fleet = None;
    let declaration = RaftNamespaceRegistration {
        namespace: foreign.bind_namespace(config)?,
        class: "Counter".into(),
    };
    f.spec.capability_digest =
        RaftManagedHostOptions::capability_digest(std::slice::from_ref(&declaration))?;
    let authorization = f.enroll().await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (mut config, services) = f.config(&node, authorization).await?;
    config.host.managed.as_mut().unwrap().namespaces = vec![declaration];
    let error = node
        .start_raft_cells(config, services)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("foreign namespace owner accepted"))?;
    ensure!(format!("{error:#}").contains("managed host declarations disagree"));
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .authorize_host(&root, f.spec.node, TIMEOUT)
        .await?
        .storage_instance()
        .is_none());
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}
