//! Actual empty managed hosts, durable genesis, native SQLite and TLS quorum.
//! Active node metadata is fixture input, not capacity-activation qualification.
use super::*;
use anyhow::Context as _;
use cellar::application::consensus::verification::{
    FleetBirthAuthorization, FleetHistoryState, FleetImportPhase, NamespaceCatalog,
};

#[path = "raft_retirement_native.rs"]
mod retirement;

#[path = "raft_fleet_trust_native.rs"]
mod trust;

#[path = "raft_fleet_revocation_native.rs"]
mod revocation;

#[path = "raft_fleet_placement_native.rs"]
mod placement;

#[path = "raft_archive_coverage.rs"]
mod archive_coverage;
#[path = "raft_evacuation_native.rs"]
mod evacuation;
#[path = "raft_peer_fleet_observation.rs"]
mod peer_observation;
#[path = "raft_published_cut.rs"]
mod published_cut;

#[path = "raft_resource_native.rs"]
mod resources;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_genesis_imported_namespace() -> Result<()> {
    let f = Fixture::with_domains(false, true).await?;
    Box::pin(imported_namespace(f)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_extent_genesis_imported_namespace_preserves_guards_and_data() -> Result<()> {
    let f = Box::pin(Fixture::with_storage(
        false,
        true,
        FleetReplicaSeparation::PhysicalHosts,
        RaftImageStorage::Extents,
    ))
    .await?;
    Box::pin(imported_namespace(f)).await
}

async fn imported_namespace(f: Fixture) -> Result<()> {
    let nodes = fleet(&f).await?;
    let (namespace, _) = initial_birth(&f).await?;
    ensure!(namespace.config().managed_fleet.is_none());
    let cut = f
        .domain
        .run(namespace.freeze_births(ProposalId::new([100; 16])?, TIMEOUT))
        .await??;
    ensure!(cut.is_empty());
    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .begin_namespace_import(
            &root,
            &Fixture::request(190, root.revision()),
            &namespace,
            &cut,
            TIMEOUT,
        )
        .await?;
    for id in 191..=194 {
        let root = f.catalog.read(TIMEOUT).await?;
        f.catalog
            .advance_namespace_import(
                &root,
                &Fixture::request(id, root.revision()),
                &namespace,
                None,
                TIMEOUT,
            )
            .await?;
    }
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .namespace_import(&root, &namespace, TIMEOUT)
            .await?
            .phase()
            == FleetImportPhase::Complete
    );
    let (proposal, auth) = birth(&f).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    write_read(&f, &cells, 1, 4, 4).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .check_inventory(&root, std::slice::from_ref(&namespace), TIMEOUT)
            .await?
            .histories()
            == 1
    );
    let retained = stop(nodes).await?;
    let nodes = restart(&f, retained, &proposal).await?;
    let cells = handles(&nodes, &proposal)?;
    write_read(&f, &cells, 2, 5, 9).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .check_inventory(&root, std::slice::from_ref(&namespace), TIMEOUT)
            .await?
            .histories()
            == 1
    );
    for cell in &cells {
        let guard = cell
            .fleet_membership_status()
            .await?
            .context("imported namespace guard")?;
        ensure!(guard.binding == f.catalog.group_binding(&proposal.scope)?);
    }
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

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

async fn initial_birth(f: &Fixture) -> Result<(NamespaceCatalog, BirthProposal)> {
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
    Ok((namespace, proposal))
}

async fn reserved_birth(
    f: &Fixture,
) -> Result<(NamespaceCatalog, BirthProposal, FleetBirthAuthorization)> {
    let (namespace, proposal) = initial_birth(f).await?;
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
    Ok((namespace, proposal, reserved.authorization))
}

async fn confirm(
    f: &Fixture,
    namespace: &NamespaceCatalog,
    proposal: &BirthProposal,
    reserved: &FleetBirthAuthorization,
) -> Result<FleetGroupAuthorization> {
    let root = f.catalog.read(TIMEOUT).await?;
    let confirmed = f
        .catalog
        .confirm_birth(
            &root,
            &Fixture::request(131, root.revision()),
            namespace,
            reserved,
            TIMEOUT,
        )
        .await?;
    Ok(f.catalog
        .authorize_group(&confirmed.snapshot, &proposal.scope, TIMEOUT)
        .await?)
}

async fn birth(f: &Fixture) -> Result<(BirthProposal, FleetGroupAuthorization)> {
    let (namespace, proposal, reserved) = reserved_birth(f).await?;
    f.domain
        .run(namespace.resolve_managed_birth(&reserved, TIMEOUT))
        .await??;
    let auth = confirm(f, &namespace, &proposal, &reserved).await?;
    Ok((proposal, auth))
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
    // A lost initialization reply can be retried after startup recovery,
    // which has no dormant registration to consult.
    activate(&cells, &auth).await?;
    for cell in &cells {
        ensure!(cell.fleet_membership_status().await?.unwrap().changed_at == LogIndex::new(1));
    }
    write_read(&f, &cells, 2, 5, 12).await?;
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_birth_drain_race() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (namespace, proposal, reserved) = reserved_birth(&f).await?;
    f.store.birth_hang.store(true, Ordering::Release);
    f.store.hold_next_birth.store(true, Ordering::Release);
    let writer_namespace = namespace.clone();
    let writer_auth = reserved.clone();
    let writer = f.domain.spawn(async move {
        writer_namespace
            .resolve_managed_birth(&writer_auth, TIMEOUT)
            .await
    })?;
    f.birth_waiting().await?;
    // Trusted fixture transitions exclude future placements. They are not
    // evacuation/stop receipts; the admitted reference must still settle.
    let excluded = [
        FleetNodeState::Draining,
        FleetNodeState::RestartPending,
        FleetNodeState::ResizePending,
    ];
    for (i, node) in nodes.iter().enumerate() {
        f.state(node.spec.node, excluded[i], 140 + i as u8).await?;
    }
    let root = f.catalog.read(TIMEOUT).await?;
    let history = f.catalog.history(&root, &proposal.scope, TIMEOUT).await?;
    ensure!(*history.state() == FleetHistoryState::Reserved);
    ensure!(history.initial_placement()?.voters == proposal.voters);
    ensure!(matches!(
        f.catalog
            .authorize_group(&root, &proposal.scope, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    for node in &nodes {
        ensure!(node.host.observe_node(TIMEOUT).await?.node.groups == 0);
    }
    f.store.birth_hang.store(false, Ordering::Release);
    f.store.birth_resume.notify_waiters();
    writer.await??;
    let auth = confirm(&f, &namespace, &proposal, &reserved).await?;
    let (_spare_root, spare_node, spare) = f.peer_host(7).await?;
    f.state(NodeId::new([7; 16])?, FleetNodeState::Draining, 143)
        .await?;
    ensure!(matches!(
        spare.register_managed_history(auth.clone(), TIMEOUT).await,
        Err(CommandFailure::Unavailable)
    ));
    let spare_view = spare.observe_node(TIMEOUT).await?;
    ensure!(spare_view.node.groups == 0 && spare_view.dormant_histories == 0);
    spare_node.shutdown(ShutdownMode::Preserve).await?;
    let cells = registered(&nodes, &auth).await?;
    activate(&cells, &auth).await?;
    write_read(&f, &cells, 1, 8, 8).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let history = f.catalog.history(&root, &proposal.scope, TIMEOUT).await?;
    ensure!(*history.state() == FleetHistoryState::Live);
    ensure!(history.initial_placement()?.voters == proposal.voters);
    for (i, node) in nodes.iter().enumerate() {
        ensure!(
            f.catalog
                .authorize_host(&root, node.spec.node, TIMEOUT)
                .await?
                .state()
                == excluded[i]
        );
    }
    stop(nodes).await?;
    f.domain.shutdown().await;

    // The opposite root-CAS ordering cannot publish a birth or a local voter,
    // even though the caller prepared its request while all nodes were active.
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (namespace, proposal) = initial_birth(&f).await?;
    let old = f.catalog.read(TIMEOUT).await?;
    f.store.fleet_cas_hang.store(true, Ordering::Release);
    f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let catalog = f.catalog.clone();
    let writer_namespace = namespace.clone();
    let writer_proposal = proposal.clone();
    let writer = f.domain.spawn(async move {
        catalog
            .reserve_birth(
                &old,
                &Fixture::request(130, old.revision()),
                &writer_namespace,
                writer_proposal,
                TIMEOUT,
            )
            .await
    })?;
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !f.store.fleet_cas_waiting.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    // A distinct adapter can win CAS while the reservation's adapter is held.
    let bucket = Bucket::from_clients(
        f.store.clone(),
        f.store.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "enrollment".into(),
        "tenant/".into(),
    )?;
    let other = FleetCatalog::new(
        &f.domain,
        bucket,
        ClusterId::new([1; 16])?,
        IncarnationId::new([80; 16])?,
        "hosts/",
    )?;
    let concurrent = Fixture {
        catalog: other,
        ..f
    };
    concurrent
        .state(nodes[0].spec.node, FleetNodeState::Draining, 140)
        .await?;
    concurrent
        .store
        .fleet_cas_hang
        .store(false, Ordering::Release);
    concurrent.store.fleet_cas_resume.notify_waiters();
    ensure!(matches!(writer.await?, Err(FleetCatalogError::Conflict)));
    let root = concurrent.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        concurrent
            .catalog
            .reserve_birth(
                &root,
                &Fixture::request(132, root.revision()),
                &namespace,
                proposal.clone(),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(concurrent
        .catalog
        .page(&root, FleetTable::Histories, None, 128, TIMEOUT)
        .await?
        .entries
        .is_empty());
    ensure!(namespace
        .birth_disposition(&proposal.scope.cell)
        .await?
        .is_none());
    for node in &nodes {
        let view = node.host.observe_node(TIMEOUT).await?;
        ensure!(view.node.groups == 0 && view.dormant_histories == 0);
    }
    stop(nodes).await?;
    concurrent.domain.shutdown().await;
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
    let binding = f.catalog.group_binding(&proposal.scope)?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_inventory_reserved_births() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (namespace, proposal, reserved) = reserved_birth(&f).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(namespace
        .birth_disposition(&proposal.scope.cell)
        .await?
        .is_none());
    let inventory = f
        .catalog
        .check_inventory(&root, std::slice::from_ref(&namespace), TIMEOUT)
        .await?;
    ensure!(inventory.namespaces() == 1 && inventory.histories() == 1);
    f.domain
        .run(namespace.resolve_managed_birth(&reserved, TIMEOUT))
        .await??;
    // The lost/unfinished root confirmation still retains the live decision.
    ensure!(
        *f.catalog
            .history(&root, &proposal.scope, TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Reserved
    );
    ensure!(
        f.catalog
            .check_inventory(&root, std::slice::from_ref(&namespace), TIMEOUT)
            .await?
            .histories()
            == 1
    );
    let mut pending = proposal.clone();
    pending.scope.cell = "Counter:cancelled-pending".into();
    pending.scope.group = GroupId::new([83; 16])?;
    pending.scope.incarnation = IncarnationId::new([84; 16])?;
    let reserved = f
        .catalog
        .reserve_birth(
            &root,
            &Fixture::request(132, root.revision()),
            &namespace,
            pending.clone(),
            TIMEOUT,
        )
        .await?;
    let root = f.catalog.read(TIMEOUT).await?;
    f.domain
        .run(namespace.cancel_managed_birth(
            &reserved.authorization,
            ProposalId::new([210; 16])?,
            TIMEOUT,
        ))
        .await??;
    ensure!(
        f.catalog
            .check_inventory(&root, std::slice::from_ref(&namespace), TIMEOUT)
            .await?
            .histories()
            == 2
    );
    f.catalog
        .confirm_birth(
            &root,
            &Fixture::request(133, root.revision()),
            &namespace,
            &reserved.authorization,
            TIMEOUT,
        )
        .await?;
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.catalog
            .history(&root, &pending.scope, TIMEOUT)
            .await?
            .state(),
        FleetHistoryState::Cancelled { .. }
    ));
    ensure!(
        f.catalog
            .check_inventory(&root, &[namespace], TIMEOUT)
            .await?
            .histories()
            == 2
    );
    stop(nodes).await?;
    f.domain.shutdown().await;
    Ok(())
}
