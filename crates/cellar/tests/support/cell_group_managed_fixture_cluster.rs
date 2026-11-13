// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed RF3 bootstrap, capacity and selected group fixture.

use super::*;

pub fn counter_class() -> Result<RaftClass> {
    counter_class_with_callback(|| {})
}

pub fn counter_class_with_callback(
    on_command: impl Fn() + Send + Sync + 'static,
) -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [81; 32])?
        .initializer([82; 32], |_, sql| {
            sql.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)",
            )?;
            Ok(())
        })?
        .command("Add", [83; 32], move |context, input| {
            on_command();
            let delta = i64::from_be_bytes(input.try_into()?);
            let current: i64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |r| r.get(0))?;
            let value = current.checked_add(delta).context("counter overflow")?;
            context
                .sql()
                .execute("UPDATE counter SET value=?1", [value])?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [84; 32], |context, _| {
            let value: i64 = context.query_row("SELECT value FROM counter", [], |r| r.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?)
}

pub async fn peer_fixture(f: &Fixture, id: u8) -> Result<Fixture> {
    let mut spec = f.spec.clone();
    spec.node = NodeId::new([id; 16])?;
    spec.disk = DiskLineage::new([id + 10; 16])?;
    spec.physical_host = IncarnationId::new([id + 90; 16])?;
    spec.label = format!("shared-managed-{id}");
    let cut = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .register_node(&cut, &request(id, cut.revision()), &spec, TIMEOUT)
        .await?;
    Ok(Fixture {
        domain: f.domain.clone(),
        store: Arc::new(fault::Store::around(f.store.clone())),
        host_store: None,
        catalog: f.catalog.clone(),
        declaration: f.declaration.clone(),
        additional_namespaces: f.additional_namespaces.clone(),
        application: f.application.clone(),
        configuration: f.configuration.clone(),
        cohosted: f.cohosted.clone(),
        external_http: f.external_http.clone(),
        // A newly registered peer owns independent physical I/O.
        filesystem: Arc::new(cellar_ltx::DirectFileSystem),
        effects: f.effects,
        cell_limits: f.cell_limits,
        sql_workers: f.sql_workers,
        ordinary_admission: f.ordinary_admission,
        max_pending_transactions: f.max_pending_transactions,
        max_client_identities: f.max_client_identities,
        max_input_bytes: f.max_input_bytes,
        stale_source_timeout_ms: f.stale_source_timeout_ms,
        small_segments: f.small_segments,
        settle_periodic_purges: false,
        payloads: f.payloads,
        image_bytes: f.image_bytes,
        logical_byte_capacity: f.logical_byte_capacity,
        placement_domains: f.placement_domains.clone(),
        spec,
    })
}

pub async fn inspect_current(host: &RaftCells, node: NodeId, revision: u64) -> Result<()> {
    // The directory's actual timer must install the new rooted trust/process
    // view. Retrying this read does not mint capacity or alter the fleet cut.
    let mut last = None;
    let observed = tokio::time::timeout(TIMEOUT, async {
        loop {
            match host.inspect_peer_node(node, revision, TIMEOUT).await {
                Ok(view) => {
                    ensure!(view
                        .view
                        .fleet_trust_revision
                        .is_some_and(|r| r >= revision));
                    return Ok::<_, anyhow::Error>(());
                }
                Err(error) => {
                    last = Some(error);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
    .await;
    observed.with_context(|| {
        format!(
        "trust refresh origin={:?}, target={node:?}, floor={revision}, closed={}, last={last:?}",
        host.identity().node,
        host.is_closed(),
    )
    })??;
    Ok(())
}

pub async fn admit_capacity(
    f: &Fixture,
    host: &RaftCells,
    directory: &RangeDirectory,
    id: u8,
) -> Result<()> {
    let local = host.observe_node(TIMEOUT).await?;
    let evidence = FleetCapacityAttestation {
        boot: local.fleet_boot.context("capacity boot")?,
        profile_revision: 1,
        profile_digest: fleet_capacity_profile_digest(&local.node.profile)?,
        cpu_millis: 1000,
        memory_bytes: 3 * 1024 * 1024 * 1024,
        durable_bytes: local.node.profile.byte_capacity.max(1024 * 1024 * 1024),
        engine_slots: local.node.profile.group_capacity,
        memory_swap_disabled: true,
        used_memory_bytes: 64 * 1024 * 1024,
        used_durable_bytes: 1024 * 1024,
        evidence_digest: [71; 32],
        evidence_reference: "fixture-only/deployment-attestation".into(),
    };
    let mut directories = vec![directory.clone()];
    for declaration in &f.additional_namespaces {
        let adapter = Catalog::new(
            f.store.clone(),
            CatalogOptions {
                prefix: "managed/".into(),
                ..Default::default()
            },
        )?;
        directories.push(RangeDirectory::new(
            adapter
                .existing_namespace(declaration.namespace.clone())
                .await?,
            DirectoryLimits::default(),
        )?);
    }
    for (offset, action, state) in [
        (0, FleetCapacityAction::Qualify, FleetNodeState::Ready),
        (1, FleetCapacityAction::Activate, FleetNodeState::Active),
    ] {
        let cut = f.catalog.read(TIMEOUT).await?;
        inspect_current(host, f.spec.node, cut.revision()).await?;
        // Capacity actions have their own request-ID domain. A larger fleet's
        // sequence must not collide with first-group bootstrap requests 60/61.
        let mut identity = request(id + offset, cut.revision());
        identity.id = (1_000_000 + u128::from(id + offset)).to_be_bytes();
        let accepted = f
            .catalog
            .admit_node_capacity(
                &cut,
                &FleetCapacityRequest {
                    identity,
                    attestation: evidence.clone(),
                    action,
                },
                host,
                &directories,
                TIMEOUT,
            )
            .await?;
        ensure!(accepted.state() == state);
        let current = f.catalog.read(TIMEOUT).await?;
        let capacity = f
            .catalog
            .authorize_capacity(&current, f.spec.node, TIMEOUT)
            .await?;
        ensure!(capacity.receive_replicas() == (state == FleetNodeState::Active));
    }
    Ok(())
}

pub async fn leader(hosts: &[RaftCells], history: GroupHistory) -> Result<usize> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for (i, host) in hosts.iter().enumerate() {
                let status = host.group(history)?.status().await?;
                if status.ready && status.leader == Some(i as u64 + 1) {
                    return Ok::<_, anyhow::Error>(i);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

pub async fn close_group_nodes(nodes: Vec<Node>, hosts: &[RaftCells]) -> Result<()> {
    close_group_nodes_with_groups(nodes, hosts, 1).await
}

pub async fn close_group_nodes_with_groups(
    nodes: Vec<Node>,
    hosts: &[RaftCells],
    expected: usize,
) -> Result<()> {
    for (node, host) in nodes.into_iter().zip(hosts) {
        let outcome = node.shutdown(ShutdownMode::Preserve).await?;
        ensure!(
            matches!(outcome, NodeOutcome::Stopped(_)),
            "managed node stop: {outcome:?}"
        );
        let report = host.close().await?;
        ensure!(
            report.groups_total == expected
                && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
        );
    }
    Ok(())
}

pub fn checkpoint_limits() -> CellGroupCheckpointLimits {
    CellGroupCheckpointLimits {
        max_sources: 2,
        max_retained_bytes: 32 * 1024 * 1024,
        max_cells: 32,
        max_transfer_bytes: 32 * 1024 * 1024,
        max_chunk_bytes: 4093,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ArchiveSetup {
    Disabled,
    Initialized,
    Missing,
}

pub enum FirstGroupSetup {
    Normal,
    ExerciseReplay,
    Operator,
}

pub struct ManagedCluster {
    pub fixtures: Vec<Fixture>,
    pub roots: Vec<tempfile::TempDir>,
    pub nodes: Vec<Node>,
    pub hosts: Vec<RaftCells>,
    pub peers: Vec<RaftPeerNode>,
    pub directory: RangeDirectory,
    pub history: GroupHistory,
}

impl ManagedCluster {
    pub async fn start(rooted: bool, spares: &[u8]) -> Result<Self> {
        Box::pin(Self::start_with_archives(
            rooted,
            spares,
            ArchiveSetup::Initialized,
        ))
        .await
    }

    pub async fn start_with_archives(
        rooted: bool,
        spares: &[u8],
        archives: ArchiveSetup,
    ) -> Result<Self> {
        Box::pin(Self::start_with_store(
            rooted,
            spares,
            archives,
            Arc::new(fault::Store::default()),
        ))
        .await
    }

    pub async fn start_with_store(
        rooted: bool,
        spares: &[u8],
        archives: ArchiveSetup,
        store: Arc<fault::Store>,
    ) -> Result<Self> {
        Box::pin(Self::start_with_catalog_clients(
            rooted, spares, archives, store, false,
        ))
        .await
    }

    pub async fn start_with_catalog_clients(
        rooted: bool,
        spares: &[u8],
        archives: ArchiveSetup,
        store: Arc<fault::Store>,
        independent: bool,
    ) -> Result<Self> {
        let first = Fixture::with_store(rooted, counter_class()?, store).await?;
        Box::pin(Self::start_with_applications(
            first,
            spares,
            archives,
            independent,
            None,
            FirstGroupSetup::Normal,
        ))
        .await
    }

    pub async fn start_with_applications(
        first: Fixture,
        spares: &[u8],
        archives: ArchiveSetup,
        independent: bool,
        applications: Option<Vec<Arc<dyn Application>>>,
        first_group: FirstGroupSetup,
    ) -> Result<Self> {
        Box::pin(Self::start_configured(
            first,
            spares,
            archives,
            independent,
            applications,
            first_group,
            None,
        ))
        .await
    }

    pub async fn start_with_partitions(
        rooted: bool,
        spares: &[u8],
        store: Arc<fault::Store>,
        partitions: cellar::sdk::fleet_operations::RaftFleetAutomaticPartitionOptions,
    ) -> Result<Self> {
        let first = Fixture::with_store(rooted, counter_class()?, store).await?;
        Box::pin(Self::start_configured(
            first,
            spares,
            ArchiveSetup::Initialized,
            true,
            None,
            FirstGroupSetup::Normal,
            Some(partitions),
        ))
        .await
    }

    async fn start_configured(
        first: Fixture,
        spares: &[u8],
        archives: ArchiveSetup,
        independent: bool,
        applications: Option<Vec<Arc<dyn Application>>>,
        first_group: FirstGroupSetup,
        partitions: Option<cellar::sdk::fleet_operations::RaftFleetAutomaticPartitionOptions>,
    ) -> Result<Self> {
        let second = peer_fixture(&first, 5).await?;
        let third = peer_fixture(&first, 6).await?;
        let mut fixtures = vec![first, second, third];
        for id in spares {
            fixtures.push(peer_fixture(&fixtures[0], *id).await?);
        }
        if let Some(applications) = applications {
            ensure!(applications.len() == fixtures.len());
            for (fixture, application) in fixtures.iter_mut().zip(applications) {
                fixture.application = application;
            }
        }
        if independent {
            for fixture in fixtures.iter_mut().skip(1) {
                let bucket = Bucket::from_clients(
                    fixture.store.clone(),
                    fixture.store.clone(),
                    Arc::new(NoListing),
                    StorageBackend::S3,
                    "managed-component".into(),
                    "tenant/".into(),
                )?;
                fixture.catalog = FleetCatalog::new(
                    &fixture.domain,
                    bucket,
                    fixture.declaration.namespace.cluster,
                    IncarnationId::new([80; 16])?,
                    "hosts/",
                )?;
            }
        }
        let f = &fixtures[0];
        let adapter = Catalog::new(
            f.store.clone(),
            CatalogOptions {
                prefix: "managed/".into(),
                ..Default::default()
            },
        )?;
        let namespace = f
            .domain
            .run(adapter.existing_namespace(f.declaration.namespace.clone()))
            .await??;
        let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
        let mut inventory_directories = vec![directory.clone()];
        for declaration in &f.additional_namespaces {
            inventory_directories.push(RangeDirectory::new(
                adapter
                    .existing_namespace(declaration.namespace.clone())
                    .await?,
                DirectoryLimits::default(),
            )?);
        }
        let standby = f
            .catalog
            .observe_managed_standby(
                &adapter,
                &f.declaration,
                DirectoryLimits::default(),
                TIMEOUT,
            )
            .await?;
        let observed = f.domain.run(directory.observe()).await??;
        ensure!(observed.revision() == 1 && observed.groups() == 0 && observed.digest() == standby);
        let history = GroupHistory {
            cluster: f.declaration.namespace.cluster,
            namespace: f.declaration.namespace.namespace,
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([7; 16])?,
        };
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
        let roots = (0..fixtures.len())
            .map(|_| tempfile::tempdir())
            .collect::<std::io::Result<Vec<_>>>()?;
        let mut nodes = Vec::new();
        let mut hosts = Vec::new();
        let mut peers = Vec::new();
        for (i, fixture) in fixtures.iter().enumerate() {
            let node = fixture.start_node(roots[i].path()).await?;
            let (mut config, services) = fixture
                .config(&node, LogOpenMode::Create, 24 + i as u8)
                .await?;
            if !spares.is_empty() || archives != ArchiveSetup::Disabled {
                config.shared.checkpoints = Some(checkpoint_limits());
            }
            if archives != ArchiveSetup::Disabled {
                archives::configure(&mut config);
            }
            config.host.automatic_partitions = partitions.clone().map(|mut options| {
                options.service.transition.coordinate = i == 3;
                options.service.execution = if i == 3 {
                    cellar::sdk::fleet_operations::RaftFleetPartitionExecutionPolicy::Complete
                } else {
                    cellar::sdk::fleet_operations::RaftFleetPartitionExecutionPolicy::AdmissionOnly
                };
                options
            });
            peers.push(config.peers[0].clone());
            let host = node.start_raft_cells(config, services).await?;
            ensure!(host.observe_node(TIMEOUT).await?.node.groups == 0);
            nodes.push(node);
            hosts.push(host);
        }
        let reserve = |reservation| -> Result<RaftManagedFirstGroupRequest> {
            Ok(RaftManagedFirstGroupRequest {
                reservation,
                standby,
                publication: ProposalId::new([121; 16])?,
                group: proposal()?,
            })
        };
        let cut = f.catalog.read(TIMEOUT).await?;
        ensure!(matches!(
            f.catalog
                .reserve_managed_first_group(
                    &adapter,
                    &f.declaration,
                    reserve(request(59, cut.revision()))?,
                    DirectoryLimits::default(),
                    TIMEOUT,
                )
                .await,
            Err(FleetCatalogError::Conflict)
        ));
        ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
        for (i, fixture) in fixtures.iter().enumerate() {
            admit_capacity(fixture, &hosts[i], &directory, 40 + 2 * i as u8).await?;
        }
        let cut = f.catalog.read(TIMEOUT).await?;
        let reserve_identity = request(60, cut.revision());
        let public_operator = matches!(first_group, FirstGroupSetup::Operator);
        let authorization = if public_operator {
            Box::pin(operator::provision(
                f,
                &directory,
                &hosts[0],
                reserve(reserve_identity)?,
            ))
            .await?
        } else {
            let reserved = f
                .catalog
                .reserve_managed_first_group(
                    &adapter,
                    &f.declaration,
                    reserve(reserve_identity.clone())?,
                    DirectoryLimits::default(),
                    TIMEOUT,
                )
                .await?;
            let cut = f.catalog.read(TIMEOUT).await?;
            ensure!(
                f.catalog
                    .check_inventory(&cut, &inventory_directories, TIMEOUT)
                    .await?
                    .histories()
                    == 1,
                "unselected reservation must retain its physical references"
            );
            let confirmation = request(61, cut.revision());
            let reserved = if matches!(first_group, FirstGroupSetup::ExerciseReplay) {
                verify_first_group_replay(
                    f,
                    &adapter,
                    &directory,
                    &reserved,
                    &confirmation,
                    reserve(reserve_identity)?,
                    history,
                )
                .await?
            } else {
                reserved
            };
            f.catalog
                .publish_managed_first_group(&reserved, &confirmation, TIMEOUT)
                .await?
        };
        if archives == ArchiveSetup::Initialized && !public_operator {
            archives::initialize(f, &adapter, history, roots[0].path()).await?;
        }
        let cut = f.catalog.read(TIMEOUT).await?;
        for host in hosts.iter().take(3) {
            for fixture in &fixtures {
                inspect_current(host, fixture.spec.node, cut.revision()).await?;
            }
            if public_operator {
                continue;
            }
            let result = host
                .activate_managed_initial_group(authorization.clone(), TIMEOUT)
                .await;
            if archives == ArchiveSetup::Missing {
                ensure!(
                    matches!(
                        result,
                        Err(CellGroupStartupError::Unconfirmed(
                            CellGroupControllerError::Archive(_)
                        ))
                    ),
                    "missing archive must refuse before bootstrap: {result:?}"
                );
                ensure!(host.observe_node(TIMEOUT).await?.node.groups == 0);
                ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
            } else {
                if let Err(error) = result {
                    // A failed activation may already own durable state. Join
                    // every started owner before preserving its disks; dropping
                    // node handles alone only requests cancellation.
                    let mut cleanup = Vec::new();
                    for node in nodes {
                        if let Err(error) = node.cancel().await {
                            cleanup.push(format!("node join: {error:#}"));
                        }
                    }
                    for host in &hosts {
                        if let Err(error) = host.close().await {
                            cleanup.push(format!("Raft close: {error:#}"));
                        }
                    }
                    for root in roots {
                        eprintln!("retained managed startup disk: {}", root.keep().display());
                    }
                    return Err(anyhow::Error::from(error)
                        .context(format!("managed startup cleanup: {}", cleanup.join("; "))));
                }
            }
        }
        if public_operator {
            let rooted = f.declaration.namespace.image_storage == RaftImageStorage::Extents;
            let selected = futures_util::future::try_join_all(
                hosts.iter().take(3).enumerate().map(|(index, voter)| {
                    let startup = voter.start_managed_namespace_first_group(
                        history.cluster,
                        history.namespace,
                        TIMEOUT,
                    );
                    async move {
                        startup.await.with_context(|| format!("initial group readiness voter={index}, extents={rooted}, history={history:?}"))
                    }
                }),
            ).await;
            let selected = match selected {
                Ok(selected) => selected,
                Err(error) => {
                    super::diagnostics::record_parts(
                        &hosts,
                        &fixtures,
                        history,
                        "initial-group-readiness",
                    )
                    .await;
                    let joined = movement::close(nodes, &hosts).await;
                    for root in roots {
                        eprintln!(
                            "retained initial group readiness disk: {}",
                            root.keep().display()
                        );
                    }
                    f.domain.shutdown().await;
                    if let Err(close_error) = joined {
                        eprintln!("initial group readiness joined shutdown: {close_error:?}");
                    }
                    return Err(error);
                }
            };
            ensure!(
                selected.iter().all(|selected| *selected == history),
                "unexpected managed namespace group"
            );
        }
        Ok(Self {
            fixtures,
            roots,
            nodes,
            hosts,
            peers,
            directory,
            history,
        })
    }
}

pub struct SeededCells {
    pub cells: Vec<CellHistory>,
    pub receipts: Vec<CommandReceipt>,
    pub retry: u64,
}

async fn verify_first_group_replay(
    f: &Fixture,
    adapter: &Catalog,
    directory: &RangeDirectory,
    prepared: &RaftManagedFirstGroup,
    confirmation: &FleetRequestIdentity,
    request_plan: RaftManagedFirstGroupRequest,
    history: GroupHistory,
) -> Result<RaftManagedFirstGroup> {
    let cut = f.catalog.read(TIMEOUT).await?;
    let reservation = f
        .catalog
        .recover_birth_reservation(&cut, directory, &history, TIMEOUT)
        .await?;
    let mut changed = request_plan.clone();
    changed.publication = ProposalId::new([125; 16])?;
    ensure!(matches!(
        f.catalog
            .reopen_managed_first_group(
                adapter,
                &f.declaration,
                changed.clone(),
                DirectoryLimits::default(),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(matches!(
        f.catalog
            .reserve_managed_first_group(
                adapter,
                &f.declaration,
                changed,
                DirectoryLimits::default(),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let gate = &f.store.first_group_birth_after;
    gate.armed.store(true, Ordering::Release);
    let conflicting = request(62, cut.revision() + 1);
    let (publication, cancellation) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            f.catalog.publish_managed_first_group(prepared, &conflicting, TIMEOUT),
            async {
                gate.entered.notified().await;
                let result = async {
                    let view = f.domain.run(directory.observe()).await??;
                    ensure!(view.groups() == 0 && view.digest() == request_plan.standby);
                    let winner = f.domain.run(directory.cancel_fleet_group_birth(
                        &reservation, ProposalId::new([124; 16])?,
                    )).await??;
                    ensure!(matches!(winner, cellar::application::consensus::verification::cell_groups::CellGroupBirthDisposition::Live(birth) if birth.route() == reservation.route()));
                    Ok::<_, anyhow::Error>(())
                }.await;
                gate.resume.notify_one();
                result
            }
        )
    }).await?;
    cancellation?;
    ensure!(matches!(publication, Err(FleetCatalogError::Conflict)));
    ensure!(f.domain.run(directory.observe()).await??.groups() == 1);
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(matches!(
        f.catalog.authorize_group(&cut, &history, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(matches!(
        f.catalog
            .publish_managed_first_group(prepared, confirmation, TIMEOUT)
            .await,
        Err(FleetCatalogError::Storage(_))
    ));
    ensure!(!f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision() + 1);
    // A confirmed range with a lost immutable birth must refuse rather than
    // reconstructing it from the operator's retained setup.
    use object_store::{path::Path as ObjectPath, ObjectStore};
    let key = ObjectPath::from(format!(
        "managed/namespaces/{}/cell-groups/births/{}/{}",
        hex::encode(history.namespace.bytes()),
        hex::encode(history.group.bytes()),
        hex::encode(history.incarnation.bytes()),
    ));
    let saved = f.store.inner.get(&key).await?.bytes().await?;
    f.store.inner.delete(&key).await?;
    let replay = f
        .catalog
        .publish_managed_first_group(prepared, confirmation, TIMEOUT)
        .await;
    let missing = matches!(
        f.store.inner.get(&key).await,
        Err(object_store::Error::NotFound { .. })
    );
    f.store.inner.put(&key, saved.into()).await?;
    ensure!(matches!(replay, Err(FleetCatalogError::Corrupt(_))) && missing);
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision() + 1);
    Ok(f.catalog
        .reopen_managed_first_group(
            adapter,
            &f.declaration,
            request_plan,
            DirectoryLimits::default(),
            TIMEOUT,
        )
        .await?)
}

pub async fn seed_cells(hosts: &[RaftCells], history: GroupHistory) -> Result<SeededCells> {
    let index = leader(&hosts[..3], history).await?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(60_000)
    .context("retry deadline")?;
    let mut cells = Vec::new();
    let mut receipts = Vec::new();
    for (i, delta) in [7i64, 11].into_iter().enumerate() {
        let cell = CellHistory {
            cluster: history.cluster,
            namespace: history.namespace,
            cell: CellId::new(format!("Counter:cold-{i}"))?,
            incarnation: IncarnationId::new([91; 16])?,
        };
        let handle = hosts[index].cell(cell.clone())?;
        handle
            .enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([180 + i as u8; 16])?,
                    durability: Durability::Quorum,
                },
                TIMEOUT,
            )
            .await?;
        let id = CommandId::new(ClientId::new([200; 16])?, 1)?;
        let receipt = handle
            .command(
                &handle.contract("Add")?,
                delta.to_be_bytes().to_vec(),
                CommandOptions::new(id, retry),
            )
            .await?;
        ensure!(receipt.result == delta.to_be_bytes());
        cells.push(cell);
        receipts.push(receipt);
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if hosts.iter().take(3).all(|host| {
                host.resource_usage().host.controller.groups == 1
                    && host.resource_usage().retained_manifest_cells == Some(2)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(SeededCells {
        cells,
        receipts,
        retry,
    })
}
