// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cold managed bootstrap must obtain inventory before reserving its first group.
use super::*;
use cellar::application::consensus::verification::cell_groups::RangeDirectory;

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
    let initialization = directory.prepare_standby_initialization(ProposalId::new([120; 16])?)?;
    let publication = f.domain.run(directory.publish(&initialization)).await??;
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

fn counter_class() -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [81; 32])?
        .initializer([82; 32], |_, sql| {
            sql.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)",
            )?;
            Ok(())
        })?
        .command("Add", [83; 32], |context, input| {
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

async fn peer_fixture(f: &Fixture, id: u8) -> Result<Fixture> {
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
        store: f.store.clone(),
        catalog: f.catalog.clone(),
        declaration: f.declaration.clone(),
        application: f.application.clone(),
        spec,
    })
}

async fn inspect_current(host: &RaftCells, node: NodeId, revision: u64) -> Result<()> {
    // The directory's actual timer must install the new rooted trust/process
    // view. Retrying this read does not mint capacity or alter the fleet cut.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match host.inspect_peer_node(node, revision, TIMEOUT).await {
                Ok(view) => {
                    ensure!(view
                        .view
                        .fleet_trust_revision
                        .is_some_and(|r| r >= revision));
                    return Ok::<_, anyhow::Error>(());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await??;
    Ok(())
}

async fn admit_capacity(
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
        engine_slots: 2,
        memory_swap_disabled: true,
        used_memory_bytes: 64 * 1024 * 1024,
        used_durable_bytes: 1024 * 1024,
        evidence_digest: [71; 32],
        evidence_reference: "fixture-only/deployment-attestation".into(),
    };
    for (offset, action, state) in [
        (0, FleetCapacityAction::Qualify, FleetNodeState::Ready),
        (1, FleetCapacityAction::Activate, FleetNodeState::Active),
    ] {
        let cut = f.catalog.read(TIMEOUT).await?;
        inspect_current(host, f.spec.node, cut.revision()).await?;
        let accepted = f
            .catalog
            .admit_node_capacity(
                &cut,
                &FleetCapacityRequest {
                    identity: request(id + offset, cut.revision()),
                    attestation: evidence.clone(),
                    action,
                },
                host,
                std::slice::from_ref(directory),
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

async fn leader(hosts: &[RaftCells], history: GroupHistory) -> Result<usize> {
    Ok(tokio::time::timeout(TIMEOUT, async {
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
    .await??)
}

async fn close_group_nodes(nodes: Vec<Node>, hosts: &[RaftCells]) -> Result<()> {
    for (node, host) in nodes.into_iter().zip(hosts) {
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        let report = host.close().await?;
        ensure!(
            report.groups_total == 1
                && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
        );
    }
    Ok(())
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
    let second = peer_fixture(&first, 5).await?;
    let third = peer_fixture(&first, 6).await?;
    let fixtures = [first, second, third];
    let f = &fixtures[0];
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
    let empty = directory.prepare_standby_initialization(ProposalId::new([120; 16])?)?;
    f.domain.run(directory.publish(&empty)).await??;
    let standby = f.domain.run(directory.observe()).await??;
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
    let roots = [
        tempfile::tempdir()?,
        tempfile::tempdir()?,
        tempfile::tempdir()?,
    ];
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
            .config(&node, LogOpenMode::Create, 24 + i as u8)
            .await?;
        let host = node.start_raft_cells(config, services).await?;
        ensure!(host.observe_node(TIMEOUT).await?.node.groups == 0);
        nodes.push(node);
        hosts.push(host);
    }
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.catalog
            .reserve_birth(
                &cut,
                &request(59, cut.revision()),
                &directory,
                proposal()?,
                "Counter",
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    for (i, fixture) in fixtures.iter().enumerate() {
        admit_capacity(fixture, &hosts[i], &directory, 40 + 2 * i as u8).await?;
    }
    let cut = f.catalog.read(TIMEOUT).await?;
    let reservation = f
        .catalog
        .reserve_birth(
            &cut,
            &request(60, cut.revision()),
            &directory,
            proposal()?,
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
            == 1,
        "unselected reservation must retain its physical references"
    );
    let ranges = directory.prepare_initialization_at(
        &standby,
        ProposalId::new([121; 16])?,
        std::slice::from_ref(reservation.authorization.route()),
    )?;
    f.domain.run(directory.publish(&ranges)).await??;
    let cut = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .confirm_birth(
            &cut,
            &request(61, cut.revision()),
            &directory,
            &reservation.authorization,
            TIMEOUT,
        )
        .await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    for host in &hosts {
        for fixture in &fixtures {
            inspect_current(host, fixture.spec.node, cut.revision()).await?;
        }
        let authorization = f.catalog.authorize_group(&cut, &history, TIMEOUT).await?;
        host.activate_managed_initial_group(authorization, TIMEOUT)
            .await?;
    }
    let index = leader(&hosts, history).await?;
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
            if hosts.iter().all(|host| {
                host.resource_usage().host.controller.groups == 1
                    && host.resource_usage().retained_manifest_cells == Some(2)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
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
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, 34 + i as u8)
            .await?;
        config.groups.push(RaftGroupAssignment {
            history,
            placement: RaftGroupPlacement::Replica,
        });
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
