//! Actual move participants must install trust before consuming a fresh permit.
use super::retirement::{admission, discover, reserve, step, stopped};
use super::*;

#[path = "raft_fleet_trust_store.rs"]
mod store;

#[path = "raft_fleet_ingress_native.rs"]
mod ingress;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_catalog_scoped_node_trust() -> Result<()> {
    let f = Fixture::new().await?;
    let (_source_root, source_node, source) = f.empty_host().await?;
    let source_view = source.observe_node(TIMEOUT).await?;
    let bucket = Bucket::from_clients(
        f.store.clone(),
        f.store.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "enrollment".into(),
        "tenant/".into(),
    )?;
    let foreign = FleetCatalog::new(
        &f.domain,
        bucket,
        f.declaration.namespace.cluster,
        IncarnationId::new([80; 16])?,
        "hosts-foreign/",
    )?;
    foreign.initialize(TIMEOUT).await?;
    let namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(f.declaration.namespace.clone())
    .await?;
    let root = foreign.read(TIMEOUT).await?;
    foreign
        .declare_namespace(
            &root,
            &Fixture::request(52, root.revision()),
            &namespace,
            "Counter",
            TIMEOUT,
        )
        .await?;
    let mut source_spec = f.spec.clone();
    source_spec.storage_instance = Some(source_view.node.storage_instance);
    let root = foreign.read(TIMEOUT).await?;
    foreign
        .register_node(
            &root,
            &Fixture::request(50, root.revision()),
            &source_spec,
            TIMEOUT,
        )
        .await?;
    let mut spec = f.peer_spec(7)?;
    let root = foreign.read(TIMEOUT).await?;
    foreign
        .register_node(
            &root,
            &Fixture::request(51, root.revision()),
            &spec,
            TIMEOUT,
        )
        .await?;
    let root = foreign.read(TIMEOUT).await?;
    let authorization = foreign.authorize_host(&root, spec.node, TIMEOUT).await?;
    let target_root = tempfile::tempdir()?;
    let target_node = public_node::node_named(
        target_root.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&target_node, authorization, &spec).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let target_address = config.peers[0].control_address;
    config.peers[0].control_address = listener.local_addr()?;
    let accepted = Arc::new(AtomicUsize::new(0));
    let seen = accepted.clone();
    let proxy = f.domain.spawn(async move {
        let (mut source, _) = listener.accept().await?;
        seen.fetch_add(1, Ordering::AcqRel);
        let mut target = tokio::net::TcpStream::connect(target_address).await?;
        tokio::io::copy_bidirectional(&mut source, &mut target).await?;
        Ok::<_, anyhow::Error>(())
    })?;
    let target = target_node.start_raft_cells(config, services).await?;
    let target_view = target.observe_node(TIMEOUT).await?;
    let epoch = target_view
        .current_enrollment
        .context("missing foreign enrollment")?
        .epoch;
    ensure!((1..=8).contains(&epoch));
    spec.storage_instance = Some(target_view.node.storage_instance);
    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .register_node(
            &root,
            &Fixture::request(70, root.revision()),
            &spec,
            TIMEOUT,
        )
        .await?;
    for ordinal in 1..epoch {
        f.state(spec.node, FleetNodeState::Registered, 80 + ordinal as u8)
            .await?;
    }
    let required = f.catalog.read(TIMEOUT).await?.revision();
    for ordinal in 0..8 {
        let root = foreign.read(TIMEOUT).await?;
        if root.revision() >= required {
            break;
        }
        foreign
            .commit(
                &root,
                &FleetMutation {
                    id: [210 + ordinal; 16],
                    operation: [232; 16],
                    principal: "catalog-scope-fixture".into(),
                    expected_revision: root.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Policy,
                        key: format!("scope-cut-{ordinal}"),
                        value: Some(vec![1]),
                    }],
                },
                TIMEOUT,
            )
            .await?;
    }
    ensure!(
        f.catalog
            .bind_namespace(f.declaration.namespace.clone())?
            .managed_fleet
            != foreign
                .bind_namespace(f.declaration.namespace.clone())?
                .managed_fleet
    );
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if source
                    .observe_node(TIMEOUT)
                    .await?
                    .fleet_trust_revision
                    .is_some_and(|r| r >= required)
                    && target
                        .observe_node(TIMEOUT)
                        .await?
                        .fleet_trust_revision
                        .is_some_and(|r| r >= required)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await???;
    let answer = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let result = source.inspect_peer_node(spec.node, required, TIMEOUT).await;
                if accepted.load(Ordering::Acquire) > 0 {
                    return Ok::<_, anyhow::Error>(result);
                }
                ensure!(result.is_err(), "metadata bypassed its advertised endpoint");
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await???;
    ensure!(
        matches!(answer, Err(InspectionError::Refused)),
        "another catalog acknowledged local trust: {answer:?}"
    );
    proxy.await??;
    ensure!(
        source.observe_node(TIMEOUT).await?.node.groups == 0
            && target.observe_node(TIMEOUT).await?.node.groups == 0
    );
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    stopped(source_node).await?;
    stopped(target_node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_move_trust_acknowledgements() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let nodes = fleet(&f).await?;
    let (proposal, authorization) = birth(&f).await?;
    let cells = registered(&nodes, &authorization).await?;
    activate(&cells, &authorization).await?;
    write_read(&f, &cells, 1, 7, 7).await?;
    let primary = primary(&f, &cells).await?;
    let source = &cells[primary];
    let spec = f.peer_spec(7)?;
    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .register_node(
            &root,
            &Fixture::request(57, root.revision()),
            &spec,
            TIMEOUT,
        )
        .await?;
    let gate = Arc::new(store::RootGate::new(f.store.clone()));
    let bucket = Bucket::from_clients(
        gate.clone(),
        gate.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "enrollment".into(),
        "tenant/".into(),
    )?;
    let catalog = FleetCatalog::new(
        &f.domain,
        bucket,
        f.declaration.namespace.cluster,
        IncarnationId::new([80; 16])?,
        "hosts/",
    )?;
    let root = catalog.read(TIMEOUT).await?;
    let auth = catalog.authorize_host(&root, spec.node, TIMEOUT).await?;
    let target_root = tempfile::tempdir()?;
    let target_node = public_node::node_named(
        target_root.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (config, services) = f.config_for(&target_node, auth, &spec).await?;
    let target_host = target_node.start_raft_cells(config, services).await?;
    f.state(spec.node, FleetNodeState::Active, 150).await?;
    discover(
        &f,
        &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &target_host],
    )
    .await?;
    let target = target_host
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(target.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    let held = gate.hold();
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while gate.active.load(Ordering::Acquire) == 0 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    let stale = target_host
        .observe_node(TIMEOUT)
        .await?
        .fleet_trust_revision
        .context("missing target trust cut")?;
    let (plan, permit) = reserve(
        &f,
        source,
        &proposal.scope,
        1,
        160,
        MemberId::new(((primary + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: spec.node,
            disk: spec.disk,
        },
    )
    .await?;
    let required = f.catalog.read(TIMEOUT).await?.revision();
    ensure!(required > stale);
    let mut pending = Box::pin(source.begin_managed_repair(permit.clone(), plan.clone(), TIMEOUT));
    let early = f
        .domain
        .run(async {
            cellar::asyncrt::select! {
                result = &mut pending => Some(result),
                _ = cellar::asyncrt::sleep(Duration::from_millis(150)) => None,
            }
        })
        .await?;
    ensure!(
        early.is_none(),
        "move consumed a permit before target trust acknowledgement: {early:?}"
    );
    let guard = source
        .fleet_membership_status()
        .await?
        .context("missing group guard")?;
    ensure!(guard.consumed_sequence == 0);
    let proof = source
        .observe_fleet_membership(authorization.clone(), TIMEOUT)
        .await?;
    ensure!(proof.configuration().learners().is_empty());
    drop(pending);
    ensure!(
        matches!(
            source
                .begin_managed_repair(permit.clone(), plan.clone(), Duration::from_millis(100))
                .await,
            Err(CommandFailure::Unavailable)
        ),
        "pre-admission expiry was not a clean refusal"
    );
    ensure!(
        source
            .fleet_membership_status()
            .await?
            .context("missing cancelled guard")?
            .consumed_sequence
            == 0
    );
    write_read(&f, &cells, 2, 5, 12).await?;
    ensure!(matches!(
        source
            .begin_managed_repair(permit.clone(), plan.clone(), Duration::from_secs(120))
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    let pending = source.begin_managed_repair(permit.clone(), plan.clone(), TIMEOUT);
    drop(held);
    ensure!(pending.await?.phase == RepairPhase::Seeding);
    let observed = f.remote(&nodes[primary].host, spec.node, required).await?;
    ensure!(observed
        .view
        .fleet_trust_revision
        .is_some_and(|r| r >= required));
    stopped(target_node).await?;
    // An admitted action must remain resumable with an unreachable participant,
    // even when a later authorization carries a newer root revision.
    f.state(spec.node, FleetNodeState::Draining, 151).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let retry = f
        .catalog
        .authorize_membership(&root, &proposal.scope, TIMEOUT)
        .await?;
    ensure!(
        source
            .begin_managed_repair(retry.clone(), plan, TIMEOUT)
            .await?
            .phase
            == RepairPhase::Seeding
    );
    ensure!(
        step(&f, source, &retry, RepairStep::AbortLearner)
            .await?
            .phase
            == RepairPhase::Aborted
    );
    let proof = source
        .observe_fleet_membership(authorization, TIMEOUT)
        .await?;
    ensure!(proof.status().consumed_sequence == 1 && proof.configuration().learners().is_empty());
    admission(&f, &proposal.scope, 162, None).await?;
    write_read(&f, &cells, 3, 1, 13).await?;
    ensure!(target_host.resource_usage().control.bytes == 0);
    let (plan, permit) = reserve(
        &f,
        source,
        &proposal.scope,
        2,
        170,
        MemberId::new(((primary + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(5)?,
            node: spec.node,
            disk: spec.disk,
        },
    )
    .await?;
    let unpolled = source.begin_managed_repair(permit, plan, Duration::from_secs(60));
    ensure!(
        source
            .fleet_membership_status()
            .await?
            .context("missing pre-close guard")?
            .consumed_sequence
            == 1
    );
    let report = f
        .domain
        .run(cellar::asyncrt::timeout(
            TIMEOUT,
            nodes[primary].host.close(),
        ))
        .await???;
    ensure!(
        report.unfinished.is_empty(),
        "unexpected unfinished commands: {:?}",
        report.unfinished
    );
    ensure!(
        nodes[primary].host.resource_usage().control.bytes == 4096,
        "physical admission resources survived shutdown: {:?}",
        nodes[primary].host.resource_usage().control
    );
    ensure!(matches!(unpolled.await, Err(CommandFailure::Unavailable)));
    ensure!(nodes[primary].host.resource_usage().control.bytes == 0);
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .get(
                &root,
                FleetTable::Admissions,
                &f.catalog.group_binding(&proposal.scope)?.key()?,
                TIMEOUT
            )
            .await?
            .is_some(),
        "shutdown released an unresolved root reservation"
    );
    for node in nodes {
        stopped(node.node).await?;
    }
    drop(target_root);
    f.domain.shutdown().await;
    Ok(())
}
