// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh learner staging, physical install, catch-up and deleted-disk replacement.
use super::*;
use cellar::application::consensus::verification::JoiningMember;
use cellar_logic::consensus::{OperationId, ReplicaIdentity};
use cellar_ltx::internal::NodeExtentStore;
use raft::{
    prelude::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2},
    Storage,
};
use std::path::Path;

pub(super) fn target(history: GroupHistory) -> Result<ReplicaIdentity<GroupHistory>> {
    Ok(ReplicaIdentity {
        scope: history,
        node: NodeId::new([4; 16])?,
        disk: DiskLineage::new([14; 16])?,
        member: MemberId::new(4)?,
        process: ProcessGeneration::new([24; 16])?,
    })
}

pub(super) fn stores(
    base: &Path,
    rooted: bool,
) -> Result<(ManifestPageStore, CellGroupApplyImages)> {
    stores_with_filesystem(base, rooted, Arc::new(DirectFileSystem))
}

pub(super) fn stores_with_filesystem(
    base: &Path,
    rooted: bool,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<(ManifestPageStore, CellGroupApplyImages)> {
    let pages = ManifestPageStore::open(
        &base.join("pages"),
        filesystem.clone(),
        1024,
        16 * 1024 * 1024,
        16,
    )?;
    let images =
        GenerationStore::open(base.join("images"), filesystem.clone(), Default::default())?;
    let extents = if rooted {
        Some(NodeExtentStore::open(
            base.join("extents"),
            filesystem,
            Default::default(),
        )?)
    } else {
        None
    };
    Ok((
        pages,
        super::super::checkpoint_receive::config(images, extents, base)?,
    ))
}

async fn survivors(fixture: &RuntimeCluster, history: GroupHistory, index: LogIndex) -> Result<()> {
    controller::wait_for(async || {
        for runtime in &fixture.runtimes[..2] {
            if runtime.status(history).await?.applied.index < index {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await
}

async fn configure(
    fixture: &RuntimeCluster,
    route: &GroupRoute,
    command: ConfChangeV2,
) -> Result<LogIndex> {
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    let change = CellGroupMembershipChange::new(route, &group.membership()?, command, &[])?;
    let accepted = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match fixture.runtimes[0]
                .configure(route.history, change.clone())
                .await
            {
                Ok(accepted) => return Ok::<_, anyhow::Error>(accepted),
                Err(RuntimeError::NotReady) => {
                    fixture.runtimes[0].tick().await?;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await??;
    survivors(fixture, route.history, accepted.position.index).await?;
    Ok(accepted.position.index)
}

async fn prepare(
    fixture: &RuntimeCluster,
    transport: &RaftTransport,
    source: RaftPeer<GroupHistory>,
) -> Result<CellGroupSeedSession> {
    let deadline = reads::deadline(fixture, 10_000).await?;
    for attempt in 0..16 {
        let result = Box::pin(transport.prepare_group_seed(
            target(source.replica.scope)?,
            source.clone(),
            fixture.cluster.directory.clone(),
            fixture.cluster.birth.clone(),
            deadline,
        ))
        .await;
        match result {
            Ok(session) => return Ok(session),
            // A dropped exchange may still own the sole physical source slot.
            // This staging operation reconciles the SAME identities and engine;
            // retry only a transient transport refusal, within the original deadline.
            Err(CellGroupSeedError::Transport(
                TransportError::Io(_) | TransportError::Overloaded,
            )) if attempt < 15 => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!("bounded retries always return")
}

pub(crate) async fn seed_install(rooted: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
    Box::pin(replace(fixture, route, a, b, rooted)).await
}

async fn replace(
    mut fixture: RuntimeCluster,
    route: GroupRoute,
    a: CellHistory,
    b: CellHistory,
    rooted: bool,
) -> Result<()> {
    let second = controller::add_second(&mut fixture, route.history).await?;
    fixture.connect_tcp().await?;
    let source_pools = fixture
        .transports
        .iter()
        .map(|transport| -> Result<_> {
            let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
            transport.enable_group_checkpoints(pool.clone())?;
            Ok(pool)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    fixture.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    let local = target(route.history)?;
    membership::configure(
        &fixture,
        &route,
        membership::change(
            4,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Auto,
        ),
        &[JoiningMember {
            member: local.member,
            node: local.node,
            disk: local.disk,
        }],
    )
    .await?;

    let mut physical = fixture.tcp_route(route.history)?;
    // Remove a real voter's entire fixture disk before starting the transfer.
    controllers.remove(2).close().await?;
    fixture.appliers.retain(|(node, _), _| *node != 2);
    fixture.cluster.voters[2].log.close().await?;
    std::fs::remove_dir_all(fixture.cluster._root.path().join("node-3"))?;
    if rooted {
        std::fs::remove_dir_all(fixture.cluster._root.path().join("rooted-2"))?;
    }
    let erased = fixture
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                fixture.cluster._root.path().join("node-3/log"),
                NodeId::new([3; 16])?,
                DiskLineage::new([13; 16])?,
                ProcessGeneration::new([33; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    assert!(
        matches!(
            fixture
                .cluster
                .domain
                .run(
                    fixture
                        .cluster
                        .directory
                        .bootstrap_group_member(&fixture.cluster.birth, &erased)
                )
                .await?,
            Err(CatalogError::LostStableState)
        ),
        "old member identity must not recreate a lost disk"
    );
    erased.close().await?;

    let base = fixture.cluster._root.path().join("node-4");
    let gate = fault::Gate::new(None);
    let log = fixture
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                local.node,
                local.disk,
                local.process,
                LogOpenMode::Create,
            ),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let (runtime, events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            ..Default::default()
        },
    )
    .await?;
    let transport = tcp::start(&runtime, 3).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    transport.enable_group_checkpoints(pool.clone())?;
    physical.revision = 2;
    physical.peers.push(RaftPeer {
        replica: local.clone(),
        label: "node-4".into(),
        server_name: "localhost".into(),
        control_address: transport.control_address(),
        data_address: transport.data_address(),
        query_address: transport.query_address(),
    });
    let mut registrations = Vec::new();
    for node in 0..2 {
        fixture.transports[node].install_physical_group(physical.clone())?;
        registrations.push(
            fixture.transports[node]
                .register_group_checkpoint(fixture.appliers[&(node, route.history)].1.clone())?,
        );
    }
    super::super::seed_probes::source_protocol(
        &fixture,
        &physical,
        &source_pools[0],
        &mut registrations[0],
    )
    .await
    .context("seed protocol probes")?;
    // A stale ready flag cannot replace a fresh source quorum. Keep the seed
    // connection discoverable while disconnecting the remaining voting peer.
    let mut isolated = physical.clone();
    isolated.revision = 3;
    isolated.peers.retain(|peer| peer.replica.member.get() != 2);
    fixture.transports[1].remove_physical_group(&route.history)?;
    fixture.transports[0].install_physical_group(isolated)?;
    registrations[0] = fixture.transports[0]
        .register_group_checkpoint(fixture.appliers[&(0, route.history)].1.clone())?;
    assert!(fixture.runtimes[0].status(route.history).await?.ready);
    assert!(Box::pin(transport.prepare_group_seed(
        local.clone(),
        physical.peers[0].clone(),
        fixture.cluster.directory.clone(),
        fixture.cluster.birth.clone(),
        reads::deadline(&fixture, 100).await?
    ))
    .await
    .is_err());
    assert!(
        matches!(log.cell_group(&route.history), Err(LogError::NotFound)),
        "unproven source must not create a stage"
    );
    physical.revision = 4;
    for (node, registration) in registrations.iter_mut().enumerate() {
        fixture.transports[node].install_physical_group(physical.clone())?;
        *registration = fixture.transports[node]
            .register_group_checkpoint(fixture.appliers[&(node, route.history)].1.clone())?;
    }
    let baseline_tasks = transport.usage().tasks;
    let source = physical.peers[0].clone();
    let session = Box::pin(prepare(&fixture, &transport, source.clone()))
        .await
        .context("authenticate and stage fresh learner")?;
    assert!(
        fixture
            .cluster
            .domain
            .run(fixture.cluster.directory.bootstrap_joined_group_member(
                &fixture.cluster.birth,
                local.member,
                &log
            ))
            .await?
            .is_err(),
        "empty stage must never grant startup"
    );
    assert!(
        fixture
            .cluster
            .domain
            .run(
                fixture
                    .cluster
                    .directory
                    .bootstrap_group_member(&fixture.cluster.birth, &log)
            )
            .await?
            .is_err(),
        "learner is not a birth member"
    );
    let group = session.group().clone();
    let (pages, images) = stores(&base, rooted)?;
    let recovering = group.clone();
    let (applier, staged_root) = fixture
        .cluster
        .domain
        .blocking(move || {
            CellGroupApplier::recover(
                recovering,
                pages,
                images,
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )
        })?
        .await??;
    assert_eq!(staged_root.marker().root().cells(), 0);
    drop(staged_root);
    drop(session);
    controller::wait_for(async || Ok(transport.usage().tasks == baseline_tasks))
        .await
        .context("cancel staged exchange")?;
    let staged = group.group_manifest()?;
    let revoked = Box::pin(prepare(&fixture, &transport, source.clone())).await?;
    fixture.transports[0].remove_physical_group(&route.history)?;
    assert!(
        revoked.install(applier.clone()).await.is_err(),
        "revoked source cannot install its previously signed cut"
    );
    controller::wait_for(async || {
        Ok(transport.usage().tasks == baseline_tasks && source_pools[0].usage().sources == 0)
    })
    .await?;
    assert_eq!(group.group_manifest()?, staged);
    assert_eq!(group.checkpoint_manifest()?, None);
    assert_eq!(pool.usage().sources, 0);
    fixture.transports[0].install_physical_group(physical.clone())?;
    registrations[0] = fixture.transports[0]
        .register_group_checkpoint(fixture.appliers[&(0, route.history)].1.clone())?;
    let expired = Box::pin(transport.prepare_group_seed(
        local.clone(),
        source.clone(),
        fixture.cluster.directory.clone(),
        fixture.cluster.birth.clone(),
        reads::deadline(&fixture, 500).await?,
    ))
    .await?;
    // Wait for the bounded exchange to expire while holding the user session.
    // No sleep or fabricated process clock establishes the refusal.
    controller::wait_for(async || {
        Ok(transport.usage().tasks == baseline_tasks && source_pools[0].usage().sources == 0)
    })
    .await?;
    assert!(
        expired.install(applier.clone()).await.is_err(),
        "expired session cannot acquire a fresh install deadline"
    );
    assert_eq!(group.group_manifest()?, staged);
    assert_eq!(group.checkpoint_manifest()?, None);
    assert!(fixture
        .cluster
        .domain
        .run(fixture.cluster.directory.bootstrap_joined_group_member(
            &fixture.cluster.birth,
            local.member,
            &log
        ))
        .await?
        .is_err());
    // Even a fully verified generic snapshot install cannot mint the local
    // authenticated seed receipt. Metadata and physical bytes alone are insufficient.
    let authority = fixture.runtimes[0]
        .seed_authority(local.clone(), reads::deadline(&fixture, 10_000).await?)
        .await?;
    let capture_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let receiver_pool = pool.clone();
    let receiver_applier = applier.clone();
    let directory = base.clone();
    let prepared = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let mut source = authority.capture(&capture_pool)?;
            let mut receiver = receiver_pool.receive(
                receiver_applier,
                super::super::checkpoint_receive::scratch()?,
                directory,
            )?;
            source.stream(|chunk| receiver.append(chunk))?;
            Ok(receiver.finish()?)
        })?
        .await??;
    let mut write = GroupWrite::new(&group);
    write.hard_state = Some(raft::prelude::HardState {
        term: prepared.snapshot().get_metadata().term,
        commit: prepared.snapshot().get_metadata().index,
        ..Default::default()
    });
    write.snapshot = Some(SnapshotChange::CellGroup(prepared));
    log.persist_cells(OperationId::new(1801), vec![write])
        .await?;
    assert!(
        fixture
            .cluster
            .domain
            .run(fixture.cluster.directory.bootstrap_joined_group_member(
                &fixture.cluster.birth,
                local.member,
                &log
            ))
            .await?
            .is_err(),
        "generic snapshot must not mint local seed receipt"
    );
    let session = Box::pin(prepare(&fixture, &transport, source))
        .await
        .context("retry same staged engine")?;
    let expected = fixture.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("source cut")?;

    let release = gate.arm(fault::HOLD_SYNC);
    let installer = applier.clone();
    let pending = fixture
        .cluster
        .domain
        .spawn(async move { session.install(installer).await })?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    assert!(
        fixture
            .cluster
            .domain
            .run(fixture.cluster.directory.bootstrap_joined_group_member(
                &fixture.cluster.birth,
                local.member,
                &log
            ))
            .await?
            .is_err(),
        "receipt cannot precede the storage fence"
    );
    assert_eq!(
        pool.usage().sources,
        1,
        "physical receiver retained through sync"
    );
    // While the replacement receiver is physically held at its install fence,
    // both the surviving group and an unrelated group must keep committing.
    // These later mutations must reach the learner through the Raft suffix.
    let later = history(&route, "after-seed-cut")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(73);")?;
    let accepted = fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &later, &bytes, rooted)?,
        )
        .await?;
    survivors(&fixture, route.history, accepted.position.index).await?;
    let other = fixture.cluster.voters[0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("second group")?;
    let unrelated = fixture.runtimes[0]
        .propose(
            second,
            apply::enrollment_bytes(
                other.route(),
                &history(other.route(), "while-seeding")?,
                &bytes,
                false,
            )?,
        )
        .await?;
    survivors(&fixture, second, unrelated.position.index).await?;
    let (permit, root) = if rooted {
        drop(release);
        pending.await??
    } else {
        pending.abort();
        assert!(pending.await.is_err());
        assert_eq!(
            pool.usage().sources,
            1,
            "caller cancellation cannot release physical owner"
        );
        drop(release);
        controller::wait_for(async || Ok(transport.usage().tasks == baseline_tasks)).await?;
        let permit = fixture
            .cluster
            .domain
            .run(fixture.cluster.directory.bootstrap_joined_group_member(
                &fixture.cluster.birth,
                local.member,
                &log,
            ))
            .await??;
        let recovering = applier.clone();
        let root = fixture
            .cluster
            .domain
            .blocking(move || recovering.recover_selected())?
            .await??;
        (permit, root)
    };
    assert_eq!(root.marker(), &expected);
    assert!(root.pin_cell(&a.cell).is_some());
    assert!(root.pin_cell(&b.cell).is_some());
    assert!(root.pin_cell(&later.cell).is_none());
    assert_eq!(group.initial_state()?.conf_state.learners, vec![4]);
    transport.install_physical_group(physical.clone())?;
    runtime.attach(permit, root).await?;
    assert!(
        !runtime.status(route.history).await?.ready,
        "learner cannot serve writes"
    );
    let sql = CellGroupSqlPool::start(
        &log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 1024 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?;
    let controller = CellGroupController::start(
        runtime.clone(),
        events,
        transport.clone(),
        sql.clone(),
        vec![applier.clone()],
        controller::limits(),
    )
    .await?;
    controller::wait_for(async || {
        fixture.runtimes[0].tick().await?;
        Ok(runtime.status(route.history).await?.applied.index >= accepted.position.index)
    })
    .await
    .context("fresh learner catches up suffix")?;
    assert_eq!(
        group.group_manifest()?.context("caught up")?.root(),
        fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("leader")?
            .root()
    );

    let joint = ConfChangeV2 {
        transition: ConfChangeTransition::Explicit,
        changes: vec![
            ConfChangeSingle {
                node_id: 4,
                change_type: ConfChangeType::AddNode,
                ..Default::default()
            },
            ConfChangeSingle {
                node_id: 3,
                change_type: ConfChangeType::RemoveNode,
                ..Default::default()
            },
        ]
        .into(),
        ..Default::default()
    };
    let entered = configure(&fixture, &route, joint)
        .await
        .context("promote caught-up replacement")?;
    controller::wait_for(
        async || Ok(runtime.status(route.history).await?.applied.index >= entered),
    )
    .await?;
    let mut joint_conf = group.initial_state()?.conf_state;
    joint_conf.voters.sort_unstable();
    joint_conf.voters_outgoing.sort_unstable();
    assert_eq!(joint_conf.voters, vec![1, 2, 4]);
    assert_eq!(joint_conf.voters_outgoing, vec![1, 2, 3]);
    let left = configure(&fixture, &route, ConfChangeV2::default()).await?;
    controller::wait_for(async || Ok(runtime.status(route.history).await?.applied.index >= left))
        .await?;
    assert!(group.initial_state()?.conf_state.voters_outgoing.is_empty());
    assert!(group.initial_state()?.conf_state.learners.is_empty());
    controller::wait_for(async || {
        match fixture.runtimes[0].transfer_leader(route.history, 4).await {
            Ok(()) => Ok(true),
            Err(RuntimeError::NotReady) => Ok(false),
            Err(error) => Err(error.into()),
        }
    })
    .await?;
    controller::wait_for(async || Ok(runtime.status(route.history).await?.ready)).await?;
    let final_cell = history(&route, "replacement-leader")?;
    let committed = runtime
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &final_cell, &bytes, rooted)?,
        )
        .await?;
    survivors(&fixture, route.history, committed.position.index).await?;
    controller::wait_for(async || {
        Ok(runtime.status(route.history).await?.applied.index >= committed.position.index)
    })
    .await?;
    let current = group.group_manifest()?.context("replacement root")?;
    let checkpoint = group
        .checkpoint_manifest()?
        .context("installed checkpoint")?;
    controller.close().await?;
    drop((controller, sql, applier));
    log.close().await?;
    drop((transport, runtime, group, log));
    let resumed = fixture
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                local.node,
                local.disk,
                ProcessGeneration::new([25; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let permit = fixture
        .cluster
        .domain
        .run(fixture.cluster.directory.bootstrap_joined_group_member(
            &fixture.cluster.birth,
            local.member,
            &resumed,
        ))
        .await??;
    let resumed_group = resumed.cell_group(&route.history)?;
    assert_eq!(
        resumed_group.checkpoint_manifest()?.as_ref(),
        Some(&checkpoint)
    );
    let (pages, images) = stores(&base, rooted)?;
    let (applier, root) = fixture
        .cluster
        .domain
        .blocking(move || {
            CellGroupApplier::recover(
                resumed_group,
                pages,
                images,
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )
        })?
        .await??;
    assert_eq!(root.marker(), &current);
    for cell in [&a, &b, &later, &final_cell] {
        assert!(root.pin_cell(&cell.cell).is_some());
    }
    Box::pin(reconnect(
        &fixture, physical, resumed, permit, root, applier, rooted,
    ))
    .await
    .context("replacement process reconnect")?;
    for controller in controllers {
        controller.close().await?;
    }
    drop(registrations);
    assert_eq!(pool.usage().sources, 0);
    fixture.close().await
}

/// Start a new process from the installed disk and use only production transport
/// and controllers to reacquire quorum, then prove a minority cannot commit.
async fn reconnect(
    fixture: &RuntimeCluster,
    mut physical: cellar::application::consensus::verification::GroupRoute<GroupHistory>,
    log: ReplicaLog,
    permit: CellGroupBootstrapPermit,
    root: PreparedGroupManifest,
    applier: CellGroupApplier,
    rooted: bool,
) -> Result<()> {
    let route = root.marker().route().clone();
    let group = log.cell_group(&route.history)?;
    let (runtime, events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            ..Default::default()
        },
    )
    .await?;
    runtime.attach(permit, root).await?;
    assert!(
        !runtime.status(route.history).await?.ready,
        "disk receipt cannot renew quorum"
    );
    let transport = tcp::start(&runtime, 3).await?;
    transport
        .enable_group_checkpoints(CellGroupCheckpointPool::new(checkpoint_fixture::limits())?)?;
    physical.revision += 1;
    physical.peers.retain(|peer| peer.replica.member.get() != 3);
    let peer = physical
        .peers
        .iter_mut()
        .find(|peer| peer.replica.member.get() == 4)
        .context("replacement route")?;
    peer.replica = group.identity().clone();
    peer.control_address = transport.control_address();
    peer.data_address = transport.data_address();
    peer.query_address = transport.query_address();
    for source in &fixture.transports[..2] {
        source
            .install_physical_group(physical.clone())
            .context("refresh survivor route after process restart")?;
    }
    transport
        .install_physical_group(physical)
        .context("install restarted voter route")?;
    let sql = CellGroupSqlPool::start(
        &log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 1024 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?;
    let controller = CellGroupController::start(
        runtime.clone(),
        events,
        transport.clone(),
        sql,
        vec![applier],
        controller::limits(),
    )
    .await?;
    runtime.campaign(route.history).await?;
    controller::wait_for(async || {
        // Every fixture voter has automatic ticking disabled. Survivors must
        // also advance past the old process's leader lease before campaigning
        // can succeed. If one wins first, use the real caught-up handoff path.
        for survivor in &fixture.runtimes[..2] {
            survivor.tick().await?;
            if survivor.status(route.history).await?.ready {
                match survivor.transfer_leader(route.history, 4).await {
                    Ok(()) | Err(RuntimeError::NotReady) => (),
                    Err(error) => return Err(error.into()),
                }
            }
        }
        runtime.tick().await?;
        Ok(runtime.status(route.history).await?.ready)
    })
    .await
    .context("restarted voter regains quorum")?;
    let cell = history(&route, "after-process-restart")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(91);")?;
    let accepted = runtime
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &cell, &bytes, rooted)?,
        )
        .await?;
    survivors(fixture, route.history, accepted.position.index).await?;
    controller::wait_for(async || {
        Ok(runtime.status(route.history).await?.applied.index >= accepted.position.index)
    })
    .await?;
    drop(
        runtime
            .read_authority(
                route.history,
                cell.clone(),
                CellReadConsistency::Linearizable,
                reads::deadline(fixture, 5000).await?,
            )
            .await?,
    );
    let before = group.group_manifest()?.context("quorum root")?;
    for source in &fixture.transports[..2] {
        source.remove_physical_group(&route.history)?;
    }
    transport.remove_physical_group(&route.history)?;
    assert!(
        runtime.status(route.history).await?.ready,
        "exercise stale ready status"
    );
    let pending_cell = history(&route, "isolated-cannot-commit")?;
    let pending = runtime
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &pending_cell, &bytes, rooted)?,
        )
        .await?;
    // Admission is not a commit. A fresh correlated quorum read must fail even
    // though the old leader can still enqueue a proposal during check-quorum lag.
    assert!(matches!(
        runtime
            .read_authority(
                route.history,
                cell,
                CellReadConsistency::Linearizable,
                reads::deadline(fixture, 150).await?
            )
            .await,
        Err(RuntimeError::DeadlineExceeded)
    ));
    assert!(runtime.status(route.history).await?.applied.index < pending.position.index);
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    controller::wait_for(async || {
        runtime.tick().await?;
        Ok(!runtime.status(route.history).await?.ready)
    })
    .await?;
    assert!(matches!(
        runtime
            .propose(
                route.history,
                apply::enrollment_bytes(&route, &pending_cell, &bytes, rooted)?
            )
            .await,
        Err(RuntimeError::NotLeader | RuntimeError::NotReady)
    ));
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    controller.close().await?;
    log.close().await?;
    Ok(())
}
