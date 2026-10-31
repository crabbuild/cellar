// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resume the original voter disks and retry an owned, killed learner's disk.
use super::*;
use cellar_ltx::internal::NodeExtentStore;
use std::collections::VecDeque;

pub(crate) async fn seed_retry(rooted: bool) -> Result<()> {
    for cut in ["retry-prefix", "retry-synced"] {
        let directory = tempfile::tempdir()?;
        crash_fixture::kill_at(directory.path(), "seed_process_fixture", rooted, cut).await?;
        Box::pin(retry(directory.path(), rooted, cut))
            .await
            .with_context(|| format!("cold retry {rooted}/{cut}"))?;
    }
    Ok(())
}

async fn restore_source(
    root: &Path,
    oracle: &Oracle,
    domain: ExecutionDomain,
    directory: RangeDirectory,
    birth: CellGroupBirth,
    store: Arc<object_store::memory::InMemory>,
) -> Result<RuntimeCluster> {
    anyhow::ensure!(oracle.source_root.starts_with(root), "owned source disks");
    let mut voters = Vec::new();
    let mut appliers = Vec::new();
    let options = RaftRuntimeOptions::default();
    for n in 0..3 {
        let base = oracle.source_root.join(format!("node-{}", n + 1));
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    base.join("log"),
                    NodeId::new([n as u8 + 1; 16])?,
                    DiskLineage::new([n as u8 + 11; 16])?,
                    ProcessGeneration::new([95; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let images = GenerationStore::open(
            base.join("images"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let extents = if oracle.rooted {
            Some(NodeExtentStore::open(
                oracle.source_root.join(format!("rooted-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?)
        } else {
            None
        };
        let pages = ManifestPageStore::open(
            &base.join("pages"),
            Arc::new(DirectFileSystem),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let config =
            super::super::super::checkpoint_receive::config(images.clone(), extents.clone(), root)?;
        let group = log.cell_group(&oracle.expected.route().history)?;
        let (applier, prepared) = domain
            .blocking(move || {
                CellGroupApplier::recover(
                    group,
                    pages,
                    config,
                    ManifestLimits::default(),
                    GroupEntryLimits::default(),
                    100,
                )
            })?
            .await??;
        let permit = domain
            .run(directory.bootstrap_group_member(&birth, &log))
            .await??;
        let owner = domain
            .run(CellGroupOwner::start(
                permit,
                prepared,
                options.clone(),
                [71; 32],
            ))
            .await??;
        appliers.push(applier);
        voters.push(Voter {
            log,
            owner: Some(owner),
            pages: None,
            images,
            extents,
            tree: ManifestTree::new(birth.route().clone(), ManifestLimits::default())?,
            gate: fault::Gate::new(None),
            paused: false,
            held: None,
        });
    }
    let cluster = Cluster {
        _root: tempfile::tempdir_in(root)?,
        domain,
        directory,
        store,
        birth,
        voters,
        network: VecDeque::new(),
        reads: vec![],
        options,
    };
    let mut f = Box::pin(RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000)).await?;
    for (n, applier) in appliers.into_iter().enumerate() {
        let pool = apply::pool(&f, n)?;
        f.appliers.insert((n, applier.history()), (pool, applier));
    }
    f.connect_tcp().await.context("connect cold source peers")?;
    Ok(f)
}

async fn retry(root: &Path, rooted: bool, cut: &str) -> Result<()> {
    let oracle: Oracle = serde_json::from_slice(&std::fs::read(root.join("oracle.json"))?)?;
    assert_eq!(oracle.rooted, rooted);
    assert_eq!(oracle.cut, cut);
    let (partial, length): (std::path::PathBuf, u64) =
        serde_json::from_slice(&std::fs::read(root.join("dependency.json"))?)?;
    assert!(length > 0);
    if rooted {
        assert_eq!(
            partial.parent(),
            Some(root.join("learner/extents").as_path())
        );
        assert_eq!(std::fs::metadata(&partial)?.len(), length);
        let text = partial
            .file_name()
            .context("extent")?
            .to_str()
            .context("name")?;
        let full = u64::from_str_radix(&text[2..10], 16)?;
        if cut == "retry-prefix" {
            assert!(length < full);
        } else {
            assert_eq!(length, full);
        }
    }
    let route = oracle.expected.route().clone();
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (directory, birth, store) =
        crash_fixture::restore_catalog_store(oracle.objects.clone(), &route, &domain).await?;
    let mut f = Box::pin(restore_source(
        root,
        &oracle,
        domain,
        directory.clone(),
        birth.clone(),
        store,
    ))
    .await
    .context("restore cold source quorum")?;
    let controllers = controller::start_with_limits(&mut f, controller::limits())
        .await
        .context("start cold source controllers")?;
    f.runtimes[0]
        .campaign(route.history)
        .await
        .context("campaign cold source")?;
    controller::wait_for(async || Ok(f.runtimes[0].status(route.history).await?.ready)).await?;
    assert!(f.runtimes[0].status(route.history).await?.term > oracle.expected.position().term);
    let later = history(&route, "after-crash-before-retry")?;
    let bytes = images::image(
        None,
        "CREATE TABLE value(n); INSERT INTO value VALUES(317);",
    )?;
    let accepted = f.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &later, &bytes, rooted)?,
        )
        .await?;
    controller::applied(&f, route.history, accepted.position.index).await?;
    let expected = f.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("fresh source cut")?;
    assert_eq!(expected.root().cells(), 3);
    let base = root.join("learner");
    let mut local = seed_install::target(route.history)?;
    local.process = ProcessGeneration::new([94; 16])?;
    let log = f
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                local.node,
                local.disk,
                local.process,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let engine = log.storage_instance()?;
    let group = log.cell_group(&route.history)?;
    assert_eq!(group.group_manifest()?.as_ref(), Some(&oracle.staged));
    let (pages, images) = seed_install::stores(&base, rooted)?;
    let recovering = group.clone();
    let (applier, empty) = f
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
    assert_eq!(empty.pinned_images(), 0);
    drop(empty);
    assert!(f
        .cluster
        .domain
        .run(directory.bootstrap_joined_group_member(&birth, local.member, &log))
        .await?
        .is_err());
    let mut deleted = 0;
    for _ in 0..16 {
        let report = log
            .reclaim_cell_storage(
                vec![applier.clone()],
                vec![],
                CellGroupReclaimLimits {
                    max_selected_cells: 64,
                    max_files: 1,
                    max_page_scan: 64,
                },
            )
            .await?;
        assert!(report.image_files + report.page_files <= 1);
        deleted += report.image_files;
        if report.image_files + report.page_files == 0 && (!rooted || !partial.exists()) {
            break;
        }
    }
    if rooted {
        assert!(deleted > 0);
        assert!(!partial.exists());
    }
    assert_eq!(group.group_manifest()?.as_ref(), Some(&oracle.staged));
    assert!(f
        .cluster
        .domain
        .run(directory.bootstrap_joined_group_member(&birth, local.member, &log))
        .await?
        .is_err());
    let (runtime, events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            ..Default::default()
        },
    )
    .await?;
    let transport = tcp::start(&runtime, 3).await?;
    transport
        .enable_group_checkpoints(CellGroupCheckpointPool::new(checkpoint_fixture::limits())?)?;
    let source_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    f.transports[0].enable_group_checkpoints(source_pool)?;
    let mut physical = f.tcp_route(route.history)?;
    physical.revision += 1;
    physical.peers.push(RaftPeer {
        replica: local.clone(),
        label: "node-4".into(),
        server_name: "localhost".into(),
        control_address: transport.control_address(),
        data_address: transport.data_address(),
        query_address: transport.query_address(),
    });
    for source in &f.transports {
        source
            .install_physical_group(physical.clone())
            .context("install fresh learner route at source")?;
    }
    assert!(
        matches!(
            transport.install_physical_group(physical.clone()),
            Err(TransportError::StalePeer)
        ),
        "an empty stage cannot authorize ordinary group routing"
    );
    let _registration =
        f.transports[0].register_group_checkpoint(f.appliers[&(0, route.history)].1.clone())?;

    let session = Box::pin(transport.prepare_group_seed(
        local.clone(),
        physical.peers[0].clone(),
        directory.clone(),
        birth.clone(),
        reads::deadline(&f, 10_000).await?,
    ))
    .await
    .context("prepare fresh authenticated seed")?;
    let (permit, installed) = session
        .install(applier.clone())
        .await
        .context("install retried seed")?;
    assert_eq!(installed.marker(), &expected);
    transport
        .install_physical_group(physical.clone())
        .context("route authenticated installed learner")?;
    for name in ["registered-a", "registered-b", "after-crash-before-retry"] {
        assert!(installed.pin_cell(&CellId::new(name)?).is_some());
    }
    assert_eq!(log.storage_instance()?, engine);
    runtime.attach(permit, installed.clone()).await?;
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
    let learner = CellGroupController::start(
        runtime.clone(),
        events,
        transport.clone(),
        sql.clone(),
        vec![applier.clone()],
        controller::limits(),
    )
    .await?;
    let live = history(&route, "live-after-retry")?;
    let written = f.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &live, &bytes, rooted)?,
        )
        .await?;
    controller::applied(&f, route.history, written.position.index).await?;
    controller::wait_for(async || {
        f.runtimes[0].tick().await?;
        Ok(runtime.status(route.history).await?.applied.index >= written.position.index)
    })
    .await?;
    let expected = f.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("live suffix source")?;
    assert_eq!(expected.root().cells(), 4);
    assert_eq!(group.group_manifest()?.as_ref(), Some(&expected));
    assert_eq!(
        group.checkpoint_manifest()?.as_ref(),
        Some(installed.marker())
    );
    learner.close().await?;
    drop((learner, sql, installed, transport, runtime, applier, group));
    log.close().await?;
    drop(log);
    // Reopen the same successful retry independently of every previous owner.
    let log = f
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                local.node,
                local.disk,
                ProcessGeneration::new([96; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    assert_eq!(log.storage_instance()?, engine);
    let group = log.cell_group(&route.history)?;
    let (pages, images) = seed_install::stores(&base, rooted)?;
    let recovering = group.clone();
    let (applier, recovered) = f
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
    assert_eq!(recovered.marker(), &expected);
    assert_eq!(recovered.pinned_images(), 4);
    for name in [
        "registered-a",
        "registered-b",
        "after-crash-before-retry",
        "live-after-retry",
    ] {
        let (leaf, _) = recovered
            .pin_cell(&CellId::new(name)?)
            .context("cold recovered cell")?;
        assert_eq!(leaf.version.sequence.get(), 1);
    }
    let permit = f
        .cluster
        .domain
        .run(directory.bootstrap_joined_group_member(&birth, local.member, &log))
        .await??;
    let (runtime, events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            ..Default::default()
        },
    )
    .await?;
    runtime.attach(permit, recovered.clone()).await?;
    assert!(
        !runtime.status(route.history).await?.ready,
        "local seed receipt is not fresh quorum authority"
    );
    runtime.close().await?;
    drop((runtime, events, applier, recovered, group));
    log.close().await?;
    for c in controllers {
        c.close().await?;
    }
    drop(_registration);
    f.close().await
}
