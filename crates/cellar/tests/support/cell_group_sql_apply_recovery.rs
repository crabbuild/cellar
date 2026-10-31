// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cold selected-root verification and replay of committed, unpublished work.
use super::*;

pub(crate) async fn recover(rooted: bool) -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let route = fixture.cluster.birth.route().clone();
    if rooted {
        for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
            voter.extents = Some(NodeExtentStore::open(
                fixture.cluster._root.path().join(format!("rooted-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?);
        }
    }
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let a = history(&route, "recover-a")?;
    let b = history(&route, "recover-b")?;
    apply::install(&mut fixture, &a, 0).await?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(29);")?;
    let accepted_a = fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &a, &bytes, rooted)?,
        )
        .await?;
    fixture.drive(Some(accepted_a.position.index)).await?;
    let selected = fixture.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("selected A")?;
    let accepted_b = fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &b, &bytes, rooted)?,
        )
        .await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fixture.pump(&[route.history], &mut held).await?;
            if held.contains_key(&route.history) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let work = held.remove(&route.history).context("unpublished B")?;
    let (pool, applier) = &fixture.appliers[&(0, route.history)];
    let prepared = pool.prepare_apply(applier, work).await?;
    assert_eq!(prepared.manifest().position(), accepted_b.position);
    drop(prepared);
    assert_eq!(
        fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .as_ref(),
        Some(&selected)
    );
    let directory = fixture.cluster.directory.clone();
    let birth = fixture.cluster.birth.clone();
    let disk = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
    fixture.close().await?;

    // All prior engine, page, image, worker and runtime owners are gone.
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let base = disk.path().join("node-1");
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                NodeId::new([1; 16])?,
                DiskLineage::new([11; 16])?,
                ProcessGeneration::new([31; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(fault::FaultFileSystem {
                gate: fault::Gate::new(None),
            }),
        ))
        .await??;
    let permit = domain
        .run(directory.bootstrap_group_member(&birth, &log))
        .await??;
    let group = log.cell_group(&route.history)?;
    let image_directory = if rooted {
        disk.path().join("rooted-0")
    } else {
        base.join("images")
    };
    let config = open_images(&image_directory, rooted)?;
    let pages = ManifestPageStore::open(
        &base.join("pages"),
        Arc::new(DirectFileSystem),
        1024,
        16 * 1024 * 1024,
        16,
    )?;
    let recovering = group.clone();
    let (applier, recovered) = domain
        .blocking(move || {
            CellGroupApplier::recover(
                recovering,
                pages,
                config,
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )
        })?
        .await??;
    assert_eq!(recovered.marker(), &selected);
    assert_eq!(recovered.pinned_images(), 1);
    assert_eq!(
        recovered.pin_cell(&a.cell).context("A")?.0.witness.position,
        accepted_a.position
    );
    assert!(
        recovered.pin_cell(&b.cell).is_none(),
        "unselected files became authoritative"
    );
    let (runtime, mut events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            election_seed: Some([71; 32]),
            ..Default::default()
        },
    )
    .await?;
    runtime.attach(permit, recovered).await?;
    let pool = CellGroupSqlPool::start(
        &log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 2,
            max_pending_cells: 2,
            group_pending_cells: 2,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?;
    // Replay durable committed B with only this node present. It is apply, not
    // renewed serving authority; messages are deliberately not delivered.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = events.recv().await.context("runtime event")?;
            match event.effect().map_err(|e| anyhow::anyhow!(e.to_string()))? {
                CellGroupEffect::Apply(_) => {
                    let work = event.retain_apply().context("replay")?;
                    pool.prepare_apply(&applier, work)
                        .await?
                        .complete(&runtime)
                        .await?;
                }
                CellGroupEffect::Applied { manifest, .. }
                    if manifest.position() == accepted_b.position =>
                {
                    break
                }
                CellGroupEffect::Message(_) | CellGroupEffect::Applied { .. } => (),
                _ => anyhow::bail!("unexpected recovery event"),
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(
        group.group_manifest()?.context("B marker")?.root().cells(),
        2
    );
    assert!(!runtime.status(route.history).await?.ready);
    assert!(runtime
        .execution_authority(route.history, b.clone())
        .await
        .is_err());
    pool.close().await?;
    runtime.close().await?;
    drop(events);
    drop(runtime);
    drop(pool);
    drop(applier);
    let selected = group.group_manifest()?.context("replayed root")?;
    // The open engine cache pins its published page/image closure too. A real
    // restart closes that owner before reopening the same directories.
    drop(group);
    log.close().await?;
    drop(log);
    domain.shutdown().await;

    // Missing physical closure must fail even though the engine marker remains
    // intact. Only this test's disposable directory is moved.
    std::fs::rename(&image_directory, image_directory.with_extension("saved"))?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                NodeId::new([1; 16])?,
                DiskLineage::new([11; 16])?,
                ProcessGeneration::new([32; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(fault::FaultFileSystem {
                gate: fault::Gate::new(None),
            }),
        ))
        .await??;
    let group = log.cell_group(&route.history)?;
    let config = open_images(&image_directory, rooted)?;
    let pages = ManifestPageStore::open(
        &base.join("pages"),
        Arc::new(DirectFileSystem),
        1024,
        16 * 1024 * 1024,
        16,
    )?;
    let recovering = group.clone();
    assert!(domain
        .blocking(move || CellGroupApplier::recover(
            recovering,
            pages,
            config,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100
        ))?
        .await?
        .is_err());
    assert_eq!(group.group_manifest()?.as_ref(), Some(&selected));
    drop(group);
    log.close().await?;
    drop(log);
    domain.shutdown().await;
    drop(disk);
    Ok(())
}

fn open_images(directory: &std::path::Path, rooted: bool) -> Result<CellGroupApplyImages> {
    let services = cellar_ltx::generation::ReadServices {
        now_unix_millis: Arc::new(|| 10),
        fill_random: Arc::new(|bytes| bytes.fill(17)),
    };
    Ok(if rooted {
        CellGroupApplyImages::Extents {
            images: NodeExtentStore::open(
                directory.into(),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?,
            limits: Default::default(),
            services,
            cache_directory: directory.with_extension("cache"),
            cache_pool: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * EXTENT_BYTES,
                max_files: 2,
            })?,
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        }
    } else {
        CellGroupApplyImages::Materialized {
            images: GenerationStore::open(
                directory.into(),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?,
            services,
        }
    })
}
