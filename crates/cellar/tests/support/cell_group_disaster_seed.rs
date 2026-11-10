// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual fresh engine seeding after removing every old voter directory. The
//! authority remains a contract fixture; no Docker launch fence is qualified.
use super::*;

pub(super) async fn erase_voters(s: &mut Scenario, rooted: bool) -> Result<()> {
    for mut voter in std::mem::take(&mut s.cluster.voters) {
        drop(voter.owner.take());
        drop(voter.held.take());
        voter.log.close().await?;
    }
    s.cluster.network.clear();
    let directory = s.cluster._root.path().to_owned();
    s.cluster
        .domain
        .blocking(move || -> Result<()> {
            for n in 1..=3 {
                let node = directory.join(format!("node-{n}"));
                assert!(node.join("log").exists());
                std::fs::remove_dir_all(&node)?;
                assert!(!node.exists());
                if rooted {
                    let extents = directory.join(format!("extents-{}", n - 1));
                    assert!(extents.exists());
                    std::fs::remove_dir_all(extents)?;
                }
            }
            Ok(())
        })?
        .await??;
    // Replace and erase the earlier publication verifier cache as well. The
    // fence/recovery now has neither a voter disk nor that warm recovery image.
    let cold = destination_at(
        &s.cluster,
        rooted,
        Arc::new(DirectFileSystem),
        s.cluster._root.path().join("verify-after-voter-loss"),
        s.cluster.birth.route().clone(),
    )
    .await?;
    drop(std::mem::replace(&mut s.destination, cold));
    let old_cache = s.cluster._root.path().join("archive-data");
    s.cluster
        .domain
        .blocking(move || std::fs::remove_dir_all(old_cache))?
        .await??;
    Ok(())
}

pub(crate) async fn seed_engine(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    let mut s = Box::new(Scenario::new(rooted, store).await?);
    let old_disk = s.cluster.voters[0]
        .log
        .cell_group(&s.cluster.birth.route().history)?
        .identity()
        .disk;
    erase_voters(&mut s, rooted).await?;
    let proof = s.fence(&s.cluster.directory, &s.request).await?;
    let route = s.request.replacement().route().clone();
    let node = s.request.replacement().voters()[0].node;
    let member = s.request.replacement().voters()[0].member;
    let physical = s.cluster._root.path().join("recovered-images");
    let destination = destination_at(
        &s.cluster,
        rooted,
        Arc::new(DirectFileSystem),
        physical.clone(),
        route.clone(),
    )
    .await?;
    let pool = CellGroupRecoveryPool::new(Default::default())?;
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?;
    let prepared = pool
        .prepare(
            &s.cluster.domain,
            proof,
            destination.store.clone(),
            scratch.clone(),
            s.cluster._root.path().into(),
        )
        .await?;
    let marker = prepared.manifest().marker().clone();
    let wrong = s
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                s.cluster._root.path().join("wrong-recovery-disk/log"),
                node,
                old_disk,
                ProcessGeneration::new([199; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    assert!(matches!(
        s.cluster
            .directory
            .seed_group_recovery(&s.cluster.domain, prepared.clone(), &wrong, &s.authority)
            .await,
        Err(CellGroupRecoveryError::Catalog(CatalogError::Conflict(
            "reused recovery storage"
        )))
    ));
    wrong.close().await?;
    drop(wrong);
    let gate = fault::Gate::new(None);
    let mut options = ReplicaLogOptions::new(
        s.cluster._root.path().join("recovered-node/log"),
        node,
        DiskLineage::new([200; 16])?,
        ProcessGeneration::new([201; 16])?,
        LogOpenMode::Create,
    );
    options.purge_interval_ms = None;
    let log = s
        .cluster
        .domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let release = gate.arm(fault::HOLD_SYNC);
    let mut work = Box::pin(s.cluster.directory.seed_group_recovery(
        &s.cluster.domain,
        prepared.clone(),
        &log,
        &s.authority,
    ));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(15),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, work.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => anyhow::bail!("genesis skipped engine sync"),
    }
    assert!(matches!(
        log.cell_group(&route.history),
        Err(LogError::NotFound)
    ));
    drop(work);
    drop(release);
    let group = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match log.cell_group(&route.history) {
                Ok(group) => break Ok::<_, LogError>(group),
                Err(LogError::NotFound) => tokio::task::yield_now().await,
                Err(error) => break Err(error),
            }
        }
    })
    .await??;
    assert_eq!(group.group_manifest()?.as_ref(), Some(&marker));
    assert!(!group.group_manifest()?.unwrap().is_active());
    assert_eq!(group.identity().member, member);
    let key = group.numeric_key();
    assert!(!s
        .remote
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .iter()
        .any(|meta| meta.location.as_ref().contains(&format!(
            "recovery/{}/seeds/",
            hex::encode(s.request.digest())
        )) && meta.location.as_ref().ends_with("/complete")));
    let seeded = s
        .cluster
        .directory
        .seed_group_recovery(&s.cluster.domain, prepared.clone(), &log, &s.authority)
        .await?;
    assert_eq!(seeded.group().numeric_key(), key);
    assert_eq!(seeded.manifest().marker(), &marker);
    assert_ne!(seeded.binding(), [0; 32]);
    tail_fixture::check_images(&s.cluster, seeded.manifest(), &s.expected).await?;
    let ordinary = CellGroupBootstrap {
        route: route.clone(),
        member,
        voters: s
            .request
            .replacement()
            .voters()
            .iter()
            .map(|m| m.member)
            .collect(),
        learners: vec![],
    };
    assert!(matches!(
        log.create_cell_group(ordinary.clone()).await,
        Err(LogError::WrongIdentity)
    ));
    drop(prepared);
    assert_eq!(pool.usage().operations, 0);
    // The engine owns the complete physical closure after import admission ends.
    let applier = seeded.applier().clone();
    let selected = s
        .cluster
        .domain
        .blocking(move || applier.recover_selected())?
        .await??;
    tail_fixture::check_images(&s.cluster, &selected, &s.expected).await?;
    drop(selected);
    drop(group);
    drop(seeded);
    drop(destination);
    log.close().await?;
    drop(log);
    options.mode = LogOpenMode::Resume;
    options.process = ProcessGeneration::new([202; 16])?;
    let log = s
        .cluster
        .domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let target = destination_at(
        &s.cluster,
        rooted,
        Arc::new(DirectFileSystem),
        physical,
        route.clone(),
    )
    .await?;
    let proof = s.fence(&s.cluster.directory, &s.request).await?;
    let prepared = pool
        .prepare(
            &s.cluster.domain,
            proof,
            target.store.clone(),
            scratch,
            s.cluster._root.path().into(),
        )
        .await?;
    let seeded = s
        .cluster
        .directory
        .seed_group_recovery(&s.cluster.domain, prepared.clone(), &log, &s.authority)
        .await?;
    assert_eq!(seeded.group().numeric_key(), key);
    assert_eq!(seeded.group().identity().process, options.process);
    assert_eq!(seeded.manifest().marker(), &marker);
    tail_fixture::check_images(&s.cluster, seeded.manifest(), &s.expected).await?;
    assert!(matches!(
        log.create_cell_group(ordinary).await,
        Err(LogError::WrongIdentity)
    ));
    drop(seeded);
    log.close().await?;
    drop(log);
    let path = options.directory.clone();
    s.cluster
        .domain
        .blocking(move || std::fs::remove_dir_all(path))?
        .await??;
    options.mode = LogOpenMode::Create;
    options.process = ProcessGeneration::new([203; 16])?;
    let blank = s
        .cluster
        .domain
        .run(ReplicaLog::open(options, Arc::new(DirectFileSystem)))
        .await??;
    assert!(matches!(
        s.cluster
            .directory
            .seed_group_recovery(&s.cluster.domain, prepared.clone(), &blank, &s.authority)
            .await,
        Err(CellGroupRecoveryError::Catalog(
            CatalogError::LostStableState
        ))
    ));
    assert!(matches!(
        blank.cell_group(&route.history),
        Err(LogError::NotFound)
    ));
    blank.close().await?;
    drop(prepared);
    drop(target);
    assert_eq!(pool.usage().operations, 0);
    assert_eq!(s.receivers.usage().sources, 0);
    let s = *s;
    s.cluster.close().await
}
