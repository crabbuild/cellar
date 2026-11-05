// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the exact rooted seal through the multiplexed three-voter owner.
use super::*;

pub(crate) async fn source_seal_blocks_delayed_work() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let route = fixture.cluster.birth.route().clone();
    let scope = route.history;
    attach_source_appliers(&mut fixture)?;
    let directory = fixture.cluster.directory.clone();
    let log = fixture.cluster.voters[0].log.clone();
    let domain = &fixture.cluster.domain;
    let plan = split_plan(&fixture.cluster).await?;
    domain.run(directory.plan_transition(&plan)).await??;
    domain
        .run(directory.prepare_transition(&plan, [14; 32]))
        .await??;
    anyhow::ensure!(matches!(
        domain
            .run(directory.prepare_source_seal(&plan, scope, &log))
            .await?,
        Err(CatalogError::Fenced)
    ));
    domain
        .run(directory.authorize_transition_seal(&plan))
        .await??;
    fixture.runtimes[0].campaign(scope).await?;
    fixture
        .drive(None)
        .await
        .context("initial source election")?;
    let cell = history(&route, "before-seal")?;
    let delayed = history(&route, "delayed-enrollment")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let accepted = fixture.runtimes[0]
        .propose(scope, request(&route, cell.clone(), 1, &cut)?)
        .await?;
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    anyhow::ensure!(matches!(
        fixture.runtimes[0].seal_source(permit).await,
        Err(RuntimeError::NotReady)
    ));
    fixture
        .drive(Some(accepted.position.index))
        .await
        .context("drain accepted enrollment")?;
    let before = log
        .cell_group(&scope)?
        .group_manifest()?
        .context("committed source image")?;
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    let CellGroupSealOutcome::Accepted(seal) = fixture.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("seal must first enter the log");
    };
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    let CellGroupSealOutcome::Accepted(retry) = fixture.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("seal committed without a voter quorum");
    };
    anyhow::ensure!(retry.position == seal.position && retry.digest == seal.digest);
    anyhow::ensure!(matches!(
        fixture.runtimes[0]
            .read_index(scope, b"late-read".to_vec())
            .await,
        Err(RuntimeError::NotReady)
    ));
    anyhow::ensure!(matches!(
        fixture.runtimes[0]
            .propose(scope, request(&route, delayed.clone(), 2, &cut)?)
            .await,
        Err(RuntimeError::NotReady)
    ));
    tokio::time::timeout(Duration::from_secs(20), async {
        while !fixture.applied[&scope]
            .iter()
            .all(|index| *index >= seal.position.index)
        {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("replicate and apply the source seal")??;
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    let CellGroupSealOutcome::Applied(applied) = fixture.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("committed seal was proposed a second time");
    };
    anyhow::ensure!(applied.position() == seal.position);
    let deadline = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() })
        .await?
        .checked_add(5_000)
        .context("read deadline")?;
    for (voter, runtime) in fixture.cluster.voters.iter().zip(&fixture.runtimes) {
        let marker = voter
            .log
            .cell_group(&scope)?
            .group_manifest()?
            .context("sealed source marker")?;
        anyhow::ensure!(marker.source_seal() == Some(&applied) && marker.root() == before.root());
        anyhow::ensure!(marker.root().cells() == 1 && !marker.is_active());
        let restored: GroupManifest = serde_json::from_slice(&serde_json::to_vec(&marker)?)?;
        anyhow::ensure!(restored == marker);
        anyhow::ensure!(matches!(
            runtime
                .read_authority(
                    scope,
                    cell.clone(),
                    CellReadConsistency::Linearizable,
                    deadline
                )
                .await,
            Err(RuntimeError::NotReady)
        ));
        anyhow::ensure!(matches!(
            runtime.read_index(scope, b"sealed-read".to_vec()).await,
            Err(RuntimeError::NotReady)
        ));
    }
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let snapshot = fixture.runtimes[0].capture_checkpoint(scope, &pool).await?;
    anyhow::ensure!(snapshot.marker().source_seal() == Some(&applied));
    let checkpoint = snapshot.marker().clone();
    fixture.runtimes[0]
        .publish_checkpoint(scope, snapshot.prepare_publication()?)
        .await?;
    let group = log.cell_group(&scope)?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while group.checkpoint_manifest()?.as_ref() != Some(&checkpoint) {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("publish the exact sealed checkpoint")??;
    drop(group);
    drop(log);
    let disk = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
    let birth = fixture.cluster.birth.clone();
    let store = fixture.cluster.store.clone();
    let options = fixture.cluster.options.clone();
    fixture.close().await?;
    let mut resumed = Box::pin(reopen_voters(
        disk,
        directory,
        store,
        birth,
        options,
        &checkpoint,
    ))
    .await?;
    resumed.runtimes[0].campaign(scope).await?;
    let convergence = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            resumed.pump(&[], &mut BTreeMap::new()).await?;
            let mut converged = true;
            for runtime in &resumed.runtimes {
                let status = runtime.status(scope).await?;
                converged &= status.leader == Some(1)
                    && status.term > applied.position().term
                    && status.applied.term == status.term
                    && status.applied.index == status.committed;
            }
            if converged {
                break Ok::<_, anyhow::Error>(());
            }
            // Deliver and persist the explicit campaign's messages before
            // advancing election time. A tight tick loop races disk sync.
            tokio::task::yield_now().await;
        }
    })
    .await;
    if convergence.is_err() {
        for (index, runtime) in resumed.runtimes.iter().enumerate() {
            eprintln!(
                "retained voter {}: {:?}",
                index + 1,
                runtime.status(scope).await
            );
        }
    }
    convergence.context("establish a fresh committed term on the reopened voters")??;
    let deadline = resumed
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() })
        .await?
        .checked_add(5_000)
        .context("retained read deadline")?;
    for (voter, runtime) in resumed.cluster.voters.iter().zip(&resumed.runtimes) {
        let status = runtime.status(scope).await?;
        anyhow::ensure!(!status.ready);
        let marker = voter
            .log
            .cell_group(&scope)?
            .group_manifest()?
            .context("retained source cut")?;
        anyhow::ensure!(marker.source_seal() == Some(&applied) && marker.root() == before.root());
        anyhow::ensure!(matches!(
            runtime
                .read_authority(
                    scope,
                    cell.clone(),
                    CellReadConsistency::Linearizable,
                    deadline
                )
                .await,
            Err(RuntimeError::NotReady)
        ));
        anyhow::ensure!(matches!(
            runtime
                .propose(scope, request(&route, delayed.clone(), 2, &cut)?)
                .await,
            Err(RuntimeError::NotReady)
        ));
    }
    let permit = resumed
        .cluster
        .domain
        .run(resumed.cluster.directory.prepare_source_seal(
            &plan,
            scope,
            &resumed.cluster.voters[0].log,
        ))
        .await??;
    let CellGroupSealOutcome::Applied(retry) = resumed.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("retained seal replay appended an entry");
    };
    anyhow::ensure!(retry == applied);
    resumed.close().await
}

pub(super) fn attach_source_appliers(fixture: &mut RuntimeCluster) -> Result<()> {
    let scope = fixture.cluster.birth.route().history;
    for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
        let pool = apply_pool(&voter.log)?;
        let applier = CellGroupApplier::new(
            voter.log.cell_group(&scope)?,
            voter.pages.take().context("source page owner")?,
            checkpoint_receive::config(
                voter.images.clone(),
                voter.extents.clone(),
                fixture.cluster._root.path(),
            )?,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        fixture.appliers.insert((n, scope), (pool, applier));
    }
    Ok(())
}

pub(super) async fn split_plan(cluster: &Cluster) -> Result<DirectoryMutation> {
    let directory = &cluster.directory;
    let domain = &cluster.domain;
    let route = cluster.birth.route().clone();
    let scope = route.history;
    let view = domain.run(directory.observe()).await??;
    let (left, right) = route
        .range
        .split(cell_routing_key(scope.namespace, &CellId::new("pivot")?))?;
    let mut children = Vec::new();
    for (id, range) in [(11, left), (12, right)] {
        let child = directory.prepare_group_birth(CellGroupBirthSpec {
            history: GroupHistory {
                group: GroupId::new([id; 16])?,
                incarnation: IncarnationId::new([id + 10; 16])?,
                ..scope
            },
            range,
            epoch: RouteEpoch::new(2)?,
            creation: ProposalId::new([id; 16])?,
            voters: (1..=3)
                .map(|n| {
                    Ok(InitialMember {
                        member: MemberId::new(n)?,
                        node: NodeId::new([n as u8; 16])?,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        })?;
        domain.run(directory.reserve_group_birth(&child)).await??;
        children.push(child.route().clone());
    }
    Ok(directory.prepare_replacement(
        &view,
        ProposalId::new([13; 16])?,
        std::slice::from_ref(&route),
        &children,
    )?)
}

fn apply_pool(log: &ReplicaLog) -> Result<CellGroupSqlPool> {
    Ok(CellGroupSqlPool::start(
        log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 1024 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?)
}

async fn reopen_voters(
    disk: tempfile::TempDir,
    directory: RangeDirectory,
    store: Arc<dyn object_store::ObjectStore>,
    birth: CellGroupBirth,
    options: RaftRuntimeOptions,
    checkpoint: &GroupManifest,
) -> Result<RuntimeCluster> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let scope = birth.route().history;
    let mut voters = Vec::with_capacity(3);
    let mut runtimes = Vec::with_capacity(3);
    let mut events = Vec::with_capacity(3);
    let mut appliers = BTreeMap::new();
    for n in 1..=3u8 {
        let base = disk.path().join(format!("node-{n}"));
        let filesystem = Arc::new(DirectFileSystem);
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    base.join("log"),
                    NodeId::new([n; 16])?,
                    DiskLineage::new([n + 10; 16])?,
                    ProcessGeneration::new([n + 90; 16])?,
                    LogOpenMode::Resume,
                ),
                filesystem.clone(),
            ))
            .await??;
        let group = log.cell_group(&scope)?;
        anyhow::ensure!(group.group_manifest()?.as_ref() == Some(checkpoint));
        if n == 1 {
            anyhow::ensure!(group.checkpoint_manifest()?.as_ref() == Some(checkpoint));
        }
        let permit = domain
            .run(directory.bootstrap_group_member(&birth, &log))
            .await??;
        let pages = ManifestPageStore::open(
            &base.join("pages"),
            filesystem.clone(),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let images = GenerationStore::open(base.join("images"), filesystem, Default::default())?;
        let config = checkpoint_receive::config(images.clone(), None, &base)?;
        let (applier, root) = domain
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
        anyhow::ensure!(root.marker() == checkpoint && root.pinned_images() == 1);
        let (runtime, receiver) = CellGroupRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([71; 32]),
                ..options.clone()
            },
        )
        .await?;
        runtime.attach(permit, root).await?;
        runtimes.push(runtime);
        events.push(receiver);
        appliers.insert((usize::from(n - 1), scope), (apply_pool(&log)?, applier));
        voters.push(Voter {
            log,
            owner: None,
            pages: None,
            images,
            extents: None,
            tree: ManifestTree::new(birth.route().clone(), ManifestLimits::default())?,
            gate: fault::Gate::new(None),
            paused: false,
            held: None,
        });
    }
    let cluster = Cluster {
        _root: disk,
        domain,
        directory,
        store,
        birth,
        voters,
        network: VecDeque::new(),
        reads: vec![],
        options,
    };
    Ok(RuntimeCluster {
        snapshot_sources: BTreeMap::new(),
        snapshot_transfers: 0,
        drop_snapshot_delivery: false,
        retry_snapshot: false,
        failed_snapshot: false,
        retry_checkpoint: None,
        transports: Vec::new(),
        appliers,
        cluster,
        runtimes,
        events,
        applied: BTreeMap::from([(scope, vec![checkpoint.position().index; 3])]),
        extra: BTreeMap::new(),
        network: VecDeque::new(),
    })
}
