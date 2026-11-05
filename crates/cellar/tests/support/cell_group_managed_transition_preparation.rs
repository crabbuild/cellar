// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rebuild a durable pre-copy from the same real managed archive hold.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupApplyImages, CellGroupArchiveError, CellGroupArchiveHold, CellGroupArchiveRegistry,
    CellGroupArchiveStore, CellGroupArchiveTailPool, CellGroupBirth, CellGroupCheckpointPool,
    CellGroupRecoveryLimits, CellGroupRecoveryPool, CellGroupTransferTarget,
    CellGroupTransitionPreparationInputs, CellGroupTransitionSource, GroupRoute, ManifestPageStore,
};
use cellar_ltx::{
    generation::{GenerationStore, ReadServices},
    internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem,
};
use rand::RngCore as _;

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    controller: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parent: &CellGroupBirth,
    children: &[GroupRoute],
    rooted: bool,
) -> Result<(CellGroupArchiveRegistry, CellGroupArchiveHold)> {
    let seeded = seed_cells(&cluster.hosts, cluster.history).await?;
    let domain = &cluster.fixtures[0].domain;
    let leader = leader(&cluster.hosts[..3], cluster.history).await?;
    let cell = &seeded.cells[0];
    let handle = cluster.hosts[leader].cell(cell.clone())?;
    let client = seeded.receipts[0].id.client;
    let mut options = CommandOptions::new(CommandId::new(client, 2)?, seeded.retry);
    options.durability = Durability::S3;
    let archived = handle
        .command(
            &handle.contract("Add")?,
            2i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    ensure!(archived.durability == Durability::S3 && archived.result == 9i64.to_be_bytes());
    let archives = CellGroupArchivePool::new(Default::default())?;
    let registry = directory
        .open_group_archive(domain.as_ref(), &archives, parent)
        .await?;
    let held = acquire_hold(domain, &registry)
        .await
        .context("acquire active source archive hold before pre-copy")?;
    let scratch = tempfile::tempdir()?;
    let mut selected = None;
    for stage in ["first", "reopened"] {
        let mut routes = vec![parent.route().clone()];
        routes.extend_from_slice(children);
        let root = scratch.path().join(stage);
        let mut stores = domain
            .blocking(move || {
                routes
                    .into_iter()
                    .enumerate()
                    .map(|(index, route)| store(root.join(index.to_string()), route, rooted))
                    .collect::<Result<Vec<_>>>()
            })?
            .await??
            .into_iter();
        let source = stores.next().context("missing source receiver")?;
        let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
            source.store,
            source.scratch,
            source.directory,
        )?;
        let prepared = controller
            .prepare_managed_transition(
                &transition.request,
                directory,
                &transition.plan,
                CellGroupTransitionPreparationInputs {
                    archives: archives.clone(),
                    tails: CellGroupArchiveTailPool::new(Default::default())?,
                    restore: recovery_pool()?,
                    sources: vec![CellGroupTransitionSource {
                        birth: parent.clone(),
                        hold: held.clone(),
                        receiver,
                    }],
                    targets: stores.collect(),
                },
                TIMEOUT,
            )
            .await?;
        ensure!(prepared.journal().phase() == TransitionPhase::Prepared);
        ensure!(prepared
            .children()
            .iter()
            .all(|child| child.genesis().is_none()));
        let digest = prepared
            .journal()
            .preparation()
            .context("missing pre-copy digest")?;
        ensure!(selected.is_none_or(|original| original == digest));
        selected = Some(digest);
        ensure!(
            prepared
                .children()
                .iter()
                .map(|child| child.root().cells())
                .sum::<u32>()
                == 2
        );
        let mut staged = prepared
            .children()
            .iter()
            .filter_map(|child| child.pin_cell(&cell.cell));
        let (leaf, _) = staged.next().context("missing archived cell")?;
        ensure!(leaf.version == archived.version && leaf.witness == archived.witness);
        ensure!(staged.next().is_none());
        ensure!(
            domain
                .run(directory.managed_preparation_holds(&transition.plan))
                .await??
                == vec![held.clone()]
        );
        if stage == "first" {
            // The pre-copy must preserve its exact historical cut while the
            // parent continues accepting subsequent quorum commands.
            let next = handle
                .command(
                    &handle.contract("Add")?,
                    1i64.to_be_bytes().to_vec(),
                    CommandOptions::new(CommandId::new(client, 3)?, seeded.retry),
                )
                .await?;
            ensure!(next.result == 10i64.to_be_bytes() && next.version != archived.version);
        }
    }
    Ok((registry, held))
}

fn recovery_pool() -> Result<CellGroupRecoveryPool> {
    Ok(CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
        max_cells: 4,
        max_image_bytes: 1024 * 1024,
        max_restore_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })?)
}

fn store(
    base: std::path::PathBuf,
    route: GroupRoute,
    rooted: bool,
) -> Result<CellGroupTransferTarget> {
    let filesystem = Arc::new(DirectFileSystem);
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?;
    let services = ReadServices {
        now_unix_millis: Arc::new(cellar::asyncrt::wall_ms),
        fill_random: Arc::new(|bytes| cellar::asyncrt::rng("transition-precopy").fill_bytes(bytes)),
    };
    let images = if rooted {
        CellGroupApplyImages::Extents {
            images: NodeExtentStore::open(
                base.join("extents"),
                filesystem.clone(),
                Default::default(),
            )?,
            services,
            limits: Default::default(),
            cache_directory: base.join("cache"),
            cache_pool: scratch.clone(),
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        }
    } else {
        CellGroupApplyImages::Materialized {
            images: GenerationStore::open(
                base.join("images"),
                filesystem.clone(),
                Default::default(),
            )?,
            services,
        }
    };
    let limits = limits();
    Ok(CellGroupTransferTarget {
        store: CellGroupArchiveStore::new(
            route,
            ManifestPageStore::open(
                &base.join("pages"),
                filesystem,
                limits.manifest_pages,
                limits.manifest_bytes,
                limits.retained_roots,
            )?,
            images,
            limits.manifest,
            limits.entries,
            limits.max_effect_rows,
        )?,
        scratch,
        directory: base,
    })
}

async fn acquire_hold(
    domain: &ExecutionDomain,
    registry: &CellGroupArchiveRegistry,
) -> Result<CellGroupArchiveHold> {
    Ok(tokio::time::timeout(TIMEOUT, async {
        let mut intent = registry.prepare_acquire(domain).await?;
        loop {
            match registry.acquire(domain, &intent).await {
                Ok(hold) => return Ok(hold),
                // A newer registry revision has fenced this exact intent.
                Err(CellGroupArchiveError::Expired) => {
                    intent = registry.prepare_acquire(domain).await?;
                }
                // Reconcile the same allocation until a hold or definite
                // expiry is observed. The archive publisher is still live.
                Err(CellGroupArchiveError::Conflict | CellGroupArchiveError::Unconfirmed) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??)
}

pub(super) async fn verify_sealed_cut(
    cluster: &mut ManagedCluster,
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parent: &CellGroupBirth,
    old_hold: CellGroupArchiveHold,
    seal: &GroupSourceSeal,
) -> Result<()> {
    let domain = &cluster.fixtures[0].domain;
    let archives = CellGroupArchivePool::new(Default::default())?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let registry = directory
        .open_group_archive(domain.as_ref(), &archives, parent)
        .await?;
    let scratch = tempfile::tempdir()?;
    let rooted = directory.namespace_config().image_storage == RaftImageStorage::Extents;
    let source = |label: &str, hold: CellGroupArchiveHold| {
        let base = scratch.path().join(label);
        let route = parent.route().clone();
        async move {
            let receiver_store = domain
                .blocking(move || store(base, route, rooted))?
                .await??;
            let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
                receiver_store.store,
                receiver_store.scratch,
                receiver_store.directory,
            )?;
            Ok::<_, anyhow::Error>(CellGroupTransitionSource {
                birth: parent.clone(),
                hold,
                receiver,
            })
        }
    };
    ensure!(matches!(
        fleet
            .verify_managed_sealed_sources(
                transition,
                directory,
                &archives,
                &tails,
                vec![source("pre-copy", old_hold).await?],
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for host in &cluster.hosts[..3] {
                let group = host.group(parent.route().history)?;
                if group
                    .archive_status()?
                    .and_then(|status| status.published)
                    .is_some_and(|marker| marker.source_seal() == Some(seal))
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("managed archiver did not publish the sealed cut")??;
    let held = acquire_hold(domain, &registry).await?;
    ensure!(held.marker().source_seal() == Some(seal));

    let first = fleet
        .verify_managed_sealed_sources(
            transition,
            directory,
            &archives,
            &tails,
            vec![source("verify", held.clone()).await?],
            TIMEOUT,
        )
        .await?;
    ensure!(first.journal().phase() == TransitionPhase::Sealed);
    ensure!(first.sources().len() == 1 && first.sources()[0].marker().source_seal() == Some(seal));
    let journal = first.journal().clone();
    let marker = first.sources()[0].marker().clone();
    ensure!(tails.usage().operations == 1);
    drop(first);
    ensure!(tails.usage().operations == 0);
    let reopened = domain
        .run(directory.sealed_source_holds(&transition.plan))
        .await??;
    ensure!(reopened == vec![held]);
    let replay = fleet
        .verify_managed_sealed_sources(
            transition,
            directory,
            &archives,
            &tails,
            vec![
                source(
                    "reopen",
                    reopened
                        .into_iter()
                        .next()
                        .context("selected source hold")?,
                )
                .await?,
            ],
            TIMEOUT,
        )
        .await?;
    ensure!(replay.journal() == &journal);
    ensure!(replay.sources()[0].marker() == &marker);

    let root = scratch.path().join("final");
    let routes = replay.children().to_vec();
    let targets = domain
        .blocking(move || {
            routes
                .into_iter()
                .enumerate()
                .map(|(index, route)| store(root.join(index.to_string()), route, rooted))
                .collect::<Result<Vec<_>>>()
        })?
        .await??;
    let prepared = recovery_pool()?
        .prepare_transfer_partition(domain.as_ref(), replay.clone(), targets)
        .await?;
    ensure!(prepared.children().len() == 2);
    ensure!(prepared
        .children()
        .iter()
        .all(|child| child.genesis().is_some()));
    ensure!(
        prepared
            .children()
            .iter()
            .map(|child| child.root().cells())
            .sum::<u32>()
            == 2
    );
    let first = prepared
        .children()
        .iter()
        .find(|child| child.root().cells() > 0)
        .context("retained restart requires a nonempty child")?
        .route()
        .history;
    drop(prepared);
    let transition = Arc::new(
        fleet
            .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
            .await?
            .context("reopen managed sealed transition for install")?,
    );
    let recovery = recovery_pool()?;
    ensure!(matches!(
        cluster.hosts[0]
            .install_managed_transfer_child(
                transition.clone(),
                replay.clone(),
                first,
                recovery.clone(),
                TIMEOUT,
            )
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    ensure!(matches!(
        cluster.hosts[0].group(first),
        Err(RuntimeError::NotFound)
    ));
    let mut retained = None;
    for route in replay.children() {
        let mut foreign_route = route.clone();
        foreign_route.epoch = RouteEpoch::new(route.epoch.get() + 1)?;
        ensure!(matches!(
            fleet
                .observe_managed_transfer_install_quorum(
                    &transition,
                    directory,
                    &foreign_route,
                    TIMEOUT,
                )
                .await,
            Err(FleetCatalogError::Conflict)
        ));
        ensure!(fleet
            .observe_managed_transfer_install_quorum(&transition, directory, route, TIMEOUT)
            .await?
            .is_none());
        let mut installed = Vec::new();
        for (index, host) in cluster.hosts[3..].iter().enumerate() {
            let receipt = host
                .install_managed_transfer_child(
                    transition.clone(),
                    replay.clone(),
                    route.history,
                    recovery.clone(),
                    TIMEOUT,
                )
                .await?;
            ensure!(receipt.member.get() == index as u64 + 1);
            ensure!(receipt.node == host.identity().node && receipt.disk == host.identity().disk);
            installed.push(receipt.clone());
            let observed = fleet
                .observe_managed_transfer_install_quorum(&transition, directory, route, TIMEOUT)
                .await?;
            if index == 0 {
                ensure!(observed.is_none());
                ensure!(matches!(
                    host.attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
                        .await,
                    Err(CellGroupControllerError::Catalog(_))
                ));
            } else {
                let observed = observed.context("managed install majority")?;
                ensure!(observed.genesis.route() == route && observed.voters == installed);
            }
            let repeated = host
                .install_managed_transfer_child(
                    transition.clone(),
                    replay.clone(),
                    route.history,
                    recovery.clone(),
                    TIMEOUT,
                )
                .await?;
            ensure!(repeated == receipt);
            if route.history == first && index == 0 {
                retained = Some(receipt);
            }
            ensure!(matches!(
                host.group(route.history)?.status().await,
                Err(RuntimeError::NotFound)
            ));
        }
    }
    Box::pin(restart_destination(
        cluster,
        directory,
        transition,
        replay,
        first,
        recovery,
        retained.context("first destination receipt")?,
    ))
    .await?;
    Ok(())
}

async fn restart_destination(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: Arc<ReopenedManagedCellGroupTransition>,
    sources: VerifiedCellGroupTransitionSources,
    child: GroupHistory,
    recovery: CellGroupRecoveryPool,
    expected: InstalledTransferVoter,
) -> Result<()> {
    const DESTINATION: usize = 3;
    let old = cluster.nodes.remove(DESTINATION);
    let old_host = cluster.hosts.remove(DESTINATION);
    let previous = old_host.identity();
    stop_transfer_destination(old, &old_host, 2, false).await?;
    let fixture = &cluster.fixtures[DESTINATION];
    let node = start_node(
        cluster.roots[DESTINATION].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let startup = async {
        let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 109).await?;
        crate::managed_fixture::archives::configure(&mut config);
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match startup {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!("retained transfer startup, shutdown={stopped:?}")));
        }
    };
    cluster.nodes.insert(DESTINATION, node);
    cluster.hosts.insert(DESTINATION, host);
    let host = &cluster.hosts[DESTINATION];
    ensure!(host.identity().process != previous.process);
    ensure!(host.observe_node(TIMEOUT).await?.node.groups == 2);
    for route in sources.children() {
        ensure!(matches!(
            host.group(route.history)?.status().await,
            Err(RuntimeError::NotFound)
        ));
    }
    ensure!(matches!(
        host.install_managed_transfer_child(
            transition.clone(),
            sources.clone(),
            child,
            recovery.clone(),
            TIMEOUT,
        )
        .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    let route = sources
        .children()
        .iter()
        .find(|route| route.history == child)
        .cloned()
        .context("retained child route")?;
    let historical = fixture
        .catalog
        .observe_managed_transfer_install_quorum(&transition, directory, &route, TIMEOUT)
        .await?
        .context("durable receipts survive an ordinary boot")?;
    ensure!(historical.voters.len() == 3 && historical.voters.contains(&expected));
    ensure!(matches!(
        host.attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    admit_capacity(fixture, host, directory, 225).await?;
    let reopened = host
        .install_managed_transfer_child(transition.clone(), sources, child, recovery, TIMEOUT)
        .await?;
    ensure!(reopened == expected);
    ensure!(matches!(
        host.group(child)?.status().await,
        Err(RuntimeError::NotFound)
    ));
    let voters = &cluster.hosts[DESTINATION..];
    voters[0]
        .attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
        .await?;
    ensure!(matches!(
        voters[0]
            .group(child)?
            .managed_pending_transfer_quorum(TIMEOUT)
            .await,
        Err(CellGroupControllerError::Runtime(
            RuntimeError::NotLeader | RuntimeError::NotReady
        ))
    ));
    for voter in &voters[1..] {
        voter
            .attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
            .await?;
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let mut applied = true;
            let mut witness = false;
            for voter in voters {
                let group = voter.group(child)?;
                let status = group.status().await?;
                ensure!(!status.ready, "a pending child served before publication");
                applied &= status.applied.index.get() > 1;
                match group.managed_pending_transfer_quorum(TIMEOUT).await {
                    Ok(_) => witness = true,
                    Err(CellGroupControllerError::Runtime(
                        RuntimeError::NotLeader | RuntimeError::NotReady,
                    )) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            if applied && witness {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("pending transfer child must commit a fresh term")??;
    for voter in voters {
        voter
            .attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
            .await?;
    }
    Ok(())
}
