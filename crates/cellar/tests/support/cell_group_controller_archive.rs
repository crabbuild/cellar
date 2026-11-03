// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real three-voter controllers, conditional object-store I/O and private replay.
use super::*;
use cellar_logic::consensus::OperationId;
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use std::sync::atomic::Ordering;

pub(super) use crate::archive_store;

#[path = "cell_group_log_reclaim.rs"]
mod log_reclaim;
pub(crate) use log_reclaim::physical_log_reclamation;

pub(super) async fn verifier(
    fixture: &RuntimeCluster,
    route: &GroupRoute,
    rooted: bool,
    name: &str,
) -> Result<CellGroupArchiveStore> {
    let route = route.clone();
    let path = fixture.cluster._root.path().join(name);
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let filesystem = Arc::new(DirectFileSystem);
            let pages = ManifestPageStore::open(
                &path.join("pages"),
                filesystem.clone(),
                1024,
                16 * 1024 * 1024,
                16,
            )?;
            let services = cellar_ltx::generation::ReadServices {
                now_unix_millis: Arc::new(|| 10),
                fill_random: Arc::new(|b| b.fill(17)),
            };
            let images = if rooted {
                CellGroupApplyImages::Extents {
                    images: NodeExtentStore::open(
                        path.join("extents"),
                        filesystem,
                        Default::default(),
                    )?,
                    services,
                    limits: Default::default(),
                    cache_directory: path.join("cache"),
                    cache_pool: ScratchPool::new(ScratchLimits {
                        max_bytes: 32 * 1024 * 1024,
                        max_files: 1,
                    })?,
                    cache: CacheLimits {
                        max_cached_bytes: EXTENT_BYTES,
                        max_cached_extents: 1,
                    },
                }
            } else {
                CellGroupApplyImages::Materialized {
                    images: GenerationStore::open(
                        path.join("images"),
                        filesystem,
                        Default::default(),
                    )?,
                    services,
                }
            };
            Ok(CellGroupArchiveStore::new(
                route,
                pages,
                images,
                Default::default(),
                Default::default(),
                100,
            )?)
        })?
        .await?
}

pub(super) async fn registration(
    fixture: &RuntimeCluster,
    route: &GroupRoute,
    rooted: bool,
) -> Result<(CellGroupArchiveRegistration, Arc<archive_store::Store>)> {
    registration_minimum(fixture, route, rooted, Durability::Quorum).await
}

pub(super) async fn registration_minimum(
    fixture: &RuntimeCluster,
    route: &GroupRoute,
    rooted: bool,
    minimum: Durability,
) -> Result<(CellGroupArchiveRegistration, Arc<archive_store::Store>)> {
    let io = CellGroupArchivePool::new(Default::default())?;
    let binding = fixture
        .cluster
        .directory
        .prepare_group_archive_binding(&fixture.cluster.domain, &io, &fixture.cluster.birth)
        .await?;
    let bytes = binding.encode().to_vec();
    let path = fixture.cluster._root.path().join("archive-binding-intent");
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::File::open(path.parent().unwrap())?.sync_all()?;
            Ok(())
        })?
        .await??;
    binding.commit(&fixture.cluster.domain).await?;
    let remote = archive_store::Store::around(fixture.cluster.store.clone());
    let catalog = Catalog::new(remote.clone(), Default::default())?;
    let mut config = NamespaceConfig::new(
        route.history.cluster,
        route.history.namespace,
        cellar_logic::NamespaceMode::Raft,
        route.schema_digest,
    );
    config.minimum_durability = minimum;
    config.image_storage = fixture.cluster.directory.namespace_config().image_storage;
    let namespace = fixture
        .cluster
        .domain
        .run(catalog.existing_namespace(config))
        .await??;
    let directory = RangeDirectory::new(namespace, Default::default())?;
    let birth = fixture
        .cluster
        .domain
        .run(directory.materialize_selected_group(route))
        .await??;
    let store = verifier(fixture, route, rooted, "controller-archive").await?;
    Ok((
        CellGroupArchiveRegistration {
            directory,
            birth,
            io,
            owners: CellGroupArchiveUploadPool::new(Default::default())?,
            tails: CellGroupArchiveTailPool::new(Default::default())?,
            checkpoints: CellGroupCheckpointPool::new(checkpoint_fixture::limits())?,
            store,
            scratch: ScratchPool::new(ScratchLimits {
                max_bytes: 32 * 1024 * 1024,
                max_files: 1,
            })?,
            scratch_directory: fixture.cluster._root.path().into(),
        },
        remote,
    ))
}

pub(super) async fn published(
    controller: &CellGroupController,
    group: GroupHistory,
    index: LogIndex,
) -> Result<()> {
    controller::wait_for(async || {
        let status = controller
            .archive_status(group)?
            .context("archive status")?;
        if let Some(error) = &status.error {
            if status.stopped {
                anyhow::bail!("archive stopped: {error}");
            }
        }
        Ok(status
            .published
            .is_some_and(|marker| marker.position().index >= index))
    })
    .await
}

async fn add(
    fixture: &RuntimeCluster,
    route: &GroupRoute,
    rooted: bool,
    ordinal: u8,
) -> Result<LogIndex> {
    let image = images::image(
        None,
        &format!("CREATE TABLE value(n); INSERT INTO value VALUES({ordinal})"),
    )?;
    let cell = history(route, &format!("later-{ordinal}"))?;
    let proposal = fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(route, &cell, &image, rooted)?,
        )
        .await?;
    controller::applied(fixture, route.history, proposal.position.index).await?;
    Ok(proposal.position.index)
}

async fn setup(rooted: bool) -> Result<(RuntimeCluster, GroupRoute)> {
    setup_store(rooted, Arc::new(object_store::memory::InMemory::new())).await
}

async fn setup_store(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<(RuntimeCluster, GroupRoute)> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (fixture, route, _, _) = Box::pin(registered::setup_store(rooted, &class, store)).await?;
    Ok((fixture, route))
}

pub(super) async fn verified_current(
    fixture: &RuntimeCluster,
    config: &CellGroupArchiveRegistration,
    rooted: bool,
    name: &str,
) -> Result<CellGroupArchiveCoverage> {
    let route = config.birth.route();
    let registry = config
        .directory
        .open_group_archive(&fixture.cluster.domain, &config.io, &config.birth)
        .await?;
    let directory = fixture
        .cluster
        ._root
        .path()
        .join("node-1/log/cell-group-archive-journals");
    let envelope = fixture
        .cluster
        .domain
        .blocking(move || -> Result<serde_json::Value> {
            let entry = std::fs::read_dir(directory)?
                .next()
                .context("journal directory")??;
            Ok(serde_json::from_slice(&std::fs::read(
                entry.path().join("state"),
            )?)?)
        })?
        .await??;
    let hold: CellGroupArchiveHold = serde_json::from_value(envelope["record"]["head"].clone())?;
    assert!(envelope["record"]["retire"].is_null());
    assert_eq!(envelope["record"]["pending"], "None");
    let destination = verifier(fixture, route, rooted, name).await?;
    let coverage = registry
        .verify_hold(
            &fixture.cluster.domain,
            &hold,
            &config.tails,
            config.checkpoints.receive_archive(
                destination,
                config.scratch.clone(),
                config.scratch_directory.clone(),
            )?,
        )
        .await?;
    Ok(coverage)
}

async fn audit(
    fixture: &RuntimeCluster,
    config: &CellGroupArchiveRegistration,
    rooted: bool,
    last: u8,
) -> Result<()> {
    let coverage = verified_current(fixture, config, rooted, "controller-archive-audit").await?;
    let root = coverage.recovery().manifest();
    assert_eq!(root.pinned_images(), usize::from(last) + 2);
    let names = ["registered-a".to_owned(), "registered-b".to_owned()]
        .into_iter()
        .chain((1..=last).map(|n| format!("later-{n}")));
    for name in names {
        let expected = if let Some(n) = name.strip_prefix("later-") {
            images::image(
                None,
                &format!("CREATE TABLE value(n); INSERT INTO value VALUES({n})"),
            )?
        } else {
            images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(0);")?
        };
        let (_, pin) = root
            .pin_cell(&CellId::new(&name)?)
            .context("archived cell")?;
        let restored = fixture
            .cluster
            .domain
            .blocking(move || -> Result<Vec<u8>> {
                Ok(match pin {
                    CellImagePin::Empty => Vec::new(),
                    CellImagePin::Materialized(pin) => pin
                        .open_reader()?
                        .read_exact_at(0, pin.state().length as usize)?,
                    CellImagePin::Extents(pin) => {
                        let mut bytes = Vec::new();
                        for n in 0..pin.manifest().geometry().extent_count() {
                            bytes.extend(
                                pin.view()
                                    .read_extent(pin.manifest().extent(n).context("extent")?)?,
                            );
                        }
                        bytes
                    }
                })
            })?
            .await??;
        assert_eq!(restored, expected);
    }
    Ok(())
}

pub(crate) async fn archive_controller_publish(rooted: bool) -> Result<()> {
    let (mut fixture, route) = setup(rooted).await?;
    publish(&mut fixture, &route, rooted).await?;
    fixture.close().await
}

pub(crate) async fn archive_controller_publish_store(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let (mut fixture, route) = setup_store(rooted, store).await?;
    publish(&mut fixture, &route, rooted).await?;
    fixture.close().await
}

async fn publish(fixture: &mut RuntimeCluster, route: &GroupRoute, rooted: bool) -> Result<()> {
    let (config, remote) = registration(fixture, route, rooted).await?;
    let initial = fixture.runtimes[0]
        .status(route.history)
        .await?
        .applied
        .index;
    let gate = remote.hold_publication();
    let controllers = controller::start_with_limits(fixture, controller::limits()).await?;
    let call = controllers[0].archive(config.clone())?;
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified()).await?;
    // A held archive publication does not stop real quorum/apply progress.
    let later = add(fixture, route, rooted, 1).await?;
    assert!(later > initial);
    assert!(controllers[0]
        .archive_status(route.history)?
        .unwrap()
        .published
        .is_none());
    gate.release.notify_one();
    call.response().await?;
    published(&controllers[0], route.history, later).await?;
    assert!(remote
        .publication_cuts
        .lock()
        .unwrap()
        .contains(&initial.get()));
    for controller in &controllers {
        controller.close().await?;
    }
    assert!(
        controllers[0]
            .archive_status(route.history)?
            .unwrap()
            .stopped
    );
    audit(fixture, &config, rooted, 1).await?;
    drop(config);
    Ok(())
}

pub(crate) async fn archive_controller_reconcile(rooted: bool) -> Result<()> {
    let (mut fixture, route) = setup(rooted).await?;
    reconcile(&mut fixture, &route, rooted).await?;
    fixture.close().await
}

pub(crate) async fn archive_controller_reconcile_store(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let (mut fixture, route) = setup_store(rooted, store).await?;
    reconcile(&mut fixture, &route, rooted).await?;
    fixture.close().await
}

async fn reconcile(fixture: &mut RuntimeCluster, route: &GroupRoute, rooted: bool) -> Result<()> {
    let (config, remote) = registration(fixture, route, rooted).await?;
    let initial = fixture.runtimes[0]
        .status(route.history)
        .await?
        .applied
        .index;
    remote
        .lose_publication_and_reads
        .store(true, Ordering::Release);
    let controllers = controller::start_with_limits(fixture, controller::limits()).await?;
    // Cancelling a registration response does not cancel its admitted owner.
    drop(controllers[0].archive(config.clone())?);
    controller::wait_for(async || Ok(remote.outage.load(Ordering::Acquire))).await?;
    let mut latest = initial;
    for n in 1..=4 {
        latest = add(fixture, route, rooted, n).await?;
    }
    let status = controllers[0].archive_status(route.history)?.unwrap();
    assert!(status.published.is_none());
    assert!(status.error.is_some());
    // Reconciliation must recover the exact earlier publication without a new
    // allocation/CAS. Hold the first subsequent registry write to inspect that.
    let gate = remote.hold_publication();
    remote.outage.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified()).await?;
    assert_eq!(
        controllers[0]
            .archive_status(route.history)?
            .unwrap()
            .published
            .unwrap()
            .position()
            .index,
        initial
    );
    let mut closing = Box::pin(controllers[0].close());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), closing.as_mut())
            .await
            .is_err()
    );
    gate.release.notify_one();
    closing.await?;
    // Closing joins the already admitted archive job, including its new cut.
    let selected = controllers[0].archive_status(route.history)?.unwrap();
    assert!(selected.published.unwrap().position().index >= latest);
    for controller in controllers.iter().skip(1) {
        controller.close().await?;
    }
    audit(fixture, &config, rooted, 4).await?;
    let objects = remote.list(None).try_collect::<Vec<_>>().await?;
    let key = objects
        .iter()
        .find(|o| o.location.as_ref().ends_with("/ownership/registry"))
        .context("registry")?;
    let bytes = remote.get(&key.location).await?.bytes().await?;
    let registry: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(registry["holds"].as_array().context("holds")?.len(), 1);
    drop(config);
    Ok(())
}

/// A configured archive blocks local compaction, not quorum apply or a sibling's
/// checkpoint lane. Only actual complete coverage can reopen that boundary.
pub(crate) async fn archive_checkpoint_frontier(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    use raft::Storage as _;
    let (mut fixture, route) = setup_store(rooted, store).await?;
    let second = controller::add_second(&mut fixture, route.history).await?;
    fixture.connect_tcp().await?;
    let pools: Vec<_> = (0..3)
        .map(|_| CellGroupCheckpointPool::new(checkpoint_fixture::limits()))
        .collect::<std::result::Result<_, _>>()?;
    for (transport, pool) in fixture.transports.iter().zip(&pools) {
        transport.enable_group_checkpoints(pool.clone())?;
    }
    let (config, remote) = registration(&fixture, &route, rooted).await?;
    let publication = remote.hold_publication();
    let mut limits = controller::limits();
    limits.checkpoints = Some(CellGroupCheckpointPolicy {
        interval_entries: 1,
        max_parallel: 1,
    });
    let controllers = controller::start_with_limits(&mut fixture, limits).await?;
    fixture.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    let before = group.checkpoint_manifest()?;
    let first = group.first_index()?;
    let call = controllers[0].archive(config.clone())?;
    tokio::time::timeout(Duration::from_secs(10), publication.entered.notified())
        .await
        .with_context(|| {
            format!(
                "archive publication did not enter (rooted={rooted}): {:?}",
                controllers[0].archive_status(route.history).map(|status| {
                    status.map(|status| {
                        (
                            status.attempts,
                            status.published,
                            status.error,
                            status.stopped,
                        )
                    })
                })
            )
        })?;
    let later = add(&fixture, &route, rooted, 1).await?;
    assert_eq!(group.checkpoint_manifest()?, before);
    assert_eq!(group.first_index()?, first);
    assert!(controllers[0]
        .archive_status(route.history)?
        .unwrap()
        .published
        .is_none());
    let sibling = fixture.cluster.voters[0].log.cell_group(&second)?;
    let sibling_route = sibling
        .group_manifest()?
        .context("sibling")?
        .route()
        .clone();
    add(&fixture, &sibling_route, false, 1).await?;
    let sibling_base = sibling.group_manifest()?.context("sibling base")?;
    add(&fixture, &sibling_route, false, 2).await?;
    controller::wait_for(async || {
        Ok(sibling
            .checkpoint_manifest()?
            .as_ref()
            .is_some_and(|cut| cut.position().index >= sibling_base.position().index))
    })
    .await?;
    assert_eq!(group.checkpoint_manifest()?, before);
    assert_eq!(controllers[0].usage().quarantined_groups, 0);
    assert_eq!(pools[0].usage().sources, 0);
    let marker = group.group_manifest()?.context("held archive cut")?;
    let source = fixture.runtimes[0]
        .capture_checkpoint(route.history, &pools[0])
        .await?;
    let prepared = fixture
        .cluster
        .domain
        .blocking(move || source.prepare_publication())?
        .await??;
    assert_eq!(prepared.marker(), &marker);
    controller::wait_for(async || {
        match fixture.runtimes[0]
            .publish_checkpoint(route.history, prepared.clone())
            .await
        {
            Err(RuntimeError::Overloaded) => Ok(true),
            Err(RuntimeError::NotReady) => Ok(false),
            other => anyhow::bail!("unverified checkpoint admitted: {other:?}"),
        }
    })
    .await?;
    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::CellGroupCheckpoint(prepared.clone()));
    assert!(matches!(
        fixture.cluster.voters[0]
            .log
            .persist_cells(OperationId::new(911), vec![write])
            .await,
        Err(LogError::Overloaded)
    ));
    assert_eq!(group.checkpoint_manifest()?, before);
    assert_eq!(group.first_index()?, first);

    publication.release.notify_one();
    call.response().await?;
    published(&controllers[0], route.history, later).await?;
    fixture.runtimes[0]
        .publish_checkpoint(route.history, prepared)
        .await?;
    controller::wait_for(async || Ok(group.checkpoint_manifest()?.as_ref() == Some(&marker)))
        .await?;
    assert_eq!(group.first_index()?, later.get() + 1);
    remote.outage.store(true, Ordering::Release);
    add(&fixture, &route, rooted, 2).await?;
    let last = add(&fixture, &route, rooted, 3).await?;
    assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&marker));
    assert_eq!(group.first_index()?, later.get() + 1);
    assert!(group.last_index()? >= last.get());
    assert_eq!(controllers[0].usage().quarantined_groups, 0);
    remote.outage.store(false, Ordering::Release);
    published(&controllers[0], route.history, last).await?;
    audit(&fixture, &config, rooted, 3).await?;
    for controller in &controllers {
        controller.close().await?;
    }
    assert_eq!(config.tails.usage().operations, 0);
    fixture.close().await
}
