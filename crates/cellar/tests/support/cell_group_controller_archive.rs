// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real three-voter controllers, conditional object-store I/O and private replay.
use super::*;
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use std::sync::atomic::Ordering;

#[path = "cell_group_archive_store.rs"]
#[allow(dead_code)]
mod archive_store;

async fn verifier(
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

async fn registration(
    fixture: &RuntimeCluster,
    route: &GroupRoute,
    rooted: bool,
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
    let namespace = fixture
        .cluster
        .domain
        .run(catalog.existing_namespace(NamespaceConfig::new(
            route.history.cluster,
            route.history.namespace,
            cellar_logic::NamespaceMode::Raft,
            route.schema_digest,
        )))
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

async fn published(
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

async fn audit(
    fixture: &RuntimeCluster,
    config: &CellGroupArchiveRegistration,
    rooted: bool,
    last: u8,
) -> Result<()> {
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
    let destination = verifier(fixture, route, rooted, "controller-archive-audit").await?;
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
