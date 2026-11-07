// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete engine inventory over two groups sharing the same physical images.
use super::*;
use cellar_ltx::internal::{ExtentId, ExtentManifest, Geometry, ImageBinding, EXTENT_BYTES};
use std::path::PathBuf;

pub(super) fn limits() -> CellGroupReclaimLimits {
    CellGroupReclaimLimits {
        max_selected_cells: 64,
        max_files: 1,
        max_page_scan: 64,
    }
}

fn appliers(f: &RuntimeCluster) -> Vec<CellGroupApplier> {
    f.appliers
        .iter()
        .filter(|((node, _), _)| *node == 0)
        .map(|(_, (_, a))| a.clone())
        .collect()
}

async fn images_for(
    f: &RuntimeCluster,
    cell: &CellHistory,
    node: usize,
) -> Result<CellGroupApplyImages> {
    Ok(match f.cell_options_at(cell, node).await? {
        CellSqlOptions::Materialized(o) => CellGroupApplyImages::Materialized {
            images: o.images,
            services: o.query_services,
        },
        CellSqlOptions::Extents(o) => CellGroupApplyImages::Extents {
            images: o.images,
            services: o.query_services,
            limits: Default::default(),
            cache_directory: o.cache_directory,
            cache_pool: o.cache_pool,
            cache: o.cache,
        },
    })
}

async fn enroll(f: &RuntimeCluster, group: GroupHistory, name: &str, rooted: bool) -> Result<()> {
    let selected = f.cluster.voters[0]
        .log
        .cell_group(&group)?
        .group_manifest()?
        .context("group")?;
    let cell = history(selected.route(), name)?;
    let bytes = images::image(
        None,
        "CREATE TABLE value(n); INSERT INTO value VALUES(113);",
    )?;
    let accepted = f.runtimes[0]
        .propose(
            group,
            apply::enrollment_bytes(selected.route(), &cell, &bytes, rooted)?,
        )
        .await?;
    controller::applied(f, group, accepted.position.index).await
}

fn physical_image(
    images: &CellGroupApplyImages,
    base: PathBuf,
    cell: CellHistory,
    number: u8,
) -> Result<(CellImagePin, PathBuf)> {
    let bytes = images::image(
        None,
        &format!("CREATE TABLE value(n); INSERT INTO value VALUES({number});"),
    )?;
    let state = images::Cut::new(&[], &bytes, 0, true, false).result;
    match images {
        CellGroupApplyImages::Materialized { images, .. } => {
            let source = base.join(format!("private-{number}.sqlite"));
            std::fs::write(&source, &bytes)?;
            let id = ImageId::new([number; 16])?;
            let pin = images.prepare_image_file(id, &source, state)?;
            images.retire(id)?;
            Ok((
                CellImagePin::Materialized(pin),
                base.join("node-1/images")
                    .join(format!("{}.sqlite", hex::encode(id.bytes()))),
            ))
        }
        CellGroupApplyImages::Extents { images, .. } => {
            let ids = bytes
                .chunks(EXTENT_BYTES as usize)
                .map(ExtentId::of)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let manifest = ExtentManifest::new(
                ImageBinding {
                    history: cell_image_history_digest(&cell),
                    schema: [81; 32],
                    epoch: state.epoch,
                    txid: state.txid.0,
                    checksum: state.checksum,
                },
                Geometry::new(state.page_size, state.length)?,
                &ids,
            )?;
            let view = images.stage_transient_root(manifest.clone())?;
            for (id, chunk) in ids.iter().zip(bytes.chunks(EXTENT_BYTES as usize)) {
                images.put_extent_bytes(*id, chunk)?;
            }
            let pin = images.seal_image_root(manifest, &mut vec![0; EXTENT_BYTES as usize])?;
            drop(view);
            let id = ids[0];
            Ok((
                CellImagePin::Extents(Arc::new(pin)),
                base.join("rooted-0").join(format!(
                    "e-{:08x}-{}",
                    id.length(),
                    hex::encode(id.bytes())
                )),
            ))
        }
    }
}

fn read(pin: &CellImagePin) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    match pin {
        CellImagePin::Materialized(pin) => {
            let mut r = pin.open_reader()?;
            let mut offset = 0;
            while offset < r.length() {
                let count = (r.length() - offset).min(65536) as usize;
                hash.update(r.read_exact_at(offset, count)?);
                offset += count as u64;
            }
        }
        CellImagePin::Extents(pin) => {
            for n in 0..pin.manifest().geometry().extent_count() {
                hash.update(
                    pin.view()
                        .read_extent(pin.manifest().extent(n).context("extent")?)?,
                );
            }
        }
        CellImagePin::Empty => (),
    }
    Ok(hash.finalize().into())
}

pub(crate) async fn shared_inventory(rooted: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut f, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let second = f.add_group(31).await?;
    let selected_images = images_for(&f, &a, 0).await?;
    for node in 0..3 {
        let images = images_for(&f, &a, node).await?;
        let pool = f.appliers[&(node, route.history)].0.clone();
        let voter = &mut f.extra.get_mut(&second).context("second")?[node];
        let applier = CellGroupApplier::new(
            voter.log.cell_group(&second)?,
            voter.pages.take().context("page store")?,
            images,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        f.appliers.insert((node, second), (pool, applier));
    }
    let mut controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
    f.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(f.runtimes[0].status(second).await?.ready)).await?;
    enroll(&f, second, "other-group", rooted).await?;
    let mut selected = Vec::new();
    for group in [route.history, second] {
        let applier = f.appliers[&(0, group)].1.clone();
        let log = f.cluster.voters[0].log.cell_group(&group)?;
        let (root, checkpoint) = f
            .cluster
            .domain
            .blocking(move || -> Result<_> {
                let root = applier.recover_selected()?;
                let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
                let checkpoint = pool.capture(&log, &root)?.prepare_publication()?;
                Ok((root, checkpoint))
            })?
            .await??;
        let marker = root.marker().clone();
        f.runtimes[0].publish_checkpoint(group, checkpoint).await?;
        controller::wait_for(async || {
            Ok(f.cluster.voters[0]
                .log
                .cell_group(&group)?
                .checkpoint_manifest()?
                .as_ref()
                == Some(&marker))
        })
        .await?;
        selected.push(root.marker().clone());
        drop(root);
    }
    enroll(&f, route.history, "after-checkpoint", rooted).await?;
    enroll(&f, second, "other-after-checkpoint", rooted).await?;
    let images = selected_images.clone();
    let base = f.cluster._root.path().to_path_buf();
    let cell = history(&route, "private-reader")?;
    let (reader, reader_path, orphan_path) = f
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let (reader, path) = physical_image(&images, base.clone(), cell.clone(), 211)?;
            let (orphan, orphan_path) = physical_image(&images, base, cell, 212)?;
            drop(orphan);
            Ok((reader, path, orphan_path))
        })?
        .await??;
    let expected = read(&reader)?;
    let selected_appliers = appliers(&f);
    let log = &f.cluster.voters[0].log;
    assert!(matches!(
        log.reclaim_cell_storage(selected_appliers.clone(), vec![], limits())
            .await,
        Err(CellGroupReclaimError::Busy)
    ));
    controllers.remove(0).close().await?;
    f.appliers.retain(|(node, _), _| *node != 0);
    assert!(matches!(
        log.reclaim_cell_storage(vec![selected_appliers.clone()[0].clone()], vec![], limits())
            .await,
        Err(CellGroupReclaimError::Invalid(_))
    ));
    assert!(orphan_path.exists());
    assert!(matches!(
        log.reclaim_cell_storage(
            vec![selected_appliers.clone()[0].clone(); 2],
            vec![],
            limits()
        )
        .await,
        Err(CellGroupReclaimError::Invalid(_))
    ));
    let foreign = f.appliers[&(1, route.history)].1.clone();
    assert!(matches!(
        log.reclaim_cell_storage(
            vec![foreign, selected_appliers.clone()[1].clone()],
            vec![],
            limits()
        )
        .await,
        Err(CellGroupReclaimError::Invalid(_))
    ));
    let mut small = limits();
    small.max_selected_cells = 1;
    assert!(matches!(
        log.reclaim_cell_storage(selected_appliers.clone(), vec![], small)
            .await,
        Err(CellGroupReclaimError::Limit)
    ));
    assert!(orphan_path.exists());
    let mut deleted = 0;
    for _ in 0..32 {
        let report = log
            .reclaim_cell_storage(selected_appliers.clone(), vec![], limits())
            .await?;
        assert_eq!(report.groups, 2);
        assert_eq!(report.selected_roots, 4);
        assert!(report.page_files + report.image_files <= 1);
        assert!(report.pages_scanned <= 64);
        deleted += report.image_files;
        assert_eq!(read(&reader)?, expected);
        if !orphan_path.exists() {
            break;
        }
    }
    assert!(deleted > 0 && !orphan_path.exists());
    assert!(reader_path.exists());
    for marker in &selected {
        let applier = selected_appliers
            .iter()
            .find(|a| a.history() == marker.route().history)
            .context("group applier")?
            .clone();
        let current = f
            .cluster
            .domain
            .blocking(move || applier.recover_selected())?
            .await??;
        for name in if marker.route().history == route.history {
            vec!["registered-a", "registered-b"]
        } else {
            vec!["other-group"]
        } {
            let (_, pin) = current
                .pin_cell(&CellId::new(name)?)
                .context("retained cell")?;
            read(&pin)?;
        }
    }
    drop(reader);
    for _ in 0..32 {
        log.reclaim_cell_storage(selected_appliers.clone(), vec![], limits())
            .await?;
        if !reader_path.exists() {
            break;
        }
    }
    assert!(!reader_path.exists());
    for (index, group) in [route.history, second].into_iter().enumerate() {
        assert_eq!(
            log.cell_group(&group)?.checkpoint_manifest()?.as_ref(),
            Some(&selected[index])
        );
        let applier = selected_appliers
            .iter()
            .find(|a| a.history() == group)
            .context("group applier")?
            .clone();
        let current = f
            .cluster
            .domain
            .blocking(move || applier.recover_selected())?
            .await??;
        assert!(current.marker().position().index > selected[index].position().index);
    }
    // Queue maintenance behind a real engine fence, then lose its waiter.
    // Neither cancellation nor a cancelled close may refund physical ownership.
    let image_store = selected_images.clone();
    let base = f.cluster._root.path().to_path_buf();
    let cell = history(&route, "cancelled-orphan")?;
    let orphan = f
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let (pin, path) = physical_image(&image_store, base, cell, 213)?;
            drop(pin);
            Ok(path)
        })?
        .await??;
    let gate = f.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let writer = (*log).clone();
    let ahead = f
        .cluster
        .domain
        .spawn(async move { writer.reserve_node_revision().await })?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let writer = (*log).clone();
    let owned = selected_appliers.clone();
    let pending = f.cluster.domain.spawn(async move {
        writer
            .reclaim_cell_storage(
                owned,
                vec![],
                CellGroupReclaimLimits {
                    max_files: 4096,
                    max_page_scan: 4096,
                    ..limits()
                },
            )
            .await
    })?;
    controller::wait_for(async || {
        let sample = log.performance_snapshot();
        assert!(sample.available && !sample.saturated);
        let queue = sample
            .operations
            .iter()
            .find(|o| o.name == "engine_queue")
            .context("engine queue")?;
        Ok(queue.started - queue.succeeded - queue.failed - queue.abandoned == 1)
    })
    .await?;
    pending.abort();
    assert!(pending.await.is_err());
    assert!(matches!(
        CellGroupRuntime::start((*log).clone(), RaftRuntimeOptions::default()).await,
        Err(RuntimeError::Storage(LogError::AlreadyExists))
    ));
    assert!(apply::pool(&f, 0).is_err());
    assert!(tokio::time::timeout(Duration::from_millis(30), log.close())
        .await
        .is_err());
    assert!(orphan.exists());
    drop(release);
    ahead.await??;
    tokio::time::timeout(Duration::from_secs(5), log.close()).await??;
    assert!(
        !orphan.exists(),
        "queued collection must finish after waiter cancellation"
    );
    for c in controllers {
        c.close().await?;
    }
    drop((selected, selected_appliers));
    f.close().await
}
