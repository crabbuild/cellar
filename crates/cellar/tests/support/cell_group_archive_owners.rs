// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cancellation leaves the actual archive file reader owning its old closure.
use super::*;

fn collect(cluster: &mut Cluster) -> Result<()> {
    for voter in &mut cluster.voters {
        let pages = voter.pages.as_mut().context("physical page owner")?;
        pages.reconcile_retained(&[])?;
        pages.reclaim(1024)?;
        voter.images.retain_only(&[])?;
        voter.images.reclaim(1024)?;
        if let Some(extents) = &voter.extents {
            extents.retain_only(&[])?;
            extents.reclaim(1024)?;
        }
    }
    Ok(())
}

pub(super) async fn cancelled_checkpoint_read(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    cancelled_checkpoint_read_with_start_delay(rooted, store, Duration::ZERO).await
}

pub(super) async fn cancelled_checkpoint_read_with_start_delay(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    delay: Duration,
) -> Result<()> {
    let gate = fault::Gate::new(None);
    let filesystem = Arc::new(fault::FaultFileSystem { gate: gate.clone() });
    // Keep the real disk owner outside the consumed cluster until shutdown
    // joins; failures retain it rather than losing the physical cut.
    let placeholder = tempfile::tempdir()?;
    let mut cluster = Cluster::with_storage(
        RaftRuntimeOptions::default(),
        tempfile::tempdir()?,
        filesystem.clone(),
    )
    .await?;
    let result = exercise(&mut cluster, rooted, store, &gate, filesystem, delay).await;
    let root = std::mem::replace(&mut cluster._root, placeholder);
    let joined = cluster.close().await;
    if result.is_err() || joined.is_err() {
        eprintln!(
            "retained failed archive-owner disk: {}",
            root.keep().display()
        );
    }
    result
        .with_context(|| format!("archive physical owner, extents={rooted}; joined={joined:?}"))?;
    joined
}

async fn exercise(
    cluster: &mut Cluster,
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    gate: &Arc<fault::Gate>,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
    delay: Duration,
) -> Result<()> {
    cluster.elect().await?;
    // Unique values prevent another cell from retaining the selected old extent.
    let expected = checkpoint_fixture::enroll_images_with_filesystem_and_payload(
        cluster,
        rooted,
        false,
        1,
        128 * 1024,
        filesystem,
        None,
    )
    .await?;
    let root = cluster.voters[0]
        .owner
        .as_ref()
        .context("source owner")?
        .manifest();
    let marker = root.marker().clone();
    let (_, pin) = root
        .pin_cell(&CellId::new("alpha")?)
        .context("source image")?;
    let image = match &pin {
        CellImagePin::Materialized(pin) => cluster
            ._root
            .path()
            .join("node-1/images")
            .join(format!("{}.sqlite", hex::encode(pin.id().bytes()))),
        CellImagePin::Extents(pin) => {
            let extent = pin.manifest().extent(0).context("first source extent")?;
            cluster._root.path().join("extents-0").join(format!(
                "e-{:08x}-{}",
                extent.length(),
                hex::encode(extent.bytes())
            ))
        }
        CellImagePin::Empty => anyhow::bail!("populated source image"),
    };
    drop(pin);
    let pages = cluster._root.path().join("node-1/pages");
    let old_page = marker.root().digest().context("populated source root")?;
    let old_page_bytes = manifest_pages::read(&pages, old_page)?;
    let old_image_bytes = std::fs::read(&image)?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let captured = source(&sources, cluster)?;
    let source_usage = sources.usage();
    let archive = new_archive(
        cluster,
        store.clone(),
        Path::from("physical-read-owner"),
        CellGroupArchiveLimits {
            max_operations: 1,
            ..Default::default()
        },
    )
    .await?;
    // Select the immutable image, rather than an earlier manifest/header read.
    *gate.image_read_paths.lock().unwrap() = vec![image.clone()];
    let release = gate.arm(if rooted {
        fault::HOLD_EXTENT_READ
    } else {
        fault::HOLD_IMAGE_READ
    });
    let mut pending = Box::pin(async {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        archive.upload_checkpoint(&cluster.domain, captured).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    // The upload can still await earlier metadata I/O after the first poll
    // window. Keep driving this same operation until its actual file read is
    // held; waiting only for the notification would park the producer itself.
    tokio::time::timeout(Duration::from_secs(5), async {
        cellar::asyncrt::select_biased! {
            "observe the held physical read before treating upload completion as an early exit";
            _ = gate.entered.notified() => Ok::<_, anyhow::Error>(()),
            result = &mut pending => anyhow::bail!(
                "checkpoint upload ended before the selected physical read; success={}",
                result.is_ok()
            ),
        }
    })
    .await
    .context("archive physical read did not start under the original deadline")??;
    let archive_usage = archive.usage();
    assert_eq!(archive_usage.operations, 1);
    assert!(archive_usage.retained_bytes > 0 && source_usage.retained_bytes > 0);
    drop(pending);
    assert_eq!(
        sources.usage(),
        source_usage,
        "physical reader retains source admission"
    );
    assert_eq!(
        archive.usage(),
        archive_usage,
        "physical reader retains archive admission"
    );
    let refused = source(&sources, cluster)
        .err()
        .context("physical reader must retain the only source slot")?;
    assert!(matches!(
        refused.downcast_ref::<CellGroupCheckpointError>(),
        Some(CellGroupCheckpointError::Busy)
    ));

    // Change every populated cell through the real Raft/apply path. The old
    // source is now retained only by the cancelled upload's physical producer.
    for (name, bytes) in &expected {
        if !bytes.is_empty() {
            checkpoint_fixture::advance_partition(cluster, name, bytes, rooted, None).await?;
        }
    }
    let current = cluster.voters[0]
        .owner
        .as_ref()
        .context("advanced owner")?
        .manifest()
        .marker()
        .clone();
    assert!(current.position().index > marker.position().index);
    assert_ne!(current.root(), marker.root());
    collect(cluster)?;
    let held_page_bytes = manifest_pages::read(&pages, old_page)?;
    let held_image_bytes = std::fs::read(&image)?;
    assert_eq!(held_page_bytes, old_page_bytes);
    assert_eq!(held_image_bytes, old_image_bytes);
    assert_eq!(sources.usage(), source_usage);
    assert_eq!(archive.usage(), archive_usage);

    // Only actual file-read exit may release either reservation or its pins.
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), async {
        while sources.usage().sources != 0 || archive.usage().operations != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("archive physical read did not release its actual ownership")?;
    let released_source_bytes = sources.usage().retained_bytes;
    let released_archive_bytes = archive.usage().retained_bytes;
    assert_eq!(released_source_bytes, 0);
    assert_eq!(released_archive_bytes, 0);
    collect(cluster)?;
    collect(cluster)?;
    assert!(manifest_pages::find(&pages, old_page)?.is_none());
    assert!(!image.exists(), "physical exit permits old image deletion");
    let cancelled_objects = store
        .list(Some(&Path::from("physical-read-owner")))
        .try_collect::<Vec<_>>()
        .await?;
    assert!(
        cancelled_objects
            .iter()
            .all(|object| !object.location.as_ref().contains("/indexes/")),
        "cancelled upload cannot publish a complete index"
    );

    // A fresh upload and independent receiver must still recover the live cut.
    let reference = archive
        .upload_checkpoint(&cluster.domain, source(&sources, cluster)?)
        .await?;
    let destination = destination(cluster).await?;
    let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let recovered = archive
        .verify_checkpoint(
            &cluster.domain,
            &reference,
            receiver(&receivers, &destination, cluster)?,
        )
        .await?;
    assert_eq!(recovered.checkpoint().marker(), &current);
    cellar::note!(
        "ARCHIVE_PHYSICAL_OWNER_EVIDENCE {}",
        serde_json::json!({
            "schema": 1, "case": "archive_cancelled_waiter_keeps_physical_checkpoint",
            "extents": rooted, "history": format!("{:?}", marker.route().history),
            "old_position": marker.position().index.get(), "new_position": current.position().index.get(),
            "old_root": hex::encode(old_page), "new_root": hex::encode(current.root().digest().context("live root")?),
            "old_page_sha256": hex::encode(Sha256::digest(&old_page_bytes)),
            "old_image_sha256": hex::encode(Sha256::digest(&old_image_bytes)),
            "held_page_sha256": hex::encode(Sha256::digest(&held_page_bytes)),
            "held_image_sha256": hex::encode(Sha256::digest(&held_image_bytes)),
            "old_image_bytes": old_image_bytes.len(), "held_source_bytes": source_usage.retained_bytes,
            "held_archive_bytes": archive_usage.retained_bytes, "held_sources": source_usage.sources,
            "held_archive_operations": archive_usage.operations, "cancelled_indexes": 0,
            "released_source_bytes": released_source_bytes, "released_archive_bytes": released_archive_bytes,
            "old_page_removed": manifest_pages::find(&pages, old_page)?.is_none(),
            "old_image_removed": !image.exists(), "fresh_checkpoint_verified": true,
        })
    );
    drop(recovered);
    assert_eq!(receivers.usage().sources, 0);
    assert_eq!(archive.usage().operations, 0);
    Ok(())
}
