// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Verify real checkpoint bytes with a bounded, ordered download window.

use super::*;
use anyhow::ensure;
use std::sync::atomic::Ordering;

async fn entered(gate: &archive_store::ObjectIoGate) {
    loop {
        let changed = gate.entered.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if gate.started.load(Ordering::Acquire) & 3 == 3 {
            return;
        }
        changed.await;
    }
}

pub(super) async fn qualify(rooted: bool, backend: Arc<dyn ObjectStore>) -> Result<()> {
    let (mut cluster, expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 2, 8192).await?;
    retain_archive_fixture_disks(&mut cluster);
    let store = archive_store::Store::around(backend);
    let archive = new_archive(
        &cluster,
        store.clone(),
        Path::from("checkpoint-read-window"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let reference = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    let (index_path, encoded) = index(store.as_ref(), &reference).await?;
    let (prefix, _) = generation_prefix(&index_path)?;
    let paths = encoded["chunks"]
        .as_array()
        .context("checkpoint chunks")?
        .iter()
        .take(4)
        .map(|chunk| {
            let object = &chunk["object"];
            let digest: Vec<u8> = serde_json::from_value(object["digest"].clone())?;
            Ok(Path::from(format!(
                "{prefix}/generations/{}/chunks/{}-{}",
                object["generation"],
                object["length"],
                hex::encode(digest)
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(paths.len() == 4, "expected four real checkpoint chunks");
    ensure!(
        paths
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == 4,
        "checkpoint gate requires distinct object reads"
    );
    let applier = destination(&cluster).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let receiver_group = cluster.voters[2]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let selected = receiver_group.group_manifest()?;

    // Hold both headers. A completed second download must wait for the first
    // before physical append, and must not open a third download slot.
    let hold = store.hold_object_reads(paths.clone());
    let mut pending = Box::pin(archive.verify_checkpoint(
        &cluster.domain,
        &reference,
        receiver(&pool, &applier, &cluster)?,
    ));
    cellar::asyncrt::select! {
        result = &mut pending => {
            result?;
            anyhow::bail!("checkpoint verification ended before overlapping reads");
        }
        result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => {
            result.context("checkpoint verifier serialized independent chunk reads")?;
        }
    }
    assert_eq!(hold.0.started.load(Ordering::Acquire), 3);
    hold.0.release(1);
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(hold.0.started.load(Ordering::Acquire), 3);
    assert_eq!(archive.usage().operations, 1);
    assert_eq!(pool.usage().sources, 1);
    let charged = archive.usage().retained_bytes;
    assert!(charged > 0 && charged <= CellGroupArchiveLimits::default().max_retained_bytes);
    for n in 0..paths.len() {
        hold.0.release(n);
    }
    let recovered = pending.await?;
    assert_eq!(hold.0.started.load(Ordering::Acquire), 15);
    tail_fixture::check_images(&cluster, recovered.checkpoint().manifest(), &expected).await?;
    assert_eq!(receiver_group.group_manifest()?, selected);
    drop(recovered);
    drop(hold);
    assert_eq!(archive.usage().operations, 0);
    assert_eq!(pool.usage().sources, 0);

    // Keep the original locator and length but corrupt actual remote bytes.
    // Do not refill after failure or release admission before the entered peer
    // and physical receiver finish. Later reads are deliberately allowed.
    let original = store.inner.get(&paths[0]).await?.bytes().await?;
    let mut corrupt = original.to_vec();
    *corrupt.last_mut().context("empty checkpoint chunk")? ^= 1;
    store.inner.put(&paths[0], corrupt.into()).await?;
    let hold = store.hold_object_reads(paths.clone());
    let mut pending = Box::pin(archive.verify_checkpoint(
        &cluster.domain,
        &reference,
        receiver(&pool, &applier, &cluster)?,
    ));
    cellar::asyncrt::select! {
        result = &mut pending => {
            anyhow::bail!("checkpoint verification ended before both reads entered: {}", result.is_ok());
        }
        result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => result?,
    }
    for n in 2..paths.len() {
        hold.0.release(n);
    }
    hold.0.release(0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(hold.0.started.load(Ordering::Acquire), 3);
    assert_eq!(archive.usage().operations, 1);
    assert_eq!(archive.usage().retained_bytes, charged);
    assert_eq!(pool.usage().sources, 1);
    hold.0.release(1);
    assert!(matches!(
        pending.await,
        Err(CellGroupArchiveError::Invalid("object digest/length"))
    ));
    assert_eq!(hold.0.started.load(Ordering::Acquire), 3);
    assert_eq!(archive.usage().operations, 0);
    assert_eq!(pool.usage().sources, 0);
    assert_eq!(receiver_group.group_manifest()?, selected);
    drop(hold);
    store.inner.put(&paths[0], original.into()).await?;
    cluster.close().await?;
    Ok(())
}
