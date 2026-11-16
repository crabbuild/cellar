// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the real archive downloader, digest checks and physical replay.

use super::*;
use anyhow::ensure;
use std::sync::atomic::Ordering;

async fn entered(gate: &archive_store::EntryReadGate) {
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
    let (mut cluster, mut expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 2, 8192).await?;
    let store = archive_store::Store::around(backend);
    let archive = new_archive(
        &cluster,
        store.clone(),
        Path::from("tail-read-window"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let base = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    for name in ["alpha", "beta", "cell-000", "cell-001"] {
        checkpoint_fixture::advance_partition(
            &mut cluster,
            name,
            &expected[name],
            rooted,
            Some(2),
        )
        .await?;
        expected.insert(
            name.into(),
            images::image(Some(&expected[name]), "UPDATE value SET n=n+1")?,
        );
    }
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let current = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let reference = archive
        .upload_tail(&cluster.domain, &tails, &base, &group, &current)
        .await?;
    let (index, encoded) = tail_fixture::tail_index(store.as_ref(), &reference).await?;
    let paths = encoded["entries"]
        .as_array()
        .context("tail entries")?
        .iter()
        .map(|entry| tail_fixture::entry_path(&index, entry))
        .collect::<Result<Vec<_>>>()?;
    ensure!(paths.len() == 4, "expected four real committed changes");
    let applier = destination(&cluster).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let receiver_group = cluster.voters[2]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let selected = receiver_group.group_manifest()?;

    // The first two headers are held independently. Completing the second must
    // neither expose a proof nor refill past the ordered two-read window.
    let hold = store.hold_entry_reads(paths.clone());
    let mut pending = Box::pin(archive.verify_tail(
        &cluster.domain,
        &tails,
        &reference,
        receiver(&pool, &applier, &cluster)?,
    ));
    tokio::select! {
        result = &mut pending => {
            result?;
            anyhow::bail!("tail verification ended before overlapping reads");
        }
        result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => {
            result.context("tail verifier serialized independent entry reads")?;
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
    assert_eq!(tails.usage().operations, 1);
    for n in 0..paths.len() {
        hold.0.release(n);
    }
    let recovered = pending.await?;
    assert_eq!(hold.0.started.load(Ordering::Acquire), 15);
    assert_eq!(recovered.manifest().marker(), current.marker());
    tail_fixture::check_images(&cluster, recovered.manifest(), &expected).await?;
    assert_eq!(receiver_group.group_manifest()?, selected);
    drop(recovered);
    drop(hold);
    assert_eq!(tails.usage().operations, 0);
    assert_eq!(pool.usage().sources, 0);

    // Corrupt an actual stored entry, preserving its locator and length. An
    // early digest error must stop new reads and join the already entered peer.
    let original = store.inner.get(&paths[0]).await?.bytes().await?;
    let mut corrupt = original.to_vec();
    *corrupt.last_mut().context("empty tail entry")? ^= 1;
    store.inner.put(&paths[0], corrupt.into()).await?;
    let hold = store.hold_entry_reads(paths.clone());
    let mut pending = Box::pin(archive.verify_tail(
        &cluster.domain,
        &tails,
        &reference,
        receiver(&pool, &applier, &cluster)?,
    ));
    tokio::select! {
        result = &mut pending => {
            anyhow::bail!("tail verification ended before both reads entered: {}", result.is_ok());
        }
        result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => result?,
    }
    // Later reads are permitted by the adapter, so their absence proves the
    // downloader stops admitting work after the first failure.
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
    assert_eq!(tails.usage().operations, 1);
    hold.0.release(1);
    assert!(matches!(
        pending.await,
        Err(CellGroupArchiveError::Invalid("object digest/length"))
    ));
    assert_eq!(hold.0.started.load(Ordering::Acquire), 3);
    assert_eq!(archive.usage().operations, 0);
    assert_eq!(tails.usage().operations, 0);
    assert_eq!(pool.usage().sources, 0);
    assert_eq!(receiver_group.group_manifest()?, selected);
    drop(hold);
    store.inner.put(&paths[0], original.into()).await?;
    cluster.close().await?;
    Ok(())
}
