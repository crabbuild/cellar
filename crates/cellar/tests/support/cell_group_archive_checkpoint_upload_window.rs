// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bound actual immutable creates, their confirmation and the physical producer.

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

fn paths(prefix: &str, encoded: &serde_json::Value, generation: u64) -> Result<Vec<Path>> {
    encoded["chunks"]
        .as_array()
        .context("checkpoint chunks")?
        .iter()
        .take(4)
        .map(|chunk| {
            let object = &chunk["object"];
            let digest: Vec<u8> = serde_json::from_value(object["digest"].clone())?;
            Ok(Path::from(format!(
                "{prefix}/generations/{generation}/chunks/{}-{}",
                object["length"],
                hex::encode(digest)
            )))
        })
        .collect()
}

pub(super) async fn qualify(rooted: bool, backend: Arc<dyn ObjectStore>) -> Result<()> {
    let (mut cluster, expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 2, 131072).await?;
    retain_archive_fixture_disks(&mut cluster);
    let store = archive_store::Store::around(backend);
    let first = new_archive(
        &cluster,
        store.clone(),
        Path::from("checkpoint-create-window"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let base = first
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    let (index_path, encoded) = index(store.as_ref(), &base).await?;
    let (prefix, _) = generation_prefix(&index_path)?;
    ensure!(encoded["chunks"].as_array().context("chunks")?.len() > 4);
    first
        .registry()
        .abort(&cluster.domain, &first.ticket())
        .await?;

    // A new protected generation has no uploaded-object cache. Its paths come
    // from a real independently encoded cut, not synthetic chunk payloads.
    let upload = start_upload(&cluster, &first, "checkpoint-create-window").await?;
    ensure!(upload.ticket().generation() > base.generation());
    let objects = paths(prefix, &encoded, upload.ticket().generation())?;
    ensure!(
        objects.len() == 4
            && objects
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 4
    );
    let hold = store.hold_chunk_creates(objects.clone());
    store
        .lose_immutable_put_response
        .store(true, Ordering::Release);
    let mut pending =
        Box::pin(upload.upload_checkpoint(&cluster.domain, source(&sources, &cluster)?));
    cellar::asyncrt::select! {
        result = &mut pending => {
            result?;
            anyhow::bail!("checkpoint upload ended before overlapping creates");
        }
        result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => {
            result.context("checkpoint uploader serialized independent chunk creates")?;
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
    assert_eq!(upload.usage().operations, 1);
    assert_eq!(sources.usage().sources, 1);
    let charged = upload.usage().retained_bytes;
    assert!(charged > 0 && charged <= CellGroupArchiveLimits::default().max_retained_bytes);
    for n in 0..objects.len() {
        hold.0.release(n);
    }
    let reference = pending.await?;
    assert_eq!(hold.0.started.load(Ordering::Acquire), 15);
    assert!(!store.lose_immutable_put_response.load(Ordering::Acquire));
    assert_eq!(upload.usage().operations, 0);
    assert_eq!(sources.usage().sources, 0);
    drop(hold);
    let applier = destination(&cluster).await?;
    let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let group = cluster.voters[2]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let selected = group.group_manifest()?;
    let verified = upload
        .verify_checkpoint(
            &cluster.domain,
            &reference,
            receiver(&receivers, &applier, &cluster)?,
        )
        .await?;
    tail_fixture::check_images(&cluster, verified.checkpoint().manifest(), &expected).await?;
    assert_eq!(group.group_manifest()?, selected);
    drop(verified);
    assert_eq!(receivers.usage().sources, 0);
    upload
        .registry()
        .abort(&cluster.domain, &upload.ticket())
        .await?;

    // A genuine create refusal has no confirmation object. Stop admission,
    // drain the entered peer, then release the blocked physical producer.
    let upload = start_upload(&cluster, &upload, "checkpoint-create-window").await?;
    let generation = upload.ticket().generation();
    let objects = paths(prefix, &encoded, generation)?;
    let hold = store.hold_chunk_creates(objects);
    store.deny_chunk_creates.store(true, Ordering::Release);
    let mut pending =
        Box::pin(upload.upload_checkpoint(&cluster.domain, source(&sources, &cluster)?));
    cellar::asyncrt::select! {
        result = &mut pending => {
            anyhow::bail!("checkpoint upload ended before both creates entered: {}", result.is_ok());
        }
        result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => result?,
    }
    for n in 2..4 {
        hold.0.release(n);
    }
    hold.0.release(0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(hold.0.started.load(Ordering::Acquire), 3);
    assert_eq!(upload.usage().operations, 1);
    assert_eq!(upload.usage().retained_bytes, charged);
    assert_eq!(sources.usage().sources, 1);
    hold.0.release(1);
    assert!(matches!(
        pending.await,
        Err(CellGroupArchiveError::Storage(
            object_store::Error::Generic {
                store: "archive chunk creates denied",
                ..
            }
        ))
    ));
    assert_eq!(hold.0.started.load(Ordering::Acquire), 3);
    assert_eq!(upload.usage().operations, 0);
    assert_eq!(sources.usage().sources, 0);
    assert_eq!(group.group_manifest()?, selected);
    let unselected: Vec<_> = store
        .list(Some(&Path::from(format!(
            "{prefix}/generations/{generation}/indexes",
        ))))
        .try_collect()
        .await?;
    ensure!(unselected.is_empty(), "failed upload emitted an index");
    drop(hold);
    store.deny_chunk_creates.store(false, Ordering::Release);
    upload
        .registry()
        .abort(&cluster.domain, &upload.ticket())
        .await?;

    // Cancel after two creates were accepted, but before either GET can
    // confirm them. Join the physical producer and retry the same protected
    // generation; conditional-create conflicts must reconcile exact bytes.
    let upload = start_upload(&cluster, &upload, "checkpoint-create-window").await?;
    let objects = paths(prefix, &encoded, upload.ticket().generation())?;
    let hold = store.hold_object_reads(objects.clone());
    let mut pending =
        Box::pin(upload.upload_checkpoint(&cluster.domain, source(&sources, &cluster)?));
    cellar::asyncrt::select! {
        result = &mut pending => {
            anyhow::bail!("checkpoint upload ended before confirmation gates: {}", result.is_ok());
        }
        result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => result?,
    }
    for object in &objects[..2] {
        ensure!(store.inner.head(object).await?.size > 0);
    }
    assert_eq!(sources.usage().sources, 1);
    drop(pending);
    tokio::time::timeout(Duration::from_secs(5), async {
        while upload.usage().operations != 0 || sources.usage().sources != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let unselected: Vec<_> = store
        .list(Some(&Path::from(format!(
            "{prefix}/generations/{}/indexes",
            upload.ticket().generation(),
        ))))
        .try_collect()
        .await?;
    ensure!(unselected.is_empty(), "cancelled upload emitted an index");
    assert_eq!(group.group_manifest()?, selected);
    drop(hold);
    let conflicts = store.immutable_create_conflicts.load(Ordering::Acquire);
    let reference = upload
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    assert_eq!(reference.generation(), upload.ticket().generation());
    ensure!(store.immutable_create_conflicts.load(Ordering::Acquire) >= conflicts + 2);
    let verified = upload
        .verify_checkpoint(
            &cluster.domain,
            &reference,
            receiver(&receivers, &applier, &cluster)?,
        )
        .await?;
    tail_fixture::check_images(&cluster, verified.checkpoint().manifest(), &expected).await?;
    drop(verified);
    assert_eq!(sources.usage().sources, 0);
    assert_eq!(receivers.usage().sources, 0);
    assert_eq!(upload.usage().operations, 0);
    assert_eq!(group.group_manifest()?, selected);
    upload
        .registry()
        .abort(&cluster.domain, &upload.ticket())
        .await?;
    cluster.close().await?;
    Ok(())
}
