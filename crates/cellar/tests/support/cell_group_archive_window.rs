// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordered checkpoint object windows retain the existing physical admission.
use super::*;
use futures_util::FutureExt;

pub(super) async fn qualify(rooted: bool, uploading: bool) -> Result<()> {
    let placeholder = tempfile::tempdir()?;
    let (mut cluster, expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
    let work = std::panic::AssertUnwindSafe(async {
        let remote = archive_store::Store::new();
        let archive = new_archive(
            &cluster,
            remote.clone(),
            Path::from("checkpoint-object-window"),
            CellGroupArchiveLimits {
                max_operations: 1,
                ..Default::default()
            },
        )
        .await?;
        let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let original = archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        let (path, index) = index(remote.as_ref(), &original).await?;
        let chunks = index["chunks"].as_array().context("checkpoint chunks")?;
        anyhow::ensure!(chunks.len() > 4, "source must exceed the I/O/channel window");
        let (prefix, _) = generation_prefix(&path)?;
        let first = &chunks[0]["object"];
        let digest: Vec<u8> = serde_json::from_value(first["digest"].clone())?;
        let held = Path::from(format!(
            "{prefix}/generations/{}/chunks/{}-{}",
            first["generation"],
            first["length"],
            hex::encode(digest)
        ));
        let before = remote.chunk_gets.load(Ordering::Acquire);
        let gate = remote.hold(held);

        struct Release(Arc<archive_store::Store>, Arc<archive_store::Gate>);

        impl Drop for Release {
            fn drop(&mut self) {
                self.0.clear();
                self.1.release.notify_one();
            }
        }
        let release = Release(remote.clone(), gate.clone());
        let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let destination = destination(&cluster).await?;
        let mut pending = Box::pin(async {
            if uploading {
                let repeated = archive
                    .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
                    .await?;
                assert_eq!(repeated, original);
            } else {
                let verified = archive
                    .verify_checkpoint(
                        &cluster.domain,
                        &original,
                        receiver(&receivers, &destination, &cluster)?,
                    )
                    .await?;
                tail_fixture::check_images(
                    &cluster,
                    verified.checkpoint().manifest(),
                    &expected,
                )
                .await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        cellar::asyncrt::select! {
            entered = tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()) => entered?,
            result = &mut pending => anyhow::bail!("checkpoint skipped held first object: {result:?}"),
        }
        let second = async {
            loop {
                let changed = remote.chunk_get_entered.notified();
                if remote.chunk_gets.load(Ordering::Acquire) >= before + 2 {
                    return;
                }
                changed.await;
            }
        };
        cellar::asyncrt::select! {
            observed = tokio::time::timeout(Duration::from_secs(5), second) => {
                observed.context("second checkpoint object serialized behind the held first object")?;
            },
            result = &mut pending => anyhow::bail!("checkpoint completed with a held first object: {result:?}"),
        }
        assert!(tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err());
        assert_eq!(
            remote.chunk_gets.load(Ordering::Acquire),
            before + 2,
            "the held first object must bound the ordered window to two"
        );
        assert_eq!(archive.usage().operations, 1);
        assert!(archive.usage().retained_bytes > 0);
        if uploading {
            assert_eq!(sources.usage().sources, 1);
        }
        drop(release);
        pending.await?;
        assert_eq!(sources.usage().sources, 0);
        assert_eq!(archive.usage().operations, 0);
        assert_eq!(archive.usage().retained_bytes, 0);
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    let root = std::mem::replace(&mut cluster._root, placeholder);
    let closed = cluster.close().await;
    if !matches!(&work, Ok(Ok(()))) || closed.is_err() {
        eprintln!(
            "retained failed archive-window disk: {}",
            root.keep().display()
        );
    }
    match work {
        Ok(result) => result.and(closed),
        Err(panic) => {
            if let Err(error) = closed {
                eprintln!("archive-window cleanup after panic failed: {error}");
            }
            std::panic::resume_unwind(panic)
        }
    }
}
