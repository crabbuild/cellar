// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read actual identity/state bytes concurrently without weakening either check.
use super::*;
use anyhow::ensure;
use object_store::{PutMode, PutOptions, UpdateVersion};

async fn entered(gate: &archive_store::ObjectIoGate) {
    loop {
        let changed = gate.entered.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if gate.started.load(Ordering::Acquire) == 3 {
            return;
        }
        changed.await;
    }
}

async fn replace(store: &dyn ObjectStore, path: &Path, bytes: Vec<u8>) -> Result<()> {
    let metadata = store.head(path).await?;
    store
        .put_opts(
            path,
            bytes.into(),
            PutOptions::from(PutMode::Update(UpdateVersion {
                e_tag: metadata.e_tag,
                version: metadata.version,
            })),
        )
        .await?;
    Ok(())
}

pub(crate) async fn registry_reads_overlap_and_preserve_validation(
    rooted: bool,
    backend: Arc<dyn ObjectStore>,
) -> Result<()> {
    let mut s = Scenario::with_store(
        rooted,
        Default::default(),
        archive_store::Store::around(backend),
    )
    .await?;
    retain_archive_fixture_disks(&mut s.cluster);
    let work = async {
        let objects: Vec<_> = s
            .remote
            .inner
            .list(Some(&Path::from("protected-head")))
            .try_collect()
            .await?;
        let paths = ["identity", "registry"]
            .map(|name| {
                let matches: Vec<_> = objects
                    .iter()
                    .filter(|object| object.location.filename() == Some(name))
                    .collect();
                ensure!(matches.len() == 1, "one actual {name} object");
                Ok(matches[0].location.clone())
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let puts = s.remote.puts.load(Ordering::Acquire);
        let hold = s.remote.hold_object_reads(paths.clone());
        let mut pending = Box::pin(s.registry.prepare_upload(&s.archive_domain));
        cellar::asyncrt::select! {
            result = &mut pending => {
                result?;
                anyhow::bail!("registry load ended before both reads entered");
            }
            result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => {
                result.context("registry loader serialized independent control reads")?;
            }
        }
        let charged = s.upload.usage().retained_bytes;
        ensure!(s.upload.usage().operations == 1 && charged > 0);
        hold.0.release(1);
        ensure!(tokio::time::timeout(Duration::from_millis(40), &mut pending).await.is_err());
        ensure!(s.upload.usage().operations == 1);
        hold.0.release(0);
        let intent = pending.await?;
        drop(hold);
        ensure!(s.upload.usage().operations == 0 && s.remote.puts.load(Ordering::Acquire) == puts);

        // Both validations remain fresh. A corrupt object must not return early
        // while its entered peer still owns a remote read and archive admission.
        for invalid in 0..2 {
            let saved = s.remote.inner.get(&paths[invalid]).await?.bytes().await?.to_vec();
            replace(s.remote.inner.as_ref(), &paths[invalid], b"invalid control bytes".to_vec()).await?;
            let hold = s.remote.hold_object_reads(paths.clone());
            let mut pending = Box::pin(s.registry.prepare_upload(&s.archive_domain));
            cellar::asyncrt::select! {
                result = &mut pending => anyhow::bail!("corrupt load skipped its entered peer: {result:?}"),
                result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => result?,
            }
            hold.0.release(invalid);
            ensure!(tokio::time::timeout(Duration::from_millis(100), &mut pending).await.is_err());
            ensure!(s.upload.usage().operations == 1 && s.upload.usage().retained_bytes == charged);
            hold.0.release(1 - invalid);
            let rejected = pending.await;
            let expected = if invalid == 0 { "archive registry identity changed" } else { "archive registry encoding" };
            ensure!(matches!(rejected, Err(CellGroupArchiveError::Invalid(message)) if message == expected));
            ensure!(s.upload.usage().operations == 0);
            drop(hold);
            replace(s.remote.inner.as_ref(), &paths[invalid], saved).await?;
        }

        let hold = s.remote.hold_object_reads(paths);
        let mut pending = Box::pin(s.registry.prepare_upload(&s.archive_domain));
        cellar::asyncrt::select! {
            result = &mut pending => anyhow::bail!("cancelled load skipped its entered reads: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(5), entered(&hold.0)) => result?,
        }
        drop(pending);
        ensure!(s.upload.usage().operations == 0 && s.remote.puts.load(Ordering::Acquire) == puts);
        drop(hold);

        let saved = persist_archive_intent(&s.cluster, &intent).await?;
        s.remote.lose_put_response.store(true, Ordering::Release);
        let ticket = s.registry.reserve(&s.archive_domain, &saved).await?;
        ensure!(!s.remote.lose_put_response.load(Ordering::Acquire));
        ensure!(s.registry.reserve(&s.archive_domain, &saved).await? == ticket);
        s.registry.abort(&s.archive_domain, &ticket).await?;
        let checkpoint = s.checkpoint(&s.upload).await?;
        let tail = s.tail(&s.upload, &checkpoint).await?;
        let coverage = s.publish(&s.upload, &tail).await?;
        tail_fixture::check_images(&s.cluster, coverage.recovery().manifest(), &s.expected).await?;
        let hold = coverage.hold().clone();
        drop(coverage);
        drop(tail);
        s.registry.release(&s.archive_domain, &hold).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = s.finish().await;
    work?;
    closed
}
