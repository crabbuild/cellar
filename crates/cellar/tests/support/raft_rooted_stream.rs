// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical extent streams through the ordinary native snapshot capabilities.

use super::*;
use anyhow::{ensure, Context as _};
use raft::Storage as _;

fn package(host: &mut Host) -> Result<SnapshotPackage> {
    Ok(RootSnapshotSource::capture(
        &host.log.group(&scope())?,
        host.applier.current().context("source image")?,
        SnapshotLimits::default(),
    )?
    .into_package(&host.store, &mut host.buffer)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_root_stream_deduplicates_extents_checkpoints_and_installs() -> Result<()> {
    let root = tempfile::tempdir()?;
    let learners = vec![MemberId::new(2)?];
    let (mut source, captured) = snapshots::source_padding(
        &root.path().join("source"),
        [7; 32],
        learners.clone(),
        24 * 1024 * 1024,
        true,
    )
    .await?;
    let mut target = Host::open_members(
        &root.path().join("target"),
        false,
        2,
        [9; 32],
        [7; 32],
        2,
        learners,
    )
    .await?;
    let result = async {
        let set = RootSnapshotSet::for_group(
            &source.log.group(&scope())?,
            [9; 32],
            &captured.snapshot(),
            captured.manifest(),
            SnapshotLimits::default(),
        )?;
        let expected = set.marker().clone();
        let metadata_bytes = captured.manifest().len();
        let unique_bytes = set.extents().map(|id| id.length() as usize).sum::<usize>();
        let package = captured.into_package(&source.store, &mut source.buffer)?;
        ensure!(package.encoded_length() == metadata_bytes + unique_bytes);
        ensure!(
            unique_bytes < expected.length as usize / 2,
            "duplicate extents were transferred"
        );
        source
            .runtime
            .checkpoint(package.checkpoint_material())
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match source.events.recv().await.context("events closed")?.kind {
                    RuntimeEventKind::Checkpointed { .. } => return Ok::<_, anyhow::Error>(()),
                    RuntimeEventKind::Failed { error, .. } => return Err(error.into()),
                    _ => (),
                }
            }
        })
        .await??;
        ensure!(
            source
                .log
                .group(&scope())?
                .initial_state()?
                .hard_state
                .commit
                >= 3
        );
        let mut receive = SnapshotDownload::in_extents(
            &target.log.group(&scope())?,
            [9; 32],
            package.snapshot(),
            SnapshotLimits::default(),
            target.store.clone(),
        )?;
        let mut offset = 0;
        while offset < package.encoded_length() {
            // Cross both manifest and object boundaries using non-page windows.
            let length = (package.encoded_length() - offset).min(49157);
            receive.append(offset, &package.chunk(offset, length)?)?;
            offset += length;
        }
        ensure!(target.log.group(&scope())?.applied()?.image.is_none());
        let prepared = receive.finish_root(&target.store)?;
        let ticket = snapshot_installs::snapshot_ticket(&mut target, package.snapshot()).await?;
        target
            .runtime
            .complete_snapshot(prepared.completion(&ticket)?)
            .await?;
        snapshot_installs::installed(&mut target, &ticket).await?;
        let image = prepared.publish_root(&mut target.applier, &mut target.buffer)?;
        ensure!(read(&image, &root.path().join("read"))? == 7);
        ensure!(target.store.usage()?.objects == set.extents().count());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let source_closed = source.close().await;
    let target_closed = target.close().await;
    result?;
    source_closed?;
    target_closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_stream_refuses_corrupt_bodies_short_streams_and_foreign_inventory() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (mut source, captured) = snapshots::source(&root.path().join("source")).await?;
    let target = Host::open(&root.path().join("target"), false, 2).await?;
    let metadata_bytes = captured.manifest().len();
    let result = (|| -> Result<()> {
        for scenario in 0..3 {
            let package = package(&mut source)?;
            let mut receive = SnapshotDownload::in_extents(
                &target.log.group(&scope())?,
                [9; 32],
                package.snapshot(),
                SnapshotLimits::default(),
                target.store.clone(),
            )?;
            let total = package.encoded_length();
            let end = if scenario == 1 { total - 1 } else { total };
            let mut offset = 0;
            let mut rejected = false;
            while offset < end {
                let length = (end - offset).min(65536);
                let mut body = package.chunk(offset, length)?;
                let corrupt_at = metadata_bytes + 100;
                if scenario == 0 && offset <= corrupt_at && corrupt_at < offset + length {
                    body[corrupt_at - offset] ^= 1;
                }
                if receive.append(offset, &body).is_err() {
                    rejected = true;
                    break;
                }
                offset += length;
            }
            if scenario == 0 {
                ensure!(rejected, "corrupt object was accepted");
            }
            let other;
            let destination = if scenario == 2 {
                other = NodeExtentStore::open(
                    root.path().join("foreign-inventory"),
                    Arc::new(DirectFileSystem),
                    Default::default(),
                )?;
                &other
            } else {
                &target.store
            };
            ensure!(receive.finish_root(destination).is_err());
            ensure!(target.applier.current().is_none());
            ensure!(target.log.group(&scope())?.applied()?.image.is_none());
        }
        Ok(())
    })();
    let source_closed = source.close().await;
    let target_closed = target.close().await;
    result?;
    source_closed?;
    target_closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_stream_rejects_a_false_closure_length_before_importing_any_objects() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (mut source, captured) = snapshots::source_padding(
        &root.path().join("source"),
        [7; 32],
        vec![],
        24 * 1024 * 1024,
        true,
    )
    .await?;
    let target = Host::open(&root.path().join("target"), false, 2).await?;
    let metadata = captured.manifest().to_vec();
    let result = (|| -> Result<()> {
        let package = package(&mut source)?;
        let mut snapshot = package.snapshot();
        let body = String::from_utf8(snapshot.data.to_vec())?;
        let original = format!("\"transfer_length\":{}", package.encoded_length());
        ensure!(body.matches(&original).count() == 1);
        snapshot.data = body
            .replace(
                &original,
                &format!("\"transfer_length\":{}", package.encoded_length() + 1),
            )
            .into_bytes()
            .into();
        let mut receive = SnapshotDownload::in_extents(
            &target.log.group(&scope())?,
            [9; 32],
            snapshot,
            SnapshotLimits::default(),
            target.store.clone(),
        )?;
        ensure!(receive.append(0, &metadata).is_err());
        ensure!(target.store.usage()?.objects == 0);
        ensure!(receive.finish_root(&target.store).is_err());
        Ok(())
    })();
    let source_closed = source.close().await;
    let target_closed = target.close().await;
    result?;
    source_closed?;
    target_closed
}
