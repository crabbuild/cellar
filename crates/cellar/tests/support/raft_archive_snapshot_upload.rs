// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A producer must verify stored bytes without downloading its own image twice.

use super::*;
use cellar_ltx::stream_snapshot::{ScratchLimits, ScratchPool, ScratchUsage};

async fn multipart_fixture() -> Result<(Fixture, CommittedArchiveCut)> {
    let mut fixture = Fixture::new(ArchiveLimits {
        max_capture_bytes: 16 * 1024 * 1024,
        ..Default::default()
    })
    .await?;
    let image = images::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7);\
         CREATE TABLE padding(bytes BLOB);INSERT INTO padding VALUES(randomblob(9437184))",
    )?;
    let cut = fixture.commit_bytes(image, vec![], |_| {}).await?;
    Ok((fixture, cut))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_upload_overlaps_a_bounded_window_of_exact_confirmations() -> Result<()> {
    let (fixture, cut) = multipart_fixture().await?;
    let store = fixture.store.clone();
    store.hold_part_confirmations.store(true, Ordering::Release);
    let archive = fixture.archive.clone();
    let verifier = fixture.verifier.clone();
    let task = tokio::spawn(async move { archive.publish(cut, &verifier).await });
    let entered = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store.active_part_confirmations.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    let active = store.active_part_confirmations.load(Ordering::Acquire);
    let selected = fixture.archive.recovery_metadata().await;
    let created = store.snapshot_part_puts.load(Ordering::Acquire);
    store
        .hold_part_confirmations
        .store(false, Ordering::Release);
    store.part_confirmations_resume.notify_waiters();
    let result = task.await?;
    let peak = store.peak_part_confirmations.load(Ordering::Acquire);
    let drained = store.active_part_confirmations.load(Ordering::Acquire);
    let restored = result
        .as_ref()
        .ok()
        .map(|set| restored(set, 7))
        .transpose()?;
    let expected = fixture.bytes.clone();
    fixture.close().await?;
    ensure!(
        entered.is_ok(),
        "upload remained serial at {active} confirmation(s)"
    );
    ensure!(
        active == 2 && created == 2 && peak == 2,
        "upload window escaped its bound"
    );
    ensure!(
        selected?.is_none(),
        "head appeared before all confirmations"
    );
    ensure!(drained == 0);
    result?;
    ensure!(restored == Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_snapshot_parts_are_not_downloaded_again_by_the_producer() -> Result<()> {
    let (fixture, cut) = multipart_fixture().await?;
    let set = fixture.publish(cut).await?;
    let parts = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|object| object.location.as_ref().contains("/snapshot-parts/"))
        .count();
    ensure!(
        parts >= 3,
        "the source must contain several incompressible parts"
    );
    let publication_reads = fixture.store.snapshot_part_reads.load(Ordering::Acquire);
    ensure!(restored(&set, 7)? == fixture.bytes);
    let selected = fixture.archive.recovery_set_at(&set.reference()).await?;
    let recovery_reads =
        fixture.store.snapshot_part_reads.load(Ordering::Acquire) - publication_reads;
    ensure!(restored(&selected, 7)? == fixture.bytes);
    fixture.close().await?;
    ensure!(
        publication_reads == parts,
        "producer read {publication_reads} parts for {parts} exact read-back confirmations"
    );
    ensure!(
        recovery_reads == parts,
        "independent recovery must fetch every selected part"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_image_rebinds_configuration_without_uploading_image_parts() -> Result<()> {
    let (fixture, first) = multipart_fixture().await?;
    fixture.publish(first).await?;
    let parts = fixture.store.snapshot_part_puts.load(Ordering::Acquire);
    ensure!(parts >= 3);
    let changed = fixture.protocol(true).await?;
    let expected_position = changed.position();
    let set = fixture.publish(changed).await?;
    let uploaded = fixture.store.snapshot_part_puts.load(Ordering::Acquire) - parts;
    ensure!(set.position() == expected_position && set.tail_entries() == 0);
    ensure!(set.configuration()?.learners == vec![2]);
    ensure!(set.membership() == fixture.group.membership()?);
    ensure!(restored(&set, 7)? == fixture.bytes);
    let remote = fixture.archive.recovery_set_at(&set.reference()).await?;
    ensure!(remote.position() == expected_position);
    ensure!(remote.configuration()? == set.configuration()?);
    ensure!(remote.membership() == set.membership());
    ensure!(restored(&remote, 7)? == fixture.bytes);
    fixture.close().await?;
    ensure!(
        uploaded == 0,
        "unchanged image uploaded {uploaded} snapshot parts"
    );
    Ok(())
}

async fn owned_source_fixture() -> Result<(
    Fixture,
    CommittedArchiveCut,
    Arc<image_fault::Faults>,
    ScratchPool,
)> {
    let mut fixture = Fixture::new(ArchiveLimits {
        max_capture_bytes: 16 * 1024 * 1024,
        ..Default::default()
    })
    .await?;
    let directory = fixture._root.path().join("owned-source-images");
    let faults = image_fault::Faults::new(directory.clone(), None);
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 128 * 1024 * 1024,
        max_files: 4,
    })?;
    fixture.images = GenerationStore::open_with_scratch(
        directory,
        Arc::new(image_fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits::default(),
        None,
        scratch.clone(),
    )?;
    let image = images::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7);\
         CREATE TABLE padding(bytes BLOB);INSERT INTO padding VALUES(randomblob(9437184))",
    )?;
    let cut = fixture.commit_bytes(image, vec![], |_| {}).await?;
    Ok((fixture, cut, faults, scratch))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_upload_drains_entered_confirmations_before_releasing_its_source() -> Result<()> {
    let (fixture, cut, _, scratch) = owned_source_fixture().await?;
    let store = fixture.store.clone();
    store.hold_part_confirmations.store(true, Ordering::Release);
    store
        .fail_first_part_after_window_enters
        .store(true, Ordering::Release);
    let archive = fixture.archive.clone();
    let verifier = fixture.verifier.clone();
    let first = cut.clone();
    let task = tokio::spawn(async move { archive.publish(first, &verifier).await });
    let entered = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while store
            .fail_first_part_after_window_enters
            .load(Ordering::Acquire)
            || store.active_part_confirmations.load(Ordering::Acquire) != 1
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    let waiting = !task.is_finished();
    let held = scratch.usage();
    let before = fixture.archive.recovery_metadata().await;
    store
        .hold_part_confirmations
        .store(false, Ordering::Release);
    store.part_confirmations_resume.notify_waiters();
    let result = task.await?;
    let drained = store.active_part_confirmations.load(Ordering::Acquire);
    let released = scratch.usage();
    let after = fixture.archive.recovery_metadata().await;
    let retry = fixture.publish(cut).await;
    let value = retry.as_ref().ok().map(|set| restored(set, 7)).transpose();
    let expected = fixture.bytes.clone();
    drop(retry);
    fixture.close().await?;
    ensure!(
        entered.is_ok() && waiting,
        "publication did not retain entered work"
    );
    let held = held?;
    ensure!(held.reserved_files == 1 && held.reserved_bytes > 9 * 1024 * 1024);
    ensure!(
        before?.is_none() && after?.is_none(),
        "failed window published a head"
    );
    ensure!(result.is_err() && drained == 0);
    ensure!(released? == ScratchUsage::default());
    ensure!(
        value? == Some(expected),
        "exact retry did not restore the image"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_upload_source_corruption_cannot_publish_a_head() -> Result<()> {
    let (fixture, cut, faults, scratch) = owned_source_fixture().await?;
    *fixture
        .store
        .corrupt_scratch_after_part_read
        .lock()
        .unwrap() = Some(faults.corrupt_next_scratch_read.clone());
    let result = fixture.publish(cut.clone()).await;
    ensure!(faults.scratch_read_corruptions.load(Ordering::Acquire) == 1);
    ensure!(
        matches!(
            result,
            Err(ArchiveError::Corrupt("uploaded snapshot stream digest"))
        ),
        "confirmed parts cannot replace whole-stream binding"
    );
    ensure!(fixture.archive.recovery_metadata().await?.is_none());
    ensure!(scratch.usage()? == ScratchUsage::default());
    let set = fixture.publish(cut).await?;
    ensure!(restored(&set, 7)? == fixture.bytes);
    let selected = fixture.archive.recovery_set_at(&set.reference()).await?;
    ensure!(restored(&selected, 7)? == fixture.bytes);
    drop(selected);
    drop(set);
    ensure!(scratch.usage()? == ScratchUsage::default());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_part_confirmation_releases_source_and_success_keeps_it_owned() -> Result<()> {
    let (fixture, cut, _, scratch) = owned_source_fixture().await?;
    fixture
        .store
        .fail_confirmation
        .store(true, Ordering::Release);
    ensure!(fixture.publish(cut.clone()).await.is_err());
    ensure!(!fixture.store.fail_confirmation.load(Ordering::Acquire));
    ensure!(fixture.archive.recovery_metadata().await?.is_none());
    ensure!(scratch.usage()? == ScratchUsage::default());
    let set = fixture.publish(cut).await?;
    let usage = scratch.usage()?;
    ensure!(usage.reserved_files == 1 && usage.reserved_bytes > 9 * 1024 * 1024);
    ensure!(restored(&set, 7)? == fixture.bytes);
    drop(set);
    ensure!(scratch.usage()? == ScratchUsage::default());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_source_still_requires_independent_destination_sync() -> Result<()> {
    for rebind in [false, true] {
        let (fixture, first, _, scratch) = owned_source_fixture().await?;
        let (cut, previous) = if rebind {
            let set = fixture.publish(first).await?;
            let reference = set.reference();
            drop(set);
            (fixture.protocol(true).await?, Some(reference))
        } else {
            (first, None)
        };
        let directory = fixture._root.path().join("failed-independent-verifier");
        let faults = image_fault::Faults::new(directory.clone(), None);
        let verifier = ArchiveVerifier::new(
            GenerationStore::open(
                directory,
                Arc::new(image_fault::FaultFileSystem {
                    faults: faults.clone(),
                }),
                GenerationLimits::default(),
            )?,
            images::id(93),
        )?;
        faults
            .mode
            .store(image_fault::FAIL_IMAGE_SYNC, Ordering::Release);
        let result = fixture.archive.publish(cut.clone(), &verifier).await;
        ensure!(
            result.is_err(),
            "read-back confirmations cannot replace destination sealing"
        );
        ensure!(faults.mode.load(Ordering::Acquire) == 0);
        ensure!(faults.image_creates.load(Ordering::Acquire) > 0);
        ensure!(scratch.usage()? == ScratchUsage::default());
        ensure!(
            fixture
                .archive
                .recovery_metadata()
                .await?
                .map(|m| m.reference())
                == previous
        );
        // An uncertain destination remains refused instead of being recycled
        // as a successful proof on the next publication attempt.
        ensure!(fixture.archive.publish(cut, &verifier).await.is_err());
        if let Some(reference) = previous {
            let set = fixture.archive.recovery_set_at(&reference).await?;
            ensure!(restored(&set, 7)? == fixture.bytes);
        }
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configuration_rebind_refuses_missing_prior_parts_without_repairing_them() -> Result<()> {
    let (fixture, first) = multipart_fixture().await?;
    let set = fixture.publish(first).await?;
    let reference = set.reference();
    let changed = fixture.protocol(true).await?;
    let part = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|object| object.location.as_ref().contains("/snapshot-parts/"))
        .context("snapshot part")?;
    let bytes = fixture
        .store
        .inner
        .get(&part.location)
        .await?
        .bytes()
        .await?;
    fixture.store.inner.delete(&part.location).await?;
    let before = fixture.store.snapshot_part_puts.load(Ordering::Acquire);
    ensure!(fixture.publish(changed.clone()).await.is_err());
    ensure!(fixture.store.snapshot_part_puts.load(Ordering::Acquire) == before);
    ensure!(
        fixture
            .archive
            .recovery_metadata()
            .await?
            .context("selected head")?
            .reference()
            == reference
    );
    fixture
        .store
        .inner
        .put(&part.location, bytes.into())
        .await?;
    let repaired = fixture.publish(changed).await?;
    ensure!(restored(&repaired, 7)? == fixture.bytes);
    ensure!(repaired.configuration()?.learners == vec![2]);
    fixture.close().await
}
