// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A prior replay proof can save destination work, never remote object checks.
use super::*;

fn instrument(fixture: &mut Fixture) -> Result<Arc<image_fault::Faults>> {
    let directory = fixture._root.path().join("memo-verifier");
    let faults = image_fault::Faults::new(directory.clone(), None);
    let mut limits = GenerationLimits::default();
    limits.apply.decode.max_encoded_bytes = 64 * 1024 * 1024;
    limits.apply.max_total_encoded_bytes = 64 * 1024 * 1024;
    fixture.verifier = ArchiveVerifier::new(
        GenerationStore::open(
            directory,
            Arc::new(image_fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            limits,
        )?,
        images::id(93),
    )?;
    Ok(faults)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_sql_projection_reuses_replay_across_new_configuration_cuts() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let faults = instrument(&mut fixture)?;
    let result = async {
        let first = fixture.commit(7, vec![]).await?;
        let initial = fixture.publish(first).await?;
        restored(&initial, 7)?;
        let initial_creates = faults.image_creates.load(Ordering::Acquire);
        ensure!(initial_creates > 0, "first proof skipped physical replay");
        let noop = fixture.protocol(false).await?;
        let next = fixture.publish(noop).await?;
        ensure!(next.position().index == LogIndex::new(3));
        ensure!(
            faults.image_creates.load(Ordering::Acquire) == initial_creates,
            "identical SQL projection repeated physical replay"
        );
        let changed = fixture.protocol(true).await?;
        let next = fixture.publish(changed).await?;
        ensure!(next.configuration()?.learners == vec![2]);
        ensure!(next.membership() == fixture.group.membership()?);
        ensure!(faults.image_creates.load(Ordering::Acquire) == initial_creates);
        restored(&next, 7)?;
        let data = fixture.commit(9, vec![]).await?;
        let next = fixture.publish(data).await?;
        let data_creates = faults.image_creates.load(Ordering::Acquire);
        ensure!(
            data_creates > initial_creates,
            "changed SQL reused an old proof"
        );
        restored(&next, 9)?;
        let noop = fixture.protocol(false).await?;
        let next = fixture.publish(noop).await?;
        ensure!(
            faults.image_creates.load(Ordering::Acquire) == data_creates,
            "unchanged data-bearing tail was replayed again"
        );
        restored(&next, 9)?;
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_replay_never_hides_missing_or_corrupt_remote_parts() -> Result<()> {
    let (mut fixture, first) = snapshot_upload::multipart_fixture().await?;
    let faults = instrument(&mut fixture)?;
    let result = async {
        let initial = fixture.publish(first).await?;
        let reference = initial.reference();
        let creates = faults.image_creates.load(Ordering::Acquire);
        let puts = fixture.store.snapshot_part_puts.load(Ordering::Acquire);
        ensure!(creates > 0 && puts > 0);
        let part = fixture
            .store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .find(|object| object.location.as_ref().contains("/snapshot-parts/"))
            .context("multipart base missing")?;
        let original = fixture
            .store
            .inner
            .get(&part.location)
            .await?
            .bytes()
            .await?;
        let next = fixture.protocol(false).await?;
        fixture.store.inner.delete(&part.location).await?;
        ensure!(
            fixture.publish(next.clone()).await.is_err(),
            "memo hid a missing remote part"
        );
        ensure!(
            fixture
                .archive
                .recovery_metadata()
                .await?
                .map(|m| m.reference())
                == Some(reference.clone())
        );
        let mut corrupt = original.to_vec();
        corrupt[0] ^= 1;
        fixture
            .store
            .inner
            .put(&part.location, corrupt.into())
            .await?;
        ensure!(
            fixture.publish(next.clone()).await.is_err(),
            "memo hid corrupt remote bytes"
        );
        ensure!(
            fixture
                .archive
                .recovery_metadata()
                .await?
                .map(|m| m.reference())
                == Some(reference)
        );
        ensure!(
            fixture.store.snapshot_part_puts.load(Ordering::Acquire) == puts,
            "publication silently repaired the selected history"
        );
        fixture
            .store
            .inner
            .put(&part.location, original.into())
            .await?;
        let reads = fixture.store.snapshot_part_reads.load(Ordering::Acquire);
        let set = fixture.publish(next).await?;
        ensure!(
            fixture.store.snapshot_part_reads.load(Ordering::Acquire) > reads,
            "memo bypassed the current dependency download"
        );
        ensure!(faults.image_creates.load(Ordering::Acquire) == creates);
        ensure!(restored(&set, 7)? == fixture.bytes);
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_changed_image_quarantines_even_a_previously_cached_projection() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let faults = instrument(&mut fixture)?;
    let result = async {
        let first = fixture.commit(7, vec![]).await?;
        let initial = fixture.publish(first.clone()).await?;
        let reference = initial.reference();
        let creates = faults.image_creates.load(Ordering::Acquire);
        let changed = fixture.commit(9, vec![]).await?;
        faults
            .mode
            .store(image_fault::FAIL_IMAGE_SYNC, Ordering::Release);
        ensure!(fixture.publish(changed).await.is_err());
        ensure!(
            faults.mode.load(Ordering::Acquire) == 0
                && faults.image_creates.load(Ordering::Acquire) > creates,
            "changed projection did not enter physical verification"
        );
        ensure!(
            fixture
                .archive
                .recovery_metadata()
                .await?
                .map(|m| m.reference())
                == Some(reference.clone())
        );
        ensure!(
            matches!(
                fixture.publish(first).await,
                Err(ArchiveError::Invalid("archive verifier requires recovery"))
            ),
            "an old memo bypassed workspace quarantine"
        );
        let retained = fixture.archive.recovery_set_at(&reference).await?;
        restored(&retained, 7)?;
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_verifier_cannot_inherit_another_owners_replay_proof() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let faults = instrument(&mut fixture)?;
    let result = async {
        let first = fixture.commit(7, vec![]).await?;
        let initial = fixture.publish(first.clone()).await?;
        let reference = initial.reference();
        let creates = faults.image_creates.load(Ordering::Acquire);
        let directory = fixture._root.path().join("fresh-verifier");
        let fresh = image_fault::Faults::new(directory.clone(), None);
        let verifier = ArchiveVerifier::new(
            GenerationStore::open(
                directory,
                Arc::new(image_fault::FaultFileSystem {
                    faults: fresh.clone(),
                }),
                GenerationLimits::default(),
            )?,
            images::id(94),
        )?;
        fresh
            .mode
            .store(image_fault::FAIL_IMAGE_SYNC, Ordering::Release);
        ensure!(fixture
            .archive
            .publish(first.clone(), &verifier)
            .await
            .is_err());
        ensure!(
            fresh.mode.load(Ordering::Acquire) == 0
                && fresh.image_creates.load(Ordering::Acquire) > 0
        );
        ensure!(
            fixture
                .archive
                .recovery_metadata()
                .await?
                .map(|m| m.reference())
                == Some(reference)
        );
        let reused = fixture.publish(first).await?;
        ensure!(faults.image_creates.load(Ordering::Acquire) == creates);
        restored(&reused, 7)?;
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_database_bytes_from_another_history_require_physical_replay() -> Result<()> {
    let mut first = Fixture::new(ArchiveLimits::default()).await?;
    let faults = instrument(&mut first)?;
    let mut second = Fixture::new_group(ArchiveLimits::default(), first.store.clone(), 8).await?;
    let result = async {
        let a = first.commit(7, vec![]).await?;
        let b = second.commit(7, vec![]).await?;
        ensure!(
            first.bytes == second.bytes,
            "fixture did not preserve identical SQLite bytes"
        );
        let set = first.publish(a.clone()).await?;
        restored(&set, 7)?;
        let before = faults.image_creates.load(Ordering::Acquire);
        let set = second.archive.publish(b, &first.verifier).await?;
        restored(&set, 7)?;
        let other = faults.image_creates.load(Ordering::Acquire);
        ensure!(other > before, "foreign history reused a physical proof");
        let set = first.publish(a).await?;
        ensure!(
            faults.image_creates.load(Ordering::Acquire) > other,
            "verifier retained an unbounded per-history memo"
        );
        restored(&set, 7)?;
        Ok(())
    }
    .await;
    let close_second = second.close().await;
    let close_first = first.close().await;
    result.and(close_second).and(close_first)
}
