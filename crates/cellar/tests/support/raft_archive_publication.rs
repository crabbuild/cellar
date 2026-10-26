// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real conditional claims are scheduling hints, never archive/output proofs.

use super::*;
use std::time::Duration;

async fn hint(fixture: &Fixture) -> Result<(ObjectPath, Vec<u8>)> {
    let path = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|m| m.location.filename() == Some("publication"))
        .context("publication hint")?
        .location;
    let bytes = fixture
        .store
        .inner
        .get(&path)
        .await?
        .bytes()
        .await?
        .to_vec();
    Ok((path, bytes))
}

async fn entered(store: &faults::Store) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.active_part_confirmations.load(Ordering::Acquire) != 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("first publisher did not enter")?;
    Ok(())
}

fn release(store: &faults::Store) {
    store
        .hold_part_confirmations
        .store(false, Ordering::Release);
    store.part_confirmations_resume.notify_waiters();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_replicas_share_one_conditional_publication() -> Result<()> {
    let (fixture, cut) = snapshot_upload::multipart_fixture().await?;
    let peers = (1..=3)
        .map(|id| {
            fixture
                .archive
                .clone()
                .with_publication_coordination([id; 32], 5000, 0)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let results = futures_util::future::join_all(
        peers
            .iter()
            .map(|peer| peer.publish(cut.clone(), &fixture.verifier)),
    )
    .await;
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let expected_failures = results
        .iter()
        .all(|r| matches!(r, Ok(_) | Err(ArchiveError::Unconfirmed)));
    let parts = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|m| m.location.as_ref().contains("/snapshot-parts/"))
        .count();
    let puts = fixture.store.snapshot_part_puts.load(Ordering::Acquire);
    let selected = fixture.archive.recovery_set().await?;
    let value = selected.as_ref().map(|set| restored(set, 7)).transpose()?;
    let expected = fixture.bytes.clone();
    fixture.close().await?;
    ensure!(successes >= 1 && expected_failures);
    ensure!(
        parts >= 3 && puts == parts,
        "replicas redundantly uploaded {puts} parts for {parts} objects"
    );
    ensure!(value == Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_release_cannot_replace_a_new_publishers_claim_or_head() -> Result<()> {
    let (mut fixture, cut) = snapshot_upload::multipart_fixture().await?;
    let first = fixture
        .archive
        .clone()
        .with_publication_coordination([1; 32], 5000, 0)?;
    let peer = fixture
        .archive
        .clone()
        .with_publication_coordination([2; 32], 100, 0)?;
    let store = fixture.store.clone();
    store
        .hold_next_publication_release
        .store(true, Ordering::Release);
    let verifier = fixture.verifier.clone();
    let task = tokio::spawn(async move { first.publish(cut, &verifier).await });
    let checks = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !store.publication_release_waiting.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        let next = fixture.commit(8, vec![]).await?;
        ensure!(matches!(
            peer.publish(next.clone(), &fixture.verifier).await,
            Err(ArchiveError::Unconfirmed)
        ));
        tokio::time::sleep(Duration::from_millis(120)).await;
        let published = peer.publish(next, &fixture.verifier).await?;
        ensure!(restored(&published, 8)? == fixture.bytes);
        Ok::<_, anyhow::Error>((published.reference(), hint(&fixture).await?.1))
    }
    .await;
    store
        .publication_release_waiting
        .store(false, Ordering::Release);
    store.publication_release_resume.notify_waiters();
    let first = task.await;
    let actual_hint = hint(&fixture).await;
    let selected = fixture.archive.recovery_metadata().await;
    fixture.close().await?;
    first??;
    let (reference, expected_hint) = checks?;
    ensure!(
        actual_hint?.1 == expected_hint,
        "old release replaced the new claim"
    );
    ensure!(selected?.context("head")?.reference() == reference);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_publication_hint_reconciles_lost_reply_and_renews_without_a_data_head() -> Result<()>
{
    let (fixture, cut) = snapshot_upload::multipart_fixture().await?;
    let first = fixture
        .archive
        .clone()
        .with_publication_coordination([1; 32], 600, 0)?;
    let peer = fixture
        .archive
        .clone()
        .with_publication_coordination([2; 32], 600, 0)?;
    let store = fixture.store.clone();
    store.fail_after.store(true, Ordering::Release);
    store.slow_part_confirmations.store(true, Ordering::Release);
    let verifier = fixture.verifier.clone();
    let input = cut.clone();
    let task = tokio::spawn(async move { first.publish(input, &verifier).await });
    let checks = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.snapshot_part_reads.load(Ordering::Acquire) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        ensure!(
            !store.fail_after.load(Ordering::Acquire),
            "claim lost-response cut was not reached"
        );
        ensure!(matches!(
            peer.publish(cut.clone(), &fixture.verifier).await,
            Err(ArchiveError::Unconfirmed)
        ));
        let started = std::time::Instant::now();
        // Observe four version changes, including a full stale interval.
        // No claim timestamp or synchronized wall clock participates.
        for _ in 0..4 {
            let before = hint(&fixture).await?.1;
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if hint(&fixture).await?.1 != before {
                        break Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await??;
            ensure!(matches!(
                peer.publish(cut.clone(), &fixture.verifier).await,
                Err(ArchiveError::Unconfirmed)
            ));
        }
        ensure!(started.elapsed() >= Duration::from_millis(600));
        ensure!(fixture.archive.recovery_metadata().await?.is_none());
        ensure!(store.snapshot_part_puts.load(Ordering::Acquire) == 2);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    store
        .slow_part_confirmations
        .store(false, Ordering::Release);
    let published = task.await?;
    let before = store.snapshot_part_puts.load(Ordering::Acquire);
    let shared = peer.publish(cut, &fixture.verifier).await;
    let after = store.snapshot_part_puts.load(Ordering::Acquire);
    let expected = fixture.bytes.clone();
    let restored = shared.as_ref().ok().map(|set| restored(set, 7)).transpose();
    fixture.close().await?;
    checks?;
    published?;
    shared?;
    ensure!(before == after, "peer uploaded a covering image again");
    ensure!(restored? == Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_but_stalled_publisher_cannot_renew_its_own_idle_hint_forever() -> Result<()> {
    let (fixture, cut) = snapshot_upload::multipart_fixture().await?;
    let first = fixture
        .archive
        .clone()
        .with_publication_coordination([1; 32], 300, 0)?;
    let peer = fixture
        .archive
        .clone()
        .with_publication_coordination([2; 32], 300, 0)?;
    let store = fixture.store.clone();
    store.hold_part_confirmations.store(true, Ordering::Release);
    let verifier = fixture.verifier.clone();
    let input = cut.clone();
    let primary = tokio::spawn(async move { first.publish(input, &verifier).await });
    let initially_entered = entered(&store).await;
    let verifier = fixture.verifier.clone();
    let replacement = tokio::spawn(async move {
        loop {
            match peer.publish(cut.clone(), &verifier).await {
                Err(ArchiveError::Unconfirmed) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                result => break result,
            }
        }
    });
    let takeover = tokio::time::timeout(Duration::from_secs(5), async {
        while store.active_part_confirmations.load(Ordering::Acquire) < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let alive = !primary.is_finished();
    release(&store);
    let original = primary.await;
    let recovered = replacement.await;
    let selected = fixture.archive.recovery_set().await;
    let value = selected
        .as_ref()
        .ok()
        .and_then(|s| s.as_ref())
        .map(|s| restored(s, 7))
        .transpose();
    let expected = fixture.bytes.clone();
    fixture.close().await?;
    initially_entered?;
    ensure!(
        takeover.is_ok() && alive,
        "idle live process stranded publication"
    );
    original??;
    recovered??;
    selected?;
    ensure!(value? == Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_publication_hint_expires_on_the_observer_clock() -> Result<()> {
    let (fixture, cut) = snapshot_upload::multipart_fixture().await?;
    let first = fixture
        .archive
        .clone()
        .with_publication_coordination([1; 32], 5000, 0)?;
    let peer = fixture
        .archive
        .clone()
        .with_publication_coordination([2; 32], 100, 0)?;
    let store = fixture.store.clone();
    store.hold_part_confirmations.store(true, Ordering::Release);
    let verifier = fixture.verifier.clone();
    let input = cut.clone();
    let task = tokio::spawn(async move { first.publish(input, &verifier).await });
    let observed = entered(&store).await;
    task.abort();
    let stopped = task.await;
    release(&store);
    let before = fixture.archive.recovery_metadata().await;
    let refused = peer.publish(cut.clone(), &fixture.verifier).await;
    tokio::time::sleep(Duration::from_millis(120)).await;
    let recovered = peer.publish(cut, &fixture.verifier).await;
    let value = recovered
        .as_ref()
        .ok()
        .map(|set| restored(set, 7))
        .transpose();
    let expected = fixture.bytes.clone();
    fixture.close().await?;
    observed?;
    ensure!(stopped.is_err_and(|e| e.is_cancelled()));
    ensure!(store.active_part_confirmations.load(Ordering::Acquire) == 0);
    ensure!(before?.is_none(), "aborted claim became a data head");
    ensure!(matches!(refused, Err(ArchiveError::Unconfirmed)));
    recovered?;
    ensure!(value? == Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_advisory_hint_cannot_replace_physical_archive_validation() -> Result<()> {
    let (fixture, cut) = snapshot_upload::multipart_fixture().await?;
    let first = fixture
        .archive
        .clone()
        .with_publication_coordination([1; 32], 5000, 0)?;
    let peer = fixture
        .archive
        .clone()
        .with_publication_coordination([2; 32], 100, 0)?;
    let store = fixture.store.clone();
    store.hold_part_confirmations.store(true, Ordering::Release);
    let verifier = fixture.verifier.clone();
    let input = cut.clone();
    let task = tokio::spawn(async move { first.publish(input, &verifier).await });
    let observed = entered(&store).await;
    task.abort();
    let _ = task.await;
    release(&store);
    observed?;
    let (path, _) = hint(&fixture).await?;
    store
        .inner
        .put(&path, b"invalid advisory record".as_slice().into())
        .await?;
    let published = peer.publish(cut.clone(), &fixture.verifier).await?;
    let reference = published.reference();
    ensure!(restored(&published, 7)? == fixture.bytes);
    let part = store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|m| m.location.as_ref().contains("/snapshot-parts/"))
        .context("part")?;
    store.inner.delete(&part.location).await?;
    let refused = peer.publish(cut, &fixture.verifier).await;
    let selected = fixture.archive.recovery_metadata().await?;
    fixture.close().await?;
    ensure!(
        refused.is_err(),
        "advisory record replaced missing physical data"
    );
    ensure!(selected.context("selected head")?.reference() == reference);
    Ok(())
}
