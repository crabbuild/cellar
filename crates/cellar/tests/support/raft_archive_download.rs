// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real immutable recovery sets exercise bounded overlap and cancellation.
use super::*;
use std::time::Duration;

async fn history() -> Result<(Fixture, ArchiveReference)> {
    let mut fixture = Fixture::new(ArchiveLimits {
        // Retain six tails so the bounded download/cancellation window is full.
        max_tail_image_ratio: 128,
        ..Default::default()
    })
    .await?;
    let first = fixture
        .commit(
            1,
            vec![
                ArchiveDependency(b"first dependency".to_vec()),
                ArchiveDependency(b"second dependency".to_vec()),
            ],
        )
        .await?;
    let mut set = fixture.publish(first).await?;
    for value in 2..=7 {
        let cut = fixture.commit(value, vec![]).await?;
        set = fixture.publish(cut).await?;
    }
    assert_eq!(set.tail_entries(), 6);
    fixture.store.peak_payload_reads.store(0, Ordering::Release);
    fixture
        .store
        .hold_archive_payloads
        .store(true, Ordering::Release);
    Ok((fixture, set.reference()))
}

async fn full_window(store: &faults::Store) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.active_payload_reads.load(Ordering::Acquire) < 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("archive payload reads did not overlap while the first read was held")
}

fn release(store: &faults::Store) {
    store.hold_archive_payloads.store(false, Ordering::Release);
    store.payload_window_resume.notify_waiters();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_download_overlaps_bounded_payloads_and_preserves_replay_order() -> Result<()> {
    let (fixture, reference) = history().await?;
    let mut read = Box::pin(fixture.archive.recovery_set_at(&reference));
    let overlap = cellar::asyncrt::select! {
        result = &mut read => panic!("held payload read completed: {:?}", result.err()),
        result = full_window(&fixture.store) => result,
    };
    let active = fixture.store.active_payload_reads.load(Ordering::Acquire);
    release(&fixture.store);
    let set = tokio::time::timeout(Duration::from_secs(5), read).await??;
    assert_eq!(set.reference(), reference);
    assert_eq!(set.tail_entries(), 6);
    assert_eq!(set.dependencies().count(), 2);
    for expected in [
        b"first dependency".as_slice(),
        b"second dependency".as_slice(),
    ] {
        assert!(set.dependencies().any(|(_, bytes)| bytes == expected));
    }
    restored(&set, 7)?;
    let peak = fixture.store.peak_payload_reads.load(Ordering::Acquire);
    assert_eq!(
        fixture.store.active_payload_reads.load(Ordering::Acquire),
        0
    );
    fixture.close().await?;
    overlap?;
    assert_eq!(active, 4);
    assert_eq!(
        peak, 4,
        "archive downloads exceeded the four-request window"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_archive_download_releases_every_pending_read() -> Result<()> {
    let (fixture, reference) = history().await?;
    let mut read = Box::pin(fixture.archive.recovery_set_at(&reference));
    let overlap = cellar::asyncrt::select! {
        result = &mut read => panic!("held payload read completed: {:?}", result.err()),
        result = full_window(&fixture.store) => result,
    };
    drop(read);
    assert_eq!(
        fixture.store.active_payload_reads.load(Ordering::Acquire),
        0
    );
    release(&fixture.store);
    let set = fixture.archive.recovery_set_at(&reference).await?;
    assert_eq!(set.reference(), reference);
    restored(&set, 7)?;
    fixture.close().await?;
    overlap
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_object_length_is_checked_before_reading_its_payload() -> Result<()> {
    let (fixture, reference) = history().await?;
    release(&fixture.store);
    fixture.store.payload_size_delta.store(1, Ordering::Release);
    let before = fixture.store.payload_chunks.load(Ordering::Acquire);
    let result = fixture.archive.recovery_set_at(&reference).await;
    let chunks = fixture.store.payload_chunks.load(Ordering::Acquire) - before;
    fixture.close().await?;
    ensure!(
        matches!(
            result,
            Err(ArchiveError::Corrupt("archive object digest/length"))
        ),
        "unexpected object-length failure: {:?}",
        result.err()
    );
    assert_eq!(
        chunks, 0,
        "downloaded a body exceeding its manifest allocation"
    );
    Ok(())
}
