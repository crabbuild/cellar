// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test]
async fn sandbox_retirement_recovers_partial_publication_and_rejects_changed_identity() -> Result<()>
{
    let objects = Arc::new(object_store::memory::InMemory::new());
    let store = Store::new(
        Some((objects.clone(), "tenant/".into())),
        std::path::Path::new("unused"),
    );
    let record = Record::new(Evidence::Parent {
        scope: "Parent:alice".into(),
        incarnation: "a".repeat(32),
    })?;
    // Crash after durable queue publication, before the external barrier.
    store
        .immutable(&store.pending_key(record.evidence.scope()), &record)
        .await?;
    assert_eq!(store.get("Parent:alice").await?, None);
    assert_eq!(next(&store, false).await?, Some(record.clone()));
    assert_eq!(
        store.get("Parent:alice/facets/one").await?,
        Some(record.clone())
    );
    assert_eq!(store.status("Parent:alice").await?, Some("pending"));
    // Slow inventory I/O cannot delay barrier publication for a new intent.
    let scan = store.scans[0].lock().await;
    tokio::time::timeout(std::time::Duration::from_millis(100), store.begin(&record)).await??;
    drop(scan);
    let changed = Record::new(Evidence::Parent {
        scope: "Parent:alice".into(),
        incarnation: "b".repeat(32),
    })?;
    assert!(store.begin(&changed).await.is_err());
    store.complete(&record).await?;
    assert_eq!(store.status("Parent:alice").await?, Some("erased"));
    // Completion keeps an audit discoverable, including after a collector restart.
    let restarted = Store::new(
        Some((objects.clone(), "tenant/".into())),
        std::path::Path::new("unused"),
    );
    assert_eq!(next(&restarted, false).await?, None);
    assert_eq!(next(&restarted, true).await?, Some(record.clone()));
    assert_eq!(restarted.get("Parent:alice2").await?, None);
    objects
        .put(&key("tenant/", "Parent:alice"), b"invalid".to_vec().into())
        .await?;
    assert!(restarted.get("Parent:alice").await.is_err());
    assert!(restarted.status("Parent:alice").await.is_err());
    Ok(())
}

async fn next(store: &Store, audit: bool) -> Result<Option<Record>> {
    match store.discover(audit, 1, 0).await?.first() {
        Some(candidate) => store.load(candidate).await,
        None => Ok(None),
    }
}

#[tokio::test]
async fn sandbox_retirement_discovery_is_bounded_unordered_and_skips_corrupt_records() -> Result<()>
{
    use futures_util::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let objects = Arc::new(object_store::memory::InMemory::new());
    let store = Store::new(
        Some((objects.clone(), String::new())),
        std::path::Path::new("unused"),
    );
    let mut metadata = Vec::new();
    for index in 0..64 {
        let record = Record::new(Evidence::Parent {
            scope: format!("Parent:{index:04}"),
            incarnation: "a".repeat(32),
        })?;
        store.begin(&record).await?;
        metadata.push(
            objects
                .head(&store.pending_key(record.evidence.scope()))
                .await?,
        );
    }
    // The ObjectStore trait does not promise listing order. Keep its stream,
    // rather than selecting a lexical cursor that could skip unseen entries.
    metadata.reverse();
    let observed = Arc::new(AtomicUsize::new(0));
    let counter = observed.clone();
    store.scans[0].lock().await.stream = Some(
        futures_util::stream::iter(metadata.into_iter().map(Ok))
            .inspect(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .boxed(),
    );
    objects
        .put(&store.pending_key("Parent:0062"), b"broken".to_vec().into())
        .await?;
    let mut found = std::collections::BTreeSet::new();
    for batch in 0..8 {
        let candidates = store.discover(false, 8, 0).await?;
        assert_eq!(candidates.len(), 8);
        assert_eq!(observed.load(Ordering::SeqCst), (batch + 1) * 8);
        for candidate in candidates {
            if candidate.scope == "Parent:0062" {
                assert!(store.load(&candidate).await.is_err());
            } else {
                assert!(store.load(&candidate).await?.is_some());
            }
            assert!(found.insert(candidate.scope));
        }
    }
    assert_eq!(found.len(), 64);
    assert!(store.discover(false, 8, 0).await?.is_empty());
    // A completed pass restarts, so late arrivals and failed records stay discoverable.
    assert!(store.discover(false, 8, 999).await?.is_empty());
    assert_eq!(store.discover(false, 8, 1_000).await?.len(), 8);
    store.close_scans().await;
    assert!(store.scans[0].lock().await.stream.is_none());
    Ok(())
}
