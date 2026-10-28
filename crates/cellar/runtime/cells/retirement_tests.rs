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
    assert_eq!(store.next().await?, Some(record.clone()));
    assert_eq!(
        store.get("Parent:alice/facets/one").await?,
        Some(record.clone())
    );
    assert_eq!(store.status("Parent:alice").await?, Some("pending"));
    store.begin(&record).await?;
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
    assert_eq!(restarted.next().await?, None);
    assert_eq!(restarted.next().await?, Some(record.clone()));
    assert_eq!(restarted.get("Parent:alice2").await?, None);
    objects
        .put(&key("tenant/", "Parent:alice"), b"invalid".to_vec().into())
        .await?;
    assert!(restarted.get("Parent:alice").await.is_err());
    assert!(restarted.status("Parent:alice").await.is_err());
    Ok(())
}
