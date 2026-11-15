// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{cell_group::CellId, consensus::IncarnationId};

#[tokio::test]
async fn command_handoff_pending_roots_and_late_puts_preserve_their_owners() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = domain
        .run(async {
            let pool = CellGroupPayloadPool::new(Default::default())?;
            let remote = Arc::new(object_store::memory::InMemory::new());
            let history = CellHistory {
                cluster: ClusterId::new([2; 16])?,
                namespace: NamespaceId::new([3; 16])?,
                cell: CellId::new("Counter:handoff")?,
                incarnation: IncarnationId::new([4; 16])?,
            };
            let store = pool.bind(
                remote.clone(),
                Path::from("namespace"),
                history.cluster,
                history.namespace,
            );
            let timeout = Duration::from_secs(3);
            let bytes = bytes::Bytes::from_static(b"retained across source archive lag");
            let published = store.publish(&history, bytes.clone(), timeout).await?;
            let original = store.retain(&published, [5; 32], timeout).await?;
            store
                .protect_command(&history, published.reference(), [6; 32], false, timeout)
                .await?;
            store.release(&original, timeout).await?;
            // A registry identifier cannot turn a command-generation pin into
            // an archive pin, even when its bytes match the reserved scope.
            store
                .retire_archive_roots(
                    COMMAND_SCOPE,
                    3,
                    &BTreeSet::new(),
                    &BTreeSet::new(),
                    256,
                    timeout,
                )
                .await?;
            assert_eq!(
                store
                    .collect(Default::default(), timeout)
                    .await?
                    .deleted_objects,
                0
            );
            assert_eq!(
                store
                    .get(&history, published.reference(), timeout)
                    .await?
                    .as_ref(),
                bytes.as_ref()
            );
            let root = PayloadRootOwner {
                scope: [7; 32],
                identity: [8; 32],
                generation: 2,
                confirmed: true,
            };
            store
                .pin_archive(
                    &history,
                    published.reference(),
                    root.clone(),
                    false,
                    timeout,
                )
                .await?;
            store
                .adopt_command(published.reference(), [6; 32], timeout)
                .await?;
            store
                .retire_archive_roots(
                    root.scope,
                    3,
                    &BTreeSet::from([2]),
                    &BTreeSet::new(),
                    256,
                    timeout,
                )
                .await?;
            assert_eq!(
                store
                    .collect(Default::default(), timeout)
                    .await?
                    .protected_objects,
                1
            );
            store
                .retire_archive_roots(
                    root.scope,
                    3,
                    &BTreeSet::new(),
                    &BTreeSet::new(),
                    256,
                    timeout,
                )
                .await?;
            assert_eq!(
                store
                    .collect(Default::default(), timeout)
                    .await?
                    .deleted_objects,
                1
            );
            assert!(matches!(
                store.get(&history, published.reference(), timeout).await,
                Err(CellGroupPayloadError::Retired)
            ));
            // A PUT already entered before retirement may appear after its first
            // DELETE. The permanent tombstone must make a subsequent pass remove it.
            remote
                .put(&store.path(published.reference()), bytes.clone().into())
                .await?;
            assert_eq!(
                store
                    .collect(Default::default(), timeout)
                    .await?
                    .deleted_objects,
                1
            );
            assert!(matches!(
                remote.get(&store.path(published.reference())).await,
                Err(object_store::Error::NotFound { .. })
            ));
            assert_eq!(pool.usage().retained_bytes, 0);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    outcome
}

#[tokio::test]
async fn incomplete_inventory_and_corrupt_protected_bytes_refuse_before_delete(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = domain
        .run(async {
            let pool = CellGroupPayloadPool::new(Default::default())?;
            let remote = Arc::new(object_store::memory::InMemory::new());
            let history = CellHistory {
                cluster: ClusterId::new([2; 16])?,
                namespace: NamespaceId::new([3; 16])?,
                cell: CellId::new("Counter:inventory")?,
                incarnation: IncarnationId::new([4; 16])?,
            };
            let store = pool.bind(
                remote.clone(),
                Path::from("namespace"),
                history.cluster,
                history.namespace,
            );
            let timeout = Duration::from_secs(3);
            let garbage = store
                .publish(&history, bytes::Bytes::from_static(b"orphan"), timeout)
                .await?;
            store.abort_upload(&garbage, timeout).await?;
            let protected = store
                .publish(&history, bytes::Bytes::from_static(b"protected"), timeout)
                .await?;
            store.retain(&protected, [5; 32], timeout).await?;
            assert!(matches!(
                store
                    .collect(
                        CellGroupPayloadCollectionLimits {
                            max_scan_objects: 1,
                            max_delete_objects: 1
                        },
                        timeout
                    )
                    .await,
                Err(CellGroupPayloadError::Overloaded)
            ));
            assert!(remote.head(&store.path(garbage.reference())).await.is_ok());
            remote
                .put(
                    &store.path(protected.reference()),
                    bytes::Bytes::from_static(b"corrupted").into(),
                )
                .await?;
            assert!(matches!(
                store.collect(Default::default(), timeout).await,
                Err(CellGroupPayloadError::Invalid(_))
            ));
            assert!(remote.head(&store.path(garbage.reference())).await.is_ok());
            remote
                .put(
                    &store.path(protected.reference()),
                    bytes::Bytes::from_static(b"protected").into(),
                )
                .await?;
            assert_eq!(
                store
                    .collect(Default::default(), timeout)
                    .await?
                    .deleted_objects,
                1
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    outcome
}
