// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{cell_group::CellId, consensus::IncarnationId};
use futures_util::FutureExt;

#[tokio::test]
async fn resolved_payload_capture_keeps_the_shared_bank_and_isolates_ordinary_callers(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = domain
        .run(async {
            let pool = CellGroupPayloadPool::new(Default::default())?;
            let history = CellHistory {
                cluster: ClusterId::new([2; 16])?,
                namespace: NamespaceId::new([3; 16])?,
                cell: CellId::new("Counter:captured")?,
                incarnation: IncarnationId::new([4; 16])?,
            };
            let ordinary = pool.bind(
                Arc::new(object_store::memory::InMemory::new()),
                Path::from("namespace"),
                history.cluster,
                history.namespace,
            )?;
            let timeout = Duration::from_secs(5);
            let body = bytes::Bytes::from_static(b"actual payload");
            let value = ordinary.publish(&history, body.clone(), timeout).await?;
            let mut held = (0..CellGroupPayloadLimits::default().max_operations)
                .map(|_| pool.acquire(1).unwrap())
                .collect::<Vec<_>>();
            let capture = capacity::Capture::new();
            let mut observed = pool.clone();
            observed.capture_capacity(capture.clone());
            let resolved = observed.resolve(history.cluster, history.namespace)?;
            assert!(matches!(
                resolved.get(&history, value.reference(), timeout).await,
                Err(CellGroupPayloadError::Overloaded)
            ));
            let mut released = Box::pin(
                capture
                    .take()
                    .expect("actual resolved GET refusal")
                    .released(),
            );
            assert!(released.as_mut().now_or_never().is_none());
            assert!(matches!(
                ordinary.get(&history, value.reference(), timeout).await,
                Err(CellGroupPayloadError::Overloaded)
            ));
            assert!(
                capture.take().is_none(),
                "ordinary caller inherited the job's capture"
            );
            // Joined work refunds its operation slot while retaining its bytes.
            let retained = held.pop().unwrap().retain_bytes(1).unwrap();
            released.await;
            assert_eq!(
                pool.usage().operations,
                CellGroupPayloadLimits::default().max_operations - 1
            );
            assert_eq!(
                pool.usage().retained_bytes,
                CellGroupPayloadLimits::default().max_operations
            );
            // A hint grants no reservation: a real competitor can take the credit.
            let competing = pool.acquire(1).unwrap();
            assert!(matches!(
                resolved.get(&history, value.reference(), timeout).await,
                Err(CellGroupPayloadError::Overloaded)
            ));
            drop((held, competing));
            let actual = resolved.get(&history, value.reference(), timeout).await?;
            assert_eq!(actual.as_ref(), body.as_ref());
            drop(actual);
            assert_eq!(pool.usage().operations, 0);
            assert_eq!(pool.usage().retained_bytes, 1);
            drop(retained);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    outcome
}

#[tokio::test]
async fn namespace_resolution_uses_the_bound_backend_and_refuses_aliases() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = domain
        .run(async {
            let pool = CellGroupPayloadPool::new(Default::default())?;
            let first = Arc::new(object_store::memory::InMemory::new());
            let second = Arc::new(object_store::memory::InMemory::new());
            let cluster = ClusterId::new([2; 16])?;
            let a = NamespaceId::new([3; 16])?;
            let b = NamespaceId::new([8; 16])?;
            pool.bind(first.clone(), Path::from("source"), cluster, a)?;
            assert!(pool.resolve(cluster, b).is_err());
            assert!(matches!(
                pool.bind(second.clone(), Path::from("source"), cluster, a),
                Err(CellGroupPayloadError::Conflict)
            ));
            let target = pool.bind(second.clone(), Path::from("foreign-catalog"), cluster, b)?;
            let history = CellHistory {
                cluster,
                namespace: b,
                cell: CellId::new("Counter:foreign")?,
                incarnation: IncarnationId::new([4; 16])?,
            };
            let bytes = bytes::Bytes::from_static(b"actual target backend");
            let timeout = Duration::from_secs(3);
            let value = target.publish(&history, bytes.clone(), timeout).await?;
            let resolved = pool.resolve(cluster, b)?;
            assert_eq!(
                resolved
                    .get(&history, value.reference(), timeout)
                    .await?
                    .as_ref(),
                bytes.as_ref()
            );
            // Identical replicas in the source store cannot substitute for the
            // backend execution/recovery was configured to use for this namespace.
            let control = target.control_path(value.reference());
            first
                .put(&control, second.get(&control).await?.bytes().await?.into())
                .await?;
            first
                .put(&target.path(value.reference()), bytes.into())
                .await?;
            second.delete(&target.path(value.reference())).await?;
            assert!(matches!(
                resolved.get(&history, value.reference(), timeout).await,
                Err(CellGroupPayloadError::Missing)
            ));
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    outcome
}

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
            )?;
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
            )?;
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
