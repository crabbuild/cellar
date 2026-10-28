// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test]
async fn sandbox_erasure_bounds_deletion_preserves_neighbors_and_blocks_restore_and_log_replay(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let objects = Arc::new(object_store::memory::InMemory::new());
            let backend = LtxRepl::start(
                directory.path(),
                ReplicaStorage::new(objects.clone(), ReplicaStorageOptions::default())?,
                ReplicationOptions {
                    flush_interval: Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let scope = "Sandbox:erased";
            for index in 0..101 {
                objects
                    .put(
                        &format!("cells/{scope}/objects/{index}").into(),
                        vec![42].into(),
                    )
                    .await?;
            }
            for key in [
                format!("cells/{scope}/own.json"),
                "cells/Sandbox:erased-neighbor/value".into(),
            ] {
                objects.put(&key.into(), vec![7].into()).await?;
            }
            assert!(backend.erase_retired_batch(scope).await.is_err());
            let retirements = crate::cell_runtime::retirement::Store::new(
                Some((objects.clone(), String::new())),
                directory.path(),
            );
            let record = crate::cell_runtime::retirement::Record::new(
                crate::cell_runtime::retirement::Evidence::Child {
                    scope: scope.into(),
                    identity_digest: "a".repeat(64),
                },
            )?;
            retirements.begin(&record).await?;
            assert!(!backend.erase_retired_batch(scope).await?);
            assert!(!backend.erase_retired_batch(scope).await?);
            assert!(backend.erase_retired_batch(scope).await?);
            assert!(objects
                .get(&format!("cells/{scope}/own.json").into())
                .await
                .is_ok());
            assert!(objects
                .get(&"cells/Sandbox:erased-neighbor/value".into())
                .await
                .is_ok());
            assert!(backend.restore_snapshot(scope).await.is_err());
            assert!(backend
                .activate(ActivationOptions {
                    cell: scope,
                    epoch: 2,
                    fresh: true,
                    took_over: false,
                    resume_local: false,
                    prior: None
                })
                .await
                .is_err());
            backend
                .upload_raw_l0(scope, 1, 1, 1, b"retired history")
                .await?;
            backend
                .upload_raw_l0(&format!("{scope}/facets/one"), 1, 1, 1, b"retired facet")
                .await?;
            assert!(
                backend.erase_retired_batch(scope).await?,
                "recovery recreated erased objects"
            );
            assert_eq!(backend.covered_txid(scope, 1, 1).await, u64::MAX);
            assert_eq!(
                backend
                    .observed_covered_txid(&format!("{scope}/facets/one"), 1)
                    .await,
                u64::MAX
            );
            // A delayed object PUT cannot reopen the identity and remains reclaimable.
            objects
                .put(&format!("cells/{scope}/late").into(), vec![42].into())
                .await?;
            assert!(!backend.erase_retired_batch(scope).await?);
            assert!(backend.erase_retired_batch(scope).await?);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
