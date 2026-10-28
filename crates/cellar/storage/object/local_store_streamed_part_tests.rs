// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]

use super::*;
use crate::{object_multipart::ResumableMultipartStore, sdk::ExecutionDomain};
use object_store::multipart::MultipartStore;
use sha2::{Digest, Sha256};

#[tokio::test]
async fn streamed_parts_publish_exact_bytes_and_preserve_prior_receipts_on_failure(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = LocalStore::open(directory.path().join("objects.db"))?;
            let key = Path::from("blob/streamed/key");
            let id = store.create_multipart(&key).await?;
            let old =
                MultipartStore::put_part(&store, &key, &id, 0, b"old".to_vec().into()).await?;
            let capacity = Arc::new(tokio::sync::Semaphore::new(16));
            let lease = Arc::new(capacity.clone().try_acquire_many_owned(16)?);
            let mut short = store.begin_part(&key, &id, 0, 4, lease).await?;
            short.write(Bytes::from_static(b"bad")).await?;
            assert!(short.complete().await.is_err());
            assert_eq!(capacity.available_permits(), 16);
            let prior: String = store.connect()?.query_row(
                "SELECT etag FROM __sys_local_parts WHERE upload_id=?1 AND part_index=0",
                [&id],
                |row| row.get(0),
            )?;
            assert_eq!(prior, old.content_id);
            assert!(store
                .begin_part(&Path::from("foreign"), &id, 0, 4, Arc::new(()))
                .await
                .is_err());
            let length = 5 * 1024 * 1024 + 17;
            let mut part = store
                .begin_part(&key, &id, 0, length as u64, Arc::new(()))
                .await?;
            let mut hash = Sha256::new();
            let mut offset = 0;
            while offset < length {
                let count = (64 * 1024).min(length - offset);
                let bytes = Bytes::from(
                    (offset..offset + count)
                        .map(|i| (i % 251) as u8)
                        .collect::<Vec<_>>(),
                );
                hash.update(&bytes);
                part.write(bytes).await?;
                offset += count;
            }
            assert!(part.write(Bytes::from_static(b"excess")).await.is_err());
            let receipt = part.complete().await?;
            assert_eq!(receipt.content_id, hex::encode(hash.finalize()));
            store.complete_multipart(&key, &id, vec![receipt]).await?;
            let bytes = store.get(&key).await?.bytes().await?;
            assert_eq!(bytes.len(), length);
            assert!(bytes
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == (index % 251) as u8));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn cancelled_part_completion_retains_spool_capacity_through_entered_sqlite_work(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = LocalStore::open(directory.path().join("objects.db"))?;
            let key = Path::from("blob/streamed/cancelled");
            let id = store.create_multipart(&key).await?;
            let capacity = Arc::new(tokio::sync::Semaphore::new(16));
            let lease = Arc::new(capacity.clone().try_acquire_many_owned(16)?);
            let mut part = store.begin_part(&key, &id, 0, 3, lease).await?;
            let blocked = store.connect()?;
            blocked.execute_batch("BEGIN IMMEDIATE")?;
            // Scratch writes must finish while an unrelated SQLite writer is held.
            tokio::time::timeout(
                Duration::from_secs(5),
                part.write(Bytes::from_static(b"new")),
            )
            .await??;
            let mut pending = part.complete();
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            drop(pending);
            assert_eq!(capacity.available_permits(), 0);
            blocked.execute_batch("COMMIT")?;
            tokio::time::timeout(Duration::from_secs(5), async {
                while capacity.available_permits() != 16 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            let bytes: Vec<u8> = store.connect()?.query_row(
                "SELECT body FROM __sys_local_parts WHERE upload_id=?1",
                [&id],
                |row| row.get(0),
            )?;
            assert_eq!(bytes, b"new");
            store.abort_multipart(&key, &id).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}
