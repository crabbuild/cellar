// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]

use super::*;
use crate::object_multipart::{MultipartMaintenancePolicy, ResumableMultipartStore};
use crate::sdk::ExecutionDomain;
use anyhow::{ensure, Result};
use object_store::multipart::MultipartStore;
use std::sync::atomic::{AtomicI64, Ordering};

fn policy() -> MultipartMaintenancePolicy {
    MultipartMaintenancePolicy {
        upload_ttl: Some(Duration::from_millis(100)),
        ..Default::default()
    }
}

fn domain(clock: &Arc<AtomicI64>) -> ExecutionDomain {
    let clock = clock.clone();
    ExecutionDomain::with_wall_clock(
        tokio::runtime::Handle::current(),
        Arc::new(move || clock.load(Ordering::SeqCst)),
    )
}

fn counts(store: &LocalStore, id: &str) -> Result<(usize, usize)> {
    Ok(store.connect()?.query_row(
        "SELECT (SELECT count(*) FROM __sys_local_uploads WHERE id = ?1),
                (SELECT count(*) FROM __sys_local_parts WHERE upload_id = ?1)",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}

#[tokio::test]
async fn expiry_survives_reconnect_policy_change_and_clock_rollback() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("objects.sqlite");
    let clock = Arc::new(AtomicI64::new(1_000));
    let owner = domain(&clock);
    let result = owner
        .run(async {
            let key = Path::from("archive");
            let store = LocalStore::open_with_multipart_policy(&database, policy())?;
            store
                .put(&key, Bytes::from_static(b"published").into())
                .await?;
            let id = store.create_multipart(&key).await?;
            let mut receipts = Vec::new();
            for index in 0..3 {
                receipts.push(
                    MultipartStore::put_part(
                        &store,
                        &key,
                        &id,
                        index,
                        Bytes::from_static(b"unpublished").into(),
                    )
                    .await?,
                );
            }
            drop(store);
            let store = LocalStore::open_with_multipart_policy(
                &database,
                MultipartMaintenancePolicy {
                    upload_ttl: None,
                    ..policy()
                },
            )?;
            let deadline: i64 = store.connect()?.query_row(
                "SELECT expires_ms FROM __sys_local_uploads WHERE id = ?1",
                [&id],
                |row| row.get(0),
            )?;
            ensure!(deadline == 1_100);
            clock.store(1_100, Ordering::SeqCst);
            ensure!(matches!(
                MultipartStore::put_part(&store, &key, &id, 0, Bytes::new().into()).await,
                Err(Error::NotFound { .. })
            ));
            clock.store(900, Ordering::SeqCst);
            ensure!(store
                .begin_part(&key, &id, 0, 1, Arc::new(()))
                .await
                .is_err());
            ensure!(store.complete_multipart(&key, &id, receipts).await.is_err());
            ensure!(
                MultipartStore::put_part(&store, &key, &id, 0, Bytes::new().into())
                    .await
                    .is_err()
            );
            let first = store.reap_expired("").await?;
            ensure!(first.deleted_parts == 1 && first.deleted_uploads == 0 && first.pending);
            ensure!(counts(&store, &id)? == (1, 2));
            // The first transaction is durable even when its maintenance caller
            // loses the response and a fresh client performs the next pass.
            drop(store);
            let store = LocalStore::open(&database)?;
            ensure!(store.reap_expired("").await?.pending);
            let last = store.reap_expired("").await?;
            ensure!(last.deleted_parts == 1 && last.deleted_uploads == 1 && !last.pending);
            ensure!(counts(&store, &id)? == (0, 0));
            ensure!(store.get(&key).await?.bytes().await? == b"published"[..]);
            let immortal = store.create_multipart(&key).await?;
            store.abort_multipart(&key, &immortal).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    owner.shutdown().await;
    result?
}

#[tokio::test]
async fn every_admission_commits_retirement_and_staged_completion_rechecks() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicI64::new(1_000));
    let owner = domain(&clock);
    let result = owner
        .run(async {
            let store = LocalStore::open_with_multipart_policy(
                directory.path().join("objects.sqlite"),
                policy(),
            )?;
            let key = Path::from("archive");
            for operation in ["begin", "complete", "stage"] {
                clock.store(1_000, Ordering::SeqCst);
                let id = store.create_multipart(&key).await?;
                let receipt = MultipartStore::put_part(
                    &store,
                    &key,
                    &id,
                    0,
                    Bytes::from_static(b"old").into(),
                )
                .await?;
                let mut stage = store.begin_part(&key, &id, 0, 1, Arc::new(())).await?;
                stage.write(Bytes::from_static(b"x")).await?;
                clock.store(1_100, Ordering::SeqCst);
                match operation {
                    "begin" => ensure!(store
                        .begin_part(&key, &id, 0, 0, Arc::new(()))
                        .await
                        .is_err()),
                    "complete" => ensure!(store
                        .complete_multipart(&key, &id, vec![receipt])
                        .await
                        .is_err()),
                    _ => ensure!(stage.complete().await.is_err()),
                }
                clock.store(900, Ordering::SeqCst);
                ensure!(
                    MultipartStore::put_part(&store, &key, &id, 0, Bytes::new().into())
                        .await
                        .is_err(),
                    "{operation} rolled back its expiry observation"
                );
                ensure!(counts(&store, &id)? == (1, 1));
                ensure!(store.get(&key).await.is_err());
                store.abort_multipart(&key, &id).await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    owner.shutdown().await;
    result?
}

#[tokio::test]
async fn collection_is_scope_exact_header_bounded_and_preserves_completed_objects() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicI64::new(1_000));
    let owner = domain(&clock);
    let result = owner
        .run(async {
            let store = LocalStore::open_with_multipart_policy(
                directory.path().join("objects.sqlite"),
                policy(),
            )?;
            let a = "tenant%25_雪/";
            let b = "tenant%25_雪/neighbor/";
            let key_a = Path::parse(format!("{a}archive"))?;
            let key_b = Path::parse(format!("{b}archive"))?;
            let mut a_ids = Vec::new();
            for _ in 0..65 {
                a_ids.push(store.create_scoped(&key_a, Default::default(), a).await?);
            }
            let foreign = store.create_scoped(&key_b, Default::default(), b).await?;
            ensure!(store.abort_multipart(&key_a, &foreign).await.is_err());
            ensure!(store
                .create_scoped(&key_a, Default::default(), b)
                .await
                .is_err());
            let completed = store.create_scoped(&key_a, Default::default(), a).await?;
            let receipt = MultipartStore::put_part(
                &store,
                &key_a,
                &completed,
                0,
                Bytes::from_static(b"complete").into(),
            )
            .await?;
            clock.store(1_099, Ordering::SeqCst);
            store
                .complete_multipart(&key_a, &completed, vec![receipt])
                .await?;
            clock.store(1_100, Ordering::SeqCst);
            let first = store.reap_expired(a).await?;
            ensure!(first.retired_uploads == 64 && first.deleted_uploads == 64 && first.pending);
            ensure!(counts(&store, &foreign)? == (1, 0));
            let last = store.reap_expired(a).await?;
            ensure!(last.retired_uploads == 1 && last.deleted_uploads == 1 && !last.pending);
            ensure!(store.get(&key_a).await?.bytes().await? == b"complete"[..]);
            ensure!(
                store.reap_expired("").await? == Default::default(),
                "root scan crossed exact scopes"
            );
            ensure!(counts(&store, &foreign)? == (1, 0));
            ensure!(store.reap_expired(b).await?.deleted_uploads == 1);
            for id in a_ids {
                ensure!(counts(&store, &id)? == (0, 0));
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    owner.shutdown().await;
    result?
}

#[test]
fn incompatible_schema_refuses_without_migration_or_data_loss() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("objects.sqlite");
    let connection = Connection::open(&database)?;
    connection.execute_batch("CREATE TABLE __sys_local_uploads(id TEXT PRIMARY KEY, key TEXT NOT NULL, attributes TEXT NOT NULL);
        INSERT INTO __sys_local_uploads VALUES ('old', 'archive', 'metadata');")?;
    let error = LocalStore::open(&database).unwrap_err();
    ensure!(error
        .to_string()
        .contains("incompatible local multipart schema"));
    let value: String = connection.query_row(
        "SELECT attributes FROM __sys_local_uploads WHERE id = 'old'",
        [],
        |row| row.get(0),
    )?;
    ensure!(value == "metadata");
    let objects: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '__sys_local_objects')",
        [],
        |row| row.get(0),
    )?;
    ensure!(!objects, "failed open created a partial new schema");
    Ok(())
}

#[tokio::test]
async fn disabled_deadlines_and_overflow_refuse_without_allocating() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicI64::new(i64::MAX - 50));
    let owner = domain(&clock);
    let result = owner
        .run(async {
            let database = directory.path().join("objects.sqlite");
            let store = LocalStore::open_with_multipart_policy(&database, policy())?;
            let key = Path::from("archive");
            ensure!(store.create_multipart(&key).await.is_err());
            let total: usize = store.connect()?.query_row(
                "SELECT count(*) FROM __sys_local_uploads",
                [],
                |row| row.get(0),
            )?;
            ensure!(total == 0);
            let store = LocalStore::open_with_multipart_policy(
                &database,
                MultipartMaintenancePolicy {
                    upload_ttl: None,
                    ..policy()
                },
            )?;
            let id = store.create_multipart(&key).await?;
            ensure!(!store.reap_expired("").await?.pending);
            ensure!(counts(&store, &id)? == (1, 0));
            store.abort_multipart(&key, &id).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    owner.shutdown().await;
    result??;
    ensure!(LocalStore::open_with_multipart_policy(
        directory.path().join("invalid.sqlite"),
        MultipartMaintenancePolicy {
            upload_ttl: Some(Duration::ZERO),
            ..policy()
        }
    )
    .is_err());
    ensure!(!directory.path().join("invalid.sqlite").exists());
    Ok(())
}

#[tokio::test]
async fn lost_maintenance_waiter_commits_one_pass_and_fresh_scans_converge() -> Result<()> {
    use std::future::Future as _;

    let directory = tempfile::tempdir()?;
    let clock = Arc::new(AtomicI64::new(1_000));
    let owner = domain(&clock);
    let result = owner
        .run(async {
            let store = LocalStore::open_with_multipart_policy(
                directory.path().join("objects.sqlite"),
                policy(),
            )?;
            let key = Path::from("archive");
            let id = store.create_multipart(&key).await?;
            for index in 0..3 {
                MultipartStore::put_part(
                    &store,
                    &key,
                    &id,
                    index,
                    Bytes::from_static(b"orphan").into(),
                )
                .await?;
            }
            clock.store(1_100, Ordering::SeqCst);
            let blocked = store.connect()?;
            blocked.execute_batch("BEGIN IMMEDIATE")?;
            let mut pending = Box::pin(store.reap_expired(""));
            let first =
                std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx))).await;
            ensure!(first.is_pending());
            drop(pending);
            blocked.execute_batch("COMMIT")?;
            tokio::time::timeout(Duration::from_secs(5), async {
                while counts(&store, &id)? != (1, 2) {
                    tokio::task::yield_now().await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            clock.store(900, Ordering::SeqCst);
            ensure!(store.reap_expired("").await?.deleted_parts == 1);
            let last = store.reap_expired("").await?;
            ensure!(last.deleted_uploads == 1 && !last.pending);
            ensure!(counts(&store, &id)? == (0, 0));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    owner.shutdown().await;
    result?
}

#[tokio::test]
async fn host_loop_uses_canonical_scope_and_joins_on_shutdown() -> Result<()> {
    use crate::bucket::{BlobAttributes, Bucket};

    let directory = tempfile::tempdir()?;
    let database = directory.path().join("objects.sqlite");
    let clock = Arc::new(AtomicI64::new(1_000));
    let owner = domain(&clock);
    let result = owner
        .run(async {
            let mut bucket = Bucket::open_dev_with_multipart_policy(
                &database,
                MultipartMaintenancePolicy {
                    idle_interval: Duration::from_millis(5),
                    backlog_interval: Duration::from_millis(1),
                    ..policy()
                },
            )?;
            bucket.prefix = "tenant%_雪".into();
            let mut neighbor = bucket.clone();
            neighbor.prefix.push_str("/neighbor");
            let own = bucket
                .create_resumable_multipart("archive", &BlobAttributes::default())
                .await?;
            let foreign = neighbor
                .create_resumable_multipart("archive", &BlobAttributes::default())
                .await?;
            let observer = LocalStore::open(&database)?;
            crate::host_channels::set_blob_store(bucket);
            // A second registration must not start maintenance for another scope.
            crate::host_channels::set_blob_store(neighbor.clone());
            clock.store(1_100, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(5), async {
                while counts(&observer, &own)? != (0, 0) {
                    tokio::task::yield_now().await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            ensure!(counts(&observer, &foreign)? == (1, 0));
            neighbor
                .abort_resumable_multipart("archive", &foreign)
                .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    tokio::time::timeout(Duration::from_secs(5), owner.shutdown()).await?;
    result?
}
