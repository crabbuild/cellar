// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Historical coverage is a cache, not a process-lifetime epoch inventory.
use super::*;

#[tokio::test]
async fn fixed_population_epoch_churn_keeps_coverage_memory_bounded() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let storage = ReplicaStorage::new(
                Arc::new(object_store::memory::InMemory::new()),
                ReplicaStorageOptions::default(),
            )?;
            let backend = LtxRepl::start(
                directory.path(),
                storage,
                ReplicationOptions {
                    flush_interval: Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
            )?;
            for epoch in 1..=128 {
                for cell in 0..128 {
                    backend.note_covered(&format!("Counter:churn-{cell}"), epoch, epoch);
                }
                assert!(
                    backend.covered_by_cell.lock().unwrap().len() <= 4096,
                    "historical epochs grew beyond the coverage cache budget"
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn evicted_coverage_reloads_durable_state_and_retries_failed_reads() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let directory = tempfile::tempdir()?;
            let database = directory.path().join("objects.sqlite");
            let backup = directory.path().join("unavailable.sqlite");
            let store = Arc::new(crate::local_store::LocalStore::open(&database)?);
            let backend = LtxRepl::start(
                &directory.path().join("node"),
                ReplicaStorage::new(store, ReplicaStorageOptions::default())?,
                ReplicationOptions {
                    coverage_cache_entries: 1,
                    flush_interval: Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let cell = "Counter:coverage";
            let key = (cell.to_owned(), 1);
            let active = backend
                .activate(ActivationOptions {
                    cell,
                    epoch: 1,
                    fresh: true,
                    took_over: false,
                    resume_local: false,
                    prior: None,
                })
                .await?;
            let host = backend.ltx_host.clone();
            host.run_blocking(move || -> anyhow::Result<()> {
                let writer = rusqlite::Connection::open(active.path)?;
                writer.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES (41)")?;
                Ok(())
            })
            .await??;
            assert!(matches!(
                backend.sync_wait(cell, 1, Duration::from_secs(5)).await,
                SyncWait::Durable
            ));
            backend.release(cell, 1).await?;
            assert!(!backend.cells.lock().unwrap().contains_key(&key));
            let through = backend.covered_txid(cell, 1, 1).await;
            assert!(through > 0 && through < u64::MAX);
            backend.note_covered("Counter:other", 1, 0);
            assert!(backend.covered_by_cell.lock().unwrap().get(&key).is_none());
            // Remove only the disposable object-store database. Reads open a new,
            // uninitialized database and fail; they must not cache zero coverage.
            let filesystem = host.filesystem();
            let original = database.clone();
            let held = backup.clone();
            host.run_blocking(move || filesystem.rename(&original, &held))
                .await??;
            assert!(backend
                .client_for(cell, 1)
                .max_txid_all_levels()
                .await
                .is_err());
            assert_eq!(backend.covered_txid(cell, 1, through).await, 0);
            assert!(backend.covered_by_cell.lock().unwrap().get(&key).is_none());
            let filesystem = host.filesystem();
            host.run_blocking(move || filesystem.rename(&backup, &database))
                .await??;
            assert_eq!(backend.covered_txid(cell, 1, through).await, through);
            assert_eq!(
                backend.covered_by_cell.lock().unwrap().get(&key),
                Some(&through)
            );
            let snapshot = backend
                .restore_snapshot(cell)
                .await?
                .expect("durable snapshot after cache eviction");
            let restored = host
                .run_blocking(move || -> anyhow::Result<i64> {
                    let connection = rusqlite::Connection::open(snapshot.path())?;
                    Ok(connection.query_row("SELECT n FROM value", [], |row| row.get(0))?)
                })
                .await??;
            assert_eq!(restored, 41);
            // A successful empty listing is cacheable, unlike the failed listing.
            assert_eq!(backend.covered_txid("Counter:empty", 1, 1).await, 0);
            assert_eq!(
                backend
                    .covered_by_cell
                    .lock()
                    .unwrap()
                    .get(&("Counter:empty".into(), 1)),
                Some(&0)
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
