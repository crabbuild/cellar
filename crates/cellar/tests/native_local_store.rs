// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real development-store operations must not turn a short metadata read into
//! the last connection's synchronous WAL checkpoint. Exercise the public bucket
//! boundary and reopen the store before checking conditional-write identity.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ExecutionDomain};
use std::io::Read as _;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cas_has_one_winner_before_and_after_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("objects.sqlite");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bucket = Bucket::open_dev(&database)?;
            let old = bucket
                .put_cas("owner", b"initial".to_vec(), None)
                .await?
                .context("initial CAS did not apply")?;
            let mut contenders = Vec::new();
            for index in 0_u8..32 {
                let bucket = bucket.clone();
                let old = old.clone();
                contenders.push(cellar::asyncrt::try_spawn(async move {
                    bucket.put_cas("owner", vec![index], Some(&old)).await
                })?);
            }
            let mut winners = Vec::new();
            for (index, result) in futures_util::future::join_all(contenders)
                .await
                .into_iter()
                .enumerate()
            {
                if let Some(token) = result?? {
                    winners.push((index as u8, token));
                }
            }
            ensure!(
                winners.len() == 1,
                "same-token CAS race applied {} times",
                winners.len()
            );
            let (index, token) = winners.pop().unwrap();
            drop(bucket);
            let reopened = Bucket::open_dev(&database)?;
            let (body, saved_token) = reopened.get("owner").await?.context("winner disappeared")?;
            ensure!(
                body.as_ref() == [index] && saved_token == token && saved_token != old,
                "reopen lost the single winning body/token"
            );
            ensure!(
                reopened
                    .put_cas("owner", b"stale".to_vec(), Some(&old))
                    .await?
                    .is_none(),
                "stale CAS applied after reopen"
            );
            ensure!(
                reopened
                    .put_cas("owner", b"next".to_vec(), Some(&token))
                    .await?
                    .is_some(),
                "winner token could not advance after reopen"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

/// This checks the library actually linked into native applications, not the
/// SQLite CLI or Python interpreter installed on the development machine.
#[test]
fn linked_sqlite_includes_wal_reset_fix() {
    eprintln!("linked SQLite {}", rusqlite::version());
    assert_eq!(
        rusqlite::version_number(),
        rusqlite::ffi::SQLITE_VERSION_NUMBER,
        "linked SQLite differs from the bundled binding headers"
    );
    assert!(
        rusqlite::version_number() >= 3_051_003,
        "linked SQLite {} predates the WAL-reset fix",
        rusqlite::version()
    );
}

#[test]
fn unsigned_sql_values_keep_checked_conversion() -> Result<()> {
    let connection = rusqlite::Connection::open_in_memory()?;
    for value in [9_007_199_254_740_993_u64, i64::MAX as u64] {
        let actual: u64 = connection.query_row("SELECT ?1", [value], |row| row.get(0))?;
        ensure!(actual == value, "unsigned SQL value lost precision");
    }
    ensure!(
        connection
            .query_row("SELECT -1", [], |row| row.get::<_, u64>(0))
            .is_err(),
        "negative SQL integer became unsigned"
    );
    ensure!(
        connection
            .query_row("SELECT ?1", [u64::MAX], |row| row.get::<_, i64>(0))
            .is_err(),
        "unsigned overflow reached SQLite"
    );
    Ok(())
}

#[tokio::test]
async fn operation_connections_leave_commits_in_wal_and_reopen_with_etags() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("objects.sqlite");
    let wal = directory.path().join("objects.sqlite-wal");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bucket = Bucket::open_dev(&database)?;
            let token = bucket
                .put_cas("nodes/owner.json", b"first".to_vec(), None)
                .await?
                .context("initial lease CAS did not apply")?;
            // The FULL commit is in the WAL. No operation connection remains;
            // closing an operation must leave checkpoint work to the normal
            // automatic checkpoint rather than acquiring an exclusive close lock.
            let committed =
                std::fs::read(&wal).context("operation close removed the committed WAL")?;
            ensure!(committed.len() > 32, "commit has no WAL frames");
            let main = std::fs::read(&database)?;
            for _ in 0..16 {
                let (body, etag) = bucket
                    .get("nodes/owner.json")
                    .await?
                    .context("lease disappeared")?;
                ensure!(
                    body.as_ref() == b"first" && etag == token,
                    "read changed lease identity"
                );
                ensure!(bucket.head("nodes/owner.json").await?.is_some());
            }
            ensure!(
                std::fs::read(&database)? == main,
                "read checkpointed the database"
            );
            ensure!(
                std::fs::read(&wal)? == committed,
                "read changed the committed WAL"
            );
            drop(bucket);

            let reopened = Bucket::open_dev(&database)?;
            let (body, etag) = reopened
                .get("nodes/owner.json")
                .await?
                .context("reopen lost the lease")?;
            ensure!(
                body.as_ref() == b"first" && etag == token,
                "reopen changed lease identity"
            );
            let next = reopened
                .put_cas("nodes/owner.json", b"second".to_vec(), Some(&token))
                .await?
                .context("reopened lease CAS did not apply")?;
            ensure!(next != token, "reopened CAS reused an ETag");
            ensure!(
                reopened
                    .put_cas("nodes/owner.json", b"stale".to_vec(), Some(&token))
                    .await?
                    .is_none(),
                "stale lease CAS overwrote a newer commit"
            );
            let (body, etag) = reopened
                .get("nodes/owner.json")
                .await?
                .context("updated lease disappeared")?;
            ensure!(
                body.as_ref() == b"second" && etag == next,
                "stale CAS changed the lease"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn small_writes_recycle_checkpointed_wal_between_operations() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("objects.sqlite");
    let wal = directory.path().join("objects.sqlite-wal");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bucket = Bucket::open_dev(&database)?;
            // Large bodies are staged in bounded transactions, so their upload
            // can checkpoint and recycle the WAL before publishing the object.
            // Retain this object's bytes and CAS identity through later recycling.
            let body = vec![0x5a; 8 << 20];
            let token = bucket
                .put_cas("large", body.clone(), None)
                .await?
                .context("initial large object CAS did not apply")?;
            let header = || -> Result<[u8; 32]> {
                let mut bytes = [0; 32];
                std::fs::File::open(&wal)?.read_exact(&mut bytes)?;
                Ok(bytes)
            };
            // Warm the same bounded overwrite workload through multiple automatic
            // checkpoint cycles. An arbitrary handful of one-byte writes after
            // the staged upload need not cross the 1,000-page checkpoint threshold.
            // No external connection or explicit checkpoint may preserve the WAL
            // index on behalf of the public store under test.
            for round in 0..8 {
                for index in 0..16 {
                    bucket
                        .put(
                            &format!("small-{index}"),
                            vec![round * 16 + index; 64 << 10],
                        )
                        .await?;
                }
            }
            let before = header()?;
            let initial_size = std::fs::metadata(&wal)?.len();
            // The WAL-index lifetime must follow surviving clones, rather than
            // just the handle that created the store.
            let cloned = bucket.clone();
            drop(bucket);
            for round in 8..16 {
                for index in 0..16 {
                    cloned
                        .put(
                            &format!("small-{index}"),
                            vec![round * 16 + index; 64 << 10],
                        )
                        .await?;
                }
            }
            let after = header()?;
            // SQLite changes the WAL header salts on recycling after a checkpoint.
            // File existence alone cannot detect repeated WAL-index recovery.
            ensure!(
                before[16..24] != after[16..24],
                "small writes did not recycle the checkpointed WAL"
            );
            ensure!(
                std::fs::metadata(&wal)?.len() <= initial_size,
                "small writes appended to the old large WAL"
            );
            drop(cloned);

            let reopened = Bucket::open_dev(&database)?;
            let (saved, etag) = reopened
                .get("large")
                .await?
                .context("large object disappeared after WAL recycling/reopen")?;
            ensure!(
                saved.as_ref() == body && etag == token,
                "WAL recycling changed the large object or ETag"
            );
            for index in 0..16 {
                let (saved, _) = reopened
                    .get(&format!("small-{index}"))
                    .await?
                    .context("small object disappeared after reopen")?;
                ensure!(
                    saved.as_ref() == vec![15 * 16 + index as u8; 64 << 10],
                    "small object changed"
                );
            }
            ensure!(
                reopened
                    .put_cas("large", b"next".to_vec(), Some(&token))
                    .await?
                    .is_some(),
                "recycled/reopened store lost conditional-write identity"
            );
            ensure!(
                reopened
                    .put_cas("large", b"stale".to_vec(), Some(&token))
                    .await?
                    .is_none(),
                "stale CAS applied after WAL recycling"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}
