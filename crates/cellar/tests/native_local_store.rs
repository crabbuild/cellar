// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real development-store operations must not turn a short metadata read into
//! the last connection's synchronous WAL checkpoint. Exercise the public bucket
//! boundary and reopen the store before checking conditional-write identity.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ExecutionDomain};

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
            // The FULL commit is in the WAL. No connection remains checked out;
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
