// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Capture and maintenance use the real SQLite WAL and restorable LTX chain.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{Db, FileReplicaClient, LtxHost, ReplicaClient, TXID};
use std::time::Duration;

fn aged_host() -> LtxHost {
    LtxHost::new(
        || 120_000,
        |_| Ok(Duration::from_secs(120)),
        |path| async move { std::fs::read(path) },
        |job| async move {
            job();
            Ok(())
        },
    )
}

async fn assert_restores(db: &mut Db, directory: &std::path::Path, value: i64) {
    let client = FileReplicaClient::new(directory.join("replica").to_str().unwrap());
    let position = db.pos().unwrap();
    for txid in 1..=position.txid.0 {
        let txid = TXID(txid);
        let data = db.read_ltx_file(0, txid, txid).unwrap();
        client.write_ltx_file(0, txid, txid, &data).await.unwrap();
    }
    let output = directory.join(format!("restored-{value}.sqlite"));
    cellar_ltx::restore(&client, &output, position.txid)
        .await
        .unwrap();
    let restored = rusqlite::Connection::open(output).unwrap();
    assert_eq!(
        restored
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        value
    );
}

#[tokio::test]
async fn aged_capture_defers_only_time_maintenance_and_preserves_restore() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.sqlite");
    let mut db = Db::open_with_host(&path, aged_host()).unwrap();
    let writer = rusqlite::Connection::open(path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (1)")
        .unwrap();
    db.capture().unwrap();
    assert_eq!(
        db.last_sync_timing().checkpoint_runs,
        0,
        "database age must not checkpoint in the proof capture path"
    );
    assert_restores(&mut db, directory.path(), 1).await;
    // A commit can arrive after capture, before the maintenance barrier.
    writer.execute("UPDATE counter SET value = 2", []).unwrap();
    assert!(db.checkpoint_if_due().unwrap());
    assert_restores(&mut db, directory.path(), 2).await;
    assert!(
        !db.checkpoint_if_due().unwrap(),
        "idle maintenance must not manufacture checkpoints"
    );
    writer.execute("UPDATE counter SET value = 3", []).unwrap();
    db.capture().unwrap();
    assert_eq!(db.last_sync_timing().checkpoint_runs, 0);
    assert_restores(&mut db, directory.path(), 3).await;
    db.close().unwrap();
}

#[tokio::test]
async fn capture_still_enforces_passive_and_truncate_size_thresholds() {
    for truncate in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("counter.sqlite");
        let mut db = Db::open_with_host(&path, aged_host()).unwrap();
        db.min_checkpoint_page_n = if truncate { u32::MAX } else { 1 };
        db.truncate_page_n = if truncate { 1 } else { 0 };
        let writer = rusqlite::Connection::open(path).unwrap();
        writer
            .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (1)")
            .unwrap();
        db.capture().unwrap();
        // TRUNCATE uses the previous captured WAL size. The second capture
        // exercises it even with no separately scheduled time maintenance.
        if truncate {
            db.capture().unwrap();
        }
        assert_eq!(db.last_sync_timing().checkpoint_runs, 1);
        assert_restores(&mut db, directory.path(), 1).await;
        db.close().unwrap();
    }
}

#[test]
fn ordinary_sync_keeps_its_existing_checkpoint_policy() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.sqlite");
    let mut db = Db::open_with_host(&path, aged_host()).unwrap();
    let writer = rusqlite::Connection::open(path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (1)")
        .unwrap();
    db.sync().unwrap();
    assert_eq!(db.last_sync_timing().checkpoint_runs, 1);
    db.close().unwrap();
}

#[tokio::test]
async fn pinned_reader_does_not_make_maintenance_omit_later_commits() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.sqlite");
    let mut db = Db::open_with_host(&path, aged_host()).unwrap();
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (1)")
        .unwrap();
    db.capture().unwrap();
    let reader = rusqlite::Connection::open(path).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    assert_eq!(
        reader
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    writer.execute("UPDATE counter SET value = 2", []).unwrap();
    db.capture().unwrap();
    assert!(db.checkpoint_if_due().unwrap());
    assert_restores(&mut db, directory.path(), 2).await;
    reader.execute_batch("ROLLBACK").unwrap();
    writer.execute("UPDATE counter SET value = 3", []).unwrap();
    db.capture().unwrap();
    assert!(db.checkpoint_if_due().unwrap());
    assert_restores(&mut db, directory.path(), 3).await;
    db.close().unwrap();
}

#[tokio::test]
async fn competing_checkpoint_before_maintenance_barrier_preserves_the_capture_chain() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.sqlite");
    let mut db = Db::open_with_host(&path, aged_host()).unwrap();
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (1)")
        .unwrap();
    db.capture().unwrap();
    // Use the existing checkpoint cut to force another writer to backfill
    // and restart the WAL after maintenance releases its read lock, before
    // maintenance acquires the sealing writer barrier.
    cellar_ltx::internal::db::checkpoint_passive_with_unlocked_hook(
        &mut db,
        Box::new(move || {
            let other = rusqlite::Connection::open(path).unwrap();
            other.execute_batch("UPDATE counter SET value = 2; PRAGMA wal_checkpoint(TRUNCATE); UPDATE counter SET value = 3").unwrap();
        }),
    ).unwrap();
    assert_restores(&mut db, directory.path(), 3).await;
    writer.execute("UPDATE counter SET value = 4", []).unwrap();
    db.capture().unwrap();
    assert_restores(&mut db, directory.path(), 4).await;
    db.close().unwrap();
}

#[test]
fn checkpoint_sealing_write_does_not_rearm_idle_maintenance() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.sqlite");
    let mut db = Db::open_with_host(&path, aged_host()).unwrap();
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (1)")
        .unwrap();
    db.capture().unwrap();
    let reader = rusqlite::Connection::open(path).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    reader
        .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))
        .unwrap();
    assert!(db.checkpoint_if_due().unwrap());
    let after_checkpoint = db.pos().unwrap();
    db.capture().unwrap();
    assert_eq!(
        db.pos().unwrap(),
        after_checkpoint,
        "maintenance must capture its own sealing write instead of handing it to a later proof"
    );
    assert!(
        !db.checkpoint_if_due().unwrap(),
        "capturing checkpoint control writes must not restart the idle checkpoint loop"
    );
    reader.execute_batch("ROLLBACK").unwrap();
    db.close().unwrap();
}
