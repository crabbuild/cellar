// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical main-file/WAL inspection agrees with SQLite's own pager view across
//! transactions, rollback, checkpoints, page reuse and database truncation.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{CheckpointMode, Db};
use rusqlite::{Connection, DatabaseName};
use sha2::{Digest, Sha256};

fn compare(db: &Db, writer: &Connection) {
    let bytes = writer.serialize(DatabaseName::Main).unwrap();
    let page_size: u32 = writer
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        db.committed_image_digest(bytes.len() as u64, page_size, 64 * 1024 * 1024)
            .unwrap(),
        <[u8; 32]>::from(Sha256::digest(&*bytes)),
    );
}

#[test]
fn bounded_image_digest_matches_sqlite_across_wal_lifecycles() {
    for page_size in [512, 4096, 65536] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("main.sqlite");
        let writer = Connection::open(&path).unwrap();
        writer.pragma_update(None, "page_size", page_size).unwrap();
        writer
            .execute_batch("PRAGMA auto_vacuum=FULL; VACUUM;")
            .unwrap();
        let mut db = Db::open_disposable_with_host(&path, Default::default(), None).unwrap();
        writer
            .execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB);")
            .unwrap();
        for id in 1..=8 {
            writer
                .execute("INSERT INTO items VALUES(?1,zeroblob(131072))", [id])
                .unwrap();
            compare(&db, &writer);
        }
        db.capture().unwrap();
        compare(&db, &writer);
        writer
            .execute_batch("BEGIN; UPDATE items SET value=randomblob(65536); ROLLBACK;")
            .unwrap();
        compare(&db, &writer);
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ] {
            db.checkpoint(mode).unwrap();
            compare(&db, &writer);
            writer
                .execute("DELETE FROM items WHERE id=(SELECT max(id) FROM items)", [])
                .unwrap();
            db.capture().unwrap();
            compare(&db, &writer);
        }
        writer
            .execute_batch("UPDATE items SET value=randomblob(131072); VACUUM;")
            .unwrap();
        db.capture().unwrap();
        compare(&db, &writer);
        assert!(db
            .committed_image_digest(512, page_size, 64 * 1024 * 1024)
            .is_err());
        assert!(db
            .committed_image_digest(10_000_000_001, page_size, 64 * 1024 * 1024)
            .is_err());
        assert!(db.committed_image_digest(512, page_size, 31).is_err());
        drop(writer);
        db.close().unwrap();
    }
}

#[test]
fn committed_image_refuses_corrupt_or_incomplete_latest_commit_at_the_same_size() {
    let root = tempfile::tempdir().unwrap();
    let db =
        Db::open_disposable_with_host(root.path().join("main.sqlite"), Default::default(), None)
            .unwrap();
    let writer = Connection::open(db.path()).unwrap();
    writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1);")
        .unwrap();
    compare(&db, &writer);
    let length = writer.serialize(DatabaseName::Main).unwrap().len() as u64;
    writer.execute("UPDATE counter SET value=2", []).unwrap();
    assert_eq!(
        writer.serialize(DatabaseName::Main).unwrap().len() as u64,
        length
    );
    let wal = std::fs::read(db.wal_path()).unwrap();
    let mut corrupt = wal.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    let mut salt = wal.clone();
    let final_header = wal.len() - db.page_size() as usize - 24;
    salt[final_header + 8] ^= 1;
    for bytes in [&corrupt[..], &salt[..], &wal[..wal.len() - 1]] {
        // The current SQLite WAL index still names the newer commit. An
        // unchanged database length cannot authorize falling back to an older
        // complete transaction after a bad checksum, salt or physical cut.
        std::fs::write(db.wal_path(), bytes).unwrap();
        let result = db.committed_image_digest(length, db.page_size(), 64 * 1024 * 1024);
        std::fs::write(db.wal_path(), &wal).unwrap();
        assert!(
            result.is_err(),
            "latest committed frame must be present and valid"
        );
    }
    compare(&db, &writer);
    drop(writer);
    db.close().unwrap();
}

#[test]
fn committed_image_excludes_uncommitted_spilled_frames() {
    let root = tempfile::tempdir().unwrap();
    let db =
        Db::open_disposable_with_host(root.path().join("main.sqlite"), Default::default(), None)
            .unwrap();
    let writer = Connection::open(db.path()).unwrap();
    writer
        .execute_batch(
            "PRAGMA wal_autocheckpoint=0; PRAGMA cache_size=16;
        CREATE TABLE payload(value BLOB); INSERT INTO payload VALUES(zeroblob(1048576));",
        )
        .unwrap();
    let bytes = writer.serialize(DatabaseName::Main).unwrap();
    let expected: [u8; 32] = Sha256::digest(&*bytes).into();
    // Keep the committed WAL snapshot pinned. Without a reader SQLite may
    // restart the WAL and overwrite its existing allocation, so file growth
    // would not reliably distinguish new uncommitted frames.
    let committed = Connection::open(db.path()).unwrap();
    committed
        .execute_batch("BEGIN; SELECT value FROM payload;")
        .unwrap();
    let before = std::fs::metadata(db.wal_path()).unwrap().len();
    writer
        .execute_batch("BEGIN; UPDATE payload SET value=randomblob(1048576);")
        .unwrap();
    // Cache pressure alone does not guarantee SQLite spills on every run.
    // Explicitly flush dirty pages while keeping the transaction uncommitted,
    // so the physical WAL assertion below establishes the tested condition.
    writer.cache_flush().unwrap();
    assert!(std::fs::metadata(db.wal_path()).unwrap().len() > before);
    assert!(db
        .committed_image_digest(bytes.len() as u64, db.page_size(), 64 * 1024 * 1024)
        .is_err());
    writer.execute_batch("ROLLBACK;").unwrap();
    assert_eq!(
        db.committed_image_digest(bytes.len() as u64, db.page_size(), 64 * 1024 * 1024)
            .unwrap(),
        expected
    );
    compare(&db, &writer);
    drop(committed);
    drop(writer);
    db.close().unwrap();
}
