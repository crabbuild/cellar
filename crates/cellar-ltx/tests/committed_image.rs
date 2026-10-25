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
