// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compare independently observed extent roots with SQLite's actual pager.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    internal::{
        db::{observe_extents, ExtentObservation},
        ExtentId, ExtentManifest, Geometry, ImageBinding, EXTENT_BYTES,
    },
    CheckpointMode, Db,
};
use rusqlite::{Connection, MAIN_DB};
use std::time::Duration;

fn binding(txid: u64) -> ImageBinding {
    ImageBinding {
        history: [1; 32],
        schema: [2; 32],
        epoch: 3,
        txid,
        checksum: 0,
    }
}

fn fixture(path: &std::path::Path, page_size: u32) -> (Db, Connection) {
    let setup = Connection::open(path).unwrap();
    setup.pragma_update(None, "page_size", page_size).unwrap();
    setup
        .execute_batch("PRAGMA auto_vacuum=FULL; VACUUM;")
        .unwrap();
    drop(setup);
    let mut db = Db::open_disposable_with_host(path, Default::default(), None).unwrap();
    db.min_checkpoint_page_n = u32::MAX;
    db.truncate_page_n = 0;
    db.checkpoint_interval = Duration::ZERO;
    let writer = Connection::open(path).unwrap();
    writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    writer
        .execute_batch(
            "CREATE TABLE payload(id INTEGER PRIMARY KEY, value BLOB);
         CREATE TABLE counter(value INTEGER);
         INSERT INTO counter VALUES(0);
         BEGIN;",
        )
        .unwrap();
    for id in 1..=8 {
        writer
            .execute("INSERT INTO payload VALUES(?1,zeroblob(1048576))", [id])
            .unwrap();
    }
    writer.execute_batch("COMMIT;").unwrap();
    (db, writer)
}

fn compare(
    db: &Db,
    writer: &Connection,
    previous: Option<&ExtentObservation>,
    txid: u64,
) -> ExtentObservation {
    let bytes = writer.serialize(MAIN_DB).unwrap();
    let extents = bytes
        .chunks(EXTENT_BYTES as usize)
        .map(|chunk| ExtentId::of(chunk).unwrap())
        .collect::<Vec<_>>();
    let expected = ExtentManifest::new(
        binding(txid),
        Geometry::new(db.page_size(), bytes.len() as u64).unwrap(),
        &extents,
    )
    .unwrap();
    let observed = observe_extents(
        db,
        previous,
        binding(txid),
        bytes.len() as u64,
        64 * 1024 * 1024,
    )
    .unwrap();
    assert_eq!(observed.manifest().root(), expected.root());
    assert_eq!(
        observed.manifest().encode().unwrap(),
        expected.encode().unwrap()
    );
    assert!(observed.work().image_bytes_read <= bytes.len() as u64);
    observed
}

#[test]
fn small_commits_hash_changed_extents_and_preserve_parent_observations() {
    for page_size in [512, 4096, 65536] {
        let root = tempfile::tempdir().unwrap();
        let (db, writer) = fixture(&root.path().join("main.sqlite"), page_size);
        let mut previous = compare(&db, &writer, None, 1);
        assert!(!previous.work().reused_base);
        assert_eq!(
            previous.work().image_bytes_read,
            previous.manifest().geometry().length()
        );
        let parent = previous.manifest().encode().unwrap();
        for txid in 2..=6 {
            writer
                .execute("UPDATE counter SET value=value+1", [])
                .unwrap();
            let current = compare(&db, &writer, Some(&previous), txid);
            assert!(current.work().reused_base);
            assert_eq!(current.work().extents_hashed, 1);
            assert_eq!(current.work().image_bytes_read, EXTENT_BYTES);
            if txid == 2 {
                assert_eq!(previous.manifest().encode().unwrap(), parent);
            }
            previous = current;
        }
        let same = compare(&db, &writer, Some(&previous), 6);
        assert!(same.work().reused_base);
        assert_eq!(same.work().image_bytes_read, 0);
        assert_eq!(same.work().extents_hashed, 0);
        assert_eq!(same.manifest().root(), previous.manifest().root());
        drop(writer);
        db.close().unwrap();
    }
}

#[test]
fn observations_match_pager_across_growth_shrink_capture_and_wal_reset() {
    for page_size in [512, 4096, 65536] {
        let root = tempfile::tempdir().unwrap();
        let (mut db, writer) = fixture(&root.path().join("main.sqlite"), page_size);
        let mut previous = compare(&db, &writer, None, 1);
        let operations = [
            "INSERT INTO payload VALUES(9,zeroblob(2097152))",
            "UPDATE payload SET value=randomblob(1048576) WHERE id=8",
            "DELETE FROM payload WHERE id>3",
            "INSERT INTO payload VALUES(10,zeroblob(5242880))",
            "DELETE FROM payload WHERE id>1",
            "VACUUM",
        ];
        for (index, sql) in operations.iter().enumerate() {
            writer.execute_batch(sql).unwrap();
            db.capture().unwrap();
            previous = compare(&db, &writer, Some(&previous), index as u64 + 2);
        }
        for (index, mode) in [
            CheckpointMode::Passive,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ]
        .into_iter()
        .enumerate()
        {
            db.checkpoint(mode).unwrap();
            writer
                .execute("UPDATE counter SET value=value+1", [])
                .unwrap();
            let current = compare(&db, &writer, Some(&previous), index as u64 + 8);
            if mode != CheckpointMode::Passive {
                assert!(
                    !current.work().reused_base,
                    "a reset WAL cannot establish prefix continuity"
                );
                assert_eq!(
                    current.work().image_bytes_read,
                    current.manifest().geometry().length()
                );
            }
            previous = current;
        }
        drop(writer);
        db.close().unwrap();
    }
}

#[test]
fn uncommitted_spilled_frames_never_change_the_observed_committed_root() {
    let root = tempfile::tempdir().unwrap();
    let (db, writer) = fixture(&root.path().join("main.sqlite"), 4096);
    let previous = compare(&db, &writer, None, 1);
    let wal_before = std::fs::metadata(db.wal_path()).unwrap().len();
    writer
        .execute_batch("PRAGMA cache_size=16; BEGIN; UPDATE payload SET value=randomblob(1048576);")
        .unwrap();
    assert!(std::fs::metadata(db.wal_path()).unwrap().len() > wal_before);
    // A live writer transaction blocks the metadata sample without waiting.
    assert!(observe_extents(
        &db,
        Some(&previous),
        binding(1),
        previous.manifest().geometry().length(),
        64 * 1024 * 1024
    )
    .is_err());
    writer.execute_batch("ROLLBACK;").unwrap();
    // Rolled-back spilled frames remain physical bytes beyond the live
    // committed cut and must not contribute to the observed image.
    let during = observe_extents(
        &db,
        Some(&previous),
        binding(1),
        previous.manifest().geometry().length(),
        64 * 1024 * 1024,
    )
    .unwrap();
    assert_eq!(during.manifest().root(), previous.manifest().root());
    assert!(during.work().reused_base);
    assert_eq!(during.work().image_bytes_read, 0);
    compare(&db, &writer, Some(&previous), 1);
    drop(writer);
    db.close().unwrap();
}

#[test]
fn wrong_owner_history_and_bounds_refuse_reuse_without_mutating_the_parent() {
    let root = tempfile::tempdir().unwrap();
    let (db, writer) = fixture(&root.path().join("main.sqlite"), 4096);
    let previous = compare(&db, &writer, None, 5);
    let parent = previous.manifest().encode().unwrap();
    let length = previous.manifest().geometry().length();
    for field in 0..5 {
        let mut changed = binding(6);
        match field {
            0 => changed.history[0] ^= 1,
            1 => changed.schema[0] ^= 1,
            2 => changed.epoch += 1,
            3 => changed.txid = 4,
            _ => changed.checksum = 1,
        }
        assert!(observe_extents(&db, Some(&previous), changed, length, 64 * 1024 * 1024).is_err());
    }
    for (length, wal) in [
        (length, 32),
        (length, 31),
        (length, 512 * 1024 * 1024 + 1),
        (length - 4096, 64 * 1024 * 1024),
        (10_000_000_001, 64 * 1024 * 1024),
    ] {
        assert!(observe_extents(&db, Some(&previous), binding(6), length, wal).is_err());
    }
    let other_root = tempfile::tempdir().unwrap();
    let (other, other_writer) = fixture(&other_root.path().join("main.sqlite"), 4096);
    assert!(observe_extents(
        &other,
        Some(&previous),
        binding(6),
        length,
        64 * 1024 * 1024
    )
    .is_err());
    drop(other_writer);
    other.close().unwrap();
    drop(writer);
    db.close().unwrap();
    // Reusing a path after reopening does not make it the same writer owner.
    let reopened = Db::open(root.path().join("main.sqlite")).unwrap();
    assert!(observe_extents(
        &reopened,
        Some(&previous),
        binding(6),
        length,
        64 * 1024 * 1024
    )
    .is_err());
    assert_eq!(previous.manifest().encode().unwrap(), parent);
    reopened.close().unwrap();
}

#[test]
fn corrupt_or_incomplete_committed_wal_cannot_relabel_the_older_image() {
    let root = tempfile::tempdir().unwrap();
    let (db, writer) = fixture(&root.path().join("main.sqlite"), 4096);
    let previous = compare(&db, &writer, None, 1);
    let parent = previous.manifest().encode().unwrap();
    writer
        .execute("UPDATE counter SET value=value+1", [])
        .unwrap();
    let wal = std::fs::read(db.wal_path()).unwrap();
    let mut corrupt = wal.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    for bytes in [&corrupt[..], &wal[..wal.len() - 1]] {
        // Fault injection changes only the physical WAL. SQLite's live index
        // still identifies the new committed frame, and logical length is
        // unchanged. Falling back to the previous complete commit is unsafe.
        std::fs::write(db.wal_path(), bytes).unwrap();
        let result = observe_extents(
            &db,
            Some(&previous),
            binding(2),
            previous.manifest().geometry().length(),
            64 * 1024 * 1024,
        );
        std::fs::write(db.wal_path(), &wal).unwrap();
        assert!(
            result.is_err(),
            "a missing committed frame must refuse observation"
        );
        assert_eq!(previous.manifest().encode().unwrap(), parent);
    }
    compare(&db, &writer, Some(&previous), 2);
    drop(writer);
    db.close().unwrap();
}
