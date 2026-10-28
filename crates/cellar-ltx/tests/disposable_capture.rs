// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable capture still produces a complete, independently restorable cut.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    Db, DirectFileSystem, FileReplicaClient, FileSystem, HostDirEntry, HostFile, HostFileIo,
    HostMetadata, LtxHost, ReplicaClient, TXID,
};
use std::io;
use std::path::Path;
use std::sync::Arc;

#[test]
fn incremental_only_capture_permits_bootstrap_and_refuses_durable_owners() {
    let root = tempfile::tempdir().unwrap();
    let mut durable = Db::open(root.path().join("durable.sqlite")).unwrap();
    assert!(durable.require_incremental_capture().is_err());
    durable.close().unwrap();
    let path = root.path().join("private.sqlite");
    let mut db = Db::open_disposable_with_host(&path, Default::default(), None).unwrap();
    db.require_incremental_capture().unwrap();
    db.min_checkpoint_page_n = u32::MAX;
    db.truncate_page_n = 0;
    db.checkpoint_interval = std::time::Duration::ZERO;
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(7)")
        .unwrap();
    let bootstrap = db.capture_with_files(8, 1024 * 1024).unwrap();
    assert_eq!(bootstrap.len(), 1);
    assert_eq!(bootstrap[0].txid, TXID(1));
    writer.execute("UPDATE counter SET value=8", []).unwrap();
    let update = db.capture_with_files(8, 1024 * 1024).unwrap();
    assert_eq!(update.len(), 1);
    assert_eq!(update[0].txid, TXID(2));
    drop(writer);
    db.close().unwrap();
}

#[test]
fn capture_decoded_budget_rejects_compressible_growth_before_page_collection() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("bounded.sqlite");
    let mut db = Db::open_disposable_with_host(&path, Default::default(), None).unwrap();
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "CREATE TABLE padding(value BLOB); INSERT INTO padding VALUES(zeroblob(4194304));",
        )
        .unwrap();
    let limits = cellar_ltx::ltx::DecodeLimits {
        max_encoded_bytes: 1024 * 1024,
        max_decoded_bytes: 8192,
        max_pages: 2,
        ..Default::default()
    };
    assert!(db.capture_with_limits(64, limits).is_err());
    assert_eq!(db.pos().unwrap().txid, TXID(0));
    let cuts = db.capture_with_files(64, 1024 * 1024).unwrap();
    assert!(!cuts.is_empty());
    let decoded = cellar_ltx::ltx::decode_file_bounded(&cuts[0].bytes, Default::default()).unwrap();
    assert!(decoded.decoded_bytes() > 4 * 1024 * 1024);
    assert!(cuts[0].bytes.len() < 1024 * 1024);
    assert_eq!(
        writer
            .query_row("SELECT length(value) FROM padding", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        4194304
    );
    drop(writer);
    db.close().unwrap();
}

struct RejectSyncFile(HostFile);

impl HostFileIo for RejectSyncFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.0.read_exact_at(offset, len)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        Err(io::Error::other("injected capture sync failure"))
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.0.file_len()
    }
}

struct RejectCaptureSync;

impl FileSystem for RejectCaptureSync {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(RejectSyncFile(
            DirectFileSystem.create(path)?,
        )))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }
}

#[tokio::test]
async fn disposable_capture_restores_without_sync_but_default_requires_it() {
    for disposable in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("working.sqlite");
        let host = LtxHost::default().with_filesystem(Arc::new(RejectCaptureSync));
        let mut db = if disposable {
            Db::open_disposable_with_host(&path, host, None)
        } else {
            Db::open_with_host(&path, host)
        }
        .unwrap();
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer
            .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1)")
            .unwrap();
        if !disposable {
            assert!(
                db.capture().is_err(),
                "durable capture swallowed a sync failure"
            );
            drop(writer);
            db.close().unwrap();
            continue;
        }
        db.capture().unwrap();
        db.min_checkpoint_page_n = 1;
        writer.execute("UPDATE counter SET value=2", []).unwrap();
        db.capture().unwrap();
        assert_eq!(db.last_sync_timing().checkpoint_runs, 1);
        let position = db.pos().unwrap();
        let client = FileReplicaClient::new(directory.path().join("replica").to_str().unwrap());
        for index in 1..=position.txid.0 {
            let txid = TXID(index);
            let bytes = db.read_ltx_file(0, txid, txid).unwrap();
            client.write_ltx_file(0, txid, txid, &bytes).await.unwrap();
        }
        drop(writer);
        db.close().unwrap();
        // The captured bytes must be sufficient after all private state is lost.
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir_all(Db::meta_path_for_path(&path)).unwrap();
        let output = directory.path().join("restored.sqlite");
        cellar_ltx::restore(&client, &output, position.txid)
            .await
            .unwrap();
        let restored = rusqlite::Connection::open(output).unwrap();
        assert_eq!(
            restored
                .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}

#[test]
fn disposable_open_refuses_existing_history_before_mutating_the_database() {
    for suffix in ["-wal", "-shm", "-journal", "metadata"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("working.sqlite");
        let database = rusqlite::Connection::open(&path).unwrap();
        database
            .execute_batch("CREATE TABLE retained(value); INSERT INTO retained VALUES(7)")
            .unwrap();
        drop(database);
        let before = std::fs::read(&path).unwrap();
        if suffix == "metadata" {
            std::fs::create_dir(Db::meta_path_for_path(&path)).unwrap();
        } else {
            std::fs::write(
                directory.path().join(format!("working.sqlite{suffix}")),
                b"untrusted history",
            )
            .unwrap();
        }
        assert!(Db::open_disposable_with_host(&path, LtxHost::default(), None).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}

#[tokio::test]
async fn capture_handoff_includes_checkpoint_cuts_and_restores_after_workspace_loss() {
    for disposable in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("working.sqlite");
        let mut db = if disposable {
            Db::open_disposable_with_host(&path, LtxHost::default(), None)
        } else {
            Db::open(&path)
        }
        .unwrap();
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer
            .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1)")
            .unwrap();
        let mut cuts = db.capture_with_files(8, 4 * 1024 * 1024).unwrap();
        assert!(!cuts.is_empty());
        db.min_checkpoint_page_n = 1;
        writer.execute("UPDATE counter SET value=2", []).unwrap();
        let checkpoint = db.capture_with_files(8, 4 * 1024 * 1024).unwrap();
        assert_eq!(db.last_sync_timing().checkpoint_runs, 1);
        assert!(checkpoint.len() > 1, "checkpoint control cuts were omitted");
        cuts.extend(checkpoint);
        let position = db.pos().unwrap();
        db.min_checkpoint_page_n = u32::MAX;
        assert!(db
            .capture_with_files(8, 4 * 1024 * 1024)
            .unwrap()
            .is_empty());
        let client = FileReplicaClient::new(directory.path().join("replica").to_str().unwrap());
        for (index, cut) in cuts.iter().enumerate() {
            assert_eq!(cut.txid, TXID(index as u64 + 1));
            assert_eq!(
                cut.bytes.as_ref(),
                db.read_ltx_file(0, cut.txid, cut.txid).unwrap(),
                "handoff changed the bytes written by capture"
            );
            client
                .write_ltx_file(0, cut.txid, cut.txid, &cut.bytes)
                .await
                .unwrap();
        }
        assert_eq!(cuts.last().unwrap().txid, position.txid);
        drop(writer);
        db.close().unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir_all(Db::meta_path_for_path(&path)).unwrap();
        let output = directory.path().join("restored.sqlite");
        cellar_ltx::restore(&client, &output, position.txid)
            .await
            .unwrap();
        let restored = rusqlite::Connection::open(output).unwrap();
        assert_eq!(
            restored
                .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
}

#[test]
fn capture_handoff_refuses_limits_and_releases_its_batch_owner() {
    for (files, bytes) in [(0, 4 * 1024 * 1024), (8, 0), (8, 1), (1, 4 * 1024 * 1024)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("working.sqlite");
        let mut db = Db::open_disposable_with_host(&path, LtxHost::default(), None).unwrap();
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer
            .execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(1)")
            .unwrap();
        db.capture().unwrap();
        db.min_checkpoint_page_n = 1;
        writer.execute("UPDATE counter SET value=2", []).unwrap();
        assert!(
            db.capture_with_files(files, bytes).is_err(),
            "bounded output returned a partial success"
        );
        // A failed batch releases its collector. The ordinary capture state may
        // already have advanced; this call returns only subsequently encoded cuts.
        db.capture_with_files(8, 4 * 1024 * 1024).unwrap();
        drop(writer);
        db.close().unwrap();
    }
}

#[test]
fn capture_handoff_preserves_durable_sync_failures() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("working.sqlite");
    let host = LtxHost::default().with_filesystem(Arc::new(RejectCaptureSync));
    let mut db = Db::open_with_host(&path, host).unwrap();
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(1)")
        .unwrap();
    for _ in 0..2 {
        let error = db.capture_with_files(8, 4 * 1024 * 1024).unwrap_err();
        assert!(error.to_string().contains("injected capture sync failure"));
    }
    drop(writer);
    db.close().unwrap();
}
