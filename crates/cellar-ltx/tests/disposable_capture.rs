//! Disposable capture still produces a complete, independently restorable cut.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    Db, DirectFileSystem, FileReplicaClient, FileSystem, HostDirEntry, HostFile, HostFileIo,
    HostMetadata, LtxHost, ReplicaClient, TXID,
};
use std::io;
use std::path::Path;
use std::sync::Arc;

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

#[tokio::test]
async fn ephemeral_capture_has_no_files_and_restores_every_checkpoint_cut() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("working.sqlite");
    let mut db = Db::open_disposable_with_host(&path, LtxHost::default(), None).unwrap();
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(1)")
        .unwrap();
    let mut cuts = db.capture_ephemeral(8, 4 * 1024 * 1024).unwrap();
    db.min_checkpoint_page_n = 1;
    writer.execute("UPDATE counter SET value=2", []).unwrap();
    let checkpoint = db.capture_ephemeral(8, 4 * 1024 * 1024).unwrap();
    assert_eq!(db.last_sync_timing().checkpoint_runs, 1);
    assert!(checkpoint.len() > 1, "checkpoint control cuts were omitted");
    cuts.extend(checkpoint);
    db.min_checkpoint_page_n = u32::MAX;
    assert!(db.capture_ephemeral(8, 4 * 1024 * 1024).unwrap().is_empty());
    for value in 3..=9 {
        writer
            .execute("UPDATE counter SET value=?", [value])
            .unwrap();
        cuts.extend(db.capture_ephemeral(8, 4 * 1024 * 1024).unwrap());
        assert_eq!(db.last_sync_timing().file_write_us, 0);
        assert!(!db.meta_path().join("ltx/0").exists());
    }
    let position = db.pos().unwrap();
    assert_eq!(position.txid, cuts.last().unwrap().txid);
    let client = FileReplicaClient::new(directory.path().join("replica").to_str().unwrap());
    for (index, cut) in cuts.iter().enumerate() {
        assert_eq!(cut.txid, TXID(index as u64 + 1));
        client
            .write_ltx_file(0, cut.txid, cut.txid, &cut.bytes)
            .await
            .unwrap();
    }
    // No API may silently reconstruct this continuation from absent/stale files
    // or produce a checkpoint cut that the bounded owner does not receive.
    assert!(db.capture_with_files(8, 4 * 1024 * 1024).is_err());
    assert!(db.capture().is_err());
    assert!(db.sync().is_err());
    assert!(db.checkpoint_if_due().is_err());
    assert!(db.checkpoint(cellar_ltx::CheckpointMode::Passive).is_err());
    assert!(db.reset_local_state().is_err());
    assert!(db
        .seed_l0_baseline(TXID(1), TXID(1), &cuts[0].bytes)
        .is_err());
    assert!(db.read_ltx_file(0, position.txid, position.txid).is_err());
    assert_eq!(db.pos().unwrap(), position);
    drop(writer);
    db.close().unwrap();
    assert!(Db::open_disposable_with_host(&path, LtxHost::default(), None).is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_dir_all(Db::meta_path_for_path(&path)).unwrap();
    let output = directory.path().join("restored.sqlite");
    cellar_ltx::restore(&client, &output, position.txid)
        .await
        .unwrap();
    let restored = rusqlite::Connection::open(&output).unwrap();
    assert_eq!(
        restored
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        9
    );
    let pages: u32 = restored
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    drop(restored);
    // Rebuild from that independently restored image, then continue the same
    // TXID chain. Only the seed baseline is local; subsequent cuts stay returned.
    let mut resumed = Db::open_disposable_with_host(&output, LtxHost::default(), None).unwrap();
    resumed.seed_continuation(position.txid, pages).unwrap();
    let writer = rusqlite::Connection::open(&output).unwrap();
    writer.execute("UPDATE counter SET value=10", []).unwrap();
    let next = resumed.capture_ephemeral(8, 4 * 1024 * 1024).unwrap();
    assert!(!next.is_empty());
    assert_eq!(next[0].txid, TXID(position.txid.0 + 1));
    assert_eq!(
        std::fs::read_dir(resumed.meta_path().join("ltx/0"))
            .unwrap()
            .count(),
        1
    );
    for cut in &next {
        client
            .write_ltx_file(0, cut.txid, cut.txid, &cut.bytes)
            .await
            .unwrap();
    }
    let last = resumed.pos().unwrap();
    drop(writer);
    resumed.close().unwrap();
    let final_path = directory.path().join("final.sqlite");
    cellar_ltx::restore(&client, &final_path, last.txid)
        .await
        .unwrap();
    let restored = rusqlite::Connection::open(final_path).unwrap();
    assert_eq!(
        restored
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        10
    );
}

#[test]
fn ephemeral_capture_rejects_durable_owners_and_poisoned_partial_batches() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("durable.sqlite");
    let mut durable = Db::open(&path).unwrap();
    assert!(durable.capture_ephemeral(8, 4 * 1024 * 1024).is_err());
    durable.capture_with_files(8, 4 * 1024 * 1024).unwrap();
    durable.close().unwrap();
    for (files, bytes) in [(8, 1), (1, 4 * 1024 * 1024)] {
        let path = directory.path().join(format!("working-{files}.sqlite"));
        let mut db = Db::open_disposable_with_host(&path, LtxHost::default(), None).unwrap();
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer
            .execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(1)")
            .unwrap();
        db.capture_ephemeral(8, 4 * 1024 * 1024).unwrap();
        db.min_checkpoint_page_n = 1;
        writer.execute("UPDATE counter SET value=2", []).unwrap();
        assert!(db.capture_ephemeral(files, bytes).is_err());
        assert!(
            db.pos().is_err(),
            "failed partial capture exposed usable progress"
        );
        assert!(db.capture_ephemeral(8, 4 * 1024 * 1024).is_err());
        assert!(db.capture_with_files(8, 4 * 1024 * 1024).is_err());
        assert!(db.reset_local_state().is_err());
        assert!(!db.meta_path().join("ltx/0").exists());
        drop(writer);
        db.close().unwrap();
    }
}
