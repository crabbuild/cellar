// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cache advice is selected by logical geometry and never becomes data proof.
#![allow(clippy::disallowed_methods)]

#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

use bounded_files::BoundedFiles;
use cellar_ltx::{
    apply::ImageState,
    stream_snapshot::{self, Download, Limits, Metadata, ScratchLimits, ScratchPool, ScratchUsage},
    DirectFileSystem, FileSystem, TXID,
};
use sha2::{Digest, Sha256};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

const LARGE: u64 = 256 * 1024 * 1024;

fn files(root: &tempfile::TempDir, mode: usize) -> BoundedFiles {
    let files = BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    };
    files.metrics.streaming_mode.store(mode, Ordering::Release);
    files
}

fn pool() -> ScratchPool {
    ScratchPool::new(ScratchLimits {
        max_bytes: 512 * 1024 * 1024,
        max_files: 4,
    })
    .unwrap()
}

fn declaration(length: u64) -> Metadata {
    Metadata {
        image: ImageState {
            epoch: 1,
            txid: TXID(1),
            checksum: 0,
            page_size: 4096,
            length,
            digest: [1; 32],
        },
        encoded_length: 4096,
        encoded_digest: [2; 32],
    }
}

#[test]
fn receiver_uses_logical_size_even_when_the_declared_stream_is_small() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root, 1);
    let scratch = pool();
    for (length, calls) in [(LARGE - 4096, 0), (LARGE, 1), (9_999_998_976, 2)] {
        let receiver = Download::new_admitted_in(
            &files,
            Some(root.path()),
            declaration(length),
            Limits::default(),
            &scratch,
        )
        .unwrap();
        assert_eq!(files.metrics.streaming_calls.load(Ordering::Acquire), calls);
        assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 1);
        // Metadata acceptance and advice supply no complete receive proof.
        assert!(receiver.finish().is_err());
        assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
        assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
    }
}

#[test]
fn failed_advice_closes_files_and_refunds_before_source_io() {
    for mode in [2, 3] {
        let root = tempfile::tempdir().unwrap();
        let files = files(&root, mode);
        let scratch = pool();
        let reads = AtomicUsize::new(0);
        let result = stream_snapshot::encode_admitted_in(
            &files,
            root.path(),
            declaration(LARGE).image,
            Limits::default(),
            &scratch,
            |_, _| {
                reads.fetch_add(1, Ordering::AcqRel);
                Err(io::Error::other("source must not run after advice refusal"))
            },
        );
        assert!(
            matches!(result, Err(stream_snapshot::Error::Io(ref error)) if error.kind() == io::ErrorKind::PermissionDenied)
        );
        assert_eq!(reads.load(Ordering::Acquire), 0);
        assert_eq!(
            files.metrics.streaming_calls.load(Ordering::Acquire),
            if mode == 2 { 1 } else { 2 }
        );
        assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
        assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
    }
}

#[test]
fn receiver_advice_failure_and_invalid_input_cannot_retain_storage() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root, 2);
    let scratch = pool();
    let result = Download::new_admitted_in(
        &files,
        Some(root.path()),
        declaration(LARGE),
        Limits::default(),
        &scratch,
    );
    assert!(
        matches!(result, Err(stream_snapshot::Error::Io(ref error)) if error.kind() == io::ErrorKind::PermissionDenied)
    );
    assert_eq!(files.metrics.streaming_calls.load(Ordering::Acquire), 1);
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
    let invalid = declaration(10_000_000_001);
    assert!(Download::new_admitted_in(&files, None, invalid, Limits::default(), &scratch).is_err());
    assert_eq!(files.metrics.streaming_calls.load(Ordering::Acquire), 1);
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
}

#[test]
fn direct_advice_does_not_change_bounded_bytes_or_sync_behavior() {
    let root = tempfile::tempdir().unwrap();
    let mut file = DirectFileSystem.temporary_file(Some(root.path())).unwrap();
    #[cfg(target_os = "macos")]
    file.advise_streaming().unwrap();
    #[cfg(not(target_os = "macos"))]
    assert_eq!(
        file.advise_streaming().unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    file.write_all_at(0, &[7; 4096]).unwrap();
    file.sync_all().unwrap();
    assert_eq!(file.read_exact_at(0, 4096).unwrap(), vec![7; 4096]);
    assert_eq!(file.file_len().unwrap(), 4096);
}

fn hash_file(path: &std::path::Path) -> [u8; 32] {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path).unwrap();
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    loop {
        let count = file.read(&mut bytes).unwrap();
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    hash.finalize().into()
}

/// Real SQLite pages, compression and physical verification exercise both the
/// accepted-hint path and the portable Unsupported fallback. No full image is
/// collected in memory, including in this fixture's independent hash oracle.
#[test]
fn cache_advice_preserves_compressed_sqlite_bytes_and_physical_proofs() {
    for payload in [1024 * 1024, LARGE] {
        let root = tempfile::tempdir().unwrap();
        let source_path = root.path().join("source.sqlite");
        let db = rusqlite::Connection::open(&source_path).unwrap();
        db.execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF; CREATE TABLE state(value INTEGER);
            INSERT INTO state VALUES(7); CREATE TABLE padding(value BLOB);",
        )
        .unwrap();
        db.execute("INSERT INTO padding VALUES(zeroblob(?1))", [payload])
            .unwrap();
        drop(db);
        let image = ImageState {
            length: std::fs::metadata(&source_path).unwrap().len(),
            digest: hash_file(&source_path),
            ..declaration(4096).image
        };
        let large = image.length >= LARGE;
        let mut expected = None;
        for mode in [0, 1, 4] {
            let files = files(&root, mode);
            let scratch = pool();
            let mut source = DirectFileSystem.open(&source_path).unwrap();
            let encoded = stream_snapshot::encode_admitted_in(
                &files,
                root.path(),
                image,
                Limits::default(),
                &scratch,
                |offset, length| source.read_exact_at(offset, length),
            )
            .unwrap();
            drop(source);
            let metadata = encoded.metadata();
            assert!(metadata.encoded_length < LARGE);
            if let Some(expected) = expected {
                assert_eq!(metadata, expected);
            }
            expected = Some(metadata);
            assert_eq!(
                files.metrics.streaming_calls.load(Ordering::Acquire),
                if large { 2 } else { 0 }
            );
            encoded.verify().unwrap();
            let mut download = Download::new_admitted_in(
                &files,
                Some(root.path()),
                metadata,
                Limits::default(),
                &scratch,
            )
            .unwrap();
            let mut offset = 0;
            while offset < metadata.encoded_length {
                let length = (metadata.encoded_length - offset).min(65536) as usize;
                download
                    .append(offset, &encoded.chunk(offset, length).unwrap())
                    .unwrap();
                offset += length as u64;
            }
            let received = download.finish().unwrap();
            assert_eq!(
                files.metrics.streaming_calls.load(Ordering::Acquire),
                if large { 3 } else { 0 }
            );
            let destination = root.path().join(format!("restored-{mode}.sqlite"));
            let mut target = files.create_read_write(&destination).unwrap();
            assert_eq!(received.write_image(&mut target).unwrap(), image);
            target.sync_all().unwrap();
            drop(target);
            assert_eq!(hash_file(&destination), image.digest);
            let db = rusqlite::Connection::open(&destination).unwrap();
            let row: (i64, u64) = db
                .query_row(
                    "SELECT (SELECT value FROM state),length(value) FROM padding",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(row, (7, payload));
            assert_eq!(
                db.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            drop(db);
            drop(encoded);
            let retained = received.clone();
            drop(received);
            assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 1);
            assert_eq!(
                scratch.usage().unwrap(),
                ScratchUsage {
                    reserved_bytes: metadata.encoded_length,
                    reserved_files: 1
                }
            );
            drop(retained);
            assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
            assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
            assert!(files.metrics.max_read.load(Ordering::Acquire) <= 65536);
            assert!(files.metrics.max_write.load(Ordering::Acquire) <= 65536);
        }
    }
}
