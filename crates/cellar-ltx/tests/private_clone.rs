// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! COW and portable copies preserve immutable sources and independent output
//! verification. Faults cannot turn an uncertain clone into a publication pin.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::apply::{ApplyError, ImageState, ImageUpdate, LtxSegment};
use cellar_ltx::generation::{GenerationError, GenerationLimits, GenerationStore, ImageId};
use cellar_ltx::{ltx, DirectFileSystem, FileSystem, TXID};
use sha2::{Digest, Sha256};
use std::sync::{atomic::Ordering, Arc};

#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

fn source(path: &std::path::Path) -> Vec<u8> {
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch("PRAGMA page_size=4096; CREATE TABLE items(value BLOB); INSERT INTO items VALUES(zeroblob(196608));").unwrap();
    drop(db);
    std::fs::read(path).unwrap()
}

fn state(bytes: &[u8], txid: u64) -> ImageState {
    ImageState {
        epoch: 1,
        txid: TXID(txid),
        checksum: 0,
        page_size: 4096,
        length: bytes.len() as u64,
        digest: Sha256::digest(bytes).into(),
    }
}

#[test]
fn native_clone_is_exclusive_and_its_writes_never_mutate_the_source() {
    let root = tempfile::tempdir().unwrap();
    let original = root.path().join("source.sqlite");
    let bytes = source(&original);
    let target = root.path().join("clone.sqlite");
    if let Some(mut copy) = DirectFileSystem
        .clone_read_write(&original, &target)
        .unwrap()
    {
        assert_eq!(copy.file_len().unwrap(), bytes.len() as u64);
        copy.write_all_at(4096, &[17; 4096]).unwrap();
        copy.sync_all().unwrap();
        assert_eq!(std::fs::read(&original).unwrap(), bytes);
    } else {
        assert!(!target.exists(), "unsupported clone left a destination");
    }
    let collision = root.path().join("collision.sqlite");
    std::fs::write(&collision, b"unowned data").unwrap();
    assert!(DirectFileSystem
        .clone_read_write(&original, &collision)
        .is_err());
    assert_eq!(std::fs::read(collision).unwrap(), b"unowned data");
    assert_eq!(std::fs::read(original).unwrap(), bytes);
}

#[test]
fn raw_image_import_and_private_writer_copy_preserve_exact_bytes() {
    for portable in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let original = root.path().join("source.sqlite");
        let bytes = source(&original);
        let filesystem: Arc<dyn FileSystem> = if portable {
            Arc::new(bounded_files::BoundedFiles {
                directory: root.path().into(),
                metrics: Arc::default(),
            })
        } else {
            Arc::new(DirectFileSystem)
        };
        let store = GenerationStore::open(
            root.path().join("images"),
            filesystem.clone(),
            GenerationLimits::default(),
        )
        .unwrap();
        let pin = store
            .prepare_image_file(ImageId::new([1; 16]).unwrap(), &original, state(&bytes, 1))
            .unwrap();
        let private = root.path().join("working.sqlite");
        pin.private_copy(&filesystem, &private).unwrap();
        assert_eq!(std::fs::read(&private).unwrap(), bytes);
        let writer = rusqlite::Connection::open(&private).unwrap();
        writer
            .execute("UPDATE items SET value=x'010203'", [])
            .unwrap();
        drop(writer);
        let mut reader = pin.open_reader().unwrap();
        let mut observed = Vec::new();
        while observed.len() < bytes.len() {
            let n = (bytes.len() - observed.len()).min(65536);
            observed.extend(reader.read_exact_at(observed.len() as u64, n).unwrap());
        }
        assert_eq!(observed, bytes);
        assert_eq!(std::fs::read(&original).unwrap(), bytes);
    }
}

#[test]
fn corrupt_clone_and_ambiguous_creation_never_publish_or_refund_uncertain_capacity() {
    let root = tempfile::tempdir().unwrap();
    let original = root.path().join("source.sqlite");
    let bytes = source(&original);
    let files = Arc::new(bounded_files::BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    });
    let filesystem: Arc<dyn FileSystem> = files.clone();
    let store = GenerationStore::open(
        root.path().join("images"),
        filesystem.clone(),
        GenerationLimits::default(),
    )
    .unwrap();
    let base = state(&bytes, 1);
    let pin = store
        .prepare_image_file(ImageId::new([1; 16]).unwrap(), &original, base)
        .unwrap();
    let next_path = root.path().join("next.sqlite");
    std::fs::write(&next_path, &bytes).unwrap();
    let db = rusqlite::Connection::open(&next_path).unwrap();
    db.execute("UPDATE items SET value=x'010203'", []).unwrap();
    drop(db);
    let next = std::fs::read(next_path).unwrap();
    let pages: Vec<_> = next
        .chunks_exact(4096)
        .enumerate()
        .filter(|(i, p)| bytes.get(i * 4096..i * 4096 + 4096) != Some(*p))
        .map(|(i, p)| (i as u32 + 1, p.to_vec()))
        .collect();
    let header = ltx::Header {
        version: ltx::VERSION,
        flags: ltx::HEADER_FLAG_NO_CHECKSUM,
        page_size: 4096,
        commit: (next.len() / 4096) as u32,
        min_txid: TXID(2),
        max_txid: TXID(2),
        ..Default::default()
    };
    let encoded = ltx::encode_file_v0_5_2(&header, &pages, 0).unwrap();
    let update = ImageUpdate {
        base,
        result: state(&next, 2),
        segments: vec![LtxSegment {
            bytes: &encoded,
            min_txid: TXID(2),
            max_txid: TXID(2),
            page_count: pages.len(),
            decoded_bytes: pages.len() as u64 * 4096,
        }],
    };
    files.metrics.clone_mode.store(1, Ordering::Release);
    files.metrics.corrupt_output.store(true, Ordering::Release);
    assert!(matches!(
        store.prepare(ImageId::new([2; 16]).unwrap(), Some(&pin), &update),
        Err(GenerationError::Apply(ApplyError::BaseDigest))
    ));
    assert_eq!(store.usage().unwrap().generations, 1);
    let private = root.path().join("failed-writer.sqlite");
    files.metrics.corrupt_output.store(true, Ordering::Release);
    assert!(pin.private_copy(&filesystem, &private).is_err());
    let healthy = store
        .prepare(ImageId::new([3; 16]).unwrap(), Some(&pin), &update)
        .unwrap();
    assert_eq!(healthy.state(), state(&next, 2));
    files.metrics.clone_mode.store(2, Ordering::Release);
    assert!(store
        .prepare(ImageId::new([4; 16]).unwrap(), Some(&pin), &update)
        .is_err());
    assert!(matches!(store.usage(), Err(GenerationError::Quarantined)));
    assert!(store
        .prepare(ImageId::new([5; 16]).unwrap(), Some(&pin), &update)
        .is_err());
    assert_eq!(std::fs::read(original).unwrap(), bytes);
}
