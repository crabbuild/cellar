// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receive scratch is disposable; only the independently checked destination
//! can supply durable image bytes. A cached pin never excuses verification.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::{
    apply::ImageState,
    generation::{GenerationPin, GenerationReceive, GenerationStore, ImageId},
    stream_snapshot::ScratchPool,
    TXID,
};
use sha2::{Digest, Sha256};
use std::sync::{atomic::Ordering, Arc};

#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

struct Fixture {
    root: tempfile::TempDir,
    files: Arc<bounded_files::BoundedFiles>,
    scratch: ScratchPool,
    store: GenerationStore,
    bytes: Vec<u8>,
    state: ImageState,
    id: ImageId,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.sqlite");
        let db = rusqlite::Connection::open(&source).unwrap();
        db.execute_batch("PRAGMA page_size=4096; CREATE TABLE items(value BLOB); INSERT INTO items VALUES(zeroblob(196608));").unwrap();
        drop(db);
        let bytes = std::fs::read(source).unwrap();
        let state = ImageState {
            epoch: 1,
            txid: TXID(1),
            checksum: 0,
            page_size: 4096,
            length: bytes.len() as u64,
            digest: Sha256::digest(&bytes).into(),
        };
        let files = Arc::new(bounded_files::BoundedFiles {
            directory: root.path().into(),
            metrics: Arc::default(),
        });
        files
            .metrics
            .reject_scratch_sync
            .store(true, Ordering::Release);
        let store = GenerationStore::open(
            root.path().join("images"),
            files.clone(),
            Default::default(),
        )
        .unwrap();
        Self {
            root,
            files,
            scratch: ScratchPool::default(),
            store,
            bytes,
            state,
            id: ImageId::new([1; 16]).unwrap(),
        }
    }

    fn receive(&self, bytes: &[u8]) -> GenerationReceive {
        let mut receive = self
            .store
            .receive_image(self.id, self.state, &self.scratch, self.root.path())
            .unwrap();
        for (index, chunk) in bytes.chunks(65536).enumerate() {
            receive.append((index * 65536) as u64, chunk).unwrap();
        }
        receive
    }

    fn assert_bytes(&self, pin: &GenerationPin) {
        assert_eq!(pin.state(), self.state);
        let mut reader = pin.open_reader().unwrap();
        let mut observed = Vec::new();
        while observed.len() < self.bytes.len() {
            let offset = observed.len();
            observed.extend(
                reader
                    .read_exact_at(offset as u64, (self.bytes.len() - offset).min(65536))
                    .unwrap(),
            );
        }
        assert_eq!(observed, self.bytes);
        assert_eq!(self.scratch.usage().unwrap(), Default::default());
        assert_eq!(
            self.files
                .metrics
                .scratch_sync_calls
                .load(Ordering::Acquire),
            0
        );
    }
}

#[test]
fn anonymous_receive_needs_no_sync_but_new_cached_and_reopened_destinations_do() {
    let mut f = Fixture::new();
    for reopened in [false, true] {
        if reopened {
            // Release the last directory owner before reopening. The on-disk
            // image has no serving authority merely because it survived.
            let placeholder = GenerationStore::open(
                f.root.path().join("placeholder"),
                f.files.clone(),
                Default::default(),
            )
            .unwrap();
            drop(std::mem::replace(&mut f.store, placeholder));
            f.store = GenerationStore::open(
                f.root.path().join("images"),
                f.files.clone(),
                Default::default(),
            )
            .unwrap();
        }
        for _ in 0..2 {
            let files = f.files.metrics.file_sync_calls.load(Ordering::Acquire);
            let directories = f.files.metrics.dir_sync_calls.load(Ordering::Acquire);
            let pin = f.receive(&f.bytes).finish().unwrap();
            f.assert_bytes(&pin);
            assert!(f.files.metrics.file_sync_calls.load(Ordering::Acquire) > files);
            assert!(f.files.metrics.dir_sync_calls.load(Ordering::Acquire) > directories);
        }
    }
    assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 0);
}

#[test]
fn failed_destination_barriers_never_return_a_received_pin() {
    for cached in [false, true] {
        for directory in [false, true] {
            let f = Fixture::new();
            if cached {
                drop(f.receive(&f.bytes).finish().unwrap());
            }
            let receive = f.receive(&f.bytes);
            if directory {
                f.files.metrics.fail_dir_sync.store(true, Ordering::Release);
            } else {
                f.files.metrics.fail_sync.store(true, Ordering::Release);
            }
            assert!(
                receive.finish().is_err(),
                "cached={cached}, directory={directory}"
            );
            assert_eq!(f.scratch.usage().unwrap(), Default::default());
            assert_eq!(
                f.files.metrics.scratch_sync_calls.load(Ordering::Acquire),
                0
            );
            assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 0);
            if cached {
                // The failed verification must not poison the valid identity;
                // a retry independently checks bytes and both barriers again.
                let files = f.files.metrics.file_sync_calls.load(Ordering::Acquire);
                let directories = f.files.metrics.dir_sync_calls.load(Ordering::Acquire);
                let pin = f.receive(&f.bytes).finish().unwrap();
                f.assert_bytes(&pin);
                assert!(f.files.metrics.file_sync_calls.load(Ordering::Acquire) > files);
                assert!(f.files.metrics.dir_sync_calls.load(Ordering::Acquire) > directories);
            }
        }
    }
}

#[test]
fn existing_identity_cannot_hide_corrupt_incoming_or_destination_bytes() {
    for destination in [false, true] {
        let f = Fixture::new();
        let held = f.receive(&f.bytes).finish().unwrap();
        let mut corrupt = f.bytes.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        if destination {
            let path = std::fs::read_dir(f.root.path().join("images"))
                .unwrap()
                .map(|e| e.unwrap().path())
                .find(|p| p.extension().is_some_and(|e| e == "sqlite"))
                .unwrap();
            std::fs::write(path, corrupt).unwrap();
            assert!(f.receive(&f.bytes).finish().is_err());
        } else {
            assert!(f.receive(&corrupt).finish().is_err());
            f.assert_bytes(&held);
        }
        assert_eq!(f.scratch.usage().unwrap(), Default::default());
        assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 0);
    }
}
