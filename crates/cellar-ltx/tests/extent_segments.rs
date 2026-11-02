// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Append persistence, conservative physical accounting, recovery and shared
//! barriers. Public SQL/image and native host boundaries have separate tests.
#![allow(clippy::disallowed_methods)]
#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

use cellar_ltx::{
    internal::{
        ExtentId, ExtentManifest, ExtentRootReference, Geometry, ImageBinding, NodeExtentLimits,
        NodeExtentStore, StoreError,
    },
    DirectFileSystem, FileSystem,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};

struct Fixture {
    directory: tempfile::TempDir,
    files: Arc<bounded_files::BoundedFiles>,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let files = Arc::new(bounded_files::BoundedFiles {
            directory: directory.path().into(),
            metrics: Arc::default(),
        });
        Self { directory, files }
    }

    fn path(&self) -> PathBuf {
        self.directory.path().join("store")
    }

    fn open(&self, target: u64) -> NodeExtentStore {
        self.open_limits(target, Default::default()).unwrap()
    }

    fn open_limits(
        &self,
        target: u64,
        limits: NodeExtentLimits,
    ) -> Result<NodeExtentStore, StoreError> {
        NodeExtentStore::open_append(self.path(), self.files.clone(), limits, 3, target, true)
    }

    fn reset(&self) {
        self.files
            .metrics
            .file_sync_calls
            .store(0, Ordering::Release);
        self.files
            .metrics
            .dir_sync_calls
            .store(0, Ordering::Release);
        self.files
            .metrics
            .random_write_calls
            .store(0, Ordering::Release);
    }

    fn physical_bytes(&self) -> u64 {
        std::fs::read_dir(self.path())
            .unwrap()
            .map(|v| v.unwrap())
            .filter(|v| v.file_name() != "extents.lock")
            .map(|v| v.metadata().unwrap().len())
            .sum()
    }
}

fn manifest(id: ExtentId, sequence: u64) -> ExtentManifest {
    ExtentManifest::new(
        ImageBinding {
            history: [1; 32],
            schema: [2; 32],
            epoch: 1,
            txid: sequence,
            checksum: 0,
        },
        Geometry::new(512, u64::from(id.length())).unwrap(),
        &[id],
    )
    .unwrap()
}

fn reference(root: &ExtentManifest) -> ExtentRootReference {
    ExtentRootReference::new(root.root(), root.encode().unwrap().len() as u32).unwrap()
}

fn wait_for(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "bounded test wait expired");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn restart_keeps_offsets_and_charges_but_requires_fresh_byte_proofs() {
    let f = Fixture::new();
    let store = f.open(8192);
    let bytes = vec![7; 512];
    let id = ExtentId::of(&bytes).unwrap();
    store.put_extent_bytes(id, &bytes).unwrap();
    let root = manifest(id, 1);
    let expected = reference(&root);
    drop(store.seal_root(root.clone()).unwrap());
    assert_eq!(store.usage().unwrap().bytes, f.physical_bytes());
    drop(store);
    let store = f.open(8192);
    assert!(matches!(
        store.seal_root(root.clone()),
        Err(StoreError::Unverified)
    ));
    f.reset();
    store.verify_extent(id).unwrap();
    assert_eq!(f.files.metrics.file_sync_calls.load(Ordering::Acquire), 1);
    assert_eq!(f.files.metrics.dir_sync_calls.load(Ordering::Acquire), 1);
    let sealed = store.seal_root(root).unwrap();
    assert_eq!(sealed.view().read_extent(id).unwrap(), bytes);
    assert_eq!(sealed.view().reference(), expected);
    assert_eq!(store.usage().unwrap().bytes, f.physical_bytes());
    assert!(
        NodeExtentStore::open_parallel(f.path(), f.files.clone(), Default::default(), 2, true)
            .is_err()
    );
    drop(sealed);
    drop(store);
    assert!(matches!(
        NodeExtentStore::open(f.path(), f.files.clone(), Default::default()),
        Err(StoreError::Layout)
    ));
}

#[test]
fn compatible_waiters_share_a_barrier_but_late_records_need_the_next_one() {
    let f = Fixture::new();
    let store = f.open(65536);
    store
        .put_extent_bytes(ExtentId::of(&[0; 512]).unwrap(), &[0; 512])
        .unwrap();
    f.reset();
    let (entered, entered_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let syncs = Arc::new(AtomicUsize::new(0));
    let seen = syncs.clone();
    *f.files.metrics.before_file_sync.lock().unwrap() = Some(Arc::new(move |_| {
        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            entered.send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
        Ok(())
    }));
    let (done, done_rx) = mpsc::channel();
    let first = store.clone();
    let first_done = done.clone();
    let a = std::thread::spawn(move || {
        let bytes = vec![1; 512];
        first
            .put_extent_bytes(ExtentId::of(&bytes).unwrap(), &bytes)
            .unwrap();
        first_done.send(()).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let mut workers = vec![a];
    for value in [2, 3] {
        let worker = store.clone();
        let done = done.clone();
        workers.push(std::thread::spawn(move || {
            let bytes = vec![value; 512];
            worker
                .put_extent_bytes(ExtentId::of(&bytes).unwrap(), &bytes)
                .unwrap();
            done.send(()).unwrap();
        }));
    }
    wait_for(|| f.files.metrics.random_write_calls.load(Ordering::Acquire) >= 9);
    assert!(
        done_rx.try_recv().is_err(),
        "a barrier in flight cannot cover records appended after its snapshot"
    );
    release.send(()).unwrap();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        syncs.load(Ordering::Acquire),
        2,
        "the two completed waiters must share the next barrier"
    );
    assert_eq!(f.files.metrics.dir_sync_calls.load(Ordering::Acquire), 0);
    assert_eq!(store.usage().unwrap().bytes, f.physical_bytes());
}

#[test]
fn same_object_has_one_record_and_readers_detect_later_damage() {
    let f = Fixture::new();
    let store = f.open(65536);
    store
        .put_extent_bytes(ExtentId::of(&[0; 512]).unwrap(), &[0; 512])
        .unwrap();
    f.reset();
    std::thread::scope(|scope| {
        for _ in 0..3 {
            let store = &store;
            scope.spawn(move || {
                store
                    .put_extent_bytes(ExtentId::of(&[5; 512]).unwrap(), &[5; 512])
                    .unwrap();
            });
        }
    });
    assert_eq!(
        f.files.metrics.random_write_calls.load(Ordering::Acquire),
        3
    );
    let id = ExtentId::of(&[5; 512]).unwrap();
    let view = store.stage_root(manifest(id, 1)).unwrap();
    f.files
        .metrics
        .corrupt_read_countdown
        .store(1, Ordering::Release);
    assert!(view.read_extent(id).is_err());
    assert!(matches!(
        store.seal_root(manifest(id, 1)),
        Err(StoreError::Unverified)
    ));
}

#[test]
fn quota_includes_framing_and_refuses_before_creation() {
    let f = Fixture::new();
    let limits = NodeExtentLimits {
        max_bytes: 512 + 128 + 63,
        ..Default::default()
    };
    let store = f.open_limits(4096, limits).unwrap();
    f.reset();
    assert!(matches!(
        store.put_extent_bytes(ExtentId::of(&[7; 512]).unwrap(), &[7; 512]),
        Err(StoreError::Limit)
    ));
    assert_eq!(store.usage().unwrap().bytes, 0);
    assert!(!store.usage().unwrap().quarantined);
    assert_eq!(f.physical_bytes(), 0);
    assert_eq!(
        f.files.metrics.random_write_calls.load(Ordering::Acquire),
        0
    );
}

#[test]
fn segment_gc_preserves_pins_and_refunds_only_deleted_files() {
    let f = Fixture::new();
    let store = f.open(4096);
    let a = vec![1; 4096];
    let aid = ExtentId::of(&a).unwrap();
    store.put_extent_bytes(aid, &a).unwrap();
    let old = store.stage_root(manifest(aid, 1)).unwrap();
    let b = vec![2; 4096];
    let bid = ExtentId::of(&b).unwrap();
    store.put_extent_bytes(bid, &b).unwrap();
    let new = store.stage_root(manifest(bid, 2)).unwrap();
    store.retain_only(&[new.reference()]).unwrap();
    assert_eq!(store.reclaim(64).unwrap(), 0);
    assert_eq!(old.read_extent(aid).unwrap(), a);
    drop(old);
    assert_eq!(store.reclaim(64).unwrap(), 2);
    assert_eq!(new.read_extent(bid).unwrap(), b);
    assert_eq!(store.usage().unwrap().bytes, f.physical_bytes());
    drop(new);
    store.retain_only(&[]).unwrap();
    assert_eq!(store.reclaim(64).unwrap(), 2);
    assert_eq!(store.usage().unwrap().bytes, 0);
    store.put_extent_bytes(aid, &a).unwrap();
    assert_eq!(store.usage().unwrap().bytes, f.physical_bytes());
}

#[test]
fn mixed_segments_retain_dead_bytes_and_their_dependencies() {
    let f = Fixture::new();
    let store = f.open(8192);
    let a = vec![1; 4096];
    let aid = ExtentId::of(&a).unwrap();
    store.put_extent_bytes(aid, &a).unwrap();
    let b = vec![2; 4096];
    let bid = ExtentId::of(&b).unwrap();
    store.put_extent_bytes(bid, &b).unwrap();
    let old = store.stage_root(manifest(aid, 1)).unwrap();
    let new = store.stage_root(manifest(bid, 2)).unwrap();
    drop(old);
    store.retain_only(&[new.reference()]).unwrap();
    let charged = store.usage().unwrap().bytes;
    assert_eq!(
        store.reclaim(64).unwrap(),
        0,
        "a retired root sharing a live segment still pins its dependencies"
    );
    assert_eq!(store.usage().unwrap().bytes, charged);
    assert_eq!(new.read_extent(bid).unwrap(), b);
    drop(new);
    store.retain_only(&[]).unwrap();
    assert_eq!(store.reclaim(64).unwrap(), 2);
    assert_eq!(store.usage().unwrap().bytes, 0);
}

#[test]
fn failed_append_sync_and_deletion_quarantine_without_refund() {
    for failure in 0..3 {
        let f = Fixture::new();
        let store = f.open(8192);
        store
            .put_extent_bytes(ExtentId::of(&[0; 512]).unwrap(), &[0; 512])
            .unwrap();
        let before = store.usage().unwrap().bytes;
        match failure {
            0 => f.files.metrics.fail_append.store(true, Ordering::Release),
            1 => f.files.metrics.fail_sync.store(true, Ordering::Release),
            _ => f
                .files
                .metrics
                .corrupt_output
                .store(true, Ordering::Release),
        }
        assert!(store
            .put_extent_bytes(ExtentId::of(&[7; 512]).unwrap(), &[7; 512])
            .is_err());
        assert!(store.usage().unwrap().quarantined);
        assert!(store.usage().unwrap().bytes >= before + 640);
        assert!(matches!(
            store.verify_extent(ExtentId::of(&[0; 512]).unwrap()),
            Err(StoreError::Quarantined)
        ));
    }
    let f = Fixture::new();
    let store = f.open(8192);
    store
        .put_extent_bytes(ExtentId::of(&[7; 512]).unwrap(), &[7; 512])
        .unwrap();
    store.retain_only(&[]).unwrap();
    let before = store.usage().unwrap();
    f.files.metrics.fail_dir_sync.store(true, Ordering::Release);
    assert!(store.reclaim(64).is_err());
    assert_eq!(store.usage().unwrap().bytes, before.bytes);
    assert!(store.usage().unwrap().quarantined);
}

#[test]
fn torn_tail_stays_charged_and_new_writes_rotate_without_reusing_it() {
    for keep in [0, 13, 64, 64 + 30, 64 + 96 + 100, 64 + 96 + 512 + 10] {
        let f = Fixture::new();
        let store = f.open(8192);
        store
            .put_extent_bytes(ExtentId::of(&[1; 512]).unwrap(), &[1; 512])
            .unwrap();
        drop(store);
        let path = f.path().join("s-0000000000000001");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(keep)
            .unwrap();
        let store = f.open(8192);
        assert_eq!(store.usage().unwrap().objects, 0);
        assert_eq!(store.usage().unwrap().bytes, keep);
        store
            .put_extent_bytes(ExtentId::of(&[2; 512]).unwrap(), &[2; 512])
            .unwrap();
        assert_eq!(store.usage().unwrap().bytes, f.physical_bytes());
        assert!(f.path().join("s-0000000000000002").exists() || keep == 64);
    }
}

#[test]
fn complete_corrupt_framing_refuses_recovery_and_incomplete_data_cannot_seal() {
    let f = Fixture::new();
    let store = f.open(8192);
    let id = ExtentId::of(&[1; 512]).unwrap();
    store.put_extent_bytes(id, &[1; 512]).unwrap();
    drop(store);
    let path = f.path().join("s-0000000000000001");
    let mut file = DirectFileSystem.open_read_write(&path).unwrap();
    file.write_all_at(64 + 96 + 512, &[0; 32]).unwrap();
    drop(file);
    assert!(matches!(
        f.open_limits(8192, Default::default()),
        Err(StoreError::Integrity)
    ));
    assert!(matches!(
        NodeExtentStore::open_append(f.path(), f.files.clone(), Default::default(), 0, 8192, true),
        Err(StoreError::Limit)
    ));
}

#[test]
fn corruption_exposed_after_sync_cannot_create_a_reusable_proof() {
    let f = Fixture::new();
    let store = f.open(8192);
    store
        .put_extent_bytes(ExtentId::of(&[0; 512]).unwrap(), &[0; 512])
        .unwrap();
    let path = f.path().join("s-0000000000000001");
    *f.files.metrics.after_file_sync.lock().unwrap() = Some(Arc::new(move |_| {
        let mut file = DirectFileSystem.open_read_write(&path)?;
        // The second record follows the 64-byte segment header and 640-byte first record.
        file.write_all_at(64 + 640 + 96, &[99])
    }));
    let id = ExtentId::of(&[1; 512]).unwrap();
    assert!(store.put_extent_bytes(id, &[1; 512]).is_err());
    assert!(store.usage().unwrap().quarantined);
    *f.files.metrics.after_file_sync.lock().unwrap() = None;
    drop(store);
    let store = f.open(8192);
    assert!(store.verify_extent(id).is_err());
    assert!(matches!(
        store.seal_root(manifest(id, 1)),
        Err(StoreError::Unverified)
    ));
}

#[test]
fn a_panicked_source_releases_the_append_owner_and_quarantines_admission() {
    let f = Fixture::new();
    let store = f.open(8192);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.put_extent(ExtentId::of(&[1; 512]).unwrap(), |_, _| {
            panic!("injected append source panic")
        })
    }));
    assert!(result.is_err());
    assert!(store.usage().unwrap().quarantined);
    assert!(matches!(
        store.put_extent_bytes(ExtentId::of(&[2; 512]).unwrap(), &[2; 512]),
        Err(StoreError::Quarantined)
    ));
    drop(store);
    let store = f.open(8192);
    assert_eq!(store.usage().unwrap().objects, 0);
    assert_eq!(store.usage().unwrap().bytes, f.physical_bytes());
}

#[test]
fn panicked_sync_leader_wakes_waiters_without_granting_a_proof() {
    let f = Fixture::new();
    let store = f.open(8192);
    store
        .put_extent_bytes(ExtentId::of(&[0; 512]).unwrap(), &[0; 512])
        .unwrap();
    f.reset();
    let (entered, entered_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    *f.files.metrics.before_file_sync.lock().unwrap() = Some(Arc::new(move |_| {
        entered.send(()).unwrap();
        release_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        panic!("injected segment sync panic");
    }));
    let a = store.clone();
    let first =
        std::thread::spawn(move || a.put_extent_bytes(ExtentId::of(&[1; 512]).unwrap(), &[1; 512]));
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let b = store.clone();
    let (finished, finished_rx) = mpsc::channel();
    let second = std::thread::spawn(move || {
        finished
            .send(b.put_extent_bytes(ExtentId::of(&[2; 512]).unwrap(), &[2; 512]))
            .unwrap();
    });
    wait_for(|| f.files.metrics.random_write_calls.load(Ordering::Acquire) >= 6);
    release.send(()).unwrap();
    assert!(first.join().is_err());
    assert!(finished_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .is_err());
    second.join().unwrap();
    assert!(store.usage().unwrap().quarantined);
}

#[test]
fn a_small_gc_budget_removes_root_segments_before_their_dependencies() {
    let f = Fixture::new();
    let store = f.open(4096);
    let bytes = vec![1; 4096];
    let id = ExtentId::of(&bytes).unwrap();
    store.put_extent_bytes(id, &bytes).unwrap();
    drop(store.stage_root(manifest(id, 1)).unwrap());
    store.retain_only(&[]).unwrap();
    assert_eq!(
        store.reclaim(1).unwrap(),
        1,
        "GC must select a removable root segment instead of stalling on its data dependency"
    );
    assert_eq!(store.usage().unwrap().roots, 0);
    assert_eq!(store.reclaim(1).unwrap(), 1);
    assert_eq!(store.usage().unwrap().bytes, 0);
}
