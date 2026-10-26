// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent owner, exact references and real SQLite restart reads. These do
//! not exercise native Raft root installation or supply its admission proofs.
#![allow(clippy::disallowed_methods)]
#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

use bounded_files::BoundedFiles;
use cellar_ltx::{
    generation::ReadServices,
    internal::{
        open_extent_image, CacheLimits, ExtentId, ExtentManifest, ExtentRootReference, Geometry,
        ImageBinding, NodeExtentLimits, NodeExtentStore, StoreError, EXTENT_BYTES,
    },
    stream_snapshot::{ScratchLimits, ScratchPool, ScratchUsage},
    DirectFileSystem, FileSystem,
};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

struct Fixture {
    root: tempfile::TempDir,
    files: Arc<BoundedFiles>,
    source: std::path::PathBuf,
    objects: Vec<(ExtentId, u64)>,
    manifest: ExtentManifest,
}

impl Fixture {
    fn new(payload: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.sqlite");
        let db = rusqlite::Connection::open(&source).unwrap();
        db.execute_batch(
            "PRAGMA page_size=4096;PRAGMA journal_mode=OFF;PRAGMA synchronous=OFF;
            CREATE TABLE counter(value);INSERT INTO counter VALUES(7);CREATE TABLE payload(value);",
        )
        .unwrap();
        if payload > 0 {
            db.execute("INSERT INTO payload VALUES(zeroblob(?1))", [payload])
                .unwrap();
        }
        drop(db);
        let mut file = DirectFileSystem.open(&source).unwrap();
        let geometry = Geometry::new(4096, file.file_len().unwrap()).unwrap();
        let mut objects = Vec::new();
        for index in 0..geometry.extent_count() {
            let offset = u64::from(index) * EXTENT_BYTES;
            let bytes = file
                .read_exact_at(offset, geometry.extent_length(index).unwrap() as usize)
                .unwrap();
            objects.push((ExtentId::of(&bytes).unwrap(), offset));
        }
        let manifest = ExtentManifest::new(
            ImageBinding {
                history: [1; 32],
                schema: [2; 32],
                epoch: 1,
                txid: 1,
                checksum: 0,
            },
            geometry,
            &objects.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        )
        .unwrap();
        let files = Arc::new(BoundedFiles {
            directory: root.path().into(),
            metrics: Arc::default(),
        });
        Self {
            root,
            files,
            source,
            objects,
            manifest,
        }
    }

    fn limits(&self) -> NodeExtentLimits {
        NodeExtentLimits {
            max_bytes: 64 * 1024 * 1024,
            max_objects: 32,
            max_roots: 8,
            max_root_extents: 64,
            max_references: 8,
            max_workspaces: 8,
        }
    }

    fn store(&self) -> NodeExtentStore {
        NodeExtentStore::open(
            self.root.path().join("store"),
            self.files.clone(),
            self.limits(),
        )
        .unwrap()
    }

    fn fill(&self, store: &NodeExtentStore) {
        let mut file = DirectFileSystem.open(&self.source).unwrap();
        for (id, offset) in &self.objects {
            store
                .put_extent(*id, |position, count| {
                    file.read_exact_at(offset + position, count)
                })
                .unwrap();
        }
    }

    fn scratch(&self) -> ScratchPool {
        ScratchPool::new(ScratchLimits {
            max_bytes: 2 * EXTENT_BYTES,
            max_files: 2,
        })
        .unwrap()
    }

    fn query(&self, view: &cellar_ltx::internal::RootView) -> u64 {
        let directory = self.root.path().join("cache");
        DirectFileSystem.create_dir_all(&directory).unwrap();
        let image = view
            .image(
                directory,
                self.scratch(),
                CacheLimits {
                    max_cached_bytes: EXTENT_BYTES,
                    max_cached_extents: 1,
                },
            )
            .unwrap();
        let db = open_extent_image(
            image,
            ReadServices {
                now_unix_millis: Arc::new(|| 1700000000000),
                fill_random: Arc::new(|b| b.fill(1)),
            },
        )
        .unwrap();
        db.connection()
            .query_row("SELECT value FROM counter", [], |row| row.get(0))
            .unwrap()
    }
}

#[test]
fn reopen_uses_metadata_only_inventory_then_verifies_sqlite_extents_on_demand() {
    let f = Fixture::new(9 * 1024 * 1024);
    let store = f.store();
    f.fill(&store);
    let sealed = store.seal_root(f.manifest.clone()).unwrap();
    let reference = sealed.view().reference();
    let before = store.usage().unwrap();
    assert_eq!(f.query(sealed.view()), 7);
    drop(sealed);
    assert_eq!(
        store.usage().unwrap(),
        before,
        "closing cell handles refunded persistent files"
    );
    drop(store);
    f.files.metrics.max_read.store(0, Ordering::Release);
    let store = f.store();
    assert_eq!(store.usage().unwrap().bytes, before.bytes);
    assert_eq!(
        f.files.metrics.max_read.load(Ordering::Acquire),
        0,
        "startup scanned object data"
    );
    assert!(
        matches!(store.reclaim(10), Err(StoreError::Unreconciled)),
        "GC ran before marker reconciliation"
    );
    let view = store.open_root(reference).unwrap();
    assert!(f.files.metrics.max_read.load(Ordering::Acquire) < 65536);
    assert!(matches!(
        store.seal_root(f.manifest.clone()),
        Err(StoreError::Unverified)
    ));
    assert_eq!(f.query(&view), 7);
    // The hot read verified one object, not the entire local image closure.
    assert!(matches!(
        store.seal_root(f.manifest.clone()),
        Err(StoreError::Unverified)
    ));
    for (id, _) in &f.objects {
        store.verify_extent(*id).unwrap();
    }
    store.seal_root(f.manifest.clone()).unwrap();
    assert!(f.files.metrics.max_read.load(Ordering::Acquire) <= 65536);
    assert!(f.files.metrics.max_write.load(Ordering::Acquire) <= 65536);
}

#[test]
fn staged_metadata_never_claims_missing_data_is_sealed_and_can_be_hydrated_later() {
    let f = Fixture::new(0);
    let store = f.store();
    let view = store.stage_root(f.manifest.clone()).unwrap();
    let reference = view.reference();
    assert!(matches!(
        store.seal_root(f.manifest.clone()),
        Err(StoreError::Unverified)
    ));
    let scratch = f.scratch();
    let directory = f.root.path().join("cache");
    DirectFileSystem.create_dir_all(&directory).unwrap();
    let image = view
        .image(directory, scratch.clone(), CacheLimits::default())
        .unwrap();
    assert!(image.read_exact_at(0, 100).is_err());
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
    f.fill(&store);
    let sealed = store.seal_root(f.manifest.clone()).unwrap();
    assert_eq!(sealed.view().reference(), reference);
    assert_eq!(f.query(sealed.view()), 7);
}

#[test]
fn quota_refuses_before_source_io_and_idempotent_import_never_overwrites() {
    let f = Fixture::new(0);
    let id = f.objects[0].0;
    let mut limits = f.limits();
    limits.max_bytes = u64::from(id.length());
    let store =
        NodeExtentStore::open(f.root.path().join("store"), f.files.clone(), limits).unwrap();
    f.fill(&store);
    let usage = store.usage().unwrap();
    let calls = AtomicUsize::new(0);
    let other = ExtentId::of(&[9; 4096]).unwrap();
    assert!(matches!(
        store.put_extent(other, |_, _| {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(vec![9; 4096])
        }),
        Err(StoreError::Limit)
    ));
    assert_eq!(calls.load(Ordering::Acquire), 0);
    store
        .put_extent(id, |_, _| {
            panic!("existing bytes must be independently read, not overwritten")
        })
        .unwrap();
    assert_eq!(store.usage().unwrap(), usage);
    assert!(matches!(
        store.stage_root(f.manifest.clone()),
        Err(StoreError::Limit)
    ));
    assert_eq!(store.usage().unwrap(), usage);
}

#[test]
fn owned_failures_close_and_sync_cleanup_before_refunding_capacity() {
    for failure in 0..4 {
        let f = Fixture::new(0);
        let store = f.store();
        let id = ExtentId::of(&[8; 4096]).unwrap();
        if failure == 1 {
            f.files
                .metrics
                .corrupt_output
                .store(true, Ordering::Release);
        }
        if failure == 2 {
            f.files.metrics.fail_sync.store(true, Ordering::Release);
        }
        if failure == 3 {
            f.files.metrics.fail_dir_sync.store(true, Ordering::Release);
        }
        let result = store.put_extent(id, |_, count| {
            if failure == 0 {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "source denied",
                ))
            } else {
                Ok(vec![8; count])
            }
        });
        assert!(result.is_err());
        if failure == 0 {
            assert!(
                matches!(result, Err(StoreError::Io(ref e)) if e.kind()==io::ErrorKind::PermissionDenied)
            );
        }
        let usage = store.usage().unwrap();
        assert_eq!(usage.bytes, 0);
        assert_eq!(usage.objects, 0);
        assert!(!usage.busy && !usage.quarantined);
        assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 0);
        store.put_extent(id, |_, count| Ok(vec![8; count])).unwrap();
    }
}

#[test]
fn uncertain_creation_and_cleanup_keep_charges_until_recovery_but_old_reads_work() {
    for uncertain_create in [false, true] {
        let f = Fixture::new(0);
        let store = f.store();
        f.fill(&store);
        let old = store.seal_root(f.manifest.clone()).unwrap();
        let reference = old.view().reference();
        let before = store.usage().unwrap();
        let id = ExtentId::of(&[9; 4096]).unwrap();
        if uncertain_create {
            f.files.metrics.create_mode.store(2, Ordering::Release);
        } else {
            f.files.metrics.fail_remove.store(true, Ordering::Release);
        }
        assert!(store
            .put_extent(id, |_, _| Err(io::Error::other("source failed")))
            .is_err());
        f.files.metrics.create_mode.store(0, Ordering::Release);
        let usage = store.usage().unwrap();
        assert!(usage.quarantined);
        assert_eq!(usage.bytes, before.bytes + 4096);
        assert!(matches!(
            store.put_extent(id, |_, n| Ok(vec![9; n])),
            Err(StoreError::Quarantined)
        ));
        assert_eq!(f.query(&store.open_root(reference).unwrap()), 7);
        drop(old);
        drop(store);
        let store = f.store();
        assert!(!store.usage().unwrap().quarantined);
        assert_eq!(f.query(&store.open_root(reference).unwrap()), 7);
        // The uncertain orphan is not promoted by its filename.
        assert!(store.verify_extent(id).is_err());
        store.retain_only(&[reference]).unwrap();
        store.reclaim(10).unwrap();
        assert_eq!(store.usage().unwrap().bytes, before.bytes);
    }
}

#[test]
fn source_panic_keeps_conservative_charges_and_requires_recovery() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    let old = store.seal_root(f.manifest.clone()).unwrap();
    let reference = old.view().reference();
    let before = store.usage().unwrap();
    let id = ExtentId::of(&[9; 4096]).unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.put_extent(id, |_, _| panic!("injected source panic"))
    }));
    assert!(failed.is_err());
    let usage = store.usage().unwrap();
    assert!(usage.quarantined && !usage.busy);
    assert_eq!(usage.bytes, before.bytes + 4096);
    assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 0);
    assert!(matches!(
        store.verify_extent(id),
        Err(StoreError::Quarantined)
    ));
    assert_eq!(f.query(old.view()), 7);
    drop(old);
    drop(store);
    let store = f.store();
    assert!(!store.usage().unwrap().quarantined);
    assert!(store.verify_extent(id).is_err());
    store.retain_only(&[reference]).unwrap();
    assert_eq!(store.reclaim(10).unwrap(), 1);
    assert_eq!(store.usage().unwrap().bytes, before.bytes);
    assert_eq!(f.query(&store.open_root(reference).unwrap()), 7);
}

#[test]
fn root_pins_and_closed_root_files_protect_shared_objects_until_synced_reclamation() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    let first = store.seal_root(f.manifest.clone()).unwrap();
    let first_ref = first.view().reference();
    let mut binding = f.manifest.binding();
    binding.txid += 1;
    let second_manifest = f
        .manifest
        .with_updates(binding, f.manifest.geometry(), &[])
        .unwrap();
    let second = store.seal_root(second_manifest).unwrap();
    let second_ref = second.view().reference();
    store.retain_only(&[first_ref, second_ref]).unwrap();
    assert_eq!(store.usage().unwrap().objects, 1);
    store.retire_root(first_ref).unwrap();
    assert_eq!(store.reclaim(10).unwrap(), 0);
    drop(first);
    assert_eq!(store.reclaim(1).unwrap(), 1);
    assert_eq!(f.query(second.view()), 7);
    drop(second);
    assert_eq!(
        store.reclaim(10).unwrap(),
        0,
        "closed retained root lost its data"
    );
    store.retire_root(second_ref).unwrap();
    assert_eq!(store.reclaim(1).unwrap(), 1);
    assert_eq!(
        store.usage().unwrap().objects,
        1,
        "object refund preceded root deletion barrier"
    );
    assert_eq!(store.reclaim(10).unwrap(), 1);
    assert_eq!(store.usage().unwrap().bytes, 0);
}

#[test]
fn monotonic_references_survive_view_closure_and_stale_release_or_resurrection() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    let first = store.seal_root(f.manifest.clone()).unwrap();
    let a = first.view().reference();
    let mut binding = f.manifest.binding();
    binding.txid += 1;
    let second = store
        .seal_root(
            f.manifest
                .with_updates(binding, f.manifest.geometry(), &[])
                .unwrap(),
        )
        .unwrap();
    let b = second.view().reference();
    store.retain_reference([1; 32], 1, a).unwrap();
    store.retain_reference([1; 32], 2, b).unwrap();
    store.release_reference([1; 32], 1).unwrap();
    assert!(matches!(
        store.retain_reference([1; 32], 2, a),
        Err(StoreError::Integrity)
    ));
    drop(first);
    drop(second);
    store.retain_only(&[]).unwrap();
    assert_eq!(store.reclaim(10).unwrap(), 1);
    assert_eq!(store.open_root(b).unwrap().reference(), b);
    store.release_reference([1; 32], 2).unwrap();
    assert!(matches!(
        store.retain_reference([1; 32], 2, b),
        Err(StoreError::Integrity)
    ));
    assert_eq!(store.reclaim(10).unwrap(), 2);
}

#[test]
fn root_metadata_failure_does_not_refund_data_or_change_an_existing_root() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    f.files.metrics.fail_sync.store(true, Ordering::Release);
    assert!(store.seal_root(f.manifest.clone()).is_err());
    assert_eq!(store.usage().unwrap().roots, 0);
    assert_eq!(store.usage().unwrap().objects, 1);
    let sealed = store.seal_root(f.manifest.clone()).unwrap();
    let reference = sealed.view().reference();
    let before = store.usage().unwrap();
    store.retain_only(&[reference]).unwrap();
    assert!(store
        .retain_only(&[ExtentRootReference::new(
            cellar_ltx::internal::ImageRoot::from_bytes([9; 32]),
            reference.encoded_length()
        )
        .unwrap()])
        .is_err());
    assert_eq!(store.usage().unwrap(), before);
    assert_eq!(f.query(sealed.view()), 7);
    assert_eq!(store.reclaim(10).unwrap(), 0);
}

#[test]
fn corrupted_existing_bytes_are_never_overwritten_or_reused_for_a_new_seal() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    f.files
        .metrics
        .corrupt_read_countdown
        .store(1, Ordering::Release);
    assert!(matches!(
        store.verify_extent(f.objects[0].0),
        Err(StoreError::Integrity)
    ));
    assert!(matches!(
        store.seal_root(f.manifest.clone()),
        Err(StoreError::Unverified)
    ));
    store.verify_extent(f.objects[0].0).unwrap();
    assert_eq!(
        f.query(store.seal_root(f.manifest.clone()).unwrap().view()),
        7
    );
}

#[test]
fn inventory_limits_and_unrecognized_or_symlink_names_refuse_without_source_reads() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    drop(store);
    let mut low = f.limits();
    low.max_bytes = 1;
    assert!(matches!(
        NodeExtentStore::open(f.root.path().join("store"), f.files.clone(), low),
        Err(StoreError::Limit)
    ));
    std::fs::write(f.root.path().join("store/unknown"), [1]).unwrap();
    assert!(matches!(
        NodeExtentStore::open(f.root.path().join("store"), f.files.clone(), f.limits()),
        Err(StoreError::Layout)
    ));
    std::fs::remove_file(f.root.path().join("store/unknown")).unwrap();
    #[cfg(unix)]
    {
        let path = f
            .root
            .path()
            .join("store")
            .join(format!("r-{}", "00".repeat(32)));
        std::os::unix::fs::symlink(&f.source, &path).unwrap();
        assert!(matches!(
            NodeExtentStore::open(f.root.path().join("store"), f.files.clone(), f.limits()),
            Err(StoreError::Layout)
        ));
    }
}

#[test]
fn sqlite_views_keep_the_node_inventory_lock_after_external_handles_drop() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    let sealed = store.seal_root(f.manifest.clone()).unwrap();
    let reference = sealed.view().reference();
    let bytes = store.usage().unwrap().bytes;
    let directory = f.root.path().join("cache");
    DirectFileSystem.create_dir_all(&directory).unwrap();
    let scratch = f.scratch();
    let image = sealed
        .view()
        .image(directory, scratch.clone(), CacheLimits::default())
        .unwrap();
    let db = open_extent_image(
        image,
        ReadServices {
            now_unix_millis: Arc::new(|| 1700000000000),
            fill_random: Arc::new(|b| b.fill(1)),
        },
    )
    .unwrap();
    drop(sealed);
    drop(store);
    assert!(
        NodeExtentStore::open(f.root.path().join("store"), f.files.clone(), f.limits()).is_err()
    );
    assert_eq!(
        db.connection()
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        7
    );
    assert!(scratch.usage().unwrap().reserved_bytes > 0);
    drop(db);
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
    let reopened = f.store();
    assert_eq!(reopened.usage().unwrap().bytes, bytes);
    assert_eq!(f.query(&reopened.open_root(reference).unwrap()), 7);
}

#[test]
fn entered_import_owns_its_capacity_and_lock_until_joined() {
    let f = Fixture::new(0);
    let store = f.store();
    let id = ExtentId::of(&vec![3; EXTENT_BYTES as usize]).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let worker = store.clone();
    let task = std::thread::spawn(move || {
        worker.put_extent(id, |offset, count| {
            if offset == 0 {
                started_tx.send(()).map_err(io::Error::other)?;
                release_rx.recv().map_err(io::Error::other)?;
            }
            Ok(vec![3; count])
        })
    });
    let entered = started_rx.recv_timeout(std::time::Duration::from_secs(5));
    if entered.is_err() {
        drop(release_tx);
        let _ = task.join();
        panic!("source did not enter: {entered:?}");
    }
    let usage = store.usage().unwrap();
    let refused = store.put_extent(ExtentId::of(&[4; 4096]).unwrap(), |_, _| {
        panic!("busy store ran another source")
    });
    drop(store);
    let locked =
        NodeExtentStore::open(f.root.path().join("store"), f.files.clone(), f.limits()).is_err();
    release_tx.send(()).unwrap();
    drop(release_tx);
    task.join().unwrap().unwrap();
    assert!(usage.busy);
    assert_eq!(usage.bytes, EXTENT_BYTES);
    assert!(matches!(refused, Err(StoreError::Busy)));
    assert!(locked);
    let store = f.store();
    assert_eq!(store.usage().unwrap().bytes, EXTENT_BYTES);
    store.verify_extent(id).unwrap();
    store.retain_only(&[]).unwrap();
    assert_eq!(store.reclaim(1).unwrap(), 1);
}

#[test]
fn failed_deletion_barrier_preserves_charges_and_quarantines_reclamation() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    let root = store.seal_root(f.manifest.clone()).unwrap();
    let reference = root.view().reference();
    store.retain_only(&[]).unwrap();
    drop(root);
    let before = store.usage().unwrap();
    f.files.metrics.fail_dir_sync.store(true, Ordering::Release);
    assert!(store.reclaim(1).is_err());
    let after = store.usage().unwrap();
    assert_eq!(after.bytes, before.bytes);
    assert_eq!(after.roots, before.roots);
    assert!(after.quarantined);
    assert!(matches!(store.reclaim(1), Err(StoreError::Quarantined)));
    drop(store);
    let store = f.store();
    assert!(matches!(
        store.open_root(reference),
        Err(StoreError::Missing)
    ));
    assert_eq!(store.usage().unwrap().roots, 0);
    assert_eq!(store.usage().unwrap().objects, 1);
    store.retain_only(&[]).unwrap();
    assert_eq!(store.reclaim(1).unwrap(), 1);
}

#[test]
#[ignore = "abrupt-exit child, invoked by root_publication_crash_keeps_exact_marker_selection"]
fn root_publication_crash_child() {
    let directory = std::path::PathBuf::from(
        std::env::var_os("CELLAR_EXTENT_STORE_CRASH_PATH").expect("parent path"),
    );
    let bytes = std::fs::read(directory.join("source.sqlite")).unwrap();
    let id = ExtentId::of(&bytes).unwrap();
    let manifest = ExtentManifest::new(
        ImageBinding {
            history: [1; 32],
            schema: [2; 32],
            epoch: 1,
            txid: 2,
            checksum: 0,
        },
        Geometry::new(4096, bytes.len() as u64).unwrap(),
        &[id],
    )
    .unwrap();
    let files = Arc::new(BoundedFiles {
        directory: directory.clone(),
        metrics: Arc::default(),
    });
    files
        .metrics
        .exit_after_root_sync
        .store(true, Ordering::Release);
    let store =
        NodeExtentStore::open(directory.join("store"), files, NodeExtentLimits::default()).unwrap();
    let _ = store.stage_root(manifest).unwrap();
    panic!("child passed the configured abrupt-exit cut");
}

#[test]
fn root_publication_crash_keeps_exact_marker_selection() {
    let f = Fixture::new(0);
    let store = f.store();
    f.fill(&store);
    let old = store.seal_root(f.manifest.clone()).unwrap();
    let old_ref = old.view().reference();
    drop(old);
    drop(store);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "root_publication_crash_child",
            "--ignored",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("CELLAR_EXTENT_STORE_CRASH_PATH", f.root.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(77),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let store = f.store();
    assert_eq!(store.usage().unwrap().roots, 2);
    let mut binding = f.manifest.binding();
    binding.txid = 2;
    let candidate = f
        .manifest
        .with_updates(binding, f.manifest.geometry(), &[])
        .unwrap();
    assert!(
        matches!(store.seal_root(candidate), Err(StoreError::Unverified)),
        "filename became a local closure seal after restart"
    );
    assert_eq!(f.query(&store.open_root(old_ref).unwrap()), 7);
    store.retain_only(&[old_ref]).unwrap();
    assert_eq!(store.reclaim(10).unwrap(), 1);
    assert_eq!(store.usage().unwrap().roots, 1);
    assert_eq!(f.query(&store.open_root(old_ref).unwrap()), 7);
}
