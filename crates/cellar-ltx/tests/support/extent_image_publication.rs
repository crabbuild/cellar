// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Image-level barriers through actual SQLite/LTX and the injected filesystem.
use super::*;
use std::{sync::mpsc, thread, time::Duration};

#[test]
fn a_retried_image_reverifies_bytes_without_repeating_completed_barriers() {
    let (fixture, before) = Fixture::new(4096, 4096);
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = fixture.seal(&before, false, &mut buffer);
    let after = fixture.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, 4096, 2, false);
    let update = fixture.update(&base, &encoded, &after, 2, false);
    let first = fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    let files = fixture
        .files
        .metrics
        .file_sync_calls
        .load(Ordering::Acquire);
    let directories = fixture.files.metrics.dir_sync_calls.load(Ordering::Acquire);
    let reads = fixture.files.metrics.read_bytes.load(Ordering::Acquire);
    let again = fixture
        .store
        .prepare_transient_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    assert_eq!(again.image.reference(), first.image.reference());
    assert_eq!(
        fixture
            .files
            .metrics
            .file_sync_calls
            .load(Ordering::Acquire),
        files
    );
    assert_eq!(
        fixture.files.metrics.dir_sync_calls.load(Ordering::Acquire),
        directories
    );
    assert!(fixture.files.metrics.read_bytes.load(Ordering::Acquire) - reads >= after.len());
    fixture.query(&again.image, 8);
}

#[test]
fn a_contiguous_ltx_chain_shares_its_final_directory_barrier() {
    let (fixture, before) = Fixture::new(4096, 4096);
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = fixture.seal(&before, true, &mut buffer);
    let middle = fixture.mutate("UPDATE counter SET value=8");
    let encoded2 = delta(&before, &middle, 4096, 2, true);
    let after = fixture.mutate("UPDATE counter SET value=9");
    let encoded3 = delta(&middle, &after, 4096, 3, true);
    let mut update = fixture.update(&base, &encoded2, &middle, 2, true);
    let next = fixture.update(&base, &encoded3, &after, 3, true);
    update.result = next.result;
    update.binding = next.binding;
    update.geometry = next.geometry;
    update.segments.extend(next.segments);
    let files = fixture
        .files
        .metrics
        .file_sync_calls
        .load(Ordering::Acquire);
    let directories = fixture.files.metrics.dir_sync_calls.load(Ordering::Acquire);
    let prepared = fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    assert_eq!(prepared.work.extents_prepared, 2);
    assert_eq!(
        fixture
            .files
            .metrics
            .file_sync_calls
            .load(Ordering::Acquire)
            - files,
        3
    );
    assert_eq!(
        fixture.files.metrics.dir_sync_calls.load(Ordering::Acquire) - directories,
        1
    );
    fixture.query(&prepared.image, 9);
    fixture.query(&base, 7);
}

#[test]
fn file_barrier_failures_never_produce_an_image_and_preserve_retry_verification() {
    for failed_file in 1..=3 {
        let (fixture, before) = Fixture::new(4096, 5 * 1024 * 1024);
        let mut buffer = vec![0; EXTENT_BYTES as usize];
        let base = fixture.seal(&before, false, &mut buffer);
        let after = fixture
            .mutate("UPDATE padding SET value=zeroblob(6291456);UPDATE counter SET value=8;VACUUM");
        let encoded = delta(&before, &after, 4096, 2, false);
        let update = fixture.update(&base, &encoded, &after, 2, false);
        fixture
            .files
            .metrics
            .fail_file_sync_countdown
            .store(failed_file, Ordering::Release);
        assert!(fixture
            .store
            .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
            .is_err());
        assert_eq!(
            fixture
                .files
                .metrics
                .fail_file_sync_countdown
                .load(Ordering::Acquire),
            0
        );
        assert!(
            !fixture.store.usage().unwrap().quarantined,
            "successful owned cleanup remains retryable"
        );
        assert!(matches!(
            fixture.store.seal_root(manifest(&after, 4096, 2, false)),
            Err(StoreError::Unverified)
        ));
        fixture.query(&base, 7);
        let prepared = fixture
            .store
            .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
            .unwrap();
        fixture.query(&prepared.image, 8);
    }
}

#[test]
fn failed_shared_directory_barrier_quarantines_all_charges_until_recovery() {
    let (fixture, before) = Fixture::new(4096, 4096);
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = fixture.seal(&before, false, &mut buffer);
    let initial = fixture.store.usage().unwrap();
    let after = fixture.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, 4096, 2, false);
    let update = fixture.update(&base, &encoded, &after, 2, false);
    let expected = manifest(&after, 4096, 2, false);
    fixture
        .files
        .metrics
        .fail_dir_sync
        .store(true, Ordering::Release);
    assert!(fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .is_err());
    let failed = fixture.store.usage().unwrap();
    assert!(failed.quarantined && !failed.busy);
    assert_eq!(failed.objects, initial.objects + 1);
    assert_eq!(failed.roots, initial.roots + 1);
    assert!(failed.bytes > initial.bytes);
    assert!(matches!(
        fixture.store.reclaim(100),
        Err(StoreError::Quarantined)
    ));
    assert_eq!(fixture.store.usage().unwrap(), failed);
    assert!(matches!(
        fixture.store.seal_root(expected.clone()),
        Err(StoreError::Quarantined)
    ));
    fixture.query(&base, 7);
    drop(base);
    let Fixture {
        root,
        path,
        files,
        store,
        page_size,
    } = fixture;
    drop(store);
    let store = NodeExtentStore::open(root.path().join("store"), files.clone(), Default::default())
        .unwrap();
    assert!(matches!(
        store.seal_root(expected.clone()),
        Err(StoreError::Unverified)
    ));
    store.open_root(reference(&expected)).unwrap();
    for index in 0..expected.geometry().extent_count() {
        store
            .verify_extent(expected.extent(index).unwrap())
            .unwrap();
    }
    let recovered = store.seal_image_root(expected, &mut buffer).unwrap();
    let fixture = Fixture {
        root,
        path,
        files,
        store,
        page_size,
    };
    fixture.query(&recovered, 8);
}

#[test]
fn readers_establish_their_own_barrier_and_failed_reads_revoke_pending_seals() {
    for (corrupt, cached) in [(false, false), (false, true), (true, false)] {
        let (fixture, before) = Fixture::new(4096, 4096);
        let mut buffer = vec![0; EXTENT_BYTES as usize];
        let base = fixture.seal(&before, false, &mut buffer);
        let after = fixture.mutate("UPDATE counter SET value=8");
        let encoded = delta(&before, &after, 4096, 2, false);
        let update = fixture.update(&base, &encoded, &after, 2, false);
        let expected = manifest(&after, 4096, 2, false);
        // A receive may already own metadata for the not-yet-imported image.
        // Its view must not treat another owner's file-only state as durable.
        let view = fixture.store.stage_root(expected.clone()).unwrap();
        let id = expected.extent(0).unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        *fixture.files.metrics.before_dir_sync.lock().unwrap() = Some(Box::new(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }));
        thread::scope(|scope| {
            let worker = scope.spawn(|| {
                fixture.store.prepare_image_root(
                    Some(&base),
                    &update,
                    &ApplyLimits::default(),
                    &mut buffer,
                )
            });
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            assert!(fixture.store.usage().unwrap().busy);
            assert!(!worker.is_finished());
            assert!(matches!(
                fixture.store.seal_root(expected.clone()),
                Err(StoreError::Busy)
            ));
            if corrupt {
                let digest = id
                    .bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                let path = fixture
                    .root
                    .path()
                    .join("store")
                    .join(format!("e-{:08x}-{digest}", id.length()));
                let original = std::fs::read(&path).unwrap();
                let mut damaged = original.clone();
                damaged[0] ^= 1;
                std::fs::write(&path, damaged).unwrap();
                assert!(view.read_extent(id).is_err());
                std::fs::write(path, original).unwrap();
            } else {
                let files = fixture
                    .files
                    .metrics
                    .file_sync_calls
                    .load(Ordering::Acquire);
                let directories = fixture.files.metrics.dir_sync_calls.load(Ordering::Acquire);
                if cached {
                    fixture.query_view(&view, 8);
                } else {
                    assert_eq!(view.read_extent(id).unwrap(), after);
                }
                assert_eq!(
                    fixture
                        .files
                        .metrics
                        .file_sync_calls
                        .load(Ordering::Acquire)
                        - files,
                    1
                );
                assert_eq!(
                    fixture.files.metrics.dir_sync_calls.load(Ordering::Acquire) - directories,
                    1
                );
            }
            release_tx.send(()).unwrap();
            let result = worker.join().unwrap();
            if corrupt {
                assert!(matches!(result, Err(StoreError::Unverified)));
            } else {
                fixture.query(&result.unwrap().image, 8);
            }
        });
        fixture.query(&base, 7);
        if corrupt {
            let retried = fixture
                .store
                .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
                .unwrap();
            fixture.query(&retried.image, 8);
        }
    }
}

const CUT_DIRECTORY: &str = "CELLAR_TEST_EXTENT_IMAGE_PUBLICATION_CUT";

#[test]
fn image_file_sync_cut_child() {
    let Some(path) = std::env::var_os(CUT_DIRECTORY) else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let after = std::fs::read(path.join("snapshot.sqlite")).unwrap();
    let encoded = delta(&[], &after, 4096, 1, false);
    let expected = manifest(&after, 4096, 1, false);
    let empty = ExtentManifest::new(
        ImageBinding {
            txid: 0,
            ..expected.binding()
        },
        Geometry::new(0, 0).unwrap(),
        &[],
    )
    .unwrap();
    let file = ltx::decode_file_bounded(&encoded, Default::default()).unwrap();
    let update = ImageRootUpdate {
        base: reference(&empty),
        result: reference(&expected),
        binding: expected.binding(),
        geometry: expected.geometry(),
        segments: vec![LtxSegment {
            bytes: &encoded,
            min_txid: TXID(1),
            max_txid: TXID(1),
            page_count: file.pages().len(),
            decoded_bytes: file.decoded_bytes(),
        }],
    };
    let files = Arc::new(bounded_files::BoundedFiles {
        directory: path.clone(),
        metrics: Arc::default(),
    });
    let store =
        NodeExtentStore::open(path.join("cut-store"), files.clone(), Default::default()).unwrap();
    files
        .metrics
        .exit_after_root_sync
        .store(true, Ordering::Release);
    let result = store.prepare_image_root(
        None,
        &update,
        &ApplyLimits::default(),
        &mut vec![0; EXTENT_BYTES as usize],
    );
    assert!(
        result.is_err(),
        "child returned an image before the intentional process exit"
    );
    panic!("child did not reach the root file-sync cut");
}

#[test]
fn abrupt_exit_after_root_file_sync_leaves_no_reusable_closure_seal() {
    let (fixture, after) = Fixture::new(4096, 5 * 1024 * 1024);
    std::fs::write(fixture.root.path().join("snapshot.sqlite"), &after).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "image_publication::image_file_sync_cut_child",
            "--nocapture",
        ])
        .env(CUT_DIRECTORY, fixture.root.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(77));
    let expected = manifest(&after, 4096, 1, false);
    let store = NodeExtentStore::open(
        fixture.root.path().join("cut-store"),
        fixture.files.clone(),
        Default::default(),
    )
    .unwrap();
    assert!(matches!(
        store.seal_root(expected.clone()),
        Err(StoreError::Unverified)
    ));
    let view = store.open_root(reference(&expected)).unwrap();
    assert_eq!(view.reference(), reference(&expected));
    for index in 0..expected.geometry().extent_count() {
        store
            .verify_extent(expected.extent(index).unwrap())
            .unwrap();
    }
    let recovered = store
        .seal_image_root(expected, &mut vec![0; EXTENT_BYTES as usize])
        .unwrap();
    fixture.query(&recovered, 7);
}
