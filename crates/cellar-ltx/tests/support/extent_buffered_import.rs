// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete admitted input uses bounded provider windows, with the ordinary
//! destination proof, persistence, quota and uncertain-write cleanup contract.

use super::*;

#[test]
fn complete_input_uses_one_native_window_and_preserves_bounded_provider_fallback() {
    for native in [true, false] {
        let f = Fixture::new(0);
        f.files
            .metrics
            .native_random_windows
            .store(native, Ordering::Release);
        let store = f.store();
        f.files.metrics.dir_sync_calls.store(0, Ordering::Release);
        let bytes = vec![7; EXTENT_BYTES as usize];
        let id = ExtentId::of(&bytes).unwrap();
        store.put_extent_bytes(id, &bytes).unwrap();
        let metrics = &f.files.metrics;
        assert_eq!(
            metrics.random_write_calls.load(Ordering::Acquire),
            if native { 1 } else { 64 }
        );
        assert_eq!(metrics.random_write_window_calls.load(Ordering::Acquire), 1);
        assert_eq!(
            metrics.max_write.load(Ordering::Acquire),
            if native { bytes.len() } else { 65536 }
        );
        assert_eq!(metrics.max_read.load(Ordering::Acquire), 65536);
        assert_eq!(metrics.file_sync_calls.load(Ordering::Acquire), 1);
        assert_eq!(metrics.dir_sync_calls.load(Ordering::Acquire), 1);
        assert_eq!(metrics.active_handles.load(Ordering::Acquire), 0);
        assert_eq!(store.usage().unwrap().bytes, EXTENT_BYTES);
        drop(store);
        // A reopened name still has no persistence/integrity proof. The new
        // input path must verify the actual existing file before reuse.
        let store = f.store();
        metrics.random_write_calls.store(0, Ordering::Release);
        metrics.file_sync_calls.store(0, Ordering::Release);
        metrics.dir_sync_calls.store(0, Ordering::Release);
        store.put_extent_bytes(id, &bytes).unwrap();
        assert_eq!(metrics.random_write_calls.load(Ordering::Acquire), 0);
        assert_eq!(metrics.file_sync_calls.load(Ordering::Acquire), 1);
        assert_eq!(metrics.dir_sync_calls.load(Ordering::Acquire), 1);
        metrics.corrupt_read_countdown.store(1, Ordering::Release);
        assert!(matches!(
            store.put_extent_bytes(id, &bytes),
            Err(StoreError::Integrity)
        ));
        assert_eq!(metrics.random_write_calls.load(Ordering::Acquire), 0);
    }
}

#[test]
fn complete_input_refuses_wrong_length_and_capacity_before_writes() {
    let f = Fixture::new(0);
    let mut limits = f.limits();
    limits.max_bytes = 4096;
    let store =
        NodeExtentStore::open(f.root.path().join("store"), f.files.clone(), limits).unwrap();
    let bytes = vec![9; EXTENT_BYTES as usize];
    let id = ExtentId::of(&bytes).unwrap();
    assert!(matches!(
        store.put_extent_bytes(id, &bytes[..4096]),
        Err(StoreError::Integrity)
    ));
    assert!(matches!(
        store.put_extent_bytes(id, &bytes),
        Err(StoreError::Limit)
    ));
    assert_eq!(
        f.files.metrics.random_write_calls.load(Ordering::Acquire),
        0
    );
    assert_eq!(store.usage().unwrap().bytes, 0);
}

#[test]
fn complete_input_preserves_independent_verification_and_owned_failure_cleanup() {
    for native in [true, false] {
        for failure in 0..5 {
            let f = Fixture::new(0);
            f.files
                .metrics
                .native_random_windows
                .store(native, Ordering::Release);
            let store = f.store();
            let bytes = vec![8; EXTENT_BYTES as usize];
            let id = ExtentId::of(&bytes).unwrap();
            match failure {
                0 => (),
                1 => f
                    .files
                    .metrics
                    .corrupt_output
                    .store(true, Ordering::Release),
                2 => f.files.metrics.fail_sync.store(true, Ordering::Release),
                3 => f.files.metrics.fail_dir_sync.store(true, Ordering::Release),
                4 => f.files.metrics.fail_append.store(true, Ordering::Release),
                _ => unreachable!(),
            }
            let result = if failure == 0 {
                let corrupt = vec![9; bytes.len()];
                store.put_extent_bytes(id, &corrupt)
            } else {
                store.put_extent_bytes(id, &bytes)
            };
            assert!(result.is_err(), "native={native} failure={failure}");
            let usage = store.usage().unwrap();
            assert_eq!(usage.bytes, 0);
            assert_eq!(usage.objects, 0);
            assert!(!usage.busy && !usage.quarantined);
            assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 0);
            store.put_extent_bytes(id, &bytes).unwrap();
        }
    }
}

#[test]
fn complete_input_keeps_uncertain_cleanup_charged_and_quarantined() {
    let f = Fixture::new(0);
    f.files
        .metrics
        .native_random_windows
        .store(true, Ordering::Release);
    let store = f.store();
    let bytes = vec![8; EXTENT_BYTES as usize];
    let id = ExtentId::of(&bytes).unwrap();
    f.files.metrics.fail_append.store(true, Ordering::Release);
    f.files.metrics.fail_remove.store(true, Ordering::Release);
    assert!(matches!(
        store.put_extent_bytes(id, &bytes),
        Err(StoreError::Cleanup { .. })
    ));
    let usage = store.usage().unwrap();
    assert!(usage.quarantined && !usage.busy);
    assert_eq!(usage.bytes, EXTENT_BYTES);
    assert_eq!(usage.objects, 1);
    assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 0);
    assert!(matches!(
        store.put_extent_bytes(id, &bytes),
        Err(StoreError::Quarantined)
    ));
}
