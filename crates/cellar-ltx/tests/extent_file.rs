// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Verify independent extent bytes, physical ownership and failure cleanup.

use cellar_ltx::internal::{ExtentDownload, ExtentId, EXTENT_BYTES};
use cellar_ltx::stream_snapshot::{Error, ScratchLimits, ScratchPool, ScratchUsage};
use std::sync::{atomic::Ordering, Arc};
#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;
use bounded_files::{BoundedFiles, Metrics};

fn pool(bytes: u64, files: usize) -> ScratchPool {
    ScratchPool::new(ScratchLimits {
        max_bytes: bytes,
        max_files: files,
    })
    .unwrap()
}

fn files(root: &tempfile::TempDir) -> BoundedFiles {
    BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    }
}

fn download(files: &BoundedFiles, scratch: &ScratchPool, bytes: &[u8]) -> ExtentDownload {
    ExtentDownload::new(
        files,
        Some(&files.directory),
        scratch,
        ExtentId::of(bytes).unwrap(),
    )
    .unwrap()
}

fn append(download: &mut ExtentDownload, bytes: &[u8]) {
    for (index, chunk) in bytes.chunks(65536).enumerate() {
        download.append(index as u64 * 65536, chunk).unwrap();
    }
}

fn assert_idle(files: &BoundedFiles, scratch: &ScratchPool) {
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
}

#[test]
fn exact_extent_bytes_use_bounded_io_and_preserve_content_identity() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root);
    let scratch = pool(EXTENT_BYTES, 1);
    for length in [1, 65535, 65536, 65537, EXTENT_BYTES as usize] {
        let bytes = (0..length)
            .map(|index| (index.wrapping_mul(137) % 251) as u8)
            .collect::<Vec<_>>();
        let mut pending = download(&files, &scratch, &bytes);
        append(&mut pending, &bytes);
        let extent = pending.finish().unwrap();
        assert_eq!(extent.identity(), ExtentId::of(&bytes).unwrap());
        extent.verify().unwrap();
        for (index, chunk) in bytes.chunks(65536).enumerate() {
            assert_eq!(
                extent.chunk(index as u64 * 65536, chunk.len()).unwrap(),
                chunk
            );
        }
        for (offset, count) in [(0, 0), (0, 65537), (length as u64, 1), (u64::MAX, 1)] {
            assert!(matches!(extent.chunk(offset, count), Err(Error::Limit)));
        }
        drop(extent);
        assert_idle(&files, &scratch);
    }
    assert_eq!(files.metrics.max_read.load(Ordering::Relaxed), 65536);
    assert_eq!(files.metrics.max_write.load(Ordering::Relaxed), 65536);
}

#[test]
fn capacity_is_reserved_before_creation_and_shared_until_last_file_owner_drops() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root);
    let scratch = pool(8192, 2);
    let bytes = vec![3; 4096];
    let mut pending = download(&files, &scratch, &bytes);
    append(&mut pending, &bytes);
    let extent = pending.finish().unwrap();
    let retained = extent.clone();
    let unfinished = download(&files, &scratch, &bytes);
    assert!(matches!(
        ExtentDownload::new(&files, None, &scratch, extent.identity()),
        Err(Error::ScratchFull)
    ));
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 2);
    assert_eq!(
        scratch.usage().unwrap(),
        ScratchUsage {
            reserved_bytes: 8192,
            reserved_files: 2
        }
    );
    drop(unfinished);
    drop(extent);
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 1);
    assert_eq!(
        scratch.usage().unwrap(),
        ScratchUsage {
            reserved_bytes: 4096,
            reserved_files: 1
        }
    );
    assert_eq!(retained.chunk(100, 20).unwrap(), vec![3; 20]);
    drop(retained);
    assert_idle(&files, &scratch);

    let small = pool(4095, 1);
    assert!(matches!(
        ExtentDownload::new(&files, None, &small, ExtentId::of(&bytes).unwrap()),
        Err(Error::ScratchFull)
    ));
    assert_idle(&files, &small);
}

#[test]
fn rejected_append_permanently_poisons_the_owner_and_retains_admission() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root);
    let scratch = pool(EXTENT_BYTES, 1);
    let bytes = vec![4; 65536];
    for (offset, bad) in [
        (1, vec![4]),
        (0, vec![]),
        (0, vec![4; 65537]),
        (u64::MAX, vec![4]),
    ] {
        let mut pending = download(&files, &scratch, &bytes);
        assert!(pending.append(offset, &bad).is_err());
        assert!(pending.append(0, &bytes).is_err());
        assert_eq!(scratch.usage().unwrap().reserved_bytes, 65536);
        assert!(pending.finish().is_err());
        assert_idle(&files, &scratch);
    }
    let mut pending = download(&files, &scratch, &[1, 2]);
    pending.append(0, &[1]).unwrap();
    assert!(pending.append(0, &[1]).is_err());
    assert!(pending.append(1, &[2]).is_err());
    assert!(pending.finish().is_err());
    assert_idle(&files, &scratch);
}

#[test]
fn incomplete_wrong_or_extra_content_never_seals() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root);
    let scratch = pool(4096, 1);
    let bytes = vec![5; 4096];
    let mut partial = download(&files, &scratch, &bytes);
    partial.append(0, &bytes[..4095]).unwrap();
    assert!(partial.finish().is_err());
    assert_idle(&files, &scratch);
    let mut wrong = download(&files, &scratch, &bytes);
    wrong.append(0, &[6; 4096]).unwrap();
    assert!(matches!(wrong.finish(), Err(Error::Integrity)));
    assert_idle(&files, &scratch);
    let mut extra = download(&files, &scratch, &bytes);
    extra.append(0, &bytes).unwrap();
    assert!(extra.append(4096, &[5]).is_err());
    assert!(extra.finish().is_err());
    assert_idle(&files, &scratch);
}

#[test]
fn physical_corruption_short_read_and_uncertain_sync_cannot_return_a_file() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root);
    let scratch = pool(65536, 1);
    for fault in ["corrupt", "short", "sync"] {
        let mut pending = download(&files, &scratch, &[7; 65536]);
        if fault == "corrupt" {
            files.metrics.corrupt_output.store(true, Ordering::Release);
        }
        pending.append(0, &[7; 65536]).unwrap();
        if fault == "short" {
            files.metrics.short_read.store(true, Ordering::Release);
        }
        if fault == "sync" {
            files.metrics.fail_sync.store(true, Ordering::Release);
        }
        assert!(pending.finish().is_err(), "accepted {fault}");
        assert_idle(&files, &scratch);
    }
}

#[test]
fn uncertain_partial_write_is_not_retryable_and_cleans_up_its_file() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root);
    let scratch = pool(4096, 1);
    let mut pending = download(&files, &scratch, &[8; 4096]);
    files.metrics.fail_append.store(true, Ordering::Release);
    assert!(pending.append(0, &[8; 4096]).is_err());
    assert!(pending.append(0, &[8; 4096]).is_err());
    assert!(matches!(
        ExtentDownload::new(&files, None, &scratch, ExtentId::of(&[8; 4096]).unwrap()),
        Err(Error::ScratchFull)
    ));
    assert!(pending.finish().is_err());
    assert_idle(&files, &scratch);
}

#[test]
fn creation_failure_refunds_the_complete_reservation() {
    let root = tempfile::tempdir().unwrap();
    let files = BoundedFiles {
        directory: root.path().join("absent"),
        metrics: Arc::<Metrics>::default(),
    };
    let scratch = pool(4096, 1);
    assert!(
        ExtentDownload::new(&files, None, &scratch, ExtentId::of(&[9; 4096]).unwrap()).is_err()
    );
    assert_idle(&files, &scratch);
}

#[test]
fn read_faults_after_sealing_never_expose_unauthenticated_bytes() {
    let root = tempfile::tempdir().unwrap();
    let files = files(&root);
    let bytes = (0..65553)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let scratch = pool(bytes.len() as u64, 1);
    let mut pending = download(&files, &scratch, &bytes);
    append(&mut pending, &bytes);
    let extent = pending.finish().unwrap();
    for (offset, length) in [(0, 1), (1, 65536), (65535, 2), (65536, 17)] {
        files
            .metrics
            .corrupt_read_countdown
            .store(1, Ordering::Release);
        assert!(
            matches!(extent.chunk(offset, length), Err(Error::Integrity)),
            "post-seal corruption escaped at {offset}/{length}"
        );
        files
            .metrics
            .corrupt_read_countdown
            .store(0, Ordering::Release);
        assert_eq!(
            extent.chunk(offset, length).unwrap(),
            bytes[offset as usize..offset as usize + length]
        );
    }
    // The first block is valid but a later block is faulty. No partial output
    // may escape after an earlier portion of the request has been validated.
    files
        .metrics
        .corrupt_read_countdown
        .store(2, Ordering::Release);
    assert!(matches!(extent.chunk(1, 65536), Err(Error::Integrity)));
    assert_eq!(scratch.usage().unwrap().reserved_bytes, bytes.len() as u64);
    drop(extent);
    assert_idle(&files, &scratch);
    assert!(files.metrics.max_read.load(Ordering::Acquire) <= 65536);
}
