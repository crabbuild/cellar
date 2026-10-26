// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Faults at the disposable destination must never become a trusted SQL base.

use super::super::{
    store::NodeExtentStore,
    tree::{ExtentId, Geometry, ImageBinding},
};
use super::*;
use crate::{stream_snapshot::ScratchLimits, DirectFileSystem};

struct Fixture {
    workspace: ExtentWorkspace,
    bytes: Vec<u8>,
    _directory: tempfile::TempDir,
}

impl Fixture {
    fn new(populated: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let bytes = if populated {
            let connection = rusqlite::Connection::open_in_memory().unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE payload(value); INSERT INTO payload VALUES(zeroblob(1048576));",
                )
                .unwrap();
            connection.serialize(rusqlite::MAIN_DB).unwrap().to_vec()
        } else {
            Vec::new()
        };
        let geometry = Geometry::new(if populated { 4096 } else { 0 }, bytes.len() as u64).unwrap();
        let extents = bytes
            .chunks(EXTENT_BYTES as usize)
            .map(|chunk| ExtentId::of(chunk).unwrap())
            .collect::<Vec<_>>();
        let manifest = ExtentManifest::new(
            ImageBinding {
                history: [1; 32],
                schema: [2; 32],
                epoch: 1,
                txid: u64::from(populated),
                checksum: 0,
            },
            geometry,
            &extents,
        )
        .unwrap();
        let store = NodeExtentStore::open(
            directory.path().join("extents"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )
        .unwrap();
        for (id, chunk) in extents.into_iter().zip(bytes.chunks(EXTENT_BYTES as usize)) {
            store
                .put_extent(id, |offset, count| {
                    Ok(chunk[offset as usize..offset as usize + count].to_vec())
                })
                .unwrap();
        }
        let base = Arc::new(
            store
                .seal_image_root(manifest, &mut vec![0; EXTENT_BYTES as usize])
                .unwrap(),
        );
        let cache = directory.path().join("cache");
        DirectFileSystem.create_dir_all(&cache).unwrap();
        let workspace = ExtentWorkspace::new(
            base,
            WorkspaceOptions {
                path: directory.path().join("working.sqlite"),
                sqlite_vfs: None,
                filesystem: Arc::new(DirectFileSystem),
                max_image_bytes: 2 * EXTENT_BYTES,
                max_wal_bytes: EXTENT_BYTES,
                cache_directory: cache,
                scratch: ScratchPool::new(ScratchLimits {
                    max_bytes: 2 * EXTENT_BYTES,
                    max_files: 2,
                })
                .unwrap(),
                cache: CacheLimits {
                    max_cached_bytes: EXTENT_BYTES,
                    max_cached_extents: 1,
                },
                retention: None,
            },
        )
        .unwrap();
        Self {
            workspace,
            bytes,
            _directory: directory,
        }
    }
}

#[derive(Default)]
struct MemoryFile {
    bytes: Vec<u8>,
    corrupt_write: bool,
    corrupt_read: bool,
    fail_read: bool,
    ignore_truncate: bool,
    size_calls: usize,
    fail_size_at: Option<usize>,
}

impl MainFile for MemoryFile {
    fn size(&mut self) -> Result<u64, i32> {
        self.size_calls += 1;
        if self.fail_size_at == Some(self.size_calls) {
            return Err(ffi::SQLITE_IOERR_FSTAT);
        }
        Ok(self.bytes.len() as u64)
    }

    fn read(&mut self, offset: u64, output: &mut [u8]) -> Result<(), i32> {
        if self.fail_read {
            return Err(ffi::SQLITE_IOERR_READ);
        }
        let start = offset as usize;
        let count = self.bytes.len().saturating_sub(start).min(output.len());
        output.fill(0);
        if count != 0 {
            output[..count].copy_from_slice(&self.bytes[start..start + count]);
        }
        if self.corrupt_read && count != 0 {
            output[count - 1] ^= 1;
        }
        if count != output.len() {
            return Err(ffi::SQLITE_IOERR_SHORT_READ);
        }
        Ok(())
    }

    fn write(&mut self, offset: u64, input: &[u8]) -> Result<(), i32> {
        let start = offset as usize;
        let end = start + input.len();
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
        }
        self.bytes[start..end].copy_from_slice(input);
        if self.corrupt_write && !input.is_empty() {
            self.bytes[end - 1] ^= 1;
        }
        Ok(())
    }

    fn truncate(&mut self, length: u64) -> Result<(), i32> {
        if !self.ignore_truncate {
            self.bytes.resize(length as usize, 0);
        }
        Ok(())
    }
}

fn poisoned(main: &MainWorkspace, file: &mut MemoryFile) {
    assert!(main.stats().is_err());
    assert!(main.cut(0).is_err());
    assert!(main.read(0, &mut [0; 16], file).is_err());
    assert!(main.write(0, &[1], file).is_err());
    assert!(main.write_wal(0, &[1], file).is_err());
    assert!(main.initialize(file).is_err());
}

#[test]
fn hydration_verifies_destination_before_exposing_the_base() {
    for fault in 0..3 {
        let fixture = Fixture::new(true);
        let main = &fixture.workspace.main;
        let root = main.base.reference();
        let mut file = MemoryFile::default();
        main.initialize(&mut file).unwrap();
        file.corrupt_write = fault == 0;
        file.corrupt_read = fault == 1;
        file.fail_read = fault == 2;
        let mut output = [0xa5; 16];
        assert!(main.read(0, &mut output, &mut file).is_err());
        assert_eq!(output, [0xa5; 16], "unverified destination reached SQL");
        assert_eq!(main.base.reference(), root);
        poisoned(main, &mut file);
    }
}

#[test]
fn verified_hydration_and_partial_truncation_preserve_the_exact_source_prefix() {
    let fixture = Fixture::new(true);
    let main = &fixture.workspace.main;
    let mut file = MemoryFile::default();
    main.initialize(&mut file).unwrap();
    // Truncate before hydration; subsequent growth must not resurrect the
    // discarded authenticated suffix, including within the same extent.
    main.truncate(96 * 1024, &mut file).unwrap();
    main.truncate(fixture.bytes.len() as u64, &mut file)
        .unwrap();
    let mut output = vec![0; fixture.bytes.len()];
    main.read(0, &mut output, &mut file).unwrap();
    assert_eq!(&output[..96 * 1024], &fixture.bytes[..96 * 1024]);
    assert!(output[96 * 1024..].iter().all(|byte| *byte == 0));
    assert_eq!(main.stats().unwrap().hydrated_extents, 1);
    assert_eq!(main.stats().unwrap().bytes_faulted, 96 * 1024);
}

#[test]
fn main_and_wal_writes_reject_corrupt_or_unreadable_destinations() {
    for wal in [false, true] {
        for fault in 0..3 {
            let fixture = Fixture::new(false);
            let main = &fixture.workspace.main;
            let mut file = MemoryFile::default();
            main.initialize(&mut file).unwrap();
            file.corrupt_write = fault == 0;
            file.corrupt_read = fault == 1;
            file.fail_read = fault == 2;
            let bytes = vec![0x63; 96 * 1024];
            let result = if wal {
                main.write_wal(17, &bytes, &mut file)
            } else {
                main.write(17, &bytes, &mut file)
            };
            assert!(result.is_err());
            poisoned(main, &mut file);
        }
    }
}

#[test]
fn main_and_wal_truncation_poison_on_false_success_or_failed_size_verification() {
    for wal in [false, true] {
        for fail_size in [false, true] {
            let fixture = Fixture::new(false);
            let main = &fixture.workspace.main;
            let mut file = MemoryFile::default();
            main.initialize(&mut file).unwrap();
            file.ignore_truncate = !fail_size;
            if fail_size {
                file.fail_size_at = Some(file.size_calls + 1);
            }
            let result = if wal {
                main.truncate_wal(4096, &mut file)
            } else {
                main.truncate(4096, &mut file)
            };
            assert!(result.is_err());
            poisoned(main, &mut file);
        }
    }
}

#[test]
fn initialization_poison_covers_initial_and_post_truncate_size_failures() {
    for call in [1, 2] {
        let fixture = Fixture::new(true);
        let main = &fixture.workspace.main;
        let mut file = MemoryFile {
            fail_size_at: Some(call),
            ..Default::default()
        };
        assert!(main.initialize(&mut file).is_err());
        poisoned(main, &mut file);
    }
}
