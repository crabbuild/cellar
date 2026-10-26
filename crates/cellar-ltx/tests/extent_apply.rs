// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual SQLite/LTX transitions over the persistent extent owner. Native
//! marker/SDK integration is a separate boundary, not implied by these tests.
#![allow(clippy::disallowed_methods)]
#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

use cellar_ltx::{
    apply::{ApplyLimits, LtxSegment},
    generation::ReadServices,
    internal::{
        open_extent_image, CacheLimits, ExtentId, ExtentManifest, ExtentRootReference, Geometry,
        ImageBinding, ImageRoot, ImageRootUpdate, NodeExtentStore, SealedImageRoot, StoreError,
        EXTENT_BYTES,
    },
    ltx,
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem, FileSystem, TXID,
};
use std::sync::{atomic::Ordering, Arc};

fn binding(bytes: &[u8], page_size: u32, txid: u64, checksums: bool) -> ImageBinding {
    let checksum = if checksums {
        bytes
            .chunks_exact(page_size as usize)
            .enumerate()
            .filter(|(index, _)| *index as u32 + 1 != ltx::lock_pgno(page_size))
            .fold(cellar_ltx::CHECKSUM_FLAG, |sum, (index, page)| {
                cellar_ltx::CHECKSUM_FLAG | (sum ^ ltx::checksum_page(index as u32 + 1, page))
            })
    } else {
        0
    };
    ImageBinding {
        history: [1; 32],
        schema: [2; 32],
        epoch: 1,
        txid,
        checksum,
    }
}

fn manifest(bytes: &[u8], page_size: u32, txid: u64, checksums: bool) -> ExtentManifest {
    ExtentManifest::new(
        binding(bytes, page_size, txid, checksums),
        Geometry::new(page_size, bytes.len() as u64).unwrap(),
        &bytes
            .chunks(EXTENT_BYTES as usize)
            .map(|part| ExtentId::of(part).unwrap())
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

fn reference(manifest: &ExtentManifest) -> ExtentRootReference {
    ExtentRootReference::new(manifest.root(), manifest.encode().unwrap().len() as u32).unwrap()
}

fn delta(before: &[u8], after: &[u8], page_size: u32, txid: u64, checksums: bool) -> Vec<u8> {
    let pages = after
        .chunks_exact(page_size as usize)
        .enumerate()
        .filter_map(|(index, page)| {
            let offset = index * page_size as usize;
            (before.get(offset..offset + page.len()) != Some(page))
                .then(|| (index as u32 + 1, page.to_vec()))
        })
        .collect::<Vec<_>>();
    ltx::encode_file_v0_5_2(
        &ltx::Header {
            version: ltx::VERSION,
            flags: if checksums {
                0
            } else {
                ltx::HEADER_FLAG_NO_CHECKSUM
            },
            page_size,
            commit: (after.len() / page_size as usize) as u32,
            min_txid: TXID(txid),
            max_txid: TXID(txid),
            pre_apply_checksum: if checksums && txid != 1 {
                binding(before, page_size, txid - 1, true).checksum
            } else {
                0
            },
            ..Default::default()
        },
        &pages,
        binding(after, page_size, txid, checksums).checksum,
    )
    .unwrap()
}

struct Fixture {
    root: tempfile::TempDir,
    path: std::path::PathBuf,
    files: Arc<bounded_files::BoundedFiles>,
    store: NodeExtentStore,
    page_size: u32,
}

impl Fixture {
    fn new(page_size: u32, payload: u64) -> (Self, Vec<u8>) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source.sqlite");
        let db = rusqlite::Connection::open(&path).unwrap();
        db.pragma_update(None, "page_size", page_size).unwrap();
        db.execute_batch("CREATE TABLE counter(value);INSERT INTO counter VALUES(7);CREATE TABLE padding(value);").unwrap();
        db.execute("INSERT INTO padding VALUES(zeroblob(?1))", [payload])
            .unwrap();
        drop(db);
        let bytes = std::fs::read(&path).unwrap();
        let files = Arc::new(bounded_files::BoundedFiles {
            directory: root.path().into(),
            metrics: Arc::default(),
        });
        let store =
            NodeExtentStore::open(root.path().join("store"), files.clone(), Default::default())
                .unwrap();
        (
            Self {
                root,
                path,
                files,
                store,
                page_size,
            },
            bytes,
        )
    }

    fn mutate(&self, sql: &str) -> Vec<u8> {
        let db = rusqlite::Connection::open(&self.path).unwrap();
        db.execute_batch(sql).unwrap();
        drop(db);
        std::fs::read(&self.path).unwrap()
    }

    fn seal(&self, bytes: &[u8], checksums: bool, buffer: &mut [u8]) -> SealedImageRoot {
        let manifest = manifest(bytes, self.page_size, 1, checksums);
        for (index, part) in bytes.chunks(EXTENT_BYTES as usize).enumerate() {
            self.store
                .put_extent(manifest.extent(index as u32).unwrap(), |offset, count| {
                    Ok(part[offset as usize..offset as usize + count].to_vec())
                })
                .unwrap();
        }
        self.store.seal_image_root(manifest, buffer).unwrap()
    }

    fn update<'a>(
        &self,
        base: &SealedImageRoot,
        encoded: &'a [u8],
        after: &[u8],
        txid: u64,
        checksums: bool,
    ) -> ImageRootUpdate<'a> {
        let expected = manifest(after, self.page_size, txid, checksums);
        let file = ltx::decode_file_bounded(encoded, Default::default()).unwrap();
        ImageRootUpdate {
            base: base.reference(),
            result: reference(&expected),
            binding: expected.binding(),
            geometry: expected.geometry(),
            segments: vec![LtxSegment {
                bytes: encoded,
                min_txid: file.header().min_txid,
                max_txid: file.header().max_txid,
                page_count: file.pages().len(),
                decoded_bytes: file.decoded_bytes(),
            }],
        }
    }

    fn query(&self, root: &SealedImageRoot, expected: u64) {
        let directory = self.root.path().join("cache");
        DirectFileSystem.create_dir_all(&directory).unwrap();
        let image = root
            .view()
            .image(
                directory,
                ScratchPool::new(ScratchLimits {
                    max_bytes: 2 * EXTENT_BYTES,
                    max_files: 2,
                })
                .unwrap(),
                CacheLimits {
                    max_cached_bytes: EXTENT_BYTES,
                    max_cached_extents: 1,
                },
            )
            .unwrap();
        let db = open_extent_image(
            image,
            ReadServices {
                now_unix_millis: Arc::new(|| 1_700_000_000_000),
                fill_random: Arc::new(|bytes| bytes.fill(1)),
            },
        )
        .unwrap();
        assert_eq!(
            db.connection()
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, u64>(0))
                .unwrap(),
            expected
        );
        assert_eq!(
            db.connection()
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }
}

#[test]
fn a_small_write_shares_unchanged_extents_and_preserves_sqlite_and_checksums() {
    for (page_size, checksums) in [(512, false), (4096, false), (65536, false), (4096, true)] {
        let (fixture, before) = Fixture::new(page_size, 12 * 1024 * 1024);
        let mut buffer = vec![0; EXTENT_BYTES as usize];
        let base = fixture.seal(&before, checksums, &mut buffer);
        let after = fixture.mutate("UPDATE counter SET value=8");
        let encoded = delta(&before, &after, page_size, 2, checksums);
        let update = fixture.update(&base, &encoded, &after, 2, checksums);
        let prepared = fixture
            .store
            .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
            .unwrap();
        assert_eq!(prepared.image.reference(), update.result);
        assert_eq!(prepared.work.extents_prepared, 1);
        assert_eq!(prepared.work.introduced_extent_bytes, EXTENT_BYTES);
        assert!(
            prepared.work.base_bytes_read <= 3 * EXTENT_BYTES,
            "small delta scanned unrelated extent bodies"
        );
        for index in 1..base.manifest().geometry().extent_count() {
            assert_eq!(
                base.manifest().extent(index),
                prepared.image.manifest().extent(index)
            );
        }
        fixture.query(&prepared.image, 8);
        fixture.query(&base, 7);
        assert!(fixture.files.metrics.max_read.load(Ordering::Acquire) <= 65536);
        assert!(fixture.files.metrics.max_write.load(Ordering::Acquire) <= 65536);
    }
}

#[test]
fn actual_sqlite_growth_and_vacuum_remove_unreachable_extents() {
    for checksums in [false, true] {
        let (fixture, before) = Fixture::new(4096, 12 * 1024 * 1024);
        let mut buffer = vec![0; EXTENT_BYTES as usize];
        let base = fixture.seal(&before, checksums, &mut buffer);
        let grown = fixture
            .mutate("INSERT INTO padding VALUES(zeroblob(5242880));UPDATE counter SET value=8");
        let encoded = delta(&before, &grown, 4096, 2, checksums);
        let update = fixture.update(&base, &encoded, &grown, 2, checksums);
        let grown_root = fixture
            .store
            .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
            .unwrap()
            .image;
        assert!(grown_root.manifest().geometry().length() > base.manifest().geometry().length());
        fixture.query(&grown_root, 8);
        let shrunk = fixture.mutate("DELETE FROM padding;UPDATE counter SET value=9;VACUUM");
        let encoded = delta(&grown, &shrunk, 4096, 3, checksums);
        let update = fixture.update(&grown_root, &encoded, &shrunk, 3, checksums);
        let prepared = fixture
            .store
            .prepare_image_root(
                Some(&grown_root),
                &update,
                &ApplyLimits::default(),
                &mut buffer,
            )
            .unwrap();
        assert_eq!(prepared.image.manifest().geometry().extent_count(), 1);
        if !checksums {
            assert!(
                prepared.work.base_bytes_read <= 3 * EXTENT_BYTES,
                "NoChecksum truncation read discarded extent bodies"
            );
        }
        fixture.query(&prepared.image, 9);
        fixture.query(&base, 7);
    }
}

#[test]
fn incomplete_corrupt_and_misdeclared_transitions_expose_no_image_root() {
    let (fixture, before) = Fixture::new(4096, 5 * 1024 * 1024);
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = fixture.seal(&before, false, &mut buffer);
    let after = fixture.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, 4096, 2, false);
    let initial = fixture.store.usage().unwrap();
    let mut update = fixture.update(&base, &encoded, &after, 2, false);
    update.segments[0].page_count = 0;
    assert!(fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .is_err());
    assert_eq!(fixture.store.usage().unwrap(), initial);
    let mut corrupt = encoded.clone();
    let at = corrupt.len() - 1;
    corrupt[at] ^= 1;
    let mut update = fixture.update(&base, &encoded, &after, 2, false);
    update.segments[0].bytes = &corrupt;
    assert!(fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .is_err());
    assert_eq!(fixture.store.usage().unwrap(), initial);
    let update = fixture.update(&base, &encoded, &after, 2, false);
    assert!(fixture
        .store
        .prepare_image_root(
            Some(&base),
            &update,
            &ApplyLimits::default(),
            &mut buffer[..65536]
        )
        .is_err());
    let mut limits = ApplyLimits::default();
    limits.decode.max_image_bytes = 4096;
    assert!(fixture
        .store
        .prepare_image_root(Some(&base), &update, &limits, &mut buffer)
        .is_err());
    assert_eq!(fixture.store.usage().unwrap(), initial);
    fixture
        .files
        .metrics
        .corrupt_output
        .store(true, Ordering::Release);
    assert!(fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .is_err());
    assert_eq!(fixture.store.usage().unwrap(), initial);
    fixture
        .files
        .metrics
        .fail_sync
        .store(true, Ordering::Release);
    assert!(fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .is_err());
    assert_eq!(fixture.store.usage().unwrap(), initial);
    let mut wrong = fixture.update(&base, &encoded, &after, 2, false);
    wrong.result = ExtentRootReference::new(
        ImageRoot::from_bytes([42; 32]),
        wrong.result.encoded_length(),
    )
    .unwrap();
    assert!(fixture
        .store
        .prepare_image_root(Some(&base), &wrong, &ApplyLimits::default(), &mut buffer)
        .is_err());
    assert_eq!(fixture.store.usage().unwrap().roots, initial.roots);
    assert!(
        fixture.store.usage().unwrap().bytes > initial.bytes,
        "failed preparation speculatively refunded a durable extent"
    );
    let prepared = fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    fixture.query(&prepared.image, 8);
}

#[test]
fn an_empty_base_bootstraps_only_after_the_exact_physical_snapshot() {
    let (fixture, bytes) = Fixture::new(4096, 1024 * 1024);
    let expected = manifest(&bytes, 4096, 1, false);
    let empty = ExtentManifest::new(
        ImageBinding {
            txid: 0,
            ..expected.binding()
        },
        Geometry::new(0, 0).unwrap(),
        &[],
    )
    .unwrap();
    let encoded = delta(&[], &bytes, 4096, 1, false);
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
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let prepared = fixture
        .store
        .prepare_image_root(None, &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    fixture.query(&prepared.image, 7);
}

#[test]
fn byte_closure_and_reopened_inventory_cannot_substitute_for_sqlite_image_validation() {
    let (fixture, bytes) = Fixture::new(4096, 8 * 1024 * 1024);
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = fixture.seal(&bytes, false, &mut buffer);
    let mut wrong_binding = base.manifest().binding();
    wrong_binding.checksum = binding(&bytes, 4096, 1, true).checksum ^ 1;
    let wrong = base
        .manifest()
        .with_updates(wrong_binding, base.manifest().geometry(), &[])
        .unwrap();
    assert!(fixture.store.seal_image_root(wrong, &mut buffer).is_err());
    let manifest = base.manifest().clone();
    drop(base);
    let Fixture {
        root,
        path,
        files,
        store,
        page_size,
    } = fixture;
    drop(store);
    let reopened =
        NodeExtentStore::open(root.path().join("store"), files.clone(), Default::default())
            .unwrap();
    let view = reopened.open_root(reference(&manifest)).unwrap();
    assert_eq!(view.reference(), reference(&manifest));
    assert!(
        matches!(
            reopened.seal_image_root(manifest.clone(), &mut buffer),
            Err(StoreError::Unverified)
        ),
        "inventory filenames became a complete SQLite image proof"
    );
    for index in 0..manifest.geometry().extent_count() {
        reopened
            .verify_extent(manifest.extent(index).unwrap())
            .unwrap();
    }
    let recovered = reopened.seal_image_root(manifest, &mut buffer).unwrap();
    let fixture = Fixture {
        root,
        path,
        files,
        store: reopened,
        page_size,
    };
    fixture.query(&recovered, 7);
}
