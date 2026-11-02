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
        Self::with_append(page_size, payload, false)
    }

    fn with_append(page_size: u32, payload: u64, append: bool) -> (Self, Vec<u8>) {
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
        let directory = root.path().join("store");
        let store = if append {
            NodeExtentStore::open_append(
                directory,
                files.clone(),
                Default::default(),
                2,
                8 * EXTENT_BYTES,
                true,
            )
        } else {
            NodeExtentStore::open(directory, files.clone(), Default::default())
        }
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

#[test]
fn image_publication_batches_extent_and_root_directory_barriers() {
    let (mut f, before) = Fixture::new(4096, 0);
    drop(f.store);
    f.store = NodeExtentStore::open_parallel(
        f.root.path().join("store"),
        f.files.clone(),
        Default::default(),
        2,
        true,
    )
    .unwrap();
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = f.seal(&before, true, &mut buffer);
    let after = f.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, f.page_size, 2, true);
    let update = f.update(&base, &encoded, &after, 2, true);
    f.files.metrics.dir_sync_calls.store(0, Ordering::Release);
    f.files.metrics.file_sync_calls.store(0, Ordering::Release);
    let prepared = f
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    f.store.validate_image_seal(&prepared.image).unwrap();
    assert_eq!(
        f.files.metrics.dir_sync_calls.load(Ordering::Acquire),
        1,
        "extent and root repeated a compatible directory barrier"
    );
    assert_eq!(
        f.files.metrics.file_sync_calls.load(Ordering::Acquire),
        2,
        "both dependency and root still require their file barriers"
    );
}

#[test]
fn batched_persistence_writes_the_complete_cohort_before_the_first_file_barrier() {
    let (mut f, before) = Fixture::new(4096, 0);
    drop(f.store);
    f.store = NodeExtentStore::open_parallel(
        f.root.path().join("store"),
        f.files.clone(),
        Default::default(),
        2,
        true,
    )
    .unwrap();
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = f.seal(&before, true, &mut buffer);
    let after = f.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, f.page_size, 2, true);
    let update = f.update(&base, &encoded, &after, 2, true);
    let root = manifest(&after, f.page_size, 2, true).root();
    let root_name: String = root.bytes().iter().map(|b| format!("{b:02x}")).collect();
    let root_path = f.root.path().join("store").join(format!("r-{root_name}"));
    *f.files.metrics.before_file_sync.lock().unwrap() = Some(Arc::new(move |is_root| {
        if !is_root && !std::fs::metadata(&root_path).is_ok_and(|m| m.len() > 0) {
            return Err(std::io::Error::other(
                "file barrier preceded the complete image cohort",
            ));
        }
        Ok(())
    }));
    let prepared = f
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    f.store.validate_image_seal(&prepared.image).unwrap();
    f.query(&prepared.image, 8);
}

#[test]
fn batched_files_are_reverified_after_their_own_syncs() {
    for corrupt_root in [false, true] {
        let (mut f, before) = Fixture::new(4096, 0);
        drop(f.store);
        f.store = NodeExtentStore::open_parallel(
            f.root.path().join("store"),
            f.files.clone(),
            Default::default(),
            2,
            true,
        )
        .unwrap();
        let mut buffer = vec![0; EXTENT_BYTES as usize];
        let base = f.seal(&before, true, &mut buffer);
        let after = f.mutate("UPDATE counter SET value=8");
        let encoded = delta(&before, &after, f.page_size, 2, true);
        let update = f.update(&base, &encoded, &after, 2, true);
        let expected = manifest(&after, f.page_size, 2, true);
        let name = if corrupt_root {
            let hex: String = expected
                .root()
                .bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            format!("r-{hex}")
        } else {
            let id = expected.extent(0).unwrap();
            let hex: String = id.bytes().iter().map(|b| format!("{b:02x}")).collect();
            format!("e-{:08x}-{hex}", id.length())
        };
        let destination = f.root.path().join("store").join(name);
        *f.files.metrics.before_file_sync.lock().unwrap() = Some(Arc::new(move |is_root| {
            if is_root == corrupt_root {
                use std::io::{Seek as _, Write as _};
                let mut file = std::fs::OpenOptions::new().write(true).open(&destination)?;
                file.seek(std::io::SeekFrom::Start(0))?;
                file.write_all(&[0xff])?;
            }
            Ok(())
        }));
        assert!(
            f.store
                .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer,)
                .is_err(),
            "pre-sync verification escaped as a durable image proof"
        );
        *f.files.metrics.before_file_sync.lock().unwrap() = None;
        f.query(&base, 7);
    }
}

#[test]
fn parallel_image_cohorts_share_namespace_barriers_and_fail_closed_together() {
    for fail in [false, true] {
        let (mut f, before) = Fixture::new(4096, 0);
        drop(f.store);
        f.store = NodeExtentStore::open_parallel(
            f.root.path().join("store"),
            f.files.clone(),
            Default::default(),
            2,
            true,
        )
        .unwrap();
        let base = f.seal(&before, true, &mut vec![0; EXTENT_BYTES as usize]);
        let after_a = f.mutate("UPDATE counter SET value=8");
        let after_b = f.mutate("UPDATE counter SET value=9");
        let encoded_a = delta(&before, &after_a, f.page_size, 2, true);
        let encoded_b = delta(&before, &after_b, f.page_size, 2, true);
        let update_a = f.update(&base, &encoded_a, &after_a, 2, true);
        let update_b = f.update(&base, &encoded_b, &after_b, 2, true);
        let roots_created = Arc::new(std::sync::Barrier::new(2));
        *f.files.metrics.before_file_sync.lock().unwrap() = Some(Arc::new(move |root| {
            if root {
                roots_created.wait();
            }
            Ok(())
        }));
        f.files.metrics.dir_sync_calls.store(0, Ordering::Release);
        f.files.metrics.file_sync_calls.store(0, Ordering::Release);
        f.files.metrics.fail_dir_sync.store(fail, Ordering::Release);
        let (a, b) = std::thread::scope(|scope| {
            let a = scope.spawn(|| {
                f.store.prepare_image_root(
                    Some(&base),
                    &update_a,
                    &ApplyLimits::default(),
                    &mut vec![0; EXTENT_BYTES as usize],
                )
            });
            let b = scope.spawn(|| {
                f.store.prepare_image_root(
                    Some(&base),
                    &update_b,
                    &ApplyLimits::default(),
                    &mut vec![0; EXTENT_BYTES as usize],
                )
            });
            (a.join().unwrap(), b.join().unwrap())
        });
        *f.files.metrics.before_file_sync.lock().unwrap() = None;
        assert_eq!(
            f.files.metrics.dir_sync_calls.load(Ordering::Acquire),
            1,
            "concurrent candidates did not share their covered namespace epoch"
        );
        assert_eq!(f.files.metrics.file_sync_calls.load(Ordering::Acquire), 4);
        if fail {
            assert!(
                a.is_err() && b.is_err(),
                "a failed shared barrier granted an image seal"
            );
            assert!(f.store.usage().unwrap().quarantined);
            assert_eq!(
                f.store.usage().unwrap().objects,
                3,
                "failed cohorts refunded uncertain files"
            );
            assert_eq!(f.store.usage().unwrap().roots, 3);
            assert!(matches!(f.store.reclaim(32), Err(StoreError::Quarantined)));
        } else {
            let a = a.unwrap();
            let b = b.unwrap();
            f.store.validate_image_seal(&a.image).unwrap();
            f.store.validate_image_seal(&b.image).unwrap();
            f.query(&a.image, 8);
            f.query(&b.image, 9);
        }
    }
}

#[test]
fn namespace_proof_never_substitutes_for_another_publishers_file_sync() {
    let (mut f, before) = Fixture::new(4096, 0);
    drop(f.store);
    f.store = NodeExtentStore::open_parallel(
        f.root.path().join("store"),
        f.files.clone(),
        Default::default(),
        2,
        true,
    )
    .unwrap();
    let base = f.seal(&before, true, &mut vec![0; EXTENT_BYTES as usize]);
    let after_a = f.mutate("UPDATE counter SET value=8");
    let after_b = f.mutate("UPDATE counter SET value=9");
    let encoded_a = delta(&before, &after_a, f.page_size, 2, true);
    let encoded_b = delta(&before, &after_b, f.page_size, 2, true);
    let update_a = f.update(&base, &encoded_a, &after_a, 2, true);
    let update_b = f.update(&base, &encoded_b, &after_b, 2, true);
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let release = std::sync::Mutex::new(release_rx);
    let first = std::sync::atomic::AtomicBool::new(true);
    *f.files.metrics.before_file_sync.lock().unwrap() = Some(Arc::new(move |root| {
        if !root && first.swap(false, Ordering::AcqRel) {
            entered_tx.send(()).map_err(std::io::Error::other)?;
            release
                .lock()
                .unwrap()
                .recv()
                .map_err(std::io::Error::other)?;
            return Err(std::io::Error::other("injected file barrier failure"));
        }
        Ok(())
    }));
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let (a, b, early) = std::thread::scope(|scope| {
        let a = scope.spawn(|| {
            done_tx
                .send(f.store.prepare_image_root(
                    Some(&base),
                    &update_a,
                    &ApplyLimits::default(),
                    &mut vec![0; EXTENT_BYTES as usize],
                ))
                .unwrap()
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // This successful peer syncs the shared directory while A's file has
        // not passed its own data barrier. A must still not return a seal.
        let b = f.store.prepare_image_root(
            Some(&base),
            &update_b,
            &ApplyLimits::default(),
            &mut vec![0; EXTENT_BYTES as usize],
        );
        let early = done_rx.recv_timeout(std::time::Duration::from_millis(50));
        // B's complete proof is valid while A still has no file proof.
        f.store
            .validate_image_seal(&b.as_ref().unwrap().image)
            .unwrap();
        f.query(&b.as_ref().unwrap().image, 9);
        release_tx.send(()).unwrap();
        a.join().unwrap();
        (
            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            b,
            early,
        )
    });
    *f.files.metrics.before_file_sync.lock().unwrap() = None;
    assert!(
        early.is_err(),
        "directory coverage exposed a file before its sync"
    );
    assert!(a.is_err());
    let b = b.unwrap();
    assert!(f.store.usage().unwrap().quarantined);
    assert!(matches!(
        f.store.validate_image_seal(&b.image),
        Err(StoreError::Quarantined)
    ));
    f.query(&b.image, 9);
}

#[test]
#[ignore = "isolated deadlock regression child, invoked by its parent"]
fn reverse_dependency_order_child() {
    let (mut f, before) = Fixture::new(4096, 3 * EXTENT_BYTES);
    drop(f.store);
    f.store = NodeExtentStore::open_parallel(
        f.root.path().join("store"),
        f.files.clone(),
        Default::default(),
        2,
        true,
    )
    .unwrap();
    let base = f.seal(&before, false, &mut vec![0; EXTENT_BYTES as usize]);
    // Byte-closure mechanics only: the first SQLite/header extent stays real.
    // The two non-header extents deliberately reverse content-addressed IDs.
    let mut a = before.clone();
    let mut b = before.clone();
    let size = EXTENT_BYTES as usize;
    a[size..2 * size].fill(31);
    a[2 * size..3 * size].fill(32);
    b[size..2 * size].fill(32);
    b[2 * size..3 * size].fill(31);
    let encoded_a = delta(&before, &a, f.page_size, 2, false);
    let encoded_b = delta(&before, &b, f.page_size, 2, false);
    let update_a = f.update(&base, &encoded_a, &a, 2, false);
    let update_b = f.update(&base, &encoded_b, &b, 2, false);
    let rendezvous = Arc::new(std::sync::Barrier::new(2));
    let calls = std::sync::atomic::AtomicUsize::new(0);
    *f.files.metrics.before_file_sync.lock().unwrap() = Some(Arc::new(move |root| {
        if !root && calls.fetch_add(1, Ordering::AcqRel) < 2 {
            rendezvous.wait();
        }
        Ok(())
    }));
    let (a, b) = std::thread::scope(|scope| {
        let a = scope.spawn(|| {
            f.store.prepare_image_root(
                Some(&base),
                &update_a,
                &ApplyLimits::default(),
                &mut vec![0; EXTENT_BYTES as usize],
            )
        });
        let b = scope.spawn(|| {
            f.store.prepare_image_root(
                Some(&base),
                &update_b,
                &ApplyLimits::default(),
                &mut vec![0; EXTENT_BYTES as usize],
            )
        });
        (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
    });
    *f.files.metrics.before_file_sync.lock().unwrap() = None;
    f.store.validate_image_seal(&a.image).unwrap();
    f.store.validate_image_seal(&b.image).unwrap();
    assert_eq!(a.image.manifest().extent(1), b.image.manifest().extent(2));
    assert_eq!(a.image.manifest().extent(2), b.image.manifest().extent(1));
}

#[test]
fn competing_image_cohorts_do_not_deadlock_on_reverse_dependency_order() {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "reverse_dependency_order_child",
            "--ignored",
            "--exact",
            "--test-threads=1",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "reverse-order publication child failed");
            return;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("reverse-order publications deadlocked");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
#[ignore = "abrupt batched-image child, invoked by its parent"]
fn batched_image_crash_child() {
    let path = std::path::PathBuf::from(std::env::var_os("CELLAR_BATCH_IMAGE_CRASH_PATH").unwrap());
    let before = std::fs::read(path.join("before.bin")).unwrap();
    let after = std::fs::read(path.join("after.bin")).unwrap();
    let encoded = std::fs::read(path.join("delta.ltx")).unwrap();
    let files = Arc::new(bounded_files::BoundedFiles {
        directory: path.clone(),
        metrics: Arc::default(),
    });
    let store = NodeExtentStore::open_parallel(
        path.join("store"),
        files.clone(),
        Default::default(),
        2,
        true,
    )
    .unwrap();
    let base_manifest = manifest(&before, 4096, 1, true);
    for id in (0..base_manifest.geometry().extent_count()).map(|i| base_manifest.extent(i).unwrap())
    {
        store.verify_extent(id).unwrap();
    }
    let base = store
        .seal_image_root(base_manifest, &mut vec![0; EXTENT_BYTES as usize])
        .unwrap();
    let expected = manifest(&after, 4096, 2, true);
    let file = ltx::decode_file_bounded(&encoded, Default::default()).unwrap();
    let update = ImageRootUpdate {
        base: base.reference(),
        result: reference(&expected),
        binding: expected.binding(),
        geometry: expected.geometry(),
        segments: vec![LtxSegment {
            bytes: &encoded,
            min_txid: TXID(2),
            max_txid: TXID(2),
            page_count: file.pages().len(),
            decoded_bytes: file.decoded_bytes(),
        }],
    };
    // Exit after the new root's file barrier, before its batch returns a seal.
    files
        .metrics
        .exit_after_root_sync
        .store(true, Ordering::Release);
    let _ = store.prepare_image_root(
        Some(&base),
        &update,
        &ApplyLimits::default(),
        &mut vec![0; EXTENT_BYTES as usize],
    );
    panic!("batched crash hook did not execute");
}

#[test]
fn interrupted_batched_image_keeps_the_selected_old_root_and_reclaims_only_orphans() {
    let (mut f, before) = Fixture::new(4096, 0);
    let base = f.seal(&before, true, &mut vec![0; EXTENT_BYTES as usize]);
    let selected = base.reference();
    let after = f.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, 4096, 2, true);
    std::fs::write(f.root.path().join("before.bin"), before).unwrap();
    std::fs::write(f.root.path().join("after.bin"), after).unwrap();
    std::fs::write(f.root.path().join("delta.ltx"), encoded).unwrap();
    drop(base);
    drop(f.store);
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "batched_image_crash_child",
            "--ignored",
            "--exact",
            "--test-threads=1",
        ])
        .env("CELLAR_BATCH_IMAGE_CRASH_PATH", f.root.path())
        .output()
        .unwrap();
    assert_eq!(child.status.code(), Some(77));
    f.store = NodeExtentStore::open_parallel(
        f.root.path().join("store"),
        f.files.clone(),
        Default::default(),
        2,
        true,
    )
    .unwrap();
    assert_eq!(f.store.usage().unwrap().roots, 2);
    let view = f.store.open_root(selected).unwrap();
    let restored = f
        .store
        .seal_image_root(view.manifest().clone(), &mut vec![0; EXTENT_BYTES as usize])
        .unwrap();
    f.query(&restored, 7);
    f.store.retain_only(&[selected]).unwrap();
    assert_eq!(f.store.reclaim(32).unwrap(), 2);
    f.query(&restored, 7);
    assert_eq!(f.store.usage().unwrap().roots, 1);
}

#[test]
fn append_publication_persists_changed_data_and_root_with_one_barrier() {
    let (fixture, before) = Fixture::with_append(4096, 0, true);
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = fixture.seal(&before, false, &mut buffer);
    let after = fixture.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, 4096, 2, false);
    let update = fixture.update(&base, &encoded, &after, 2, false);
    fixture
        .files
        .metrics
        .file_sync_calls
        .store(0, Ordering::Release);
    fixture
        .files
        .metrics
        .dir_sync_calls
        .store(0, Ordering::Release);
    let prepared = fixture
        .store
        .prepare_image_root(Some(&base), &update, &ApplyLimits::default(), &mut buffer)
        .unwrap();
    fixture.store.validate_image_seal(&prepared.image).unwrap();
    assert_eq!(
        fixture
            .files
            .metrics
            .file_sync_calls
            .load(Ordering::Acquire),
        1,
        "changed data and root metadata must share one persistence barrier"
    );
    assert_eq!(
        fixture.files.metrics.dir_sync_calls.load(Ordering::Acquire),
        0,
        "appending to an existing segment must not create names"
    );
    fixture.query(&prepared.image, 8);
}

#[test]
fn append_abrupt_exit_before_and_after_shared_sync_keeps_exact_old_selection() {
    for cut in ["before", "after"] {
        let evidence = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "append_shared_sync_exit_child"])
            .env("CELLAR_APPEND_CRASH_EVIDENCE", evidence.path())
            .env("CELLAR_APPEND_CRASH_CUT", cut)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(if cut == "before" { 83 } else { 84 }));
        let path = std::fs::read_to_string(evidence.path().join("path")).unwrap();
        let before = std::fs::read(evidence.path().join("before.sqlite")).unwrap();
        let root = manifest(&before, 4096, 1, false);
        let store = NodeExtentStore::open_append(
            std::path::Path::new(&path).join("store"),
            Arc::new(DirectFileSystem),
            Default::default(),
            2,
            8 * EXTENT_BYTES,
            true,
        )
        .unwrap();
        let view = store.open_root(reference(&root)).unwrap();
        assert_eq!(view.read_extent(root.extent(0).unwrap()).unwrap(), before);
        store.retain_only(&[reference(&root)]).unwrap();
        // The unsent candidate is not selected by append position or filename.
        assert_eq!(view.reference(), reference(&root));
        drop(view);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
#[ignore = "abrupt-exit helper launched by the parent test"]
fn append_shared_sync_exit_child() {
    let evidence =
        std::path::PathBuf::from(std::env::var_os("CELLAR_APPEND_CRASH_EVIDENCE").unwrap());
    let cut = std::env::var("CELLAR_APPEND_CRASH_CUT").unwrap();
    let (fixture, before) = Fixture::with_append(4096, 0, true);
    std::fs::write(evidence.join("path"), fixture.root.path().to_str().unwrap()).unwrap();
    std::fs::write(evidence.join("before.sqlite"), &before).unwrap();
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let base = fixture.seal(&before, false, &mut buffer);
    let after = fixture.mutate("UPDATE counter SET value=8");
    let encoded = delta(&before, &after, 4096, 2, false);
    let update = fixture.update(&base, &encoded, &after, 2, false);
    if cut == "before" {
        *fixture.files.metrics.before_file_sync.lock().unwrap() =
            Some(Arc::new(|_| std::process::exit(83)));
    } else {
        *fixture.files.metrics.after_file_sync.lock().unwrap() =
            Some(Arc::new(|_| std::process::exit(84)));
    }
    let _ = fixture.store.prepare_image_root(
        Some(&base),
        &update,
        &ApplyLimits::default(),
        &mut buffer,
    );
    panic!("shared sync cut was not reached");
}
