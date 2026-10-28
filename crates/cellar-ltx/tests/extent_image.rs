// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual SQLite reads over bounded verified extent residency. This exercises
//! the data/VFS boundary, not native Raft selection or durable root inventory.
#![allow(clippy::disallowed_methods)]

#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

#[path = "support/extent_fetch_coalescing.rs"]
mod fetch_coalescing;

use bounded_files::BoundedFiles;
use cellar_ltx::{
    generation::ReadServices,
    internal::{
        open_extent_image, CacheLimits, CacheUsage, ExtentDownload, ExtentId, ExtentImage,
        ExtentManifest, ExtentSource, Geometry, ImageBinding, EXTENT_BYTES,
    },
    stream_snapshot::{Error, ScratchLimits, ScratchPool, ScratchUsage},
    DirectFileSystem, FileSystem,
};
use std::{
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

struct Source {
    path: PathBuf,
    extents: Vec<(ExtentId, u64)>,
    calls: AtomicUsize,
    bytes: AtomicU64,
    mode: AtomicUsize,
    probe: Mutex<Option<std::sync::Weak<ExtentImage>>>,
}

impl ExtentSource for Source {
    fn read_into(&self, id: ExtentId, download: &mut ExtentDownload) -> Result<(), Error> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if let Some(image) = self
            .probe
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
        {
            // A source may inspect its owner. Source I/O must not hold the
            // cache mutex, including while an old entry is being evicted.
            image.cache_usage()?;
        }
        let mode = self.mode.load(Ordering::Acquire);
        if mode == 1 {
            return Err(io::ErrorKind::NotFound.into());
        }
        assert_ne!(mode, 5, "injected source panic");
        let position = self
            .extents
            .iter()
            .find(|(candidate, _)| *candidate == id)
            .map(|(_, offset)| *offset)
            .ok_or(io::ErrorKind::NotFound)?;
        let mut source = DirectFileSystem.open(&self.path)?;
        let mut offset = 0;
        while offset < u64::from(id.length()) {
            let length = (u64::from(id.length()) - offset).min(65536) as usize;
            let mut bytes = source.read_exact_at(position + offset, length)?;
            self.bytes.fetch_add(length as u64, Ordering::AcqRel);
            if mode == 2 && offset == 0 {
                bytes[0] ^= 1;
            }
            download.append(offset, &bytes)?;
            offset += length as u64;
            if mode == 3 {
                return Ok(());
            }
            if mode == 4 {
                return Err(io::Error::other("source failed after partial append").into());
            }
        }
        Ok(())
    }
}

struct Fixture {
    root: tempfile::TempDir,
    files: Arc<BoundedFiles>,
    source: Arc<Source>,
    manifest: ExtentManifest,
    scratch: ScratchPool,
}

impl Fixture {
    fn new(rows: u64, page_size: u32) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source.sqlite");
        let db = rusqlite::Connection::open(&path).unwrap();
        db.pragma_update(None, "page_size", page_size).unwrap();
        db.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
            CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(7);
            CREATE TABLE payload(id INTEGER PRIMARY KEY,value BLOB);",
        )
        .unwrap();
        for row in 1..=rows {
            db.execute(
                "INSERT INTO payload VALUES(?1,?2)",
                rusqlite::params![row, vec![row as u8; 1024 * 1024]],
            )
            .unwrap();
        }
        drop(db);
        let mut file = DirectFileSystem.open(&path).unwrap();
        let length = file.file_len().unwrap();
        let geometry = Geometry::new(page_size, length).unwrap();
        let mut extents = Vec::new();
        for index in 0..geometry.extent_count() {
            let offset = u64::from(index) * EXTENT_BYTES;
            let bytes = file
                .read_exact_at(offset, geometry.extent_length(index).unwrap() as usize)
                .unwrap();
            extents.push((ExtentId::of(&bytes).unwrap(), offset));
        }
        drop(file);
        let manifest = ExtentManifest::new(
            ImageBinding {
                history: [1; 32],
                schema: [2; 32],
                epoch: 1,
                txid: 1,
                checksum: 0,
            },
            geometry,
            &extents.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        )
        .unwrap();
        let source = Arc::new(Source {
            path,
            extents,
            calls: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            mode: AtomicUsize::new(0),
            probe: Mutex::default(),
        });
        let files = Arc::new(BoundedFiles {
            directory: root.path().into(),
            metrics: Arc::default(),
        });
        let scratch = ScratchPool::new(ScratchLimits {
            max_bytes: 3 * EXTENT_BYTES,
            max_files: 3,
        })
        .unwrap();
        Self {
            root,
            files,
            source,
            manifest,
            scratch,
        }
    }

    fn image(&self) -> ExtentImage {
        self.image_with(self.source.clone(), self.scratch.clone())
    }

    fn image_with(&self, source: Arc<dyn ExtentSource>, scratch: ScratchPool) -> ExtentImage {
        let encoded = self.manifest.encode().unwrap();
        let manifest = ExtentManifest::decode(&encoded, self.manifest.root()).unwrap();
        ExtentImage::new(
            manifest,
            source,
            self.files.clone(),
            self.root.path().into(),
            scratch,
            CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        )
        .unwrap()
    }

    fn idle(&self) {
        assert_eq!(self.files.metrics.active_handles.load(Ordering::Acquire), 0);
        assert_eq!(self.scratch.usage().unwrap(), ScratchUsage::default());
    }
}

fn services() -> ReadServices {
    ReadServices {
        now_unix_millis: Arc::new(|| 1_700_000_000_000),
        fill_random: Arc::new(|bytes| bytes.fill(1)),
    }
}

#[test]
fn actual_sqlite_cold_hot_set_is_independent_of_total_image_size() {
    for rows in [8, 40] {
        let f = Fixture::new(rows, 4096);
        let image = f.image();
        assert_eq!(f.source.calls.load(Ordering::Acquire), 0);
        let db = open_extent_image(image.clone(), services()).unwrap();
        assert_eq!(
            db.connection()
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, u64>(0))
                .unwrap(),
            7
        );
        assert_eq!(f.source.calls.load(Ordering::Acquire), 1);
        assert_eq!(f.source.bytes.load(Ordering::Acquire), EXTENT_BYTES);
        assert!(image.manifest().geometry().length() > 2 * EXTENT_BYTES);
        assert_eq!(
            image.cache_usage().unwrap(),
            CacheUsage {
                cached_bytes: EXTENT_BYTES,
                cached_extents: 1
            }
        );
        assert!(db
            .connection()
            .execute_batch("CREATE TABLE forbidden(value)")
            .is_err());
        let (length, prefix): (u64, String) = db
            .connection()
            .query_row(
                "SELECT length(value),hex(substr(value,1,16)) FROM payload WHERE id=?1",
                [rows],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(length, 1024 * 1024);
        assert_eq!(prefix, format!("{:02X}", rows as u8).repeat(16));
        assert!(f.source.calls.load(Ordering::Acquire) > 1);
        assert!(image.cache_usage().unwrap().cached_bytes <= EXTENT_BYTES);
        assert_eq!(image.manifest().root(), f.manifest.root());
        drop(db);
        image.evict_all().unwrap();
        assert_eq!(image.cache_usage().unwrap(), CacheUsage::default());
        f.idle();
        let before = f.source.calls.load(Ordering::Acquire);
        let db = open_extent_image(image.clone(), services()).unwrap();
        assert_eq!(
            db.connection()
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, u64>(0))
                .unwrap(),
            7
        );
        assert!(f.source.calls.load(Ordering::Acquire) > before);
        drop(db);
        drop(image);
        f.idle();
        assert!(f.files.metrics.max_read.load(Ordering::Acquire) <= 65536);
        assert!(f.files.metrics.max_write.load(Ordering::Acquire) <= 65536);
    }
}

#[test]
fn cross_extent_reads_match_independent_source_and_evict_before_admission() {
    for page_size in [512, 4096, 65536] {
        let f = Fixture::new(9, page_size);
        let image = Arc::new(f.image());
        *f.source.probe.lock().unwrap() = Some(Arc::downgrade(&image));
        let mut source = DirectFileSystem.open(&f.source.path).unwrap();
        for offset in [
            0,
            EXTENT_BYTES - 100,
            2 * EXTENT_BYTES - 100,
            source.file_len().unwrap() - 65536,
        ] {
            let length = if offset == 0 { 100 } else { 65536 };
            assert_eq!(
                image.read_exact_at(offset, length).unwrap(),
                source.read_exact_at(offset, length).unwrap()
            );
            assert!(image.cache_usage().unwrap().cached_bytes <= EXTENT_BYTES);
            assert_eq!(f.files.metrics.active_handles.load(Ordering::Acquire), 1);
        }
        // Broad SQLite verification must also work through an evicting cache.
        let db = open_extent_image((*image).clone(), services()).unwrap();
        assert_eq!(
            db.connection()
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(db);
        for (offset, count) in [
            (0, 0),
            (0, 65537),
            (u64::MAX, 10),
            (source.file_len().unwrap(), 1),
        ] {
            assert!(matches!(
                image.read_exact_at(offset, count),
                Err(Error::Limit)
            ));
        }
        drop(image);
        f.idle();
    }
}

#[test]
fn source_and_destination_failures_do_not_cache_a_partial_extent() {
    let f = Fixture::new(8, 4096);
    let image = f.image();
    for mode in [1, 2, 3, 4] {
        f.source.mode.store(mode, Ordering::Release);
        assert!(image.read_exact_at(0, 100).is_err(), "source mode {mode}");
        assert_eq!(image.cache_usage().unwrap(), CacheUsage::default());
        f.idle();
    }
    f.source.mode.store(0, Ordering::Release);
    f.files
        .metrics
        .corrupt_output
        .store(true, Ordering::Release);
    assert!(matches!(image.read_exact_at(0, 100), Err(Error::Integrity)));
    f.idle();
    assert_eq!(
        image.read_exact_at(0, 100).unwrap(),
        DirectFileSystem
            .open(&f.source.path)
            .unwrap()
            .read_exact_at(0, 100)
            .unwrap()
    );
    assert_eq!(image.manifest().root(), f.manifest.root());
    drop(image);
    f.idle();
}

#[test]
fn shared_physical_budget_refuses_before_source_io_and_is_retryable_after_close() {
    let f = Fixture::new(8, 4096);
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: EXTENT_BYTES,
        max_files: 1,
    })
    .unwrap();
    let image = ExtentImage::new(
        f.manifest.clone(),
        f.source.clone(),
        f.files.clone(),
        f.root.path().into(),
        scratch.clone(),
        CacheLimits {
            max_cached_bytes: EXTENT_BYTES,
            max_cached_extents: 1,
        },
    )
    .unwrap();
    let held = ExtentDownload::new(
        f.files.as_ref(),
        None,
        &scratch,
        f.manifest.extent(0).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        image.read_exact_at(0, 100),
        Err(Error::ScratchFull)
    ));
    assert_eq!(f.source.calls.load(Ordering::Acquire), 0);
    drop(held);
    image.read_exact_at(0, 100).unwrap();
    assert_eq!(
        scratch.usage().unwrap(),
        ScratchUsage {
            reserved_bytes: EXTENT_BYTES,
            reserved_files: 1
        }
    );
    // Evicting to another full extent must close the old file before trying to
    // spend the same single-file reservation, rather than refunding a live pin.
    image.read_exact_at(EXTENT_BYTES, 100).unwrap();
    drop(image);
    assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
    f.idle();
}

#[test]
fn sqlite_file_owner_retains_cache_and_source_after_external_image_drop() {
    let f = Fixture::new(8, 4096);
    let image = f.image();
    let db = open_extent_image(image.clone(), services()).unwrap();
    drop(image);
    assert_eq!(f.scratch.usage().unwrap().reserved_bytes, EXTENT_BYTES);
    assert_eq!(
        db.connection()
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        7
    );
    drop(db);
    f.idle();
}

#[test]
fn source_panic_is_contained_by_sqlite_without_retaining_a_partial_file() {
    let f = Fixture::new(8, 4096);
    let image = f.image();
    f.source.mode.store(5, Ordering::Release);
    assert!(open_extent_image(image.clone(), services()).is_err());
    assert_eq!(image.cache_usage().unwrap(), CacheUsage::default());
    f.idle();
    f.source.mode.store(0, Ordering::Release);
    let db = open_extent_image(image.clone(), services()).unwrap();
    drop(db);
    drop(image);
    f.idle();
}

#[test]
fn mismatched_sqlite_geometry_and_invalid_cache_limits_refuse() {
    let f = Fixture::new(8, 4096);
    for limits in [
        CacheLimits {
            max_cached_bytes: 0,
            max_cached_extents: 1,
        },
        CacheLimits {
            max_cached_bytes: EXTENT_BYTES - 1,
            max_cached_extents: 1,
        },
        CacheLimits {
            max_cached_bytes: EXTENT_BYTES,
            max_cached_extents: 0,
        },
        CacheLimits {
            max_cached_bytes: u64::MAX,
            max_cached_extents: usize::MAX,
        },
    ] {
        assert!(matches!(
            ExtentImage::new(
                f.manifest.clone(),
                f.source.clone(),
                f.files.clone(),
                f.root.path().into(),
                f.scratch.clone(),
                limits
            ),
            Err(Error::Limit)
        ));
    }
    let manifest = ExtentManifest::new(
        f.manifest.binding(),
        Geometry::new(512, f.manifest.geometry().length()).unwrap(),
        &f.source
            .extents
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let image = ExtentImage::new(
        manifest,
        f.source.clone(),
        f.files.clone(),
        f.root.path().into(),
        f.scratch.clone(),
        CacheLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        image.read_exact_at(0, 100),
        Err(Error::Invalid("extent SQLite geometry"))
    ));
    assert_eq!(image.cache_usage().unwrap(), CacheUsage::default());
    f.idle();
}

struct BytesSource(Vec<u8>);

impl ExtentSource for BytesSource {
    fn read_into(&self, _: ExtentId, download: &mut ExtentDownload) -> Result<(), Error> {
        for (index, bytes) in self.0.chunks(65536).enumerate() {
            download.append(index as u64 * 65536, bytes)?;
        }
        Ok(())
    }
}

#[test]
fn fetching_an_extent_that_contains_the_lock_page_checks_canonical_bytes() {
    let root = tempfile::tempdir().unwrap();
    let files = Arc::new(BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    });
    let geometry = Geometry::new(4096, (1 << 30) + 65536).unwrap();
    let ordinary = ExtentId::of(&vec![0; EXTENT_BYTES as usize]).unwrap();
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: EXTENT_BYTES,
        max_files: 1,
    })
    .unwrap();
    for noncanonical in [false, true] {
        let mut bytes = vec![0; 65536];
        if noncanonical {
            bytes[100] = 1;
        }
        let mut ids = vec![ordinary; geometry.extent_count() as usize];
        *ids.last_mut().unwrap() = ExtentId::of(&bytes).unwrap();
        let manifest = ExtentManifest::new(
            ImageBinding {
                history: [1; 32],
                schema: [2; 32],
                epoch: 1,
                txid: 1,
                checksum: 0,
            },
            geometry,
            &ids,
        )
        .unwrap();
        let image = ExtentImage::new(
            manifest,
            Arc::new(BytesSource(bytes)),
            files.clone(),
            root.path().into(),
            scratch.clone(),
            CacheLimits::default(),
        )
        .unwrap();
        // Read outside the lock page in the same fetched extent. These are
        // synthetic extent-geometry cases, not a >1 GiB physical SQLite drill.
        let read = image.read_exact_at((1 << 30) + 8192, 100);
        if noncanonical {
            assert!(matches!(
                read,
                Err(Error::Invalid("noncanonical extent lock page"))
            ));
            assert_eq!(image.cache_usage().unwrap(), CacheUsage::default());
        } else {
            assert_eq!(read.unwrap(), vec![0; 100]);
        }
        drop(image);
        assert_eq!(scratch.usage().unwrap(), ScratchUsage::default());
        assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
    }
}

#[test]
fn repeated_content_is_one_cached_file_without_changing_manifest_positions() {
    let f = Fixture::new(8, 4096);
    let mut ids = f
        .source
        .extents
        .iter()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    ids[1] = ids[0];
    let manifest = ExtentManifest::new(f.manifest.binding(), f.manifest.geometry(), &ids).unwrap();
    let image = ExtentImage::new(
        manifest,
        f.source.clone(),
        f.files.clone(),
        f.root.path().into(),
        f.scratch.clone(),
        CacheLimits {
            max_cached_bytes: EXTENT_BYTES,
            max_cached_extents: 1,
        },
    )
    .unwrap();
    let first = image.read_exact_at(0, 100).unwrap();
    assert_eq!(image.read_exact_at(EXTENT_BYTES, 100).unwrap(), first);
    assert_eq!(f.source.calls.load(Ordering::Acquire), 1);
    assert_eq!(f.scratch.usage().unwrap().reserved_files, 1);
    assert_ne!(image.manifest().root(), f.manifest.root());
    drop(image);
    f.idle();
}

struct GateSource {
    source: Arc<Source>,
    started: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl ExtentSource for GateSource {
    fn read_into(&self, id: ExtentId, download: &mut ExtentDownload) -> Result<(), Error> {
        self.started
            .send(())
            .map_err(|_| io::Error::other("lost gate observer"))?;
        self.release
            .lock()
            .unwrap()
            .recv()
            .map_err(|_| io::Error::other("source cancelled"))?;
        self.source.read_into(id, download)
    }
}

#[test]
fn held_source_retains_physical_admission_until_joined_even_after_cache_eviction() {
    for cancelled in [false, true] {
        let f = Fixture::new(8, 4096);
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let image = ExtentImage::new(
            f.manifest.clone(),
            Arc::new(GateSource {
                source: f.source.clone(),
                started: started_tx,
                release: Mutex::new(release_rx),
            }),
            f.files.clone(),
            f.root.path().into(),
            f.scratch.clone(),
            CacheLimits::default(),
        )
        .unwrap();
        let owner = image.clone();
        let task = std::thread::spawn(move || owner.read_exact_at(0, 100));
        let started = started_rx.recv_timeout(std::time::Duration::from_secs(5));
        if started.is_err() {
            drop(release_tx);
            let _ = task.join();
            panic!("source did not reach its gate: {started:?}");
        }
        let charged = f.scratch.usage().unwrap();
        image.evict_all().unwrap();
        let cache = image.cache_usage().unwrap();
        drop(image);
        let after_drop = f.scratch.usage().unwrap();
        if !cancelled {
            release_tx.send(()).unwrap();
        }
        drop(release_tx);
        let result = task.join().unwrap();
        assert_eq!(
            charged,
            ScratchUsage {
                reserved_bytes: EXTENT_BYTES,
                reserved_files: 1
            }
        );
        assert_eq!(after_drop, charged);
        assert_eq!(cache, CacheUsage::default());
        assert_eq!(result.is_err(), cancelled);
        f.idle();
    }
}

#[test]
fn sqlite_refuses_corruption_from_an_already_verified_cached_extent() {
    let f = Fixture::new(8, 4096);
    let image = f.image();
    image.read_exact_at(0, 100).unwrap();
    assert_eq!(f.source.calls.load(Ordering::Acquire), 1);
    // Let the cache's SQLite header check pass, then corrupt the block returned
    // to SQLite's actual xRead. This must not require another source download.
    f.files
        .metrics
        .corrupt_read_countdown
        .store(2, Ordering::Release);
    assert!(open_extent_image(image.clone(), services()).is_err());
    assert_eq!(f.source.calls.load(Ordering::Acquire), 1);
    assert_eq!(image.manifest().root(), f.manifest.root());
    assert_eq!(f.scratch.usage().unwrap().reserved_bytes, EXTENT_BYTES);
    let db = open_extent_image(image.clone(), services()).unwrap();
    assert_eq!(
        db.connection()
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, u64>(0))
            .unwrap(),
        7
    );
    drop(db);
    drop(image);
    f.idle();
}
