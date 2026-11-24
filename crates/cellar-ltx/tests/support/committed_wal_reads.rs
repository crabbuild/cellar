// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read admission at the actual committed-image/extent observation boundary.

use cellar_ltx::{
    internal::{
        db::observe_extents, ExtentId, ExtentManifest, Geometry, ImageBinding, EXTENT_BYTES,
    },
    Db, DirectFileSystem, FileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata, LtxHost,
};
use rusqlite::{Connection, MAIN_DB};
use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
};

const WINDOW: usize = 65536;

#[derive(Default)]
struct Reads {
    calls: u64,
    max_length: usize,
    short_at: Option<u64>,
    injected: bool,
}

struct CountedFile {
    file: HostFile,
    reads: Arc<Mutex<Reads>>,
}

impl HostFileIo for CountedFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let shorten = {
            let mut reads = self.reads.lock().unwrap();
            reads.calls += 1;
            reads.max_length = reads.max_length.max(length);
            let shorten = !reads.injected
                && reads.short_at.is_some_and(|at| {
                    offset <= at && offset.saturating_add(length as u64) > at
                });
            reads.injected |= shorten;
            shorten
        };
        let mut bytes = self.file.read_exact_at(offset, length)?;
        if shorten {
            bytes.pop();
        }
        Ok(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.file.file_len()
    }
}

struct CountedFs(Arc<Mutex<Reads>>);

impl FileSystem for CountedFs {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        let file = DirectFileSystem.open(path)?;
        if path.as_os_str().as_encoded_bytes().ends_with(b"-wal") {
            Ok(HostFile::from_io(CountedFile {
                file,
                reads: self.0.clone(),
            }))
        } else {
            Ok(file)
        }
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }
}

fn fixture(path: &Path, page_size: u32, reads: Arc<Mutex<Reads>>) -> (Db, Connection) {
    let setup = Connection::open(path).unwrap();
    setup.pragma_update(None, "page_size", page_size).unwrap();
    setup.execute_batch("VACUUM;").unwrap();
    drop(setup);
    let host = LtxHost::default().with_filesystem(Arc::new(CountedFs(reads)));
    let db = Db::open_disposable_with_host(path, host, None).unwrap();
    let writer = Connection::open(path).unwrap();
    writer
        .execute_batch(
            "PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=OFF;
             CREATE TABLE payload(value BLOB);
             INSERT INTO payload VALUES(zeroblob(262144));
             UPDATE payload SET value=randomblob(262144);",
        )
        .unwrap();
    (db, writer)
}

fn binding() -> ImageBinding {
    ImageBinding {
        history: [1; 32],
        schema: [2; 32],
        epoch: 3,
        txid: 1,
        checksum: 0,
    }
}

#[test]
fn extent_inspection_amortizes_wal_reads_without_skipping_prefix_validation() {
    for page_size in [512, 4096, 65536] {
        let root = tempfile::tempdir().unwrap();
        let reads = Arc::new(Mutex::new(Reads::default()));
        let (db, writer) = fixture(&root.path().join("main.sqlite"), page_size, reads.clone());
        let bytes = writer.serialize(MAIN_DB).unwrap();
        let expected = ExtentManifest::new(
            binding(),
            Geometry::new(page_size, bytes.len() as u64).unwrap(),
            &bytes
                .chunks(EXTENT_BYTES as usize)
                .map(|bytes| ExtentId::of(bytes).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let wal_length = std::fs::metadata(db.wal_path()).unwrap().len();
        assert!(wal_length > 2 * WINDOW as u64);
        *reads.lock().unwrap() = Reads::default();
        let observed = observe_extents(&db, None, binding(), bytes.len() as u64, 64 << 20)
            .unwrap();
        assert_eq!(observed.manifest().root(), expected.root());
        let budget = wal_length.div_ceil(WINDOW as u64)
            + bytes.len() as u64 / u64::from(page_size)
            + 8;
        {
            let cost = reads.lock().unwrap();
            assert!(cost.max_length <= WINDOW);
            assert!(
                cost.calls <= budget,
                "page_size={page_size}: {} WAL reads exceed {budget}",
                cost.calls
            );
        }
        *reads.lock().unwrap() = Reads::default();
        let same = observe_extents(
            &db,
            Some(&observed),
            binding(),
            bytes.len() as u64,
            64 << 20,
        )
        .unwrap();
        assert_eq!(same.manifest().root(), expected.root());
        assert!(same.work().reused_base);
        assert_eq!(same.work().image_bytes_read, 0);
        {
            let cost = reads.lock().unwrap();
            assert!(cost.calls > 0, "unchanged cuts still reverify actual WAL bytes");
            assert!(cost.max_length <= WINDOW);
            assert!(cost.calls <= wal_length.div_ceil(WINDOW as u64) + 8);
        }
        drop(writer);
        db.close().unwrap();
    }
}

#[test]
fn image_inspection_refuses_short_reads_and_corruption_across_read_windows() {
    for page_size in [512, 4096, 65536] {
        let root = tempfile::tempdir().unwrap();
        let reads = Arc::new(Mutex::new(Reads::default()));
        let (db, writer) = fixture(&root.path().join("main.sqlite"), page_size, reads.clone());
        let image = writer.serialize(MAIN_DB).unwrap();
        let wal = std::fs::read(db.wal_path()).unwrap();
        let boundary = 32 + WINDOW + 1;
        assert!(wal.len() > boundary);
        reads.lock().unwrap().short_at = Some(boundary as u64);
        assert!(db
            .committed_image_digest(image.len() as u64, page_size, 64 << 20)
            .is_err());
        assert!(reads.lock().unwrap().injected, "short-read cut was not reached");
        *reads.lock().unwrap() = Reads::default();
        let mut corrupt = wal.clone();
        corrupt[boundary] ^= 1;
        std::fs::write(db.wal_path(), corrupt).unwrap();
        let refused = db.committed_image_digest(image.len() as u64, page_size, 64 << 20);
        std::fs::write(db.wal_path(), wal).unwrap();
        assert!(refused.is_err(), "a read-window boundary hid WAL corruption");
        super::compare(&db, &writer);
        drop(writer);
        db.close().unwrap();
    }
}
