// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Streamed snapshots preserve the existing v0.5.2 bytes and proofs while
//! bounding every source read, transfer append and file write.
#![allow(clippy::disallowed_methods)]

use cellar_ltx::apply::ImageState;
use cellar_ltx::ltx::{self, Header};
use cellar_ltx::stream_snapshot::{self, Download, Limits, Metadata, SnapshotFile};
use cellar_ltx::{DirectFileSystem, FileSystem, TXID};
use sha2::{Digest, Sha256};
#[path = "support/stream_snapshot_files.rs"]
mod bounded_files;

fn database(page_size: u32) -> Vec<u8> {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source.sqlite");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "page_size", page_size)
        .unwrap();
    connection
        .execute_batch(
            "CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB);
        INSERT INTO items VALUES(1,zeroblob(262144));
        INSERT INTO items VALUES(2,x'616263');",
        )
        .unwrap();
    drop(connection);
    std::fs::read(path).unwrap()
}

fn state(bytes: &[u8], page_size: u32, checksums: bool) -> ImageState {
    let checksum = bytes
        .chunks_exact(page_size as usize)
        .enumerate()
        .fold(cellar_ltx::CHECKSUM_FLAG, |sum, (i, page)| {
            cellar_ltx::CHECKSUM_FLAG | (sum ^ ltx::checksum_page(i as u32 + 1, page))
        });
    ImageState {
        epoch: 1,
        txid: TXID(7),
        checksum: if checksums { checksum } else { 0 },
        page_size,
        length: bytes.len() as u64,
        digest: Sha256::digest(bytes).into(),
    }
}

fn encode(bytes: &[u8], state: ImageState) -> SnapshotFile {
    stream_snapshot::encode(
        &DirectFileSystem,
        state,
        Limits::default(),
        |offset, length| {
            assert!(length <= 64 * 1024);
            Ok(bytes[offset as usize..offset as usize + length].to_vec())
        },
    )
    .unwrap()
}

fn contents(file: &SnapshotFile) -> Vec<u8> {
    let mut bytes = Vec::new();
    let length = file.metadata().encoded_length;
    while (bytes.len() as u64) < length {
        let n = (length - bytes.len() as u64).min(64 * 1024) as usize;
        bytes.extend(file.chunk(bytes.len() as u64, n).unwrap());
    }
    bytes
}

fn receive(bytes: &[u8], metadata: Metadata) -> SnapshotFile {
    let mut download = Download::new(&DirectFileSystem, metadata, Limits::default()).unwrap();
    let mut offset = 0;
    for chunk in bytes.chunks(64 * 1024) {
        download.append(offset, chunk).unwrap();
        offset += chunk.len() as u64;
    }
    download.finish().unwrap()
}

#[test]
fn streamed_snapshot_is_byte_exact_and_restores_sqlite() {
    for page_size in [512, 4096, 65536] {
        for checksums in [false, true] {
            let bytes = database(page_size);
            let image = state(&bytes, page_size, checksums);
            let snapshot = encode(&bytes, image);
            snapshot.verify().unwrap();
            let encoded = contents(&snapshot);
            let pages = bytes
                .chunks_exact(page_size as usize)
                .enumerate()
                .map(|(i, page)| (i as u32 + 1, page.to_vec()))
                .collect::<Vec<_>>();
            let header = Header {
                version: ltx::VERSION,
                flags: if checksums {
                    0
                } else {
                    ltx::HEADER_FLAG_NO_CHECKSUM
                },
                page_size,
                commit: pages.len() as u32,
                min_txid: TXID(1),
                max_txid: image.txid,
                ..Default::default()
            };
            assert_eq!(
                encoded,
                ltx::encode_file_v0_5_2(&header, &pages, image.checksum).unwrap()
            );
            let downloaded = receive(&encoded, snapshot.metadata());
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("restored.sqlite");
            let mut target = DirectFileSystem.create_read_write(&path).unwrap();
            assert_eq!(downloaded.write_image(&mut target).unwrap(), image);
            target.sync_all().unwrap();
            drop(target);
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            let connection = rusqlite::Connection::open(&path).unwrap();
            let result: (i64, i64, String) = connection.query_row(
                "SELECT count(*),sum(length(value)),(SELECT hex(value) FROM items WHERE id=2) FROM items",
                [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
            assert_eq!(result, (2, 262147, "616263".into()));
            assert_eq!(
                connection
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
        }
    }
}

#[test]
fn invalid_order_bounds_truncation_and_identity_never_finish() {
    let bytes = database(4096);
    let snapshot = encode(&bytes, state(&bytes, 4096, false));
    let encoded = contents(&snapshot);
    let metadata = snapshot.metadata();
    let mut download = Download::new(&DirectFileSystem, metadata, Limits::default()).unwrap();
    assert!(download.append(1, &encoded[..16]).is_err());
    assert!(download.append(0, &vec![0; 65537]).is_err());
    download.append(0, &encoded[..16]).unwrap();
    assert!(download.append(0, &encoded[..16]).is_err());
    assert!(download.finish().is_err());
    let mut wrong = metadata;
    wrong.encoded_digest[0] ^= 1;
    let mut download = Download::new(&DirectFileSystem, wrong, Limits::default()).unwrap();
    let mut offset = 0;
    for chunk in encoded.chunks(65536) {
        download.append(offset, chunk).unwrap();
        offset += chunk.len() as u64;
    }
    assert!(download.finish().is_err());
    assert!(snapshot.chunk(0, 65537).is_err());
    assert!(snapshot.chunk(u64::MAX, 16).is_err());
}

#[test]
fn transport_digest_cannot_replace_physical_frame_index_or_image_proofs() {
    let bytes = database(4096);
    let snapshot = encode(&bytes, state(&bytes, 4096, false));
    let original = contents(&snapshot);
    let length = original.len();
    let index_size =
        u64::from_be_bytes(original[length - 24..length - 16].try_into().unwrap()) as usize;
    let index_at = length - 24 - index_size;
    for offset in [0, 80, 100, 106, 110, index_at, length - 25, length - 1] {
        let mut bytes = original.clone();
        bytes[offset] ^= 1;
        let metadata = Metadata {
            encoded_digest: Sha256::digest(&bytes).into(),
            ..snapshot.metadata()
        };
        let received = receive(&bytes, metadata);
        assert!(received.verify().is_err(), "corrupt offset {offset} passed");
    }
    let metadata = Metadata {
        image: ImageState {
            digest: [17; 32],
            ..snapshot.metadata().image
        },
        ..snapshot.metadata()
    };
    assert!(receive(&original, metadata).verify().is_err());
}

#[test]
fn uncertain_append_and_short_reads_release_scratch_without_a_proof() {
    use std::sync::{atomic::Ordering, Arc};
    let bytes = database(4096);
    let snapshot = encode(&bytes, state(&bytes, 4096, false));
    let encoded = contents(&snapshot);
    let root = tempfile::tempdir().unwrap();
    let files = bounded_files::BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    };
    let mut download = Download::new(&files, snapshot.metadata(), Limits::default()).unwrap();
    files.metrics.fail_append.store(true, Ordering::Release);
    assert!(download.append(0, &encoded[..64]).is_err());
    assert!(
        download.append(0, &encoded[..64]).is_err(),
        "uncertain append owner was reused"
    );
    assert!(download.finish().is_err());
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
    let copied = stream_snapshot::encode(
        &files,
        snapshot.metadata().image,
        Limits::default(),
        |o, n| Ok(bytes[o as usize..o as usize + n].to_vec()),
    )
    .unwrap();
    files.metrics.short_read.store(true, Ordering::Release);
    assert!(copied.verify().is_err());
    drop(copied);
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
}

#[test]
fn shared_scratch_admits_before_io_and_follows_the_last_physical_owner() {
    use std::sync::{atomic::Ordering, Arc};
    use stream_snapshot::{Error, ScratchLimits, ScratchPool, ScratchUsage};
    let bytes = database(4096);
    let image = state(&bytes, 4096, false);
    let root = tempfile::tempdir().unwrap();
    let files = bounded_files::BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    };
    let pool = ScratchPool::new(ScratchLimits {
        max_bytes: 1048576,
        max_files: 2,
    })
    .unwrap();
    let occupied = Download::new_admitted_in(
        &files,
        Some(root.path()),
        Metadata {
            image,
            encoded_length: 1048576,
            encoded_digest: [0; 32],
        },
        Limits::default(),
        &pool,
    )
    .unwrap();
    assert!(matches!(
        stream_snapshot::encode_admitted_in(
            &files,
            root.path(),
            image,
            Limits::default(),
            &pool,
            |_, _| panic!("refused encoder read source bytes"),
        ),
        Err(Error::ScratchFull)
    ));
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 1);
    drop(occupied);
    assert_eq!(pool.usage().unwrap(), ScratchUsage::default());

    let snapshot = stream_snapshot::encode_admitted_in(
        &files,
        root.path(),
        image,
        Limits::default(),
        &pool,
        |offset, n| Ok(bytes[offset as usize..offset as usize + n].to_vec()),
    )
    .unwrap();
    let encoded = contents(&snapshot);
    let metadata = snapshot.metadata();
    let clone = snapshot.clone();
    drop(snapshot);
    assert_eq!(
        pool.usage().unwrap(),
        ScratchUsage {
            reserved_bytes: metadata.encoded_length,
            reserved_files: 1
        }
    );
    let mut download = Download::new_admitted_in(
        &files,
        Some(root.path()),
        metadata,
        Limits::default(),
        &pool,
    )
    .unwrap();
    for (index, chunk) in encoded.chunks(65536).enumerate() {
        download.append(index as u64 * 65536, chunk).unwrap();
    }
    let received = download.finish().unwrap();
    assert_eq!(
        pool.usage().unwrap().reserved_bytes,
        2 * metadata.encoded_length
    );
    drop(received);
    let mut uncertain = Download::new_admitted_in(
        &files,
        Some(root.path()),
        metadata,
        Limits::default(),
        &pool,
    )
    .unwrap();
    files.metrics.fail_append.store(true, Ordering::Release);
    assert!(uncertain.append(0, &encoded[..64]).is_err());
    assert_eq!(
        pool.usage().unwrap().reserved_bytes,
        2 * metadata.encoded_length
    );
    assert!(uncertain.finish().is_err());
    assert_eq!(
        pool.usage().unwrap().reserved_bytes,
        metadata.encoded_length
    );
    drop(clone);
    assert_eq!(pool.usage().unwrap(), ScratchUsage::default());
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
    files.metrics.fail_append.store(false, Ordering::Release);
    assert!(stream_snapshot::encode_admitted_in(
        &files,
        root.path(),
        image,
        Limits::default(),
        &pool,
        |_, _| Err(std::io::ErrorKind::Interrupted.into()),
    )
    .is_err());
    assert_eq!(pool.usage().unwrap(), ScratchUsage::default());
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
}

#[test]
fn generation_publication_rereads_snapshot_output_before_returning_a_pin() {
    use cellar_ltx::generation::{GenerationError, GenerationLimits, GenerationStore, ImageId};
    use std::sync::{atomic::Ordering, Arc};
    let bytes = database(4096);
    let image = state(&bytes, 4096, false);
    let snapshot = encode(&bytes, image);
    let root = tempfile::tempdir().unwrap();
    let files = Arc::new(bounded_files::BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    });
    let store = GenerationStore::open(
        root.path().join("images"),
        files.clone(),
        GenerationLimits::default(),
    )
    .unwrap();
    let id = ImageId::new([1; 16]).unwrap();
    files.metrics.corrupt_output.store(true, Ordering::Release);
    assert!(store.prepare_snapshot(id, &snapshot).is_err());
    assert_eq!(store.usage().unwrap().generations, 0);
    assert!(matches!(
        store.adopt(id, image),
        Err(GenerationError::Missing)
    ));
    let pin = store.prepare_snapshot(id, &snapshot).unwrap();
    assert_eq!(pin.state(), image);
    drop(pin);
    assert_eq!(files.metrics.active_handles.load(Ordering::Acquire), 0);
}

#[test]
fn decimal_ten_gb_quota_is_independent_of_encoded_size() {
    assert_eq!(stream_snapshot::MAX_IMAGE_BYTES, 10_000_000_000);
    let bytes = database(4096);
    let mut image = state(&bytes, 4096, false);
    image.length = 10_000_000_000_u64.div_ceil(4096) * 4096;
    assert!(
        stream_snapshot::encode(&DirectFileSystem, image, Limits::default(), |_, _| {
            panic!("over-quota image reached I/O")
        })
        .is_err()
    );
    let metadata = Metadata {
        image,
        encoded_length: 1024,
        encoded_digest: [1; 32],
    };
    assert!(Download::new(&DirectFileSystem, metadata, Limits::default()).is_err());
}

/// Explicit physical-size qualification. This writes about 30 GB of disposable
/// data and needs a workspace-volume directory in CELLAR_LARGE_IMAGE_WORKDIR.
#[test]
#[ignore = "physical 10 GB SQLite/encoded-offset qualification; requires ~30 GB free disk"]
fn physical_ten_gb_snapshot_crosses_lock_page_and_4_gib_offsets() {
    let started = std::time::Instant::now();
    use std::sync::{atomic::Ordering, Arc};
    let workdir = std::env::var_os("CELLAR_LARGE_IMAGE_WORKDIR")
        .expect("explicit workspace scratch directory");
    let root = tempfile::tempdir_in(workdir).unwrap();
    let files = bounded_files::BoundedFiles {
        directory: root.path().into(),
        metrics: Arc::default(),
    };
    let source_path = root.path().join("source.sqlite");
    let connection = rusqlite::Connection::open(&source_path).unwrap();
    connection
        .execute_batch(
            "PRAGMA page_size=4096; PRAGMA synchronous=OFF;
        PRAGMA journal_mode=DELETE; PRAGMA cache_size=-8192;
        CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB); CREATE TABLE quota_tail(value BLOB);",
        )
        .unwrap();
    connection
        .pragma_update(
            None,
            "max_page_count",
            stream_snapshot::MAX_IMAGE_BYTES / 4096,
        )
        .unwrap();
    // Repeated rows still produce incompressible per-page LZ4 frames. The PRNG
    // is deterministic fixture data and does not supply production randomness.
    let mut payload = vec![0; 1024 * 1024];
    let mut rng = 0x1234567812345678_u64;
    for byte in &mut payload {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        *byte = rng as u8;
    }
    let payload_digest: [u8; 32] = Sha256::digest(&payload).into();
    let mut rows = 0_i64;
    loop {
        assert!(rows < 10_000, "quota did not stop growth");
        match connection.execute("INSERT INTO items(value) VALUES(?1)", [&payload]) {
            Ok(_) => rows += 1,
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::DiskFull =>
            {
                break
            }
            Err(error) => panic!("unexpected source insertion failure: {error}"),
        }
    }
    // Finish the sub-megabyte remainder one page at a time, so the physical
    // image reaches the exact page-rounded decimal quota rather than merely
    // stopping one large record below it.
    loop {
        match connection.execute("INSERT INTO quota_tail VALUES(zeroblob(2048))", []) {
            Ok(_) => (),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::DiskFull =>
            {
                break
            }
            Err(error) => panic!("unexpected quota-tail failure: {error}"),
        }
    }
    assert!(connection.is_autocommit());
    let pages: u64 = connection
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let length = pages * 4096;
    assert_eq!(length, stream_snapshot::MAX_IMAGE_BYTES / 4096 * 4096);
    eprintln!(
        "10gb stage=quota-filled bytes={length} rows={rows} elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
    // Full-quota SQLite cleanup must leave a reusable page budget.
    connection
        .execute("DELETE FROM items WHERE id=1", [])
        .unwrap();
    connection
        .execute("INSERT INTO items(id,value) VALUES(1,?1)", [&payload])
        .unwrap();
    drop(connection);
    let mut source = files.open(&source_path).unwrap();
    assert_eq!(source.file_len().unwrap(), length);
    let mut hash = Sha256::new();
    let mut offset = 0;
    while offset < length {
        let n = (length - offset).min(65536) as usize;
        hash.update(source.read_exact_at(offset, n).unwrap());
        offset += n as u64;
    }
    let image = ImageState {
        epoch: 1,
        txid: TXID(1),
        checksum: 0,
        page_size: 4096,
        length,
        digest: hash.finalize().into(),
    };
    let snapshot = stream_snapshot::encode(&files, image, Limits::default(), |offset, length| {
        source.read_exact_at(offset, length)
    })
    .unwrap();
    assert!(snapshot.metadata().encoded_length > u64::from(u32::MAX));
    eprintln!(
        "10gb stage=encoded encoded_bytes={} elapsed_s={:.3}",
        snapshot.metadata().encoded_length,
        started.elapsed().as_secs_f64()
    );
    let mut download = Download::new(&files, snapshot.metadata(), Limits::default()).unwrap();
    let mut offset = 0;
    while offset < snapshot.metadata().encoded_length {
        let n = (snapshot.metadata().encoded_length - offset).min(65536) as usize;
        download
            .append(offset, &snapshot.chunk(offset, n).unwrap())
            .unwrap();
        offset += n as u64;
    }
    let received = download.finish().unwrap();
    eprintln!(
        "10gb stage=received elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
    drop(snapshot);
    let target_path = root.path().join("target.sqlite");
    let mut target = files.create_read_write(&target_path).unwrap();
    assert_eq!(received.write_image(&mut target).unwrap(), image);
    target.sync_all().unwrap();
    eprintln!(
        "10gb stage=restored elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
    drop(target);
    let restored = rusqlite::Connection::open(&target_path).unwrap();
    assert_eq!(
        restored
            .query_row("SELECT count(*) FROM items", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        rows
    );
    for id in [1, rows / 2, rows] {
        let value: Vec<u8> = restored
            .query_row("SELECT value FROM items WHERE id=?1", [id], |r| r.get(0))
            .unwrap();
        assert_eq!(<[u8; 32]>::from(Sha256::digest(&value)), payload_digest);
    }
    assert_eq!(
        restored
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert!(files.metrics.max_read.load(Ordering::Relaxed) <= 65536);
    assert!(files.metrics.max_write.load(Ordering::Relaxed) <= 65536);
    eprintln!(
        "10gb stage=integrity-verified elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
}
