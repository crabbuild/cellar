//! Physical LTX and immutable-image verification through ordinary public APIs.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_ltx_fault.rs"]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
mod fixture;

use anyhow::Result;
use cellar_ltx::apply::ApplyError;
use cellar_ltx::generation::{GenerationError, GenerationLimits, GenerationStore};
use cellar_ltx::ltx::{self, Crc64, DecodeError, DecodeLimits, Header};
use cellar_ltx::DirectFileSystem;
use cellar_ltx::{CHECKSUM_FLAG, TXID};
use std::sync::Arc;

fn checksum(pages: &[(u32, Vec<u8>)]) -> u64 {
    pages.iter().fold(CHECKSUM_FLAG, |sum, (pgno, data)| {
        CHECKSUM_FLAG | (sum ^ ltx::checksum_page(*pgno, data))
    })
}

fn encode(pages: &[(u32, Vec<u8>)], commit: u32, no_checksum: bool, block: bool) -> Vec<u8> {
    let header = Header {
        version: ltx::VERSION,
        page_size: 512,
        commit,
        min_txid: TXID(1),
        max_txid: TXID(1),
        flags: if no_checksum {
            ltx::HEADER_FLAG_NO_CHECKSUM
        } else {
            0
        },
        ..Default::default()
    };
    let post = if no_checksum { 0 } else { checksum(pages) };
    if block {
        ltx::encode_file_v0_5_2(&header, pages, post).unwrap()
    } else {
        ltx::encode_file(&header, pages, post).unwrap()
    }
}

// This intentionally bypasses index validation while reconstructing the LTX
// checksum feed from known page bytes. Malformed index tests must remain corrupt
// even when an author has recomputed a valid file CRC.
fn repair_block_crc(bytes: &mut [u8], pages: &[(u32, Vec<u8>)]) {
    let mut crc = Crc64::new();
    crc.update(&bytes[..ltx::HEADER_SIZE]);
    let mut at = ltx::HEADER_SIZE;
    for (_, data) in pages {
        crc.update(&bytes[at..at + 10]);
        let size = u32::from_be_bytes(bytes[at + 6..at + 10].try_into().unwrap()) as usize;
        crc.update(data);
        at += 10 + size;
    }
    let end = bytes.len() - 8;
    crc.update(&bytes[at..end]);
    bytes[end..].copy_from_slice(&(CHECKSUM_FLAG | crc.sum64()).to_be_bytes());
}

fn index_start(bytes: &[u8]) -> usize {
    let at = bytes.len() - 24;
    at - u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap()) as usize
}

fn replace_index(bytes: &mut Vec<u8>, values: &[u64]) {
    let mut index = Vec::new();
    for &value in values {
        let mut value = value;
        while value >= 128 {
            index.push(value as u8 | 128);
            value >>= 7;
        }
        index.push(value as u8);
    }
    let start = index_start(bytes);
    let end = bytes.len() - 24;
    let size = index.len();
    bytes.splice(start..end, index);
    let at = bytes.len() - 24;
    bytes[at..at + 8].copy_from_slice(&(size as u64).to_be_bytes());
}

#[test]
fn bounded_decoder_matches_both_ltx_writers() -> Result<()> {
    let pages = vec![(1, vec![17; 512]), (2, vec![23; 512])];
    for block in [false, true] {
        for no_checksum in [false, true] {
            let bytes = encode(&pages, 2, no_checksum, block);
            let decoded = ltx::decode_file_bounded(&bytes, DecodeLimits::default())?;
            assert_eq!(decoded.pages(), &pages);
            assert_eq!(decoded.decoded_bytes(), 1024);
            assert_eq!(decoded.header().commit, 2);
            assert_eq!(
                decoded.trailer().post_apply_checksum,
                if no_checksum { 0 } else { checksum(&pages) }
            );
        }
    }
    Ok(())
}

#[test]
fn bounded_decoder_checks_physical_index_and_snapshot_coverage() {
    let pages = vec![(1, vec![17; 512]), (2, vec![23; 512])];
    let good = encode(&pages, 2, true, true);
    let first = 10 + u32::from_be_bytes(good[106..110].try_into().unwrap()) as u64;
    let second_at = 100 + first as usize;
    let second =
        10 + u32::from_be_bytes(good[second_at + 6..second_at + 10].try_into().unwrap()) as u64;
    let mut alias = good.clone();
    replace_index(&mut alias, &[1, 100, first, 2, 100, second, 0]);
    repair_block_crc(&mut alias, &pages);
    assert!(ltx::decode_file(&alias).is_ok());
    assert!(matches!(
        ltx::decode_file_bounded(&alias, DecodeLimits::default()),
        Err(DecodeError::Format(_))
    ));
    let mut duplicate = good.clone();
    replace_index(&mut duplicate, &[1, 100, first, 1, 100 + first, second, 0]);
    repair_block_crc(&mut duplicate, &pages);
    assert!(matches!(
        ltx::decode_file_bounded(&duplicate, DecodeLimits::default()),
        Err(DecodeError::Format(_))
    ));
    let missing = encode(&pages[..1], 3, true, true);
    assert!(
        ltx::decode_file(&missing).is_ok(),
        "ordinary file verification is not an apply coverage proof"
    );
    assert!(matches!(
        ltx::decode_file_bounded(&missing, DecodeLimits::default()),
        Err(DecodeError::Format(_))
    ));
    let mut reserved = good;
    reserved[80] = 1;
    repair_block_crc(&mut reserved, &pages);
    assert!(matches!(
        ltx::decode_file_bounded(&reserved, DecodeLimits::default()),
        Err(DecodeError::Format(_))
    ));
}

#[test]
fn bounded_decoder_refuses_expansion_before_decompression() {
    let pages = vec![(1, vec![17; 512]), (2, vec![23; 512])];
    let mut bytes = encode(&pages, 2, true, true);
    bytes[110] ^= 0xff; // Invalid compressed bytes must not be reached first.
    let limits = DecodeLimits {
        max_decoded_bytes: 512,
        ..Default::default()
    };
    assert!(matches!(
        ltx::decode_file_bounded(&bytes, limits),
        Err(DecodeError::Limit)
    ));
    let limits = DecodeLimits {
        max_pages: 1,
        ..Default::default()
    };
    assert!(matches!(
        ltx::decode_file_bounded(&bytes, limits),
        Err(DecodeError::Limit)
    ));
    let mut frame = encode(&pages, 2, true, false);
    frame[111] = 0x70; // Advertised 4-MiB scratch window, before the header CRC.
    assert!(matches!(
        ltx::decode_file_bounded(&frame, DecodeLimits::default()),
        Err(DecodeError::Limit)
    ));
    let mut huge = encode(&pages, 2, true, true);
    huge[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        ltx::decode_file_bounded(&huge, DecodeLimits::default()),
        Err(DecodeError::Limit)
    ));
}

#[test]
fn bounded_decoder_rejects_mutation_without_panics() {
    let pages = vec![(1, vec![17; 512]), (2, vec![23; 512])];
    let bytes = encode(&pages, 2, false, true);
    for length in 0..bytes.len() {
        assert!(ltx::decode_file_bounded(&bytes[..length], DecodeLimits::default()).is_err());
    }
    for offset in 0..bytes.len() {
        for bit in 0..8 {
            let mut changed = bytes.clone();
            changed[offset] ^= 1 << bit;
            assert!(
                std::panic::catch_unwind(|| ltx::decode_file_bounded(
                    &changed,
                    DecodeLimits::default()
                ))
                .is_ok(),
                "input panic at {offset}:{bit}"
            );
        }
    }
    let mut damaged = bytes;
    let end = damaged.len();
    damaged[end - 1] ^= 1;
    assert!(matches!(
        ltx::decode_file_bounded(&damaged, DecodeLimits::default()),
        Err(DecodeError::Checksum)
    ));
}

#[test]
fn bounded_decoder_round_trips_real_sqlite_state() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("source.sqlite");
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute_batch(
        "PRAGMA page_size=512; CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(42)",
    )?;
    drop(connection);
    let image = std::fs::read(path)?;
    let pages: Vec<_> = image
        .as_chunks::<512>()
        .0
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u32 + 1, p.to_vec()))
        .collect();
    for block in [false, true] {
        let bytes = encode(&pages, pages.len() as u32, false, block);
        let decoded = ltx::decode_file_bounded(&bytes, DecodeLimits::default())?;
        let restored: Vec<u8> = decoded
            .pages()
            .iter()
            .flat_map(|(_, p)| p.iter().copied())
            .collect();
        assert_eq!(restored, image);
    }
    Ok(())
}

#[test]
fn ltx_exact_base() -> Result<()> {
    use fixture::*;
    for checksums in [false, true] {
        let directory = tempfile::tempdir()?;
        let store = GenerationStore::open(
            directory.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let initial = image(
            None,
            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(7)",
        )?;
        let birth = Cut::new(&[], &initial, 0, checksums, true);
        let first = store.prepare(id(1), None, &birth.update())?;
        let changed = image(Some(&initial), "UPDATE counter SET value=8")?;
        let delta = Cut::new(&initial, &changed, 1, checksums, false);
        let mut wrong = delta.update();
        wrong.base.digest[0] ^= 1;
        assert!(matches!(
            store.prepare(id(2), Some(&first), &wrong),
            Err(GenerationError::Identity)
        ));
        let mut wrong = delta.update();
        wrong.result.digest[0] ^= 1;
        assert!(matches!(
            store.prepare(id(2), Some(&first), &wrong),
            Err(GenerationError::Apply(ApplyError::ResultDigest))
        ));
        let mut gap = delta.update();
        gap.segments[0].min_txid = TXID(3);
        gap.segments[0].max_txid = TXID(3);
        gap.result.txid = TXID(3);
        assert!(matches!(
            store.prepare(id(2), Some(&first), &gap),
            Err(GenerationError::Apply(ApplyError::Chain))
        ));
        assert_eq!(store.usage()?.generations, 1);
        assert_eq!(read(&first)?, initial);
        let second = store.prepare(id(2), Some(&first), &delta.update())?;
        assert_eq!(read(&second)?, changed);
        assert_eq!(read(&first)?, initial);
        // The bytes copied from a base are reverified, even if its process-local
        // pin still holds the originally verified digest.
        let path = directory
            .path()
            .join("images")
            .join(format!("{}.sqlite", hex::encode(first.id().bytes())));
        let mut corrupted = initial.clone();
        let at = corrupted.len() - 1;
        corrupted[at] ^= 1;
        std::fs::write(path, corrupted)?;
        assert!(matches!(
            store.prepare(id(3), Some(&first), &delta.update()),
            Err(GenerationError::Apply(ApplyError::BaseDigest))
        ));
    }
    Ok(())
}

#[test]
fn ltx_growth_truncation() -> Result<()> {
    use fixture::*;
    for checksums in [false, true] {
        let directory = tempfile::tempdir()?;
        let store = GenerationStore::open(
            directory.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let initial = image(
            None,
            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(7)",
        )?;
        let birth = Cut::new(&[], &initial, 0, checksums, true);
        let first = store.prepare(id(1), None, &birth.update())?;
        let grown=image(Some(&initial),"CREATE TABLE payloads(value BLOB);INSERT INTO payloads VALUES(zeroblob(24000));UPDATE counter SET value=8")?;
        assert!(grown.len() > initial.len());
        let growth = Cut::new(&initial, &grown, 1, checksums, false);
        let second = store.prepare(id(2), Some(&first), &growth.update())?;
        assert_eq!(read(&second)?, grown);
        let shrunk = image(
            Some(&grown),
            "DELETE FROM payloads;UPDATE counter SET value=9;VACUUM",
        )?;
        assert!(shrunk.len() < grown.len());
        let shrink = Cut::new(&grown, &shrunk, 2, checksums, true);
        let third = store.prepare(id(3), Some(&second), &shrink.update())?;
        assert_eq!(read(&third)?, shrunk);
        assert_eq!(third.state().length, shrunk.len() as u64);
        assert_eq!(read(&second)?, grown);
        // Missing growth pages cannot be filled implicitly with zeros, even
        // when the LTX file has no rolling database checksum.
        let pages: Vec<_> = growth
            .pages
            .iter()
            .filter(|(pgno, _)| *pgno != initial.len() as u32 / 512 + 1)
            .cloned()
            .collect();
        let header = Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: 512,
            commit: (grown.len() / 512) as u32,
            min_txid: TXID(2),
            max_txid: TXID(2),
            ..Default::default()
        };
        let malformed = ltx::encode_file_v0_5_2(&header, &pages, 0)?;
        let mut request = growth.update();
        request.result.checksum = 0;
        request.segments[0].bytes = &malformed;
        request.segments[0].page_count = pages.len();
        request.segments[0].decoded_bytes = pages.len() as u64 * 512;
        assert!(matches!(
            store.prepare(id(4), Some(&first), &request),
            Err(GenerationError::Apply(ApplyError::MissingPage))
        ));
    }
    Ok(())
}

#[test]
fn immutable_reader_pinning() -> Result<()> {
    use fixture::*;
    let directory = tempfile::tempdir()?;
    let store = GenerationStore::open(
        directory.path().join("images"),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    let initial = image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
    )?;
    let birth = Cut::new(&[], &initial, 0, true, true);
    let first = store.prepare(id(1), None, &birth.update())?;
    let old = query(first.clone())?;
    assert_eq!(
        old.connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?,
        7
    );
    assert_eq!(
        old.connection().query_row(
            "SELECT count(*) FROM sqlite_master WHERE name LIKE '__sys_%'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0,
        "the query opener must not initialize framework schema"
    );
    assert_eq!(
        old.connection()
            .query_row("SELECT date('now')", [], |r| r.get::<_, String>(0))?,
        "1970-01-01"
    );
    for sql in [
        "UPDATE counter SET value=0",
        "CREATE TABLE forbidden(x)",
        "CREATE TEMP TABLE forbidden(x)",
        "PRAGMA user_version=1",
        "PRAGMA query_only=OFF",
        "PRAGMA optimize",
        "ATTACH ':memory:' AS extra",
        "BEGIN IMMEDIATE",
        "VACUUM",
    ] {
        assert!(
            old.connection().execute_batch(sql).is_err(),
            "query accepted mutation: {sql}"
        );
    }
    let changed = image(Some(&initial), "UPDATE counter SET value=8")?;
    let delta = Cut::new(&initial, &changed, 1, true, false);
    let second = store.prepare(id(2), Some(&first), &delta.update())?;
    let current = query(second.clone())?;
    store.retire(first.id())?;
    drop(first);
    assert_eq!(
        store.reclaim(8)?.files,
        0,
        "the old query still pins its image"
    );
    assert_eq!(
        old.connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?,
        7
    );
    assert_eq!(
        current
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?,
        8
    );
    drop(old);
    assert_eq!(store.reclaim(1)?.files, 1);
    assert_eq!(read(&second)?, changed);
    Ok(())
}

#[test]
fn bounded_image_reclamation() -> Result<()> {
    use fixture::*;
    let directory = tempfile::tempdir()?;
    let limits = GenerationLimits {
        max_generations: 2,
        ..Default::default()
    };
    let root = directory.path().join("images");
    let store = GenerationStore::open(root.clone(), Arc::new(DirectFileSystem), limits)?;
    let initial = image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
    )?;
    let first = store.prepare(
        id(1),
        None,
        &Cut::new(&[], &initial, 0, false, true).update(),
    )?;
    let next = image(Some(&initial), "UPDATE counter SET value=8")?;
    let delta = Cut::new(&initial, &next, 1, false, true);
    let second = store.prepare(id(2), Some(&first), &delta.update())?;
    let next2 = image(Some(&next), "UPDATE counter SET value=9")?;
    let delta2 = Cut::new(&next, &next2, 2, false, true);
    assert!(matches!(
        store.prepare(id(3), Some(&second), &delta2.update()),
        Err(GenerationError::Limit)
    ));
    store.retire(id(1))?;
    assert_eq!(store.reclaim(1)?.files, 0);
    drop(first);
    assert_eq!(store.reclaim(1)?.files, 1);
    let third = store.prepare(id(3), Some(&second), &delta2.update())?;
    let second_state = second.state();
    let third_state = third.state();
    // Forgetting process-local pins does not release the host retention root.
    drop(second);
    drop(third);
    drop(store);
    let recovered = GenerationStore::open(root, Arc::new(DirectFileSystem), limits)?;
    assert_eq!(recovered.reclaim(2)?.files, 0);
    let referenced = recovered.adopt(id(2), second_state)?;
    assert_eq!(read(&referenced)?, next);
    // The directory's newest image is not implicitly chosen as the head.
    assert_eq!(referenced.state(), second_state);
    assert_ne!(referenced.state(), third_state);
    recovered.retire(id(3))?;
    assert_eq!(recovered.reclaim(1)?.files, 1);
    assert_eq!(recovered.usage()?.generations, 1);
    Ok(())
}

#[test]
fn ltx_checksum_corruption() -> Result<()> {
    use fixture::*;
    for checksums in [false, true] {
        let directory = tempfile::tempdir()?;
        let store = GenerationStore::open(
            directory.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let initial = image(
            None,
            "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
        )?;
        let birth = Cut::new(&[], &initial, 0, checksums, true);
        let first = store.prepare(id(1), None, &birth.update())?;
        let next = image(Some(&initial), "UPDATE counter SET value=8")?;
        let delta = Cut::new(&initial, &next, 1, checksums, true);
        for at in [0, 8, 100, 106, delta.bytes.len() - 1] {
            let mut changed = delta.bytes.clone();
            changed[at] ^= 0xff;
            let mut update = delta.update();
            update.segments[0].bytes = &changed;
            assert!(
                store.prepare(id(2), Some(&first), &update).is_err(),
                "corrupt byte {at} published an image"
            );
            assert_eq!(store.usage()?.generations, 1);
            assert_eq!(read(&first)?, initial);
        }
        let mut pages = delta.pages.clone();
        pages[0].1[500] ^= 1;
        let header = ltx::decode_file(&delta.bytes)?.header;
        let bad = ltx::encode_file_v0_5_2(&header, &pages, delta.result.checksum)?;
        let mut update = delta.update();
        update.segments[0].bytes = &bad;
        assert!(matches!(
            store.prepare(id(2), Some(&first), &update),
            Err(GenerationError::Apply(
                ApplyError::Checksum | ApplyError::ResultDigest
            ))
        ));
        let mut wrong = delta.update();
        wrong.segments[0].decoded_bytes = 1;
        assert!(store.prepare(id(2), Some(&first), &wrong).is_err());
        assert_eq!(store.usage()?.generations, 1);
    }
    Ok(())
}

#[test]
fn image_sync_before_marker() -> Result<()> {
    use fixture::*;
    use std::sync::atomic::Ordering;
    for mode in [fault::FAIL_IMAGE_SYNC, fault::FAIL_DIRECTORY_SYNC] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("images");
        let faults = fault::Faults::new(root.clone(), None);
        let fs = Arc::new(fault::FaultFileSystem {
            faults: faults.clone(),
        });
        let store = GenerationStore::open(root, fs, GenerationLimits::default())?;
        let initial = image(
            None,
            "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
        )?;
        let first = store.prepare(
            id(1),
            None,
            &Cut::new(&[], &initial, 0, false, true).update(),
        )?;
        save_marker(directory.path(), &first)?;
        faults.events.lock().unwrap().clear();
        faults.mode.store(mode, Ordering::Release);
        let next = image(Some(&initial), "UPDATE counter SET value=8")?;
        assert!(store
            .prepare(
                id(2),
                Some(&first),
                &Cut::new(&initial, &next, 1, false, true).update()
            )
            .is_err());
        assert_eq!(load_marker(directory.path())?.0, id(1));
        assert_eq!(store.usage()?.generations, 1);
        assert_eq!(read(&first)?, initial);
        faults.events.lock().unwrap().clear();
        let second = store.prepare(
            id(2),
            Some(&first),
            &Cut::new(&initial, &next, 1, false, true).update(),
        )?;
        assert_eq!(
            *faults.events.lock().unwrap(),
            ["image-sync", "rename", "directory-sync"]
        );
        save_marker(directory.path(), &second)?;
        assert_eq!(load_marker(directory.path())?.0, id(2));
    }
    // Crash cuts run real fsync/rename calls in a separately killed process.
    for mode in [
        fault::BEFORE_IMAGE_SYNC,
        fault::AFTER_RENAME,
        fault::AFTER_DIRECTORY_SYNC,
        fault::AFTER_MARKER_SYNC,
    ] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("images");
        let store = GenerationStore::open(
            root.clone(),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let initial = image(
            None,
            "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
        )?;
        let next = image(Some(&initial), "UPDATE counter SET value=8")?;
        std::fs::write(directory.path().join("source.sqlite"), &initial)?;
        std::fs::write(directory.path().join("next.sqlite"), &next)?;
        let first = store.prepare(
            id(1),
            None,
            &Cut::new(&[], &initial, 0, false, true).update(),
        )?;
        save_marker(directory.path(), &first)?;
        drop(first);
        drop(store);

        struct Child(std::process::Child);

        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let log = std::fs::File::create(directory.path().join("child.log"))?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--ignored", "--exact", "image_crash_process", "--nocapture"])
                .env("CELLAR_IMAGE_CRASH_ROOT", directory.path())
                .env("CELLAR_IMAGE_CRASH_MODE", mode.to_string())
                .stdout(std::process::Stdio::from(log))
                .stderr(std::process::Stdio::inherit())
                .spawn()?,
        );
        let signal = directory.path().join("reached");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !signal.exists() {
            if child.0.try_wait()?.is_some() || std::time::Instant::now() > deadline {
                anyhow::bail!(
                    "child missed cut {mode}: {}",
                    std::fs::read_to_string(directory.path().join("child.log"))?
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        child.0.kill()?;
        let _ = child.0.wait()?;
        let (marker, expected) = load_marker(directory.path())?;
        assert_eq!(
            marker,
            if mode == fault::AFTER_MARKER_SYNC {
                id(2)
            } else {
                id(1)
            }
        );
        let recovered = GenerationStore::open(
            root,
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let pin = recovered.adopt(marker, expected)?;
        assert_eq!(
            read(&pin)?,
            if mode == fault::AFTER_MARKER_SYNC {
                next
            } else {
                initial
            }
        );
    }
    Ok(())
}

#[test]
#[ignore = "child entry point invoked and killed by image_sync_before_marker"]
fn image_crash_process() -> Result<()> {
    use fixture::*;
    use std::sync::atomic::Ordering;
    let root = std::path::PathBuf::from(std::env::var("CELLAR_IMAGE_CRASH_ROOT")?);
    let mode: u8 = std::env::var("CELLAR_IMAGE_CRASH_MODE")?.parse()?;
    let faults = fault::Faults::new(root.join("images"), Some(root.join("reached")));
    let store = GenerationStore::open(
        root.join("images"),
        Arc::new(fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits::default(),
    )?;
    let (marker, state) = load_marker(&root)?;
    let first = store.adopt(marker, state)?;
    let initial = std::fs::read(root.join("source.sqlite"))?;
    let next = std::fs::read(root.join("next.sqlite"))?;
    faults.mode.store(mode, Ordering::Release);
    let second = store.prepare(
        id(2),
        Some(&first),
        &Cut::new(&initial, &next, 1, false, true).update(),
    )?;
    save_marker(&root, &second)?;
    if mode == fault::AFTER_MARKER_SYNC {
        faults.pause()?;
    }
    anyhow::bail!("crash fixture did not stop")
}

#[test]
fn immutable_query_uses_injected_filesystem() -> Result<()> {
    use fixture::*;
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("images");
    let faults = fault::Faults::new(root.clone(), None);
    let store = GenerationStore::open(
        root.clone(),
        Arc::new(fault::FaultFileSystem {
            faults: faults.clone(),
        }),
        GenerationLimits::default(),
    )?;
    let initial = image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
    )?;
    let first = store.prepare(
        id(1),
        None,
        &Cut::new(&[], &initial, 0, false, true).update(),
    )?;
    let before = faults.reads.load(Ordering::Acquire);
    let reader = query(first.clone())?;
    assert_eq!(
        reader
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?,
        7
    );
    assert!(faults.reads.load(Ordering::Acquire) > before);
    drop(reader);
    faults.fail_reads.store(true, Ordering::Release);
    if let Ok(reader) = query(first.clone()) {
        assert!(reader
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .is_err());
    }
    // Closing failed SQLite opens must release every VFS/file reference.
    drop(first);
    drop(store);
    let reopened = GenerationStore::open(
        root,
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    assert_eq!(reopened.usage()?.generations, 1);
    Ok(())
}

#[test]
fn query_services_and_directory_ownership_are_independent() -> Result<()> {
    use fixture::*;
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("images");
    let store = GenerationStore::open(
        root.clone(),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    let image = image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
    )?;
    let pin = store.prepare(id(1), None, &Cut::new(&[], &image, 0, false, true).update())?;
    let old = query(pin.clone())?;
    let next = cellar::storage::open_immutable_generation(
        pin.clone(),
        cellar::storage::ImmutableQueryOptions {
            sqlite_vec: false,
            services: cellar_ltx::generation::ReadServices {
                now_unix_millis: Arc::new(|| 86_400_000),
                fill_random: Arc::new(|b| b.fill(11)),
            },
        },
    )?;
    let a = old.connection();
    let b = next.connection();
    assert_eq!(
        a.query_row("SELECT date('now')", [], |r| r.get::<_, String>(0))?,
        "1970-01-01"
    );
    assert_eq!(
        b.query_row("SELECT date('now')", [], |r| r.get::<_, String>(0))?,
        "1970-01-02"
    );
    assert_eq!(
        a.query_row("SELECT date('now')", [], |r| r.get::<_, String>(0))?,
        "1970-01-01"
    );
    drop(pin);
    drop(store);
    drop(next);
    assert!(
        GenerationStore::open(
            root.clone(),
            Arc::new(DirectFileSystem),
            GenerationLimits::default()
        )
        .is_err(),
        "a live query owns its immutable directory"
    );
    drop(old);
    let recovered = GenerationStore::open(
        root,
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    assert_eq!(recovered.usage()?.generations, 1);
    Ok(())
}

#[test]
fn multi_file_apply_and_optional_checksum_transitions() -> Result<()> {
    use fixture::*;
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("images");
    let store = GenerationStore::open(
        root,
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    let initial = image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
    )?;
    let first = store.prepare(
        id(1),
        None,
        &Cut::new(&[], &initial, 0, false, true).update(),
    )?;
    let grown=image(Some(&initial),"CREATE TABLE payload(value BLOB);INSERT INTO payload VALUES(zeroblob(20000));UPDATE counter SET value=8")?;
    let shrunk = image(
        Some(&grown),
        "DELETE FROM payload;UPDATE counter SET value=9;VACUUM",
    )?;
    let grow = Cut::new(&initial, &grown, 1, true, false);
    let shrink = Cut::new(&grown, &shrunk, 2, true, true);
    let mut update = grow.update();
    update.base = first.state();
    update.result = shrink.result;
    update.segments.extend(shrink.update().segments);
    let result = store.prepare(id(2), Some(&first), &update)?;
    assert_eq!(read(&result)?, shrunk);
    let again = image(Some(&shrunk), "UPDATE counter SET value=10")?;
    let disable = Cut::new(&shrunk, &again, 3, false, true);
    let mut update = disable.update();
    update.base = result.state();
    let last = store.prepare(id(3), Some(&result), &update)?;
    assert_eq!(read(&last)?, again);
    assert_eq!(last.state().checksum, 0);
    Ok(())
}

#[test]
fn unexpected_final_name_is_never_overwritten() -> Result<()> {
    use fixture::*;
    use std::sync::atomic::Ordering;
    for ambiguous in [false, true] {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("images");
        let faults = fault::Faults::new(root.clone(), None);
        let store = GenerationStore::open(
            root.clone(),
            Arc::new(fault::FaultFileSystem {
                faults: faults.clone(),
            }),
            GenerationLimits::default(),
        )?;
        let initial = image(
            None,
            "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
        )?;
        if ambiguous {
            faults
                .mode
                .store(fault::FAIL_AFTER_FINAL_CREATE, Ordering::Release);
        } else {
            faults.collision.store(true, Ordering::Release);
        }
        assert!(store
            .prepare(
                id(1),
                None,
                &Cut::new(&[], &initial, 0, false, true).update()
            )
            .is_err());
        let final_path = root.join(format!("{}.sqlite", hex::encode(id(1).bytes())));
        assert_eq!(
            std::fs::read(final_path)?,
            if ambiguous {
                &b""[..]
            } else {
                &b"unowned image"[..]
            }
        );
        assert!(matches!(store.usage(), Err(GenerationError::Quarantined)));
        drop(store);
        let recovered = GenerationStore::open(
            root,
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        assert_eq!(recovered.usage()?.generations, 1);
        assert_eq!(recovered.reclaim(2)?.files, 0);
        assert!(recovered.adopt(id(1), state(&initial, 1, false)).is_err());
    }
    Ok(())
}

#[test]
fn recovered_inventory_retires_only_after_complete_reference_validation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let first = fixture::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
    )?;
    let second = fixture::image(Some(&first), "UPDATE counter SET value=8")?;
    let a = fixture::Cut::new(&[], &first, 0, true, true);
    let b = fixture::Cut::new(&first, &second, 1, true, true);
    let images = GenerationStore::open(
        root.path().join("images"),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    let first_pin = images.prepare(fixture::id(1), None, &a.update())?;
    let second_pin = images.prepare(fixture::id(2), Some(&first_pin), &b.update())?;
    drop(second_pin);
    drop(first_pin);
    drop(images);
    let images = GenerationStore::open(
        root.path().join("images"),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    assert_eq!(images.usage()?.retained, 2);
    assert!(
        matches!(
            images.retain_only(&[fixture::id(1)]),
            Err(GenerationError::Missing)
        ),
        "an unverified filename became a durable reference"
    );
    assert_eq!(
        images.usage()?.retained,
        2,
        "failed reference validation partly retired inventory"
    );
    let first_pin = images.adopt(fixture::id(1), a.result)?;
    let second_pin = images.adopt(fixture::id(2), b.result)?;
    images.retain_only(&[fixture::id(2)])?;
    images.reclaim(8)?;
    assert_eq!(
        fixture::read(&first_pin)?,
        first,
        "retirement reclaimed a live reader"
    );
    assert_eq!(images.usage()?.generations, 2);
    drop(first_pin);
    images.reclaim(8)?;
    assert_eq!(images.usage()?.generations, 1);
    assert_eq!(fixture::read(&second_pin)?, second);
    Ok(())
}
