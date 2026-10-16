use super::*;
use cellar_ltx::DirectFileSystem;

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn gather_payload_residency_stays_bounded() -> Result<()> {
    const CHILD: &str = "CELLAR_TEST_GATHER_RSS";
    if std::env::var_os(CHILD).is_none() {
        let name = format!(
            "{}::gather_payload_residency_stays_bounded",
            module_path!().split_once("::").unwrap().1
        );
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", &name, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("{stderr}");
        anyhow::ensure!(
            output.status.success() && stdout.contains("1 passed; 0 failed"),
            "isolated gather failed: {stdout}\n{stderr}"
        );
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let baseline = crate::memory::sample().rss_bytes;
    anyhow::ensure!(baseline > 0, "gather RSS unavailable");
    let mut peak = baseline;
    let mut rows = Rows::new(Arc::new(DirectFileSystem), directory.path().to_owned());
    // Same production gather store receives each follower's rows; construct
    // one row at a time so this measures retained history, not a TailResp.
    for txid in (1..=32).rev() {
        rows.insert(
            ("cell".into(), 1, txid),
            vec![txid as u8; 4 << 20],
            Priority::Preferred,
            Duplicates::Last,
        )?;
        peak = peak.max(crate::memory::sample().rss_bytes);
    }
    let mut rows = rows.finish()?;
    peak = peak.max(crate::memory::sample().rss_bytes);
    let mut expected = 1;
    loop {
        let batch = rows.next_batch()?;
        if batch.is_empty() {
            break;
        }
        for ((cell, epoch, txid), bytes) in batch {
            assert_eq!((cell.as_str(), epoch, txid), ("cell", 1, expected));
            assert_eq!(bytes.len(), 4 << 20);
            assert!(bytes.iter().all(|byte| *byte == txid as u8));
            expected += 1;
        }
        peak = peak.max(crate::memory::sample().rss_bytes);
    }
    assert_eq!(expected, 33);
    let growth = peak.saturating_sub(baseline);
    eprintln!("gather RSS: growth={growth}, baseline={baseline}, peak={peak}");
    anyhow::ensure!(
        growth <= 48 << 20,
        "gather retained history: {growth} > {}",
        48 << 20
    );
    Ok(())
}

fn gather() -> Result<(tempfile::TempDir, Rows)> {
    let directory = tempfile::tempdir()?;
    let rows = Rows::new(Arc::new(DirectFileSystem), directory.path().to_owned());
    Ok((directory, rows))
}

fn collect(rows: Rows) -> Result<Batch> {
    let mut sorted = rows.finish()?;
    let mut result = Batch::new();
    let mut previous = None;
    loop {
        let batch = sorted.next_batch()?;
        if batch.is_empty() {
            break;
        }
        assert!(batch.len() <= BATCH_ROWS);
        for (key, payload) in batch {
            if let Some(previous) = previous {
                assert!(previous < key);
            }
            previous = Some(key.clone());
            assert!(result.insert(key, payload).is_none());
        }
    }
    Ok(result)
}

#[test]
fn variable_keys_numeric_order_and_duplicate_precedence_survive_many_runs() -> Result<()> {
    for (follower_priority, follower_duplicates, bundle_priority) in [
        (Priority::Preferred, Duplicates::Last, Priority::Fallback),
        (Priority::Fallback, Duplicates::First, Priority::Preferred),
    ] {
        let (_directory, mut rows) = gather()?;
        let mut expected = Batch::new();
        for copy in 0..3_u8 {
            for id in (0..707_u64).rev() {
                let cell = if id % 11 == 0 {
                    format!("{}-{id}", "a".repeat(65500))
                } else {
                    format!("cell-{}", id % 5)
                };
                let key = (cell, [1, 9, 10, u64::MAX][id as usize % 4], id);
                let payload = vec![copy, (id % 251) as u8];
                rows.insert(
                    key.clone(),
                    payload.clone(),
                    follower_priority,
                    follower_duplicates,
                )?;
                match follower_duplicates {
                    Duplicates::First => {
                        expected.entry(key).or_insert(payload);
                    }
                    Duplicates::Last => {
                        expected.insert(key, payload);
                    }
                }
            }
        }
        // Bundle copies arrive after follower observations. Recovery keeps
        // the last follower; live folding keeps the bucket's copy instead.
        for id in [0, 9, 10, 33, 706] {
            let cell = if id % 11 == 0 {
                format!("{}-{id}", "a".repeat(65500))
            } else {
                format!("cell-{}", id % 5)
            };
            let key = (cell, [1, 9, 10, u64::MAX][id as usize % 4], id);
            rows.insert(key.clone(), vec![99], bundle_priority, Duplicates::First)?;
            if matches!(bundle_priority, Priority::Preferred) {
                expected.insert(key, vec![99]);
            }
        }
        assert!(rows.runs > 2, "fixture must exercise multiple merge passes");
        assert_eq!(collect(rows)?, expected);
    }
    Ok(())
}

#[test]
fn upload_batches_bound_empty_rows_and_allow_one_large_row() -> Result<()> {
    let (_directory, mut rows) = gather()?;
    for id in (0..301).rev() {
        rows.insert(
            ("cell".into(), 1, id),
            Vec::new(),
            Priority::Preferred,
            Duplicates::Last,
        )?;
    }
    rows.insert(
        ("oversized".into(), 1, 1),
        vec![17; BATCH_BYTES + 100],
        Priority::Preferred,
        Duplicates::Last,
    )?;
    let mut rows = rows.finish()?;
    assert_eq!(rows.next_batch()?.len(), 128);
    assert_eq!(rows.next_batch()?.len(), 128);
    assert_eq!(rows.next_batch()?.len(), 45);
    let batch = rows.next_batch()?;
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.values().next().unwrap().len(), BATCH_BYTES + 100);
    assert!(rows.next_batch()?.is_empty());
    Ok(())
}

#[test]
fn invalid_cell_is_refused_before_spooling_and_empty_needs_no_scratch() -> Result<()> {
    let (_directory, mut rows) = gather()?;
    assert!(rows
        .insert(
            ("x".repeat(65536), 1, 1),
            vec![1],
            Priority::Preferred,
            Duplicates::Last
        )
        .is_err());
    assert!(rows.payload.is_none() && rows.is_empty());
    assert!(collect(rows)?.is_empty());
    let (_directory, mut rows) = gather()?;
    let key = ("x".repeat(65535), u64::MAX, u64::MAX);
    rows.insert(key.clone(), vec![1], Priority::Preferred, Duplicates::Last)?;
    assert_eq!(collect(rows)?, Batch::from([(key, vec![1])]));
    Ok(())
}

#[derive(Clone, Copy)]
enum Fault {
    Write,
    Read,
    Short,
    Length,
    Corrupt,
    LateCorrupt,
    PayloadCorrupt,
    Cancel,
}

struct FaultFile {
    file: HostFile,
    fault: Fault,
    live: Arc<std::sync::atomic::AtomicUsize>,
    cancellation: Arc<AtomicBool>,
}

impl Drop for FaultFile {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl cellar_ltx::HostFileIo for FaultFile {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        if matches!(self.fault, Fault::Write) {
            return Err(std::io::Error::other("injected gather write"));
        }
        self.file.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        if matches!(self.fault, Fault::Read) {
            return Err(std::io::Error::other("injected gather read"));
        }
        let mut bytes = self.file.read_exact_at(offset, len)?;
        match self.fault {
            Fault::Short => {
                bytes.pop();
            }
            Fault::Corrupt => {
                if let Some(first) = bytes.first_mut() {
                    *first ^= 1;
                }
            }
            Fault::PayloadCorrupt if offset == DESCRIPTOR_TAG_BYTES as u64 => {
                if let Some(first) = bytes.first_mut() {
                    *first ^= 1;
                }
            }
            Fault::LateCorrupt if offset > BUFFER_BYTES as u64 => {
                if let Some(first) = bytes.first_mut() {
                    *first ^= 1;
                }
            }
            Fault::Cancel => self.cancellation.store(true, Ordering::Release),
            _ => (),
        }
        Ok(bytes)
    }

    fn file_len(&mut self) -> std::io::Result<u64> {
        let len = self.file.file_len()?;
        Ok(len + u64::from(matches!(self.fault, Fault::Length)))
    }

    fn sync_all(&mut self) -> std::io::Result<()> {
        self.file.sync_all()
    }
}

fn wrap(
    file: HostFile,
    fault: Fault,
    live: &Arc<std::sync::atomic::AtomicUsize>,
    cancellation: &Arc<AtomicBool>,
) -> HostFile {
    live.fetch_add(1, Ordering::SeqCst);
    HostFile::from_io(FaultFile {
        file,
        fault,
        live: live.clone(),
        cancellation: cancellation.clone(),
    })
}

#[test]
fn scratch_failures_never_return_an_empty_success_and_release_files() -> Result<()> {
    for fault in [
        Fault::Write,
        Fault::Read,
        Fault::Short,
        Fault::Length,
        Fault::Corrupt,
        Fault::Cancel,
    ] {
        let (directory, mut rows) = gather()?;
        for id in (0..400).rev() {
            rows.insert(
                ("cell".into(), 1, id),
                vec![id as u8],
                Priority::Preferred,
                Duplicates::Last,
            )?;
        }
        let live = Arc::default();
        let cancellation = rows.cancellation();
        rows.index = Some(wrap(
            rows.index.take().unwrap(),
            fault,
            &live,
            &cancellation,
        ));
        let result = collect(rows);
        assert!(result.is_err(), "scratch fault returned success");
        assert_eq!(
            live.load(Ordering::SeqCst),
            0,
            "fault retained a scratch handle"
        );
        assert_eq!(
            DirectFileSystem.read_dir(directory.path())?.len(),
            0,
            "scratch left named files"
        );
    }
    Ok(())
}

#[test]
fn indexed_payload_reread_must_match_its_digest_and_length() -> Result<()> {
    for fault in [
        Fault::Read,
        Fault::Short,
        Fault::Length,
        Fault::Corrupt,
        Fault::PayloadCorrupt,
    ] {
        let (_directory, mut rows) = gather()?;
        rows.insert(
            ("cell".into(), 1, 1),
            vec![1, 2, 3],
            Priority::Preferred,
            Duplicates::Last,
        )?;
        let mut rows = rows.finish()?;
        let live = Arc::default();
        rows.payload = Some(wrap(
            rows.payload.take().unwrap(),
            fault,
            &live,
            &rows.cancellation(),
        ));
        let error = rows
            .next_batch()
            .expect_err("damaged original payload returned success");
        if matches!(fault, Fault::PayloadCorrupt) {
            assert!(error.to_string().contains("payload checksum mismatch"));
        }
        drop(rows);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[test]
fn explicit_cancellation_refuses_staging_sorting_and_batch_reads() -> Result<()> {
    let (_directory, mut rows) = gather()?;
    rows.cancellation().store(true, Ordering::Release);
    assert!(rows
        .insert(
            ("cell".into(), 1, 1),
            vec![1],
            Priority::Preferred,
            Duplicates::Last
        )
        .is_err());
    assert!(rows.finish().is_err());
    let (_directory, mut rows) = gather()?;
    rows.insert(
        ("cell".into(), 1, 1),
        vec![1],
        Priority::Preferred,
        Duplicates::Last,
    )?;
    let mut rows = rows.finish()?;
    rows.cancellation().store(true, Ordering::Release);
    assert!(rows.next_batch().is_err());
    Ok(())
}

#[test]
fn late_index_damage_refuses_after_a_successful_prefix() -> Result<()> {
    let (_directory, mut rows) = gather()?;
    for id in 0..3000 {
        rows.insert(
            ("cell".into(), 1, id),
            vec![id as u8],
            Priority::Preferred,
            Duplicates::Last,
        )?;
    }
    let mut rows = rows.finish()?;
    let live = Arc::default();
    rows.index = Some(wrap(
        rows.index.take().unwrap(),
        Fault::LateCorrupt,
        &live,
        &rows.cancellation(),
    ));
    let mut delivered = 0;
    loop {
        match rows.next_batch() {
            Ok(batch) => {
                assert!(!batch.is_empty(), "late corruption was silently ignored");
                delivered += batch.len();
            }
            Err(error) => {
                assert!(error.to_string().contains("gather"));
                break;
            }
        }
    }
    assert!(delivered > 0 && delivered < 3000);
    drop(rows);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn unavailable_scratch_refuses_the_obligation_without_creating_a_partial_index() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let occupied = directory.path().join("occupied");
    DirectFileSystem.write(&occupied, b"keep")?;
    let mut rows = Rows::new(Arc::new(DirectFileSystem), occupied.join("scratch"));
    let error = rows
        .insert(
            ("cell".into(), 1, 1),
            vec![1],
            Priority::Preferred,
            Duplicates::Last,
        )
        .expect_err("unavailable scratch returned success");
    assert!(error.downcast_ref::<std::io::Error>().is_some());
    assert!(rows.payload.is_none() && rows.index.is_none());
    assert_eq!(DirectFileSystem.read(&occupied)?, b"keep");
    Ok(())
}

struct SubstitutedIndex {
    file: HostFile,
    offset: u64,
    replacement: Vec<u8>,
}

impl cellar_ltx::HostFileIo for SubstitutedIndex {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut bytes = self.file.read_exact_at(offset, len)?;
        let first = offset.max(self.offset);
        let end = (offset + len as u64).min(self.offset + self.replacement.len() as u64);
        if first < end {
            bytes[(first - offset) as usize..(end - offset) as usize].copy_from_slice(
                &self.replacement[(first - self.offset) as usize..(end - self.offset) as usize],
            );
        }
        Ok(bytes)
    }

    fn file_len(&mut self) -> std::io::Result<u64> {
        self.file.file_len()
    }

    fn sync_all(&mut self) -> std::io::Result<()> {
        self.file.sync_all()
    }
}

fn descriptors(rows: &mut SortedRows) -> Result<Vec<(u64, Vec<u8>)>> {
    let file = rows.index.as_mut().unwrap();
    let mut position = HEADER_BYTES as u64;
    let mut records = Vec::new();
    while position < rows.index_bytes {
        let length = u32::from_le_bytes(exact(file, position, 4)?.try_into().unwrap()) as usize;
        let bytes = exact(file, position, length + 4)?;
        Meta::decode(&bytes)?;
        records.push((position, bytes));
        position += (length + 4) as u64;
    }
    Ok(records)
}

fn substitute(rows: &mut SortedRows, offset: u64, replacement: Vec<u8>) {
    rows.index = Some(HostFile::from_io(SubstitutedIndex {
        file: rows.index.take().unwrap(),
        offset,
        replacement,
    }));
}

fn refuse_descriptor_duplication(count: u64) -> Result<()> {
    let (_directory, mut rows) = gather()?;
    for txid in 1..=count {
        rows.insert(
            ("cell".into(), 1, txid),
            vec![1],
            Priority::Preferred,
            Duplicates::Last,
        )?;
    }
    let mut rows = rows.finish()?;
    let records = descriptors(&mut rows)?;
    let last = records.len() - 1;
    // Copy a whole real descriptor, including its valid SHA, onto its
    // predecessor. Count/length/header/individual checksums remain valid.
    substitute(&mut rows, records[last - 1].0, records[last].1.clone());
    let result = rows.next_batch();
    assert!(
        result.is_err(),
        "duplicate descriptor skipped an earlier row before upload: accepted TXIDs {:?}",
        result
            .as_ref()
            .map(|batch| batch.keys().map(|key| key.2).collect::<Vec<_>>())
    );
    Ok(())
}

#[test]
fn valid_descriptor_duplication_cannot_skip_a_row() -> Result<()> {
    refuse_descriptor_duplication(2)
}

#[test]
fn valid_descriptor_duplication_cannot_cross_a_batch_watermark() -> Result<()> {
    refuse_descriptor_duplication(129)
}

#[test]
fn valid_foreign_descriptor_cannot_change_the_original_spool_identity() -> Result<()> {
    let (_directory, mut rows) = gather()?;
    for txid in 1..=2 {
        rows.insert(
            ("cell".into(), 1, txid),
            vec![1],
            Priority::Preferred,
            Duplicates::Last,
        )?;
    }
    let mut rows = rows.finish()?;
    let records = descriptors(&mut rows)?;
    let mut replacement = Meta::decode(&records[1].1)?;
    replacement.key.2 = 3;
    // Same payload digest and offsets, same sorted shape and encoded length,
    // but a valid descriptor from a different logical row.
    substitute(&mut rows, records[1].0, replacement.encode());
    assert!(
        rows.next_batch().is_err(),
        "foreign identity reached an upload batch"
    );
    Ok(())
}

#[test]
fn valid_foreign_descriptor_cannot_hide_a_missing_row_as_a_duplicate_key() -> Result<()> {
    let (_directory, mut rows) = gather()?;
    for txid in 1..=3 {
        rows.insert(
            ("cell".into(), 1, txid),
            vec![1],
            Priority::Preferred,
            Duplicates::Last,
        )?;
    }
    let mut rows = rows.finish()?;
    let records = descriptors(&mut rows)?;
    let first = Meta::decode(&records[0].1)?;
    let mut replacement = Meta::decode(&records[1].1)?;
    replacement.key = first.key;
    replacement.ordinal = first.ordinal;
    // A second descriptor for key 1 is ordered by its later original offset.
    // Key-only deduplication must not hide that it replaced original key 2.
    substitute(&mut rows, records[1].0, replacement.encode());
    assert!(
        rows.next_batch().is_err(),
        "key-only deduplication hid original row 2"
    );
    Ok(())
}
