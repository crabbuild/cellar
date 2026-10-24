// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_ltx::{DirectFileSystem, HostFileIo};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

fn record(sequence: u64) -> BatchMeta {
    BatchMeta {
        first: sequence,
        last: sequence,
        state: FollowerState {
            fragment_epoch: 7,
            base: 0,
            end: sequence,
            sealed_to: 8,
        },
        bytes: sequence + 100,
        digest: [sequence as u8; 32],
    }
}

#[test]
fn sorted_inventory_preserves_every_descriptor_across_partial_runs() -> Result<()> {
    let directory = tempfile::tempdir()?;
    for count in [0, 1, 128, 129, 257, 777] {
        let mut builder = Builder::new(&DirectFileSystem, directory.path());
        // Numeric order differs from the canonical filenames' lexical order.
        for sequence in (1..=count).rev() {
            builder.push(record(sequence as u64))?;
        }
        let mut index = builder.finish()?;
        assert_eq!(index.len(), count);
        assert_eq!(matches!(index, Index::Disk { .. }), count > CHUNK_RECORDS);
        for position in (0..count).rev().chain(0..count) {
            assert!(index.get(position)? == record(position as u64 + 1));
        }
        assert!(index.get(count).is_err());
        // Scratch has no discoverable filename, even while its handle lives.
        assert!(DirectFileSystem.read_dir(directory.path())?.is_empty());
        drop(index);
        assert!(DirectFileSystem.read_dir(directory.path())?.is_empty());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Read,
    Write,
    Short,
    Length,
    Corrupt,
}

struct Observed {
    file: HostFile,
    fault: Fault,
    read_peak: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl HostFileIo for Observed {
    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.read_peak.fetch_max(len, Ordering::SeqCst);
        if matches!(self.fault, Fault::Read) {
            return Err(io::Error::other("injected scratch read failure"));
        }
        let mut bytes = self.file.read_exact_at(offset, len)?;
        match self.fault {
            Fault::Corrupt if !bytes.is_empty() => bytes[0] ^= 1,
            Fault::Short => {
                bytes.pop();
            }
            _ => {}
        }
        Ok(bytes)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if matches!(self.fault, Fault::Write) {
            return Err(io::Error::other("injected scratch write failure"));
        }
        self.file.write_all(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        Ok(self.file.file_len()? + u64::from(matches!(self.fault, Fault::Length)))
    }
}

impl Drop for Observed {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn merge_refuses_scratch_faults_and_releases_failed_inputs() -> Result<()> {
    let directory = tempfile::tempdir()?;
    for fault in [
        Fault::None,
        Fault::Read,
        Fault::Write,
        Fault::Short,
        Fault::Length,
        Fault::Corrupt,
    ] {
        let peak = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut builder = Builder::new(&DirectFileSystem, directory.path());
        for sequence in (1..=257).rev() {
            builder.push(record(sequence))?;
        }
        builder.spill = Some(HostFile::from_io(Observed {
            file: builder.spill.take().expect("inventory spilled"),
            fault,
            read_peak: peak.clone(),
            dropped: dropped.clone(),
        }));
        let result = builder.finish();
        assert_eq!(result.is_ok(), matches!(fault, Fault::None));
        assert!(peak.load(Ordering::SeqCst) <= CHUNK_RECORDS * RECORD_BYTES);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(DirectFileSystem.read_dir(directory.path())?.is_empty());
    }
    Ok(())
}

#[test]
fn indexed_reads_refuse_damage_in_later_chunks() -> Result<()> {
    let directory = tempfile::tempdir()?;
    for fault in [Fault::Read, Fault::Short, Fault::Length, Fault::Corrupt] {
        let mut builder = Builder::new(&DirectFileSystem, directory.path());
        for sequence in 1..=257 {
            builder.push(record(sequence))?;
        }
        let index = builder.finish()?;
        let Index::Disk { file, len, .. } = index else {
            panic!("inventory must spill")
        };
        let mut damaged = Index::Disk {
            file: HostFile::from_io(Observed {
                file,
                fault,
                read_peak: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::new(AtomicUsize::new(0)),
            }),
            len,
            cached_start: 0,
            cached: Vec::new(),
        };
        assert!(damaged.get(256).is_err());
    }
    Ok(())
}

#[test]
fn unavailable_scratch_cannot_truncate_an_inventory() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let unavailable = directory.path().join("absent");
    let mut builder = Builder::new(&DirectFileSystem, &unavailable);
    for sequence in 1..=128 {
        builder.push(record(sequence))?;
    }
    assert!(builder.push(record(129)).is_err());
    assert_eq!(builder.len, 128);
    assert_eq!(builder.pending.len(), 128);
    assert!(DirectFileSystem.read_dir(directory.path())?.is_empty());
    Ok(())
}
