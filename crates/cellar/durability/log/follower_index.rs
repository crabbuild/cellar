//! Derived, bounded-memory inventory of verified committed batches.
//!
//! Small inventories stay in memory. Larger ones use sorted runs in anonymous
//! injected scratch files and a merge sort with fixed-size input/output buffers.
//! Scratch is never durability evidence: each record is checksummed, and reading
//! a payload again must verify its original digest and state against this index.

use super::FollowerState;
use anyhow::{ensure, Context, Result};
use cellar_ltx::{FileSystem, HostFile};
use sha2::{Digest, Sha256};
use std::path::Path;

const RECORD_BYTES: usize = 120;
const CHUNK_RECORDS: usize = 128;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct BatchMeta {
    pub first: u64,
    pub last: u64,
    pub state: FollowerState,
    pub bytes: u64,
    pub digest: [u8; 32],
}

impl BatchMeta {
    fn key(self, order: Order) -> (u64, u64, u64) {
        match order {
            Order::Range => (self.first, self.last, 0),
            Order::Candidate => (u64::MAX - self.state.end, self.first, self.last),
        }
    }

    fn encode(self, out: &mut Vec<u8>) {
        let start = out.len();
        for value in [
            self.first,
            self.last,
            self.state.fragment_epoch,
            self.state.base,
            self.state.end,
            self.state.sealed_to,
            self.bytes,
        ] {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&self.digest);
        let checksum = Sha256::digest(&out[start..]);
        out.extend_from_slice(&checksum);
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == RECORD_BYTES, "short follower index record");
        ensure!(
            Sha256::digest(&bytes[..88]).as_slice() == &bytes[88..],
            "follower index checksum mismatch"
        );
        let value = |offset| {
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed record"))
        };
        Ok(Self {
            first: value(0),
            last: value(8),
            state: FollowerState {
                fragment_epoch: value(16),
                base: value(24),
                end: value(32),
                sealed_to: value(40),
            },
            bytes: value(48),
            digest: bytes[56..88].try_into().expect("fixed digest"),
        })
    }
}

pub(super) struct Builder<'a> {
    filesystem: &'a dyn FileSystem,
    directory: &'a Path,
    pending: Vec<BatchMeta>,
    spill: Option<HostFile>,
    len: usize,
    order: Order,
}

#[derive(Clone, Copy)]
enum Order {
    Range,
    Candidate,
}

impl<'a> Builder<'a> {
    pub fn new(filesystem: &'a dyn FileSystem, directory: &'a Path) -> Self {
        Self {
            filesystem,
            directory,
            pending: Vec::with_capacity(CHUNK_RECORDS),
            spill: None,
            len: 0,
            order: Order::Range,
        }
    }

    pub fn candidates(filesystem: &'a dyn FileSystem, directory: &'a Path) -> Self {
        Self {
            order: Order::Candidate,
            ..Self::new(filesystem, directory)
        }
    }

    pub fn push(&mut self, record: BatchMeta) -> Result<()> {
        if self.pending.len() == CHUNK_RECORDS {
            if self.spill.is_none() {
                self.spill = Some(self.filesystem.temporary_file(Some(self.directory))?);
            }
            self.flush()?;
        }
        self.len = self
            .len
            .checked_add(1)
            .context("follower index count overflow")?;
        self.pending.push(record);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.pending.sort_by_key(|record| record.key(self.order));
        write_records(
            self.spill.as_mut().context("missing follower scratch")?,
            &self.pending,
        )?;
        self.pending.clear();
        Ok(())
    }

    pub fn finish(mut self) -> Result<Index> {
        if self.spill.is_none() {
            self.pending.sort_by_key(|record| record.key(self.order));
            return Ok(Index::Memory(self.pending));
        }
        self.flush()?;
        let mut source = self.spill.take().expect("spill exists");
        let mut width = CHUNK_RECORDS;
        check_length(&mut source, self.len)?;
        while width < self.len {
            let mut destination = self.filesystem.temporary_file(Some(self.directory))?;
            let mut start = 0;
            let mut output = Vec::with_capacity(CHUNK_RECORDS);
            while start < self.len {
                let middle = start.saturating_add(width).min(self.len);
                let end = middle.saturating_add(width).min(self.len);
                let mut left = Run::new(start, middle);
                let mut right = Run::new(middle, end);
                let mut a = left.next(&mut source)?;
                let mut b = right.next(&mut source)?;
                while a.is_some() || b.is_some() {
                    let take_left = match (a, b) {
                        (Some(a), Some(b)) => a.key(self.order) <= b.key(self.order),
                        (Some(_), None) => true,
                        _ => false,
                    };
                    output.push(if take_left {
                        let record = a.expect("left record exists");
                        a = left.next(&mut source)?;
                        record
                    } else {
                        let record = b.expect("right record exists");
                        b = right.next(&mut source)?;
                        record
                    });
                    if output.len() == CHUNK_RECORDS {
                        write_records(&mut destination, &output)?;
                        output.clear();
                    }
                }
                start = end;
            }
            write_records(&mut destination, &output)?;
            check_length(&mut source, self.len)?;
            check_length(&mut destination, self.len)?;
            source = destination;
            width = width
                .checked_mul(2)
                .context("follower index run overflow")?;
        }
        Ok(Index::Disk {
            file: source,
            len: self.len,
            cached_start: 0,
            cached: Vec::new(),
        })
    }
}

pub(super) enum Index {
    Memory(Vec<BatchMeta>),
    Disk {
        file: HostFile,
        len: usize,
        cached_start: usize,
        cached: Vec<BatchMeta>,
    },
}

impl Index {
    pub fn len(&self) -> usize {
        match self {
            Self::Memory(records) => records.len(),
            Self::Disk { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&mut self, index: usize) -> Result<BatchMeta> {
        ensure!(
            index < self.len(),
            "follower index position outside inventory"
        );
        match self {
            Self::Memory(records) => Ok(records[index]),
            Self::Disk {
                file,
                len,
                cached_start,
                cached,
            } => {
                check_length(file, *len)?;
                if index < *cached_start || index >= *cached_start + cached.len() {
                    *cached_start = index / CHUNK_RECORDS * CHUNK_RECORDS;
                    *cached = read_records(
                        file,
                        *cached_start,
                        (*len - *cached_start).min(CHUNK_RECORDS),
                    )?;
                    check_length(file, *len)?;
                }
                Ok(cached[index - *cached_start])
            }
        }
    }
}

struct Run {
    position: usize,
    end: usize,
    records: std::vec::IntoIter<BatchMeta>,
}

impl Run {
    fn new(position: usize, end: usize) -> Self {
        Self {
            position,
            end,
            records: Vec::new().into_iter(),
        }
    }

    fn next(&mut self, file: &mut HostFile) -> Result<Option<BatchMeta>> {
        if let Some(record) = self.records.next() {
            return Ok(Some(record));
        }
        if self.position == self.end {
            return Ok(None);
        }
        let count = (self.end - self.position).min(CHUNK_RECORDS);
        self.records = read_records(file, self.position, count)?.into_iter();
        self.position += count;
        Ok(self.records.next())
    }
}

fn check_length(file: &mut HostFile, records: usize) -> Result<()> {
    let expected = u64::try_from(records)?
        .checked_mul(RECORD_BYTES as u64)
        .context("follower index size overflow")?;
    ensure!(
        file.file_len()? == expected,
        "follower index length changed"
    );
    Ok(())
}

fn read_records(file: &mut HostFile, start: usize, count: usize) -> Result<Vec<BatchMeta>> {
    ensure!(
        count <= CHUNK_RECORDS,
        "follower index read exceeds chunk bound"
    );
    let offset = u64::try_from(start)?
        .checked_mul(RECORD_BYTES as u64)
        .context("follower index offset overflow")?;
    let length = count * RECORD_BYTES;
    let bytes = file.read_exact_at(offset, length)?;
    ensure!(bytes.len() == length, "short follower index read");
    bytes
        .as_chunks::<RECORD_BYTES>()
        .0
        .iter()
        .map(|record| BatchMeta::decode(record))
        .collect()
}

fn write_records(file: &mut HostFile, records: &[BatchMeta]) -> Result<()> {
    ensure!(
        records.len() <= CHUNK_RECORDS,
        "follower index write exceeds chunk bound"
    );
    let mut bytes = Vec::with_capacity(records.len() * RECORD_BYTES);
    for record in records {
        record.encode(&mut bytes);
    }
    file.write_all(&bytes)?;
    Ok(())
}

#[cfg(test)]
#[path = "follower_index_tests.rs"]
mod tests;
