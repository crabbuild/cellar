//! Derived disk gather for recovery and live-cell folding.
//!
//! Payloads are spooled immediately. Variable-length key metadata is sorted in
//! bounded runs, then merged pairwise through two bounded input caches. Neither
//! the payload history nor a run-directory grows in memory. Scratch is not
//! authority: checksums, exact lengths and counts must verify before use.
use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use cellar_ltx::{FileSystem, HostFile};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

pub(super) type Key = (String, u64, u64);
pub(super) type Batch = BTreeMap<Key, Bytes>;

pub(super) const BATCH_BYTES: usize = 4 << 20;
const BATCH_ROWS: usize = 128;
const RUN_ROWS: usize = 128;
const RUN_BYTES: usize = 1 << 20;
const BUFFER_BYTES: usize = 128 << 10;
const META_FIXED: usize = 107;
const HEADER_BYTES: usize = 52;
const DESCRIPTOR_TAG_BYTES: usize = 32;

/// One reusable row allocation per scan, including an oversized singleton.
/// The scan's spools and outstanding bodies own its lifetime together.
#[derive(Default)]
pub(super) struct BufferPool(Mutex<Option<Vec<u8>>>);

impl BufferPool {
    fn take(self: &Arc<Self>, length: usize) -> PooledBody {
        let mut bytes = self.0.lock().unwrap().take().unwrap_or_default();
        bytes.clear();
        bytes.reserve(length);
        PooledBody {
            bytes,
            pool: self.clone(),
        }
    }
}

struct PooledBody {
    bytes: Vec<u8>,
    pool: Arc<BufferPool>,
}

impl AsRef<[u8]> for PooledBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for PooledBody {
    fn drop(&mut self) {
        let mut cached = self.pool.0.lock().unwrap();
        if cached
            .as_ref()
            .is_none_or(|bytes| bytes.capacity() < self.bytes.capacity())
        {
            *cached = Some(std::mem::take(&mut self.bytes));
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Priority {
    Preferred,
    Fallback,
}

#[derive(Clone, Copy)]
pub(super) enum Duplicates {
    First,
    Last,
}

#[derive(Clone)]
struct Meta {
    key: Key,
    priority: u8,
    ordinal: u64,
    offset: u64,
    length: u64,
    digest: [u8; 32],
}

impl Meta {
    fn order(&self) -> (&str, u64, u64, u8, u64, u64) {
        (
            &self.key.0,
            self.key.1,
            self.key.2,
            self.priority,
            self.ordinal,
            self.offset,
        )
    }

    fn encoded_len(&self) -> usize {
        4 + META_FIXED + self.key.0.len()
    }

    /// The original descriptor identity is also stored beside the payload.
    /// A valid index record cannot rename another row with identical bytes.
    fn descriptor_tag(&self) -> [u8; DESCRIPTOR_TAG_BYTES] {
        let encoded = self.encode();
        encoded[encoded.len() - DESCRIPTOR_TAG_BYTES..]
            .try_into()
            .expect("encoded descriptor checksum")
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(&((self.encoded_len() - 4) as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.key.0.len() as u16).to_le_bytes());
        bytes.extend_from_slice(self.key.0.as_bytes());
        for value in [self.key.1, self.key.2] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.push(self.priority);
        for value in [self.ordinal, self.offset, self.length] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(&self.digest);
        let checksum = Sha256::digest(&bytes);
        bytes.extend_from_slice(&checksum);
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            (4 + META_FIXED..=4 + META_FIXED + u16::MAX as usize).contains(&bytes.len()),
            "invalid gather record size"
        );
        let end = bytes.len() - 32;
        ensure!(
            Sha256::digest(&bytes[..end]).as_slice() == &bytes[end..],
            "gather metadata checksum mismatch"
        );
        let cell_len = u16::from_le_bytes(bytes[4..6].try_into()?) as usize;
        ensure!(
            bytes.len() == 4 + META_FIXED + cell_len,
            "gather cell length mismatch"
        );
        let cell = std::str::from_utf8(&bytes[6..6 + cell_len])?.to_owned();
        let offset = 6 + cell_len;
        let value = |at: usize| {
            u64::from_le_bytes(
                bytes[at..at + 8]
                    .try_into()
                    .expect("checked metadata length"),
            )
        };
        let priority = bytes[offset + 16];
        ensure!(priority <= 1, "invalid gather precedence");
        Ok(Self {
            key: (cell, value(offset), value(offset + 8)),
            priority,
            ordinal: value(offset + 17),
            offset: value(offset + 25),
            length: value(offset + 33),
            digest: bytes[offset + 41..offset + 73].try_into()?,
        })
    }
}

#[derive(Clone, Copy)]
struct Header {
    records: u64,
    bytes: u64,
}

impl Header {
    fn write(self, file: &mut HostFile) -> Result<()> {
        let mut bytes = Vec::with_capacity(HEADER_BYTES);
        bytes.extend_from_slice(b"CRR1");
        bytes.extend_from_slice(&self.records.to_le_bytes());
        bytes.extend_from_slice(&self.bytes.to_le_bytes());
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        file.write_all(&bytes)?;
        Ok(())
    }

    fn read(file: &mut HostFile, offset: u64, end: u64) -> Result<(Self, u64)> {
        ensure!(
            end.saturating_sub(offset) >= HEADER_BYTES as u64,
            "truncated gather run header"
        );
        let bytes = exact(file, offset, HEADER_BYTES)?;
        ensure!(
            &bytes[..4] == b"CRR1" && Sha256::digest(&bytes[..20]).as_slice() == &bytes[20..],
            "invalid gather run header"
        );
        let header = Self {
            records: u64::from_le_bytes(bytes[4..12].try_into()?),
            bytes: u64::from_le_bytes(bytes[12..20].try_into()?),
        };
        ensure!(header.records > 0, "empty gather run");
        ensure!(
            header.bytes
                >= header
                    .records
                    .checked_mul((4 + META_FIXED) as u64)
                    .context("gather run overflow")?,
            "short gather run"
        );
        let next = offset
            .checked_add(HEADER_BYTES as u64)
            .and_then(|v| v.checked_add(header.bytes))
            .context("gather run overflow")?;
        ensure!(next <= end, "truncated gather run");
        Ok((header, next))
    }
}

pub(super) struct Rows {
    filesystem: Arc<dyn FileSystem>,
    directory: PathBuf,
    cancelled: Arc<AtomicBool>,
    payload: Option<HostFile>,
    payload_bytes: u64,
    index: Option<HostFile>,
    index_bytes: u64,
    runs: usize,
    pending: Vec<Meta>,
    pending_bytes: usize,
    ordinal: u64,
    buffers: Arc<BufferPool>,
    guards: Vec<Arc<dyn Send + Sync>>,
}

impl Rows {
    pub fn new(filesystem: Arc<dyn FileSystem>, directory: PathBuf) -> Self {
        Self {
            filesystem,
            directory,
            cancelled: Arc::default(),
            payload: None,
            payload_bytes: 0,
            index: None,
            index_bytes: 0,
            runs: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            ordinal: 0,
            buffers: Arc::default(),
            guards: Vec::new(),
        }
    }

    /// Reservations only retain resources; they grant no proof or authority.
    /// Move them with the spool through blocking work and sorted consumption.
    pub fn keep_alive(&mut self, guard: Arc<dyn Send + Sync>) {
        if !self.guards.iter().any(|held| Arc::ptr_eq(held, &guard)) {
            self.guards.push(guard);
        }
    }

    pub fn reuse_buffers(&mut self, buffers: Arc<BufferPool>) {
        self.buffers = buffers;
    }

    pub fn cancellation(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }

    pub fn with_cancellation(mut self, cancelled: Arc<AtomicBool>) -> Self {
        self.cancelled = cancelled;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.ordinal == 0
    }

    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    fn scratch(&self) -> Result<HostFile> {
        // Explicit node data-root placement avoids a possibly RAM-backed /tmp.
        self.filesystem.create_dir_all(&self.directory)?;
        Ok(self.filesystem.temporary_file(Some(&self.directory))?)
    }

    pub fn insert(
        &mut self,
        key: Key,
        bytes: impl AsRef<[u8]>,
        priority: Priority,
        duplicates: Duplicates,
    ) -> Result<()> {
        let length = u64::try_from(bytes.as_ref().len())?;
        let digest = Sha256::digest(bytes.as_ref()).into();
        self.insert_body(key, length, digest, priority, duplicates, move |file, _| {
            file.write_all(bytes.as_ref())?;
            Ok(())
        })
    }

    /// Copy one immutable range without allocating the complete row. The first
    /// pass binds the copied bytes to their metadata; the second pass must
    /// reproduce that digest before the row enters the gather index.
    pub fn insert_file(
        &mut self,
        key: Key,
        file: &mut HostFile,
        offset: u64,
        length: u64,
        prefix: &[u8],
        crc: &mut cellar_ltx::ltx::Crc64,
    ) -> Result<()> {
        let original_length = file.file_len()?;
        ensure!(
            offset
                .checked_add(length)
                .is_some_and(|end| end <= original_length)
                && prefix.len() as u64 <= length,
            "gather source range outside file"
        );
        let mut digest = Sha256::new();
        let mut position = 0;
        while position < length {
            check_cancelled(&self.cancelled)?;
            let count = usize::try_from((length - position).min(BUFFER_BYTES as u64))?;
            let bytes = exact(file, offset + position, count)?;
            if position == 0 {
                ensure!(bytes.starts_with(prefix), "gather source header changed");
            }
            digest.update(&bytes);
            crc.update(&bytes);
            position += count as u64;
        }
        let digest: [u8; 32] = digest.finalize().into();
        self.insert_body(
            key,
            length,
            digest,
            Priority::Preferred,
            Duplicates::First,
            |destination, cancelled| {
                let mut copied = Sha256::new();
                let mut position = 0;
                while position < length {
                    check_cancelled(cancelled)?;
                    let count = usize::try_from((length - position).min(BUFFER_BYTES as u64))?;
                    let bytes = exact(file, offset + position, count)?;
                    copied.update(&bytes);
                    destination.write_all(&bytes)?;
                    position += count as u64;
                }
                ensure!(
                    <[u8; 32]>::from(copied.finalize()) == digest,
                    "gather source changed while copying"
                );
                check_length(file, original_length)
            },
        )
    }

    fn insert_body(
        &mut self,
        key: Key,
        length: u64,
        digest: [u8; 32],
        priority: Priority,
        duplicates: Duplicates,
        body: impl FnOnce(&mut HostFile, &AtomicBool) -> Result<()>,
    ) -> Result<()> {
        check_cancelled(&self.cancelled)?;
        ensure!(
            u16::try_from(key.0.len()).is_ok(),
            "gather cell exceeds encoded name bound"
        );
        let next = self
            .ordinal
            .checked_add(1)
            .context("gather count overflow")?;
        if self.payload.is_none() {
            self.payload = Some(self.scratch()?);
        }
        let next_payload = self
            .payload_bytes
            .checked_add(DESCRIPTOR_TAG_BYTES as u64)
            .and_then(|end| end.checked_add(length))
            .context("gather payload overflow")?;
        let meta = Meta {
            key,
            priority: match priority {
                Priority::Preferred => 0,
                Priority::Fallback => 1,
            },
            ordinal: match duplicates {
                Duplicates::First => self.ordinal,
                Duplicates::Last => u64::MAX - self.ordinal,
            },
            offset: self.payload_bytes,
            length,
            digest,
        };
        let payload = self.payload.as_mut().expect("payload opened");
        payload.write_all(&meta.descriptor_tag())?;
        body(payload, &self.cancelled)?;
        check_length(payload, next_payload)?;
        self.payload_bytes = next_payload;
        if !self.pending.is_empty()
            && (self.pending.len() == RUN_ROWS
                || self.pending_bytes + meta.encoded_len() > RUN_BYTES)
        {
            self.flush()?;
        }
        self.pending_bytes += meta.encoded_len();
        self.pending.push(meta);
        self.ordinal = next;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        check_cancelled(&self.cancelled)?;
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.index.is_none() {
            self.index = Some(self.scratch()?);
        }
        self.pending
            .sort_unstable_by(|a, b| a.order().cmp(&b.order()));
        let header = Header {
            records: self.pending.len() as u64,
            bytes: self.pending_bytes as u64,
        };
        let index = self.index.as_mut().expect("index opened");
        header.write(index)?;
        let mut output = Output::new(index);
        for meta in self.pending.drain(..) {
            output.push(&meta)?;
        }
        output.finish()?;
        self.index_bytes = self
            .index_bytes
            .checked_add(HEADER_BYTES as u64)
            .and_then(|v| v.checked_add(header.bytes))
            .context("gather index overflow")?;
        check_length(index, self.index_bytes)?;
        self.pending_bytes = 0;
        self.runs = self
            .runs
            .checked_add(1)
            .context("gather run count overflow")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<SortedRows> {
        check_cancelled(&self.cancelled)?;
        self.flush()?;
        if let Some(payload) = &mut self.payload {
            check_length(payload, self.payload_bytes)?;
        }
        while self.runs > 1 {
            let mut source = self.index.take().expect("nonempty index");
            check_length(&mut source, self.index_bytes)?;
            let mut destination = self.scratch()?;
            let mut position = 0;
            let mut runs = 0;
            let mut destination_bytes = 0_u64;
            while position < self.index_bytes {
                check_cancelled(&self.cancelled)?;
                let (left_header, middle) = Header::read(&mut source, position, self.index_bytes)?;
                let (right_header, end) = if middle < self.index_bytes {
                    let (header, end) = Header::read(&mut source, middle, self.index_bytes)?;
                    (Some(header), end)
                } else {
                    (None, middle)
                };
                let header = Header {
                    records: left_header
                        .records
                        .checked_add(right_header.map_or(0, |h| h.records))
                        .context("gather record count overflow")?,
                    bytes: left_header
                        .bytes
                        .checked_add(right_header.map_or(0, |h| h.bytes))
                        .context("gather run bytes overflow")?,
                };
                header.write(&mut destination)?;
                let mut left =
                    Run::new(position + HEADER_BYTES as u64, middle, left_header.records);
                let mut right = Run::new(
                    middle
                        + if right_header.is_some() {
                            HEADER_BYTES as u64
                        } else {
                            0
                        },
                    end,
                    right_header.map_or(0, |h| h.records),
                );
                let mut a = left.next(&mut source)?;
                let mut b = right.next(&mut source)?;
                let mut output = Output::new(&mut destination);
                while a.is_some() || b.is_some() {
                    check_cancelled(&self.cancelled)?;
                    let take_left = match (&a, &b) {
                        (Some(a), Some(b)) => a.order() <= b.order(),
                        (Some(_), None) => true,
                        _ => false,
                    };
                    if take_left {
                        output.push(&a.take().expect("left exists"))?;
                        a = left.next(&mut source)?;
                    } else {
                        output.push(&b.take().expect("right exists"))?;
                        b = right.next(&mut source)?;
                    }
                }
                output.finish()?;
                destination_bytes = destination_bytes
                    .checked_add(HEADER_BYTES as u64)
                    .and_then(|v| v.checked_add(header.bytes))
                    .context("gather index overflow")?;
                position = end;
                runs += 1;
            }
            check_length(&mut source, self.index_bytes)?;
            check_length(&mut destination, destination_bytes)?;
            self.index = Some(destination);
            self.index_bytes = destination_bytes;
            self.runs = runs;
        }
        let run = match &mut self.index {
            Some(index) => {
                let (header, end) = Header::read(index, 0, self.index_bytes)?;
                ensure!(
                    end == self.index_bytes && header.records == self.ordinal,
                    "gather final count mismatch"
                );
                Some(Run::new(HEADER_BYTES as u64, end, header.records))
            }
            None => {
                ensure!(self.ordinal == 0, "gather index absent");
                None
            }
        };
        Ok(SortedRows {
            cancelled: self.cancelled,
            payload: self.payload,
            payload_bytes: self.payload_bytes,
            index: self.index,
            index_bytes: self.index_bytes,
            run,
            records: self.ordinal,
            pending: None,
            previous: None,
            previous_descriptor: None,
            buffers: self.buffers,
            _guards: self.guards,
        })
    }
}

pub(super) struct SortedRows {
    cancelled: Arc<AtomicBool>,
    payload: Option<HostFile>,
    payload_bytes: u64,
    index: Option<HostFile>,
    index_bytes: u64,
    run: Option<Run>,
    records: u64,
    pending: Option<Meta>,
    previous: Option<Key>,
    previous_descriptor: Option<Meta>,
    buffers: Arc<BufferPool>,
    _guards: Vec<Arc<dyn Send + Sync>>,
}

impl SortedRows {
    /// Capacity retained by every entered upload worker, independently of the
    /// caller's lifetime and the backend's stored payload representation.
    pub fn retention(&self) -> Arc<dyn Send + Sync> {
        Arc::new(self._guards.clone())
    }

    pub fn cancellation(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }

    /// A bundle must reject duplicate identities, rather than silently apply
    /// the gather's normal duplicate precedence. Verify all rows before a
    /// caller can use any of them, then restart the checked bounded reader.
    pub fn unique(mut self) -> Result<Self> {
        self.check_files()?;
        let mut count = 0_u64;
        while let Some(meta) = self.next_descriptor()? {
            self.visit_payload(&meta, |_| Ok(()))?;
            self.previous = Some(meta.key);
            count = count.checked_add(1).context("gather count overflow")?;
        }
        self.check_files()?;
        ensure!(count == self.records, "bundle has duplicate row identities");
        self.run = match &mut self.index {
            Some(index) => {
                let (header, end) = Header::read(index, 0, self.index_bytes)?;
                ensure!(
                    end == self.index_bytes && header.records == self.records,
                    "gather rewind count mismatch"
                );
                Some(Run::new(HEADER_BYTES as u64, end, header.records))
            }
            None => {
                ensure!(self.records == 0, "gather index absent");
                None
            }
        };
        self.pending = None;
        self.previous = None;
        self.previous_descriptor = None;
        Ok(self)
    }

    fn verify_descriptor(&mut self, meta: &Meta) -> Result<()> {
        ensure!(
            meta.offset
                .checked_add(DESCRIPTOR_TAG_BYTES as u64)
                .and_then(|end| end.checked_add(meta.length))
                .is_some_and(|end| end <= self.payload_bytes),
            "gather payload outside spool"
        );
        let original = exact(
            self.payload.as_mut().context("gather payload absent")?,
            meta.offset,
            DESCRIPTOR_TAG_BYTES,
        )?;
        ensure!(
            original.as_slice() == meta.descriptor_tag(),
            "gather descriptor does not match original row identity"
        );
        Ok(())
    }

    fn check_files(&mut self) -> Result<()> {
        check_cancelled(&self.cancelled)?;
        if let Some(index) = &mut self.index {
            check_length(index, self.index_bytes)?;
        }
        if let Some(payload) = &mut self.payload {
            check_length(payload, self.payload_bytes)?;
        }
        Ok(())
    }

    fn next_descriptor(&mut self) -> Result<Option<Meta>> {
        loop {
            check_cancelled(&self.cancelled)?;
            let meta = match self.pending.take() {
                Some(meta) => meta,
                None => match self.run.as_mut() {
                    Some(run) => {
                        match run.next(self.index.as_mut().context("gather index absent")?)? {
                            Some(meta) => {
                                if let Some(previous) = &self.previous_descriptor {
                                    ensure!(
                                        previous.order() < meta.order(),
                                        "gather descriptors out of order or duplicated"
                                    );
                                }
                                self.previous_descriptor = Some(meta.clone());
                                meta
                            }
                            None => return Ok(None),
                        }
                    }
                    None => return Ok(None),
                },
            };
            // Check every descriptor, including ignored duplicates and the
            // next batch's lookahead, before returning any upload watermark.
            self.verify_descriptor(&meta)?;
            if self.previous.as_ref() == Some(&meta.key) {
                continue;
            }
            if let Some(previous) = &self.previous {
                ensure!(previous < &meta.key, "gather keys out of order");
            }
            return Ok(Some(meta));
        }
    }

    fn visit_payload(
        &mut self,
        meta: &Meta,
        mut consume: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut digest = Sha256::new();
        let mut position = 0_u64;
        while position < meta.length {
            check_cancelled(&self.cancelled)?;
            let count = usize::try_from((meta.length - position).min(BUFFER_BYTES as u64))?;
            let bytes = exact(
                self.payload.as_mut().context("gather payload absent")?,
                meta.offset + DESCRIPTOR_TAG_BYTES as u64 + position,
                count,
            )?;
            digest.update(&bytes);
            consume(&bytes)?;
            position += count as u64;
        }
        ensure!(
            digest.finalize().as_slice() == meta.digest,
            "gather payload checksum mismatch"
        );
        check_cancelled(&self.cancelled)
    }

    /// Transfer verified rows to another derived spool without allocating a
    /// complete row. A failed reread never returns the partially copied gather.
    pub fn copy_into(
        mut self,
        mut destination: Rows,
        priority: Priority,
        duplicates: Duplicates,
    ) -> Result<Rows> {
        for guard in &self._guards {
            destination.keep_alive(guard.clone());
        }
        self.check_files()?;
        while let Some(meta) = self.next_descriptor()? {
            destination.insert_body(
                meta.key.clone(),
                meta.length,
                meta.digest,
                priority,
                duplicates,
                |file, cancelled| {
                    self.visit_payload(&meta, |bytes| {
                        check_cancelled(cancelled)?;
                        file.write_all(bytes)?;
                        Ok(())
                    })
                },
            )?;
            self.previous = Some(meta.key);
        }
        self.check_files()?;
        Ok(destination)
    }

    pub fn next_batch(&mut self) -> Result<Batch> {
        self.check_files()?;
        let mut batch = Batch::new();
        let mut held = 0_u64;
        while let Some(meta) = self.next_descriptor()? {
            let size = meta
                .length
                .checked_add(meta.key.0.len() as u64 + 64)
                .context("gather batch overflow")?;
            if !batch.is_empty()
                && (held.saturating_add(size) > BATCH_BYTES as u64 || batch.len() == BATCH_ROWS)
            {
                self.pending = Some(meta);
                break;
            }
            let mut body = self.buffers.take(usize::try_from(meta.length)?);
            self.visit_payload(&meta, |bytes| {
                body.bytes.extend_from_slice(bytes);
                Ok(())
            })?;
            held = held.checked_add(size).context("gather batch overflow")?;
            self.previous = Some(meta.key.clone());
            batch.insert(meta.key, Bytes::from_owner(body));
        }
        self.check_files()?;
        Ok(batch)
    }
}

struct Run {
    position: u64,
    end: u64,
    remaining: u64,
    buffer: Vec<u8>,
    buffered: usize,
}

impl Run {
    fn new(position: u64, end: u64, remaining: u64) -> Self {
        Self {
            position,
            end,
            remaining,
            buffer: Vec::new(),
            buffered: 0,
        }
    }

    fn take(&mut self, file: &mut HostFile, length: usize) -> Result<Vec<u8>> {
        ensure!(
            length as u64 <= self.end.saturating_sub(self.position),
            "gather record crosses run"
        );
        let mut bytes = Vec::with_capacity(length);
        while bytes.len() < length {
            if self.buffered == self.buffer.len() {
                let count = usize::try_from((self.end - self.position).min(BUFFER_BYTES as u64))?;
                self.buffer = exact(file, self.position, count)?;
                self.buffered = 0;
            }
            let count = (length - bytes.len()).min(self.buffer.len() - self.buffered);
            bytes.extend_from_slice(&self.buffer[self.buffered..self.buffered + count]);
            self.buffered += count;
            self.position += count as u64;
        }
        Ok(bytes)
    }

    fn next(&mut self, file: &mut HostFile) -> Result<Option<Meta>> {
        if self.remaining == 0 {
            ensure!(self.position == self.end, "gather run has extra bytes");
            return Ok(None);
        }
        let prefix = self.take(file, 4)?;
        let length = u32::from_le_bytes(prefix[..].try_into()?) as usize;
        ensure!(
            (META_FIXED..=META_FIXED + u16::MAX as usize).contains(&length),
            "invalid gather record length"
        );
        let mut bytes = self.take(file, length)?;
        bytes.splice(..0, prefix);
        self.remaining -= 1;
        Ok(Some(Meta::decode(&bytes)?))
    }
}

struct Output<'a> {
    file: &'a mut HostFile,
    buffer: Vec<u8>,
}

impl<'a> Output<'a> {
    fn new(file: &'a mut HostFile) -> Self {
        Self {
            file,
            buffer: Vec::with_capacity(BUFFER_BYTES),
        }
    }

    fn push(&mut self, meta: &Meta) -> Result<()> {
        let bytes = meta.encode();
        if self.buffer.len() + bytes.len() > BUFFER_BYTES {
            self.flush()?;
        }
        self.buffer.extend_from_slice(&bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.file.write_all(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush()
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    ensure!(
        !cancelled.load(Ordering::Acquire),
        "recovery gather cancelled"
    );
    Ok(())
}

fn exact(file: &mut HostFile, offset: u64, length: usize) -> Result<Vec<u8>> {
    let bytes = file.read_exact_at(offset, length)?;
    ensure!(bytes.len() == length, "short gather scratch read");
    Ok(bytes)
}

fn check_length(file: &mut HostFile, expected: u64) -> Result<()> {
    ensure!(
        file.file_len()? == expected,
        "gather scratch length changed"
    );
    Ok(())
}

#[cfg(test)]
#[path = "recovery_rows_tests.rs"]
mod tests;
