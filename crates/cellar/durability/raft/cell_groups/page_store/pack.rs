// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded local containers; replicated page bytes and digests stay unchanged.

use super::{digest, ManifestError, MAX_BATCH_PAGES, MAX_PAGE_BYTES};
use cellar_ltx::HostFile;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Mutex},
};

pub(super) const MAGIC: &[u8; 8] = b"CLRCMP01";
const FOOTER_MAGIC: &[u8; 8] = b"CLRCMS01";
pub(super) const HEADER_BYTES: u64 = 8;
pub(super) const FRAME_BYTES: u64 = 36;
pub(super) const FOOTER_BYTES: u64 = 52;
pub(super) const MAX_PACK_BYTES: u64 =
    HEADER_BYTES + MAX_BATCH_PAGES as u64 * (FRAME_BYTES + MAX_PAGE_BYTES as u64) + FOOTER_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Record {
    pub key: [u8; 32],
    pub offset: u64,
    pub length: u32,
}

impl Record {
    pub fn end(self) -> u64 {
        self.offset + FRAME_BYTES + u64::from(self.length)
    }

    pub fn header(self) -> [u8; FRAME_BYTES as usize] {
        let mut bytes = [0; FRAME_BYTES as usize];
        bytes[..32].copy_from_slice(&self.key);
        bytes[32..].copy_from_slice(&self.length.to_be_bytes());
        bytes
    }
}

pub(super) struct Pack {
    pub id: [u8; 32],
    pub path: PathBuf,
    pub length: u64,
    pub records: Vec<Record>,
    // A completed barrier from this owner, never an integrity cache.
    pub persisted: AtomicBool,
}

pub(super) struct Pending {
    pub path: PathBuf,
    // Preserve the store's Sync contract while retaining a Send-only host
    // writer. All access uses get_mut/into_inner under exclusive store ownership;
    // no synchronous guard crosses an await or escapes to a reader.
    pub file: Mutex<HostFile>,
    pub records: Vec<Record>,
    pub data_end: u64,
}

pub(super) fn identity(records: &[Record], data_end: u64) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar manifest pack 1\0");
    hash.update((records.len() as u32).to_be_bytes());
    hash.update(data_end.to_be_bytes());
    for record in records {
        hash.update(record.key);
        hash.update(record.offset.to_be_bytes());
        hash.update(record.length.to_be_bytes());
    }
    hash.finalize().into()
}

pub(super) fn footer(records: &[Record], data_end: u64) -> [u8; FOOTER_BYTES as usize] {
    let mut bytes = [0; FOOTER_BYTES as usize];
    bytes[..8].copy_from_slice(FOOTER_MAGIC);
    bytes[8..12].copy_from_slice(&(records.len() as u32).to_be_bytes());
    bytes[12..20].copy_from_slice(&data_end.to_be_bytes());
    bytes[20..].copy_from_slice(&identity(records, data_end));
    bytes
}

/// Startup reads only bounded framing. Selected payloads are authenticated on
/// every use, including when another record in the same file was already synced.
pub(super) fn open(
    id: [u8; 32],
    path: PathBuf,
    length: u64,
    file: &mut HostFile,
) -> Result<Pack, ManifestError> {
    if !(HEADER_BYTES + FRAME_BYTES + 1 + FOOTER_BYTES..=MAX_PACK_BYTES).contains(&length)
        || file.file_len()? != length
        || file.read_exact_at(0, HEADER_BYTES as usize)? != MAGIC
    {
        return Err(ManifestError::Invalid("manifest pack header/length"));
    }
    let data_end = length - FOOTER_BYTES;
    let tail = file.read_exact_at(data_end, FOOTER_BYTES as usize)?;
    if tail.len() != FOOTER_BYTES as usize
        || tail[..8] != *FOOTER_MAGIC
        || u64::from_be_bytes(tail[12..20].try_into().unwrap()) != data_end
        || tail[20..] != id
    {
        return Err(ManifestError::Invalid("manifest pack footer"));
    }
    let count = u32::from_be_bytes(tail[8..12].try_into().unwrap()) as usize;
    if !(1..=MAX_BATCH_PAGES).contains(&count) {
        return Err(ManifestError::Invalid("manifest pack record count"));
    }
    let mut records = Vec::with_capacity(count);
    let mut offset = HEADER_BYTES;
    for _ in 0..count {
        if offset + FRAME_BYTES > data_end {
            return Err(ManifestError::Invalid("manifest pack frame"));
        }
        let header = file.read_exact_at(offset, FRAME_BYTES as usize)?;
        if header.len() != FRAME_BYTES as usize {
            return Err(ManifestError::Invalid("manifest pack frame length"));
        }
        let record = Record {
            key: header[..32].try_into().unwrap(),
            offset,
            length: u32::from_be_bytes(header[32..].try_into().unwrap()),
        };
        if record.length == 0
            || record.length as usize > MAX_PAGE_BYTES
            || record.end() > data_end
            || records.iter().any(|r: &Record| r.key == record.key)
        {
            return Err(ManifestError::Invalid("manifest pack record"));
        }
        offset = record.end();
        records.push(record);
    }
    if offset != data_end || identity(&records, data_end) != id {
        return Err(ManifestError::Invalid("manifest pack index"));
    }
    Ok(Pack {
        id,
        path,
        length,
        records,
        persisted: AtomicBool::new(false),
    })
}

pub(super) fn read(
    file: &mut HostFile,
    file_length: u64,
    record: Record,
) -> Result<Vec<u8>, ManifestError> {
    if file.file_len()? != file_length || record.end() > file_length {
        return Err(ManifestError::Invalid("manifest pack length"));
    }
    // One bounded physical read, preserving changed-path I/O bounds. Frame
    // metadata is checked as well as the complete logical page digest.
    let mut bytes =
        file.read_exact_at(record.offset, FRAME_BYTES as usize + record.length as usize)?;
    if bytes.len() != FRAME_BYTES as usize + record.length as usize
        || bytes[..FRAME_BYTES as usize] != record.header()
        || digest(&bytes[FRAME_BYTES as usize..]) != record.key
    {
        return Err(ManifestError::Invalid("manifest page digest/frame"));
    }
    bytes.drain(..FRAME_BYTES as usize);
    Ok(bytes)
}
