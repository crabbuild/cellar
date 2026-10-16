// Modified for Cellar from Celld.
//! The bundle envelope: a container of verbatim L0 LTX files, never a
//! format.
//!
//! cellar-original, not a Litestream port. Layout: the concatenated raw L0
//! bytes, a binary footer of rows, a CRC64-ISO checksum, the footer length as
//! a little-endian u32, and a four-byte magic. The checksum covers the payload,
//! footer, length and magic. A row is `u16 cell_len | cell utf-8 |
//! u64 cell_epoch | u64 txid | u64 offset | u64 len`, all little-endian.
//! Un-bundling is arithmetic. The inner bytes are exactly what the
//! per-cell writer produces, so byte-level compatibility stays checkable.
//! At rest, the bucket is pure Litestream. A drained bundle leaves no trace
//! that this module ever existed. Nothing else in this crate depends on bundles;
//! deleting this file returns the crate to its upstream shape.

use crate::error::{Error, Result};
use crate::ltx::{Crc64, Header};
use crate::TXID;
use std::collections::BTreeSet;

const MAGIC: &[u8; 4] = b"CLB1";
const TRAILER_SIZE: usize = 16;

fn malformed(what: &str) -> Error {
    Error::Other(format!("bundle: {what}").into())
}

fn split_data_and_footer(bundle: &[u8]) -> Result<(&[u8], &[u8])> {
    if bundle.len() < TRAILER_SIZE {
        return Err(malformed("shorter than its trailer"));
    }
    let (rest, magic) = bundle.split_at(bundle.len() - 4);
    if magic != MAGIC {
        return Err(malformed("bad magic"));
    }
    let (rest, len_bytes) = rest.split_at(rest.len() - 4);
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    let (rest, _) = rest.split_at(rest.len() - 8);
    if rest.len() < len {
        return Err(malformed("footer overruns the object"));
    }
    Ok(rest.split_at(rest.len() - len))
}

fn checksum(body: &[u8], footer_len: u32) -> u64 {
    let mut hash = Crc64::new();
    hash.update(body);
    hash.update(&footer_len.to_le_bytes());
    hash.update(MAGIC);
    hash.sum64()
}

fn validate_identity(cell: &str, epoch: u64, txid: u64, payload: &[u8]) -> Result<()> {
    if cell.is_empty() || epoch == 0 || txid == 0 {
        return Err(malformed("row identity is empty or zero"));
    }
    let header = Header::parse(payload)?;
    header.validate()?;
    if header.min_txid != TXID(txid) || header.max_txid != TXID(txid) {
        return Err(malformed("row transaction does not match its L0 payload"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleRow {
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub offset: u64,
    pub len: u64,
}

impl BundleRow {
    pub fn txid(&self) -> TXID {
        TXID(self.txid)
    }
}

/// One captured L0 segment headed for a bundle.
pub struct BundleEntry {
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

pub fn encode(entries: &[BundleEntry]) -> Result<Vec<u8>> {
    if entries.is_empty() {
        return Err(malformed("no rows"));
    }
    let mut out = Vec::new();
    let mut footer = Vec::new();
    let mut identities = BTreeSet::new();
    for entry in entries {
        validate_identity(&entry.cell, entry.cell_epoch, entry.txid, &entry.bytes)?;
        if !identities.insert((&entry.cell, entry.cell_epoch, entry.txid)) {
            return Err(malformed("duplicate row identity"));
        }
        let cell = entry.cell.as_bytes();
        let cell_len: u16 = cell
            .len()
            .try_into()
            .map_err(|_| malformed("cell name too long"))?;
        footer.extend_from_slice(&cell_len.to_le_bytes());
        footer.extend_from_slice(cell);
        for value in [
            entry.cell_epoch,
            entry.txid,
            out.len() as u64,
            entry.bytes.len() as u64,
        ] {
            footer.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&entry.bytes);
    }
    let len: u32 = footer
        .len()
        .try_into()
        .map_err(|_| malformed("footer too large"))?;
    out.extend_from_slice(&footer);
    let digest = checksum(&out, len);
    out.extend_from_slice(&digest.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(MAGIC);
    Ok(out)
}

/// Verify the complete envelope before returning row metadata. These identities
/// and positions can authorize recovery or deletion, so parsing alone is unsafe.
pub fn decode_rows(bundle: &[u8]) -> Result<Vec<BundleRow>> {
    let (data, mut footer) = split_data_and_footer(bundle)?;
    let checksum_at = bundle.len() - TRAILER_SIZE;
    let expected = u64::from_le_bytes(bundle[checksum_at..checksum_at + 8].try_into().unwrap());
    if checksum(&bundle[..checksum_at], footer.len() as u32) != expected {
        return Err(malformed("checksum mismatch"));
    }
    let mut rows = Vec::new();
    let mut next = 0usize;
    let mut identities = BTreeSet::new();
    while !footer.is_empty() {
        if footer.len() < 2 {
            return Err(malformed("truncated row"));
        }
        let cell_len = u16::from_le_bytes(footer[..2].try_into().unwrap()) as usize;
        footer = &footer[2..];
        if footer.len() < cell_len + 32 {
            return Err(malformed("truncated row"));
        }
        let cell = std::str::from_utf8(&footer[..cell_len])
            .map_err(|_| malformed("cell name not utf-8"))?
            .to_string();
        footer = &footer[cell_len..];
        let mut values = [0_u64; 4];
        for value in &mut values {
            *value = u64::from_le_bytes(footer[..8].try_into().unwrap());
            footer = &footer[8..];
        }
        let row = BundleRow {
            cell,
            cell_epoch: values[0],
            txid: values[1],
            offset: values[2],
            len: values[3],
        };
        let start = usize::try_from(row.offset).map_err(|_| malformed("row overflows"))?;
        let len = usize::try_from(row.len).map_err(|_| malformed("row overflows"))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| malformed("row overflows"))?;
        if start != next || end > data.len() {
            return Err(malformed("row ranges do not partition the payload"));
        }
        validate_identity(&row.cell, row.cell_epoch, row.txid, &data[start..end])?;
        if !identities.insert((row.cell.clone(), row.cell_epoch, row.txid)) {
            return Err(malformed("duplicate row identity"));
        }
        next = end;
        rows.push(row);
    }
    if rows.is_empty() || next != data.len() {
        return Err(malformed("missing rows or unindexed payload bytes"));
    }
    Ok(rows)
}

/// One row's verbatim L0 bytes. The caller supplies a verified row from
/// `decode_rows` or the writer's trusted index. Check bounds and L0 identity
/// without rehashing the entire object for every row in a recovery gather.
pub fn slice<'a>(bundle: &'a [u8], row: &BundleRow) -> Result<&'a [u8]> {
    let (data, _) = split_data_and_footer(bundle)?;
    let start = usize::try_from(row.offset).map_err(|_| malformed("row overflows"))?;
    let len = usize::try_from(row.len).map_err(|_| malformed("row overflows"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| malformed("row overflows"))?;
    if end > data.len() {
        return Err(malformed("row overruns the data section"));
    }
    let payload = &data[start..end];
    validate_identity(&row.cell, row.cell_epoch, row.txid, payload)?;
    Ok(payload)
}
