// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fixed canonical entry framing bounds allocation before decoding. Includes
//! context and sync_log; no unknown/repeated protobuf fields are accepted.
use super::*;
use protobuf::Message as _;
use raft::prelude::{Entry, EntryType};

const ENTRY_DOMAIN: &[u8] = b"cellar-cell-group-archive-entry-1\0";
const FIXED: usize = ENTRY_DOMAIN.len() + 26;

pub(super) fn length(entry: &Entry) -> Result<usize> {
    if entry.index == 0
        || entry.term == 0
        || entry.get_unknown_fields().iter().next().is_some()
        || !matches!(
            entry.get_entry_type(),
            EntryType::EntryNormal | EntryType::EntryConfChangeV2
        )
    {
        return Err(CellGroupArchiveError::Invalid("archive entry metadata"));
    }
    entry
        .data
        .len()
        .checked_add(entry.context.len())
        .and_then(|n| n.checked_add(FIXED))
        .ok_or(CellGroupArchiveError::Limit)
}

pub(super) fn encode(entry: &Entry, limit: usize) -> Result<Vec<u8>> {
    let length = length(entry)?;
    if length > limit
        || entry.data.len() > u32::MAX as usize
        || entry.context.len() > u32::MAX as usize
    {
        return Err(CellGroupArchiveError::Limit);
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(ENTRY_DOMAIN);
    bytes.extend_from_slice(&entry.index.to_be_bytes());
    bytes.extend_from_slice(&entry.term.to_be_bytes());
    bytes.push(entry.get_entry_type() as u8);
    bytes.push(u8::from(entry.sync_log));
    bytes.extend_from_slice(&(entry.context.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&(entry.data.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&entry.context);
    bytes.extend_from_slice(&entry.data);
    Ok(bytes)
}

pub(super) fn decode(bytes: &[u8], limit: usize) -> Result<Entry> {
    if bytes.len() < FIXED || bytes.len() > limit || !bytes.starts_with(ENTRY_DOMAIN) {
        return Err(CellGroupArchiveError::Invalid("archive entry framing"));
    }
    let at = ENTRY_DOMAIN.len();
    // Every fixed slice below lies in the already checked header.
    let index = u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap());
    let term = u64::from_be_bytes(bytes[at + 8..at + 16].try_into().unwrap());
    let entry_type = match bytes[at + 16] {
        0 => EntryType::EntryNormal,
        2 => EntryType::EntryConfChangeV2,
        _ => return Err(CellGroupArchiveError::Invalid("archive entry type")),
    };
    let sync_log = match bytes[at + 17] {
        0 => false,
        1 => true,
        _ => return Err(CellGroupArchiveError::Invalid("archive entry sync bit")),
    };
    let context = u32::from_be_bytes(bytes[at + 18..at + 22].try_into().unwrap()) as usize;
    let data = u32::from_be_bytes(bytes[at + 22..at + 26].try_into().unwrap()) as usize;
    if context.checked_add(data).and_then(|n| n.checked_add(FIXED)) != Some(bytes.len()) {
        return Err(CellGroupArchiveError::Invalid("archive entry lengths"));
    }
    let entry = Entry {
        index,
        term,
        entry_type,
        sync_log,
        context: bytes[FIXED..FIXED + context].to_vec().into(),
        data: bytes[FIXED + context..].to_vec().into(),
        ..Default::default()
    };
    length(&entry)?;
    Ok(entry)
}
