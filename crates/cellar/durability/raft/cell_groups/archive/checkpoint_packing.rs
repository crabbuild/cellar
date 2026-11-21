// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded archive objects retain the original physical wire-part boundaries.
use super::{wire, CellGroupCheckpointError, CHUNK_BYTES};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, CellGroupCheckpointError>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fragment {
    pub part: wire::Part,
    pub offset: u64,
    pub length: u64,
    pub bytes: u64,
}

pub(super) struct Packed {
    pub parts: Vec<Fragment>,
    pub bytes: Vec<u8>,
}

pub(super) struct Packer {
    pending: Packed,
    parts: usize,
    bytes: u64,
    max_parts: usize,
    max_bytes: u64,
}

impl Packer {
    pub fn new(max_parts: usize, max_bytes: u64) -> Self {
        Self {
            pending: Packed {
                parts: Vec::new(),
                bytes: Vec::new(),
            },
            parts: 0,
            bytes: 0,
            max_parts,
            max_bytes,
        }
    }

    pub fn push(
        &mut self,
        chunk: wire::Chunk,
        emit: &mut impl FnMut(Packed) -> Result<()>,
    ) -> Result<()> {
        self.parts = self
            .parts
            .checked_add(1)
            .filter(|n| *n <= self.max_parts)
            .ok_or(CellGroupCheckpointError::Limit)?;
        self.bytes = self
            .bytes
            .checked_add(chunk.bytes.len() as u64)
            .filter(|n| *n <= self.max_bytes)
            .ok_or(CellGroupCheckpointError::Limit)?;
        if chunk.bytes.is_empty() || chunk.bytes.len() > CHUNK_BYTES {
            return Err(CellGroupCheckpointError::Limit);
        }
        // Keep cell payloads independent. A shared tree page may precede its
        // image, but the next tree page starts a new object's dependency set.
        let same = self
            .pending
            .parts
            .last()
            .is_none_or(|previous| compatible(&previous.part, &chunk.part));
        if self.pending.bytes.len() + chunk.bytes.len() > CHUNK_BYTES || !same {
            self.finish(emit)?;
        }
        self.pending.parts.push(Fragment {
            part: chunk.part,
            offset: chunk.offset,
            length: chunk.length,
            bytes: chunk.bytes.len() as u64,
        });
        if self.pending.bytes.capacity() == 0 {
            self.pending.bytes = Vec::with_capacity(CHUNK_BYTES);
        }
        self.pending.bytes.extend_from_slice(&chunk.bytes);
        Ok(())
    }

    pub fn finish(&mut self, emit: &mut impl FnMut(Packed) -> Result<()>) -> Result<()> {
        if !self.pending.parts.is_empty() {
            emit(Packed {
                parts: std::mem::take(&mut self.pending.parts),
                bytes: std::mem::take(&mut self.pending.bytes),
            })?;
        }
        Ok(())
    }
}

fn compatible(before: &wire::Part, after: &wire::Part) -> bool {
    use wire::Part::*;
    match (before, after) {
        (Header, Header) | (Page { .. }, Page { .. }) => true,
        (Header, _) | (_, Header) => false,
        (Page { .. }, _) => true,
        (_, Page { .. }) => false,
        (
            Materialized { cell: a } | ExtentManifest { cell: a } | Extent { cell: a, .. },
            Materialized { cell: b } | ExtentManifest { cell: b } | Extent { cell: b, .. },
        ) => a == b,
    }
}

/// The index validates every span before any object is fetched. Conversion
/// still uses checked bounds so malformed metadata cannot trigger a panic.
pub(super) fn unpack(parts: Vec<Fragment>, bytes: Vec<u8>) -> Result<Vec<wire::Chunk>> {
    let mut offset = 0usize;
    let mut chunks = Vec::with_capacity(parts.len());
    for part in parts {
        let length = usize::try_from(part.bytes).map_err(|_| CellGroupCheckpointError::Limit)?;
        let end = offset
            .checked_add(length)
            .ok_or(CellGroupCheckpointError::Limit)?;
        let body = bytes
            .get(offset..end)
            .ok_or(CellGroupCheckpointError::Invalid("packed fragment bounds"))?;
        chunks.push(wire::Chunk {
            part: part.part,
            offset: part.offset,
            length: part.length,
            bytes: body.to_vec(),
        });
        offset = end;
    }
    if offset != bytes.len() {
        return Err(CellGroupCheckpointError::Invalid("packed fragment length"));
    }
    Ok(chunks)
}
