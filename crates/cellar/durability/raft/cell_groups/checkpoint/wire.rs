// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned bounded chunk identifiers. Extent identities are resolved from the
//! receiver's already verified manifest, never reconstructed from wire claims.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(crate) enum Part {
    Header,
    Page {
        digest: [u8; 32],
    },
    Materialized {
        cell: String,
    },
    ExtentManifest {
        cell: String,
    },
    Extent {
        cell: String,
        ordinal: u32,
        digest: [u8; 32],
    },
}

pub(crate) struct Chunk {
    pub part: Part,
    pub offset: u64,
    pub length: u64,
    pub bytes: Vec<u8>,
}

impl Chunk {
    pub fn copy(chunk: CellGroupCheckpointChunk<'_>) -> Self {
        let part = match chunk.part {
            CellGroupCheckpointPart::Header => Part::Header,
            CellGroupCheckpointPart::Page(digest) => Part::Page { digest },
            CellGroupCheckpointPart::Materialized(cell) => Part::Materialized {
                cell: cell.as_str().to_owned(),
            },
            CellGroupCheckpointPart::ExtentManifest(cell) => Part::ExtentManifest {
                cell: cell.as_str().to_owned(),
            },
            CellGroupCheckpointPart::Extent { cell, ordinal, id } => Part::Extent {
                cell: cell.as_str().to_owned(),
                ordinal,
                digest: id.bytes(),
            },
        };
        Self {
            part,
            offset: chunk.offset,
            length: chunk.length,
            bytes: chunk.bytes.to_vec(),
        }
    }

    pub fn append(self, receiver: &mut CellGroupCheckpointReceiver) -> Result<()> {
        let cell = match &self.part {
            Part::Materialized { cell }
            | Part::ExtentManifest { cell }
            | Part::Extent { cell, .. } => Some(
                CellId::new(cell).map_err(|_| CellGroupCheckpointError::Invalid("wire cell"))?,
            ),
            _ => None,
        };
        let part = match &self.part {
            Part::Header => CellGroupCheckpointPart::Header,
            Part::Page { digest } => CellGroupCheckpointPart::Page(*digest),
            Part::Materialized { .. } => {
                CellGroupCheckpointPart::Materialized(cell.as_ref().unwrap())
            }
            Part::ExtentManifest { .. } => {
                CellGroupCheckpointPart::ExtentManifest(cell.as_ref().unwrap())
            }
            Part::Extent {
                ordinal, digest, ..
            } => CellGroupCheckpointPart::Extent {
                cell: cell.as_ref().unwrap(),
                ordinal: *ordinal,
                id: receiver.wire_extent(cell.as_ref().unwrap(), *ordinal, *digest, self.length)?,
            },
        };
        receiver.append(CellGroupCheckpointChunk {
            part,
            offset: self.offset,
            length: self.length,
            bytes: &self.bytes,
        })
    }
}
