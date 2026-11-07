// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-side hashing for the dependency-free cell-group contracts.

use super::protocol::{ImageCommitment, LtxImage, ProtocolError};
use cellar_logic::{
    cell_group::{CellId, CellImage, CellImageCommitment, CellKey},
    consensus::NamespaceId,
};
use cellar_ltx::internal::{ExtentRootReference, ImageRoot};
use sha2::{Digest, Sha256};

pub fn cell_routing_key(namespace: NamespaceId, cell: &CellId) -> CellKey {
    CellKey::from_digest(
        Sha256::digest(cell.routing_preimage(namespace)).into(),
        cell.clone(),
    )
}

impl TryFrom<CellImage> for LtxImage {
    type Error = ProtocolError;

    fn try_from(image: CellImage) -> Result<Self, Self::Error> {
        image.validate().map_err(|_| ProtocolError::Chain)?;
        let commitment = match image.commitment {
            CellImageCommitment::Materialized(digest) => ImageCommitment::Sha256(digest),
            CellImageCommitment::Extents {
                root,
                manifest_bytes,
            } => ImageCommitment::Extents(
                ExtentRootReference::new(ImageRoot::from_bytes(root), manifest_bytes)
                    .map_err(|_| ProtocolError::Identity)?,
            ),
        };
        Ok(Self {
            epoch: image.epoch,
            txid: image.txid,
            checksum: image.checksum,
            commitment,
        })
    }
}

impl From<LtxImage> for CellImage {
    fn from(image: LtxImage) -> Self {
        let commitment = match image.commitment {
            ImageCommitment::Sha256(digest) => CellImageCommitment::Materialized(digest),
            ImageCommitment::Extents(reference) => CellImageCommitment::Extents {
                root: reference.root().bytes(),
                manifest_bytes: reference.encoded_length(),
            },
        };
        Self {
            epoch: image.epoch,
            txid: image.txid,
            checksum: image.checksum,
            commitment,
        }
    }
}
