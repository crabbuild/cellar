// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical image proofs used while preparing complete group roots.

use super::{CellManifestLeaf, ManifestError};
use cellar_logic::cell_group::{CellHistory, CellImageCommitment};
use cellar_ltx::{generation::GenerationPin, internal::SealedImageRoot};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone)]
pub enum CellImagePin {
    Materialized(GenerationPin),
    Extents(Arc<SealedImageRoot>),
    /// Only the canonical zero-length image; never a missing-image fallback.
    Empty,
}

/// Stable across group transfer, distinct from a physical group birth digest.
pub fn cell_image_history_digest(history: &CellHistory) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-cell-image-history-1\0");
    hash.update(history.cluster.bytes());
    hash.update(history.namespace.bytes());
    hash.update(history.incarnation.bytes());
    hash.update((history.cell.as_str().len() as u32).to_be_bytes());
    hash.update(history.cell.as_str().as_bytes());
    hash.finalize().into()
}

impl CellImagePin {
    pub(super) fn check(
        &self,
        leaf: &CellManifestLeaf,
        schema: [u8; 32],
    ) -> Result<(), ManifestError> {
        let image = leaf.version.image;
        let valid = match self {
            Self::Materialized(pin) => {
                let state = pin.state();
                image.commitment == CellImageCommitment::Materialized(state.digest)
                    && image.epoch == state.epoch
                    && image.txid == state.txid.0
                    && image.checksum == state.checksum
                    && leaf.geometry.length == state.length
                    && leaf.geometry.page_size == state.page_size
            }
            Self::Extents(pin) => {
                let manifest = pin.manifest();
                let binding = manifest.binding();
                let reference = pin.reference();
                binding.history == cell_image_history_digest(&leaf.version.history)
                    && binding.schema == schema
                    && binding.epoch == image.epoch
                    && binding.txid == image.txid
                    && binding.checksum == image.checksum
                    && manifest.geometry().length() == leaf.geometry.length
                    && manifest.geometry().page_size() == leaf.geometry.page_size
                    && image.commitment
                        == CellImageCommitment::Extents {
                            root: reference.root().bytes(),
                            manifest_bytes: reference.encoded_length(),
                        }
            }
            Self::Empty => {
                image.txid == 0
                    && image.checksum == 0
                    && leaf.geometry.length == 0
                    && image.commitment
                        == CellImageCommitment::Materialized(Sha256::digest([]).into())
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ManifestError::Invalid("cell image seal"))
        }
    }
}
