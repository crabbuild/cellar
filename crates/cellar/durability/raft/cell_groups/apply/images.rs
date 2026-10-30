// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::{group_protocol::GroupLtx, ImageCommitment, LtxImage};
use cellar_logic::cell_group::CellHistory;
use cellar_ltx::{
    apply::{ApplyLimits, ImageState, ImageUpdate, LtxSegment},
    generation::{GenerationStore, ImageId, ReadServices},
    internal::{
        CacheLimits, ExtentManifest, ExtentRootReference, Geometry, ImageBinding, ImageRootUpdate,
        NodeExtentStore, EXTENT_BYTES,
    },
    stream_snapshot::ScratchPool,
    TXID,
};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Clone)]
pub enum CellGroupApplyImages {
    Materialized {
        images: GenerationStore,
        services: ReadServices,
    },
    Extents {
        images: NodeExtentStore,
        limits: ApplyLimits,
        services: ReadServices,
        cache_directory: PathBuf,
        cache_pool: ScratchPool,
        cache: CacheLimits,
    },
}

pub(super) struct ImageApply<'a> {
    pub cell: &'a CellHistory,
    pub sequence: u64,
    pub schema: [u8; 32],
    pub base: Option<&'a CellImagePin>,
    pub image: LtxImage,
    pub files: &'a [GroupLtx<'a>],
    pub max_effect_rows: u32,
}

impl CellGroupApplyImages {
    pub(super) fn memory_bound(&self) -> usize {
        let limits = match self {
            Self::Materialized { images, .. } => images.apply_limits(),
            Self::Extents { limits, .. } => *limits,
        };
        // Limits validate decoded bytes to at most 256 MiB. Include one extent
        // scratch buffer even for materialized mode to keep admission uniform.
        limits.max_total_decoded_bytes as usize + EXTENT_BYTES as usize
    }

    pub(super) fn validate(&self) -> Result<(), ExecutionError> {
        match self {
            Self::Materialized { images, .. } => images.apply_limits().validate().map_err(storage),
            Self::Extents {
                limits,
                cache_directory,
                ..
            } => {
                limits.validate().map_err(storage)?;
                if cache_directory.as_os_str().is_empty() {
                    return Err(ExecutionError::Invalid("apply cache directory"));
                }
                Ok(())
            }
        }
    }

    pub(super) fn prepare(
        &self,
        request: ImageApply<'_>,
    ) -> Result<EnrollmentImage, ExecutionError> {
        let ImageApply {
            cell,
            sequence,
            schema,
            base,
            image,
            files,
            max_effect_rows,
        } = request;
        if image.txid == 0 {
            if base.is_some()
                || !files.is_empty()
                || image.checksum != 0
                || image.commitment != ImageCommitment::Sha256(Sha256::digest([]).into())
            {
                return Err(ExecutionError::Invalid("noncanonical empty cell image"));
            }
            return Ok(EnrollmentImage {
                geometry: CellImageGeometry {
                    length: 0,
                    page_size: 512,
                },
                effects: EffectSummary::default(),
                pin: CellImagePin::Empty,
            });
        }
        let header = cellar_ltx::ltx::Header::parse(
            files
                .last()
                .ok_or(ExecutionError::Invalid("apply LTX missing"))?
                .bytes,
        )
        .map_err(storage)?;
        let geometry = CellImageGeometry {
            page_size: header.page_size,
            length: u64::from(header.page_size) * u64::from(header.commit),
        };
        let segments = files
            .iter()
            .map(|file| {
                Ok(LtxSegment {
                    bytes: file.bytes,
                    min_txid: TXID(file.min_txid),
                    max_txid: TXID(file.max_txid),
                    page_count: usize::try_from(file.page_count)
                        .map_err(|_| ExecutionError::Invalid("apply page count"))?,
                    decoded_bytes: file.decoded_bytes,
                })
            })
            .collect::<Result<Vec<_>, ExecutionError>>()?;
        let pin = match (self, image.commitment) {
            (Self::Materialized { images, .. }, ImageCommitment::Sha256(digest)) => {
                let base = match base {
                    Some(CellImagePin::Materialized(pin)) => Some(pin),
                    Some(CellImagePin::Empty) | None => None,
                    _ => return Err(ExecutionError::Invalid("materialized apply base")),
                };
                let mut id = Sha256::new();
                id.update(b"cellar-cell-apply-image-1\0");
                id.update(cell_image_history_digest(cell));
                for value in [sequence, image.epoch, image.txid, image.checksum] {
                    id.update(value.to_be_bytes());
                }
                id.update(digest);
                let bytes: [u8; 32] = id.finalize().into();
                let id = ImageId::new(bytes[..16].try_into().map_err(storage)?).map_err(storage)?;
                let update = ImageUpdate {
                    base: match base {
                        Some(pin) => pin.state(),
                        None => ImageState::empty(image.epoch).map_err(storage)?,
                    },
                    result: ImageState {
                        epoch: image.epoch,
                        txid: TXID(image.txid),
                        checksum: image.checksum,
                        digest,
                        page_size: geometry.page_size,
                        length: geometry.length,
                    },
                    segments,
                };
                CellImagePin::Materialized(match images.prepare(id, base, &update) {
                    Ok(pin) => pin,
                    Err(cellar_ltx::generation::GenerationError::Exists) => {
                        images.pin_prepared(id, update.result).map_err(storage)?
                    }
                    Err(error) => return Err(storage(error)),
                })
            }
            (Self::Extents { images, limits, .. }, ImageCommitment::Extents(result)) => {
                let base = match base {
                    Some(CellImagePin::Extents(pin)) => Some(pin.as_ref()),
                    Some(CellImagePin::Empty) | None => None,
                    _ => return Err(ExecutionError::Invalid("extent apply base")),
                };
                let binding = ImageBinding {
                    history: cell_image_history_digest(cell),
                    schema,
                    epoch: image.epoch,
                    txid: image.txid,
                    checksum: image.checksum,
                };
                let empty = ExtentManifest::new(
                    ImageBinding {
                        txid: 0,
                        checksum: 0,
                        ..binding
                    },
                    Geometry::new(0, 0).map_err(storage)?,
                    &[],
                )
                .map_err(storage)?;
                let before = match base {
                    Some(pin) => pin.reference(),
                    None => ExtentRootReference::new(
                        empty.root(),
                        empty
                            .encode()
                            .map_err(storage)?
                            .len()
                            .try_into()
                            .map_err(storage)?,
                    )
                    .map_err(storage)?,
                };
                let update = ImageRootUpdate {
                    base: before,
                    result,
                    binding,
                    geometry: Geometry::new(geometry.page_size, geometry.length)
                        .map_err(storage)?,
                    segments,
                };
                let prepared = images
                    .prepare_image_root(base, &update, limits, &mut vec![0; EXTENT_BYTES as usize])
                    .map_err(storage)?;
                CellImagePin::Extents(Arc::new(prepared.image))
            }
            _ => return Err(ExecutionError::Invalid("apply image representation")),
        };
        let database = match (self, &pin) {
            (Self::Materialized { services, .. }, CellImagePin::Materialized(pin)) => {
                crate::storage::open_immutable_generation(
                    pin.clone(),
                    crate::storage::ImmutableQueryOptions {
                        services: services.clone(),
                        sqlite_vec: false,
                    },
                )
            }
            (
                Self::Extents {
                    services,
                    cache_directory,
                    cache_pool,
                    cache,
                    ..
                },
                CellImagePin::Extents(pin),
            ) => {
                let image = pin
                    .view()
                    .image(cache_directory.clone(), cache_pool.clone(), *cache)
                    .map_err(storage)?;
                crate::storage::open_immutable_extent_image(
                    image,
                    crate::storage::ImmutableQueryOptions {
                        services: services.clone(),
                        sqlite_vec: false,
                    },
                )
            }
            _ => return Err(ExecutionError::Invalid("apply image query")),
        }
        .map_err(storage)?;
        let effects = summary::read(database.connection(), max_effect_rows)?;
        drop(database);
        Ok(EnrollmentImage {
            pin,
            geometry,
            effects,
        })
    }
}
