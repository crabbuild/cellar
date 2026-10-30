// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recover only the complete root selected by the persisted engine marker.
use super::*;
use cellar_logic::cell_group::CellImageCommitment;
use cellar_ltx::{
    apply::ImageState,
    internal::{ExtentRootReference, ImageBinding, ImageRoot, EXTENT_BYTES},
    TXID,
};

impl CellGroupApplier {
    /// Verify the persisted selected root and all physical cell images. Run on
    /// the owning blocking execution domain, before starting its Raft owner.
    /// This grants no quorum or serving authority: startup still consumes the
    /// catalog permit and must establish current-term quorum before admission.
    /// Missing/corrupt closure refuses; unselected newer files are not recovery.
    pub fn recover(
        group: GroupLog<GroupHistory>,
        pages: ManifestPageStore,
        images: CellGroupApplyImages,
        manifest_limits: ManifestLimits,
        limits: GroupEntryLimits,
        max_effect_rows: u32,
    ) -> Result<(Self, PreparedGroupManifest), ExecutionError> {
        let applier = Self::new(
            group,
            pages,
            images,
            manifest_limits,
            limits,
            max_effect_rows,
        )?;
        let marker = applier
            .group
            .group_manifest()
            .map_err(storage)?
            .ok_or(ExecutionError::Invalid("recovery manifest missing"))?;
        let prepared = {
            let mut owner = applier
                .storage
                .lock()
                .map_err(|_| ExecutionError::Invalid("poisoned recovery owner"))?;
            let Storage {
                tree,
                pages,
                images,
                ..
            } = &mut *owner;
            tree.prepare_complete(
                &applier.group,
                pages,
                marker.root(),
                marker.position(),
                marker.membership_revision(),
                |leaf| {
                    images
                        .recover_image(leaf, marker.route().schema_digest)
                        .map_err(|error| ManifestError::Io(std::io::Error::other(error)))
                },
            )
            .map_err(storage)?
        };
        if let Some(checkpoint) = applier.group.checkpoint_manifest().map_err(storage)? {
            let mut owner = applier
                .storage
                .lock()
                .map_err(|_| ExecutionError::Invalid("poisoned recovery owner"))?;
            let Storage {
                tree,
                pages,
                images,
                ..
            } = &mut *owner;
            tree.prepare_complete(
                &applier.group,
                pages,
                checkpoint.root(),
                checkpoint.position(),
                checkpoint.membership_revision(),
                |leaf| {
                    images
                        .recover_image(leaf, checkpoint.route().schema_digest)
                        .map_err(|error| ManifestError::Io(std::io::Error::other(error)))
                },
            )
            .map_err(storage)?;
        }
        if applier.group.group_manifest().map_err(storage)?.as_ref() != Some(&marker) {
            return Err(ExecutionError::Invalid("recovery selection changed"));
        }
        Ok((applier, prepared))
    }
}

impl CellGroupApplyImages {
    fn recover_image(
        &self,
        leaf: &CellManifestLeaf,
        schema: [u8; 32],
    ) -> Result<CellImagePin, ExecutionError> {
        let image = leaf.version.image;
        let pin = if image.txid == 0 {
            CellImagePin::Empty
        } else {
            match (self, image.commitment) {
                (Self::Materialized { images, .. }, CellImageCommitment::Materialized(digest)) => {
                    let expected = ImageState {
                        epoch: image.epoch,
                        txid: TXID(image.txid),
                        checksum: image.checksum,
                        digest,
                        page_size: leaf.geometry.page_size,
                        length: leaf.geometry.length,
                    };
                    let id = images::image_id(
                        &leaf.version.history,
                        leaf.version.sequence.get(),
                        expected,
                    )?;
                    CellImagePin::Materialized(images.pin_prepared(id, expected).map_err(storage)?)
                }
                (
                    Self::Extents { images, .. },
                    CellImageCommitment::Extents {
                        root,
                        manifest_bytes,
                    },
                ) => {
                    let reference =
                        ExtentRootReference::new(ImageRoot::from_bytes(root), manifest_bytes)
                            .map_err(storage)?;
                    let view = images.open_root(reference).map_err(storage)?;
                    let manifest = view.manifest();
                    let expected = ImageBinding {
                        history: cell_image_history_digest(&leaf.version.history),
                        schema,
                        epoch: image.epoch,
                        txid: image.txid,
                        checksum: image.checksum,
                    };
                    if manifest.binding() != expected
                        || manifest.geometry().length() != leaf.geometry.length
                        || manifest.geometry().page_size() != leaf.geometry.page_size
                    {
                        return Err(ExecutionError::Invalid("recovery image binding"));
                    }
                    CellImagePin::Extents(Arc::new(
                        images
                            .seal_image_root(manifest.clone(), &mut vec![0; EXTENT_BYTES as usize])
                            .map_err(storage)?,
                    ))
                }
                _ => return Err(ExecutionError::Invalid("recovery image representation")),
            }
        };
        pin.check(leaf, schema).map_err(storage)?;
        Ok(pin)
    }
}
