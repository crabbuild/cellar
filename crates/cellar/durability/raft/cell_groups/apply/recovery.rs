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
        let prepared = applier.recover_selected()?;
        Ok((applier, prepared))
    }

    /// Reconcile the current and retained checkpoint closures using this same
    /// physical store owner after an unknown installation result. Run on the
    /// owning blocking domain. Never reopen a page directory while its selected
    /// roots still hold pins. This grants no catalog, quorum or serving authority.
    pub fn recover_selected(&self) -> Result<PreparedGroupManifest, ExecutionError> {
        let mut owner = self
            .storage
            .lock()
            .map_err(|_| ExecutionError::Invalid("poisoned recovery owner"))?;
        Ok(self.recover_roots(&mut owner)?.0)
    }

    /// A retry may retain the exact complete closure. Reuse only with the same
    /// live page/engine owners and settled selected/checkpoint retention. A
    /// changed durable selection is recovered on this owner as before.
    pub(in crate::raft::cell_groups) fn recover_or_reuse_selected(
        &self,
        previous: &PreparedGroupManifest,
    ) -> Result<PreparedGroupManifest, ExecutionError> {
        let mut owner = self
            .storage
            .lock()
            .map_err(|_| ExecutionError::Invalid("poisoned recovery owner"))?;
        owner.pages.require_live().map_err(storage)?;
        if !self.belongs_to_engine(&previous.engine_owner)
            || self.history() != previous.marker().route().history
            || !Arc::ptr_eq(&owner.pages.owner, &previous.page_owner)
        {
            return Err(ExecutionError::Invalid("retained recovery owner"));
        }
        if self.group.group_manifest().map_err(storage)?.as_ref() == Some(previous.marker()) {
            previous.require_selected(&self.group).map_err(storage)?;
            if self.group.group_manifest().map_err(storage)?.as_ref() != Some(previous.marker()) {
                return Err(ExecutionError::Busy);
            }
            return Ok(previous.clone());
        }
        Ok(self.recover_roots(&mut owner)?.0)
    }

    pub(in crate::raft::cell_groups) fn recover_roots(
        &self,
        owner: &mut Storage,
    ) -> Result<(PreparedGroupManifest, Option<PreparedGroupManifest>), ExecutionError> {
        let marker = self
            .group
            .group_manifest()
            .map_err(storage)?
            .ok_or(ExecutionError::Invalid("recovery manifest missing"))?;
        let checkpoint = self.group.checkpoint_manifest().map_err(storage)?;
        let Storage {
            tree,
            pages,
            images,
            ..
        } = owner;
        let mut prepare = |marker: &GroupManifest| {
            tree.prepare_bound_complete(&self.group, pages, marker.clone(), &mut |leaf| {
                images
                    .recover_image(leaf, marker.route().schema_digest)
                    .map_err(|error| ManifestError::Io(std::io::Error::other(error)))
            })
            .map_err(storage)
        };
        let current = prepare(&marker)?;
        let retained = checkpoint.as_ref().map(&mut prepare).transpose()?;
        if self.group.group_manifest().map_err(storage)?.as_ref() != Some(&marker)
            || self.group.checkpoint_manifest().map_err(storage)? != checkpoint
        {
            return Err(ExecutionError::Invalid("recovery selection changed"));
        }
        Ok((current, retained))
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
