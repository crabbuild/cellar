// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use sha2::{Digest, Sha256};

pub(super) fn reference(manifest: &ExtentManifest) -> Result<ExtentRootReference, ImageApplyError> {
    let bytes = manifest
        .encode()
        .map_err(StoreError::from)
        .map_err(ImageApplyError::Extents)?;
    ExtentRootReference::new(manifest.root(), bytes.len() as u32).map_err(ImageApplyError::Extents)
}

pub(super) fn position(manifest: &ExtentManifest) -> Result<LtxImage, ImageApplyError> {
    let binding = manifest.binding();
    Ok(LtxImage {
        epoch: binding.epoch,
        txid: binding.txid,
        checksum: binding.checksum,
        commitment: ImageCommitment::Extents(reference(manifest)?),
    })
}

pub(in crate::raft) fn encode(image: &SealedImageRoot) -> CommittedImage {
    let reference = image.reference();
    let binding = image.manifest().binding();
    let geometry = image.manifest().geometry();
    CommittedImage {
        name: format!("{}.root", hex::encode(reference.root().bytes())),
        commitment: ImageCommitment::Extents(reference),
        length: geometry.length(),
        ltx_epoch: binding.epoch,
        txid: binding.txid,
        checksum: binding.checksum,
        page_size: geometry.page_size(),
    }
}

pub(in crate::raft) fn reference_key(group: &GroupLog, role: u8) -> [u8; 32] {
    let scope = &group.identity().scope;
    let mut hash = Sha256::new();
    hash.update(b"cellar.native-image-reference.v1\0");
    hash.update(scope.cluster.bytes());
    hash.update(scope.namespace.bytes());
    hash.update(scope.group.bytes());
    hash.update(scope.incarnation.bytes());
    hash.update(group.birth_digest);
    hash.update([role]);
    hash.finalize().into()
}

pub(super) fn adopt(
    group: &GroupLog,
    store: &NodeExtentStore,
    schema: [u8; 32],
    marker: &CommittedImage,
    max_image_bytes: u64,
    buffer: &mut [u8],
) -> Result<SealedImageRoot, ImageApplyError> {
    marker.validate().map_err(ImageApplyError::Log)?;
    if marker.length > max_image_bytes {
        return Err(ImageApplyError::Invalid("extent image exceeds quota"));
    }
    let reference = match marker.commitment {
        ImageCommitment::Extents(reference) => reference,
        ImageCommitment::Sha256(_) => {
            return Err(ImageApplyError::Invalid("extent marker required"))
        }
    };
    let view = store
        .open_root(reference)
        .map_err(ImageApplyError::Extents)?;
    let manifest = view.manifest();
    if manifest.binding()
        != (ImageBinding {
            history: group.birth_digest,
            schema,
            epoch: marker.ltx_epoch,
            txid: marker.txid,
            checksum: marker.checksum,
        })
        || manifest.geometry().length() != marker.length
        || manifest.geometry().page_size() != marker.page_size
    {
        return Err(ImageApplyError::Invalid(
            "extent marker history/schema/geometry",
        ));
    }
    match store.seal_image_root(manifest.clone(), buffer) {
        Ok(image) => Ok(image),
        Err(StoreError::Unverified) => {
            // Reopened names are metadata, not proof. Every local byte must be
            // independently verified and synced before this node can admit it.
            for index in 0..manifest.geometry().extent_count() {
                store
                    .verify_extent(
                        manifest
                            .extent(index)
                            .ok_or(ImageApplyError::Invalid("extent closure"))?,
                    )
                    .map_err(ImageApplyError::Extents)?;
            }
            store
                .seal_image_root(manifest.clone(), buffer)
                .map_err(ImageApplyError::Extents)
        }
        Err(error) => Err(ImageApplyError::Extents(error)),
    }
}
