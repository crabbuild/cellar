// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical baseline construction before any replica runtime or client write.
//! Both representations independently check the complete prepared template.

use anyhow::{ensure, Result};
use cellar::sdk::consensus::{
    verification::CommittedImage, GroupScope, ImageCommitment, RaftImageStorage,
};
use cellar_ltx::{
    apply::ImageState,
    generation::{GenerationLimits, GenerationPin, GenerationStore, ImageId},
    internal::{
        ExtentId, ExtentManifest, Geometry, ImageBinding, NodeExtentStore, SealedImageRoot,
        EXTENT_BYTES,
    },
    FileSystem,
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};

pub(super) struct Options {
    pub directory: PathBuf,
    pub template: PathBuf,
    pub state: ImageState,
    pub quota: u64,
    pub scope: GroupScope,
    pub storage: RaftImageStorage,
    pub history: [u8; 32],
    pub schema: [u8; 32],
}

pub(super) struct Baseline {
    pub marker: CommittedImage,
    _physical: Physical,
}

enum Physical {
    Materialized {
        _images: GenerationStore,
        _pin: GenerationPin,
    },
    Extents {
        _store: NodeExtentStore,
        _image: SealedImageRoot,
    },
}

pub(super) fn prepare(options: Options, filesystem: Arc<dyn FileSystem>) -> Result<Baseline> {
    let Options {
        directory,
        template,
        state,
        quota,
        scope,
        storage,
        history,
        schema,
    } = options;
    let (marker, physical) = match storage {
        RaftImageStorage::Materialized => {
            let path = directory
                .join("raft-cells")
                .join(hex::encode(scope.namespace.bytes()))
                .join(hex::encode(scope.group.bytes()))
                .join(hex::encode(scope.incarnation.bytes()))
                .join("images");
            let mut limits = GenerationLimits::default();
            limits.apply.decode.max_image_bytes = quota;
            limits.max_bytes = limits.max_bytes.max(8 * quota);
            let images = GenerationStore::open(path, filesystem, limits)?;
            let pin = images.prepare_image_file(ImageId::new([1; 16])?, &template, state)?;
            let marker = CommittedImage {
                name: format!("{}.sqlite", hex::encode(pin.id().bytes())),
                commitment: state.digest.into(),
                length: state.length,
                ltx_epoch: state.epoch,
                txid: state.txid.0,
                checksum: state.checksum,
                page_size: state.page_size,
            };
            (
                marker,
                Physical::Materialized {
                    _images: images,
                    _pin: pin,
                },
            )
        }
        RaftImageStorage::Extents => {
            ensure!(
                state.length <= quota,
                "preloaded extent image exceeds quota"
            );
            let geometry = Geometry::new(state.page_size, state.length)?;
            let store = NodeExtentStore::open_serialized(
                directory.join("raft-cells/extent-inventory"),
                filesystem.clone(),
                Default::default(),
            )?;
            let mut file = filesystem.open(&template)?;
            ensure!(
                file.file_len()? == state.length,
                "preloaded extent image length differs"
            );
            let mut digest = Sha256::new();
            let mut objects = Vec::with_capacity(geometry.extent_count() as usize);
            let mut buffer = vec![0; EXTENT_BYTES as usize];
            for index in 0..geometry.extent_count() {
                let length = geometry
                    .extent_length(index)
                    .ok_or_else(|| anyhow::anyhow!("preloaded extent geometry"))?
                    as usize;
                let bytes = &mut buffer[..length];
                file.read_exact_into_at(u64::from(index) * EXTENT_BYTES, bytes)?;
                digest.update(&*bytes);
                let id = ExtentId::of(bytes)?;
                store.put_extent(id, |offset, count| {
                    let start = offset as usize;
                    bytes
                        .get(start..start + count)
                        .map(<[u8]>::to_vec)
                        .ok_or_else(|| std::io::Error::other("preloaded extent source range"))
                })?;
                objects.push(id);
            }
            ensure!(
                <[u8; 32]>::from(digest.finalize()) == state.digest,
                "preloaded extent image digest differs"
            );
            let manifest = ExtentManifest::new(
                ImageBinding {
                    history,
                    schema,
                    epoch: state.epoch,
                    txid: state.txid.0,
                    checksum: state.checksum,
                },
                geometry,
                &objects,
            )?;
            let image = store.seal_image_root(manifest, &mut buffer)?;
            let reference = image.reference();
            let marker = CommittedImage {
                name: format!("{}.root", hex::encode(reference.root().bytes())),
                commitment: ImageCommitment::Extents(reference),
                length: state.length,
                ltx_epoch: state.epoch,
                txid: state.txid.0,
                checksum: state.checksum,
                page_size: state.page_size,
            };
            (
                marker,
                Physical::Extents {
                    _store: store,
                    _image: image,
                },
            )
        }
    };
    Ok(Baseline {
        marker,
        _physical: physical,
    })
}

#[test]
fn baseline_digest_is_checked_for_both_storage_representations() -> Result<()> {
    use cellar::sdk::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};
    let root = tempfile::tempdir()?;
    let quota = 12 * 1024 * 1024;
    let source = super::make_template(root.path(), quota, false)?;
    let (template, state, _, _) =
        super::prepared_template(&source, root.path(), quota, quota / 16)?;
    for storage in [RaftImageStorage::Materialized, RaftImageStorage::Extents] {
        let options = |directory: PathBuf, state| Options {
            directory,
            template: template.clone(),
            state,
            quota,
            scope: GroupScope {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace: NamespaceId::new([2; 16]).unwrap(),
                group: GroupId::new([3; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
                cell: "Counter:preloaded".into(),
            },
            storage,
            history: [5; 32],
            schema: [6; 32],
        };
        let good = tempfile::tempdir_in(root.path())?;
        drop(prepare(
            options(good.path().into(), state),
            Arc::new(cellar_ltx::DirectFileSystem),
        )?);
        let mut incorrect = state;
        incorrect.digest[0] ^= 1;
        let bad = tempfile::tempdir_in(root.path())?;
        ensure!(
            prepare(
                options(bad.path().into(), incorrect),
                Arc::new(cellar_ltx::DirectFileSystem)
            )
            .is_err(),
            "preloaded {storage:?} accepted changed template bytes"
        );
    }
    Ok(())
}
