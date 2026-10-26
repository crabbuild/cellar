// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical image choices beneath the one command/receipt/authority owner.
//! Commitment kinds stay explicit; an extent root is never a flat digest.

use super::*;
use crate::application::consensus::{CellUpdate, ImageCommitment, LtxImage};
use crate::raft::{cache::CommittedImage, rooted::RootPublishedImage};
use cellar_ltx::{
    internal::{
        CacheLimits, Geometry, ImageBinding, ImageRootUpdate, NodeExtentStore, NodeWorkspace,
        SealedImageRoot, WorkspaceBounds,
    },
    stream_snapshot::ScratchPool,
    TXID,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct RootPrivateOptions {
    pub workspace: NodeWorkspace,
    pub host: LtxHost,
    pub sqlite_vfs: Option<String>,
    pub images: NodeExtentStore,
    pub query_services: ReadServices,
    pub application_digest: [u8; 32],
    pub limits: ExecutionLimits,
    /// Separate admitted read-cache files; never inside the private directory.
    pub cache_directory: PathBuf,
    pub cache_pool: ScratchPool,
    pub cache: CacheLimits,
}

impl RootPrivateOptions {
    /// Reserve main/WAL plus all bounded captured files and their temporary
    /// replacements before opening a disposable SQLite writer.
    pub fn workspace_bounds(limits: ExecutionLimits) -> Result<WorkspaceBounds, ExecutionError> {
        limits.validate()?;
        // CaptureOutput enforces an aggregate encoded-byte budget before file
        // creation. Admit a retained cut, the new batch and its temporary file.
        let capture_bytes = (limits.envelope.max_payload_bytes as u64)
            .checked_mul(3)
            .ok_or(ExecutionError::Invalid("private capture reservation"))?;
        let bounds = WorkspaceBounds {
            image_bytes: limits.max_image_bytes,
            wal_bytes: super::rooted_working::WAL_BYTES,
            capture_bytes,
        };
        bounds.reservation().map_err(storage)?;
        Ok(bounds)
    }
}

#[derive(Clone)]
pub(super) struct Options {
    pub directory: PathBuf,
    pub host: LtxHost,
    pub sqlite_vfs: Option<String>,
    pub images: Images,
    pub query_services: ReadServices,
    pub application_digest: [u8; 32],
    pub limits: ExecutionLimits,
}

#[derive(Clone)]
pub(super) enum Images {
    Materialized(GenerationStore),
    Extents {
        store: NodeExtentStore,
        workspace: NodeWorkspace,
        cache_directory: PathBuf,
        cache_pool: ScratchPool,
        cache: CacheLimits,
    },
}

impl From<PrivateOptions> for Options {
    fn from(options: PrivateOptions) -> Self {
        Self {
            directory: options.directory,
            host: options.host,
            sqlite_vfs: options.sqlite_vfs,
            images: Images::Materialized(options.images),
            query_services: options.query_services,
            application_digest: options.application_digest,
            limits: options.limits,
        }
    }
}

impl From<RootPrivateOptions> for Options {
    fn from(options: RootPrivateOptions) -> Self {
        Self {
            directory: options.workspace.directory().to_path_buf(),
            host: options.host.with_filesystem(options.workspace.filesystem()),
            sqlite_vfs: options.sqlite_vfs,
            images: Images::Extents {
                store: options.images,
                workspace: options.workspace,
                cache_directory: options.cache_directory,
                cache_pool: options.cache_pool,
                cache: options.cache,
            },
            query_services: options.query_services,
            application_digest: options.application_digest,
            limits: options.limits,
        }
    }
}

impl Options {
    pub fn materialized(&self) -> anyhow::Result<PrivateOptions> {
        let Images::Materialized(images) = &self.images else {
            anyhow::bail!("materialized private storage required");
        };
        Ok(PrivateOptions {
            directory: self.directory.clone(),
            host: self.host.clone(),
            sqlite_vfs: self.sqlite_vfs.clone(),
            images: images.clone(),
            query_services: self.query_services.clone(),
            application_digest: self.application_digest,
            limits: self.limits,
        })
    }
}

#[derive(Clone)]
pub(crate) enum Published {
    Materialized(PublishedImage),
    Rooted(RootPublishedImage),
}

impl Published {
    pub fn scope(&self) -> &cellar_logic::consensus::GroupScope {
        match self {
            Self::Materialized(image) => image.scope(),
            Self::Rooted(image) => image.scope(),
        }
    }

    pub fn application_digest(&self) -> [u8; 32] {
        match self {
            Self::Materialized(image) => image.application_digest(),
            Self::Rooted(image) => image.application_digest(),
        }
    }

    pub fn position(&self) -> LogPosition {
        match self {
            Self::Materialized(image) => image.position(),
            Self::Rooted(image) => image.position(),
        }
    }

    pub fn proposal(&self) -> Option<ProposalId> {
        match self {
            Self::Materialized(image) => image.proposal(),
            Self::Rooted(image) => image.proposal(),
        }
    }

    pub fn marker(&self) -> CommittedImage {
        match self {
            Self::Materialized(image) => marker(&image.pin()),
            Self::Rooted(image) => image.marker(),
        }
    }

    pub(super) fn state(&self) -> State {
        match self {
            Self::Materialized(image) => State::Materialized(image.pin().state()),
            Self::Rooted(image) => State::Rooted(RootState::of(&image.image())),
        }
    }

    pub fn flat(&self) -> anyhow::Result<&PublishedImage> {
        match self {
            Self::Materialized(image) => Ok(image),
            Self::Rooted(_) => anyhow::bail!("materialized base required"),
        }
    }

    pub fn rooted(&self) -> anyhow::Result<&RootPublishedImage> {
        match self {
            Self::Rooted(image) => Ok(image),
            Self::Materialized(_) => anyhow::bail!("extent base required"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RootState {
    pub image: LtxImage,
    pub geometry: Geometry,
}

impl RootState {
    pub fn of(root: &SealedImageRoot) -> Self {
        let binding = root.manifest().binding();
        Self {
            image: LtxImage {
                epoch: binding.epoch,
                txid: binding.txid,
                checksum: binding.checksum,
                commitment: ImageCommitment::Extents(root.reference()),
            },
            geometry: root.manifest().geometry(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum State {
    Materialized(ImageState),
    Rooted(RootState),
}

pub(super) enum Preview {
    Materialized(GenerationPin),
    Rooted(Arc<SealedImageRoot>),
}

pub(super) enum WorkingImage {
    Materialized(Box<crate::raft::working::Working>),
    Rooted(Box<super::rooted_working::RootWorking>),
}

impl WorkingImage {
    pub fn observation_work(&self) -> Option<cellar_ltx::internal::db::ObservationWork> {
        match self {
            Self::Materialized(_) => None,
            Self::Rooted(working) => working.observation_work(),
        }
    }

    pub fn open(
        options: &Options,
        base: Option<&Published>,
        history: [u8; 32],
    ) -> anyhow::Result<Self> {
        match &options.images {
            Images::Materialized(_) => Ok(Self::Materialized(Box::new(
                crate::raft::working::Working::open(
                    &options.materialized()?,
                    base.map(Published::flat).transpose()?,
                )?,
            ))),
            Images::Extents { .. } => Ok(Self::Rooted(Box::new(
                super::rooted_working::RootWorking::open(
                    options,
                    base.map(Published::rooted).transpose()?,
                    history,
                )?,
            ))),
        }
    }

    pub fn database(&self) -> &crate::storage::CommandDatabase {
        match self {
            Self::Materialized(working) => &working.database,
            Self::Rooted(working) => &working.database,
        }
    }

    pub fn begin_mutation(&mut self) {
        match self {
            Self::Materialized(working) => working.begin_mutation(),
            Self::Rooted(working) => working.begin_mutation(),
        }
    }

    pub fn close(self) -> anyhow::Result<()> {
        match self {
            Self::Materialized(working) => (*working).close(),
            Self::Rooted(working) => (*working).close(),
        }
    }

    pub fn trim(&self, options: &Options, base: Option<&Published>) -> anyhow::Result<()> {
        match self {
            Self::Materialized(working) => working.trim(
                &options.materialized()?,
                base.map(Published::flat).transpose()?,
            ),
            Self::Rooted(working) => {
                working.trim(options, base.map(Published::rooted).transpose()?)
            }
        }
    }

    pub fn matches(&self, options: &Options, base: &Published) -> bool {
        match (self, base) {
            (Self::Materialized(working), Published::Materialized(base)) => working.matches(base),
            (Self::Rooted(working), Published::Rooted(base)) => working.matches(options, base),
            _ => false,
        }
    }

    pub fn capture(
        &mut self,
        options: &Options,
        scope: &cellar_logic::consensus::GroupScope,
        proposal: ProposalId,
        base: Option<&Published>,
    ) -> anyhow::Result<CellUpdate> {
        match self {
            Self::Materialized(working) => working.capture(
                &options.materialized()?,
                scope,
                proposal,
                base.map(Published::flat).transpose()?,
            ),
            Self::Rooted(working) => working.capture(
                options,
                scope,
                proposal,
                base.map(Published::rooted).transpose()?,
            ),
        }
    }
}

impl Preview {
    pub fn state(&self) -> State {
        match self {
            Self::Materialized(pin) => State::Materialized(pin.state()),
            Self::Rooted(root) => State::Rooted(RootState::of(root)),
        }
    }

    pub fn length(&self) -> u64 {
        match self.state() {
            State::Materialized(state) => state.length,
            State::Rooted(state) => state.geometry.length(),
        }
    }
}

impl Images {
    pub fn workspace(&self) -> Option<&NodeWorkspace> {
        match self {
            Self::Materialized(_) => None,
            Self::Extents { workspace, .. } => Some(workspace),
        }
    }

    pub fn root_store(&self) -> Option<&NodeExtentStore> {
        match self {
            Self::Materialized(_) => None,
            Self::Extents { store, .. } => Some(store),
        }
    }

    pub fn validate_workspace(&self, limits: ExecutionLimits) -> Result<(), ExecutionError> {
        if let Self::Extents {
            store, workspace, ..
        } = self
        {
            if !store.owns_workspace(workspace)
                || workspace.bounds() != RootPrivateOptions::workspace_bounds(limits)?
            {
                return Err(ExecutionError::Invalid(
                    "private workspace owner/bounds differ",
                ));
            }
        }
        Ok(())
    }

    pub fn matches(&self, base: Option<&Published>) -> bool {
        matches!(
            (self, base),
            (
                Self::Materialized(_),
                None | Some(Published::Materialized(_))
            ) | (Self::Extents { .. }, None | Some(Published::Rooted(_)))
        )
    }

    pub fn retire(&self, preview: &Preview) -> anyhow::Result<()> {
        match (self, preview) {
            (Self::Materialized(store), Preview::Materialized(pin)) => Ok(store.retire(pin.id())?),
            (Self::Extents { store, .. }, Preview::Rooted(root)) => {
                Ok(store.retire_root(root.reference())?)
            }
            _ => anyhow::bail!("preview storage kind differs"),
        }
    }

    pub fn reclaim_private(&self) -> anyhow::Result<()> {
        if let Self::Materialized(store) = self {
            store.reclaim(2)?;
        }
        // One group cannot reconcile the node's references or run node-wide GC.
        Ok(())
    }

    pub fn preview(
        &self,
        options: &Options,
        base: Option<&Published>,
        id: ImageId,
        update: &CellUpdate,
        history: [u8; 32],
    ) -> anyhow::Result<Preview> {
        match self {
            Self::Materialized(_) => {
                Ok(Preview::Materialized(crate::raft::working::verify_preview(
                    &options.materialized()?,
                    base.map(Published::flat).transpose()?,
                    id,
                    update,
                )?))
            }
            Self::Extents { store, .. } => {
                let result = match update.result.commitment {
                    ImageCommitment::Extents(reference) => reference,
                    _ => anyhow::bail!("extent result required"),
                };
                let before = match update.base.commitment {
                    ImageCommitment::Extents(reference) => reference,
                    _ => anyhow::bail!("extent base required"),
                };
                let header = cellar_ltx::ltx::Header::parse(
                    &update
                        .files
                        .last()
                        .ok_or_else(|| anyhow::anyhow!("empty update"))?
                        .bytes,
                )?;
                let base = base
                    .map(Published::rooted)
                    .transpose()?
                    .map(RootPublishedImage::image);
                let segments = update
                    .files
                    .iter()
                    .map(|file| {
                        Ok(cellar_ltx::apply::LtxSegment {
                            bytes: &file.bytes,
                            min_txid: TXID(file.min_txid),
                            max_txid: TXID(file.max_txid),
                            page_count: usize::try_from(file.page_count)?,
                            decoded_bytes: file.decoded_bytes,
                        })
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let prepared = store.prepare_image_root(
                    base.as_deref(),
                    &ImageRootUpdate {
                        base: before,
                        result,
                        binding: ImageBinding {
                            history,
                            schema: options.application_digest,
                            epoch: update.result.epoch,
                            txid: update.result.txid,
                            checksum: update.result.checksum,
                        },
                        geometry: Geometry::new(
                            header.page_size,
                            u64::from(header.commit) * u64::from(header.page_size),
                        )?,
                        segments,
                    },
                    &apply_limits(options.limits)?,
                    &mut vec![0; cellar_ltx::internal::EXTENT_BYTES as usize],
                )?;
                Ok(Preview::Rooted(Arc::new(prepared.image)))
            }
        }
    }

    pub fn read(
        &self,
        options: &Options,
        image: &Published,
    ) -> anyhow::Result<cellar_ltx::generation::ImmutableDatabase> {
        match (self, image) {
            (Self::Materialized(_), Published::Materialized(image)) => {
                Ok(cellar_ltx::generation::open_immutable(
                    image.pin(),
                    options.query_services.clone(),
                )?)
            }
            (
                Self::Extents {
                    cache_directory,
                    cache_pool,
                    cache,
                    ..
                },
                Published::Rooted(image),
            ) => Ok(cellar_ltx::internal::open_extent_image(
                image
                    .image()
                    .view()
                    .image(cache_directory.clone(), cache_pool.clone(), *cache)?,
                options.query_services.clone(),
            )?),
            _ => anyhow::bail!("receipt image storage differs"),
        }
    }
}

pub(super) fn apply_limits(
    limits: ExecutionLimits,
) -> anyhow::Result<cellar_ltx::apply::ApplyLimits> {
    let envelope = limits.envelope;
    Ok(cellar_ltx::apply::ApplyLimits {
        decode: cellar_ltx::ltx::DecodeLimits {
            max_encoded_bytes: envelope.max_payload_bytes,
            max_decoded_bytes: envelope.max_decoded_bytes,
            max_pages: usize::try_from(envelope.max_pages)?,
            max_image_bytes: limits.max_image_bytes,
            ..Default::default()
        },
        max_files: envelope.max_files,
        max_total_encoded_bytes: envelope.max_payload_bytes,
        max_total_decoded_bytes: envelope.max_decoded_bytes,
        max_total_pages: usize::try_from(envelope.max_pages)?,
    })
}
