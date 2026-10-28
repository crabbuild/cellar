// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit physical representations behind the common archive contract.
//! Neither data representation supplies a native activation capability.

use super::*;
use crate::raft::{
    rooted::RootPublishedImage,
    snapshot::{PreparedSnapshot, SnapshotLimits},
};
use cellar_ltx::{
    generation::{GenerationPin, GenerationStore, ImageId},
    internal::{NodeExtentStore, SealedImageRoot},
};

#[derive(Clone)]
/// An exact committed engine/image cut. Its physical image remains pinned
/// while publication runs, even when a later command publishes another image.
pub struct CommittedArchiveCut {
    pub(super) inner: Cut,
}

#[derive(Clone)]
pub(super) enum Cut {
    Materialized(Box<MaterializedCut>),
    Rooted(Box<super::root_head::RootCut>),
}

impl CommittedArchiveCut {
    /// Capture canonical empty materialized state without creating SQL data.
    pub fn capture_empty(
        group: &GroupLog,
        schema: [u8; 32],
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        Ok(Self {
            inner: Cut::Materialized(Box::new(MaterializedCut::capture_empty(
                group, schema, limits,
            )?)),
        })
    }

    pub fn capture(
        group: &GroupLog,
        image: PublishedImage,
        dependencies: Vec<ArchiveDependency>,
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        Ok(Self {
            inner: Cut::Materialized(Box::new(MaterializedCut::capture(
                group,
                image,
                dependencies,
                limits,
            )?)),
        })
    }

    /// Capture an extent image from its owning node inventory. Capturing data
    /// supplies no authority to install it in a different native member.
    pub fn capture_root(
        group: &GroupLog,
        image: RootPublishedImage,
        store: NodeExtentStore,
        dependencies: Vec<ArchiveDependency>,
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        Ok(Self {
            inner: Cut::Rooted(Box::new(super::root_head::RootCut::capture(
                group,
                image,
                store,
                dependencies,
                limits,
            )?)),
        })
    }

    pub fn position(&self) -> LogPosition {
        match &self.inner {
            Cut::Materialized(cut) => cut.position(),
            Cut::Rooted(cut) => cut.position,
        }
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        match &self.inner {
            Cut::Materialized(cut) => cut.memory_bytes(),
            Cut::Rooted(cut) => cut.memory_bytes,
        }
    }

    pub(super) fn image_bytes(&self) -> u64 {
        match &self.inner {
            Cut::Materialized(cut) => cut
                .image
                .as_ref()
                .map_or(0, |image| image.pin().state().length),
            Cut::Rooted(cut) => cut.image_bytes,
        }
    }
}

/// Bounded immutable metadata. Complete payload existence and independent
/// physical validation remain separate from live quorum authorization.
pub struct ArchiveMetadata {
    pub(super) inner: Metadata,
}

pub(super) enum Metadata {
    Materialized(Box<MaterializedMetadata>),
    Rooted(Box<super::root_head::RootMetadata>),
}

impl ArchiveMetadata {
    pub fn reference(&self) -> ArchiveReference {
        match &self.inner {
            Metadata::Materialized(meta) => meta.reference.clone(),
            Metadata::Rooted(meta) => meta.reference.clone(),
        }
    }

    pub fn scope(&self) -> &GroupScope {
        match &self.inner {
            Metadata::Materialized(meta) => &meta.scope,
            Metadata::Rooted(meta) => &meta.scope,
        }
    }

    pub fn position(&self) -> LogPosition {
        match &self.inner {
            Metadata::Materialized(meta) => meta.position,
            Metadata::Rooted(meta) => meta.position,
        }
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        match &self.inner {
            Metadata::Materialized(meta) => meta.manifest.schema_digest,
            Metadata::Rooted(meta) => meta.head.schema_digest,
        }
    }

    /// Selected materialized state. Extent commitments require their own
    /// representation and cannot be described by a materialized image digest.
    pub fn image_state(&self) -> Result<Option<ImageState>, ArchiveError> {
        match &self.inner {
            Metadata::Materialized(meta) => meta.manifest.image.map(Image::decode).transpose(),
            Metadata::Rooted(_) => Err(ArchiveError::Invalid(
                "extent archive has no materialized image state",
            )),
        }
    }

    pub fn configuration(&self) -> Result<ConfState, ArchiveError> {
        match &self.inner {
            Metadata::Materialized(meta) => manifest::decode_conf(&meta.manifest.conf_state),
            Metadata::Rooted(meta) => Ok(meta
                .snapshot
                .snapshot()
                .get_metadata()
                .get_conf_state()
                .clone()),
        }
    }

    pub fn membership(&self) -> super::super::member_state::GroupMembership {
        match &self.inner {
            Metadata::Materialized(meta) => meta.manifest.membership.clone(),
            Metadata::Rooted(meta) => meta.snapshot.membership().clone(),
        }
    }
}

/// Complete independently checked recovery material, without native authority.
/// Physical owners refuse a representation they cannot install.
pub struct ArchiveRecoverySet {
    pub(super) inner: Recovery,
}

pub(super) enum Recovery {
    Materialized(Box<MaterializedSet>),
    Rooted(Box<super::root_head::RootRecoverySet>),
}

impl ArchiveRecoverySet {
    pub(super) fn materialized(&self) -> Result<&MaterializedSet, ArchiveError> {
        match &self.inner {
            Recovery::Materialized(set) => Ok(set),
            Recovery::Rooted(_) => Err(ArchiveError::Invalid(
                "extent archive requires a node inventory",
            )),
        }
    }

    /// Descriptive selected materialized state; no activation authority.
    pub fn image_state(&self) -> Result<Option<ImageState>, ArchiveError> {
        self.materialized()?
            .manifest
            .image
            .map(Image::decode)
            .transpose()
    }

    /// Exact producer publication origin, including intact-restore aliases.
    pub fn image_position(&self) -> Result<Option<LogPosition>, ArchiveError> {
        self.materialized()?
            .manifest
            .image_position
            .map(Position::decode)
            .transpose()
    }

    pub(crate) fn birth_digest(&self) -> [u8; 32] {
        match &self.inner {
            Recovery::Materialized(set) => set.manifest.birth_digest,
            Recovery::Rooted(set) => set.metadata.head.birth_digest,
        }
    }

    pub fn reference(&self) -> ArchiveReference {
        match &self.inner {
            Recovery::Materialized(set) => set.reference.clone(),
            Recovery::Rooted(set) => set.metadata.reference.clone(),
        }
    }

    pub fn scope(&self) -> &GroupScope {
        match &self.inner {
            Recovery::Materialized(set) => &set.scope,
            Recovery::Rooted(set) => &set.metadata.scope,
        }
    }

    pub fn position(&self) -> LogPosition {
        match &self.inner {
            Recovery::Materialized(set) => set.position,
            Recovery::Rooted(set) => set.metadata.position,
        }
    }

    pub fn configuration(&self) -> Result<ConfState, ArchiveError> {
        match &self.inner {
            Recovery::Materialized(set) => manifest::decode_conf(&set.manifest.conf_state),
            Recovery::Rooted(set) => {
                Ok(set.root.snapshot().get_metadata().get_conf_state().clone())
            }
        }
    }

    pub fn membership(&self) -> super::super::member_state::GroupMembership {
        match &self.inner {
            Recovery::Materialized(set) => set.manifest.membership.clone(),
            Recovery::Rooted(set) => set.metadata.snapshot.membership().clone(),
        }
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        match &self.inner {
            Recovery::Materialized(set) => set.manifest.schema_digest,
            Recovery::Rooted(set) => set.metadata.head.schema_digest,
        }
    }

    pub fn tail_entries(&self) -> usize {
        match &self.inner {
            Recovery::Materialized(set) => set.entries.len(),
            Recovery::Rooted(_) => 0,
        }
    }

    pub fn dependencies(&self) -> impl Iterator<Item = ([u8; 32], &[u8])> {
        let values = match &self.inner {
            Recovery::Materialized(set) => &set.dependencies,
            Recovery::Rooted(set) => &set.dependencies,
        };
        values
            .iter()
            .map(|(object, bytes)| (object.digest, bytes.as_slice()))
    }

    pub fn restore(
        &self,
        images: &GenerationStore,
        nonce: ImageId,
    ) -> Result<GenerationPin, ArchiveError> {
        match &self.inner {
            Recovery::Materialized(set) => set.restore(images, nonce),
            Recovery::Rooted(_) => Err(ArchiveError::Invalid(
                "extent archive requires a node inventory",
            )),
        }
    }

    /// Restore selected materialized state, preserving canonical emptiness.
    pub fn restore_state(
        &self,
        images: &GenerationStore,
        nonce: ImageId,
    ) -> Result<Option<GenerationPin>, ArchiveError> {
        self.materialized()?.restore_state(images, nonce)
    }

    /// Prepare already sealed root material for an existing member's exact
    /// native snapshot ticket. This never flattens or converts commitments.
    pub fn prepare_root(
        self,
        group: &GroupLog,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<PreparedSnapshot, ArchiveError> {
        match self.inner {
            Recovery::Rooted(set) => set.root.prepare(group, schema, limits),
            Recovery::Materialized(_) => Err(ArchiveError::Invalid(
                "materialized archive requires a generation store",
            )),
        }
    }

    pub(crate) fn into_root(
        self,
        store: &NodeExtentStore,
    ) -> Result<(::raft::prelude::Snapshot, Arc<SealedImageRoot>), ArchiveError> {
        match self.inner {
            Recovery::Rooted(set) if store.same_inventory(&set.root.store) => {
                Ok((set.root.snapshot().clone(), set.root.image))
            }
            Recovery::Rooted(_) => Err(ArchiveError::Invalid("archive root inventory owner")),
            Recovery::Materialized(_) => {
                Err(ArchiveError::Invalid("extent archive material required"))
            }
        }
    }
}

impl CellArchive {
    pub(crate) fn with_image_storage(
        mut self,
        storage: super::super::catalog::RaftImageStorage,
    ) -> Self {
        self.image_storage = storage;
        self
    }

    /// Choose the destination inventory for complete root recovery. This does
    /// not convert materialized archives or admit incomplete local closures.
    pub fn with_root_store(mut self, store: NodeExtentStore) -> Self {
        self.root_store = Some(store);
        self
    }

    pub(super) fn validate_cut(&self, cut: &CommittedArchiveCut) -> Result<(), ArchiveError> {
        match &cut.inner {
            Cut::Materialized(cut) => self.validate_materialized_cut(cut),
            Cut::Rooted(cut) => cut.validate(self),
        }
    }

    pub(super) async fn publish_inner(
        &self,
        cut: CommittedArchiveCut,
        verifier: &ArchiveVerifier,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        match cut.inner {
            Cut::Materialized(cut) => {
                self.publish_materialized_inner(*cut, verifier)
                    .await
                    .map(|set| ArchiveRecoverySet {
                        inner: Recovery::Materialized(Box::new(set)),
                    })
            }
            Cut::Rooted(cut) => self
                .publish_root_inner(*cut)
                .await
                .map(|set| ArchiveRecoverySet {
                    inner: Recovery::Rooted(Box::new(set)),
                }),
        }
    }

    /// Select an immutable reference without downloading its image or blobs.
    /// Live recovery authorization must bind this exact reference separately.
    pub async fn recovery_metadata(&self) -> Result<Option<ArchiveMetadata>, ArchiveError> {
        Ok(self
            .load_head_metadata()
            .await?
            .map(|(metadata, _, _)| metadata))
    }

    pub async fn recovery_set(&self) -> Result<Option<ArchiveRecoverySet>, ArchiveError> {
        match self.load_head_metadata().await? {
            Some((metadata, _, _)) => Ok(Some(self.load_recovery_material(metadata).await?)),
            None => Ok(None),
        }
    }

    /// Load only the exact history-bound reference. Missing or corrupt material
    /// refuses recovery; a newer mutable head cannot substitute for it.
    pub async fn recovery_set_at(
        &self,
        reference: &ArchiveReference,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        self.load_recovery_material(self.load_metadata(&reference.0).await?)
            .await
    }

    pub(super) async fn load_head_metadata(
        &self,
    ) -> Result<Option<(ArchiveMetadata, Vec<u8>, UpdateVersion)>, ArchiveError> {
        let Some((bytes, version)) = self.read(&self.key("head"), 4096, None).await? else {
            return Ok(None);
        };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(ArchiveError::Invalid(
                "archive head lacks a conditional-write version",
            ));
        }
        let head: Head = manifest::decode(2, &bytes, 4096)?;
        let metadata = self.load_metadata(&head.manifest).await?;
        if Position::from(metadata.position()) != head.position {
            return Err(ArchiveError::Corrupt("archive head/cut binding"));
        }
        Ok(Some((metadata, bytes, version)))
    }

    async fn load_metadata(&self, object: &ObjectRef) -> Result<ArchiveMetadata, ArchiveError> {
        if object.kind != ObjectKind::Manifest || object.bundle.is_some() {
            return Err(ArchiveError::Corrupt("archive head domain"));
        }
        let bytes = self.get_object(object).await?;
        let rooted = bytes.get(8) == Some(&super::root_head::KIND);
        if rooted != (self.image_storage == super::super::catalog::RaftImageStorage::Extents) {
            return Err(ArchiveError::Corrupt("archive image representation"));
        }
        if rooted {
            self.decode_root_metadata(object, &bytes)
                .await
                .map(|metadata| ArchiveMetadata {
                    inner: Metadata::Rooted(Box::new(metadata)),
                })
        } else {
            self.decode_materialized_metadata(object, &bytes)
                .map(|metadata| ArchiveMetadata {
                    inner: Metadata::Materialized(Box::new(metadata)),
                })
        }
    }

    async fn load_recovery_material(
        &self,
        metadata: ArchiveMetadata,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        match metadata.inner {
            Metadata::Materialized(metadata) => {
                self.load_material(*metadata)
                    .await
                    .map(|set| ArchiveRecoverySet {
                        inner: Recovery::Materialized(Box::new(set)),
                    })
            }
            Metadata::Rooted(metadata) => {
                let store = self.root_store.as_ref().ok_or(ArchiveError::Invalid(
                    "extent archive requires a node inventory",
                ))?;
                self.load_root_material(*metadata, store)
                    .await
                    .map(|set| ArchiveRecoverySet {
                        inner: Recovery::Rooted(Box::new(set)),
                    })
            }
        }
    }
}
