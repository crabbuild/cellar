// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root archive publication and exact-reference material. Full roots rotate
//! without flat encoding; every selected dependency is still checked before
//! a conditional head or a receiver's complete local closure is returned.

use super::representation::Metadata;
use super::rooted::root_error;
use super::*;
use crate::application::consensus::protocol::WireScope;
use crate::raft::{
    rooted::RootPublishedImage,
    snapshot::{RootSnapshotSet, RootSnapshotSource, SnapshotLimits},
};
use cellar_ltx::internal::NodeExtentStore;

pub(super) const KIND: u8 = 5;

#[derive(Clone)]
pub(super) struct RootCut {
    source: Arc<RootSnapshotSource>,
    store: NodeExtentStore,
    scope: GroupScope,
    birth_digest: [u8; 32],
    schema: [u8; 32],
    pub position: LogPosition,
    pub image_bytes: u64,
    pub memory_bytes: usize,
    dependencies: Vec<ArchiveDependency>,
    membership: super::super::member_state::GroupMembership,
}

fn snapshot_limits(limits: ArchiveLimits) -> Result<SnapshotLimits, ArchiveError> {
    let limits = SnapshotLimits {
        max_image_bytes: limits.max_image_bytes,
        max_encoded_bytes: usize::try_from(limits.max_snapshot_bytes)
            .map_err(|_| ArchiveError::Limit)?,
        ..Default::default()
    };
    limits.validate().map_err(root_error)?;
    Ok(limits)
}

impl RootCut {
    pub fn capture(
        group: &GroupLog,
        image: RootPublishedImage,
        store: NodeExtentStore,
        dependencies: Vec<ArchiveDependency>,
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        limits.validate()?;
        if !store.owns_image(&image.image()) || image.scope() != &group.identity().scope {
            return Err(ArchiveError::Invalid("root archive cut owner"));
        }
        if dependencies.len() > limits.max_dependencies {
            return Err(ArchiveError::Limit);
        }
        let scope = image.scope().clone();
        let schema = image.application_digest();
        let source = Arc::new(
            RootSnapshotSource::capture(group, image, snapshot_limits(limits)?)
                .map_err(root_error)?,
        );
        let snapshot = source.snapshot();
        let set = RootSnapshotSet::for_group(
            group,
            schema,
            &snapshot,
            source.manifest(),
            snapshot_limits(limits)?,
        )
        .map_err(root_error)?;
        let position = Position {
            index: snapshot.get_metadata().index,
            term: snapshot.get_metadata().term,
        }
        .decode()?;
        let membership = set.membership().clone();
        let mut memory_bytes = source
            .manifest()
            .len()
            .checked_add(snapshot.compute_size() as usize)
            .and_then(|bytes| bytes.checked_add(membership.reserved_bytes()))
            .ok_or(ArchiveError::Limit)?;
        for dependency in &dependencies {
            memory_bytes = memory_bytes
                .checked_add(dependency.0.len())
                .ok_or(ArchiveError::Limit)?;
            if dependency.0.len() > limits.max_object_bytes || memory_bytes > limits.max_set_bytes {
                return Err(ArchiveError::Limit);
            }
        }
        if memory_bytes > limits.max_capture_bytes || memory_bytes > limits.max_set_bytes {
            return Err(ArchiveError::Limit);
        }
        Ok(Self {
            source,
            store,
            scope,
            birth_digest: group.birth_digest,
            schema,
            position,
            image_bytes: set.marker().length,
            memory_bytes,
            dependencies,
            membership,
        })
    }

    pub fn validate(&self, archive: &CellArchive) -> Result<(), ArchiveError> {
        if archive.image_storage != super::super::catalog::RaftImageStorage::Extents
            || self.scope != archive.scope
            || self.birth_digest != archive.birth
            || self.schema != archive.schema
            || !self.membership.same_birth(&archive.initial_membership)
        {
            return Err(ArchiveError::Invalid(
                "root archive producer history/schema",
            ));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RootHead {
    scope: WireScope,
    root_digest: [u8; 32],
    pub birth_digest: [u8; 32],
    pub schema_digest: [u8; 32],
    snapshot: RootArchiveReference,
    dependencies: Vec<ObjectRef>,
}

pub(super) struct RootMetadata {
    pub reference: ArchiveReference,
    pub scope: GroupScope,
    pub position: LogPosition,
    pub head: RootHead,
    pub snapshot: RootSnapshotSet,
    _retention: Option<Arc<super::super::budget::Permit>>,
}

pub(super) struct RootRecoverySet {
    pub metadata: RootMetadata,
    pub root: RootArchiveSnapshot,
    pub dependencies: Vec<(ObjectRef, Vec<u8>)>,
}

impl RootHead {
    fn validate(&self, archive: &CellArchive) -> Result<LogPosition, ArchiveError> {
        if self.scope != WireScope::from(&archive.scope)
            || self.root_digest != archive.root
            || self.birth_digest != archive.birth
            || self.schema_digest != archive.schema
        {
            return Err(ArchiveError::Corrupt("root head history/schema binding"));
        }
        let position = self.snapshot.position()?;
        if self.dependencies.len() > archive.limits.max_dependencies {
            return Err(ArchiveError::Limit);
        }
        let mut previous = None;
        let mut bytes = self.snapshot.metadata_bytes();
        for object in &self.dependencies {
            object.validate_location()?;
            if object.kind != ObjectKind::Dependency
                || object.range.is_some()
                || object.bundle.is_some()
                || previous.is_some_and(|digest| digest >= object.digest)
            {
                return Err(ArchiveError::Corrupt("root head dependency domain/order"));
            }
            previous = Some(object.digest);
            bytes = bytes
                .checked_add(object.length)
                .ok_or(ArchiveError::Limit)?;
            if object.length > archive.limits.max_object_bytes as u64 {
                return Err(ArchiveError::Limit);
            }
        }
        if bytes > archive.limits.max_set_bytes as u64 {
            return Err(ArchiveError::Limit);
        }
        Ok(position)
    }
}

impl CellArchive {
    pub(super) async fn decode_root_metadata(
        &self,
        object: &ObjectRef,
        bytes: &[u8],
    ) -> Result<RootMetadata, ArchiveError> {
        let head: RootHead = manifest::decode(KIND, bytes, self.limits.max_manifest_bytes)?;
        let position = head.validate(self)?;
        if object.kind != ObjectKind::Manifest
            || object.bundle.is_some()
            || object.range
                != Some(PositionRange {
                    first: position.into(),
                    last: position.into(),
                })
        {
            return Err(ArchiveError::Corrupt("root head object range/domain"));
        }
        let snapshot = self.root_index(&head.snapshot).await?;
        Ok(RootMetadata {
            reference: ArchiveReference(object.clone()),
            scope: self.scope.clone(),
            position,
            head,
            snapshot,
            _retention: self.read_retention.clone(),
        })
    }

    pub(super) async fn load_root_material(
        &self,
        metadata: RootMetadata,
        store: &NodeExtentStore,
    ) -> Result<RootRecoverySet, ArchiveError> {
        let root = self
            .load_root_snapshot_at(&metadata.head.snapshot, store)
            .await?;
        self.assemble_root_material(metadata, root).await
    }

    async fn assemble_root_material(
        &self,
        metadata: RootMetadata,
        root: RootArchiveSnapshot,
    ) -> Result<RootRecoverySet, ArchiveError> {
        if root.snapshot() != metadata.snapshot.snapshot() {
            return Err(ArchiveError::Corrupt("root head snapshot binding"));
        }
        let mut dependencies = Vec::with_capacity(metadata.head.dependencies.len());
        let reads = metadata
            .head
            .dependencies
            .iter()
            .map(|object| self.get_object(object))
            .collect::<Vec<_>>();
        let mut reads = futures_util::stream::iter(reads).buffered(READ_WINDOW);
        let mut objects = metadata.head.dependencies.iter();
        while let Some(bytes) = reads.try_next().await? {
            let object = objects
                .next()
                .ok_or(ArchiveError::Corrupt("root dependency coverage"))?;
            dependencies.push((object.clone(), bytes));
        }
        drop(reads);
        Ok(RootRecoverySet {
            metadata,
            root,
            dependencies,
        })
    }

    pub(super) async fn publish_root_inner(
        &self,
        cut: RootCut,
    ) -> Result<RootRecoverySet, ArchiveError> {
        cut.validate(self)?;
        tracing::info!(event = "raft_archive_phase", phase = "root_publish_start", cell = %self.scope.cell, index = cut.position.index.get(), image_bytes = cut.image_bytes);
        for _ in 0..8 {
            let previous = self.load_head_metadata().await?;
            let mut previous_root = None;
            let mode = previous
                .as_ref()
                .map_or(PutMode::Create, |(_, _, version)| {
                    PutMode::Update(version.clone())
                });
            if let Some((metadata, _, _)) = previous {
                let Metadata::Rooted(metadata) = metadata.inner else {
                    return Err(ArchiveError::Invalid("archive representation changed"));
                };
                let old = metadata.position;
                if old.index >= cut.position.index {
                    if old.term < cut.position.term
                        || old.index == cut.position.index
                            && metadata.snapshot.snapshot() != &cut.source.snapshot()
                    {
                        return Err(ArchiveError::Conflict);
                    }
                    return self.load_root_material(*metadata, &cut.store).await;
                }
                if old.term > cut.position.term {
                    return Err(ArchiveError::Conflict);
                }
                // Never hide corrupt selected history by uploading replacement
                // objects from the current source before checking the old set.
                tracing::info!(event = "raft_archive_phase", phase = "root_previous_verify_start", cell = %self.scope.cell, index = old.index.get());
                // Keep only the verified root for this CAS attempt. Drop the
                // previous Blob bodies before assembling the new dependency
                // set, preserving the existing bounded material admission.
                previous_root = Some(self.load_root_material(*metadata, &cut.store).await?.root);
                tracing::info!(event = "raft_archive_phase", phase = "root_previous_verify_done", cell = %self.scope.cell, index = old.index.get());
            }
            let confirmed = self
                .confirm_root_snapshot(cut.source.clone(), previous_root.as_ref())
                .await?;
            let snapshot = confirmed.reference().clone();
            let mut dependencies = BTreeMap::new();
            for dependency in &cut.dependencies {
                let object = self
                    .put_object(ObjectKind::Dependency, &dependency.0, None)
                    .await?;
                if let Some(old) = dependencies.insert(object.digest, object.clone()) {
                    if old != object {
                        return Err(ArchiveError::Conflict);
                    }
                }
            }
            let head = RootHead {
                scope: WireScope::from(&self.scope),
                root_digest: self.root,
                birth_digest: self.birth,
                schema_digest: self.schema,
                snapshot,
                dependencies: dependencies.into_values().collect(),
            };
            let position = head.validate(self)?;
            if position != cut.position {
                return Err(ArchiveError::Corrupt("root archive capture position"));
            }
            let bytes = manifest::encode(KIND, &head, self.limits.max_manifest_bytes)?;
            let object = self
                .put_object(
                    ObjectKind::Manifest,
                    &bytes,
                    Some(PositionRange {
                        first: position.into(),
                        last: position.into(),
                    }),
                )
                .await?;
            let metadata = self.decode_root_metadata(&object, &bytes).await?;
            let root = confirmed
                .bind(&metadata.head.snapshot, &cut.store, &self.host)
                .await?;
            let set = self.assemble_root_material(metadata, root).await?;
            let desired = manifest::encode(
                2,
                &Head {
                    manifest: object,
                    position: position.into(),
                },
                4096,
            )?;
            let outcome = self
                .io(
                    self.deadline()?,
                    self.store.put_opts(
                        &self.key("head"),
                        desired.clone().into(),
                        PutOptions::from(mode),
                    ),
                )
                .await;
            match self.read(&self.key("head"), 4096, None).await? {
                Some((observed, _)) if observed == desired => {
                    tracing::info!(event = "raft_archive_phase", phase = "root_head_confirmed", cell = %self.scope.cell, index = position.index.get());
                    return Ok(set);
                }
                Some(_) => continue,
                None => {
                    return match outcome {
                        Err(error) => Err(error),
                        Ok(_) => Err(ArchiveError::Unconfirmed),
                    }
                }
            }
        }
        Err(ArchiveError::Unconfirmed)
    }
}
