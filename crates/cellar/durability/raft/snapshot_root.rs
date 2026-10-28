// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root metadata and complete local closure transfer beneath native admission.
//! The existing descriptor binds the exact Raft cut; its body is the canonical
//! manifest, never a flat SQLite hash or a full-image encoded spool.

use super::*;
use crate::raft::{catalog::GroupBirth, member_state::GroupMembership, rooted::RootPublishedImage};
use cellar_ltx::internal::{
    ExtentId, ExtentManifest, ExtentRootReference, ImageBinding, NodeExtentStore, SealedImageRoot,
};
use std::{collections::BTreeSet, io, sync::Arc};

#[path = "snapshot_root_stream.rs"]
mod stream;
pub(super) use stream::{RootDownload, RootPayload};

/// A coherent committed source. The node's transport separately admits its
/// lifetime and each at-most-4-MiB extent read on a registered blocking owner.
/// Capture reads metadata only and retains the sealed root's actual byte owner.
pub struct RootSnapshotSource {
    group: GroupLog,
    image: Arc<SealedImageRoot>,
    snapshot: Snapshot,
    manifest: Vec<u8>,
    limits: SnapshotLimits,
}

impl RootSnapshotSource {
    pub(super) fn retained(
        group: &GroupLog,
        image: Arc<SealedImageRoot>,
        schema: [u8; 32],
        expected: &Snapshot,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let descriptor = Descriptor::decode(group, schema, expected, limits)?;
        if group.read()?.snapshot != *expected
            || descriptor.image.as_ref() != Some(&super::super::rooted::marker::encode(&image))
        {
            return Err(SnapshotError::Invalid("retained root checkpoint binding"));
        }
        let manifest = image
            .manifest()
            .encode()
            .map_err(cellar_ltx::internal::StoreError::from)?;
        // Source membership may have advanced beyond this exact retained cut.
        // The receiver separately requires the incoming configuration to
        // succeed its own state; a sender must not substitute a newer cut.
        RootSnapshotSet::bound(expected, descriptor, &manifest)?;
        Ok(Self {
            group: group.clone(),
            image,
            snapshot: expected.clone(),
            manifest,
            limits,
        })
    }

    pub(super) fn schema(&self) -> [u8; 32] {
        self.image.manifest().binding().schema
    }

    pub fn capture(
        group: &GroupLog,
        image: RootPublishedImage,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        limits.validate()?;
        let marker = image.marker();
        let schema = image.application_digest();
        let (position, conf, membership) = {
            let cache = group.read()?;
            if cache.applied.image.as_ref() != Some(&marker)
                || image.scope() != &group.identity().scope
                || image.position().index > cache.applied.position.index
                || cache.term(image.position().index.get()) != Some(image.position().term.get())
                || cache.applied.position.index.get() > cache.hard_state.commit
            {
                return Err(SnapshotError::Invalid("root source durable cut"));
            }
            (
                cache.applied.position,
                manifest::encode_conf(&cache.conf_state)?,
                cache.membership.clone(),
            )
        };
        let pin = image.image();
        let bytes = pin
            .manifest()
            .encode()
            .map_err(cellar_ltx::internal::StoreError::from)?;
        let descriptor = Descriptor {
            format: 1,
            scope: WireScope::from(&group.identity().scope),
            birth: group.birth_digest,
            schema,
            index: position.index.get(),
            term: position.term.get(),
            conf,
            membership,
            image: Some(marker),
            encoded_length: bytes.len(),
            encoded_digest: Sha256::digest(&bytes).into(),
            transfer_length: stream::transfer_length(pin.manifest(), bytes.len())?,
        };
        let snapshot = descriptor.snapshot()?;
        // Use the same decoder as a receiver. A root pointer, source pin or
        // matching final digest cannot bypass the complete metadata binding.
        RootSnapshotSet::for_group(group, schema, &snapshot, &bytes, limits)?;
        Ok(Self {
            group: group.clone(),
            image: pin,
            snapshot,
            manifest: bytes,
            limits,
        })
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.clone()
    }

    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }

    /// Reuse the sealed source proof only in its actual inventory. This is a
    /// metadata operation; callers must establish remote confirmation separately.
    pub(in crate::raft) fn local_image(
        &self,
        store: &NodeExtentStore,
    ) -> Result<Arc<SealedImageRoot>, SnapshotError> {
        self.validate_owner(&self.group)?;
        if !store.owns_image(&self.image) {
            return Err(SnapshotError::Invalid("root source inventory owner"));
        }
        Ok(self.image.clone())
    }

    /// A cached source may not be rebound to another open engine instance.
    /// Passing this check does not create a live native source grant.
    pub fn validate_owner(&self, group: &GroupLog) -> Result<(), SnapshotError> {
        if self.group.identity() != group.identity()
            || !Arc::ptr_eq(&self.group.owner, &group.owner)
        {
            return Err(SnapshotError::Invalid("root source engine owner"));
        }
        drop(group.read()?);
        Ok(())
    }

    /// Recheck exact bytes before returning a complete object to an admitted
    /// sender. Foreign extents are refused even if present in the node store.
    pub fn read_extent(&self, id: ExtentId) -> Result<Vec<u8>, SnapshotError> {
        self.validate_owner(&self.group)?;
        self.image
            .view()
            .read_extent(id)
            .map_err(SnapshotError::from)
    }

    /// Build the ordinary admitted transfer package without an image spool.
    /// The supplied node inventory must independently hold the complete root.
    pub fn into_package(
        self,
        store: &NodeExtentStore,
        workspace: &mut [u8],
    ) -> Result<SnapshotPackage, SnapshotError> {
        self.validate_owner(&self.group)?;
        let descriptor = Descriptor::decode(
            &self.group,
            self.image.manifest().binding().schema,
            &self.snapshot,
            self.limits,
        )?;
        let set = RootSnapshotSet::bound(&self.snapshot, descriptor, &self.manifest)?;
        let image = set.seal(store, workspace)?;
        let descriptor = Descriptor::decode(
            &self.group,
            image.manifest().binding().schema,
            &self.snapshot,
            self.limits,
        )?;
        Ok(SnapshotPackage {
            group: self.group.clone(),
            snapshot: self.snapshot.clone(),
            bytes: None,
            pin: Some(SnapshotPin::Rooted {
                image: Arc::new(image),
                store: store.clone(),
            }),
            limits: self.limits,
            membership: descriptor.membership,
            root: Some(RootPayload::new(self)?),
        })
    }
}

/// Bound snapshot metadata and its canonical dependency set. This value is
/// neither a durable local image nor permission to vote, install or serve it.
pub struct RootSnapshotSet {
    snapshot: Snapshot,
    marker: CommittedImage,
    reference: ExtentRootReference,
    manifest: ExtentManifest,
    dependencies: BTreeSet<ExtentId>,
    membership: GroupMembership,
}

impl RootSnapshotSet {
    pub fn for_group(
        group: &GroupLog,
        schema: [u8; 32],
        snapshot: &Snapshot,
        bytes: &[u8],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let descriptor = Descriptor::decode(group, schema, snapshot, limits)?;
        descriptor.membership.succeeds(&group.membership()?)?;
        Self::bound(snapshot, descriptor, bytes)
    }

    /// A fresh learner knows its catalog birth before it has a local group.
    /// A later native install must still check its opaque live seed grant.
    pub fn for_seed(
        birth: &GroupBirth,
        schema: [u8; 32],
        snapshot: &Snapshot,
        bytes: &[u8],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let mut conf = ::raft::prelude::ConfState::default();
        conf.set_voters(
            birth
                .voters()
                .iter()
                .map(|member| member.member.get())
                .collect(),
        );
        conf.set_learners(
            birth
                .learners()
                .iter()
                .map(|member| member.member.get())
                .collect(),
        );
        Self::for_archive(
            birth.scope(),
            birth.digest(),
            &GroupMembership::initial(&conf),
            schema,
            snapshot,
            bytes,
            limits,
        )
    }

    /// Archives retain these exact immutable catalog bindings already. Borrow
    /// them without another per-cell copy of the complete birth record.
    pub(crate) fn for_archive(
        scope: &GroupScope,
        birth: [u8; 32],
        initial: &GroupMembership,
        schema: [u8; 32],
        snapshot: &Snapshot,
        bytes: &[u8],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let descriptor = Descriptor::decode_bound(scope, birth, initial, schema, snapshot, limits)?;
        Self::bound(snapshot, descriptor, bytes)
    }

    fn bound(
        snapshot: &Snapshot,
        descriptor: Descriptor,
        bytes: &[u8],
    ) -> Result<Self, SnapshotError> {
        let marker = descriptor
            .image
            .ok_or(SnapshotError::Invalid("root snapshot has no image"))?;
        let reference = match marker.commitment {
            ImageCommitment::Extents(reference) => reference,
            ImageCommitment::Sha256(_) => {
                return Err(SnapshotError::Invalid("extent root commitment required"))
            }
        };
        if bytes.len() != reference.encoded_length() as usize
            || bytes.len() != descriptor.encoded_length
            || <[u8; 32]>::from(Sha256::digest(bytes)) != descriptor.encoded_digest
        {
            return Err(SnapshotError::Invalid("root manifest payload binding"));
        }
        let manifest = ExtentManifest::decode(bytes, reference.root())
            .map_err(cellar_ltx::internal::StoreError::from)?;
        if manifest.binding()
            != (ImageBinding {
                history: descriptor.birth,
                schema: descriptor.schema,
                epoch: marker.ltx_epoch,
                txid: marker.txid,
                checksum: marker.checksum,
            })
            || manifest.geometry().length() != marker.length
            || manifest.geometry().page_size() != marker.page_size
        {
            return Err(SnapshotError::Invalid(
                "root manifest history/schema/image binding",
            ));
        }
        let mut dependencies = BTreeSet::new();
        for index in 0..manifest.geometry().extent_count() {
            dependencies.insert(
                manifest
                    .extent(index)
                    .ok_or(SnapshotError::Invalid("root manifest extent coverage"))?,
            );
        }
        if stream::transfer_length(&manifest, bytes.len())? != descriptor.transfer_length {
            return Err(SnapshotError::Invalid("root transfer closure length"));
        }
        Ok(Self {
            snapshot: snapshot.clone(),
            marker,
            reference,
            manifest,
            dependencies,
            membership: descriptor.membership,
        })
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn marker(&self) -> &CommittedImage {
        &self.marker
    }

    pub(crate) fn membership(&self) -> &GroupMembership {
        &self.membership
    }

    pub fn reference(&self) -> ExtentRootReference {
        self.reference
    }

    pub fn extents(&self) -> impl Iterator<Item = ExtentId> + '_ {
        self.dependencies.iter().copied()
    }

    pub(in crate::raft) fn contains_extent(&self, id: ExtentId) -> bool {
        self.dependencies.contains(&id)
    }

    /// Protect the declared closure from node GC while individual imports and
    /// their network waits run. This metadata pin is not a local image seal.
    pub(crate) fn pin_receive(
        &self,
        store: &NodeExtentStore,
    ) -> Result<cellar_ltx::internal::RootView, SnapshotError> {
        Ok(store.stage_transient_root(self.manifest.clone())?)
    }

    /// Run on the node's physical owner with the complete object already under
    /// receive admission. NodeExtentStore independently verifies/syncs its own
    /// destination and never overwrites an existing corrupt object.
    pub fn install_extent(
        &self,
        store: &NodeExtentStore,
        id: ExtentId,
        bytes: &[u8],
    ) -> Result<(), SnapshotError> {
        if !self.dependencies.contains(&id) || bytes.len() != id.length() as usize {
            return Err(SnapshotError::Invalid(
                "extent outside received root or wrong length",
            ));
        }
        store.put_extent(id, |offset, count| {
            let start = usize::try_from(offset)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            let end = start
                .checked_add(count)
                .ok_or(io::ErrorKind::InvalidInput)?;
            bytes
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| io::ErrorKind::UnexpectedEof.into())
        })?;
        Ok(())
    }

    /// Only a complete independently verified local closure can produce this
    /// seal. Native engine publication and authorization remain separate gates.
    pub fn seal(
        &self,
        store: &NodeExtentStore,
        workspace: &mut [u8],
    ) -> Result<SealedImageRoot, SnapshotError> {
        store
            .seal_image_root(self.manifest.clone(), workspace)
            .map_err(SnapshotError::from)
    }

    /// Prepare this complete destination root for the ordinary native snapshot
    /// ticket. Revalidate against the receiving engine's current configuration;
    /// this does not persist a marker or make the image queryable.
    pub fn prepare(
        self,
        group: &GroupLog,
        schema: [u8; 32],
        store: &NodeExtentStore,
        limits: SnapshotLimits,
        workspace: &mut [u8],
    ) -> Result<PreparedSnapshot, SnapshotError> {
        let descriptor = Descriptor::decode(group, schema, &self.snapshot, limits)?;
        descriptor.membership.succeeds(&group.membership()?)?;
        let image = self.seal(store, workspace)?;
        self.prepare_sealed(group, schema, store, limits, Arc::new(image))
    }

    /// The physical archive owner already sealed this exact local closure.
    /// Reuse that capability, never a same-named root from another inventory.
    pub(crate) fn prepare_sealed(
        self,
        group: &GroupLog,
        schema: [u8; 32],
        store: &NodeExtentStore,
        limits: SnapshotLimits,
        image: Arc<SealedImageRoot>,
    ) -> Result<PreparedSnapshot, SnapshotError> {
        let descriptor = Descriptor::decode(group, schema, &self.snapshot, limits)?;
        descriptor.membership.succeeds(&group.membership()?)?;
        if !store.owns_image(&image) || super::super::rooted::marker::encode(&image) != self.marker
        {
            return Err(SnapshotError::Invalid("sealed root marker binding"));
        }
        Ok(PreparedSnapshot {
            group: group.clone(),
            snapshot: self.snapshot,
            pin: Some(SnapshotPin::Rooted {
                image,
                store: store.clone(),
            }),
            membership: descriptor.membership,
        })
    }
}
