// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native extent-image application and marker ownership. The node supplies one
//! shared extent inventory; this group never reconciles or reclaims other
//! groups' roots. SDK working copies and remote root transfer are separate.

mod marker;
mod query;

pub(crate) use query::query_registered_rooted_controlled;
pub use query::{query_registered_rooted, RootQueryOptions};

use super::{
    apply::{validate_committed_update, ImageApplyError},
    cache::{CommittedImage, GroupLog},
    completion::{ApplyCompletion, ApplyTicket},
};
use crate::application::consensus::{EnvelopeLimits, ImageCommitment, LtxImage};
use cellar_logic::consensus::{GroupScope, LogPosition, ProposalId};
use cellar_ltx::{
    apply::{ApplyLimits, LtxSegment},
    internal::{
        ExtentManifest, ExtentRootReference, Geometry, ImageBinding, ImageRootUpdate,
        NodeExtentStore, SealedImageRoot, StoreError,
    },
    ltx,
};
use std::sync::Arc;

/// A complete local image selected by an observed durable native marker. Query
/// consistency, schema registration and output authorization remain separate.
#[derive(Clone)]
pub struct RootPublishedImage {
    image: Arc<SealedImageRoot>,
    position: LogPosition,
    scope: GroupScope,
    proposal: Option<ProposalId>,
    application_digest: [u8; 32],
}

impl RootPublishedImage {
    pub fn image(&self) -> Arc<SealedImageRoot> {
        self.image.clone()
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn scope(&self) -> &GroupScope {
        &self.scope
    }

    pub fn proposal(&self) -> Option<ProposalId> {
        self.proposal
    }

    pub fn application_digest(&self) -> [u8; 32] {
        self.application_digest
    }

    pub fn marker(&self) -> CommittedImage {
        marker::encode(&self.image)
    }
}

struct Pending {
    ticket: ApplyTicket,
    image: Arc<SealedImageRoot>,
    marker: CommittedImage,
    proposal: ProposalId,
}

/// The physical preparation worker for extent commitments. Calls run on the
/// owning blocking domain with an admitted caller-owned 4 MiB workspace and the
/// envelope's decoded-file budget. Preparation exposes only completion metadata;
/// publishing requires observing the exact marker in the durable engine cache.
pub struct RootImageApplier {
    group: GroupLog,
    store: NodeExtentStore,
    application_digest: [u8; 32],
    envelope: EnvelopeLimits,
    limits: ApplyLimits,
    current: Option<RootPublishedImage>,
    snapshot: Option<(LogPosition, Arc<SealedImageRoot>)>,
    pending: Option<Pending>,
}

impl RootImageApplier {
    pub fn recover(
        group: GroupLog,
        store: NodeExtentStore,
        application_digest: [u8; 32],
        envelope: EnvelopeLimits,
        max_image_bytes: u64,
        buffer: &mut [u8],
    ) -> Result<Self, ImageApplyError> {
        envelope.validate().map_err(ImageApplyError::Envelope)?;
        if buffer.len() != cellar_ltx::internal::EXTENT_BYTES as usize
            || !(512..=cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES).contains(&max_image_bytes)
        {
            return Err(ImageApplyError::Invalid("root workspace/image bounds"));
        }
        let limits = ApplyLimits {
            decode: ltx::DecodeLimits {
                max_encoded_bytes: envelope.max_payload_bytes,
                max_decoded_bytes: envelope.max_decoded_bytes,
                max_pages: usize::try_from(envelope.max_pages)
                    .map_err(|_| ImageApplyError::Invalid("page bound"))?,
                max_image_bytes,
                ..Default::default()
            },
            max_files: envelope.max_files,
            max_total_encoded_bytes: envelope.max_payload_bytes,
            max_total_decoded_bytes: envelope.max_decoded_bytes,
            max_total_pages: usize::try_from(envelope.max_pages)
                .map_err(|_| ImageApplyError::Invalid("page bound"))?,
        };
        limits
            .validate()
            .map_err(|_| ImageApplyError::Invalid("root image limits"))?;
        let applied = group.applied().map_err(ImageApplyError::Log)?;
        let current = applied
            .image
            .as_ref()
            .map(|marker| {
                marker::adopt(
                    &group,
                    &store,
                    application_digest,
                    marker,
                    max_image_bytes,
                    buffer,
                )
                .map(|image| RootPublishedImage {
                    image: Arc::new(image),
                    position: applied.position,
                    scope: group.identity().scope.clone(),
                    proposal: None,
                    application_digest,
                })
            })
            .transpose()?;
        let mut applier = Self {
            group,
            store,
            application_digest,
            envelope,
            limits,
            current,
            snapshot: None,
            pending: None,
        };
        applier.refresh_snapshot(buffer)?;
        if let Some(current) = &applier.current {
            applier
                .store
                .retain_reference(
                    marker::reference_key(&applier.group, 1),
                    applied.position.index.get(),
                    current.image.reference(),
                )
                .map_err(ImageApplyError::Extents)?;
        } else if applier.snapshot.is_some() {
            return Err(ImageApplyError::Invalid("missing applied extent image"));
        }
        Ok(applier)
    }

    pub fn current(&self) -> Option<RootPublishedImage> {
        self.current.clone()
    }

    /// Capture a coherent durable engine cut for this exact image. The query
    /// controller must separately authorize the requested read consistency.
    pub fn queryable(&self) -> Result<Option<RootPublishedImage>, ImageApplyError> {
        let Some(mut image) = self.current() else {
            return Ok(None);
        };
        let cache = self.group.read().map_err(ImageApplyError::Log)?;
        let Some(position) = cache.coherent_image_position(&image.marker(), image.position) else {
            return Ok(None);
        };
        image.position = position;
        Ok(Some(image))
    }

    fn refresh_snapshot(&mut self, buffer: &mut [u8]) -> Result<(), ImageApplyError> {
        let (snapshot, applied) = {
            let cache = self.group.read().map_err(ImageApplyError::Log)?;
            (cache.snapshot.clone(), cache.applied.clone())
        };
        if snapshot.get_metadata().index == 1 && snapshot.data.is_empty() {
            if self.snapshot.is_some() {
                return Err(ImageApplyError::Invalid("extent snapshot regressed"));
            }
            return Ok(());
        }
        let max_encoded_bytes = ltx::full_snapshot_bound(self.limits.decode.max_image_bytes, 512)
            .ok_or(ImageApplyError::Invalid("snapshot bound"))?;
        let limits = super::snapshot::SnapshotLimits {
            max_image_bytes: self.limits.decode.max_image_bytes,
            max_encoded_bytes,
            max_chunk_bytes: 65536.min(max_encoded_bytes),
        };
        let marker = super::snapshot::snapshot_image(
            &self.group,
            self.application_digest,
            &snapshot,
            limits,
        )
        .map_err(|error| ImageApplyError::Snapshot(Box::new(error)))?;
        if applied.position.index.get() == snapshot.get_metadata().index && applied.image != marker
        {
            return Err(ImageApplyError::Invalid("snapshot/applied extent image"));
        }
        let Some(marker) = marker else {
            if self.snapshot.is_some() {
                return Err(ImageApplyError::Invalid("extent snapshot lost its image"));
            }
            return Ok(());
        };
        let position = LogPosition {
            index: cellar_logic::consensus::LogIndex::new(snapshot.get_metadata().index),
            term: cellar_logic::consensus::Term::new(snapshot.get_metadata().term),
        };
        if self
            .snapshot
            .as_ref()
            .is_some_and(|(before, image)| *before == position && marker::encode(image) == marker)
        {
            return Ok(());
        }
        let image = marker::adopt(
            &self.group,
            &self.store,
            self.application_digest,
            &marker,
            self.limits.decode.max_image_bytes,
            buffer,
        )?;
        self.store
            .retain_reference(
                marker::reference_key(&self.group, 2),
                position.index.get(),
                image.reference(),
            )
            .map_err(ImageApplyError::Extents)?;
        let previous = self.snapshot.replace((position, Arc::new(image)));
        if let Some((_, old)) = previous {
            if self
                .current
                .as_ref()
                .is_none_or(|current| current.image.reference() != old.reference())
                && self
                    .snapshot
                    .as_ref()
                    .is_none_or(|(_, current)| current.reference() != old.reference())
            {
                self.store
                    .retire_root(old.reference())
                    .map_err(ImageApplyError::Extents)?;
            }
        }
        Ok(())
    }

    pub fn prepare(
        &mut self,
        ticket: ApplyTicket,
        entry: &raft::prelude::Entry,
        buffer: &mut [u8],
    ) -> Result<ApplyCompletion, ImageApplyError> {
        if self.pending.is_some() {
            return Err(ImageApplyError::Busy);
        }
        let update = validate_committed_update(
            &self.group,
            &ticket,
            entry,
            self.current.as_ref().map(RootPublishedImage::marker),
            self.application_digest,
            self.envelope,
        )?;
        let expected = match update.result.commitment {
            ImageCommitment::Extents(reference) => reference,
            ImageCommitment::Sha256(_) => {
                return Err(ImageApplyError::Invalid(
                    "extent apply requires extent commitments",
                ))
            }
        };
        let base = match &self.current {
            Some(current) => current.image.manifest().clone(),
            None => ExtentManifest::new(
                ImageBinding {
                    history: self.group.birth_digest,
                    schema: self.application_digest,
                    epoch: update.base.epoch,
                    txid: 0,
                    checksum: 0,
                },
                Geometry::new(0, 0)
                    .map_err(StoreError::from)
                    .map_err(ImageApplyError::Extents)?,
                &[],
            )
            .map_err(StoreError::from)
            .map_err(ImageApplyError::Extents)?,
        };
        if marker::position(&base)? != update.base {
            return Err(ImageApplyError::Invalid("exact extent LTX base"));
        }
        let last = update
            .files
            .last()
            .ok_or(ImageApplyError::Invalid("empty LTX chain"))?;
        let header = ltx::Header::parse(&last.bytes)
            .map_err(|_| ImageApplyError::Invalid("last LTX header"))?;
        let binding = ImageBinding {
            history: self.group.birth_digest,
            schema: self.application_digest,
            epoch: update.result.epoch,
            txid: update.result.txid,
            checksum: update.result.checksum,
        };
        let geometry = Geometry::new(
            header.page_size,
            u64::from(header.commit) * u64::from(header.page_size),
        )
        .map_err(StoreError::from)
        .map_err(ImageApplyError::Extents)?;
        let segments = update
            .files
            .iter()
            .map(|file| {
                Ok(LtxSegment {
                    bytes: &file.bytes,
                    min_txid: cellar_ltx::TXID(file.min_txid),
                    max_txid: cellar_ltx::TXID(file.max_txid),
                    page_count: usize::try_from(file.page_count)
                        .map_err(|_| ImageApplyError::Invalid("page count"))?,
                    decoded_bytes: file.decoded_bytes,
                })
            })
            .collect::<Result<Vec<_>, ImageApplyError>>()?;
        let prepared = self
            .store
            .prepare_image_root(
                self.current.as_ref().map(|current| current.image.as_ref()),
                &ImageRootUpdate {
                    base: marker::reference(&base)?,
                    result: expected,
                    binding,
                    geometry,
                    segments,
                },
                &self.limits,
                buffer,
            )
            .map_err(ImageApplyError::Extents)?;
        let marker = marker::encode(&prepared.image);
        marker.validate().map_err(ImageApplyError::Log)?;
        self.pending = Some(Pending {
            ticket: ticket.clone(),
            image: Arc::new(prepared.image),
            marker: marker.clone(),
            proposal: update.proposal,
        });
        Ok(ApplyCompletion {
            ticket,
            image: marker,
        })
    }

    /// Engine call success is insufficient. Observe its exact durable marker
    /// before retaining/releasing references or exposing a queryable image.
    pub fn publish(
        &mut self,
        ticket: &ApplyTicket,
        buffer: &mut [u8],
    ) -> Result<RootPublishedImage, ImageApplyError> {
        self.refresh_snapshot(buffer)?;
        let pending = self.pending.as_ref().ok_or(ImageApplyError::Unpublished)?;
        if pending.ticket != *ticket {
            return Err(ImageApplyError::Invalid("pending extent ticket"));
        }
        let applied = self.group.applied().map_err(ImageApplyError::Log)?;
        if applied.position.index < ticket.position().index
            || applied.position.term < ticket.position().term
            || applied.image.as_ref() != Some(&pending.marker)
        {
            return Err(ImageApplyError::Unpublished);
        }
        self.store
            .retain_reference(
                marker::reference_key(&self.group, 1),
                applied.position.index.get(),
                pending.image.reference(),
            )
            .map_err(ImageApplyError::Extents)?;
        let head = RootPublishedImage {
            image: pending.image.clone(),
            position: ticket.position(),
            scope: self.group.identity().scope.clone(),
            proposal: Some(pending.proposal),
            application_digest: self.application_digest,
        };
        let previous = self.current.replace(head.clone());
        self.pending = None;
        if let Some(previous) = previous {
            if previous.image.reference() != head.image.reference()
                && self
                    .snapshot
                    .as_ref()
                    .is_none_or(|(_, image)| image.reference() != previous.image.reference())
            {
                self.store
                    .retire_root(previous.image.reference())
                    .map_err(ImageApplyError::Extents)?;
            }
        }
        Ok(head)
    }
}
