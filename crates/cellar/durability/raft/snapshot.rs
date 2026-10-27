// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded physical snapshots. Sealed bytes supply no membership or leadership
//! authority; the runtime's exact ticket and durable engine install do that.

use super::apply::{marker, marker_id, marker_state, ImageApplier, PublishedImage};
use super::cache::{CommittedImage, GroupLog, GroupWrite, SnapshotChange};
use super::completion::{ApplyTicket, SnapshotCompletion};
use super::manifest;
use super::recovery::{PENDING_SNAPSHOT_REFERENCE, SNAPSHOT_REFERENCE};
use super::storage::LogError;
use crate::application::consensus::protocol::WireScope;
use crate::application::consensus::ImageCommitment;
use ::raft::prelude::Snapshot;
use cellar_logic::consensus::{GroupScope, LogIndex, LogPosition, Term};
use cellar_ltx::apply::ApplyLimits;
use cellar_ltx::generation::{GenerationError, GenerationPin, GenerationStore};
use cellar_ltx::{ltx, stream_snapshot};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[path = "snapshot_capture.rs"]
mod capture;
pub(crate) use capture::SnapshotCapture;

#[derive(Debug)]
pub enum SnapshotError {
    Invalid(&'static str),
    Limit,
    Physical(stream_snapshot::Error),
    Log(LogError),
    Archive(crate::raft::archive::ArchiveError),
    Images(GenerationError),
    Publication(super::apply::ImageApplyError),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Raft snapshot: {self:?}")
    }
}

impl std::error::Error for SnapshotError {}

impl SnapshotError {
    pub(crate) fn scratch_full(&self) -> bool {
        matches!(
            self,
            Self::Physical(stream_snapshot::Error::ScratchFull)
                | Self::Images(GenerationError::Snapshot(
                    stream_snapshot::Error::ScratchFull
                ))
        ) || matches!(self, Self::Archive(error) if error.scratch_full())
    }
}

impl From<super::archive::ArchiveError> for SnapshotError {
    fn from(error: super::archive::ArchiveError) -> Self {
        Self::Archive(error)
    }
}

impl From<stream_snapshot::Error> for SnapshotError {
    fn from(error: stream_snapshot::Error) -> Self {
        Self::Physical(error)
    }
}

impl From<LogError> for SnapshotError {
    fn from(error: LogError) -> Self {
        Self::Log(error)
    }
}

impl From<GenerationError> for SnapshotError {
    fn from(error: GenerationError) -> Self {
        Self::Images(error)
    }
}

impl From<super::apply::ImageApplyError> for SnapshotError {
    fn from(error: super::apply::ImageApplyError) -> Self {
        Self::Publication(error)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SnapshotLimits {
    pub max_image_bytes: u64,
    pub max_encoded_bytes: usize,
    pub max_chunk_bytes: usize,
}

impl Default for SnapshotLimits {
    fn default() -> Self {
        Self {
            max_image_bytes: 64 * 1024 * 1024,
            max_encoded_bytes: 128 * 1024 * 1024,
            max_chunk_bytes: 1024 * 1024,
        }
    }
}

impl SnapshotLimits {
    pub fn validate(self) -> Result<(), SnapshotError> {
        if !(512..=stream_snapshot::MAX_IMAGE_BYTES).contains(&self.max_image_bytes)
            || !(512..=stream_snapshot::MAX_ENCODED_BYTES as usize)
                .contains(&self.max_encoded_bytes)
            || !(1..=1024 * 1024).contains(&self.max_chunk_bytes)
            || self.max_chunk_bytes > self.max_encoded_bytes
            || ltx::full_snapshot_bound(self.max_image_bytes, 512)
                .is_none_or(|bound| bound > self.max_encoded_bytes)
        {
            return Err(SnapshotError::Limit);
        }
        Ok(())
    }

    pub(crate) fn apply(self) -> ApplyLimits {
        let mut limits = ApplyLimits::default();
        limits.decode.max_image_bytes = self.max_image_bytes;
        limits
    }

    fn physical(self) -> stream_snapshot::Limits {
        stream_snapshot::Limits {
            max_image_bytes: self.max_image_bytes,
            max_encoded_bytes: self.max_encoded_bytes as u64,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    format: u32,
    scope: WireScope,
    birth: [u8; 32],
    schema: [u8; 32],
    index: u64,
    term: u64,
    conf: Vec<u8>,
    membership: super::member_state::GroupMembership,
    image: Option<CommittedImage>,
    encoded_length: usize,
    encoded_digest: [u8; 32],
}

impl Descriptor {
    fn decode(
        group: &GroupLog,
        schema: [u8; 32],
        snapshot: &Snapshot,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        Self::decode_bound(
            &group.identity().scope,
            group.birth_digest,
            &group.membership()?,
            schema,
            snapshot,
            limits,
        )
    }

    fn decode_bound(
        scope: &GroupScope,
        birth: [u8; 32],
        initial: &super::member_state::GroupMembership,
        schema: [u8; 32],
        snapshot: &Snapshot,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        limits.validate()?;
        if snapshot.compute_size() > 64 * 1024 || snapshot.data.is_empty() {
            return Err(SnapshotError::Limit);
        }
        let descriptor: Self = serde_json::from_slice(&snapshot.data)
            .map_err(|_| SnapshotError::Invalid("descriptor encoding"))?;
        let metadata = snapshot.get_metadata();
        let conf = manifest::encode_conf(metadata.get_conf_state())
            .map_err(|_| SnapshotError::Invalid("configuration"))?;
        descriptor
            .membership
            .validate(metadata.get_conf_state(), metadata.index)?;
        if !descriptor.membership.same_birth(initial) {
            return Err(SnapshotError::Invalid("snapshot birth members"));
        }
        // Compare the complete protobuf as well as canonical JSON: unknown
        // fields, alternate encodings and a different ConfState cannot hide
        // outside the digest bound to a runtime ticket.
        let canonical = descriptor.snapshot()?;
        if descriptor.format != 1
            || descriptor.scope != WireScope::from(scope)
            || descriptor.birth != birth
            || descriptor.schema != schema
            || descriptor.index < 2
            || descriptor.index >= u64::MAX - 2
            || descriptor.term == 0
            || descriptor.term >= u64::MAX - 2
            || descriptor.index != metadata.index
            || descriptor.term != metadata.term
            || descriptor.conf != conf
            || canonical != *snapshot
        {
            return Err(SnapshotError::Invalid("history/schema/position binding"));
        }
        if let Some(image) = &descriptor.image {
            image.validate()?;
            match image.commitment {
                ImageCommitment::Sha256(_) => {
                    marker_id(image)?;
                    marker_state(image)?
                        .validate(&limits.apply())
                        .map_err(|_| SnapshotError::Invalid("image state"))?;
                }
                ImageCommitment::Extents(reference) => {
                    if image.length > limits.max_image_bytes
                        || descriptor.encoded_length != reference.encoded_length() as usize
                    {
                        return Err(SnapshotError::Invalid("extent snapshot metadata"));
                    }
                }
            }
            if image.length == 0 || image.txid == 0 || descriptor.encoded_length == 0 {
                return Err(SnapshotError::Invalid("empty image"));
            }
        } else if descriptor.encoded_length != 0
            || descriptor.encoded_digest != <[u8; 32]>::from(Sha256::digest([]))
        {
            return Err(SnapshotError::Invalid("nonempty payload for empty state"));
        }
        if descriptor.encoded_length > limits.max_encoded_bytes {
            return Err(SnapshotError::Limit);
        }
        Ok(descriptor)
    }

    fn snapshot(&self) -> Result<Snapshot, SnapshotError> {
        let data =
            serde_json::to_vec(self).map_err(|_| SnapshotError::Invalid("descriptor encoding"))?;
        if data.len() > 48 * 1024 {
            return Err(SnapshotError::Limit);
        }
        let conf = manifest::decode_conf(&self.conf)
            .map_err(|_| SnapshotError::Invalid("configuration"))?;
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = self.index;
        snapshot.mut_metadata().term = self.term;
        snapshot.mut_metadata().set_conf_state(conf);
        snapshot.data = data.into();
        if snapshot.compute_size() > 64 * 1024 {
            return Err(SnapshotError::Limit);
        }
        Ok(snapshot)
    }
}

/// A source transfer pins the image until its owner finishes or cancels it.
/// Metadata capture runs on the cell's SQL owner. Physical encoding may run on
/// a separately admitted blocking owner while the immutable pin retains bytes.
/// Network scheduling separately admits transfer memory and lifetime.
pub struct SnapshotPackage {
    group: GroupLog,
    snapshot: Snapshot,
    bytes: Option<stream_snapshot::SnapshotFile>,
    pin: Option<GenerationPin>,
    limits: SnapshotLimits,
    membership: super::member_state::GroupMembership,
}

impl SnapshotPackage {
    /// Serve an exact durable checkpoint, including when newer commands have
    /// replaced the applied image. Run on the owning blocking domain. A failed
    /// capture never substitutes a newer checkpoint for the requested ticket.
    pub fn retained(
        group: &GroupLog,
        images: &GenerationStore,
        schema: [u8; 32],
        expected: &Snapshot,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        SnapshotCapture::retained(group, images, schema, expected, limits)?.encode()
    }

    pub fn capture(
        group: &GroupLog,
        image: PublishedImage,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        SnapshotCapture::capture(group, image, limits)?.encode()
    }

    pub fn capture_empty(
        group: &GroupLog,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        SnapshotCapture::capture_empty(group, schema, limits)?.encode()
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.clone()
    }

    pub fn membership(&self) -> super::member_state::GroupMembership {
        self.membership.clone()
    }

    pub fn encoded_length(&self) -> usize {
        payload_identity(self.bytes.as_ref()).0
    }

    /// Bounded file I/O; call on the owning blocking domain. The returned chunk
    /// is independent of the full image size and never exceeds the wire bound.
    pub fn chunk(&self, offset: usize, length: usize) -> Result<Vec<u8>, SnapshotError> {
        if length == 0
            || length > self.limits.max_chunk_bytes
            || offset
                .checked_add(length)
                .is_none_or(|n| n > self.encoded_length())
        {
            return Err(SnapshotError::Limit);
        }
        let source = self.bytes.as_ref().ok_or(SnapshotError::Limit)?;
        let mut bytes = Vec::with_capacity(length);
        while bytes.len() < length {
            let n = (length - bytes.len()).min(64 * 1024);
            bytes.extend_from_slice(&source.chunk((offset + bytes.len()) as u64, n)?);
        }
        Ok(bytes)
    }

    /// Mechanical fixture write with data publication quiesced. A live runtime
    /// must use `checkpoint_material` and its serialized checkpoint operation.
    pub fn checkpoint(&self) -> GroupWrite {
        let mut write = GroupWrite::new(&self.group);
        write.snapshot = Some(SnapshotChange::Checkpoint(self.snapshot.clone()));
        write
    }

    /// Only metadata and its verified pin enter consensus queues. Encoded image
    /// bytes remain with the separately admitted transfer owner.
    pub fn checkpoint_material(&self) -> SnapshotCheckpoint {
        SnapshotCheckpoint {
            group: self.group.clone(),
            snapshot: self.snapshot.clone(),
            pin: self.pin.clone(),
        }
    }

    pub fn source_pin(&self) -> Option<GenerationPin> {
        self.pin.clone()
    }
}

/// A verified local cut, not a proposal or proof of checkpoint persistence.
pub struct SnapshotCheckpoint {
    group: GroupLog,
    snapshot: Snapshot,
    pin: Option<GenerationPin>,
}

impl SnapshotCheckpoint {
    pub fn scope(&self) -> &GroupScope {
        &self.group.identity().scope
    }

    pub fn position(&self) -> LogPosition {
        LogPosition {
            index: LogIndex::new(self.snapshot.get_metadata().index),
            term: Term::new(self.snapshot.get_metadata().term),
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        self.snapshot.compute_size() as usize
    }

    pub(crate) fn validate(&self, group: &GroupLog) -> Result<(), SnapshotError> {
        if self.group.identity() != group.identity()
            || !std::sync::Arc::ptr_eq(&self.group.owner, &group.owner)
        {
            return Err(SnapshotError::Invalid("checkpoint engine owner"));
        }
        let cache = group.read()?;
        let position = self.position();
        if position.index > cache.applied.position.index
            || cache.term(position.index.get()) != Some(position.term.get())
            || position.index.get() < cache.snapshot.get_metadata().index
            || (position.index.get() == cache.snapshot.get_metadata().index
                && self.snapshot != cache.snapshot)
        {
            return Err(SnapshotError::Invalid("stale/conflicting checkpoint"));
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> Result<[u8; 32], SnapshotError> {
        Ok(Sha256::digest(
            self.snapshot
                .write_to_bytes()
                .map_err(|_| SnapshotError::Invalid("checkpoint protobuf"))?,
        )
        .into())
    }

    pub(crate) fn write(&self) -> GroupWrite {
        let mut write = GroupWrite::new(&self.group);
        write.snapshot = Some(SnapshotChange::Checkpoint(self.snapshot.clone()));
        write
    }

    pub(crate) fn retain(&self) -> Result<(), SnapshotError> {
        if let Some(pin) = &self.pin {
            pin.retain_reference(SNAPSHOT_REFERENCE, self.position().index.get())?;
            pin.release_reference(PENDING_SNAPSHOT_REFERENCE, self.position().index.get())?;
        }
        Ok(())
    }

    pub(crate) fn retain_pending(&self) -> Result<(), SnapshotError> {
        let Some(pin) = &self.pin else {
            return Ok(());
        };
        if !pin.retain_reference(PENDING_SNAPSHOT_REFERENCE, self.position().index.get())? {
            return Err(SnapshotError::Invalid("stale prospective checkpoint"));
        }
        Ok(())
    }
}

/// Ordered, bounded assembly. A transport owner must limit simultaneous
/// downloads; neither this buffer nor a completed image activates a member.
pub struct SnapshotDownload {
    group: GroupLog,
    snapshot: Snapshot,
    descriptor: Descriptor,
    payload: SnapshotPayload,
}

/// A download owns anonymous scratch storage from its execution domain. Empty
/// snapshots carry no body; their descriptor still binds the empty digest.
struct SnapshotPayload {
    download: Option<stream_snapshot::Download>,
    received: usize,
    length: usize,
    chunk_bytes: usize,
}

impl SnapshotPayload {
    fn new(
        descriptor: &Descriptor,
        limits: SnapshotLimits,
        filesystem: &dyn cellar_ltx::FileSystem,
        directory: &std::path::Path,
        scratch: &stream_snapshot::ScratchPool,
    ) -> Result<Self, SnapshotError> {
        let download = descriptor
            .image
            .as_ref()
            .map(|image| {
                Ok::<_, SnapshotError>(stream_snapshot::Download::new_admitted_in(
                    filesystem,
                    Some(directory),
                    stream_snapshot::Metadata {
                        image: marker_state(image)?,
                        encoded_length: descriptor.encoded_length as u64,
                        encoded_digest: descriptor.encoded_digest,
                    },
                    limits.physical(),
                    scratch,
                )?)
            })
            .transpose()?;
        Ok(Self {
            download,
            received: 0,
            length: descriptor.encoded_length,
            chunk_bytes: limits.max_chunk_bytes,
        })
    }

    fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        if offset != self.received
            || bytes.is_empty()
            || bytes.len() > self.chunk_bytes
            || offset
                .checked_add(bytes.len())
                .is_none_or(|n| n > self.length)
        {
            return Err(SnapshotError::Limit);
        }
        let download = self.download.as_mut().ok_or(SnapshotError::Limit)?;
        download.append_window(self.received as u64, bytes)?;
        self.received += bytes.len();
        Ok(())
    }

    fn finish(self) -> Result<Option<stream_snapshot::SnapshotFile>, SnapshotError> {
        if self.received != self.length {
            return Err(SnapshotError::Invalid("short download"));
        }
        Ok(self
            .download
            .map(stream_snapshot::Download::finish)
            .transpose()?)
    }
}

impl SnapshotDownload {
    pub(crate) fn encoded_length(&self) -> usize {
        self.descriptor.encoded_length
    }

    pub fn new(
        group: &GroupLog,
        schema: [u8; 32],
        snapshot: Snapshot,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        drop(group.read()?);
        let descriptor = Descriptor::decode(group, schema, &snapshot, limits)?;
        descriptor.membership.succeeds(&group.membership()?)?;
        Ok(Self {
            group: group.clone(),
            snapshot,
            payload: SnapshotPayload::new(
                &descriptor,
                limits,
                group.filesystem.as_ref(),
                &group.scratch_directory,
                &group.snapshot_scratch,
            )?,
            descriptor,
        })
    }

    pub fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        self.payload.append(offset, bytes)
    }

    /// Seal on the destination's blocking owner. Exact existing bytes make a
    /// repeated download idempotent; a colliding identity is never overwritten.
    pub fn finish(self, images: &GenerationStore) -> Result<PreparedSnapshot, SnapshotError> {
        let pin = seal_image(&self.descriptor, self.payload.finish()?, images)?;
        Ok(PreparedSnapshot {
            group: self.group,
            snapshot: self.snapshot,
            pin,
            membership: self.descriptor.membership,
        })
    }
}

pub struct PreparedSnapshot {
    group: GroupLog,
    snapshot: Snapshot,
    pin: Option<GenerationPin>,
    membership: super::member_state::GroupMembership,
}

/// An exclusively verified image's no-I/O retention capability. Only physical
/// preparation constructs it; it carries no leader or membership authority.
#[derive(Clone)]
pub struct SnapshotReference {
    group: GroupLog,
    pin: Option<GenerationPin>,
    position: LogPosition,
    digest: [u8; 32],
    membership: super::member_state::GroupMembership,
}

impl std::fmt::Debug for SnapshotReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotReference")
            .field("replica", self.group.identity())
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

impl SnapshotReference {
    pub(crate) fn membership(&self) -> super::member_state::GroupMembership {
        self.membership.clone()
    }

    pub(crate) fn matches(
        &self,
        ticket: &ApplyTicket,
        applied: &super::cache::DurableApply,
    ) -> bool {
        ticket.replica() == self.group.identity()
            && ticket.position() == self.position
            && ticket.entry_digest() == self.digest
            && applied.position == self.position
            && applied.image == self.pin.as_ref().map(marker)
    }

    pub(crate) fn retain(&self) -> Result<(), SnapshotError> {
        if let Some(pin) = &self.pin {
            pin.retain_reference(SNAPSHOT_REFERENCE, self.position.index.get())?;
            pin.release_reference(PENDING_SNAPSHOT_REFERENCE, self.position.index.get())?;
        }
        Ok(())
    }

    pub(crate) fn retain_pending(&self) -> Result<(), SnapshotError> {
        let Some(pin) = &self.pin else {
            return Ok(());
        };
        if !pin.retain_reference(PENDING_SNAPSHOT_REFERENCE, self.position.index.get())? {
            return Err(SnapshotError::Invalid("stale prospective install"));
        }
        Ok(())
    }
}

impl PreparedSnapshot {
    pub fn completion(&self, ticket: &ApplyTicket) -> Result<SnapshotCompletion, SnapshotError> {
        let metadata = self.snapshot.get_metadata();
        let digest: [u8; 32] = Sha256::digest(
            self.snapshot
                .write_to_bytes()
                .map_err(|_| SnapshotError::Invalid("snapshot protobuf"))?,
        )
        .into();
        if ticket.replica() != self.group.identity()
            || ticket.position().index.get() != metadata.index
            || ticket.position().term.get() != metadata.term
            || ticket.entry_digest() != digest
        {
            return Err(SnapshotError::Invalid("runtime install ticket"));
        }
        Ok(SnapshotCompletion {
            ticket: ticket.clone(),
            applied: super::cache::DurableApply {
                position: ticket.position(),
                image: self.pin.as_ref().map(marker),
            },
            reference: Some(SnapshotReference {
                group: self.group.clone(),
                pin: self.pin.clone(),
                position: ticket.position(),
                digest,
                membership: self.membership.clone(),
            }),
        })
    }

    /// This observes the durable engine marker. An accepted completion alone
    /// does not make the image queryable or acknowledge installation.
    pub fn publish(self, applier: &mut ImageApplier) -> Result<PublishedImage, SnapshotError> {
        let pin = self.pin.ok_or(SnapshotError::Invalid(
            "empty snapshot has no queryable image",
        ))?;
        Ok(applier.publish_snapshot(&self.group, &self.snapshot, pin)?)
    }

    pub fn publish_empty(self, applier: &mut ImageApplier) -> Result<(), SnapshotError> {
        if self.pin.is_some() {
            return Err(SnapshotError::Invalid("snapshot has a data image"));
        }
        applier.publish_empty_snapshot(&self.group, &self.snapshot)?;
        Ok(())
    }
}

pub(crate) fn snapshot_image(
    group: &GroupLog,
    schema: [u8; 32],
    snapshot: &Snapshot,
    limits: SnapshotLimits,
) -> Result<Option<CommittedImage>, SnapshotError> {
    let descriptor = Descriptor::decode(group, schema, snapshot, limits)?;
    let cache = group.read()?;
    if cache.applied.position.index.get() == snapshot.get_metadata().index
        && cache.membership != descriptor.membership
    {
        return Err(SnapshotError::Invalid(
            "snapshot/applied member bindings at the same cut",
        ));
    }
    Ok(descriptor.image)
}

fn encode_image(
    pin: &GenerationPin,
    limits: SnapshotLimits,
) -> Result<stream_snapshot::SnapshotFile, SnapshotError> {
    Ok(pin.snapshot(limits.physical())?)
}

fn payload_identity(bytes: Option<&stream_snapshot::SnapshotFile>) -> (usize, [u8; 32]) {
    match bytes {
        Some(bytes) => (
            bytes.metadata().encoded_length as usize,
            bytes.metadata().encoded_digest,
        ),
        None => (0, Sha256::digest([]).into()),
    }
}

fn seal_image(
    descriptor: &Descriptor,
    payload: Option<stream_snapshot::SnapshotFile>,
    images: &GenerationStore,
) -> Result<Option<GenerationPin>, SnapshotError> {
    if payload_identity(payload.as_ref()) != (descriptor.encoded_length, descriptor.encoded_digest)
    {
        return Err(SnapshotError::Invalid("transfer length/digest"));
    }
    let Some(image) = descriptor.image.as_ref() else {
        return Ok(None);
    };
    let snapshot = payload.ok_or(SnapshotError::Invalid("missing image payload"))?;
    let state = marker_state(image)?;
    if snapshot.metadata().image != state {
        return Err(SnapshotError::Invalid("physical image binding"));
    }
    let id = marker_id(image)?;
    let pin = match images.adopt(id, state) {
        Ok(pin) => {
            // A repeated transfer must still prove all of its physical bytes;
            // an existing name cannot launder a malformed encoded payload.
            snapshot.verify()?;
            pin
        }
        Err(GenerationError::Missing) => images.prepare_snapshot(id, &snapshot)?,
        Err(error) => return Err(error.into()),
    };
    Ok(Some(pin))
}

#[path = "snapshot_seed.rs"]
mod seed;
pub(crate) use seed::{PreparedSeed, SeedDownload};
