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

#[path = "snapshot_prepared.rs"]
pub(crate) mod prepared;

#[path = "snapshot_root.rs"]
mod rooted;
pub use rooted::{RootSnapshotSet, RootSnapshotSource};

#[path = "snapshot_pin.rs"]
mod retention;
use retention::SnapshotPin;

#[derive(Debug)]
pub enum SnapshotError {
    Invalid(&'static str),
    Limit,
    Physical(stream_snapshot::Error),
    Log(LogError),
    Archive(crate::raft::archive::ArchiveError),
    Images(GenerationError),
    Extents(cellar_ltx::internal::StoreError),
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
                | Self::Extents(cellar_ltx::internal::StoreError::Limit)
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

impl From<cellar_ltx::internal::StoreError> for SnapshotError {
    fn from(error: cellar_ltx::internal::StoreError) -> Self {
        Self::Extents(error)
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
    // Flat images transfer their encoded file; extent images transfer the
    // canonical manifest followed by each distinct object exactly once.
    transfer_length: usize,
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
                    if descriptor.transfer_length != descriptor.encoded_length {
                        return Err(SnapshotError::Invalid("flat transfer length"));
                    }
                    marker_id(image)?;
                    marker_state(image)?
                        .validate(&limits.apply())
                        .map_err(|_| SnapshotError::Invalid("image state"))?;
                }
                ImageCommitment::Extents(reference) => {
                    if image.length > limits.max_image_bytes
                        || descriptor.encoded_length != reference.encoded_length() as usize
                        || descriptor.transfer_length < descriptor.encoded_length
                        || !u64::try_from(descriptor.transfer_length - descriptor.encoded_length)
                            .is_ok_and(|length| length <= image.length)
                    {
                        return Err(SnapshotError::Invalid("extent snapshot metadata"));
                    }
                }
            }
            if image.length == 0 || image.txid == 0 || descriptor.encoded_length == 0 {
                return Err(SnapshotError::Invalid("empty image"));
            }
        } else if descriptor.encoded_length != 0
            || descriptor.transfer_length != 0
            || descriptor.encoded_digest != <[u8; 32]>::from(Sha256::digest([]))
        {
            return Err(SnapshotError::Invalid("nonempty payload for empty state"));
        }
        if descriptor.encoded_length > limits.max_encoded_bytes
            || descriptor.transfer_length > limits.max_encoded_bytes
        {
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
    bytes: Option<SourcePayload>,
    root: Option<rooted::RootPayload>,
    pin: Option<SnapshotPin>,
    limits: SnapshotLimits,
    membership: super::member_state::GroupMembership,
}

enum SourcePayload {
    Local(stream_snapshot::SnapshotFile),
    Shared(prepared::Shared),
}

impl SourcePayload {
    fn metadata(&self) -> stream_snapshot::Metadata {
        match self {
            Self::Local(file) => file.metadata(),
            Self::Shared(shared) => shared.metadata(),
        }
    }

    fn chunk(&self, offset: usize, length: usize) -> Result<Vec<u8>, SnapshotError> {
        match self {
            Self::Local(file) => {
                let mut bytes = vec![0; length];
                file.read_into(offset as u64, &mut bytes)?;
                Ok(bytes)
            }
            Self::Shared(shared) => shared.chunk(offset, length),
        }
    }

    fn invalidate(&self) {
        if let Self::Shared(shared) = self {
            shared.invalidate();
        }
    }

    fn validate_digest(&self, digest: [u8; 32]) -> Result<(), SnapshotError> {
        if digest != self.metadata().encoded_digest {
            self.invalidate();
            return Err(SnapshotError::Physical(stream_snapshot::Error::Integrity));
        }
        Ok(())
    }
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
        if let Some(root) = &self.root {
            return root.length();
        }
        self.bytes
            .as_ref()
            .map_or(0, |bytes| bytes.metadata().encoded_length as usize)
    }

    pub(crate) fn encoded_file_bytes(&self) -> usize {
        self.bytes
            .as_ref()
            .map_or(0, |bytes| bytes.metadata().encoded_length as usize)
    }

    pub(crate) fn validate_encoded_hash(&self, digest: [u8; 32]) -> Result<(), SnapshotError> {
        if let Some(root) = &self.root {
            // Root streams have manifest/extent commitments, never a fabricated
            // whole-SQLite digest. The ordered source has verified every object.
            return root.complete();
        }
        match &self.bytes {
            Some(bytes) => bytes.validate_digest(digest),
            None if digest == <[u8; 32]>::from(Sha256::digest([])) => Ok(()),
            None => Err(SnapshotError::Physical(stream_snapshot::Error::Integrity)),
        }
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
        if let Some(root) = &self.root {
            return root.chunk(offset, length);
        }
        let source = self.bytes.as_ref().ok_or(SnapshotError::Limit)?;
        // The source reservation already covers this complete wire window.
        // Native providers fill it directly; injected providers retain their
        // bounded fallback. A failed read never exposes a partial response.
        source.chunk(offset, length)
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
        match &self.pin {
            Some(SnapshotPin::Materialized(pin)) => Some(pin.clone()),
            _ => None,
        }
    }
}

/// A verified local cut, not a proposal or proof of checkpoint persistence.
pub struct SnapshotCheckpoint {
    group: GroupLog,
    snapshot: Snapshot,
    pin: Option<SnapshotPin>,
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
            pin.retain(&self.group, self.position().index.get())?;
        }
        Ok(())
    }

    pub(crate) fn retain_pending(&self) -> Result<(), SnapshotError> {
        let Some(pin) = &self.pin else {
            return Ok(());
        };
        pin.retain_pending(&self.group, self.position().index.get())
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

pub(crate) struct DownloadStorage<'a> {
    pub filesystem: &'a dyn cellar_ltx::FileSystem,
    pub directory: &'a std::path::Path,
    pub scratch: &'a stream_snapshot::ScratchPool,
    pub extents: Option<cellar_ltx::internal::NodeExtentStore>,
}

/// A download owns anonymous scratch storage from its execution domain. Empty
/// snapshots carry no body; their descriptor still binds the empty digest.
enum SnapshotPayload {
    Flat(Box<FlatPayload>),
    Rooted(Box<rooted::RootDownload>),
}

impl SnapshotPayload {
    fn new(
        descriptor: &Descriptor,
        limits: SnapshotLimits,
        filesystem: &dyn cellar_ltx::FileSystem,
        directory: &std::path::Path,
        scratch: &stream_snapshot::ScratchPool,
    ) -> Result<Self, SnapshotError> {
        Ok(Self::Flat(Box::new(FlatPayload::new(
            descriptor, limits, filesystem, directory, scratch,
        )?)))
    }

    fn in_extents(
        descriptor: &Descriptor,
        limits: SnapshotLimits,
        store: cellar_ltx::internal::NodeExtentStore,
    ) -> Result<Self, SnapshotError> {
        match descriptor.image.as_ref().map(|image| image.commitment) {
            Some(ImageCommitment::Extents(_)) => Ok(Self::Rooted(Box::new(
                rooted::RootDownload::new(descriptor.clone(), store, limits.max_chunk_bytes),
            ))),
            None => Err(SnapshotError::Invalid(
                "empty root payload uses the empty flat receiver",
            )),
            _ => Err(SnapshotError::Invalid(
                "extent destination requires an extent commitment",
            )),
        }
    }

    fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        match self {
            Self::Flat(payload) => payload.append(offset, bytes),
            Self::Rooted(payload) => payload.append(offset, bytes),
        }
    }

    fn set_io_profile(&mut self, profile: stream_snapshot::IoProfile) {
        if let Self::Flat(payload) = self {
            payload.set_io_profile(profile);
        }
    }

    fn finish(self) -> Result<Option<stream_snapshot::SnapshotFile>, SnapshotError> {
        match self {
            Self::Flat(payload) => payload.finish(),
            Self::Rooted(_) => Err(SnapshotError::Invalid(
                "root receiver requires its extent inventory",
            )),
        }
    }

    fn finish_root(
        self,
        store: &cellar_ltx::internal::NodeExtentStore,
    ) -> Result<Option<SnapshotPin>, SnapshotError> {
        match self {
            Self::Rooted(payload) => payload.finish(store).map(Some),
            Self::Flat(payload) => match payload.finish()? {
                None => Ok(None),
                Some(_) => Err(SnapshotError::Invalid(
                    "flat receiver cannot publish an extent image",
                )),
            },
        }
    }
}

struct FlatPayload {
    download: Option<stream_snapshot::Download>,
    received: usize,
    length: usize,
    chunk_bytes: usize,
}

impl FlatPayload {
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

    fn set_io_profile(&mut self, profile: stream_snapshot::IoProfile) {
        self.download = self
            .download
            .take()
            .map(|download| download.with_io_profile(profile));
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
    /// Transport admission covers codec buffers until sealing or discard.
    pub(crate) fn with_io_profile(mut self, profile: stream_snapshot::IoProfile) -> Self {
        self.payload.set_io_profile(profile);
        self
    }

    pub(crate) fn transfer_length(&self) -> usize {
        self.descriptor.transfer_length
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

    /// Receive canonical extent metadata and unique objects directly into the
    /// destination's admitted node inventory, without a flat SQLite spool.
    pub fn in_extents(
        group: &GroupLog,
        schema: [u8; 32],
        snapshot: Snapshot,
        limits: SnapshotLimits,
        store: cellar_ltx::internal::NodeExtentStore,
    ) -> Result<Self, SnapshotError> {
        let descriptor = Descriptor::decode(group, schema, &snapshot, limits)?;
        descriptor.membership.succeeds(&group.membership()?)?;
        let payload = if descriptor.image.is_none() {
            SnapshotPayload::new(
                &descriptor,
                limits,
                group.filesystem.as_ref(),
                &group.scratch_directory,
                &group.snapshot_scratch,
            )?
        } else {
            SnapshotPayload::in_extents(&descriptor, limits, store)?
        };
        Ok(Self {
            group: group.clone(),
            snapshot,
            descriptor,
            payload,
        })
    }

    pub fn finish_root(
        self,
        store: &cellar_ltx::internal::NodeExtentStore,
    ) -> Result<PreparedSnapshot, SnapshotError> {
        let pin = self.payload.finish_root(store)?;
        if pin.as_ref().map(SnapshotPin::marker) != self.descriptor.image {
            return Err(SnapshotError::Invalid("received root marker binding"));
        }
        Ok(PreparedSnapshot {
            group: self.group,
            snapshot: self.snapshot,
            pin,
            membership: self.descriptor.membership,
        })
    }

    /// Seal on the destination's blocking owner. Exact existing bytes make a
    /// repeated download idempotent; a colliding identity is never overwritten.
    pub fn finish(self, images: &GenerationStore) -> Result<PreparedSnapshot, SnapshotError> {
        let pin = seal_image(&self.descriptor, self.payload.finish()?, images)?;
        Ok(PreparedSnapshot {
            group: self.group,
            snapshot: self.snapshot,
            pin: pin.map(SnapshotPin::Materialized),
            membership: self.descriptor.membership,
        })
    }
}

pub struct PreparedSnapshot {
    group: GroupLog,
    snapshot: Snapshot,
    pin: Option<SnapshotPin>,
    membership: super::member_state::GroupMembership,
}

/// An exclusively verified image's no-I/O retention capability. Only physical
/// preparation constructs it; it carries no leader or membership authority.
#[derive(Clone)]
pub struct SnapshotReference {
    group: GroupLog,
    pin: Option<SnapshotPin>,
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
        ticket.belongs_to(&self.group)
            && ticket.position() == self.position
            && ticket.entry_digest() == self.digest
            && applied.position == self.position
            && applied.image == self.pin.as_ref().map(SnapshotPin::marker)
    }

    pub(crate) fn retain(&self) -> Result<(), SnapshotError> {
        if let Some(pin) = &self.pin {
            pin.retain(&self.group, self.position.index.get())?;
        }
        Ok(())
    }

    pub(crate) fn retain_pending(&self) -> Result<(), SnapshotError> {
        let Some(pin) = &self.pin else {
            return Ok(());
        };
        pin.retain_pending(&self.group, self.position.index.get())
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
        if !ticket.belongs_to(&self.group)
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
                image: self.pin.as_ref().map(SnapshotPin::marker),
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
        let pin = match self.pin {
            Some(SnapshotPin::Materialized(pin)) => pin,
            Some(SnapshotPin::Rooted { .. }) => {
                return Err(SnapshotError::Invalid(
                    "materialized publisher requires a flat image",
                ));
            }
            None => {
                return Err(SnapshotError::Invalid(
                    "empty snapshot has no queryable image",
                ))
            }
        };
        Ok(applier.publish_snapshot(&self.group, &self.snapshot, pin)?)
    }

    /// Observe this destination's exact synced engine marker before exposing
    /// its complete local extent root. Run on the owning physical worker.
    pub fn publish_root(
        self,
        applier: &mut super::rooted::RootImageApplier,
        workspace: &mut [u8],
    ) -> Result<super::rooted::RootPublishedImage, SnapshotError> {
        let Some(SnapshotPin::Rooted { image, .. }) = self.pin else {
            return Err(SnapshotError::Invalid(
                "extent publisher requires a root image",
            ));
        };
        Ok(applier.publish_snapshot(&self.group, &self.snapshot, &image, workspace)?)
    }

    pub fn publish_empty(self, applier: &mut ImageApplier) -> Result<(), SnapshotError> {
        if self.pin.is_some() {
            return Err(SnapshotError::Invalid("snapshot has a data image"));
        }
        applier.publish_empty_snapshot(&self.group, &self.snapshot)?;
        Ok(())
    }

    pub(crate) fn publish_empty_root(
        self,
        applier: &mut super::rooted::RootImageApplier,
    ) -> Result<(), SnapshotError> {
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

/// Recover retention for every durable local group, including groups absent
/// from this process's application registrations. The schema comes from the
/// canonical native descriptor solely to preserve bytes, never to serve them.
pub(crate) fn retained_image(group: &GroupLog) -> Result<Option<CommittedImage>, SnapshotError> {
    let (snapshot, applied) = {
        let cache = group.read()?;
        (cache.snapshot.clone(), cache.applied.clone())
    };
    if snapshot.get_metadata().index == 1 && snapshot.data.is_empty() {
        return Ok(None);
    }
    if snapshot.compute_size() > 64 * 1024 {
        return Err(SnapshotError::Limit);
    }
    let declared: Descriptor = serde_json::from_slice(&snapshot.data)
        .map_err(|_| SnapshotError::Invalid("retained snapshot descriptor"))?;
    let image = snapshot_image(
        group,
        declared.schema,
        &snapshot,
        SnapshotLimits {
            max_image_bytes: stream_snapshot::MAX_IMAGE_BYTES,
            max_encoded_bytes: stream_snapshot::MAX_ENCODED_BYTES as usize,
            ..Default::default()
        },
    )?;
    if applied.position.index.get() == snapshot.get_metadata().index && applied.image != image {
        return Err(SnapshotError::Invalid(
            "retained snapshot/applied image binding",
        ));
    }
    Ok(image)
}

fn encode_image(
    pin: &GenerationPin,
    limits: SnapshotLimits,
    profile: stream_snapshot::IoProfile,
) -> Result<stream_snapshot::SnapshotFile, SnapshotError> {
    Ok(pin.snapshot_with_io_profile(limits.physical(), profile, None)?)
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
pub(crate) use seed::DisasterSeed;
pub(crate) use seed::{PreparedSeed, SeedDownload};
