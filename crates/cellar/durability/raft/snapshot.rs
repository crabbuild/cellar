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
use ::raft::prelude::Snapshot;
use cellar_logic::consensus::{GroupScope, LogIndex, LogPosition, Term};
use cellar_ltx::apply::{ApplyLimits, ImageState, ImageUpdate, LtxSegment};
use cellar_ltx::generation::{GenerationError, GenerationPin, GenerationStore};
use cellar_ltx::{ltx, TXID};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub enum SnapshotError {
    Invalid(&'static str),
    Limit,
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
        if !(512..=256 * 1024 * 1024).contains(&self.max_image_bytes)
            || !(512..=512 * 1024 * 1024).contains(&self.max_encoded_bytes)
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
        limits.decode.max_encoded_bytes = self.max_encoded_bytes;
        limits.decode.max_decoded_bytes = self.max_image_bytes;
        limits.decode.max_pages = self.max_image_bytes.div_ceil(512) as usize;
        limits.max_total_encoded_bytes = self.max_encoded_bytes;
        limits.max_total_decoded_bytes = self.max_image_bytes;
        limits.max_total_pages = limits.decode.max_pages;
        limits
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
        descriptor.membership.validate_scope(scope)?;
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
            marker_id(image)?;
            marker_state(image)
                .validate(&limits.apply())
                .map_err(|_| SnapshotError::Invalid("image state"))?;
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
/// Capture/encoding runs on the cell's blocking owner. Network scheduling must
/// separately admit the bounded encoded bytes and transfer lifetime.
pub struct SnapshotPackage {
    group: GroupLog,
    snapshot: Snapshot,
    bytes: Vec<u8>,
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
        let snapshot = group.read()?.snapshot.clone();
        if snapshot != *expected {
            return Err(SnapshotError::Invalid("requested checkpoint replaced"));
        }
        let descriptor = Descriptor::decode(group, schema, &snapshot, limits)?;
        let pin = descriptor
            .image
            .as_ref()
            .map(|image| {
                images
                    .adopt(marker_id(image)?, marker_state(image))
                    .map_err(SnapshotError::Images)
            })
            .transpose()?;
        let bytes = match &pin {
            Some(pin) => encode_image(pin, limits)?,
            None => Vec::new(),
        };
        if bytes.len() != descriptor.encoded_length
            || <[u8; 32]>::from(Sha256::digest(&bytes)) != descriptor.encoded_digest
        {
            return Err(SnapshotError::Invalid(
                "retained checkpoint payload binding",
            ));
        }
        Ok(Self {
            group: group.clone(),
            snapshot,
            bytes,
            pin,
            limits,
            membership: descriptor.membership,
        })
    }

    pub fn capture(
        group: &GroupLog,
        image: PublishedImage,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        let schema = image.application_digest();
        Self::capture_at(group, Some(image), schema, limits)
    }

    pub fn capture_empty(
        group: &GroupLog,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        Self::capture_at(group, None, schema, limits)
    }

    fn capture_at(
        group: &GroupLog,
        image: Option<PublishedImage>,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        limits.validate()?;
        let pin = image.as_ref().map(PublishedImage::pin);
        let (position, conf, membership) = {
            let cache = group.read()?;
            if cache.applied.image != pin.as_ref().map(marker)
                || image.as_ref().is_some_and(|image| {
                    image.scope() != &group.identity().scope
                        || image.position().index > cache.applied.position.index
                        || cache.term(image.position().index.get())
                            != Some(image.position().term.get())
                })
                || cache.applied.position.index.get() > cache.hard_state.commit
            {
                return Err(SnapshotError::Invalid("source durable cut"));
            }
            (
                cache.applied.position,
                manifest::encode_conf(&cache.conf_state)
                    .map_err(|_| SnapshotError::Invalid("source configuration"))?,
                cache.membership.clone(),
            )
        };
        let bytes = match &pin {
            Some(pin) => encode_image(pin, limits)?,
            None => Vec::new(),
        };
        let descriptor = Descriptor {
            format: 1,
            scope: WireScope::from(&group.identity().scope),
            birth: group.birth_digest,
            schema,
            index: position.index.get(),
            term: position.term.get(),
            conf,
            membership,
            image: pin.as_ref().map(marker),
            encoded_length: bytes.len(),
            encoded_digest: Sha256::digest(&bytes).into(),
        };
        let snapshot = descriptor.snapshot()?;
        Descriptor::decode(group, schema, &snapshot, limits)?;
        Ok(Self {
            group: group.clone(),
            snapshot,
            bytes,
            pin,
            limits,
            membership: descriptor.membership,
        })
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.clone()
    }

    pub fn membership(&self) -> super::member_state::GroupMembership {
        self.membership.clone()
    }

    pub fn encoded_length(&self) -> usize {
        self.bytes.len()
    }

    pub fn chunk(&self, offset: usize, length: usize) -> Result<&[u8], SnapshotError> {
        if length == 0 || length > self.limits.max_chunk_bytes {
            return Err(SnapshotError::Limit);
        }
        let end = offset.checked_add(length).ok_or(SnapshotError::Limit)?;
        self.bytes.get(offset..end).ok_or(SnapshotError::Limit)
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
    limits: SnapshotLimits,
    bytes: Vec<u8>,
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
            descriptor,
            limits,
            bytes: Vec::new(),
        })
    }

    pub fn append(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        if offset != self.bytes.len()
            || bytes.is_empty()
            || bytes.len() > self.limits.max_chunk_bytes
            || offset
                .checked_add(bytes.len())
                .is_none_or(|end| end > self.descriptor.encoded_length)
        {
            return Err(SnapshotError::Limit);
        }
        self.bytes
            .try_reserve_exact(self.descriptor.encoded_length - self.bytes.len())
            .map_err(|_| SnapshotError::Limit)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    /// Seal on the destination's blocking owner. Exact existing bytes make a
    /// repeated download idempotent; a colliding identity is never overwritten.
    pub fn finish(self, images: &GenerationStore) -> Result<PreparedSnapshot, SnapshotError> {
        let pin = seal_image(&self.descriptor, &self.bytes, self.limits, images)?;
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

fn encode_image(pin: &GenerationPin, limits: SnapshotLimits) -> Result<Vec<u8>, SnapshotError> {
    let state = pin.state();
    state
        .validate(&limits.apply())
        .map_err(|_| SnapshotError::Invalid("source image"))?;
    if state.txid == TXID(0) || state.length == 0 {
        return Err(SnapshotError::Invalid("empty source"));
    }
    let count = state.length / u64::from(state.page_size);
    let mut reader = pin.open_reader()?;
    let mut hash = Sha256::new();
    let mut pages = Vec::with_capacity(count as usize);
    for index in 0..count {
        let page =
            reader.read_exact_at(index * u64::from(state.page_size), state.page_size as usize)?;
        hash.update(&page);
        pages.push((index as u32 + 1, page));
    }
    // Images are bounded below SQLite's reserved lock page.
    if <[u8; 32]>::from(hash.finalize()) != state.digest {
        return Err(SnapshotError::Invalid("source digest"));
    }
    let header = ltx::Header {
        version: ltx::VERSION,
        flags: if state.checksum == 0 {
            ltx::HEADER_FLAG_NO_CHECKSUM
        } else {
            0
        },
        page_size: state.page_size,
        commit: count as u32,
        min_txid: TXID(1),
        max_txid: state.txid,
        ..Default::default()
    };
    let bytes = ltx::encode_file_v0_5_2(&header, &pages, state.checksum)
        .map_err(|_| SnapshotError::Invalid("source LTX encoding"))?;
    if bytes.len() > limits.max_encoded_bytes {
        return Err(SnapshotError::Limit);
    }
    Ok(bytes)
}

fn seal_image(
    descriptor: &Descriptor,
    bytes: &[u8],
    limits: SnapshotLimits,
    images: &GenerationStore,
) -> Result<Option<GenerationPin>, SnapshotError> {
    if bytes.len() != descriptor.encoded_length
        || <[u8; 32]>::from(Sha256::digest(bytes)) != descriptor.encoded_digest
    {
        return Err(SnapshotError::Invalid("transfer length/digest"));
    }
    let Some(image) = descriptor.image.as_ref() else {
        return Ok(None);
    };
    let state = marker_state(image);
    let decoded = ltx::decode_file_bounded(bytes, limits.apply().decode)
        .map_err(|_| SnapshotError::Invalid("physical LTX"))?;
    if !decoded.header().is_snapshot()
        || decoded.header().max_txid != state.txid
        || decoded.header().page_size != state.page_size
        || u64::from(decoded.header().commit) * u64::from(state.page_size) != state.length
        || decoded.trailer().post_apply_checksum != state.checksum
    {
        return Err(SnapshotError::Invalid("physical image binding"));
    }
    let id = marker_id(image)?;
    let pin = match images.adopt(id, state) {
        Ok(pin) => pin,
        Err(GenerationError::Missing) => images.prepare(
            id,
            None,
            &ImageUpdate {
                base: ImageState::empty(state.epoch)
                    .map_err(|_| SnapshotError::Invalid("image epoch"))?,
                result: state,
                segments: vec![LtxSegment {
                    bytes,
                    min_txid: TXID(1),
                    max_txid: state.txid,
                    page_count: decoded.pages().len(),
                    decoded_bytes: decoded.decoded_bytes(),
                }],
            },
        )?,
        Err(error) => return Err(error.into()),
    };
    Ok(Some(pin))
}

#[path = "snapshot_seed.rs"]
mod seed;
pub(crate) use seed::{PreparedSeed, SeedDownload};
