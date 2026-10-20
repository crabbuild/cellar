//! Bounded physical snapshots. Sealed bytes supply no membership or leadership
//! authority; the runtime's exact ticket and durable engine install do that.

use super::apply::{marker, marker_id, marker_state, ImageApplier, PublishedImage};
use super::cache::{CommittedImage, GroupLog, GroupWrite, SnapshotChange};
use super::completion::{ApplyTicket, SnapshotCompletion};
use super::manifest;
use super::storage::LogError;
use crate::application::consensus::protocol::WireScope;
use ::raft::prelude::Snapshot;
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
            max_encoded_bytes: 64 * 1024 * 1024,
            max_chunk_bytes: 1024 * 1024,
        }
    }
}

impl SnapshotLimits {
    pub fn validate(self) -> Result<(), SnapshotError> {
        if !(512..=64 * 1024 * 1024).contains(&self.max_image_bytes)
            || !(512..=64 * 1024 * 1024).contains(&self.max_encoded_bytes)
            || !(1..=1024 * 1024).contains(&self.max_chunk_bytes)
            || self.max_chunk_bytes > self.max_encoded_bytes
        {
            return Err(SnapshotError::Limit);
        }
        Ok(())
    }

    fn apply(self) -> ApplyLimits {
        let mut limits = ApplyLimits::default();
        limits.decode.max_image_bytes = self.max_image_bytes;
        limits.decode.max_encoded_bytes = self.max_encoded_bytes;
        limits.max_total_encoded_bytes = self.max_encoded_bytes;
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
    image: CommittedImage,
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
        limits.validate()?;
        if snapshot.compute_size() > 64 * 1024 || snapshot.data.is_empty() {
            return Err(SnapshotError::Limit);
        }
        let descriptor: Self = serde_json::from_slice(&snapshot.data)
            .map_err(|_| SnapshotError::Invalid("descriptor encoding"))?;
        let metadata = snapshot.get_metadata();
        let conf = manifest::encode_conf(metadata.get_conf_state())
            .map_err(|_| SnapshotError::Invalid("configuration"))?;
        // Compare the complete protobuf as well as canonical JSON: unknown
        // fields, alternate encodings and a different ConfState cannot hide
        // outside the digest bound to a runtime ticket.
        let canonical = descriptor.snapshot()?;
        if descriptor.format != 1
            || descriptor.scope != WireScope::from(&group.identity().scope)
            || descriptor.birth != group.birth_digest
            || descriptor.schema != schema
            || descriptor.index < 2
            || descriptor.index >= u64::MAX - 2
            || descriptor.term == 0
            || descriptor.term >= u64::MAX - 2
            || descriptor.index != metadata.index
            || descriptor.term != metadata.term
            || descriptor.conf != conf
            || canonical != *snapshot
            || descriptor.encoded_length == 0
        {
            return Err(SnapshotError::Invalid("history/schema/position binding"));
        }
        descriptor.image.validate()?;
        marker_id(&descriptor.image)?;
        marker_state(&descriptor.image)
            .validate(&limits.apply())
            .map_err(|_| SnapshotError::Invalid("image state"))?;
        if descriptor.image.length == 0 || descriptor.image.txid == 0 {
            return Err(SnapshotError::Invalid("empty image"));
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
    pin: GenerationPin,
    limits: SnapshotLimits,
}

impl SnapshotPackage {
    pub fn capture(
        group: &GroupLog,
        image: PublishedImage,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        limits.validate()?;
        let pin = image.pin();
        let (position, conf) = {
            let cache = group.read()?;
            if image.scope() != &group.identity().scope
                || cache.applied.image.as_ref() != Some(&marker(&pin))
                || image.position().index > cache.applied.position.index
                || cache.term(image.position().index.get()) != Some(image.position().term.get())
                || cache.applied.position.index.get() > cache.hard_state.commit
            {
                return Err(SnapshotError::Invalid("source durable cut"));
            }
            (
                cache.applied.position,
                manifest::encode_conf(&cache.conf_state)
                    .map_err(|_| SnapshotError::Invalid("source configuration"))?,
            )
        };
        let bytes = encode_image(&pin, limits)?;
        let descriptor = Descriptor {
            format: 1,
            scope: WireScope::from(&group.identity().scope),
            birth: group.birth_digest,
            schema: image.application_digest(),
            index: position.index.get(),
            term: position.term.get(),
            conf,
            image: marker(&pin),
            encoded_length: bytes.len(),
            encoded_digest: Sha256::digest(&bytes).into(),
        };
        let snapshot = descriptor.snapshot()?;
        Descriptor::decode(group, image.application_digest(), &snapshot, limits)?;
        Ok(Self {
            group: group.clone(),
            snapshot,
            bytes,
            pin,
            limits,
        })
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.clone()
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

    /// Persist through the node log owner before advertising this checkpoint.
    /// Keep this package (or another pin) alive until the sync has completed.
    pub fn checkpoint(&self) -> GroupWrite {
        let mut write = GroupWrite::new(&self.group);
        write.snapshot = Some(SnapshotChange::Checkpoint(self.snapshot.clone()));
        write
    }

    pub fn source_pin(&self) -> GenerationPin {
        self.pin.clone()
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
    pub fn new(
        group: &GroupLog,
        schema: [u8; 32],
        snapshot: Snapshot,
        limits: SnapshotLimits,
    ) -> Result<Self, SnapshotError> {
        drop(group.read()?);
        let descriptor = Descriptor::decode(group, schema, &snapshot, limits)?;
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
            .try_reserve_exact(bytes.len())
            .map_err(|_| SnapshotError::Limit)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    /// Seal on the destination's blocking owner. Exact existing bytes make a
    /// repeated download idempotent; a colliding identity is never overwritten.
    pub fn finish(self, images: &GenerationStore) -> Result<PreparedSnapshot, SnapshotError> {
        if self.bytes.len() != self.descriptor.encoded_length
            || <[u8; 32]>::from(Sha256::digest(&self.bytes)) != self.descriptor.encoded_digest
        {
            return Err(SnapshotError::Invalid("transfer length/digest"));
        }
        let state = marker_state(&self.descriptor.image);
        let decoded = ltx::decode_file_bounded(&self.bytes, self.limits.apply().decode)
            .map_err(|_| SnapshotError::Invalid("physical LTX"))?;
        if !decoded.header().is_snapshot()
            || decoded.header().max_txid != state.txid
            || decoded.header().page_size != state.page_size
            || u64::from(decoded.header().commit) * u64::from(state.page_size) != state.length
            || decoded.trailer().post_apply_checksum != state.checksum
        {
            return Err(SnapshotError::Invalid("physical image binding"));
        }
        let id = marker_id(&self.descriptor.image)?;
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
                        bytes: &self.bytes,
                        min_txid: TXID(1),
                        max_txid: state.txid,
                        page_count: decoded.pages().len(),
                        decoded_bytes: decoded.decoded_bytes(),
                    }],
                },
            )?,
            Err(error) => return Err(error.into()),
        };
        Ok(PreparedSnapshot {
            group: self.group,
            snapshot: self.snapshot,
            pin,
        })
    }
}

pub struct PreparedSnapshot {
    group: GroupLog,
    snapshot: Snapshot,
    pin: GenerationPin,
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
                image: Some(marker(&self.pin)),
            },
        })
    }

    /// This observes the durable engine marker. An accepted completion alone
    /// does not make the image queryable or acknowledge installation.
    pub fn publish(self, applier: &mut ImageApplier) -> Result<PublishedImage, SnapshotError> {
        Ok(applier.publish_snapshot(&self.group, &self.snapshot, self.pin)?)
    }
}

pub(crate) fn recover_pin(
    group: &GroupLog,
    images: &GenerationStore,
    schema: [u8; 32],
) -> Result<Option<GenerationPin>, SnapshotError> {
    let (snapshot, applied) = {
        let cache = group.read()?;
        (cache.snapshot.clone(), cache.applied.clone())
    };
    if snapshot.get_metadata().index == 1 && snapshot.data.is_empty() {
        return Ok(None);
    }
    let descriptor = Descriptor::decode(group, schema, &snapshot, SnapshotLimits::default())?;
    if applied.position.index.get() == snapshot.get_metadata().index
        && applied.image.as_ref() != Some(&descriptor.image)
    {
        return Err(SnapshotError::Invalid(
            "snapshot/applied image at the same cut",
        ));
    }
    Ok(Some(images.adopt(
        marker_id(&descriptor.image)?,
        marker_state(&descriptor.image),
    )?))
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
