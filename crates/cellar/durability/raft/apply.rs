//! Physical images become public only after the matching engine marker is durable.
//! This owner handles committed data entries; protocol no-ops/configuration changes
//! remain in the runtime and preserve the image. It grants no leader eligibility.

use super::cache::{CommittedImage, GroupLog};
use super::completion::{ApplyCompletion, ApplyTicket};
use super::storage::LogError;
use crate::application::consensus::{CellUpdate, EnvelopeLimits, LtxImage, ProtocolError};
use ::raft::prelude::{Entry, EntryType};
use cellar_logic::consensus::LogPosition;
use cellar_ltx::apply::{ImageState, ImageUpdate, LtxSegment};
use cellar_ltx::generation::{GenerationError, GenerationPin, GenerationStore, ImageId};
use cellar_ltx::{ltx, TXID};
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Debug)]
pub enum ImageApplyError {
    Invalid(&'static str),
    Busy,
    Unpublished,
    Log(LogError),
    Envelope(ProtocolError),
    Images(GenerationError),
}

impl fmt::Display for ImageApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Raft image application: {self:?}")
    }
}

impl std::error::Error for ImageApplyError {}

/// An image covered by an observed durable engine marker. This does not prove
/// a particular read consistency, schema compatibility or command output gate.
#[derive(Clone)]
pub struct PublishedImage {
    pin: GenerationPin,
    position: LogPosition,
}

impl PublishedImage {
    pub fn pin(&self) -> GenerationPin {
        self.pin.clone()
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }
}

struct Pending {
    ticket: ApplyTicket,
    pin: GenerationPin,
    marker: CommittedImage,
}

pub struct ImageApplier {
    group: GroupLog,
    images: GenerationStore,
    application_digest: [u8; 32],
    limits: EnvelopeLimits,
    current: Option<PublishedImage>,
    pending: Option<Pending>,
}

impl ImageApplier {
    /// Run on the host's blocking execution domain, with the group's exclusively
    /// owned image store. Recovered directory inventory never selects the head.
    pub fn recover(
        group: GroupLog,
        images: GenerationStore,
        application_digest: [u8; 32],
        limits: EnvelopeLimits,
    ) -> Result<Self, ImageApplyError> {
        limits.validate().map_err(ImageApplyError::Envelope)?;
        let applied = group.applied().map_err(ImageApplyError::Log)?;
        let current = applied
            .image
            .as_ref()
            .map(|marker| {
                let id = marker_id(marker)?;
                let pin = images
                    .adopt(id, marker_state(marker))
                    .map_err(ImageApplyError::Images)?;
                Ok::<_, ImageApplyError>(PublishedImage {
                    pin,
                    position: applied.position,
                })
            })
            .transpose()?;
        Ok(Self {
            group,
            images,
            application_digest,
            limits,
            current,
            pending: None,
        })
    }

    pub fn current(&self) -> Option<PublishedImage> {
        self.current.clone()
    }

    /// Return only a runtime completion, never the prepared generation's pin.
    /// The caller submits it to the runtime and waits for durable application.
    pub fn prepare(
        &mut self,
        ticket: ApplyTicket,
        entry: &Entry,
        id: ImageId,
    ) -> Result<ApplyCompletion, ImageApplyError> {
        if self.pending.is_some() {
            return Err(ImageApplyError::Busy);
        }
        if ticket.replica() != self.group.identity()
            || entry.get_entry_type() != EntryType::EntryNormal
            || entry.data.is_empty()
            || entry.index != ticket.position().index.get()
            || entry.term != ticket.position().term.get()
        {
            return Err(ImageApplyError::Invalid("apply ticket/entry identity"));
        }
        {
            let cache = self.group.read().map_err(ImageApplyError::Log)?;
            if cache.entries.get(&entry.index) != Some(entry) {
                return Err(ImageApplyError::Invalid("apply ticket/entry identity"));
            }
            if cache.applied.position.index.get().checked_add(1) != Some(entry.index)
                || cache.hard_state.commit < entry.index
                || cache.applied.image != self.current.as_ref().map(|head| marker(&head.pin))
            {
                return Err(ImageApplyError::Invalid("committed apply head"));
            }
        }
        // Equality with the bounded durable entry precedes serialization, so
        // an unrelated caller-supplied Entry cannot cause an unbounded copy.
        if <[u8; 32]>::from(Sha256::digest(
            entry
                .write_to_bytes()
                .map_err(|_| ImageApplyError::Invalid("entry encoding"))?,
        )) != ticket.entry_digest()
        {
            return Err(ImageApplyError::Invalid("apply ticket/entry identity"));
        }
        let update =
            CellUpdate::decode(&entry.data, self.limits).map_err(ImageApplyError::Envelope)?;
        if update.scope != ticket.replica().scope
            || update.application_digest != self.application_digest
        {
            return Err(ImageApplyError::Invalid("group/schema identity"));
        }
        let base = match &self.current {
            Some(current) => current.pin.state(),
            None => ImageState::empty(update.base.epoch)
                .map_err(|_| ImageApplyError::Invalid("empty base"))?,
        };
        if image_position(base) != update.base {
            return Err(ImageApplyError::Invalid("exact LTX base"));
        }
        let last = update
            .files
            .last()
            .ok_or(ImageApplyError::Invalid("empty chain"))?;
        let header = ltx::Header::parse(&last.bytes)
            .map_err(|_| ImageApplyError::Invalid("last LTX header"))?;
        let result = ImageState {
            epoch: update.result.epoch,
            txid: TXID(update.result.txid),
            checksum: update.result.checksum,
            digest: update.result.image_digest,
            page_size: header.page_size,
            length: header.commit as u64 * header.page_size as u64,
        };
        marker_for(id, result)
            .validate()
            .map_err(ImageApplyError::Log)?;
        let segments = update
            .files
            .iter()
            .map(|file| {
                Ok(LtxSegment {
                    bytes: &file.bytes,
                    min_txid: TXID(file.min_txid),
                    max_txid: TXID(file.max_txid),
                    page_count: usize::try_from(file.page_count)
                        .map_err(|_| ImageApplyError::Invalid("page count"))?,
                    decoded_bytes: file.decoded_bytes,
                })
            })
            .collect::<Result<Vec<_>, ImageApplyError>>()?;
        let pin = self
            .images
            .prepare(
                id,
                self.current.as_ref().map(|h| &h.pin),
                &ImageUpdate {
                    base,
                    result,
                    segments,
                },
            )
            .map_err(ImageApplyError::Images)?;
        let image = marker(&pin);
        self.pending = Some(Pending {
            ticket: ticket.clone(),
            pin,
            marker: image.clone(),
        });
        Ok(ApplyCompletion { ticket, image })
    }

    /// Call after `Applied`, or while reconciling a lost event. A successful
    /// `complete_apply` call is not sufficient: the cache changes only after sync.
    pub fn publish(&mut self, ticket: &ApplyTicket) -> Result<PublishedImage, ImageApplyError> {
        let pending = self.pending.as_ref().ok_or(ImageApplyError::Unpublished)?;
        if pending.ticket != *ticket {
            return Err(ImageApplyError::Invalid("pending ticket"));
        }
        let applied = self.group.applied().map_err(ImageApplyError::Log)?;
        // Later protocol-only entries may already have advanced this cache.
        // They must still name this exact image, never a maximum from another cut.
        if applied.position.index < ticket.position().index
            || applied.position.term < ticket.position().term
            || applied.image.as_ref() != Some(&pending.marker)
        {
            return Err(ImageApplyError::Unpublished);
        }
        let head = PublishedImage {
            pin: pending.pin.clone(),
            position: ticket.position(),
        };
        self.current = Some(head.clone());
        self.pending = None;
        Ok(head)
    }
}

pub(crate) fn image_position(state: ImageState) -> LtxImage {
    LtxImage {
        epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        image_digest: state.digest,
    }
}

fn marker(pin: &GenerationPin) -> CommittedImage {
    marker_for(pin.id(), pin.state())
}

fn marker_for(id: ImageId, state: ImageState) -> CommittedImage {
    CommittedImage {
        name: format!("{}.sqlite", hex::encode(id.bytes())),
        digest: state.digest,
        length: state.length,
        ltx_epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        page_size: state.page_size,
    }
}

fn marker_state(marker: &CommittedImage) -> ImageState {
    ImageState {
        epoch: marker.ltx_epoch,
        txid: TXID(marker.txid),
        checksum: marker.checksum,
        page_size: marker.page_size,
        length: marker.length,
        digest: marker.digest,
    }
}

fn marker_id(marker: &CommittedImage) -> Result<ImageId, ImageApplyError> {
    let name = marker
        .name
        .strip_suffix(".sqlite")
        .ok_or(ImageApplyError::Invalid("image marker name"))?;
    let mut bytes = [0; 16];
    if name.len() != 32
        || !name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ImageApplyError::Invalid("image marker name"));
    }
    hex::decode_to_slice(name, &mut bytes)
        .map_err(|_| ImageApplyError::Invalid("image marker name"))?;
    ImageId::new(bytes).map_err(ImageApplyError::Images)
}
