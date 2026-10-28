//! Fixed publication cuts pair the durable configuration with its SQL image.
//! A data position can precede configuration-only applied progress.
use super::*;
use crate::application::consensus::ImageCommitment;
use crate::raft::cache::{CommittedImage, DurableApply};
use crate::raft::inspection::MemberInspection;
use crate::raft::member_state::FleetGroupBinding;
use cellar_ltx::{apply::ImageState, TXID};
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "host_fleet_cut_tests.rs"]
mod tests;

/// Logical published image state with an explicit physical hash domain.
/// This descriptor grants no local seal, activation or quorum authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetImageState {
    epoch: u64,
    txid: u64,
    checksum: u64,
    page_size: u32,
    length: u64,
    commitment: ImageCommitment,
}

impl FleetImageState {
    pub(crate) fn from_marker(image: &CommittedImage) -> Result<Self, CommandFailure> {
        image.validate().map_err(|_| CommandFailure::CorruptState)?;
        let state = Self {
            epoch: image.ltx_epoch,
            txid: image.txid,
            checksum: image.checksum,
            page_size: image.page_size,
            length: image.length,
            commitment: image.commitment,
        };
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn new(
        epoch: u64,
        txid: u64,
        checksum: u64,
        page_size: u32,
        length: u64,
        commitment: ImageCommitment,
    ) -> Result<Self, CommandFailure> {
        let state = Self {
            epoch,
            txid,
            checksum,
            page_size,
            length,
            commitment,
        };
        state.validate()?;
        Ok(state)
    }

    pub(crate) fn validate(&self) -> Result<(), CommandFailure> {
        if self.epoch == 0
            || self.txid == 0
            || self.length == 0
            || self.length > cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES
            || !cellar_ltx::ltx::is_valid_page_size(self.page_size)
            || !self.length.is_multiple_of(u64::from(self.page_size))
        {
            return Err(CommandFailure::CorruptState);
        }
        match self.commitment {
            ImageCommitment::Sha256(_) => self
                .materialized()?
                .validate(&crate::raft::archive::apply_limits())
                .map_err(|_| CommandFailure::CorruptState),
            ImageCommitment::Extents(reference) => {
                let expected = 112 + 32 * self.length.div_ceil(cellar_ltx::internal::EXTENT_BYTES);
                if u64::from(reference.encoded_length()) != expected {
                    return Err(CommandFailure::CorruptState);
                }
                Ok(())
            }
        }
    }

    /// The domain tag is part of image identity; root bytes are never a
    /// materialized-file digest, even when the byte values happen to match.
    pub fn commitment(&self) -> ImageCommitment {
        self.commitment
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn txid(&self) -> u64 {
        self.txid
    }

    pub fn checksum(&self) -> u64 {
        self.checksum
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    /// Only a materialized commitment can describe a full SQLite file digest.
    pub fn materialized(&self) -> Result<ImageState, CommandFailure> {
        let digest = self
            .commitment
            .sha256()
            .map_err(|_| CommandFailure::UnsupportedCapability)?;
        Ok(ImageState {
            epoch: self.epoch,
            txid: TXID(self.txid),
            checksum: self.checksum,
            page_size: self.page_size,
            length: self.length,
            digest,
        })
    }

    pub(crate) fn same_representation(&self, other: &Self) -> bool {
        matches!(
            (self.commitment, other.commitment),
            (ImageCommitment::Sha256(_), ImageCommitment::Sha256(_))
                | (ImageCommitment::Extents(_), ImageCommitment::Extents(_))
        )
    }
}

impl From<ImageState> for FleetImageState {
    fn from(state: ImageState) -> Self {
        Self {
            epoch: state.epoch,
            txid: state.txid.0,
            checksum: state.checksum,
            page_size: state.page_size,
            length: state.length,
            commitment: state.digest.into(),
        }
    }
}

/// An immutable published data image. This value alone supplies neither a
/// quorum barrier nor permission to activate a member or acknowledge a write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetPublishedImage {
    position: LogPosition,
    state: FleetImageState,
}

impl FleetPublishedImage {
    pub(crate) fn new(position: LogPosition, state: ImageState) -> Result<Self, CommandFailure> {
        Self::new_state(position, state.into())
    }

    pub(crate) fn new_state(
        position: LogPosition,
        state: FleetImageState,
    ) -> Result<Self, CommandFailure> {
        state.validate()?;
        if position.index.get() == 0
            || position.term.get() == 0
            || position.index.get() >= u64::MAX - 2
            || position.term.get() >= u64::MAX - 2
        {
            return Err(CommandFailure::CorruptState);
        }
        Ok(Self { position, state })
    }

    pub fn state(&self) -> FleetImageState {
        self.state
    }

    pub fn commitment(&self) -> ImageCommitment {
        self.state.commitment()
    }

    /// The publication's committed position. Restoring the same image may
    /// relabel it at a later configuration-only cut.
    pub fn position(&self) -> LogPosition {
        self.position
    }

    /// Full-file digest only. Extent roots require the tagged commitment.
    pub fn digest(&self) -> Result<[u8; 32], CommandFailure> {
        Ok(self.state.materialized()?.digest)
    }

    pub fn length(&self) -> u64 {
        self.state.length
    }

    pub fn page_size(&self) -> u32 {
        self.state.page_size
    }

    pub fn epoch(&self) -> u64 {
        self.state.epoch
    }

    pub fn txid(&self) -> u64 {
        self.state.txid
    }

    pub fn checksum(&self) -> u64 {
        self.state.checksum
    }
}

/// Captured under the same cache read as the membership inspection. Ordinary
/// status inspection does not transport this stronger publication statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Publication {
    Empty,
    Pending,
    Image(FleetPublishedImage),
}

impl Publication {
    pub(crate) fn capture(
        applied: &DurableApply,
        published: Option<(&CommittedImage, LogPosition)>,
    ) -> Result<Self, CommandFailure> {
        if published.as_ref().is_some_and(|(_, position)| {
            position.index > applied.position.index
                || !consistent_positions(*position, applied.position)
        }) {
            return Err(CommandFailure::CorruptState);
        }
        if let (Some(durable), Some((visible, _))) = (applied.image.as_ref(), published) {
            if !marker_state(durable)?.same_representation(&marker_state(visible)?) {
                return Err(CommandFailure::CorruptState);
            }
        }
        match (applied.image.as_ref(), published) {
            (None, None) => Ok(Self::Empty),
            (None, Some(_)) => Err(CommandFailure::CorruptState),
            (Some(_), None) => Ok(Self::Pending),
            (Some(durable), Some((visible, position))) if durable == visible => Ok(Self::Image(
                FleetPublishedImage::new_state(position, marker_state(durable)?)?,
            )),
            // Installing an equivalent snapshot can change the local generation
            // name at the same cut before its publication handoff completes.
            // Preserve the exact-marker requirement without calling it empty
            // or falsely reporting corruption during that legitimate handoff.
            (Some(durable), Some((visible, _)))
                if marker_state(durable)? == marker_state(visible)? =>
            {
                Ok(Self::Pending)
            }
            (Some(_), Some((_, position))) if position.index < applied.position.index => {
                Ok(Self::Pending)
            }
            _ => Err(CommandFailure::CorruptState),
        }
    }

    pub(crate) fn validate(&self, member: &MemberInspection) -> Result<(), CommandFailure> {
        match self {
            Self::Empty if member.published.is_none() => Ok(()),
            Self::Pending => Ok(()),
            Self::Image(image)
                if member.published == Some(image.position)
                    && image.position.index <= member.applied.index
                    && image.position.term <= member.applied.term
                    && (image.position.index != member.applied.index
                        || image.position.term == member.applied.term) =>
            {
                FleetPublishedImage::new_state(image.position, image.state).map(|_| ())
            }
            _ => Err(CommandFailure::CorruptState),
        }
    }
}

fn marker_state(image: &CommittedImage) -> Result<FleetImageState, CommandFailure> {
    FleetImageState::from_marker(image)
}

/// Historical proof that a published image (or canonical empty state) covers
/// the complete durable apply at this configuration cut after safe ReadIndex.
/// Holding it supplies no current health, archive coverage or removal lease.
#[derive(Clone, Debug)]
pub struct FleetPublishedCut {
    binding: FleetGroupBinding,
    replica: ReplicaIdentity,
    storage_instance: EngineInstanceId,
    birth: [u8; 32],
    schema: [u8; 32],
    revision: LogIndex,
    applied: LogPosition,
    image: Option<FleetPublishedImage>,
}

impl FleetPublishedCut {
    /// Re-establish a stored historical prefix from an actual current quorum
    /// publication. Serialized receipt material alone cannot construct a cut.
    pub(crate) fn checked_prefix(
        &self,
        applied: LogPosition,
        revision: LogIndex,
        image: Option<FleetPublishedImage>,
    ) -> Result<Self, CommandFailure> {
        let prefix = Self {
            applied,
            revision,
            image,
            ..self.clone()
        };
        if !self.covers(&prefix)? {
            return Err(CommandFailure::Unavailable);
        }
        Ok(prefix)
    }

    pub(crate) fn capture(
        membership: &FleetMembershipObservation,
        storage_instance: EngineInstanceId,
        publication: &Publication,
    ) -> Option<Self> {
        let image = match publication {
            Publication::Pending => return None,
            Publication::Empty => None,
            Publication::Image(image) => Some(image.clone()),
        };
        Some(Self {
            binding: membership.status().binding.clone(),
            replica: membership.replica().clone(),
            storage_instance,
            birth: membership.birth_digest(),
            schema: membership.schema_digest(),
            revision: membership.membership_revision(),
            applied: membership.applied(),
            image,
        })
    }

    pub fn binding(&self) -> &FleetGroupBinding {
        &self.binding
    }

    pub fn replica(&self) -> &ReplicaIdentity {
        &self.replica
    }

    pub fn storage_instance(&self) -> EngineInstanceId {
        self.storage_instance
    }

    pub fn birth_digest(&self) -> [u8; 32] {
        self.birth
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema
    }

    pub fn membership_revision(&self) -> LogIndex {
        self.revision
    }

    /// Includes configuration-only progress even when the SQL data position
    /// has not advanced. The image state was checked against this durable cut.
    pub fn applied(&self) -> LogPosition {
        self.applied
    }

    /// None is proven canonical empty state, not missing publication metadata.
    pub fn image(&self) -> Option<&FleetPublishedImage> {
        self.image.as_ref()
    }

    /// Compare committed prefix coverage at the same exact configuration.
    /// Physical replicas may differ. Configuration changes require a fresh
    /// verification attempt; inconsistent same-position images are corruption.
    pub fn covers(&self, minimum: &Self) -> Result<bool, CommandFailure> {
        if self.binding != minimum.binding
            || self.birth != minimum.birth
            || self.schema != minimum.schema
            || self.revision != minimum.revision
        {
            return Err(CommandFailure::CommandConflict);
        }
        if self
            .image
            .as_ref()
            .zip(minimum.image.as_ref())
            .is_some_and(|(a, b)| !a.state.same_representation(&b.state))
        {
            return Err(CommandFailure::CorruptState);
        }
        if self.applied.index < minimum.applied.index {
            return Ok(false);
        }
        if self.applied.term < minimum.applied.term
            || self.applied.index == minimum.applied.index
                && self.applied.term != minimum.applied.term
        {
            return Err(CommandFailure::CorruptState);
        }
        match (&self.image, &minimum.image) {
            (None, None) => Ok(true),
            (None, Some(_)) => Err(CommandFailure::CorruptState),
            (Some(current), prior) => {
                if !consistent_positions(current.position, minimum.applied)
                    || prior.as_ref().is_some_and(|prior| {
                        !consistent_positions(current.position, prior.position)
                    })
                {
                    return Err(CommandFailure::CorruptState);
                }
                // Live and restored replicas can publish exactly the same
                // state at different protocol-only positions. Changed state
                // must instead follow the entire minimum applied prefix.
                if prior
                    .as_ref()
                    .is_some_and(|prior| current.state == prior.state)
                {
                    return Ok(true);
                }
                if current.position.index <= minimum.applied.index {
                    return Err(CommandFailure::CorruptState);
                }
                Ok(true)
            }
        }
    }
}

fn consistent_positions(a: LogPosition, b: LogPosition) -> bool {
    match a.index.cmp(&b.index) {
        std::cmp::Ordering::Equal => a.term == b.term,
        std::cmp::Ordering::Less => a.term <= b.term,
        std::cmp::Ordering::Greater => a.term >= b.term,
    }
}
