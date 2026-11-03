//! Fixed complete-group roots pair durable membership with all cell state.
//! Authenticated mutation summaries distinguish data from protocol-only apply.
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

/// Historical proof of a complete group publication at a safe ReadIndex cut.
/// Holding it supplies no current health, archive coverage or removal lease.
#[derive(Clone, Debug)]
pub struct FleetPublishedCut {
    binding: FleetGroupBinding,
    replica: ReplicaIdentity<cellar_logic::cell_group::GroupHistory>,
    storage_instance: EngineInstanceId,
    root: crate::raft::cell_groups::GroupManifest,
    configuration_digest: [u8; 32],
}

impl FleetPublishedCut {
    pub(crate) fn capture_group(
        membership: &FleetMembershipObservation,
        storage_instance: EngineInstanceId,
    ) -> Self {
        Self {
            binding: membership.status().binding.clone(),
            replica: membership.replica().clone(),
            storage_instance,
            root: membership.root().clone(),
            configuration_digest: membership.configuration_digest(),
        }
    }

    /// Stored journal data is historical material, never an independently minted
    /// proof. Re-establish its prefix through an actual current quorum cut.
    pub(crate) fn checked_prefix(
        &self,
        root: crate::raft::cell_groups::GroupManifest,
        configuration_digest: [u8; 32],
    ) -> Result<Self, CommandFailure> {
        root.check().map_err(|_| CommandFailure::CorruptState)?;
        let prefix = Self {
            root,
            configuration_digest,
            ..self.clone()
        };
        if !self.covers(&prefix)? {
            return Err(CommandFailure::Unavailable);
        }
        Ok(prefix)
    }

    pub fn binding(&self) -> &FleetGroupBinding {
        &self.binding
    }

    pub fn replica(&self) -> &ReplicaIdentity<cellar_logic::cell_group::GroupHistory> {
        &self.replica
    }

    pub fn storage_instance(&self) -> EngineInstanceId {
        self.storage_instance
    }

    pub fn birth_digest(&self) -> [u8; 32] {
        self.root.route().birth_digest
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.root.route().schema_digest
    }

    pub fn membership_revision(&self) -> LogIndex {
        LogIndex::new(self.root.membership_revision())
    }

    pub fn applied(&self) -> LogPosition {
        self.root.position()
    }

    /// The complete group commitment includes the route, every cell's image,
    /// enrollment/receipt summary and recovery lineage. Empty is an explicit
    /// zero-cell manifest; missing/pending publication cannot construct a cut.
    pub fn root(&self) -> &crate::raft::cell_groups::GroupManifest {
        &self.root
    }

    pub fn configuration_digest(&self) -> [u8; 32] {
        self.configuration_digest
    }

    pub fn covers(&self, minimum: &Self) -> Result<bool, CommandFailure> {
        if self.binding != minimum.binding
            || self.membership_revision() != minimum.membership_revision()
            || self.configuration_digest != minimum.configuration_digest
            || self.root.route() != minimum.root.route()
        {
            return Err(CommandFailure::CommandConflict);
        }
        group_prefix(&self.root, &minimum.root)
    }
}

pub(crate) fn group_prefix(
    current: &crate::raft::cell_groups::GroupManifest,
    minimum: &crate::raft::cell_groups::GroupManifest,
) -> Result<bool, CommandFailure> {
    current
        .check()
        .and_then(|()| minimum.check())
        .map_err(|_| CommandFailure::CorruptState)?;
    if current.route() != minimum.route()
        || current.membership_revision() != minimum.membership_revision()
    {
        return Err(CommandFailure::CommandConflict);
    }
    let (a, b) = (current.position(), minimum.position());
    if a.index < b.index {
        return Ok(false);
    }
    if a.term < b.term
        || a.index == b.index && current != minimum
        || !current.preserves_recovery(minimum)
    {
        return Err(CommandFailure::CorruptState);
    }
    if current.root() != minimum.root() {
        let mutation = current
            .root()
            .last_mutation()
            .ok_or(CommandFailure::CorruptState)?;
        if mutation.index <= b.index || mutation.term < b.term {
            return Err(CommandFailure::CorruptState);
        }
    }
    Ok(true)
}

fn consistent_positions(a: LogPosition, b: LogPosition) -> bool {
    match a.index.cmp(&b.index) {
        std::cmp::Ordering::Equal => a.term == b.term,
        std::cmp::Ordering::Less => a.term <= b.term,
        std::cmp::Ordering::Greater => a.term >= b.term,
    }
}
