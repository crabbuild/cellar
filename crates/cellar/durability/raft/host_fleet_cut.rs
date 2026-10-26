//! Fixed publication cuts pair the durable configuration with its SQL image.
//! A data position can precede configuration-only applied progress.
use super::*;
use crate::raft::cache::{CommittedImage, DurableApply};
use crate::raft::inspection::MemberInspection;
use crate::raft::member_state::FleetGroupBinding;
use cellar_ltx::{apply::ImageState, TXID};

#[cfg(test)]
#[path = "host_fleet_cut_tests.rs"]
mod tests;

/// An immutable published data image. This value alone supplies neither a
/// quorum barrier nor permission to activate a member or acknowledge a write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetPublishedImage {
    position: LogPosition,
    state: ImageState,
}

impl FleetPublishedImage {
    pub(crate) fn new(position: LogPosition, state: ImageState) -> Result<Self, CommandFailure> {
        state
            .validate(&crate::raft::archive::apply_limits())
            .map_err(|_| CommandFailure::CorruptState)?;
        if position.index.get() == 0
            || position.term.get() == 0
            || position.index.get() >= u64::MAX - 2
            || position.term.get() >= u64::MAX - 2
            || state.txid == TXID(0)
            || state.length == 0
        {
            return Err(CommandFailure::CorruptState);
        }
        Ok(Self { position, state })
    }

    /// The publication's committed position. Restoring the same image may
    /// relabel it at a later configuration-only cut.
    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn digest(&self) -> [u8; 32] {
        self.state.digest
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
        self.state.txid.0
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
        match (applied.image.as_ref(), published) {
            (None, None) => Ok(Self::Empty),
            (None, Some(_)) => Err(CommandFailure::CorruptState),
            (Some(_), None) => Ok(Self::Pending),
            (Some(durable), Some((visible, position))) if durable == visible => Ok(Self::Image(
                FleetPublishedImage::new(position, marker_state(durable))?,
            )),
            // Installing an equivalent snapshot can change the local generation
            // name at the same cut before its publication handoff completes.
            // Preserve the exact-marker requirement without calling it empty
            // or falsely reporting corruption during that legitimate handoff.
            (Some(durable), Some((visible, _)))
                if marker_state(durable) == marker_state(visible) =>
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
                FleetPublishedImage::new(image.position, image.state).map(|_| ())
            }
            _ => Err(CommandFailure::CorruptState),
        }
    }
}

fn marker_state(image: &CommittedImage) -> ImageState {
    ImageState {
        epoch: image.ltx_epoch,
        txid: TXID(image.txid),
        checksum: image.checksum,
        page_size: image.page_size,
        length: image.length,
        digest: image.digest,
    }
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
