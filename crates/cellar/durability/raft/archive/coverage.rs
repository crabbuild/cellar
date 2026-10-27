// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical verification of one exact immutable archive and fixed publication.
use super::*;
use crate::raft::host::{FleetPublishedCut, FleetPublishedImage};
use crate::raft::member_state::FleetGroupBinding;
use cellar_logic::consensus::LogIndex;
use std::time::Duration;

/// Historical, physically replayed coverage of one selected immutable archive.
/// This supplies no current health, quorum, placement or removal authority.
/// A lifecycle owner must obtain fresh survivor observations and recheck its
/// rooted operation before committing a terminal receipt or releasing resources.
#[derive(Clone, Debug)]
pub struct VerifiedArchiveCoverage {
    reference: ArchiveReference,
    binding: FleetGroupBinding,
    birth: [u8; 32],
    schema: [u8; 32],
    revision: LogIndex,
    position: LogPosition,
    image_position: Option<LogPosition>,
    image: Option<ImageState>,
}

impl VerifiedArchiveCoverage {
    fn capture(set: &ArchiveRecoverySet) -> Result<Self, ArchiveError> {
        let membership = &set.manifest.membership;
        let status = membership
            .fleet_status()
            .ok_or(ArchiveError::Invalid("archive has no fleet enrollment"))?;
        let image = set.image_state()?;
        // A full snapshot's applied position can include configuration-only
        // progress. Preserve the producer's exact publication origin, including
        // legitimate intact-restore aliases; never infer it from that snapshot.
        let image_position = set
            .manifest
            .image_position
            .map(Position::decode)
            .transpose()?;
        Ok(Self {
            reference: set.reference(),
            binding: status.binding,
            birth: set.birth_digest(),
            schema: set.schema_digest(),
            revision: membership.revision(),
            position: set.position(),
            image_position,
            image,
        })
    }

    pub fn reference(&self) -> &ArchiveReference {
        &self.reference
    }

    pub fn binding(&self) -> &FleetGroupBinding {
        &self.binding
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

    pub fn position(&self) -> LogPosition {
        self.position
    }

    /// None denotes a physically verified canonical-empty archive, never a
    /// missing payload or a pending SQL publication.
    pub fn image_state(&self) -> Option<ImageState> {
        self.image
    }

    pub fn image_position(&self) -> Option<LogPosition> {
        self.image_position
    }

    /// Compare complete prefix coverage at the same exact fleet/history and
    /// membership revision. A configuration change requires a new attempt.
    pub fn covers(&self, minimum: &FleetPublishedCut) -> Result<bool, ArchiveError> {
        if &self.binding != minimum.binding()
            || self.birth != minimum.birth_digest()
            || self.schema != minimum.schema_digest()
            || self.revision != minimum.membership_revision()
        {
            return Err(ArchiveError::Conflict);
        }
        if self.position.index < minimum.applied().index {
            return Ok(false);
        }
        if !consistent(self.position, minimum.applied()) {
            return Err(ArchiveError::Corrupt("archive publication prefix term"));
        }
        match (self.image, minimum.image()) {
            (None, None) => Ok(true),
            (None, Some(_)) => Err(ArchiveError::Corrupt("archive erased published image")),
            (Some(state), prior) => {
                let position = self
                    .image_position
                    .ok_or(ArchiveError::Corrupt("archive image position"))?;
                if !consistent(position, minimum.applied())
                    || prior.is_some_and(|image| !consistent(position, image.position()))
                {
                    return Err(ArchiveError::Corrupt("archive data prefix term"));
                }
                if prior.is_some_and(|image| matches_image(state, image)) {
                    return Ok(true);
                }
                if position.index <= minimum.applied().index {
                    return Err(ArchiveError::Corrupt("archive conflicting published image"));
                }
                Ok(true)
            }
        }
    }
}

fn matches_image(state: ImageState, image: &FleetPublishedImage) -> bool {
    state.epoch == image.epoch()
        && state.txid.0 == image.txid()
        && state.checksum == image.checksum()
        && state.page_size == image.page_size()
        && state.length == image.length()
        && state.digest == image.digest()
}

fn consistent(a: LogPosition, b: LogPosition) -> bool {
    match a.index.cmp(&b.index) {
        std::cmp::Ordering::Equal => a.term == b.term,
        std::cmp::Ordering::Less => a.term <= b.term,
        std::cmp::Ordering::Greater => a.term >= b.term,
    }
}

impl CellArchive {
    /// Download and physically replay exactly `reference`, then prove coverage
    /// of a fixed quorum-backed publication cut. A missing selected object
    /// refuses; this operation never substitutes a newer mutable head.
    ///
    /// The original deadline covers every object read, workspace wait and
    /// replay. Blocking work retains its inputs until joined; expiry/cancellation
    /// suppresses the proof even if that work finishes later. The caller owns
    /// scratch storage and admission for the configured recovery-memory bound.
    pub async fn verify_published_at(
        &self,
        reference: &ArchiveReference,
        minimum: &FleetPublishedCut,
        verifier: &ArchiveVerifier,
        timeout: Duration,
    ) -> Result<VerifiedArchiveCoverage, ArchiveError> {
        let millis = u64::try_from(timeout.as_millis()).map_err(|_| ArchiveError::Limit)?;
        if !(1..=60_000).contains(&millis) || timeout > Duration::from_secs(60) {
            return Err(ArchiveError::Invalid("archive verification timeout"));
        }
        let mut archive = self.clone();
        let now = archive
            .io_domain
            .as_ref()
            .ok_or(ArchiveError::Stopped)?
            .mono_ms()
            .map_err(|_| ArchiveError::Stopped)?;
        let deadline = now.checked_add(millis).ok_or(ArchiveError::Limit)?;
        archive.verification_deadline = Some(
            archive
                .verification_deadline
                .map_or(deadline, |previous| previous.min(deadline)),
        );
        archive.check_verification()?;
        let set = archive.recovery_set_at(reference).await?;
        let worker = archive.clone();
        let verifier = verifier.clone();
        let minimum = minimum.clone();
        let proof = archive
            .host
            .run_blocking(move || {
                worker.check_verification()?;
                let proof = VerifiedArchiveCoverage::capture(&set)?;
                if !proof.covers(&minimum)? {
                    return Err(ArchiveError::Unconfirmed);
                }
                verifier.verify_checked(&set, || worker.check_verification())?;
                worker.check_verification()?;
                Ok(proof)
            })
            .await
            .map_err(ArchiveError::Worker)??;
        // Delivery can be delayed after the worker completed, including by an
        // unpolled caller. A completed replay is no exception to this deadline.
        archive.check_verification()?;
        Ok(proof)
    }

    fn check_verification(&self) -> Result<(), ArchiveError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|cancel| *cancel.borrow() || cancel.has_changed().is_err())
        {
            return Err(ArchiveError::Stopped);
        }
        self.deadline().map(|_| ())
    }
}
