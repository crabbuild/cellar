// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordered enrollment preparation against one coherent group root. Publication
//! still belongs to the engine and requires the actual committed log position.

use super::{
    decide_enrollment, CellImageGeometry, CellImagePin, CellManifestLeaf, EffectSummary,
    EnrollmentDecision, EnrollmentError, EnrollmentPhase, EnrollmentRecord, ManifestError,
    ManifestPageStore, ManifestTree, PreparedGroupManifest,
};
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryLimits,
};
use cellar_logic::{cell_group::*, consensus::LogPosition};

pub struct EnrollmentApply<'a> {
    pub position: LogPosition,
    pub phase: EnrollmentPhase,
    pub entry: &'a [u8],
    pub limits: GroupEntryLimits,
}

/// Independently prepared and synced initial image, returned only for a new
/// enrollment. A repeated creation must never invoke its image producer again.
pub struct EnrollmentImage {
    pub geometry: CellImageGeometry,
    pub effects: EffectSummary,
    pub pin: CellImagePin,
}

#[derive(Debug)]
pub enum EnrollmentApplyError {
    Enrollment(EnrollmentError),
    Manifest(ManifestError),
}

impl std::fmt::Display for EnrollmentApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enrollment(error) => error.fmt(f),
            Self::Manifest(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for EnrollmentApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Enrollment(error) => Some(error),
            Self::Manifest(error) => Some(error),
        }
    }
}

impl From<EnrollmentError> for EnrollmentApplyError {
    fn from(error: EnrollmentError) -> Self {
        Self::Enrollment(error)
    }
}

impl From<ManifestError> for EnrollmentApplyError {
    fn from(error: ManifestError) -> Self {
        Self::Manifest(error)
    }
}

impl ManifestTree {
    /// Recheck enrollment against the complete preceding root at ordered apply.
    /// Exactly matching retries advance the group prefix while retaining the
    /// current image, original creation and cell sequence. Conflicts or invalid
    /// prefixes refuse before invoking the physical image producer.
    ///
    /// Run this synchronous preparation on the owning blocking execution domain.
    /// The returned decision is not an acknowledgement: publish its manifest
    /// through `persist_cells` only for the corresponding committed entry.
    pub fn prepare_enrollment(
        &self,
        store: &mut ManifestPageStore,
        previous: &PreparedGroupManifest,
        apply: EnrollmentApply<'_>,
        image: impl FnOnce(&EnrollmentRecord) -> Result<EnrollmentImage, ManifestError>,
    ) -> Result<(PreparedGroupManifest, EnrollmentDecision), EnrollmentApplyError> {
        // Validate route, store ownership and contiguous position before image
        // I/O. The unchanged candidate also supplies the idempotent apply path.
        let unchanged = self.prepare_unchanged(store, previous, apply.position)?;
        let entry = CellGroupEntry::decode(apply.entry, &self.route, apply.limits)
            .map_err(EnrollmentError::Protocol)?;
        let GroupCommand::Enroll { cell, .. } = entry.command else {
            return Err(EnrollmentError::NotEnrollment.into());
        };
        let committed = previous.pin_cell(&cell.cell);
        let decision = decide_enrollment(
            &self.route,
            apply.phase,
            apply.entry,
            committed.as_ref().map(|(leaf, _)| &leaf.enrollment),
            apply.limits,
        )?;
        match &decision {
            EnrollmentDecision::Existing(_) => Ok((unchanged, decision)),
            EnrollmentDecision::Propose(record) => {
                let prepared = image(record)?;
                let leaf = CellManifestLeaf {
                    enrollment: record.clone(),
                    version: CellVersion {
                        history: record.cell().clone(),
                        sequence: MutationSequence::new(1)
                            .map_err(|_| ManifestError::Invalid("initial sequence"))?,
                        image: record.initial(),
                    },
                    geometry: prepared.geometry,
                    last_mutation: apply.position,
                    witness: CommitWitness {
                        group: self.route.history,
                        position: apply.position,
                        proposal: record.creation(),
                    },
                    effects: prepared.effects,
                };
                let next = self.prepare_update(store, previous, leaf, None, prepared.pin)?;
                Ok((next, decision))
            }
        }
    }
}
