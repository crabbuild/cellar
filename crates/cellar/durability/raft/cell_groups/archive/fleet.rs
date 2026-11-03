// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fleet coverage consumes complete independently recovered group closures.
//! A selected object, uploaded bytes or a single cell image is insufficient.
use super::*;
use crate::raft::{host::FleetPublishedCut, member_state::FleetGroupBinding};
use cellar_logic::consensus::{LogIndex, LogPosition};

#[derive(Clone)]
pub struct VerifiedFleetGroupArchiveCoverage {
    coverage: CellGroupArchiveCoverage,
    binding: FleetGroupBinding,
    configuration_digest: [u8; 32],
}

impl VerifiedFleetGroupArchiveCoverage {
    pub fn hold(&self) -> &CellGroupArchiveHold {
        self.coverage.hold()
    }

    pub fn root(&self) -> &GroupManifest {
        self.coverage.marker()
    }

    pub fn binding(&self) -> &FleetGroupBinding {
        &self.binding
    }

    pub fn position(&self) -> LogPosition {
        self.root().position()
    }

    pub fn membership_revision(&self) -> LogIndex {
        LogIndex::new(self.root().membership_revision())
    }

    pub fn configuration_digest(&self) -> [u8; 32] {
        self.configuration_digest
    }

    pub fn covers(&self, minimum: &FleetPublishedCut) -> Result<bool> {
        if self.binding() != minimum.binding()
            || self.configuration_digest != minimum.configuration_digest()
        {
            return Err(CellGroupArchiveError::Conflict);
        }
        crate::raft::host::group_prefix(self.root(), minimum.root())
            .map_err(|_| CellGroupArchiveError::Conflict)
    }
}

impl CellGroupArchiveRegistry {
    /// Re-read the exact persisted hold, independently restore every checkpoint
    /// dependency and replay the contiguous tail, then compare the full root.
    /// The execution owner retains entered physical work after cancellation.
    /// The caller must enforce its original deadline around this read/replay and
    /// suppress late delivery; a saved proof is historical, never a health lease.
    pub async fn verify_fleet_published_at(
        &self,
        domain: &impl CellGroupArchiveExecution,
        tails: &CellGroupArchiveTailPool,
        hold: &CellGroupArchiveHold,
        minimum: &FleetPublishedCut,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<VerifiedFleetGroupArchiveCoverage> {
        let coverage = self.verify_hold(domain, hold, tails, receiver).await?;
        let membership = coverage.recovery().membership();
        let status = membership
            .fleet_status()
            .ok_or(CellGroupArchiveError::Invalid(
                "archive has no fleet enrollment",
            ))?;
        if membership.revision().get() != coverage.marker().membership_revision()
            || !status
                .binding
                .matches_scope(&coverage.marker().route().history)
        {
            return Err(CellGroupArchiveError::Invalid("fleet archive membership"));
        }
        let configuration_digest = crate::raft::host::group_membership_digest(
            membership,
            coverage.recovery().configuration(),
        )
        .map_err(|_| CellGroupArchiveError::Invalid("fleet archive configuration commitment"))?;
        let proof = VerifiedFleetGroupArchiveCoverage {
            coverage,
            binding: status.binding,
            configuration_digest,
        };
        if !proof.covers(minimum)? {
            return Err(CellGroupArchiveError::Unconfirmed);
        }
        Ok(proof)
    }
}
