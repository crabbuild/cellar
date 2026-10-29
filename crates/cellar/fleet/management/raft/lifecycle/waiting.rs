// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Waiting describes a read-only proof attempt, never an unknown mutation.
use super::*;
use crate::application::consensus::InspectionError;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "waiting_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetVerificationStage {
    HistoryQuorum,
    TerminalQuorum,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetVerificationReason {
    Unavailable,
    Unreachable,
    Overloaded,
    StalePeer,
}

/// Ephemeral diagnostic information. This grants no receipt, stop or release.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetVerificationBlocker {
    pub stage: RaftFleetVerificationStage,
    /// Canonical history key; terminal renewal spans the completed inventory.
    #[serde(deserialize_with = "required_nullable")]
    pub history: Option<String>,
    pub reason: RaftFleetVerificationReason,
}

impl RaftFleetVerificationBlocker {
    // Call only before the authority-changing finalization/removal boundary.
    pub(super) fn observation(
        error: &FleetCatalogError,
        stage: RaftFleetVerificationStage,
        history: Option<String>,
    ) -> Option<Self> {
        let reason = match error {
            FleetCatalogError::Observation(InspectionError::Unavailable) => {
                RaftFleetVerificationReason::Unavailable
            }
            FleetCatalogError::Observation(InspectionError::Unreachable) => {
                RaftFleetVerificationReason::Unreachable
            }
            FleetCatalogError::Observation(InspectionError::Overloaded) => {
                RaftFleetVerificationReason::Overloaded
            }
            FleetCatalogError::Observation(InspectionError::StalePeer) => {
                RaftFleetVerificationReason::StalePeer
            }
            _ => return None,
        };
        Some(Self {
            stage,
            history,
            reason,
        })
    }

    pub(super) fn validate(&self, progress: &RaftFleetEvacuationProgress) -> Result<()> {
        if !progress.verifying()
            || progress.applied_revision.is_some()
            || progress.candidate_digest.is_some()
            || progress.closure_digest.is_some()
            || progress.process.is_some()
            || progress.fence_id.is_some()
        {
            return Err(RaftFleetFailure::invalid(
                "waiting proof is not an admission",
            ));
        }
        match self.stage {
            RaftFleetVerificationStage::HistoryQuorum => {
                let history = self
                    .history
                    .as_deref()
                    .ok_or_else(|| RaftFleetFailure::invalid("waiting history key"))?;
                super::super::records::catalog_id(history)?;
                if progress.checked_histories >= progress.total_histories {
                    return Err(RaftFleetFailure::invalid("waiting history was consumed"));
                }
            }
            RaftFleetVerificationStage::TerminalQuorum => {
                if self.history.is_some()
                    || progress.phase != Phase::VerifyingRemoval
                    || progress.checked_histories != progress.total_histories
                {
                    return Err(RaftFleetFailure::invalid("incomplete terminal renewal"));
                }
            }
        }
        Ok(())
    }
}

pub(super) fn required_nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::deserialize(decoder)
}
