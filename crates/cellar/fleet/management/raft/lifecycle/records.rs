// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Descriptive lifecycle requests/results. Serialized values grant no proof.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeOperation {
    pub catalog_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub expected_revision: u64,
}

impl RaftFleetNodeOperation {
    pub fn validate(&self) -> Result<()> {
        super::super::records::catalog_id(&self.catalog_id)?;
        super::super::records::id(&self.operation_id)?;
        super::super::records::id(&self.request_id)?;
        super::super::records::revision(self.expected_revision)
    }

    pub(in crate::management::raft) fn identity(
        &self,
        principal: &str,
    ) -> Result<FleetRequestIdentity> {
        self.validate()?;
        if principal.is_empty() || principal.len() > 128 || principal.chars().any(char::is_control)
        {
            return Err(RaftFleetFailure::invalid("operator principal"));
        }
        Ok(FleetRequestIdentity {
            id: super::super::records::id(&self.request_id)?,
            operation: super::super::records::id(&self.operation_id)?,
            expected_revision: self.expected_revision,
            principal: principal.into(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeStop {
    pub operation: RaftFleetNodeOperation,
    /// The exact process shown by the historical finalization receipt.
    pub expected_process: String,
}

impl RaftFleetNodeStop {
    pub fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        super::super::records::id(&self.expected_process)?;
        Ok(())
    }
}

/// A cancellation before finalization has neither selector field. Afterwards
/// retain the exact process and child fence ID across changed-root retries.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeCancel {
    pub operation: RaftFleetNodeOperation,
    pub expected_process: Option<String>,
    pub fence_id: Option<String>,
}

impl RaftFleetNodeCancel {
    pub fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        if self.expected_process.is_some() != self.fence_id.is_some() {
            return Err(RaftFleetFailure::invalid(
                "cancellation process and fence ID must be supplied together",
            ));
        }
        for id in self.expected_process.iter().chain(self.fence_id.iter()) {
            super::super::records::id(id)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetNodeRemove {
    pub operation: RaftFleetNodeOperation,
    pub stop_id: String,
}

impl RaftFleetNodeRemove {
    pub fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        super::super::records::id(&self.stop_id)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetEvacuationPhase {
    Excluded,
    VerifyingBeforeStop,
    Finalizing,
    StopRequested,
    Joined,
    VerifyingRemoval,
    Removed,
    Cancelled,
}

/// Verification progress is local and ephemeral. Finalizing/Removed are
/// journal-confirmed historical admissions, not current health or deletion grants.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetEvacuationProgress {
    pub catalog_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub node_id: String,
    pub expected_revision: u64,
    pub phase: RaftFleetEvacuationPhase,
    pub checked_histories: u64,
    pub total_histories: u64,
    pub applied_revision: Option<u64>,
    pub observed_revision: u64,
    pub candidate_digest: Option<String>,
    pub closure_digest: Option<String>,
    pub process: Option<String>,
    pub stop_id: Option<String>,
    pub fence_id: Option<String>,
    pub terminal: bool,
    #[serde(deserialize_with = "super::waiting::required_nullable")]
    pub blocker: Option<RaftFleetVerificationBlocker>,
}

impl RaftFleetEvacuationProgress {
    pub fn validate(&self) -> Result<()> {
        if let Some(blocker) = &self.blocker {
            blocker.validate(self)?;
        }
        RaftFleetNodeOperation {
            catalog_id: self.catalog_id.clone(),
            operation_id: self.operation_id.clone(),
            request_id: self.request_id.clone(),
            expected_revision: self.expected_revision,
        }
        .validate()?;
        super::super::records::id(&self.node_id)?;
        if let Some(process) = &self.process {
            super::super::records::id(process)?;
        }
        for id in self.stop_id.iter().chain(self.fence_id.iter()) {
            super::super::records::id(id)?;
        }
        for digest in self
            .candidate_digest
            .iter()
            .chain(self.closure_digest.iter())
        {
            super::super::records::catalog_id(digest)?;
        }
        let admitted = matches!(
            self.phase,
            RaftFleetEvacuationPhase::Excluded
                | RaftFleetEvacuationPhase::Finalizing
                | RaftFleetEvacuationPhase::Removed
                | RaftFleetEvacuationPhase::Cancelled
        );
        let stopping = matches!(
            self.phase,
            RaftFleetEvacuationPhase::StopRequested
                | RaftFleetEvacuationPhase::Joined
                | RaftFleetEvacuationPhase::VerifyingRemoval
                | RaftFleetEvacuationPhase::Removed
        );
        let process_required = matches!(
            self.phase,
            RaftFleetEvacuationPhase::Finalizing
                | RaftFleetEvacuationPhase::StopRequested
                | RaftFleetEvacuationPhase::Joined
        );
        if self.phase != RaftFleetEvacuationPhase::Cancelled && self.fence_id.is_some()
            || self.phase == RaftFleetEvacuationPhase::Cancelled
                && (self.fence_id.is_some() != self.process.is_some())
            || stopping != self.stop_id.is_some()
            || process_required && self.process.is_none()
            || matches!(
                self.phase,
                RaftFleetEvacuationPhase::Finalizing | RaftFleetEvacuationPhase::Removed
            ) && self.checked_histories != self.total_histories
            || self.checked_histories > self.total_histories
            || self.total_histories > 10_000
            || self.observed_revision < self.expected_revision
            || self.terminal
                != matches!(
                    self.phase,
                    RaftFleetEvacuationPhase::Removed | RaftFleetEvacuationPhase::Cancelled
                )
            || admitted != self.applied_revision.is_some()
            || self
                .applied_revision
                .is_some_and(|r| r <= self.expected_revision || r > self.observed_revision)
            || matches!(
                self.phase,
                RaftFleetEvacuationPhase::Finalizing | RaftFleetEvacuationPhase::Removed
            ) && self.candidate_digest.is_none()
            || matches!(
                self.phase,
                RaftFleetEvacuationPhase::Joined | RaftFleetEvacuationPhase::Removed
            ) && self.closure_digest.is_none()
        {
            return Err(RaftFleetFailure::invalid(
                "inconsistent evacuation progress",
            ));
        }
        Ok(())
    }

    pub fn verifying(&self) -> bool {
        matches!(
            self.phase,
            RaftFleetEvacuationPhase::VerifyingBeforeStop
                | RaftFleetEvacuationPhase::VerifyingRemoval
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetEvacuationAcceptance {
    pub progress: RaftFleetEvacuationProgress,
    pub reconciled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetEvacuationDebt {
    pub reservation: String,
    pub cluster: String,
    pub namespace: String,
    pub group: String,
    pub incarnation: String,
    pub cell: String,
    pub target: String,
    pub peak_engine_slots: u32,
    pub peak_durable_bytes: u64,
    pub peak_memory_bytes: u64,
    pub boot: String,
    pub executable_movement: bool,
}

/// One catalog cut. Pending debits remain obligations even after local stop.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetEvacuationStatus {
    pub catalog_id: String,
    pub operation_id: String,
    pub node_id: String,
    pub state: RaftFleetNodeState,
    pub observed_revision: u64,
    pub exclusion_revision: u64,
    pub finalization: Option<RaftFleetEvacuationProgress>,
    pub removal: Option<RaftFleetEvacuationProgress>,
    pub cancellation: Option<RaftFleetEvacuationProgress>,
    pub pending_movements: Vec<RaftFleetEvacuationDebt>,
    /// Local ephemeral proof progress at this exact root, without admission authority.
    #[serde(deserialize_with = "super::waiting::required_nullable")]
    pub verification: Option<RaftFleetEvacuationProgress>,
}

impl RaftFleetEvacuationStatus {
    pub fn validate(&self) -> Result<()> {
        super::super::records::catalog_id(&self.catalog_id)?;
        super::super::records::id(&self.operation_id)?;
        super::super::records::id(&self.node_id)?;
        if self.exclusion_revision == 0
            || self.exclusion_revision > self.observed_revision
            || self.pending_movements.len() > 2
            || self.removal.is_some()
                && (self.state != RaftFleetNodeState::Removed || self.cancellation.is_some())
            || self.state == RaftFleetNodeState::Removed
                && self.removal.is_none()
                && self.cancellation.is_none()
        {
            return Err(RaftFleetFailure::invalid("inconsistent evacuation status"));
        }
        for progress in self
            .finalization
            .iter()
            .chain(self.removal.iter())
            .chain(self.cancellation.iter())
            .chain(self.verification.iter())
        {
            progress.validate()?;
            if progress.catalog_id != self.catalog_id
                || progress.operation_id != self.operation_id
                || progress.node_id != self.node_id
                || progress.observed_revision != self.observed_revision
            {
                return Err(RaftFleetFailure::invalid("mixed evacuation status"));
            }
        }
        if let Some(progress) = &self.verification {
            if !progress.verifying()
                || (progress.phase == RaftFleetEvacuationPhase::VerifyingBeforeStop
                    && (self.state != RaftFleetNodeState::Draining || self.finalization.is_some()))
                || (progress.phase == RaftFleetEvacuationPhase::VerifyingRemoval
                    && self.state != RaftFleetNodeState::Finalizing)
            {
                return Err(RaftFleetFailure::invalid(
                    "verification is not a terminal receipt",
                ));
            }
        }
        Ok(())
    }
}
