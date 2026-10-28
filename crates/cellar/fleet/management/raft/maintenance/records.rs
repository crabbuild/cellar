// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Wire values describe historical admissions, never live health capabilities.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetMaintenancePrepare {
    pub operation: RaftFleetNodeOperation,
    pub expected_process: String,
    pub grace_seconds: u16,
    pub birth_resolution: RaftFleetBirthResolution,
}

impl RaftFleetMaintenancePrepare {
    pub fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        super::super::records::id(&self.expected_process)?;
        if !(1..=300).contains(&self.grace_seconds) {
            return Err(RaftFleetFailure::invalid(
                "maintenance grace must be 1..=300 seconds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetMaintenanceCancel {
    pub operation: RaftFleetNodeOperation,
    pub fence_id: String,
}

impl RaftFleetMaintenanceCancel {
    pub fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        super::super::records::id(&self.fence_id)?;
        if self.operation.request_id == self.operation.operation_id {
            return Err(RaftFleetFailure::invalid(
                "cancellation requires a distinct request ID",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetMaintenanceAcceptance {
    pub catalog_id: String,
    pub node_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub expected_revision: u64,
    pub applied_revision: u64,
    pub observed_revision: u64,
    pub reconciled: bool,
}

impl RaftFleetMaintenanceAcceptance {
    pub fn validate(&self) -> Result<()> {
        RaftFleetNodeOperation {
            catalog_id: self.catalog_id.clone(),
            operation_id: self.operation_id.clone(),
            request_id: self.request_id.clone(),
            expected_revision: self.expected_revision,
        }
        .validate()?;
        super::super::records::id(&self.node_id)?;
        if self.expected_revision.checked_add(1) != Some(self.applied_revision)
            || self.observed_revision < self.applied_revision
        {
            return Err(RaftFleetFailure::invalid(
                "inconsistent maintenance admission",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetMaintenanceStatus {
    pub catalog_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub principal: String,
    pub expected_revision: u64,
    pub applied_revision: u64,
    pub observed_revision: u64,
    pub node_id: String,
    pub process: String,
    pub boot: String,
    pub enrollment_epoch: u64,
    pub grace_seconds: u16,
    pub birth_resolution: RaftFleetBirthResolution,
    pub phase: RaftFleetMaintenancePhase,
    pub execution_operation: Option<String>,
    /// Historical only; stop must independently renew current proof.
    pub readiness_request: Option<String>,
    pub cancellation_request: Option<String>,
    pub cancelled_revision: Option<u64>,
    pub cancellation_refused: bool,
    pub completed_revision: Option<u64>,
    pub stop_request: Option<String>,
    pub return_request: Option<String>,
    pub resume_request: Option<String>,
    pub stop_consent: Option<FleetRequestIdentity>,
    pub offline: Option<FleetRequestIdentity>,
    pub returning: Option<RaftFleetMaintenanceReturnContext>,
    pub resume_consent: Option<FleetRequestIdentity>,
}

impl RaftFleetMaintenanceStatus {
    pub fn validate(&self) -> Result<()> {
        RaftFleetMaintenanceAcceptance {
            catalog_id: self.catalog_id.clone(),
            node_id: self.node_id.clone(),
            operation_id: self.operation_id.clone(),
            request_id: self.request_id.clone(),
            expected_revision: self.expected_revision,
            applied_revision: self.applied_revision,
            observed_revision: self.observed_revision,
            reconciled: false,
        }
        .validate()?;
        for id in [&self.process, &self.boot]
            .into_iter()
            .chain(self.execution_operation.iter())
            .chain(self.readiness_request.iter())
            .chain(self.cancellation_request.iter())
            .chain(self.stop_request.iter())
            .chain(self.return_request.iter())
            .chain(self.resume_request.iter())
        {
            super::super::records::id(id)?;
        }
        use RaftFleetMaintenancePhase as P;
        let cancelling = matches!(
            self.phase,
            P::CancelRequested | P::Cancelling | P::Cancelled
        );
        if self.enrollment_epoch == 0
            || !(1..=300).contains(&self.grace_seconds)
            || self.principal.is_empty()
            || self.principal.len() > 128
            || self.principal.chars().any(char::is_control)
            || (cancelling || self.cancellation_refused) != self.cancellation_request.is_some()
            || self.cancellation_refused
                && (self.offline.is_none() || self.cancelled_revision.is_some())
            || self.phase == P::Ready && self.readiness_request.is_none()
            || (self.phase == P::Complete) != self.completed_revision.is_some()
            || self
                .completed_revision
                .is_some_and(|r| r <= self.applied_revision || r > self.observed_revision)
            || self.completed_revision.is_some() && self.resume_consent.is_none()
            || (self.phase == P::Cancelled) != self.cancelled_revision.is_some()
            || self.readiness_request.is_some() && self.execution_operation.is_none()
            || self
                .cancelled_revision
                .is_some_and(|r| r <= self.applied_revision || r > self.observed_revision)
        {
            return Err(RaftFleetFailure::invalid("inconsistent maintenance status"));
        }
        for identity in self
            .stop_consent
            .iter()
            .chain(self.offline.iter())
            .chain(self.resume_consent.iter())
            .chain(
                self.returning
                    .iter()
                    .flat_map(|r| [&r.identity, &r.offline]),
            )
        {
            super::super::records::id(&hex::encode(identity.id))?;
            super::super::records::revision(identity.expected_revision)?;
            if Some(hex::encode(identity.operation)).as_ref() != self.execution_operation.as_ref()
                || identity.principal.is_empty()
                || identity.expected_revision >= self.observed_revision
            {
                return Err(RaftFleetFailure::invalid("inconsistent maintenance effect"));
            }
        }
        if let Some(returning) = &self.returning {
            super::super::records::id(&returning.next_process)?;
            if returning.next_process == self.process
                || self.offline.as_ref() != Some(&returning.offline)
            {
                return Err(RaftFleetFailure::invalid("inconsistent maintenance return"));
            }
        }
        if self.offline.is_some() && self.stop_consent.is_none()
            || self.returning.is_some() && self.return_request.is_none()
            || self.resume_consent.is_some() && self.resume_request.is_none()
        {
            return Err(RaftFleetFailure::invalid(
                "inconsistent maintenance effects",
            ));
        }
        Ok(())
    }
}

/// One independent human grant. The controller, rather than HTTP polling,
/// consumes the actual SDK stop/return/resume boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum RaftFleetMaintenanceAction {
    Stop {
        operation: RaftFleetNodeOperation,
        expected_process: String,
        stop_id: String,
    },
    Return {
        operation: RaftFleetNodeOperation,
        next_process: String,
    },
    Resume {
        operation: RaftFleetNodeOperation,
        attestation: Box<RaftFleetCapacityAttestation>,
        resume_id: String,
    },
}

impl RaftFleetMaintenanceAction {
    pub fn operation(&self) -> &RaftFleetNodeOperation {
        match self {
            Self::Stop { operation, .. }
            | Self::Return { operation, .. }
            | Self::Resume { operation, .. } => operation,
        }
    }

    pub fn validate(&self) -> Result<()> {
        self.operation().validate()?;
        match self {
            Self::Stop {
                expected_process,
                stop_id,
                ..
            } => {
                super::super::records::id(expected_process)?;
                super::super::records::id(stop_id)?;
            }
            Self::Return { next_process, .. } => {
                super::super::records::id(next_process)?;
            }
            Self::Resume {
                attestation,
                resume_id,
                ..
            } => {
                attestation.validate()?;
                super::super::records::id(resume_id)?;
            }
        }
        Ok(())
    }
}

/// Data for a trusted deployment adapter. The SDK retained engine owner still
/// checks and consumes canonical Root consent; this JSON is no boot capability.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetMaintenanceReturnContext {
    pub identity: FleetRequestIdentity,
    pub offline: FleetRequestIdentity,
    pub next_process: String,
}
