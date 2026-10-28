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
            || cancelling != self.cancellation_request.is_some()
            || (self.phase == P::Ready) != self.readiness_request.is_some()
            || (self.phase == P::Cancelled) != self.cancelled_revision.is_some()
            || self.readiness_request.is_some() && self.execution_operation.is_none()
            || self
                .cancelled_revision
                .is_some_and(|r| r <= self.applied_revision || r > self.observed_revision)
        {
            return Err(RaftFleetFailure::invalid("inconsistent maintenance status"));
        }
        Ok(())
    }
}
