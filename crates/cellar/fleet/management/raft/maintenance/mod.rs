// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Human preparation/cancellation admission and strictly read-only status.
mod records;
use super::*;
pub use crate::raft::fleet_catalog::{
    FleetRestartBirthResolution as RaftFleetBirthResolution,
    FleetRestartOperatorPhase as RaftFleetMaintenancePhase, FleetRestartService,
    FleetRestartServiceOptions, FleetRestartServiceStatus,
};
use crate::raft::fleet_catalog::{FleetRestartOperatorCancelRequest, FleetRestartOperatorRequest};
use cellar_logic::consensus::ProcessGeneration;
pub use records::*;

impl RaftFleetControl {
    /// Explicit default-off execution, owned by the enrolled Node rather than
    /// the HTTP server. Recreating this service reconstructs durable intent;
    /// all ephemeral verification is repeated. Keep its handle until shutdown.
    pub async fn start_maintenance_service(
        &self,
        evidence: Option<Arc<dyn crate::raft::fleet_catalog::FleetMovementEvidence>>,
        options: FleetRestartServiceOptions,
    ) -> Result<FleetRestartService> {
        self.domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        Ok(self
            .catalog
            .start_restart_service(
                self.node_observer
                    .clone()
                    .ok_or_else(RaftFleetFailure::unsupported)?,
                self.namespaces.to_vec(),
                evidence,
                options,
            )
            .await?)
    }

    /// Journal admission only. A running owned service performs preparation;
    /// this call never supplies stop consent or restores placement capacity.
    pub async fn prepare_node_maintenance(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetMaintenancePrepare,
        timeout: Duration,
    ) -> Result<RaftFleetMaintenanceAcceptance> {
        request.validate()?;
        self.check_catalog(&request.operation.catalog_id)?;
        let identity = request.operation.identity(principal)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let admission = self
            .catalog
            .begin_restart_operator(
                &root,
                &FleetRestartOperatorRequest {
                    identity,
                    node: NodeId::new(super::records::id(node)?)
                        .map_err(|_| RaftFleetFailure::invalid("node ID"))?,
                    expected_process: ProcessGeneration::new(super::records::id(
                        &request.expected_process,
                    )?)
                    .map_err(|_| RaftFleetFailure::invalid("process ID"))?,
                    grace_seconds: request.grace_seconds,
                    birth_resolution: request.birth_resolution,
                },
                &self.namespaces,
                turn.remaining()?,
            )
            .await?;
        Ok(acceptance(
            self,
            node,
            &request.operation,
            admission.applied_revision(),
            admission.observed_revision(),
        ))
    }

    pub async fn cancel_node_maintenance(
        &self,
        principal: &str,
        node: &str,
        request: RaftFleetMaintenanceCancel,
        timeout: Duration,
    ) -> Result<RaftFleetMaintenanceAcceptance> {
        request.validate()?;
        self.check_catalog(&request.operation.catalog_id)?;
        let identity = request.operation.identity(principal)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let operation = ProposalId::new(identity.operation)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let intent = self
            .catalog
            .restart_operator(&root, operation, turn.remaining()?)
            .await?
            .ok_or_else(not_found)?;
        if intent.boot().node != super::records::id(node)? {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        let admission = self
            .catalog
            .cancel_restart_operator(
                &root,
                &FleetRestartOperatorCancelRequest {
                    identity,
                    fence: ProposalId::new(super::records::id(&request.fence_id)?)
                        .map_err(|_| RaftFleetFailure::invalid("fence ID"))?,
                },
                turn.remaining()?,
            )
            .await?;
        Ok(acceptance(
            self,
            node,
            &request.operation,
            admission.applied_revision(),
            admission.observed_revision(),
        ))
    }

    /// Reports one explicit Root snapshot. Never creates a driver, performs a
    /// proof walk, sends a physical command or writes coordination metadata.
    pub async fn maintenance_status(
        &self,
        operation: &str,
        timeout: Duration,
    ) -> Result<RaftFleetMaintenanceStatus> {
        let operation_id = ProposalId::new(super::records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("operation ID"))?;
        let turn = Turn::new(&self.domain, timeout)?;
        let root = self.catalog.read(turn.remaining()?).await?;
        let status = self
            .catalog
            .restart_operator_status(&root, operation_id, turn.remaining()?)
            .await?
            .ok_or_else(not_found)?;
        let a = &status.admission;
        let value = RaftFleetMaintenanceStatus {
            catalog_id: self.catalog_id.clone(),
            operation_id: operation.into(),
            request_id: hex::encode(a.identity().id),
            principal: a.identity().principal.clone(),
            expected_revision: a.identity().expected_revision,
            applied_revision: a.applied_revision(),
            observed_revision: root.revision(),
            node_id: hex::encode(a.boot().node),
            process: hex::encode(a.boot().process),
            boot: hex::encode(a.boot().boot),
            enrollment_epoch: a.boot().epoch,
            grace_seconds: a.grace_seconds(),
            birth_resolution: a.birth_resolution(),
            phase: status.phase,
            execution_operation: status.execution_operation.map(hex::encode),
            readiness_request: status.readiness.map(|r| hex::encode(r.id)),
            cancellation_request: status.cancellation.map(|r| hex::encode(r.id)),
            cancelled_revision: status.cancelled.map(|r| r.applied_revision()),
        };
        value.validate()?;
        Ok(value)
    }
}

fn not_found() -> RaftFleetFailure {
    RaftFleetFailure {
        code: RaftFleetFailureCode::NotFound,
        outcome: RaftFleetOutcome::Unknown,
        message:
            "Maintenance intent is absent at the selected Root; retain any unconfirmed request."
                .into(),
    }
}

fn acceptance(
    control: &RaftFleetControl,
    node: &str,
    request: &RaftFleetNodeOperation,
    applied: u64,
    observed: u64,
) -> RaftFleetMaintenanceAcceptance {
    RaftFleetMaintenanceAcceptance {
        catalog_id: control.catalog_id.clone(),
        node_id: node.into(),
        operation_id: request.operation_id.clone(),
        request_id: request.request_id.clone(),
        expected_revision: request.expected_revision,
        applied_revision: applied,
        observed_revision: observed,
        reconciled: false,
    }
}
