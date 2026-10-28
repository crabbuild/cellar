// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Internal fleet actions preserve one original bounded waiter.
use super::*;
use crate::raft::fleet_catalog::{FleetCatalogError, FleetExecutionCommand, MovementCommand};
use crate::raft::fleet_stop::BootBinding;

pub(crate) struct PeerFleetActionCall {
    pub command: FleetExecutionCommand,
    pub minimum_revision: u64,
}

pub(crate) struct LocalFleetActionCall {
    pub command: FleetExecutionCommand,
    pub origin: BootBinding,
    pub minimum_revision: u64,
}

impl RaftCells {
    pub(crate) async fn start_restart_worker(
        &self,
        work: crate::raft::fleet_catalog::RestartServiceWork,
        timeout: Duration,
    ) -> Result<(), FleetCatalogError> {
        match self
            .node_call(NodeKind::StartRestartService(Box::new(work)), timeout)
            .await
        {
            Ok(Response::FleetExecutionAction(result)) => result,
            Err(CommandFailure::Overloaded) => Err(FleetCatalogError::Overloaded),
            _ => Err(FleetCatalogError::Unconfirmed),
        }
    }

    pub(crate) async fn start_movement_worker(
        &self,
        work: crate::raft::fleet_catalog::MovementServiceWork,
        timeout: Duration,
    ) -> Result<(), FleetCatalogError> {
        match self
            .node_call(NodeKind::StartMovementService(Box::new(work)), timeout)
            .await
        {
            Ok(Response::FleetExecutionAction(result)) => result,
            Err(CommandFailure::Overloaded) => Err(FleetCatalogError::Overloaded),
            _ => Err(FleetCatalogError::Unconfirmed),
        }
    }

    pub(crate) fn request_peer_movement(
        &self,
        command: MovementCommand,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        self.request_peer_fleet_action(
            FleetExecutionCommand::Movement(command),
            minimum_revision,
            timeout,
        )
    }

    pub(crate) fn request_peer_fleet_action(
        &self,
        command: FleetExecutionCommand,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        if command.validate().is_err() {
            return Box::pin(async { Err(FleetCatalogError::Invalid("fleet execution command")) });
        }
        self.fleet_action_call(
            NodeKind::PeerFleetAction(Box::new(PeerFleetActionCall {
                command,
                minimum_revision,
            })),
            minimum_revision,
            timeout,
        )
    }

    pub(crate) fn execute_local_fleet_action(
        &self,
        command: FleetExecutionCommand,
        origin: BootBinding,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        self.fleet_action_call(
            NodeKind::LocalFleetAction(Box::new(LocalFleetActionCall {
                command,
                origin,
                minimum_revision,
            })),
            minimum_revision,
            timeout,
        )
    }

    fn fleet_action_call(
        &self,
        kind: NodeKind,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        if minimum_revision == 0
            || minimum_revision >= u64::MAX - 2
            || timeout.as_millis() == 0
            || timeout > Duration::from_secs(60)
        {
            return Box::pin(async { Err(FleetCatalogError::Invalid("fleet action bounds")) });
        }
        let call = self.node_call(kind, timeout);
        Box::pin(async move {
            match call.await {
                Ok(Response::FleetExecutionAction(result)) => result,
                Err(CommandFailure::Overloaded) => Err(FleetCatalogError::Overloaded),
                _ => Err(FleetCatalogError::Unconfirmed),
            }
        })
    }
}
