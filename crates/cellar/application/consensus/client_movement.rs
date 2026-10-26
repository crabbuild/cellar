// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Internal movement controls preserve one original bounded waiter.
use super::*;
use crate::raft::fleet_catalog::{FleetCatalogError, MovementCommand};
use crate::raft::fleet_stop::BootBinding;

pub(crate) struct PeerMovementCall {
    pub command: MovementCommand,
    pub minimum_revision: u64,
}

pub(crate) struct LocalMovementCall {
    pub command: MovementCommand,
    pub origin: BootBinding,
    pub minimum_revision: u64,
}

impl RaftCells {
    pub(crate) fn request_peer_movement(
        &self,
        command: MovementCommand,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        if command.validate().is_err() {
            return Box::pin(async { Err(FleetCatalogError::Invalid("movement command")) });
        }
        self.movement_call(
            NodeKind::PeerMovement(Box::new(PeerMovementCall {
                command,
                minimum_revision,
            })),
            minimum_revision,
            timeout,
        )
    }

    pub(crate) fn execute_local_movement(
        &self,
        command: MovementCommand,
        origin: BootBinding,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        self.movement_call(
            NodeKind::LocalMovement(Box::new(LocalMovementCall {
                command,
                origin,
                minimum_revision,
            })),
            minimum_revision,
            timeout,
        )
    }

    fn movement_call(
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
            return Box::pin(async { Err(FleetCatalogError::Invalid("movement bounds")) });
        }
        let call = self.node_call(kind, timeout);
        Box::pin(async move {
            match call.await {
                Ok(Response::FleetMovementAction(result)) => result,
                Err(CommandFailure::Overloaded) => Err(FleetCatalogError::Overloaded),
                _ => Err(FleetCatalogError::Unconfirmed),
            }
        })
    }
}
