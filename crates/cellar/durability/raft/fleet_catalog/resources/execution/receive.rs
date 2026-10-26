// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The receiving process independently resolves its current canonical permit.
use super::*;
use crate::application::consensus::{client::LearnerActivation, CommandFailure};
use crate::raft::member_state::RepairStep;
use cellar_logic::consensus::{LogIndex, MemberId};

pub(crate) fn command_failure(error: CommandFailure) -> FleetCatalogError {
    match error {
        CommandFailure::Overloaded => FleetCatalogError::Overloaded,
        CommandFailure::CommandConflict => FleetCatalogError::Conflict,
        CommandFailure::CorruptState => FleetCatalogError::Corrupt("movement host state"),
        CommandFailure::UnsupportedCapability => {
            FleetCatalogError::Invalid("movement host capability")
        }
        _ => FleetCatalogError::Unconfirmed,
    }
}

impl FleetHostAuthorization {
    pub(crate) async fn execute_movement(
        &self,
        command: MovementCommand,
        origin: BootBinding,
        minimum_revision: u64,
        gateway: &RaftCells,
        timeout: Duration,
    ) -> Result<()> {
        command.validate()?;
        origin.validate().map_err(|_| invalid())?;
        let catalog = &self.catalog;
        let deadline = super::super::super::deadline::Deadline::new(catalog, timeout)?;
        let snapshot = catalog.read(deadline.remaining()?).await?;
        if snapshot.revision() < minimum_revision {
            return Err(FleetCatalogError::Conflict);
        }
        let row = catalog
            .run(deadline.remaining()?, async {
                let current = catalog.host_from_record(&snapshot, self.node()).await?;
                self.require_running_successor(&current)?;
                if current.boot_binding()? != command.target {
                    return Err(FleetCatalogError::Conflict);
                }
                let origin_host = catalog
                    .host_from(&snapshot, NodeId::new(origin.node).map_err(|_| invalid())?)
                    .await?;
                if origin_host.boot_binding()? != origin
                    || !matches!(
                        origin_host.state(),
                        FleetNodeState::Active | FleetNodeState::Draining
                    )
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let row = catalog
                    .admitted_resource(&snapshot, command.reservation)
                    .await?;
                catalog.check_executable_movement(&snapshot, &row).await?;
                if hash(&row.encode()?) != command.resource_digest
                    || catalog.movement_boot(&snapshot, &row, self.node()).await? != command.target
                {
                    return Err(FleetCatalogError::Conflict);
                }
                if let MovementAction::Activate {
                    source,
                    source_node,
                    ..
                } = command.action
                {
                    let owner = row
                        .seed_owners
                        .as_ref()
                        .ok_or_else(invalid)?
                        .iter()
                        .find(|owner| owner.member == source && owner.node == source_node)
                        .ok_or_else(invalid)?;
                    if row.node != self.node() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    catalog
                        .movement_boot(
                            &snapshot,
                            &row,
                            NodeId::new(owner.node).map_err(|_| invalid())?,
                        )
                        .await?;
                }
                Ok(row)
            })
            .await?;
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let scope = permit.binding().scope().map_err(|_| invalid())?;
        let authorization = catalog
            .authorize_membership(&snapshot, &scope, deadline.remaining()?)
            .await?;
        let cell = match gateway.cell(scope.namespace, &scope.cell) {
            Ok(cell) if cell.scope() == &scope => cell,
            _ if matches!(command.action, MovementAction::Activate { .. }) => {
                let group = catalog
                    .authorize_group(&snapshot, &scope, deadline.remaining()?)
                    .await?;
                gateway
                    .register_managed_history(group, deadline.remaining()?)
                    .await
                    .map_err(command_failure)?
            }
            _ => return Err(FleetCatalogError::Conflict),
        };
        match command.action {
            MovementAction::Begin => cell
                .begin_managed_repair(
                    authorization,
                    row.request.movement.as_ref().ok_or_else(invalid)?.plan()?,
                    deadline.remaining()?,
                )
                .await
                .map(|_| ())
                .map_err(command_failure),
            MovementAction::Activate {
                source,
                source_node,
                from_s3,
            } => cell
                .activate_managed_learner(
                    authorization,
                    LearnerActivation {
                        member: permit.replacement().map_err(|_| invalid())?.member,
                        source: MemberId::new(source).map_err(|_| invalid())?,
                        source_node: NodeId::new(source_node).map_err(|_| invalid())?,
                        from_s3,
                        timeout: deadline.remaining()?,
                    },
                )
                .await
                .map(|_| ())
                .map_err(command_failure),
            MovementAction::Promote { revision } | MovementAction::Finish { revision } => {
                let step = if matches!(command.action, MovementAction::Promote { .. }) {
                    RepairStep::PromoteAndRemove
                } else {
                    RepairStep::FinishJoint
                };
                cell.advance_managed_repair(
                    authorization,
                    step,
                    LogIndex::new(revision),
                    deadline.remaining()?,
                )
                .await
                .map(|_| ())
                .map_err(command_failure)
            }
        }
    }
}
