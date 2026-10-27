// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared authenticated dispatch, with independent rooted action authorities.
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::fleet_stop::BootBinding;
use futures_util::future::BoxFuture;
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "command",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum FleetExecutionCommand {
    Movement(MovementCommand),
    RestartGenesis(RestartActionContext),
    RestartLeadership(Box<RestartLeadershipCommand>),
}

impl FleetExecutionCommand {
    pub(crate) fn target(&self) -> &BootBinding {
        match self {
            Self::Movement(command) => &command.target,
            Self::RestartGenesis(command) => &command.target,
            Self::RestartLeadership(command) => &command.context.target,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Movement(command) => command.validate(),
            Self::RestartGenesis(command) => command.validate(),
            Self::RestartLeadership(command) => command.validate(),
        }
    }
}

impl FleetHostAuthorization {
    pub(crate) fn execute_fleet_command<'a>(
        &'a self,
        command: FleetExecutionCommand,
        origin: BootBinding,
        minimum_revision: u64,
        gateway: &'a RaftCells,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            match command {
                FleetExecutionCommand::Movement(command) => {
                    self.execute_movement(command, origin, minimum_revision, gateway, timeout)
                        .await
                }
                FleetExecutionCommand::RestartLeadership(command) => {
                    self.execute_restart_leadership(
                        *command,
                        origin,
                        minimum_revision,
                        gateway,
                        timeout,
                    )
                    .await
                }
                FleetExecutionCommand::RestartGenesis(command) => {
                    self.execute_restart_genesis(
                        command,
                        origin,
                        minimum_revision,
                        gateway,
                        timeout,
                    )
                    .await
                }
            }
        })
    }
}
