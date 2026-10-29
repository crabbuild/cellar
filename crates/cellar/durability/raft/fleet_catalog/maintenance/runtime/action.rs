// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted context shared by distinct restart receiving authorities.
use super::*;
use crate::raft::fleet_stop::BootBinding;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestartActionContext {
    pub parent: [u8; 16],
    pub operation: [u8; 16],
    pub inputs: [u8; 32],
    pub group: String,
    pub history: [u8; 32],
    pub target: BootBinding,
}

impl RestartActionContext {
    pub(crate) fn validate(&self) -> Result<()> {
        ProposalId::new(self.parent)
            .map_err(|_| FleetCatalogError::Invalid("restart gate parent"))?;
        ProposalId::new(self.operation)
            .map_err(|_| FleetCatalogError::Invalid("restart operation"))?;
        self.target
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("restart boot"))?;
        if self.inputs == [0; 32]
            || self.history == [0; 32]
            || self.group.len() != 64
            || !self
                .group
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(FleetCatalogError::Invalid("restart input pin"));
        }
        Ok(())
    }
}

pub(super) struct ActionHistory {
    pub gate: Gate,
    pub history: FleetHistory,
    pub voters: BTreeMap<NodeId, MemberId>,
    pub permit: Option<crate::raft::member_state::FleetMembershipPermit>,
}

impl FleetCatalog {
    pub(super) async fn restart_action_history(
        &self,
        snapshot: &FleetSnapshot,
        command: &RestartActionContext,
    ) -> Result<ActionHistory> {
        command.validate()?;
        let gate = self
            .checked_restart_gate(snapshot, command.operation)
            .await?;
        if gate.request.id != command.parent
            || Basis::capture(snapshot, &gate)?.input_digest()? != command.inputs
        {
            return Err(FleetCatalogError::Conflict);
        }
        self.require_restart_settlement_idle(snapshot, command.operation)
            .await?;
        let history = self.history_inner(snapshot, &command.group).await?;
        if *history.state() != FleetHistoryState::Live
            || hash(&history.encode()?) != command.history
        {
            return Err(FleetCatalogError::Conflict);
        }
        let main = NodeId::new(gate.node.node)
            .map_err(|_| FleetCatalogError::Corrupt("restart action node"))?;
        let Some((voters, permit)) = self.restart_voters(snapshot, &history, main).await? else {
            return Err(FleetCatalogError::Conflict);
        };
        let target = NodeId::new(command.target.node)
            .map_err(|_| FleetCatalogError::Invalid("restart action target"))?;
        if !voters.contains_key(&target) {
            return Err(FleetCatalogError::Conflict);
        }
        let host = self.host_from_record(snapshot, target).await?;
        let state = if target == main {
            FleetNodeState::RestartPending
        } else {
            FleetNodeState::Active
        };
        if host.boot_binding()? != command.target || host.state() != state {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(ActionHistory {
            gate,
            history,
            voters,
            permit,
        })
    }
}
