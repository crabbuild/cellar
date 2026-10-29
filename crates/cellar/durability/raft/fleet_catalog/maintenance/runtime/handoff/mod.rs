// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh matched-progress checks and an observed survivor leader. No stop grant.
mod receive;
mod reconcile;

use super::*;
use crate::raft::fleet_stop::BootBinding;
use serde::Deserialize;
use std::collections::BTreeMap;

pub(super) use reconcile::reconcile;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestartLeadershipCommand {
    pub context: RestartActionContext,
    pub term: u64,
    pub revision: u64,
    pub through: u64,
    pub action: LeadershipAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum LeadershipAction {
    Observe,
    Transfer {
        destination: BootBinding,
        member: u64,
    },
}

impl RestartLeadershipCommand {
    pub(crate) fn validate(&self) -> Result<()> {
        self.context.validate()?;
        if [self.term, self.revision, self.through]
            .iter()
            .any(|n| *n == 0 || *n >= u64::MAX - 2)
        {
            return Err(FleetCatalogError::Invalid("restart leadership indices"));
        }
        if let LeadershipAction::Transfer {
            destination,
            member,
        } = &self.action
        {
            destination
                .validate()
                .map_err(|_| FleetCatalogError::Invalid("restart leadership boot"))?;
            MemberId::new(*member)
                .map_err(|_| FleetCatalogError::Invalid("restart leadership member"))?;
            if destination.catalog != self.context.target.catalog
                || destination.fleet != self.context.target.fleet
                || destination.node == self.context.target.node
            {
                return Err(FleetCatalogError::Invalid("restart leadership destination"));
            }
        }
        Ok(())
    }
}
