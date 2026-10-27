// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete already-admitted original voters; never replace a missing engine.
mod materialize;
mod receive;

use super::*;
use crate::raft::fleet_stop::BootBinding;
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestartGenesisCommand {
    pub parent: [u8; 16],
    pub operation: [u8; 16],
    pub inputs: [u8; 32],
    pub group: String,
    pub history: [u8; 32],
    pub target: BootBinding,
}

impl RestartGenesisCommand {
    pub(crate) fn validate(&self) -> Result<()> {
        ProposalId::new(self.parent)
            .map_err(|_| FleetCatalogError::Invalid("genesis gate parent"))?;
        ProposalId::new(self.operation)
            .map_err(|_| FleetCatalogError::Invalid("genesis operation"))?;
        self.target
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("genesis boot"))?;
        if self.inputs == [0; 32]
            || self.history == [0; 32]
            || self.group.len() != 64
            || !self
                .group
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(FleetCatalogError::Invalid("genesis input pin"));
        }
        Ok(())
    }
}

/// One owned turn either materializes an original voter or verifies the pending
/// history. Materialized is an observed physical result, never a quorum lease.
#[derive(Clone, Debug)]
pub enum FleetRestartRuntimeProgress {
    Materialized {
        group: GroupScope,
        node: NodeId,
        member: MemberId,
        previously_present: bool,
    },
    Verified(FleetRestartHistoryProgress),
}

impl FleetRestartVerification {
    /// At most one original voter is initialized per turn. The immutable Live
    /// birth and one-time member claim remain the intent after a lost reply.
    /// Quorum/publication verification is a later turn after all voters exist.
    pub fn reconcile_next(
        &mut self,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<Option<FleetRestartRuntimeProgress>>> {
        Box::pin(async move {
            let catalog = self.catalog.clone();
            let deadline = Deadline::new(&catalog, timeout)?;
            let Some(history) = self.next_history(deadline.remaining()?).await? else {
                return Ok(None);
            };
            if let Some(progress) = materialize::next(self, &history, &deadline).await? {
                return Ok(Some(progress));
            }
            self.verify_next(deadline.remaining()?)
                .await
                .map(|p| p.map(FleetRestartRuntimeProgress::Verified))
        })
    }
}
