// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded construction, handoff or verified-history turn.
use super::*;

/// One owned turn materializes an original voter, requests handoff, or verifies
/// the pending history. Progress is historical and never a readiness lease.
#[derive(Clone, Debug)]
pub enum FleetRestartRuntimeProgress {
    /// Transfer admission only. A later turn must observe the survivor leader.
    HandoffRequested {
        group: GroupScope,
        from: MemberId,
        to: MemberId,
        term: cellar_logic::consensus::Term,
    },
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
    /// Once all voters exist, the receiver checks current leader-local matched
    /// and published progress. A requested transfer retains the history until a
    /// later turn observes a surviving leader. Normal elections continue.
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
            if let Some(progress) = genesis::next(self, &history, &deadline).await? {
                return Ok(Some(progress));
            }
            self.verify_one(deadline.remaining()?, true).await
        })
    }
}
