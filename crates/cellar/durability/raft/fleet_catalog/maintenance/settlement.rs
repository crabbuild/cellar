// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete rooted pending work, distinct from live survivor/stop readiness.
use super::*;
use cellar_logic::consensus::GroupScope;

/// Descriptive work at one current root. Memory is bounded to the next birth
/// and the active resource book (at most two rows). An empty result proves no
/// current health, leadership, worker exit or permission to stop/release debt.
#[derive(Clone, Debug)]
pub struct FleetRestartPendingWork {
    pub observed_revision: u64,
    pub pending_births: u64,
    pub next_birth: Option<GroupScope>,
    pub movements: Vec<FleetPendingMovement>,
    pub reservations: Vec<FleetPendingReservation>,
}

impl FleetCatalog {
    /// Discover every pre-admitted obligation involving this restart target.
    /// Caller namespace handles must cover the complete declared inventory.
    /// Re-read after each mutation; no stale report is an execution grant.
    pub async fn pending_restart_work(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetRestartPendingWork> {
        self.check_snapshot(snapshot)?;
        if namespaces.len() > namespaces::MAX_NAMESPACES {
            return Err(FleetCatalogError::Limit);
        }
        self.run(timeout, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            let Some(super::super::disruption::Disruption::Restart(preparation)) =
                self.active_disruption(snapshot).await?
            else {
                return Err(FleetCatalogError::Conflict);
            };
            if preparation.request.operation != operation.bytes() {
                return Err(FleetCatalogError::Conflict);
            }
            let node = NodeId::new(preparation.node.node)
                .map_err(|_| FleetCatalogError::Corrupt("restart node"))?;
            let current = self.host_from_record(snapshot, node).await?;
            if current.record != preparation.node {
                // The only supported successor is the exact journal-confirmed
                // reversible gate; cancellation/stop/rejoin cannot reuse this.
                self.checked_restart_gate(snapshot, operation.bytes())
                    .await?;
            }
            self.inner
                .domain
                .run(Box::pin(self.check_inventory_inner(snapshot, namespaces)))
                .await
                .map_err(|_| FleetCatalogError::Closed)??;
            let result = self.restart_pending_work_inner(snapshot, node).await?;
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(result)
        })
        .await
    }
}
