// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Demand-driven metadata refresh retains node budgets and original deadlines.
use super::*;
use crate::raft::{completion::RuntimeError, group_host::RouterRefreshRequest};
use tokio::sync::mpsc;

impl RaftCells {
    /// Catch up a lagging inspection receiver within the incoming request's
    /// original deadline. This grants no group or application authority.
    pub(crate) async fn refresh_inspection_trust(
        &self,
        previous: crate::raft::fleet_catalog::FleetTrustSnapshot,
        minimum_revision: u64,
        deadline: u64,
    ) -> Result<(), cellar_logic::consensus::CommandFailure> {
        use crate::raft::fleet_catalog::{FleetCatalogError, FleetTrustReader};
        use cellar_logic::consensus::CommandFailure;

        self.metadata_live(deadline)?;
        let remaining = deadline
            .checked_sub(
                self.handle
                    .domain
                    .mono_ms()
                    .map_err(|_| CommandFailure::Unavailable)?,
            )
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .ok_or(CommandFailure::VersionUnavailable)?;
        let (_, _memory) =
            self.metadata_admission(remaining, FleetTrustReader::RESERVATION_BYTES)?;
        let reader = self
            .handle
            .core
            .fleet_inspection
            .as_ref()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let trust = self
            .handle
            .domain
            .run(crate::asyncrt::timeout_at(
                deadline,
                reader.refresh(Some(&previous), remaining),
            ))
            .await
            .map_err(|_| CommandFailure::Unavailable)?
            .map_err(|_| CommandFailure::VersionUnavailable)?
            .map_err(|error| match error {
                FleetCatalogError::Overloaded | FleetCatalogError::Limit => {
                    CommandFailure::Overloaded
                }
                _ => CommandFailure::Unavailable,
            })?;
        self.metadata_live(deadline)?;
        if trust.revision() < minimum_revision {
            return Err(CommandFailure::Unavailable);
        }
        crate::raft::group_host::require_local_fleet_boot(&trust, &self.handle.core.log)
            .map_err(|_| CommandFailure::Unavailable)?;
        let trust = self
            .handle
            .core
            .transport
            .install_refreshed_fleet_trust(trust)
            .map_err(|_| CommandFailure::Unavailable)?;
        crate::raft::group_host::require_local_fleet_boot(&trust, &self.handle.core.log)
            .map_err(|_| CommandFailure::Unavailable)
    }

    async fn refresh_local_namespace_routes(
        &self,
        history: GroupHistory,
        deadline: u64,
    ) -> Result<(), RuntimeError> {
        self.metadata_live(deadline)
            .map_err(|_| RuntimeError::Closed)?;
        self.handle
            .domain
            .run(crate::asyncrt::timeout_at(
                deadline,
                self.handle.core.host.refresh_namespace_routes(history),
            ))
            .await
            .map_err(|_| RuntimeError::Closed)?
            .map_err(|_| RuntimeError::DeadlineExceeded)?
            .map_err(|_| RuntimeError::NotReady)?;
        self.metadata_live(deadline)
            .map_err(|_| RuntimeError::DeadlineExceeded)?;
        Ok(())
    }

    pub(crate) fn bind_router_refresh(
        &self,
    ) -> Result<mpsc::Receiver<RouterRefreshRequest>, RuntimeError> {
        self.handle
            .core
            .transport
            .bind_router_refresh(self.control_budget().clone())
    }

    pub(crate) fn refresh_managed_router(
        &self,
        history: GroupHistory,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send + 'static {
        let admitted = self
            .metadata_admission(timeout, 4096)
            .map_err(|error| match error {
                cellar_logic::consensus::CommandFailure::Overloaded => RuntimeError::Overloaded,
                _ => RuntimeError::Closed,
            });
        let owner = self.internal_observer();
        async move {
            let (deadline, _memory) = admitted?;
            owner
                .handle
                .core
                .transport
                .refresh_managed_router(history, deadline)
                .await?;
            owner
                .refresh_local_namespace_routes(history, deadline)
                .await
        }
    }
}
