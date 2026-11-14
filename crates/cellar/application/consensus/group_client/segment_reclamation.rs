// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::{
    catalog::CatalogError,
    completion::RuntimeError,
    fleet_catalog::{FleetCatalogError, FleetHostAuthorization},
};

impl RaftCells {
    pub(crate) fn settles_periodic_raft_purges(&self) -> bool {
        self.handle.core.log.settles_periodic_purges()
    }

    pub(crate) async fn settle_periodic_raft_purge(
        &self,
        authority: &FleetHostAuthorization,
        settled: u64,
        timeout: Duration,
    ) -> Result<u64, FleetCatalogError> {
        if self.is_closed() {
            return Err(FleetCatalogError::Closed);
        }
        authority
            .settle_due_raft_segment_purge(&self.handle.core.log, settled, timeout)
            .await
    }

    /// Purge local Raft segments and settle their file lengths in the managed
    /// Fleet. Returns the new rooted segment-file byte floor for this engine.
    ///
    /// Start with number one, retry the same number after an uncertain result,
    /// and advance by one after settlement. The latest number is replayable
    /// from a retained boot on the same disk and engine.
    pub fn reclaim_raft_segments(
        &self,
        number: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<u64, CellGroupControllerError>> + Send + 'static {
        let admitted = self
            .metadata_admission(
                timeout,
                crate::raft::physical_inventory::SEGMENT_SETTLEMENT_BYTES,
            )
            .map_err(super::startup::admission_error);
        let owner = self.internal_observer();
        async move {
            let (deadline, _memory) = admitted?;
            let core = &owner.handle.core;
            let host = core.managed.as_ref().ok_or_else(|| {
                super::startup::catalog_error(CatalogError::ManagedAdmissionRequired)
            })?;
            owner
                .handle
                .domain
                .run(Box::pin(crate::asyncrt::timeout_at(
                    deadline,
                    Box::pin(async {
                        owner.startup_live(host, deadline)?;
                        host.settle_raft_segment_purge(
                            &core.log,
                            number,
                            owner.startup_remaining(deadline)?,
                        )
                        .await
                        .map_err(|error| super::startup::catalog_error(error.into()))
                    }),
                )))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))?
        }
    }
}
