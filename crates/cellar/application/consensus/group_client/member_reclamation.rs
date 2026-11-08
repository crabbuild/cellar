// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit local cleanup after the canonical membership completion receipt.
use super::*;
use crate::raft::{catalog::CatalogError, completion::RuntimeError};

impl RaftCells {
    /// Drain and collect this boot's obsolete replica after a completed Fleet
    /// membership move. Returns false when bounded page collection needs another
    /// turn. Retry the same reservation after uncertainty. The synced fence
    /// rejects old member IDs while allowing a later managed learner seed.
    ///
    /// This physical-only operation retains the Fleet resource floor. Automatic
    /// cleanup discovery, retained successor boots and exact slot settlement
    /// require their own managed ownership boundaries.
    pub fn reclaim_removed_member(
        &self,
        reservation: [u8; 16],
        timeout: Duration,
    ) -> impl Future<Output = Result<bool, CellGroupControllerError>> + Send + 'static {
        let admitted = self
            .metadata_admission(timeout, 4096)
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
                        let now = owner
                            .handle
                            .domain
                            .mono_ms()
                            .map_err(|_| CellGroupControllerError::Closed)?;
                        let permit = host
                            .prepare_member_reclamation(
                                reservation,
                                &core.log,
                                Duration::from_millis(deadline.saturating_sub(now)),
                            )
                            .await
                            .map_err(|error| super::startup::catalog_error(error.into()))?;
                        owner.startup_live(host, deadline)?;
                        core.controller.reclaim_removed_member(permit).await
                    }),
                )))
                .await
                .map_err(|_| CellGroupControllerError::Closed)?
                .map_err(|_| CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded))?
        }
    }
}
