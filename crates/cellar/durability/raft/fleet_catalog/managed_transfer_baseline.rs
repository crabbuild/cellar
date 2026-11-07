// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A published transfer's original data remains replayable after all disk loss.
use super::{histories::catalog_error, managed_transitions::transfer_archive_error, *};
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{
        CellGroupRecoveryPool, CellGroupTransitionSource, PreparedCellGroupTransfer,
        TransitionPhase,
    },
};
use cellar_logic::consensus::ProposalId;

impl FleetCatalog {
    /// Independently rebuild the immutable baseline selected by a Published
    /// managed transfer, using only its retained remote closure and new private
    /// verifier inventories. No local voter data, callback or serving owner is
    /// needed. This does not include writes made by an already activated child;
    /// disaster recovery must also account for those children's archived tails.
    ///
    /// The result grants no membership, disk initialization, activation or write
    /// authority. Live recovery still requires fencing and new incarnations.
    pub async fn replay_managed_transfer_baseline(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        verifier: &FleetGroupArchiveVerifier,
        restore: CellGroupRecoveryPool,
        timeout: Duration,
    ) -> Result<PreparedCellGroupTransfer> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .observe_managed_transition(directory, operation, deadline.remaining()?)
            .await?
            .filter(|observation| observation.fleet_slot_held)
            .map(|observation| observation.transition)
            .ok_or(FleetCatalogError::Conflict)?;
        if !transition
            .journal
            .as_ref()
            .is_some_and(|journal| journal.phase() == TransitionPhase::Published)
        {
            return Err(FleetCatalogError::Conflict);
        }
        let (snapshot, permit) = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let snapshot = self.inner.current().await?;
                    let permit = self
                        .managed_transition_permit_at(
                            &snapshot,
                            &transition.request,
                            directory,
                            &transition.plan,
                        )
                        .await?;
                    self.verify_managed_transfer_storage_at(
                        &snapshot,
                        &transition,
                        directory,
                        &permit,
                    )
                    .await?;
                    Ok((snapshot, permit))
                }),
            )
            .await?;
        let domain = &self.inner.domain;
        let restored = domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(async {
                    let descriptors = directory
                        .managed_transfer_baseline_sources(&transition.plan, &permit)
                        .await
                        .map_err(transfer_archive_error)?;
                    let mut sources = Vec::with_capacity(descriptors.len());
                    let storage = directory.namespace_config().image_storage;
                    for (birth, hold) in descriptors {
                        let receiver = verifier.receiver(domain, birth.route(), storage).await?;
                        sources.push(CellGroupTransitionSource {
                            birth,
                            hold,
                            receiver,
                        });
                    }
                    let inputs = super::managed_transition_replay::transfer_inputs(
                        domain,
                        directory,
                        &transition,
                        verifier,
                        restore,
                        sources,
                    )
                    .await?;
                    directory
                        .replay_managed_transfer_baseline(domain, &transition.plan, &permit, inputs)
                        .await
                        .map_err(transfer_archive_error)
                }),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)??;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                permit
                    .check(directory, &transition.plan)
                    .map_err(catalog_error)?;
                Ok(())
            }),
        )
        .await?;
        Ok(restored)
    }
}
