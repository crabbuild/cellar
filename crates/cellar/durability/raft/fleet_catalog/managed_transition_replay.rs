// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resume a managed transfer from its exact retained source cuts.

use super::{managed_transitions::transfer_archive_error, *};
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{
        CellGroupRecoveryPool, CellGroupTransitionPreparationInputs, CellGroupTransitionSource,
        TransitionJournal, TransitionPhase, VerifiedCellGroupTransitionSources,
    },
};
use cellar_logic::consensus::ProposalId;

impl FleetCatalog {
    /// Rebuild the exact sealed input proofs for a replacement controller.
    /// The verifier must have room to pin one complete cut per parent. Reuse
    /// the returned proofs for destination install while the holds stay live.
    pub async fn replay_managed_sealed_sources(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        verifier: &FleetGroupArchiveVerifier,
        timeout: Duration,
    ) -> Result<VerifiedCellGroupTransitionSources> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .observe_transfer_replay(
                directory,
                operation,
                &[
                    TransitionPhase::Sealed,
                    TransitionPhase::Installed,
                    TransitionPhase::Archived,
                ],
                deadline.remaining()?,
            )
            .await?;
        let domain = &self.inner.domain;
        let sources = domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                open_transfer_sources(domain, directory, &transition, verifier),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)??;
        self.verify_managed_sealed_sources(
            &transition,
            directory,
            verifier.archives(),
            verifier.tails(),
            sources,
            deadline.remaining()?,
        )
        .await
    }

    /// Resume transfer-archive verification after the complete installed set
    /// was selected. Rebuild every child in the supplied verifier's private
    /// inventory and commit only if all installed roots match.
    pub async fn resume_managed_transfer_archive(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        verifier: &FleetGroupArchiveVerifier,
        restore: CellGroupRecoveryPool,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .observe_transfer_replay(
                directory,
                operation,
                &[TransitionPhase::Installed, TransitionPhase::Archived],
                deadline.remaining()?,
            )
            .await?;
        let domain = &self.inner.domain;
        let inputs = domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(async {
                    let sources =
                        open_transfer_sources(domain, directory, &transition, verifier).await?;
                    let (_, children) = directory
                        .transition_routes(&transition.plan)
                        .map_err(histories::catalog_error)?;
                    let storage = directory.namespace_config().image_storage;
                    let mut targets = Vec::with_capacity(children.len());
                    for route in &children {
                        targets.push(verifier.transfer_target(domain, route, storage).await?);
                    }
                    Ok::<_, FleetCatalogError>(CellGroupTransitionPreparationInputs {
                        archives: verifier.archives().clone(),
                        tails: verifier.tails().clone(),
                        restore,
                        sources,
                        targets,
                    })
                }),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)??;
        self.verify_managed_transfer_archive(&transition, directory, inputs, deadline.remaining()?)
            .await
    }

    async fn observe_transfer_replay(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        phases: &[TransitionPhase],
        timeout: Duration,
    ) -> Result<ReopenedManagedCellGroupTransition> {
        let observation = self
            .observe_managed_transition(directory, operation, timeout)
            .await?
            .filter(|observation| observation.fleet_slot_held)
            .ok_or(FleetCatalogError::Conflict)?;
        if !observation
            .transition
            .journal
            .as_ref()
            .is_some_and(|journal| phases.contains(&journal.phase()))
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(observation.transition)
    }
}

async fn open_transfer_sources(
    domain: &DomainToken,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    verifier: &FleetGroupArchiveVerifier,
) -> Result<Vec<CellGroupTransitionSource>> {
    let (parents, _) = directory
        .transition_routes(&transition.plan)
        .map_err(histories::catalog_error)?;
    let holds = directory
        .sealed_source_holds(&transition.plan)
        .await
        .map_err(transfer_archive_error)?;
    if parents.len() != holds.len() {
        return Err(FleetCatalogError::Corrupt("managed transition hold count"));
    }
    let storage = directory.namespace_config().image_storage;
    let mut sources = Vec::with_capacity(parents.len());
    for (route, hold) in parents.into_iter().zip(holds) {
        let birth = directory
            .materialize_selected_history(route.history)
            .await
            .map_err(histories::catalog_error)?;
        if birth.route() != &route || hold.marker().route() != &route {
            return Err(FleetCatalogError::Conflict);
        }
        let receiver = verifier.receiver(domain, &route, storage).await?;
        sources.push(CellGroupTransitionSource {
            birth,
            hold,
            receiver,
        });
    }
    Ok(sources)
}
