// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resume a managed transfer from its exact retained source cuts.

use super::{histories::validate_request, managed_transitions::transfer_archive_error, *};
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{
        CellGroupArchiveHold, CellGroupArchivePool, CellGroupArchiveTailPool,
        CellGroupRecoveryPool, CellGroupTransitionPreparationInputs, CellGroupTransitionSource,
        TransitionJournal, TransitionPhase, VerifiedCellGroupTransitionSources,
    },
};
use cellar_logic::{cell_group::GroupRoute, consensus::ProposalId};

impl FleetCatalog {
    /// Independently verify and durably select one sealed parent hold. Retain
    /// the exact hold through retirement. After an uncertain result, observe
    /// the staged slots before resolving the original hold allocation.
    pub async fn stage_managed_sealed_source(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        source: CellGroupTransitionSource,
        timeout: Duration,
    ) -> Result<()> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let permit = self
            .run(
                deadline.remaining()?,
                Box::pin(self.active_managed_transition_permit(
                    &transition.request,
                    directory,
                    &transition.plan,
                )),
            )
            .await?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.stage_managed_sealed_source(
                    domain,
                    archives,
                    tails,
                    &transition.plan,
                    source,
                    &permit,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Read the plan-order staged holds for the active operation. Missing
    /// slots identify parents that still need a verified archive cut.
    pub async fn staged_managed_sealed_sources(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Vec<(GroupRoute, Option<CellGroupArchiveHold>)>> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .observe_transfer_replay(
                directory,
                operation,
                &[
                    TransitionPhase::SealAuthorized,
                    TransitionPhase::Sealed,
                    TransitionPhase::Installed,
                    TransitionPhase::Archived,
                ],
                deadline.remaining()?,
            )
            .await?;
        self.inner
            .domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.managed_staged_source_holds(&transition.plan)),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Rebuild the exact sealed input proofs for a replacement controller.
    /// The verifier must have room to pin one complete cut per parent. Reuse
    /// the returned proofs for destination install while the holds stay live.
    /// A complete set of staged holds can establish the Sealed decision.
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
                    TransitionPhase::SealAuthorized,
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
    let holds = if transition
        .journal
        .as_ref()
        .is_some_and(|journal| journal.phase() == TransitionPhase::SealAuthorized)
    {
        directory
            .managed_staged_source_holds(&transition.plan)
            .await
            .map_err(transfer_archive_error)?
            .into_iter()
            .map(|(_, hold)| hold.ok_or(FleetCatalogError::Unconfirmed))
            .collect::<Result<Vec<_>>>()?
    } else {
        directory
            .sealed_source_holds(&transition.plan)
            .await
            .map_err(transfer_archive_error)?
    };
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
