// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resume a managed transfer from its exact retained source cuts.

use super::{histories::validate_request, managed_transitions::transfer_archive_error, *};
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{
        CellGroupArchiveHold, CellGroupArchivePool, CellGroupArchiveTailPool,
        CellGroupRecoveryPool, CellGroupTransitionPreparationInputs, CellGroupTransitionSource,
        PreparedManagedCellGroupTransition, TransitionJournal, TransitionPhase,
        VerifiedCellGroupTransitionSources,
    },
};
use cellar_logic::{
    cell_group::{GroupHistory, GroupRoute},
    consensus::ProposalId,
};

impl FleetCatalog {
    /// Stage operation-owned holds from the selected active parent archives.
    /// A retry reconciles the rooted acquire set before any new allocation.
    pub async fn allocate_managed_preparation_holds(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .observe_transfer_replay(
                directory,
                operation,
                &[TransitionPhase::Planned, TransitionPhase::Prepared],
                deadline.remaining()?,
            )
            .await?;
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
                Box::pin(directory.allocate_managed_preparation_holds(
                    domain,
                    archives,
                    &transition.plan,
                    &permit,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Clean up only the cancelled transition's own acquire intents. The
    /// controller must join entered pre-copy work before invoking cleanup.
    pub async fn release_cancelled_preparation_holds(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .reopen_managed_transition(directory, operation, deadline.remaining()?)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.release_cancelled_preparation_acquires(
                    domain,
                    archives,
                    &transition.plan,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Persist the controller's own active source holds in plan order before
    /// opening physical pre-copy receivers. Keep the selected holds alive
    /// through retirement, or join pre-copy work before cancelled cleanup.
    pub async fn stage_managed_preparation_holds(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        holds: &[CellGroupArchiveHold],
        timeout: Duration,
    ) -> Result<TransitionJournal> {
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
                Box::pin(directory.stage_managed_preparation_holds(
                    domain,
                    archives,
                    &transition.plan,
                    &permit,
                    holds,
                    None,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Observe the selected inputs without opening physical data or changing
    /// the decision. Cancelled transitions retain the descriptors so the hold
    /// owner can reconcile and clean up after joining its work.
    pub async fn staged_managed_preparation_holds(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<Vec<CellGroupArchiveHold>>> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .reopen_managed_transition(directory, operation, deadline.remaining()?)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        self.inner
            .domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.staged_managed_preparation_holds(&transition.plan)),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Independently rebuild the selected pre-copy after a controller restart.
    /// Staged inputs can advance a Planned transition. Reopening a Prepared
    /// transition must reproduce the same physical partition and journal.
    pub async fn resume_managed_preparation(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        verifier: &FleetGroupArchiveVerifier,
        restore: CellGroupRecoveryPool,
        timeout: Duration,
    ) -> Result<PreparedManagedCellGroupTransition> {
        let deadline = deadline::Deadline::new(self, timeout)?;
        let transition = self
            .observe_transfer_replay(
                directory,
                operation,
                &[TransitionPhase::Planned, TransitionPhase::Prepared],
                deadline.remaining()?,
            )
            .await?;
        let domain = &self.inner.domain;
        let inputs = domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(async {
                    let holds = match directory
                        .staged_managed_preparation_holds(&transition.plan)
                        .await
                        .map_err(transfer_archive_error)?
                    {
                        Some(holds) => holds,
                        None if transition.journal.as_ref().is_some_and(|journal| {
                            journal.phase() == TransitionPhase::Prepared
                        }) =>
                        {
                            directory
                                .managed_preparation_holds(&transition.plan)
                                .await
                                .map_err(histories::catalog_error)?
                        }
                        None => return Err(FleetCatalogError::Unconfirmed),
                    };
                    let sources =
                        open_sources_for_holds(domain, directory, &transition, verifier, holds)
                            .await?;
                    transfer_inputs(domain, directory, &transition, verifier, restore, sources)
                        .await
                }),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)??;
        self.prepare_managed_transition(
            &transition.request,
            directory,
            &transition.plan,
            inputs,
            deadline.remaining()?,
        )
        .await
    }

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
                    (source, None),
                    &permit,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Capture the selected parent's archive under the irreversible plan.
    /// An uncertain reply leaves an exact operation-owned slot for the next
    /// controller to reconcile before opening another physical receiver.
    pub async fn capture_managed_sealed_source(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        history: GroupHistory,
        verifier: &FleetGroupArchiveVerifier,
        timeout: Duration,
    ) -> Result<CellGroupArchiveHold> {
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
                Box::pin(async {
                    let (hold, intent) = directory
                        .acquire_managed_sealed_source(
                            domain,
                            verifier.archives(),
                            &transition.plan,
                            &permit,
                            history,
                        )
                        .await
                        .map_err(transfer_archive_error)?;
                    if let Some(intent) = intent {
                        let birth = directory
                            .materialize_selected_history(history)
                            .await
                            .map_err(histories::catalog_error)?;
                        let receiver = verifier
                            .receiver(
                                domain,
                                hold.marker().route(),
                                directory.namespace_config().image_storage,
                            )
                            .await?;
                        Box::pin(directory.stage_managed_sealed_source(
                            domain,
                            verifier.archives(),
                            verifier.tails(),
                            &transition.plan,
                            (
                                CellGroupTransitionSource {
                                    birth,
                                    hold: hold.clone(),
                                    receiver,
                                },
                                Some(intent),
                            ),
                            &permit,
                        ))
                        .await
                        .map_err(transfer_archive_error)?;
                    }
                    Ok(hold)
                }),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
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
                    transfer_inputs(domain, directory, &transition, verifier, restore, sources)
                        .await
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
    open_sources_for_holds(domain, directory, transition, verifier, holds).await
}

async fn open_sources_for_holds(
    domain: &DomainToken,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    verifier: &FleetGroupArchiveVerifier,
    holds: Vec<CellGroupArchiveHold>,
) -> Result<Vec<CellGroupTransitionSource>> {
    let (parents, _) = directory
        .transition_routes(&transition.plan)
        .map_err(histories::catalog_error)?;
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

pub(super) async fn transfer_inputs(
    domain: &DomainToken,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    verifier: &FleetGroupArchiveVerifier,
    restore: CellGroupRecoveryPool,
    sources: Vec<CellGroupTransitionSource>,
) -> Result<CellGroupTransitionPreparationInputs> {
    let (_, children) = directory
        .transition_routes(&transition.plan)
        .map_err(histories::catalog_error)?;
    let storage = directory.namespace_config().image_storage;
    let mut targets = Vec::with_capacity(children.len());
    for route in &children {
        targets.push(verifier.transfer_target(domain, route, storage).await?);
    }
    Ok(CellGroupTransitionPreparationInputs {
        archives: verifier.archives().clone(),
        tails: verifier.tails().clone(),
        restore,
        sources,
        targets,
    })
}
