// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted fleet ownership of a managed split or merge before any seal.
use super::disruption::{Disruption, SLOT_KEY};
use super::histories::{catalog_error, history_mutation, validate_request, Origin};
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::{
    catalog::CatalogError,
    cell_groups::{
        CellGroupArchiveBindingError, CellGroupArchiveCoverage, CellGroupArchiveHold,
        CellGroupArchivePool, CellGroupArchiveTailPool, CellGroupSourceRetirementAck,
        CellGroupTransferActivationAck, CellGroupTransferArchive,
        CellGroupTransitionPreparationInputs, CellGroupTransitionSource, DirectoryMutation,
        DirectoryPublication, PreparedManagedCellGroupTransition, RangeDirectory,
        TransferChildQuorum, TransferInstallQuorum, TransitionJournal,
        VerifiedCellGroupTransitionSources,
    },
    fleet_stop::BootBinding,
    group_host::RaftGroupArchiveInitialization,
    member_state::FleetGroupBinding,
};

use cellar_logic::{
    cell_group::{GroupHistory, GroupRoute},
    consensus::{LogPosition, NodeId, ProposalId},
};
use serde::Deserialize;
use std::collections::BTreeMap;

pub(super) const KIND: u8 = 201;
const OPERATION_PREFIX: &str = "managed-transition/";
const RECORD_LIMIT: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    catalog: [u8; 32],
    namespace_root: [u8; 32],
    input_root: [u8; 32],
    plan: [u8; 32],
    request: FleetRequestIdentity,
}

impl Record {
    pub(super) fn validate(&self, fleet: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        if self.catalog != fleet.catalog_id()?
            || [self.namespace_root, self.input_root, self.plan].contains(&[0; 32])
        {
            return Err(FleetCatalogError::Invalid("managed transition binding"));
        }
        Ok(())
    }

    pub(super) fn operation(&self) -> [u8; 16] {
        self.request.operation
    }

    pub(super) fn key(&self) -> String {
        operation_key(self.operation())
    }

    pub(super) fn check_basis(&self, snapshot: &FleetSnapshot) -> Result<()> {
        if self.request.expected_revision != snapshot.revision()
            || self.input_root != root_digest(snapshot)?
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    pub(super) fn mutation(&self) -> Result<FleetMutation> {
        let value = encode(KIND, self, RECORD_LIMIT)?;
        Ok(history_mutation(
            &self.request,
            vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: self.key(),
                    value: Some(value.clone()),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: SLOT_KEY.into(),
                    value: Some(value),
                },
            ],
        ))
    }
}

fn operation_key(operation: [u8; 16]) -> String {
    format!("{OPERATION_PREFIX}{}", hex::encode(operation))
}

pub(super) fn transfer_archive_error(error: CellGroupArchiveBindingError) -> FleetCatalogError {
    match error {
        CellGroupArchiveBindingError::Catalog(error) => catalog_error(error),
        CellGroupArchiveBindingError::Archive(error) => error.into(),
    }
}

pub(super) fn root_digest(snapshot: &FleetSnapshot) -> Result<[u8; 32]> {
    Ok(hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?))
}

/// Private capability recovered only from an exact committed fleet intent.
pub(crate) struct ManagedTransitionPermit(Record);

impl ManagedTransitionPermit {
    pub(crate) fn check(
        &self,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
    ) -> std::result::Result<(), CatalogError> {
        self.check_directory(directory)?;
        let record = &self.0;
        if plan.operation().bytes() != record.operation() || plan.digest() != record.plan {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(())
    }

    fn check_directory(&self, directory: &RangeDirectory) -> std::result::Result<(), CatalogError> {
        let record = &self.0;
        if directory
            .namespace_config()
            .managed_fleet
            .as_ref()
            .map(|owner| owner.catalog)
            != Some(record.catalog)
            || directory.namespace().root_digest() != record.namespace_root
        {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(())
    }
}

/// Exact admitted plan and any directory decision. A missing decision means
/// the fleet selected the plan before directory publication was confirmed.
/// This observation grants no sealing, installation or serving authority.
pub struct ReopenedManagedCellGroupTransition {
    pub request: FleetRequestIdentity,
    pub plan: DirectoryMutation,
    pub journal: Option<TransitionJournal>,
}

enum TransferCompletion {
    Replayed(Box<FleetCommit>),
    Pending {
        mutation: FleetMutation,
        disruption: [u8; 32],
        release_holds: bool,
    },
}

pub(crate) struct ManagedTransitionObservation {
    pub(crate) fleet_revision: u64,
    pub(crate) fleet_slot_held: bool,
    pub(crate) transition: ReopenedManagedCellGroupTransition,
}

pub(crate) struct ManagedTransitionRecoveryObservation {
    pub(crate) fleet_revision: u64,
    pub(crate) fleet_digest: [u8; 32],
    pub(crate) selection: crate::raft::catalog::ManagedTransitionRecoverySelection,
}

impl FleetCatalog {
    /// Reopen an admitted transition by operation ID. This reads the original
    /// fleet request and canonical plan bytes even after the caller loses its
    /// local copy. Resume planning by resubmitting the returned exact request.
    pub async fn reopen_managed_transition(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<ReopenedManagedCellGroupTransition>> {
        Ok(self
            .observe_managed_transition(directory, operation, timeout)
            .await?
            .map(|observation| observation.transition))
    }

    pub(crate) async fn observe_managed_transition(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<Option<ManagedTransitionObservation>> {
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                let Some(record) = self
                    .managed_transition_record(&current, operation.bytes())
                    .await?
                else {
                    return Ok(None);
                };
                let permit = ManagedTransitionPermit(record);
                permit.check_directory(directory).map_err(catalog_error)?;
                let record = &permit.0;
                let active = matches!(
                    self.active_disruption(&current).await?,
                    Some(Disruption::Transition(active)) if active == *record
                );
                let (plan, journal) = self
                    .inner
                    .domain
                    .run(Box::pin(directory.reopen_managed_transition_plan(
                        operation,
                        record.plan,
                        &permit,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                if !active
                    && !journal
                        .as_ref()
                        .is_some_and(|decision| !decision.owns_topology())
                {
                    return Err(FleetCatalogError::Corrupt(
                        "released managed transition has no terminal decision",
                    ));
                }
                Ok(Some(ManagedTransitionObservation {
                    fleet_revision: current.revision(),
                    fleet_slot_held: active,
                    transition: ReopenedManagedCellGroupTransition {
                        request: record.request.clone(),
                        plan,
                        journal,
                    },
                }))
            }),
        )
        .await
    }

    pub(crate) async fn observe_managed_transition_recovery(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<ManagedTransitionRecoveryObservation> {
        self.run(
            timeout,
            Box::pin(async {
                let snapshot = self.inner.current().await?;
                let fleet_digest = root_digest(&snapshot)?;
                let Some(Disruption::Transition(record)) =
                    self.active_disruption(&snapshot).await?
                else {
                    return Err(FleetCatalogError::Conflict);
                };
                if record.operation() != operation.bytes() {
                    return Err(FleetCatalogError::Conflict);
                }
                let digest = record.plan;
                let permit = ManagedTransitionPermit(record);
                permit.check_directory(directory).map_err(catalog_error)?;
                let selection = self
                    .inner
                    .domain
                    .run(Box::pin(directory.observe_managed_transition_recovery(
                        operation, digest, &permit,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                if root_digest(&self.inner.current().await?)? != fleet_digest {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(ManagedTransitionRecoveryObservation {
                    fleet_revision: snapshot.revision(),
                    fleet_digest,
                    selection,
                })
            }),
        )
        .await
    }

    pub(crate) async fn verify_managed_transition_recovery_archives(
        &self,
        directory: &RangeDirectory,
        observation: &ManagedTransitionRecoveryObservation,
        request: &FleetRequestIdentity,
        holds: Vec<CellGroupArchiveHold>,
        verifier: &FleetGroupArchiveVerifier,
        timeout: Duration,
    ) -> Result<Vec<CellGroupArchiveCoverage>> {
        self.run(
            timeout,
            Box::pin(async {
                let snapshot = self.inner.current().await?;
                if root_digest(&snapshot)? != observation.fleet_digest {
                    return Err(FleetCatalogError::Conflict);
                }
                let selection = &observation.selection;
                let permit = self
                    .managed_transition_permit_at(&snapshot, request, directory, &selection.plan)
                    .await?;
                let domain = &self.inner.domain;
                domain
                    .run(Box::pin(directory.check_managed_transition_recovery_holds(
                        selection, &permit, &holds,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                let storage = directory.namespace_config().image_storage;
                let mut verified = Vec::with_capacity(holds.len());
                for (route, hold) in selection.archive_routes().iter().zip(&holds) {
                    let birth = domain
                        .run(directory.materialize_selected_history(route.history))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(catalog_error)?;
                    if birth.route() != route {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let registry = directory
                        .open_group_archive(domain, verifier.archives(), &birth)
                        .await
                        .map_err(transfer_archive_error)?;
                    registry.require_selected_hold(domain, hold).await?;
                    let receiver = verifier.receiver(domain, route, storage).await?;
                    let coverage = registry
                        .verify_hold(domain, hold, verifier.tails(), receiver)
                        .await?;
                    verified.push((registry, coverage));
                }
                for (registry, coverage) in &verified {
                    registry
                        .require_selected_hold(domain, coverage.hold())
                        .await?;
                }
                let current = domain
                    .run(directory.observe())
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                if current.digest() != selection.directory_digest
                    || root_digest(&self.inner.current().await?)? != observation.fleet_digest
                {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(verified.into_iter().map(|(_, coverage)| coverage).collect())
            }),
        )
        .await
    }

    /// Acquire the fleet disruption slot and plan the exact range mutation.
    /// Reserve all child births against the same operation before calling.
    /// Persist the request and plan before submission. Replaying both after
    /// controller loss recovers the original admission or cancellation.
    /// This step grants no seal, installation or serving authority.
    pub async fn plan_managed_transition(
        &self,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(request)?;
        let (parents, children) = directory.transition_routes(plan).map_err(catalog_error)?;
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                let original = self
                    .managed_transition_record(&current, request.operation)
                    .await?;
                let replay = original.is_some();
                let record = match original {
                    Some(record) if record.request == *request => record,
                    Some(_) => return Err(FleetCatalogError::PayloadConflict),
                    None => Record {
                        catalog: self.catalog_id()?,
                        namespace_root: directory.namespace().root_digest(),
                        input_root: root_digest(&current)?,
                        plan: plan.digest(),
                        request: request.clone(),
                    },
                };
                let permit = ManagedTransitionPermit(record);
                permit.check(directory, plan).map_err(catalog_error)?;
                let record = &permit.0;
                if replay {
                    if !matches!(
                        self.active_disruption(&current).await?,
                        Some(Disruption::Transition(active)) if active == *record
                    ) {
                        let status = self
                            .inner
                            .domain
                            .run(Box::pin(directory.managed_transition_status(plan, &permit)))
                            .await
                            .map_err(|_| FleetCatalogError::Closed)?
                            .map_err(catalog_error)?;
                        return status.filter(|journal| !journal.owns_topology()).ok_or(
                            FleetCatalogError::Corrupt("missing terminal managed transition"),
                        );
                    }
                } else {
                    let snapshot = &current;
                    record.validate(self)?;
                    record.check_basis(snapshot)?;
                    if let Some(restart) = self.active_restart_operator_inner(snapshot).await? {
                        return Err(FleetCatalogError::DisruptionInProgress {
                            operation: restart.identity().operation,
                        });
                    }
                    if let Some(active) = self.active_disruption(snapshot).await? {
                        return Err(FleetCatalogError::DisruptionInProgress {
                            operation: active.operation(),
                        });
                    }
                    self.require_namespace_declaration(snapshot, directory.namespace(), None)
                        .await?;
                    self.require_transition_membership_idle(snapshot).await?;
                    for (route, state) in parents
                        .iter()
                        .map(|route| (route, FleetHistoryState::Live))
                        .chain(
                            children
                                .iter()
                                .map(|route| (route, FleetHistoryState::Reserved)),
                        )
                    {
                        let binding = self.binding(&route.history)?;
                        let key = binding
                            .key()
                            .map_err(|_| FleetCatalogError::Invalid("transition group binding"))?;
                        let history = self.history_inner(snapshot, &key).await?;
                        if history.origin != Origin::Managed
                            || history.state != state
                            || !history.check_route(directory, route)
                            || state == FleetHistoryState::Reserved
                                && history.operation != record.operation()
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    self.inner
                        .domain
                        .run(Box::pin(
                            directory.stage_managed_transition_plan(plan, &permit),
                        ))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(catalog_error)?;
                    let mutation = record.mutation()?;
                    mutation.validate()?;
                    // The request journal persists this exact root-bound
                    // attempt. An uncommitted candidate owns no operation key.
                    self.commit_with_disruption(
                        snapshot,
                        &mutation,
                        None,
                        resources::Authority::ManagedTransition,
                        || Ok(()),
                    )
                    .await?;
                }
                self.inner
                    .domain
                    .run(Box::pin(directory.plan_managed_transition(plan, &permit)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)
            }),
        )
        .await
    }

    /// The supplied archive tail pool must admit a pinned cut per parent.
    /// Independently replay the planned active parent holds and sync all
    /// non-serving child partitions. The directory selects the resulting
    /// immutable preparation under the same CAS as pre-seal cancellation.
    /// A replacement controller replays the exact recorded source holds.
    pub async fn prepare_managed_transition(
        &self,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        inputs: CellGroupTransitionPreparationInputs,
        timeout: Duration,
    ) -> Result<PreparedManagedCellGroupTransition> {
        validate_request(request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let permit = self
            .run(
                deadline.remaining()?,
                Box::pin(self.active_managed_transition_permit(request, directory, plan)),
            )
            .await?;
        // The catalog turn ends before replay/copy. The directory's final CAS
        // still arbitrates cancellation, including release by another controller.
        let remaining = deadline.remaining()?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(async {
                crate::asyncrt::timeout(
                    remaining,
                    Box::pin(
                        directory.prepare_managed_transition_data(domain, plan, &permit, inputs),
                    ),
                )
                .await
            }))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Verify and select the exact committed parent cuts of a managed transition.
    /// Supply an independently configured receiver per held archive in plan
    /// order. Retain every hold through retirement and retry with the same
    /// descriptors after an unknown directory outcome.
    pub async fn verify_managed_sealed_sources(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        sources: Vec<CellGroupTransitionSource>,
        timeout: Duration,
    ) -> Result<VerifiedCellGroupTransitionSources> {
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
        let remaining = deadline.remaining()?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(async {
                crate::asyncrt::timeout(
                    remaining,
                    Box::pin(directory.verify_and_record_managed_sealed_sources(
                        domain,
                        archives,
                        tails,
                        &transition.plan,
                        sources,
                        &permit,
                    )),
                )
                .await
            }))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Independently rebuild all installed child roots from the retained
    /// parent archives and select their durable transfer closure. The
    /// directory keeps the parent ranges selected after verification.
    pub async fn verify_managed_transfer_archive(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        inputs: CellGroupTransitionPreparationInputs,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let permit = self
            .run(
                deadline.remaining()?,
                Box::pin(self.verified_managed_transfer_permit(transition, directory)),
            )
            .await?;
        // Replay and partition work use the caller's original deadline. The
        // directory CAS binds the resulting closure to the installed set.
        let remaining = deadline.remaining()?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                remaining,
                Box::pin(directory.verify_and_record_managed_transfer_archive(
                    domain,
                    &transition.plan,
                    inputs,
                    &permit,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Reopen the archive closure selected for the exact active managed plan.
    /// This reads the durable references and their install bindings.
    pub async fn reopen_managed_transfer_archive(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<Option<CellGroupTransferArchive>> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                let permit = self
                    .managed_transition_permit_at(
                        &current,
                        &transition.request,
                        directory,
                        &transition.plan,
                    )
                    .await?;
                let closure = self
                    .inner
                    .domain
                    .run(Box::pin(
                        directory.managed_transfer_archive(&transition.plan, &permit),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                if closure.is_some() {
                    self.verify_managed_transfer_storage_at(
                        &current, transition, directory, &permit,
                    )
                    .await?;
                }
                if self.inner.current().await?.token != current.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(closure)
            }),
        )
        .await
    }

    /// Select the archived children in one directory decision after rechecking
    /// their reserved storage and all source holds. Replay uses the exact plan
    /// and returns the selected publication even after a lost CAS response.
    /// Publication grants no application admission to a pending child.
    pub async fn publish_managed_transfer(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<DirectoryPublication> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .verified_managed_transfer_permit(transition, directory)
                    .await?;
                let domain = &self.inner.domain;
                domain
                    .run(Box::pin(directory.publish_managed_transfer(
                        domain,
                        archives,
                        &transition.plan,
                        &permit,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(|error| match error {
                        CellGroupArchiveBindingError::Catalog(error) => catalog_error(error),
                        CellGroupArchiveBindingError::Archive(error) => error.into(),
                    })
            }),
        )
        .await
    }

    /// Select a complete ordered set of durable activation acknowledgements.
    /// The operation continues owning the topology lane for parent retirement.
    pub async fn record_managed_transfer_activations(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        acknowledgements: Vec<CellGroupTransferActivationAck>,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .verified_managed_transfer_permit(transition, directory)
                    .await?;
                self.inner
                    .domain
                    .run(Box::pin(directory.record_managed_transfer_activations(
                        &transition.plan,
                        acknowledgements,
                        &permit,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)
            }),
        )
        .await
    }

    pub(crate) async fn resume_managed_transfer_activation(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        history: GroupHistory,
        acknowledgement: Option<CellGroupTransferActivationAck>,
        timeout: Duration,
    ) -> Result<Option<(LogPosition, TransitionJournal)>> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .verified_managed_transfer_permit(transition, directory)
                    .await?;
                self.inner
                    .domain
                    .run(Box::pin(directory.resume_managed_transfer_activation(
                        &transition.plan,
                        history,
                        acknowledgement,
                        &permit,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)
            }),
        )
        .await
    }

    pub(crate) async fn initialize_managed_transfer_child_archive(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        history: GroupHistory,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<()> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let permit = self
            .run(
                deadline.remaining()?,
                Box::pin(self.verified_managed_transfer_permit(transition, directory)),
            )
            .await?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.initialize_managed_child_archive(
                    domain,
                    archives,
                    &transition.plan,
                    history,
                    &permit,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Prepare a one-use archive identity for a child of the active transfer.
    /// Persist the exact intent before committing it. Reopen or verify that
    /// binding after an uncertain outcome.
    pub async fn prepare_managed_transfer_child_archive(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        history: GroupHistory,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<RaftGroupArchiveInitialization> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .verified_managed_transfer_permit(transition, directory)
                    .await?;
                let domain = &self.inner.domain;
                domain
                    .run(Box::pin(async {
                        let (birth, coverage_selected) = directory
                            .managed_transfer_child_archive_birth(
                                &transition.plan,
                                history,
                                &permit,
                            )
                            .await
                            .map_err(catalog_error)?;
                        if coverage_selected
                            || directory
                                .has_group_archive_binding(&birth)
                                .await
                                .map_err(transfer_archive_error)?
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                        let prepared = directory
                            .prepare_group_archive_binding(domain, archives, &birth)
                            .await
                            .map_err(transfer_archive_error)?;
                        Ok(RaftGroupArchiveInitialization { prepared })
                    }))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
            }),
        )
        .await
    }

    /// Verify the exact persisted child archive binding after an uncertain
    /// setup reply. An incomplete registry refuses without creating state.
    pub async fn verify_managed_transfer_child_archive(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        history: GroupHistory,
        archives: &CellGroupArchivePool,
        intent: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .verified_managed_transfer_permit(transition, directory)
                    .await?;
                let domain = &self.inner.domain;
                domain
                    .run(Box::pin(async {
                        let (birth, _) = directory
                            .managed_transfer_child_archive_birth(
                                &transition.plan,
                                history,
                                &permit,
                            )
                            .await
                            .map_err(catalog_error)?;
                        directory
                            .verify_group_archive_binding(domain, archives, &birth, intent)
                            .await
                            .map_err(transfer_archive_error)
                    }))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
            }),
        )
        .await
    }

    /// Acquire the plan-owned child holds and replay each one into a private
    /// receiver. A retry resumes the exact acquires selected before replay.
    pub async fn verify_managed_transfer_child_archives(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        verifier: &FleetGroupArchiveVerifier,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let permit = self
            .run(
                deadline.remaining()?,
                Box::pin(self.verified_managed_transfer_permit(transition, directory)),
            )
            .await?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(async {
                    let archives = verifier.archives();
                    let held = directory
                        .acquire_managed_child_archives(domain, archives, &transition.plan, &permit)
                        .await
                        .map_err(transfer_archive_error)?;
                    let storage = directory.namespace_config().image_storage;
                    let mut children = Vec::with_capacity(held.len());
                    for (birth, hold) in held {
                        let receiver = verifier.receiver(domain, birth.route(), storage).await?;
                        children.push(CellGroupTransitionSource {
                            birth,
                            hold,
                            receiver,
                        });
                    }
                    directory
                        .verify_and_record_managed_child_archives(
                            domain,
                            archives,
                            verifier.tails(),
                            &transition.plan,
                            children,
                            &permit,
                        )
                        .await
                        .map_err(transfer_archive_error)
                }),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
    }

    /// Reopen the selected references for retirement reconciliation.
    pub async fn reopen_managed_transfer_child_holds(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<Option<Vec<CellGroupArchiveHold>>> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .verified_managed_transfer_permit(transition, directory)
                    .await?;
                self.inner
                    .domain
                    .run(Box::pin(
                        directory.managed_retirement_child_holds(&transition.plan, &permit),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)
            }),
        )
        .await
    }

    pub(crate) async fn resume_managed_source_retirement(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        history: GroupHistory,
        acknowledgement: Option<CellGroupSourceRetirementAck>,
        timeout: Duration,
    ) -> Result<Option<(LogPosition, TransitionJournal)>> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let permit = self
            .run(
                deadline.remaining()?,
                Box::pin(self.verified_managed_transfer_permit(transition, directory)),
            )
            .await?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.resume_managed_source_retirement(
                    domain,
                    archives,
                    &transition.plan,
                    history,
                    acknowledgement,
                    &permit,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Root the exact committed parent tombstones after verifying that every
    /// selected child archive hold is still live. Both topology and the parent
    /// archive holds remain owned pending cleanup.
    pub async fn settle_managed_transfer_retirement(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        acknowledgements: Vec<CellGroupSourceRetirementAck>,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let permit = self
            .run(
                deadline.remaining()?,
                Box::pin(self.verified_managed_transfer_permit(transition, directory)),
            )
            .await?;
        let domain = &self.inner.domain;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.settle_managed_transfer_retirement(
                    domain,
                    archives,
                    &transition.plan,
                    acknowledgements,
                    &permit,
                )),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(transfer_archive_error)
    }

    /// Promote every child of the retired transfer in one fleet CAS. The
    /// exact request can be retried after a lost response. The disruption
    /// slot and all parent capacity references remain owned.
    pub async fn confirm_managed_transfer_children(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        validate_request(&transition.request)?;
        if request.operation != transition.request.operation
            || request.id == transition.request.id
            || request.principal != transition.request.principal
            || request.expected_revision <= transition.request.expected_revision
        {
            return Err(FleetCatalogError::Invalid(
                "managed transfer child confirmation",
            ));
        }
        let (_, children) = directory
            .transition_routes(&transition.plan)
            .map_err(catalog_error)?;
        self.run(
            timeout,
            Box::pin(async {
                let mut all_reserved = true;
                let mut changes = Vec::with_capacity(children.len());
                for route in &children {
                    let (key, mut history) = self
                        .transfer_child_history(snapshot, transition, directory, route)
                        .await?;
                    all_reserved &= history.state == FleetHistoryState::Reserved;
                    history.state = FleetHistoryState::Live;
                    changes.push(FleetChange {
                        table: FleetTable::Histories,
                        key,
                        value: Some(history.encode()?),
                    });
                }
                let mutation = history_mutation(request, changes);
                mutation.validate()?;
                let current = self.inner.current().await?;
                let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
                let key = hex::encode(request.id);
                if let Some(applied_revision) =
                    self.inner.committed(&current.head, &key, digest).await?
                {
                    return Ok(FleetCommit {
                        snapshot: current,
                        applied_revision,
                    });
                }
                if !all_reserved
                    || current.token != snapshot.token
                    || request.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let permit = self
                    .managed_transition_permit_at(
                        &current,
                        &transition.request,
                        directory,
                        &transition.plan,
                    )
                    .await?;
                self.verify_managed_transfer_storage_at(&current, transition, directory, &permit)
                    .await?;
                let domain = &self.inner.domain;
                domain
                    .run(Box::pin(
                        directory.require_managed_transfer_retirement_coverage(
                            domain,
                            archives,
                            &transition.plan,
                            &permit,
                        ),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(transfer_archive_error)?;
                self.commit_with_disruption(
                    snapshot,
                    &mutation,
                    None,
                    resources::Authority::ManagedTransferChildren,
                    || Ok(()),
                )
                .await
            }),
        )
        .await
    }

    /// Release the prepared and sealed parent holds after authenticating every
    /// parked replica named by the final parent configurations. The fleet
    /// disruption slot and physical capacity references stay owned.
    pub async fn release_managed_transfer_parent_holds(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        self.release_managed_transfer_parent_holds_at(
            None, transition, directory, archives, observer, timeout,
        )
        .await
    }

    async fn release_managed_transfer_parent_holds_at(
        &self,
        expected: Option<&FleetSnapshot>,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let (_, children) = directory
            .transition_routes(&transition.plan)
            .map_err(catalog_error)?;
        let (current, permit) = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let current = self.inner.current().await?;
                    if expected.is_some_and(|expected| expected.token != current.token) {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let permit = self
                        .managed_transition_permit_at(
                            &current,
                            &transition.request,
                            directory,
                            &transition.plan,
                        )
                        .await?;
                    for route in &children {
                        let (_, child) = self
                            .transfer_child_history(&current, transition, directory, route)
                            .await?;
                        if child.state != FleetHistoryState::Live {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    Ok((current, permit))
                }),
            )
            .await?;
        let domain = &self.inner.domain;
        let (journal, parents) = domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.managed_retired_sources(&transition.plan, &permit)),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(catalog_error)?;
        if journal.parent_holds_released() {
            return Ok(journal);
        }
        // One authenticated physical cut per node covers all its parent
        // slots. Drop each bounded inventory before sampling the next node.
        let mut placements = BTreeMap::new();
        for parent in &parents {
            for member in &parent.members {
                let node = NodeId::new(member.node)
                    .map_err(|_| FleetCatalogError::Corrupt("retired source node"))?;
                placements
                    .entry(node)
                    .or_insert_with(Vec::new)
                    .push((&parent.route, member));
            }
        }
        for (node, expected) in placements {
            let boot = self
                .run(deadline.remaining()?, self.host_from(&current, node))
                .await?
                .boot_binding()?;
            let sample = observer
                .inspect_peer_physical_inventory(node, current.revision(), deadline.remaining()?)
                .await
                .map_err(FleetCatalogError::Observation)?;
            if sample.view.fleet_boot.as_ref() != Some(&boot)
                || sample.view.node.identity.node != node
                || sample.view.node.identity.disk.bytes() != boot.disk
                || sample.view.node.identity.process.bytes() != boot.process
                || sample.view.node.storage_instance.bytes() != boot.engine
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let inventory = sample
                .inventory
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            for (route, member) in expected {
                let row = inventory
                    .group_history(&route.history)
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                if member.disk.is_some_and(|disk| disk != boot.disk)
                    || row.birth_digest() != route.birth_digest
                    || row.member().get() != member.member
                    || !row.parked_retired_source()
                {
                    return Err(FleetCatalogError::Unconfirmed);
                }
            }
        }
        self.run(deadline.remaining()?, async {
            if self.inner.current().await?.token != current.token {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(())
        })
        .await?;
        domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.release_managed_retired_parent_holds(
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

    /// Retire every historical parent and release the disruption slot in one
    /// fleet CAS after directory cleanup. The permanent history rows continue
    /// to identify occupied engine slots. Reconcile with the exact request.
    pub async fn complete_managed_transfer(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.complete_managed_transfer_with_parent_release(
            snapshot, request, transition, directory, None, timeout,
        )
        .await
    }

    pub(crate) async fn complete_managed_transfer_with_parent_release(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        parent_release: Option<(&CellGroupArchivePool, &RaftCells)>,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        validate_request(&transition.request)?;
        if request.operation != transition.request.operation
            || request.id == transition.request.id
            || request.principal != transition.request.principal
            || request.expected_revision <= transition.request.expected_revision
        {
            return Err(FleetCatalogError::Invalid("managed transfer completion"));
        }
        let (parents, children) = directory
            .transition_routes(&transition.plan)
            .map_err(catalog_error)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let prepare = Box::pin(async {
            let current = self.inner.current().await?;
            let record = self
                .managed_transition_record(&current, request.operation)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            if record.request != transition.request {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let permit = ManagedTransitionPermit(record);
            permit
                .check(directory, &transition.plan)
                .map_err(catalog_error)?;
            let (journal, _) = self
                .inner
                .domain
                .run(Box::pin(
                    directory.managed_retired_sources(&transition.plan, &permit),
                ))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?;
            let retirement = journal
                .retired_parents()
                .ok_or(FleetCatalogError::Conflict)?;
            let release_holds = !journal.parent_holds_released();
            if release_holds && parent_release.is_none() {
                return Err(FleetCatalogError::Conflict);
            }
            let retired = FleetHistoryState::Retired {
                operation: request.operation,
                retirement,
            };
            let mut changes = Vec::with_capacity(parents.len() + 1);
            let mut all_live = true;
            for route in &parents {
                let binding = self.binding(&route.history)?;
                let key = binding
                    .key()
                    .map_err(|_| FleetCatalogError::Corrupt("retired source binding"))?;
                let mut history = self.history_inner(snapshot, &key).await?;
                if history.binding != binding
                    || history.origin != Origin::Managed
                    || !history.check_route(directory, route)
                    || (history.state != FleetHistoryState::Live && history.state != retired)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                all_live &= history.state == FleetHistoryState::Live;
                history.state = retired.clone();
                changes.push(FleetChange {
                    table: FleetTable::Histories,
                    key,
                    value: Some(history.encode()?),
                });
            }
            changes.push(FleetChange {
                table: FleetTable::Policy,
                key: SLOT_KEY.into(),
                value: None,
            });
            let mutation = history_mutation(request, changes);
            mutation.validate()?;
            let bytes = encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?;
            let key = hex::encode(request.id);
            if let Some(applied_revision) = self
                .inner
                .committed(&current.head, &key, hash(&bytes))
                .await?
            {
                return Ok(TransferCompletion::Replayed(Box::new(FleetCommit {
                    snapshot: current,
                    applied_revision,
                })));
            }
            if !all_live
                || current.token != snapshot.token
                || request.expected_revision != snapshot.revision()
            {
                return Err(FleetCatalogError::Conflict);
            }
            let Some(Disruption::Transition(active)) = self.active_disruption(&current).await?
            else {
                return Err(FleetCatalogError::Conflict);
            };
            if active != permit.0 {
                return Err(FleetCatalogError::Conflict);
            }
            for route in &children {
                let (_, child) = self
                    .transfer_child_history(&current, transition, directory, route)
                    .await?;
                if child.state != FleetHistoryState::Live {
                    return Err(FleetCatalogError::Conflict);
                }
            }
            let disruption = current.head.disruption.ok_or(FleetCatalogError::Conflict)?;
            if release_holds {
                // Claim the exact completion payload before releasing any
                // remote hold. An interrupted call must use this same ID.
                self.inner
                    .create_exact(&format!("requests/{key}"), &bytes, REQUEST_LIMIT)
                    .await?;
            }
            Ok(TransferCompletion::Pending {
                mutation,
                disruption,
                release_holds,
            })
        });
        let completion = self.run(deadline.remaining()?, prepare).await?;
        let (mutation, disruption, release_holds) = match completion {
            TransferCompletion::Replayed(commit) => return Ok(*commit),
            TransferCompletion::Pending {
                mutation,
                disruption,
                release_holds,
            } => (mutation, disruption, release_holds),
        };
        if release_holds {
            let (archives, observer) = parent_release.ok_or(FleetCatalogError::Conflict)?;
            let journal = self
                .release_managed_transfer_parent_holds_at(
                    Some(snapshot),
                    transition,
                    directory,
                    archives,
                    observer,
                    deadline.remaining()?,
                )
                .await?;
            if !journal.parent_holds_released() {
                return Err(FleetCatalogError::Unconfirmed);
            }
        }
        self.run(
            deadline.remaining()?,
            Box::pin(self.commit_with_disruption(
                snapshot,
                &mutation,
                Some(disruption),
                resources::Authority::ManagedTransferCompletion,
                || Ok(()),
            )),
        )
        .await
    }

    pub(super) async fn require_retired_history(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        route: &GroupRoute,
        operation: [u8; 16],
        retirement: [u8; 32],
        known_nodes: &[super::nodes::NodeRecord],
    ) -> Result<()> {
        let record = self
            .managed_transition_record(snapshot, operation)
            .await?
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let permit = ManagedTransitionPermit(record);
        permit.check_directory(directory).map_err(catalog_error)?;
        let operation = ProposalId::new(operation)
            .map_err(|_| FleetCatalogError::Corrupt("retired source operation"))?;
        let (plan, _) = directory
            .reopen_managed_transition_plan(operation, permit.0.plan, &permit)
            .await
            .map_err(catalog_error)?;
        let (journal, parents) = directory
            .managed_retired_sources(&plan, &permit)
            .await
            .map_err(catalog_error)?;
        if !journal.parent_holds_released()
            || journal.retired_parents() != Some(retirement)
            || !parents.iter().any(|source| {
                &source.route == route
                    && source
                        .members
                        .iter()
                        .all(|member| known_nodes.iter().any(|node| node.node == member.node))
            })
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        Ok(())
    }

    async fn verified_managed_transfer_permit(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
    ) -> Result<ManagedTransitionPermit> {
        let current = self.inner.current().await?;
        let permit = self
            .managed_transition_permit_at(
                &current,
                &transition.request,
                directory,
                &transition.plan,
            )
            .await?;
        self.verify_managed_transfer_storage_at(&current, transition, directory, &permit)
            .await?;
        if self.inner.current().await?.token != current.token {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(permit)
    }

    async fn verify_managed_transfer_storage_at(
        &self,
        current: &FleetSnapshot,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        permit: &ManagedTransitionPermit,
    ) -> Result<()> {
        let (_, children) = directory
            .transition_routes(&transition.plan)
            .map_err(catalog_error)?;
        for route in &children {
            self.managed_transfer_install_quorum_at(current, transition, directory, route, permit)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
        }
        Ok(())
    }

    pub(super) async fn active_managed_transition_permit(
        &self,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
    ) -> Result<ManagedTransitionPermit> {
        let current = self.inner.current().await?;
        self.managed_transition_permit_at(&current, request, directory, plan)
            .await
    }

    async fn managed_transition_permit_at(
        &self,
        current: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
    ) -> Result<ManagedTransitionPermit> {
        let Some(Disruption::Transition(active)) = self.active_disruption(current).await? else {
            return Err(FleetCatalogError::Conflict);
        };
        if active.request != *request {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let permit = ManagedTransitionPermit(active);
        permit.check(directory, plan).map_err(catalog_error)?;
        Ok(permit)
    }

    /// Select the irreversible seal decision for a physically prepared plan.
    /// The exact admitted request is retryable. Cancellation competes on the
    /// same directory head. This decision alone grants no replica authority.
    pub async fn authorize_managed_transition_seal(
        &self,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(request)?;
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .active_managed_transition_permit(request, directory, plan)
                    .await?;
                self.inner
                    .domain
                    .run(Box::pin(
                        directory.authorize_managed_transition_seal(plan, &permit),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)
            }),
        )
        .await
    }

    /// Cancel the exact pre-seal plan and only then release its fleet slot.
    /// Child identities and their existing capacity debits remain inventoried.
    pub async fn cancel_managed_transition(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                let record = self
                    .managed_transition_record(&current, request.operation)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if request.id == record.request.id
                    || request.principal != record.request.principal
                    || request.expected_revision <= record.request.expected_revision
                {
                    return Err(FleetCatalogError::Invalid(
                        "managed transition cancellation",
                    ));
                }
                let permit = ManagedTransitionPermit(record);
                permit.check(directory, plan).map_err(catalog_error)?;
                let mutation = history_mutation(
                    request,
                    vec![FleetChange {
                        table: FleetTable::Policy,
                        key: SLOT_KEY.into(),
                        value: None,
                    }],
                );
                mutation.validate()?;
                let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
                let key = hex::encode(request.id);
                if let Some(applied_revision) =
                    self.inner.committed(&current.head, &key, digest).await?
                {
                    return Ok(FleetCommit {
                        snapshot: current,
                        applied_revision,
                    });
                }
                if current.token != snapshot.token
                    || request.expected_revision != snapshot.revision()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let Some(Disruption::Transition(active)) = self.active_disruption(snapshot).await?
                else {
                    return Err(FleetCatalogError::Conflict);
                };
                if active != permit.0 {
                    return Err(FleetCatalogError::Conflict);
                }
                let release = snapshot
                    .head
                    .disruption
                    .ok_or(FleetCatalogError::Conflict)?;
                self.inner
                    .domain
                    .run(Box::pin(directory.cancel_managed_transition(plan, &permit)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                self.commit_with_disruption(
                    snapshot,
                    &mutation,
                    Some(release),
                    resources::Authority::ManagedTransition,
                    || Ok(()),
                )
                .await
            }),
        )
        .await
    }

    async fn managed_transition_record(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<Option<Record>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &operation_key(operation),
            )
            .await?
        else {
            return Ok(None);
        };
        let record: Record = decode(KIND, &bytes, RECORD_LIMIT)?;
        record.validate(self)?;
        if record.operation() != operation {
            return Err(FleetCatalogError::Corrupt("managed transition index"));
        }
        self.confirm_managed_transition(snapshot, &record).await?;
        Ok(Some(record))
    }

    pub(super) async fn confirm_managed_transition(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
    ) -> Result<()> {
        let digest = hash(&encode(REQUEST_KIND, &record.mutation()?, REQUEST_LIMIT)?);
        if self
            .inner
            .committed(&snapshot.head, &hex::encode(record.request.id), digest)
            .await?
            .is_none()
        {
            return Err(FleetCatalogError::Corrupt("managed transition journal"));
        }
        Ok(())
    }

    pub(super) async fn check_managed_transition_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        authority: resources::Authority,
    ) -> Result<()> {
        for change in mutation
            .changes
            .iter()
            .filter(|change| change.table == FleetTable::Histories)
        {
            if authority != resources::Authority::ManagedTransferCompletion
                && change
                    .value
                    .as_deref()
                    .and_then(|bytes| FleetHistory::decode(bytes).ok())
                    .is_some_and(|history| {
                        matches!(history.state, FleetHistoryState::Retired { .. })
                    })
            {
                return Err(FleetCatalogError::Invalid(
                    "managed history retirement authority",
                ));
            }
            if self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Histories.slot()],
                    &change.key,
                )
                .await?
                .as_deref()
                .and_then(|bytes| FleetHistory::decode(bytes).ok())
                .is_some_and(|history| matches!(history.state, FleetHistoryState::Retired { .. }))
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        if authority != resources::Authority::ManagedTransition
            && mutation.changes.iter().any(|change| {
                change.table == FleetTable::Operations && change.key.starts_with(OPERATION_PREFIX)
            })
        {
            return Err(FleetCatalogError::Invalid(
                "managed transition operation authority",
            ));
        }
        if snapshot.head.disruption.is_some()
            && mutation.changes.iter().any(|change| match change.table {
                FleetTable::Histories => !matches!(
                    authority,
                    resources::Authority::ManagedTransferChildren
                        | resources::Authority::ManagedTransferCompletion
                ),
                FleetTable::Admissions => true,
                FleetTable::Policy => {
                    !matches!(
                        authority,
                        resources::Authority::ManagedTransition
                            | resources::Authority::ManagedTransferCompletion
                    ) && change.key == SLOT_KEY
                }
                _ => false,
            })
        {
            if let Some(Disruption::Transition(record)) = self.active_disruption(snapshot).await? {
                return Err(FleetCatalogError::DisruptionInProgress {
                    operation: record.operation(),
                });
            }
        }
        Ok(())
    }
}

pub(crate) struct ManagedTransferInstallAuthority {
    pub(crate) transition: ManagedTransitionPermit,
    pub(crate) group: FleetGroupBinding,
    pub(crate) boot: BootBinding,
}

impl FleetCatalog {
    async fn reserved_transfer_child(
        &self,
        current: &FleetSnapshot,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        route: &GroupRoute,
    ) -> Result<FleetHistory> {
        let (_, history) = self
            .transfer_child_history(current, transition, directory, route)
            .await?;
        if history.state != FleetHistoryState::Reserved {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(history)
    }

    async fn transfer_child_history(
        &self,
        current: &FleetSnapshot,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        route: &GroupRoute,
    ) -> Result<(String, FleetHistory)> {
        let key = self
            .binding(&route.history)?
            .key()
            .map_err(|_| FleetCatalogError::Invalid("transfer child binding"))?;
        let history = self.history_inner(current, &key).await?;
        if history.origin != Origin::Managed
            || !matches!(
                history.state,
                FleetHistoryState::Reserved | FleetHistoryState::Live
            )
            || history.operation != transition.request.operation
            || !history.check_route(directory, route)
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok((key, history))
    }

    /// Observe a majority of physically installed voters of the exact reserved
    /// managed child. Receipt storage identities must match permanent fleet
    /// claims. An installation receipt grants no live boot or quorum authority.
    pub async fn observe_managed_transfer_install_quorum(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        route: &GroupRoute,
        timeout: Duration,
    ) -> Result<Option<TransferInstallQuorum>> {
        validate_request(&transition.request)?;
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                let permit = self
                    .managed_transition_permit_at(
                        &current,
                        &transition.request,
                        directory,
                        &transition.plan,
                    )
                    .await?;
                let proof = self
                    .managed_transfer_install_quorum_at(
                        &current, transition, directory, route, &permit,
                    )
                    .await?;
                if self.inner.current().await?.token != current.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(proof)
            }),
        )
        .await
    }

    async fn managed_transfer_install_quorum_at(
        &self,
        current: &FleetSnapshot,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        route: &GroupRoute,
        permit: &ManagedTransitionPermit,
    ) -> Result<Option<TransferInstallQuorum>> {
        let child = self
            .reserved_transfer_child(current, transition, directory, route)
            .await?;
        let proof = self
            .inner
            .domain
            .run(Box::pin(directory.observe_managed_transfer_install_quorum(
                &transition.plan,
                permit,
                route.history,
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(catalog_error)?;
        if let Some(proof) = &proof {
            for voter in &proof.voters {
                let host = self.host_from(current, voter.node).await?;
                if host.disk() != voter.disk
                    || host.storage_instance() != Some(voter.engine)
                    || !child.voters.iter().any(|member| {
                        member.node == voter.node.bytes() && member.member == voter.member.get()
                    })
                {
                    return Err(FleetCatalogError::Conflict);
                }
            }
        }
        Ok(proof)
    }

    pub(crate) async fn resume_managed_transfer_child_quorum(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        history: GroupHistory,
        proof: Option<TransferChildQuorum>,
        timeout: Duration,
    ) -> Result<Option<(LogPosition, TransitionJournal)>> {
        validate_request(&transition.request)?;
        let (_, children) = directory
            .transition_routes(&transition.plan)
            .map_err(catalog_error)?;
        let route = children
            .iter()
            .find(|route| route.history == history)
            .ok_or(FleetCatalogError::Conflict)?;
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                let permit = self
                    .managed_transition_permit_at(
                        &current,
                        &transition.request,
                        directory,
                        &transition.plan,
                    )
                    .await?;
                self.managed_transfer_install_quorum_at(
                    &current, transition, directory, route, &permit,
                )
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
                let all_current = match self
                    .verify_managed_transfer_storage_at(&current, transition, directory, &permit)
                    .await
                {
                    Ok(()) => true,
                    Err(FleetCatalogError::Unconfirmed) => false,
                    Err(error) => return Err(error),
                };
                if self.inner.current().await?.token != current.token {
                    return Err(FleetCatalogError::Conflict);
                }
                self.inner
                    .domain
                    .run(Box::pin(directory.resume_managed_transfer_child_quorum(
                        &transition.plan,
                        history,
                        proof,
                        &permit,
                        all_current,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)
            }),
        )
        .await
    }

    /// Select the exact ordered set of managed child fresh-term witnesses.
    /// The fleet reservation and physical storage claims must still match.
    /// The resulting Installed decision leaves routing and serving fenced.
    pub async fn record_managed_transfer_child_quorums(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        proofs: Vec<TransferChildQuorum>,
        timeout: Duration,
    ) -> Result<TransitionJournal> {
        validate_request(&transition.request)?;
        let (_, children) = directory
            .transition_routes(&transition.plan)
            .map_err(catalog_error)?;
        if proofs.len() != children.len() {
            return Err(FleetCatalogError::Invalid(
                "incomplete transfer child quorum set",
            ));
        }
        self.run(
            timeout,
            Box::pin(async {
                let permit = self
                    .verified_managed_transfer_permit(transition, directory)
                    .await?;
                self.inner
                    .domain
                    .run(Box::pin(directory.record_managed_transfer_child_quorums(
                        &transition.plan,
                        proofs,
                        &permit,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)
            }),
        )
        .await
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferVoterAdmission {
    Install,
    Retained,
}

impl FleetHostAuthorization {
    pub(crate) async fn managed_transfer_install_authority(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        route: &GroupRoute,
        timeout: Duration,
    ) -> Result<ManagedTransferInstallAuthority> {
        self.managed_transfer_voter_authority(
            transition,
            directory,
            route,
            TransferVoterAdmission::Install,
            timeout,
        )
        .await
    }

    pub(crate) async fn managed_transfer_voter_authority(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        route: &GroupRoute,
        admission: TransferVoterAdmission,
        timeout: Duration,
    ) -> Result<ManagedTransferInstallAuthority> {
        validate_request(&transition.request)?;
        let catalog = &self.catalog;
        catalog
            .run(
                timeout,
                Box::pin(async {
                    let current = catalog.inner.current().await?;
                    let host = catalog.host_from_record(&current, self.node()).await?;
                    self.require_running_successor(&host)?;
                    let boot = self.boot_binding()?;
                    let permit = catalog
                        .managed_transition_permit_at(
                            &current,
                            &transition.request,
                            directory,
                            &transition.plan,
                        )
                        .await?;
                    let history = catalog
                        .reserved_transfer_child(&current, transition, directory, route)
                        .await?;
                    if !history.voters.iter().any(|voter| voter.node == boot.node) {
                        return Err(FleetCatalogError::Conflict);
                    }
                    if admission == TransferVoterAdmission::Install {
                        let capacity = catalog.capacity_from(&current, self.node()).await?;
                        if !capacity.receive_replicas() || capacity.attestation().boot != boot {
                            return Err(FleetCatalogError::Conflict);
                        }
                    }
                    if catalog.inner.current().await?.token != current.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    Ok(ManagedTransferInstallAuthority {
                        transition: permit,
                        group: history.binding,
                        boot,
                    })
                }),
            )
            .await
    }

    pub(crate) async fn managed_source_authority(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        source: GroupHistory,
        timeout: Duration,
    ) -> Result<(ManagedTransitionPermit, FleetGroupAuthorization)> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(&self.catalog, timeout)?;
        let permit = self
            .catalog
            .run(
                deadline.remaining()?,
                Box::pin(self.catalog.active_managed_transition_permit(
                    &transition.request,
                    directory,
                    &transition.plan,
                )),
            )
            .await?;
        let group = self
            .group_authorization(&source, deadline.remaining()?)
            .await?;
        Ok((permit, group))
    }
}
