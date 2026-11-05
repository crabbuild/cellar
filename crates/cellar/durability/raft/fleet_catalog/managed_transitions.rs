// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted fleet ownership of a managed split or merge before any seal.
use super::disruption::{Disruption, SLOT_KEY};
use super::histories::{catalog_error, history_mutation, validate_request, Origin};
use super::*;
use crate::raft::{
    catalog::CatalogError,
    cell_groups::{
        CellGroupArchiveBindingError, CellGroupArchivePool, CellGroupArchiveTailPool,
        CellGroupTransferArchive, CellGroupTransitionPreparationInputs, CellGroupTransitionSource,
        DirectoryMutation, PreparedManagedCellGroupTransition, RangeDirectory, TransferChildQuorum,
        TransferInstallQuorum, TransitionJournal, VerifiedCellGroupTransitionSources,
    },
    fleet_stop::BootBinding,
    member_state::FleetGroupBinding,
};

use cellar_logic::{
    cell_group::{GroupHistory, GroupRoute},
    consensus::ProposalId,
};
use serde::Deserialize;

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

fn root_digest(snapshot: &FleetSnapshot) -> Result<[u8; 32]> {
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
                    && !journal.as_ref().is_some_and(|decision| {
                        decision.phase() == crate::raft::cell_groups::TransitionPhase::Cancelled
                    })
                {
                    return Err(FleetCatalogError::Corrupt(
                        "released managed transition has no cancellation",
                    ));
                }
                Ok(Some(ReopenedManagedCellGroupTransition {
                    request: record.request.clone(),
                    plan,
                    journal,
                }))
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
                        return status
                            .filter(|journal| {
                                journal.phase()
                                    == crate::raft::cell_groups::TransitionPhase::Cancelled
                            })
                            .ok_or(FleetCatalogError::Corrupt(
                                "missing managed transition cancellation",
                            ));
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
                    let bytes = encode(KIND, record, RECORD_LIMIT)?;
                    self.inner
                        .create_exact(&record.key(), &bytes, RECORD_LIMIT)
                        .await?;
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
            .map_err(|error| match error {
                CellGroupArchiveBindingError::Catalog(error) => catalog_error(error),
                CellGroupArchiveBindingError::Archive(error) => error.into(),
            })
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
            .map_err(|error| match error {
                CellGroupArchiveBindingError::Catalog(error) => catalog_error(error),
                CellGroupArchiveBindingError::Archive(error) => error.into(),
            })
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
                    self.verify_managed_transfer_storage_at(
                        &current, transition, directory, &permit,
                    )
                    .await?;
                    if self.inner.current().await?.token != current.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    Ok(permit)
                }),
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
            .map_err(|error| match error {
                CellGroupArchiveBindingError::Catalog(error) => catalog_error(error),
                CellGroupArchiveBindingError::Archive(error) => error.into(),
            })
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

    async fn active_managed_transition_permit(
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
            && mutation.changes.iter().any(|change| {
                matches!(change.table, FleetTable::Histories | FleetTable::Admissions)
                    || authority != resources::Authority::ManagedTransition
                        && change.table == FleetTable::Policy
                        && change.key == SLOT_KEY
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
        let key = self
            .binding(&route.history)?
            .key()
            .map_err(|_| FleetCatalogError::Invalid("transfer child binding"))?;
        let history = self.history_inner(current, &key).await?;
        if history.origin != Origin::Managed
            || history.state != FleetHistoryState::Reserved
            || history.operation != transition.request.operation
            || !history.check_route(directory, route)
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(history)
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

impl FleetHostAuthorization {
    pub(crate) async fn managed_transfer_install_authority(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        route: &GroupRoute,
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
                    let capacity = catalog.capacity_from(&current, self.node()).await?;
                    if !capacity.receive_replicas()
                        || capacity.attestation().boot != boot
                        || catalog.inner.current().await?.token != current.token
                    {
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

    pub(crate) async fn managed_source_seal_authority(
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
