// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted fleet ownership of a managed split or merge before any seal.
use super::disruption::{Disruption, SLOT_KEY};
use super::histories::{catalog_error, history_mutation, validate_request, Origin};
use super::*;
use crate::raft::{
    catalog::CatalogError,
    cell_groups::{DirectoryMutation, RangeDirectory, TransitionJournal},
};

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
        let record = &self.0;
        if directory
            .namespace_config()
            .managed_fleet
            .as_ref()
            .map(|o| o.catalog)
            != Some(record.catalog)
            || directory.namespace().root_digest() != record.namespace_root
            || plan.operation().bytes() != record.operation()
            || plan.digest() != record.plan
        {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(())
    }
}

impl FleetCatalog {
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
                            directory.check_managed_transition_basis(plan, &permit),
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
