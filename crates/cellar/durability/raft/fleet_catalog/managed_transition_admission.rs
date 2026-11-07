// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persist the exact operator plan before reserving any child.

use super::{
    histories::{catalog_error, validate_request},
    *,
};
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{CellGroupBirthSpec, DirectoryMutation},
};
use cellar_logic::{cell_group::GroupRoute, consensus::ProposalId};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const ADMISSION: u8 = 213;
const ADMISSION_LIMIT: usize = 32 * 1024;

/// Stable audit request and step, with a distinct Fleet attempt for each
/// conditional input revision. A consumed revision cannot later commit.
pub(crate) struct TransitionOperatorStep {
    pub operation: ProposalId,
    pub principal: String,
    pub request_digest: [u8; 32],
    pub step: [u8; 16],
}

impl TransitionOperatorStep {
    pub(crate) fn identity(&self, revision: u64) -> FleetRequestIdentity {
        let mut digest = Sha256::new();
        digest.update(b"cellar-transition-operator-step-1");
        digest.update(self.request_digest);
        digest.update(self.step);
        digest.update(revision.to_be_bytes());
        let mut id = [0; 16];
        id.copy_from_slice(&digest.finalize()[..16]);
        FleetRequestIdentity {
            id,
            operation: self.operation.bytes(),
            principal: self.principal.clone(),
            expected_revision: revision,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OperatorPlan {
    catalog: [u8; 32],
    namespace: [u8; 32],
    request: [u8; 32],
    plan: Vec<u8>,
}

impl FleetCatalog {
    pub(crate) async fn prepare_transition_child_routes(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        class: &str,
        proposals: Vec<CellGroupBirthSpec>,
        timeout: Duration,
    ) -> Result<Vec<GroupRoute>> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            self.require_namespace_declaration(snapshot, directory.namespace(), Some(class))
                .await?;
            let owner = self.managed_owner_for_birth(snapshot, directory).await?;
            proposals
                .into_iter()
                .map(|proposal| {
                    Ok(directory
                        .prepare_fleet_group_birth(proposal, &owner)
                        .map_err(catalog_error)?
                        .route()
                        .clone())
                })
                .collect()
        })
        .await
    }

    pub(crate) async fn reopen_transition_operator_plan(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        request: [u8; 32],
        timeout: Duration,
    ) -> Result<Option<DirectoryMutation>> {
        self.run(timeout, async {
            let Some((record, _)) = self
                .inner
                .read::<OperatorPlan>(&operator_plan_key(operation), ADMISSION, ADMISSION_LIMIT)
                .await?
            else {
                return Ok(None);
            };
            if record.catalog != self.catalog_id()?
                || record.namespace != directory.namespace().root_digest()
                || record.request != request
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let plan = directory
                .decode_mutation(&record.plan)
                .map_err(catalog_error)?;
            if plan.operation() != operation {
                return Err(FleetCatalogError::Corrupt(
                    "operator transition plan operation",
                ));
            }
            Ok(Some(plan))
        })
        .await
    }

    /// Reopen a child reservation only when the indexed history and its
    /// committed receipt belong to this exact operator step.
    pub(crate) async fn reopen_transition_operator_child(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        route: &GroupRoute,
        step: &TransitionOperatorStep,
        timeout: Duration,
    ) -> Result<bool> {
        self.check_snapshot(snapshot)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let binding = self.binding(&route.history)?;
        let key = binding
            .key()
            .map_err(|_| FleetCatalogError::Invalid("transition child binding"))?;
        let found = self
            .run(deadline.remaining()?, async {
                let Some(bytes) = self
                    .inner
                    .lookup(snapshot.head.tables[FleetTable::Histories.slot()], &key)
                    .await?
                else {
                    return Ok(false);
                };
                let history = FleetHistory::decode(&bytes)?;
                if history.binding != binding
                    || history.operation != step.operation.bytes()
                    || history.state != FleetHistoryState::Reserved
                    || !history.check_route(directory, route)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let (mutation, _) = self
                    .inner
                    .read::<FleetMutation>(
                        &format!("requests/{}", hex::encode(history.admission)),
                        REQUEST_KIND,
                        REQUEST_LIMIT,
                    )
                    .await?
                    .ok_or(FleetCatalogError::Corrupt(
                        "missing transition child receipt",
                    ))?;
                let expected = step.identity(mutation.expected_revision);
                if history.admission != expected.id
                    || mutation.id != expected.id
                    || mutation.principal != expected.principal
                    || mutation.operation != expected.operation
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                Ok(true)
            })
            .await?;
        if !found {
            return Ok(false);
        }
        let authorization = self
            .recover_birth_reservation(snapshot, directory, &route.history, deadline.remaining()?)
            .await?;
        if authorization.route() != route {
            return Err(FleetCatalogError::Corrupt(
                "operator child reservation route",
            ));
        }
        Ok(true)
    }

    pub(crate) async fn stage_transition_operator_plan(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        identity: &FleetRequestIdentity,
        request: [u8; 32],
        plan: &DirectoryMutation,
        timeout: Duration,
    ) -> Result<()> {
        self.check_snapshot(snapshot)?;
        validate_request(identity)?;
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                if current.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                if let Some(active) = self.active_disruption(&current).await? {
                    return Err(FleetCatalogError::DisruptionInProgress {
                        operation: active.operation(),
                    });
                }
                if let Some(restart) = self.active_restart_operator_inner(&current).await? {
                    return Err(FleetCatalogError::DisruptionInProgress {
                        operation: restart.identity().operation,
                    });
                }
                self.require_transition_membership_idle(&current).await?;
                self.inner
                    .domain
                    .run(Box::pin(directory.validate_managed_transition_input(plan)))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                let record = OperatorPlan {
                    catalog: self.catalog_id()?,
                    namespace: directory.namespace().root_digest(),
                    request,
                    plan: plan.encode().to_vec(),
                };
                let bytes = encode(ADMISSION, &record, ADMISSION_LIMIT)?;
                self.inner
                    .create_exact(
                        &operator_plan_key(plan.operation()),
                        &bytes,
                        ADMISSION_LIMIT,
                    )
                    .await
            }),
        )
        .await
    }
}

fn operator_plan_key(operation: ProposalId) -> String {
    format!(
        "transition-operator-plans/{}",
        hex::encode(operation.bytes())
    )
}
