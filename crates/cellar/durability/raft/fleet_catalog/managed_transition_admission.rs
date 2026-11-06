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

const ADMISSION: u8 = 213;
const ADMISSION_LIMIT: usize = 32 * 1024;

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
