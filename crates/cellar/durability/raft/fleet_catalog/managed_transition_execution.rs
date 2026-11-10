// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit durable consent to automatically execute an admitted transition.
use super::{disruption::Disruption, histories::validate_request, *};
use crate::raft::{catalog::RangeDirectory, cell_groups::TransitionPhase};
use serde::Deserialize;

const KIND: u8 = 214;
const LIMIT: usize = 4096;

/// Advisory execution selection. It grants no membership, seal or serving proof;
/// each step must revalidate its current directory/Fleet/Raft owners.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedTransitionExecution {
    pub(crate) catalog: [u8; 32],
    pub(crate) namespace: [u8; 32],
    pub(crate) plan: [u8; 32],
    pub(crate) identity: FleetRequestIdentity,
}

fn key(operation: [u8; 16]) -> String {
    format!("transition-execution/{}", hex::encode(operation))
}

impl FleetCatalog {
    /// Reconcile immutable consent even after the disruption slot is released.
    /// A historical record grants no current slot, seal or serving authority.
    pub(crate) async fn transition_execution(
        &self,
        operation: [u8; 16],
        timeout: Duration,
    ) -> Result<Option<ManagedTransitionExecution>> {
        self.run(
            timeout,
            Box::pin(async {
                let Some((record, _)) = self
                    .inner
                    .read::<ManagedTransitionExecution>(&key(operation), KIND, LIMIT)
                    .await?
                else {
                    return Ok(None);
                };
                validate_request(&record.identity)?;
                if record.catalog != self.catalog_id()? || record.identity.operation != operation {
                    return Err(FleetCatalogError::Corrupt("transition execution consent"));
                }
                Ok(Some(record))
            }),
        )
        .await
    }

    pub(crate) async fn consent_transition_execution(
        &self,
        directory: &RangeDirectory,
        transition: &ReopenedManagedCellGroupTransition,
        identity: &FleetRequestIdentity,
        timeout: Duration,
    ) -> Result<()> {
        validate_request(identity)?;
        if identity.operation != transition.plan.operation().bytes()
            || identity.principal != transition.request.principal
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let record = ManagedTransitionExecution {
            catalog: self.catalog_id()?,
            namespace: directory.namespace().root_digest(),
            plan: transition.plan.digest(),
            identity: identity.clone(),
        };
        self.run(
            timeout,
            Box::pin(async {
                let record_key = key(identity.operation);
                if let Some((existing, _)) = self
                    .inner
                    .read::<ManagedTransitionExecution>(&record_key, KIND, LIMIT)
                    .await?
                {
                    return if existing == record {
                        Ok(())
                    } else {
                        Err(FleetCatalogError::PayloadConflict)
                    };
                }
                let current = self.inner.current().await?;
                if current.revision() != identity.expected_revision {
                    return Err(FleetCatalogError::Conflict);
                }
                self.managed_transition_permit_at(
                    &current,
                    &transition.request,
                    directory,
                    &transition.plan,
                )
                .await?;
                if transition
                    .journal
                    .as_ref()
                    .is_none_or(|journal| journal.phase() == TransitionPhase::Cancelled)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                // An immutable consent may race cancellation. It does not reopen a
                // cancelled operation: discovery and every action recheck the slot.
                self.inner
                    .create_exact(&record_key, &encode(KIND, &record, LIMIT)?, LIMIT)
                    .await
            }),
        )
        .await
    }

    /// One exact disruption-slot lookup, with no history or namespace scan.
    pub(crate) async fn active_transition_execution(
        &self,
        timeout: Duration,
    ) -> Result<Option<ManagedTransitionExecution>> {
        self.run(
            timeout,
            Box::pin(async {
                let current = self.inner.current().await?;
                let Some(Disruption::Transition(active)) = self.active_disruption(&current).await?
                else {
                    return Ok(None);
                };
                let Some((record, _)) = self
                    .inner
                    .read::<ManagedTransitionExecution>(&key(active.operation()), KIND, LIMIT)
                    .await?
                else {
                    return Ok(None);
                };
                validate_request(&record.identity)?;
                if record.catalog != self.catalog_id()?
                    || record.namespace != active.namespace_root
                    || record.plan != active.plan
                    || record.identity.operation != active.operation()
                    || record.identity.principal != active.request.principal
                {
                    return Err(FleetCatalogError::Corrupt("transition execution consent"));
                }
                Ok(Some(record))
            }),
        )
        .await
    }
}
