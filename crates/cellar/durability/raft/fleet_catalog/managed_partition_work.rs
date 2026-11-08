// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One fleet-root-selected cursor, recoverable by another controller. The full
//! request is immutable; its index value stays inside normal mutation bounds.
use super::{histories::history_mutation, managed_transition_admission::TransitionOperatorStep, *};
use crate::raft::{catalog::RangeDirectory, cell_groups::TransitionPhase};
use cellar_logic::consensus::{NodeId, ProposalId};
use serde::Deserialize;

const BODY_KIND: u8 = 239;
const LOCATOR_KIND: u8 = 240;
const BODY_LIMIT: usize = 32 * 1024;
const PAYLOAD_LIMIT: usize = 16 * 1024;
pub(super) const PREFIX: &str = "partition-work/";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedPartitionWork {
    catalog: [u8; 32],
    origin: [u8; 16],
    pub(crate) namespace: [u8; 32],
    pub(crate) operation: [u8; 16],
    pub(crate) request: [u8; 32],
    pub(crate) plan: [u8; 32],
    pub(crate) principal: String,
    pub(crate) payload: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Locator {
    body: [u8; 32],
}

const ACTIVE_KEY: &str = "partition-work/active";

fn finished_key(operation: ProposalId) -> String {
    format!("{PREFIX}finished/{}", hex::encode(operation.bytes()))
}

fn body_key(digest: [u8; 32]) -> String {
    format!("partition-work-requests/{}", hex::encode(digest))
}

impl ManagedPartitionWork {
    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        if self.catalog != catalog.catalog_id()?
            || NodeId::new(self.origin).is_err()
            || [self.namespace, self.request, self.plan].contains(&[0; 32])
            || self.operation == [0; 16]
            || self.principal.is_empty()
            || self.principal.len() > 128
            || self.principal.chars().any(char::is_control)
            || self.payload.is_empty()
            || self.payload.len() > PAYLOAD_LIMIT
            || hash(self.payload.as_bytes()) != self.request
        {
            return Err(FleetCatalogError::Corrupt("partition work binding"));
        }
        Ok(())
    }

    // The first coordinator is provenance, not a process lease. A surviving
    // controller continues the selected request without rebinding its origin.
    fn matches_request(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.namespace == other.namespace
            && self.operation == other.operation
            && self.request == other.request
            && self.plan == other.plan
            && self.principal == other.principal
            && self.payload == other.payload
    }

    fn identity(&self, revision: u64, finish: bool) -> Result<FleetRequestIdentity> {
        let mut bytes = Vec::from(b"cellar-partition-work-step-1".as_slice());
        bytes.extend(self.origin);
        bytes.push(u8::from(finish));
        let mut step = [0; 16];
        step.copy_from_slice(&hash(&bytes)[..16]);
        Ok(TransitionOperatorStep {
            operation: ProposalId::new(self.operation)
                .map_err(|_| FleetCatalogError::Corrupt("partition operation"))?,
            principal: self.principal.clone(),
            request_digest: self.request,
            step,
        }
        .identity(revision))
    }
}

impl FleetCatalog {
    async fn partition_work_at(
        &self,
        snapshot: &FleetSnapshot,
    ) -> Result<Option<ManagedPartitionWork>> {
        self.partition_work_value(snapshot, ACTIVE_KEY).await
    }

    async fn partition_work_value(
        &self,
        snapshot: &FleetSnapshot,
        key: &str,
    ) -> Result<Option<ManagedPartitionWork>> {
        let Some(bytes) = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Operations.slot()], key)
            .await?
        else {
            return Ok(None);
        };
        let locator: Locator = decode(LOCATOR_KIND, &bytes, 1024)?;
        let (work, _) = self
            .inner
            .read::<ManagedPartitionWork>(&body_key(locator.body), BODY_KIND, BODY_LIMIT)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing partition work request"))?;
        if hash(&encode(BODY_KIND, &work, BODY_LIMIT)?) != locator.body {
            return Err(FleetCatalogError::Corrupt("partition work request digest"));
        }
        work.validate(self)?;
        Ok(Some(work))
    }

    /// One fleet point lookup, independent of the number of cells and histories.
    /// The locator is advisory; restoring it still requires its original plan.
    pub(crate) async fn pending_partition_work(
        &self,
        timeout: Duration,
    ) -> Result<Option<ManagedPartitionWork>> {
        self.run(timeout, async {
            let current = self.inner.current().await?;
            self.partition_work_at(&current).await
        })
        .await
    }

    /// Select the complete request before any child reservation. A cancelled
    /// or ambiguous CAS retains this exact request; it never grants seal consent.
    pub(crate) async fn stage_partition_work(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        origin: NodeId,
        step: &TransitionOperatorStep,
        payload: String,
        timeout: Duration,
    ) -> Result<()> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            let current = self.inner.current().await?;
            let staged = self
                .transition_operator_plan_at(directory, step.operation, step.request_digest)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let work = ManagedPartitionWork {
                catalog: self.catalog_id()?,
                origin: origin.bytes(),
                namespace: directory.namespace().root_digest(),
                operation: step.operation.bytes(),
                request: step.request_digest,
                plan: staged.digest(),
                principal: step.principal.clone(),
                payload,
            };
            work.validate(self)?;
            if let Some(finished) = self
                .partition_work_value(&current, &finished_key(step.operation))
                .await?
            {
                return if finished.matches_request(&work) {
                    Ok(())
                } else {
                    Err(FleetCatalogError::PayloadConflict)
                };
            }
            if let Some(previous) = self.partition_work_at(&current).await? {
                return if previous.matches_request(&work) {
                    Ok(())
                } else {
                    Err(FleetCatalogError::DisruptionInProgress {
                        operation: previous.operation,
                    })
                };
            }
            if current.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            // Coordinator identity is provenance, never a capacity proof.
            // Ordinary admission revalidates every destination boot.
            let host = self.host_from(&current, origin).await?;
            if !host.state().permits_admitted_movement() {
                return Err(FleetCatalogError::Conflict);
            }
            let body = encode(BODY_KIND, &work, BODY_LIMIT)?;
            let locator = Locator { body: hash(&body) };
            self.inner
                .create_exact(&body_key(locator.body), &body, BODY_LIMIT)
                .await?;
            let mutation = history_mutation(
                &work.identity(current.revision(), false)?,
                vec![FleetChange {
                    table: FleetTable::Operations,
                    key: ACTIVE_KEY.into(),
                    value: Some(encode(LOCATOR_KIND, &locator, 1024)?),
                }],
            );
            mutation.validate()?;
            self.commit_with_disruption(
                &current,
                &mutation,
                None,
                resources::Authority::PartitionWork,
                || Ok(()),
            )
            .await?;
            Ok(())
        })
        .await
    }

    /// Clearing the scheduler cursor does not release physical resources. The
    /// original transition must have a proved terminal journal and no Fleet slot.
    pub(crate) async fn finish_partition_work(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        request: [u8; 32],
        timeout: Duration,
    ) -> Result<bool> {
        self.run(timeout, async {
            let current = self.inner.current().await?;
            let Some(work) = self.partition_work_at(&current).await? else {
                let finished = self
                    .partition_work_value(&current, &finished_key(operation))
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                return if finished.operation == operation.bytes()
                    && finished.request == request
                    && finished.namespace == directory.namespace().root_digest()
                {
                    Ok(true)
                } else {
                    Err(FleetCatalogError::PayloadConflict)
                };
            };
            if work.operation != operation.bytes()
                || work.request != request
                || work.namespace != directory.namespace().root_digest()
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let observed = self
                .observe_managed_transition_at(&current, directory, operation)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            if observed.transition.plan.digest() != work.plan {
                return Err(FleetCatalogError::PayloadConflict);
            }
            if observed.fleet_slot_held
                || !observed.transition.journal.as_ref().is_some_and(|j| {
                    matches!(
                        j.phase(),
                        TransitionPhase::Retired | TransitionPhase::Cancelled
                    )
                })
            {
                return Ok(false);
            }
            let locator = Locator {
                body: hash(&encode(BODY_KIND, &work, BODY_LIMIT)?),
            };
            let mutation = history_mutation(
                &work.identity(current.revision(), true)?,
                vec![
                    FleetChange {
                        table: FleetTable::Operations,
                        key: ACTIVE_KEY.into(),
                        value: None,
                    },
                    FleetChange {
                        table: FleetTable::Operations,
                        key: finished_key(operation),
                        value: Some(encode(LOCATOR_KIND, &locator, 1024)?),
                    },
                ],
            );
            mutation.validate()?;
            self.commit_with_disruption(
                &current,
                &mutation,
                None,
                resources::Authority::PartitionWork,
                || Ok(()),
            )
            .await?;
            Ok(true)
        })
        .await
    }
}
