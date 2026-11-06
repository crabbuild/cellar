// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Promote the exact replacement histories while both recovery fences remain held.
use super::{
    histories::{catalog_error, history_mutation},
    managed_recovery_quorum::publication_input,
    *,
};
use crate::raft::catalog::{CatalogError, RangeDirectory};
use cellar_logic::consensus::ProposalId;
use serde::Deserialize;
use std::ops::Range;

pub(super) const PREFIX: &str = "managed-recovery-settlement/";
const KIND: u8 = 230;
const LIMIT: usize = 4096;

// The Fleet mutation also carries one protected cursor update.
const BATCH: usize = 15;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Facts {
    intent: [u8; 32],
    plan: [u8; 32],
    seeds: [u8; 32],
    publication: [u8; 32],
    activations: [u8; 32],
    total: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    facts: Facts,
    promoted: usize,
    request: FleetRequestIdentity,
}

/// Rooted progress through the selected recovery's replacement histories.
/// Serving still requires the recovery fence to be released.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetRecoverySettlement {
    pub promoted_ranges: usize,
    pub total_ranges: usize,
    pub applied_revision: u64,
    pub publication_digest: [u8; 32],
}

struct Selection<'a> {
    directory: &'a RangeDirectory,
    intent: &'a FleetTransitionRecoveryIntent,
    plan: &'a FleetTransitionRecoveryReplacement,
}

impl Record {
    fn identity(
        facts: &Facts,
        request: &FleetRecoveryAttachRequest,
        revision: u64,
        promoted: usize,
    ) -> Result<FleetRequestIdentity> {
        let step = u8::try_from(promoted).map_err(|_| FleetCatalogError::Limit)?;
        Ok(TransitionOperatorStep {
            operation: request.operation,
            principal: request.principal.clone(),
            request_digest: hash(&encode(KIND, facts, LIMIT)?),
            step: [step; 16],
        }
        .identity(revision))
    }

    fn check(
        &self,
        request: &FleetRecoveryAttachRequest,
        intent: &FleetTransitionRecoveryIntent,
        publication: [u8; 32],
    ) -> Result<()> {
        if self.facts.intent != intent.digest()
            || self.facts.plan != request.plan_digest
            || self.facts.seeds != request.seed_digest
            || self.facts.publication != publication
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        if !(1..=64).contains(&self.facts.total)
            || !(1..=self.facts.total).contains(&self.promoted)
            || (self.promoted != self.facts.total && !self.promoted.is_multiple_of(BATCH))
            || self.facts.activations == [0; 32]
            || self.request.expected_revision == 0
            || self.request
                != Self::identity(
                    &self.facts,
                    request,
                    self.request.expected_revision,
                    self.promoted,
                )?
        {
            return Err(FleetCatalogError::Corrupt(
                "managed recovery settlement cursor",
            ));
        }
        Ok(())
    }

    fn status(&self, applied_revision: u64) -> FleetRecoverySettlement {
        FleetRecoverySettlement {
            promoted_ranges: self.promoted,
            total_ranges: self.facts.total,
            applied_revision,
            publication_digest: self.facts.publication,
        }
    }

    fn last_batch(&self) -> Range<usize> {
        ((self.promoted - 1) / BATCH * BATCH)..self.promoted
    }
}

impl FleetCatalog {
    pub(crate) async fn advance_managed_recovery_settlement(
        &self,
        directory: &RangeDirectory,
        request: &FleetRecoveryAttachRequest,
        timeout: Duration,
    ) -> Result<FleetRecoverySettlement> {
        self.run(
            timeout,
            Box::pin(async {
                let (intent, plan, seeds) = self
                    .read_managed_recovery_publication_inputs(
                        directory,
                        request.operation,
                        &request.principal,
                        (request.plan_digest, request.seed_digest),
                    )
                    .await?;
                let snapshot = self.inner.current().await?;
                self.managed_recovery_permit_at(&snapshot, directory, intent.basis())
                    .await?;
                let input = publication_input(&intent, &plan, request.seed_digest)?;
                let (publication, activations) = self
                    .inner
                    .domain
                    .run(Box::pin(async {
                        let publication = directory
                            .observe_managed_recovery_publication(&input)
                            .await?
                            .ok_or(CatalogError::Fenced)?
                            .journal_digest();
                        let mut positions = Vec::with_capacity(plan.births().len());
                        for (birth, seed) in plan.births().iter().zip(&seeds.ranges) {
                            if birth.route().birth_digest != seed.birth_digest() {
                                return Err(CatalogError::Corrupt(
                                    "managed recovery settlement birth",
                                ));
                            }
                            let position = directory
                                .observe_managed_recovery_activation(
                                    birth.route(),
                                    request.plan_digest,
                                    publication,
                                    seed.genesis_commitment(),
                                )
                                .await?
                                .ok_or(CatalogError::Unconfirmed)?;
                            positions.push((position.index.get(), position.term.get()));
                        }
                        Ok::<_, CatalogError>((publication, positions))
                    }))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;

                let facts = Facts {
                    intent: intent.digest(),
                    plan: request.plan_digest,
                    seeds: request.seed_digest,
                    publication,
                    activations: hash(&encode(KIND, &activations, LIMIT)?),
                    total: plan.births().len(),
                };
                if !(1..=64).contains(&facts.total) {
                    return Err(FleetCatalogError::Limit);
                }
                let selection = Selection {
                    directory,
                    intent: &intent,
                    plan: &plan,
                };
                let mut start = 0;
                if let Some(record) = self
                    .managed_recovery_settlement_at(&snapshot, request.operation)
                    .await?
                {
                    record.check(request, &intent, publication)?;
                    if record.facts != facts {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    let previous = self
                        .recovery_settlement_mutation(
                            &snapshot,
                            &selection,
                            &record,
                            FleetHistoryState::Live,
                        )
                        .await?;
                    let digest = hash(&encode(REQUEST_KIND, &previous, REQUEST_LIMIT)?);
                    let applied_revision = self
                        .inner
                        .committed(&snapshot.head, &hex::encode(record.request.id), digest)
                        .await?
                        .filter(|applied| {
                            Some(*applied) == record.request.expected_revision.checked_add(1)
                        })
                        .ok_or(FleetCatalogError::Corrupt(
                            "managed recovery settlement journal",
                        ))?;
                    start = record.promoted;
                    if start == facts.total {
                        return Ok(record.status(applied_revision));
                    }
                }

                let promoted = (start + BATCH).min(facts.total);
                let request = Record::identity(&facts, request, snapshot.revision(), promoted)?;
                let record = Record {
                    facts,
                    promoted,
                    request,
                };
                let mutation = self
                    .recovery_settlement_mutation(
                        &snapshot,
                        &selection,
                        &record,
                        FleetHistoryState::Reserved,
                    )
                    .await?;
                let committed = Box::pin(self.commit_with_disruption(
                    &snapshot,
                    &mutation,
                    None,
                    resources::Authority::ManagedRecoverySettlement,
                    || Ok(()),
                ))
                .await?;
                Ok(record.status(committed.applied_revision))
            }),
        )
        .await
    }

    pub(super) async fn require_managed_recovery_promotion(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRecoveryAttachRequest,
        intent: &FleetTransitionRecoveryIntent,
        publication: [u8; 32],
        index: usize,
    ) -> Result<()> {
        let record = self
            .managed_recovery_settlement_at(snapshot, request.operation)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        record.check(request, intent, publication)?;
        if index >= record.promoted {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    async fn managed_recovery_settlement_at(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
    ) -> Result<Option<Record>> {
        let key = settlement_key(operation.bytes());
        self.inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], &key)
            .await?
            .map(|bytes| decode(KIND, &bytes, LIMIT))
            .transpose()
    }

    async fn recovery_settlement_mutation(
        &self,
        snapshot: &FleetSnapshot,
        selection: &Selection<'_>,
        record: &Record,
        expected: FleetHistoryState,
    ) -> Result<FleetMutation> {
        let mut changes = Vec::with_capacity(BATCH + 1);
        for birth in &selection.plan.births()[record.last_batch()] {
            let mut placement = self
                .managed_recovery_placement_at(
                    snapshot,
                    selection.directory,
                    selection.intent,
                    selection.plan,
                    birth,
                )
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            if placement.history.state != expected {
                return Err(FleetCatalogError::Conflict);
            }
            placement.history.state = FleetHistoryState::Live;
            changes.push(FleetChange {
                table: FleetTable::Histories,
                key: placement.key,
                value: Some(placement.history.encode()?),
            });
        }
        changes.push(FleetChange {
            table: FleetTable::Policy,
            key: settlement_key(record.request.operation),
            value: Some(encode(KIND, record, LIMIT)?),
        });
        let mutation = history_mutation(&record.request, changes);
        mutation.validate()?;
        Ok(mutation)
    }
}

fn settlement_key(operation: [u8; 16]) -> String {
    format!("{PREFIX}{}", hex::encode(operation))
}
