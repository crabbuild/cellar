// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Close the original disruption after a permanent fence and serving release.
use super::{
    disruption::SLOT_KEY,
    histories::{catalog_error, history_mutation, Origin},
    *,
};
use crate::raft::catalog::{DirectoryMutation, DisasterFenceAuthority, RangeDirectory};
use cellar_logic::{
    cell_group::{GroupHistory, GroupRoute},
    consensus::ProposalId,
};
use serde::Deserialize;
use std::{collections::BTreeSet, ops::Range};

pub(super) const PREFIX: &str = "managed-recovery-retirement/";
const KIND: u8 = 232;
const LIMIT: usize = 4096;

// Leave room for the protected cursor and final disruption-slot deletion.
const BATCH: usize = 14;

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Facts {
    recovery: [u8; 16],
    transition: [u8; 16],
    intent: [u8; 32],
    completion: [u8; 32],
    total: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    facts: Facts,
    retired: usize,
    request: FleetRequestIdentity,
}

/// Bounded, resumable retirement of histories fenced by disaster recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetRecoveryRetirement {
    pub retired_histories: usize,
    pub total_histories: usize,
    pub applied_revision: u64,
    pub fleet_slot_released: bool,
}

struct Sources {
    routes: Vec<GroupRoute>,
    children: BTreeSet<GroupHistory>,
}

impl Sources {
    fn new(directory: &RangeDirectory, plan: &DirectoryMutation) -> Result<Self> {
        let (mut routes, children) = directory.transition_routes(plan).map_err(catalog_error)?;
        let scopes = children.iter().map(|route| route.history).collect();
        routes.extend(children);
        routes.sort_unstable_by_key(|route| route.history);
        if !(3..=65).contains(&routes.len())
            || routes
                .windows(2)
                .any(|pair| pair[0].history == pair[1].history)
        {
            return Err(FleetCatalogError::Corrupt("recovery retirement sources"));
        }
        Ok(Self {
            routes,
            children: scopes,
        })
    }
}

impl Facts {
    fn digest(&self) -> Result<[u8; 32]> {
        Ok(hash(&encode(KIND, self, LIMIT)?))
    }

    fn identity(
        &self,
        principal: &str,
        revision: u64,
        retired: usize,
    ) -> Result<FleetRequestIdentity> {
        let step = u8::try_from(retired).map_err(|_| FleetCatalogError::Limit)?;
        Ok(TransitionOperatorStep {
            operation: ProposalId::new(self.recovery)
                .map_err(|_| FleetCatalogError::Corrupt("recovery retirement operation"))?,
            principal: principal.into(),
            request_digest: self.digest()?,
            step: [step; 16],
        }
        .identity(revision))
    }
}

impl Record {
    fn validate(&self, transition: [u8; 16], routes: usize) -> Result<()> {
        if self.facts.transition != transition
            || self.facts.total != routes
            || [self.facts.intent, self.facts.completion].contains(&[0; 32])
            || !(1..=routes).contains(&self.retired)
            || (self.retired != routes && !self.retired.is_multiple_of(BATCH))
            || self.request.expected_revision == 0
            || self.request
                != self.facts.identity(
                    &self.request.principal,
                    self.request.expected_revision,
                    self.retired,
                )?
        {
            return Err(FleetCatalogError::Corrupt("recovery retirement cursor"));
        }
        Ok(())
    }

    fn complete(&self) -> bool {
        self.retired == self.facts.total
    }

    fn last_batch(&self) -> Range<usize> {
        ((self.retired - 1) / BATCH * BATCH)..self.retired
    }

    fn status(&self, applied_revision: u64) -> FleetRecoveryRetirement {
        FleetRecoveryRetirement {
            retired_histories: self.retired,
            total_histories: self.facts.total,
            applied_revision,
            fleet_slot_released: self.complete(),
        }
    }
}

impl FleetCatalog {
    pub(crate) async fn advance_managed_recovery_retirement(
        &self,
        directory: &RangeDirectory,
        request: &FleetRecoveryAttachRequest,
        completion: [u8; 32],
        authority: &dyn DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest>,
        timeout: Duration,
    ) -> Result<FleetRecoveryRetirement> {
        self.run(
            timeout,
            Box::pin(async {
                let (intent, replacement, _) = self
                    .read_managed_recovery_publication_inputs(
                        directory,
                        request.operation,
                        &request.principal,
                        (request.plan_digest, request.seed_digest),
                    )
                    .await?;
                let transition_id = ProposalId::new(intent.transition())
                    .map_err(|_| FleetCatalogError::Corrupt("recovery retirement transition"))?;
                let snapshot = self.inner.current().await?;
                let transition = self
                    .observe_managed_transition_at(&snapshot, directory, transition_id)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?
                    .transition;
                if transition.plan.digest() != intent.basis().plan
                    || transition
                        .journal
                        .as_ref()
                        .and_then(|journal| journal.recovery_settlement())
                        != Some(completion)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let sources = Sources::new(directory, &transition.plan)?;
                let fenced: BTreeSet<_> = intent
                    .required_voter_lineages()
                    .iter()
                    .map(|lineage| lineage.group)
                    .collect();
                if !sources.routes.iter().map(|route| route.history).eq(fenced) {
                    return Err(FleetCatalogError::Conflict);
                }
                let facts = Facts {
                    recovery: request.operation.bytes(),
                    transition: transition_id.bytes(),
                    intent: intent.digest(),
                    completion,
                    total: sources.routes.len(),
                };
                let mut start = 0;
                if let Some(record) = self
                    .recovery_retirement_at(&snapshot, facts.transition)
                    .await?
                {
                    record.validate(facts.transition, facts.total)?;
                    if record.facts != facts || record.request.principal != request.principal {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    let previous = self
                        .recovery_retirement_mutation(&snapshot, directory, &sources, &record, true)
                        .await?;
                    let revision = self
                        .inner
                        .committed(
                            &snapshot.head,
                            &hex::encode(record.request.id),
                            hash(&encode(REQUEST_KIND, &previous, REQUEST_LIMIT)?),
                        )
                        .await?
                        .filter(|applied| {
                            Some(*applied) == record.request.expected_revision.checked_add(1)
                        })
                        .ok_or(FleetCatalogError::Corrupt("recovery retirement journal"))?;
                    if record.complete() {
                        return Ok(record.status(revision));
                    }
                    start = record.retired;
                }

                self.managed_recovery_permit_at(&snapshot, directory, intent.basis())
                    .await?;
                self.inner
                    .domain
                    .run(Box::pin(directory.check_managed_recovery_retirement(
                        request.operation,
                        transition_id,
                        facts.intent,
                        completion,
                        &sources.routes,
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                let (challenge, evidence) = self
                    .read_recovery_retirement_fence(intent, &replacement)
                    .await?;
                authority
                    .verify(&challenge, &evidence)
                    .await
                    .map_err(FleetCatalogError::Storage)?;

                let retired = (start + BATCH).min(facts.total);
                let record = Record {
                    request: facts.identity(&request.principal, snapshot.revision(), retired)?,
                    facts,
                    retired,
                };
                let mutation = self
                    .recovery_retirement_mutation(&snapshot, directory, &sources, &record, false)
                    .await?;
                let release = if record.complete() {
                    Some(
                        snapshot
                            .head
                            .disruption
                            .ok_or(FleetCatalogError::Conflict)?,
                    )
                } else {
                    None
                };
                let committed = Box::pin(self.commit_with_disruption(
                    &snapshot,
                    &mutation,
                    release,
                    resources::Authority::ManagedRecoveryRetirement,
                    || Ok(()),
                ))
                .await?;
                Ok(record.status(committed.applied_revision))
            }),
        )
        .await
    }

    async fn recovery_retirement_mutation(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        sources: &Sources,
        record: &Record,
        replay: bool,
    ) -> Result<FleetMutation> {
        let retired = FleetHistoryState::Retired {
            operation: record.facts.transition,
            retirement: record.facts.digest()?,
        };
        let mut changes = Vec::with_capacity(BATCH + 2);
        for route in &sources.routes[record.last_batch()] {
            let key = self
                .binding(&route.history)?
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("recovery retirement history key"))?;
            let mut history = self.history_inner(snapshot, &key).await?;
            let allowed = if replay {
                history.state == retired
            } else {
                history.state == FleetHistoryState::Live
                    || (history.state == FleetHistoryState::Reserved
                        && sources.children.contains(&route.history))
            };
            if !allowed
                || history.origin != Origin::Managed
                || !history.check_route(directory, route)
            {
                return Err(FleetCatalogError::Conflict);
            }
            history.state = retired.clone();
            changes.push(FleetChange {
                table: FleetTable::Histories,
                key,
                value: Some(history.encode()?),
            });
        }
        changes.push(FleetChange {
            table: FleetTable::Policy,
            key: retirement_key(record.facts.transition),
            value: Some(encode(KIND, record, LIMIT)?),
        });
        if record.complete() {
            changes.push(FleetChange {
                table: FleetTable::Policy,
                key: SLOT_KEY.into(),
                value: None,
            });
        }
        let mutation = history_mutation(&record.request, changes);
        mutation.validate()?;
        Ok(mutation)
    }

    async fn recovery_retirement_at(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
    ) -> Result<Option<Record>> {
        self.inner
            .lookup(
                snapshot.head.tables[FleetTable::Policy.slot()],
                &retirement_key(operation),
            )
            .await?
            .map(|bytes| decode(KIND, &bytes, LIMIT))
            .transpose()
    }

    pub(super) async fn require_recovery_retired_history(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        route: &GroupRoute,
        completion: [u8; 32],
        retirement: [u8; 32],
    ) -> Result<()> {
        let operation = plan.operation().bytes();
        let sources = Sources::new(directory, plan)?;
        let record = self
            .recovery_retirement_at(snapshot, operation)
            .await?
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        record.validate(operation, sources.routes.len())?;
        if record.facts.completion != completion
            || record.facts.digest()? != retirement
            || !sources.routes[..record.retired].contains(route)
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        Ok(())
    }
}

fn retirement_key(operation: [u8; 16]) -> String {
    format!("{PREFIX}{}", hex::encode(operation))
}
