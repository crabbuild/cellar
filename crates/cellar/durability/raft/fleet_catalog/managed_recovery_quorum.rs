// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select the exact seed receipts used to admit a managed recovery publication.
use super::{histories::catalog_error, *};
use crate::raft::{
    catalog::{
        CatalogError, CellGroupBirthRequest, DirectoryPublication, ManagedRecoveryPublicationInput,
        ManagedRecoverySeedQuorum, RangeDirectory,
    },
    cell_groups::CellGroupArchivePool,
    fleet_stop::BootBinding,
    member_state::FleetGroupBinding,
};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{LogPosition, NodeId, ProposalId},
};
use serde::Deserialize;
use std::collections::BTreeMap;

const KIND: u8 = 229;
const LIMIT: usize = 64 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    plan: [u8; 32],
    receipt: [u8; 32],
    pub(super) ranges: Vec<ManagedRecoverySeedQuorum>,
}

/// A verified durable seed set for every replacement range, in plan order.
/// Actual Raft quorum and activation require separate proofs.
pub struct FleetRecoverySeedQuorums {
    record: Record,
    digest: [u8; 32],
}

impl FleetRecoverySeedQuorums {
    pub fn plan_digest(&self) -> [u8; 32] {
        self.record.plan
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn ranges(&self) -> &[ManagedRecoverySeedQuorum] {
        &self.record.ranges
    }
}

/// Durable recovery identifiers used to reopen a published range on a voter.
#[derive(Clone, Debug)]
pub struct FleetRecoveryAttachRequest {
    pub operation: ProposalId,
    pub principal: String,
    pub plan_digest: [u8; 32],
    pub seed_digest: [u8; 32],
}

pub(crate) struct FleetPublishedRecoveryVoter {
    pub(crate) birth: CellGroupBirthRequest,
    pub(crate) seeds: ManagedRecoverySeedQuorum,
    pub(crate) publication: DirectoryPublication,
    pub(crate) group: FleetGroupBinding,
    pub(crate) seeded_boot: BootBinding,
    pub(crate) current_boot: BootBinding,
}

impl FleetHostAuthorization {
    pub(crate) async fn authorize_published_recovery_voter(
        &self,
        directory: &RangeDirectory,
        request: &FleetRecoveryAttachRequest,
        history: GroupHistory,
        timeout: Duration,
    ) -> Result<FleetPublishedRecoveryVoter> {
        let catalog = &self.catalog;
        catalog
            .run(timeout, async {
                let (intent, plan, record) = catalog
                    .read_managed_recovery_publication_inputs(
                        directory,
                        request.operation,
                        &request.principal,
                        (request.plan_digest, request.seed_digest),
                    )
                    .await?;
                let snapshot = catalog.inner.current().await?;
                catalog
                    .managed_recovery_permit_at(&snapshot, directory, intent.basis())
                    .await?;
                let index = plan
                    .births()
                    .iter()
                    .position(|birth| birth.route().history == history)
                    .ok_or(FleetCatalogError::Conflict)?;
                let birth = &plan.births()[index];
                let voter = birth
                    .voters()
                    .iter()
                    .position(|voter| voter.node == self.node())
                    .ok_or(FleetCatalogError::Conflict)?;
                let placement = catalog
                    .managed_recovery_placement_at(&snapshot, directory, &intent, &plan, birth)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                let seeded_boot = &placement.reservation.voter_boots()[voter];
                let current_boot = self.boot_binding()?;
                if !seeded_boot.same_engine_owner(&current_boot)
                    || catalog
                        .qualified_recovery_boot(&snapshot, self.node())
                        .await?
                        .as_ref()
                        != Some(&current_boot)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let input = publication_input(&intent, &plan, request.seed_digest)?;
                let publication = catalog
                    .inner
                    .domain
                    .run(Box::pin(
                        directory.observe_pending_managed_recovery_publication(&input),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?
                    .ok_or(FleetCatalogError::Conflict)?;
                if placement.history.state == FleetHistoryState::Live {
                    catalog
                        .require_managed_recovery_promotion(
                            &snapshot,
                            request,
                            &intent,
                            publication.journal_digest(),
                            index,
                        )
                        .await?;
                }
                if catalog.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                Ok(FleetPublishedRecoveryVoter {
                    birth: birth.clone(),
                    seeds: record.ranges[index].clone(),
                    publication,
                    group: catalog.binding(&history)?,
                    seeded_boot: seeded_boot.clone(),
                    current_boot,
                })
            })
            .await
    }
}

impl FleetCatalog {
    pub(crate) async fn select_managed_recovery_seed_quorums(
        &self,
        directory: &RangeDirectory,
        fenced: &FleetFencedTransitionRecovery,
        plan: &FleetTransitionRecoveryReplacement,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<FleetRecoverySeedQuorums> {
        self.run(timeout, async {
            self.require_managed_replacement(fenced, plan).await?;
            self.require_confirmed_managed_fence(directory, fenced, archives)
                .await?;
            let intent = fenced.request().intent();
            let snapshot = self.inner.current().await?;
            self.managed_recovery_permit_at(&snapshot, directory, intent.basis())
                .await?;
            let key = seed_quorum_key(intent.operation());
            let existing = self.inner.read::<Record>(&key, KIND, LIMIT).await?;
            if let Some((record, _)) = &existing {
                if record.plan != plan.digest()
                    || record.receipt != fenced.receipt_digest()
                    || record.ranges.len() != plan.births().len()
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
            }

            let ranges = self
                .verify_managed_seed_quorums(
                    &snapshot,
                    directory,
                    fenced,
                    plan,
                    existing.as_ref().map(|(record, _)| record),
                )
                .await?;
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            self.require_confirmed_managed_fence(directory, fenced, archives)
                .await?;
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            let record = Record {
                plan: plan.digest(),
                receipt: fenced.receipt_digest(),
                ranges,
            };
            let bytes = encode(KIND, &record, LIMIT)?;
            if existing.is_none() {
                self.inner.create_exact(&key, &bytes, LIMIT).await?;
            }
            Ok(FleetRecoverySeedQuorums {
                record,
                digest: hash(&bytes),
            })
        })
        .await
    }

    pub(crate) async fn publish_managed_recovery_replacement(
        &self,
        directory: &RangeDirectory,
        fenced: &FleetFencedTransitionRecovery,
        plan: &FleetTransitionRecoveryReplacement,
        selected: &FleetRecoverySeedQuorums,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<DirectoryPublication> {
        self.run(timeout, async {
            self.require_managed_replacement(fenced, plan).await?;
            let intent = fenced.request().intent();
            let key = seed_quorum_key(intent.operation());
            let (record, _) = self
                .inner
                .read::<Record>(&key, KIND, LIMIT)
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            if record.plan != plan.digest()
                || record.receipt != fenced.receipt_digest()
                || record.ranges.len() != plan.births().len()
                || hash(&encode(KIND, &record, LIMIT)?) != selected.digest()
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let snapshot = self.inner.current().await?;
            self.managed_recovery_permit_at(&snapshot, directory, intent.basis())
                .await?;
            let publication = publication_input(intent, plan, selected.digest())?;
            let domain = &self.inner.domain;
            if let Some(existing) = domain
                .run(Box::pin(
                    directory.observe_managed_recovery_publication(&publication),
                ))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?
            {
                return Ok(existing);
            }
            self.require_confirmed_managed_fence(directory, fenced, archives)
                .await?;
            self.verify_managed_seed_quorums(&snapshot, directory, fenced, plan, Some(&record))
                .await?;
            self.require_confirmed_managed_fence(directory, fenced, archives)
                .await?;
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            domain
                .run(Box::pin(directory.publish_managed_recovery(&publication)))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)
        })
        .await
    }

    pub(crate) async fn reopen_managed_recovery_publication(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        principal: &str,
        expected: ([u8; 32], [u8; 32]),
        timeout: Duration,
    ) -> Result<Option<DirectoryPublication>> {
        self.run(timeout, async {
            let (intent, plan, _) = self
                .read_managed_recovery_publication_inputs(directory, operation, principal, expected)
                .await?;
            let (_, seed_digest) = expected;
            let snapshot = self.inner.current().await?;
            self.managed_recovery_permit_at(&snapshot, directory, intent.basis())
                .await?;
            let input = publication_input(&intent, &plan, seed_digest)?;
            self.inner
                .domain
                .run(Box::pin(
                    directory.observe_managed_recovery_publication(&input),
                ))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)
        })
        .await
    }

    pub(crate) async fn reopen_managed_recovery_activation(
        &self,
        directory: &RangeDirectory,
        request: &FleetRecoveryAttachRequest,
        history: GroupHistory,
        timeout: Duration,
    ) -> Result<Option<LogPosition>> {
        self.run(timeout, async {
            let (intent, plan, record) = self
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
            let index = plan
                .births()
                .iter()
                .position(|birth| birth.route().history == history)
                .ok_or(FleetCatalogError::Conflict)?;
            let input = publication_input(&intent, &plan, request.seed_digest)?;
            self.inner
                .domain
                .run(Box::pin(async {
                    let publication = directory
                        .observe_managed_recovery_publication(&input)
                        .await?
                        .ok_or(CatalogError::Fenced)?;
                    directory
                        .observe_managed_recovery_activation(
                            plan.births()[index].route(),
                            request.plan_digest,
                            publication.journal_digest(),
                            record.ranges[index].genesis_commitment(),
                        )
                        .await
                }))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)
        })
        .await
    }

    pub(super) async fn read_managed_recovery_publication_inputs(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        principal: &str,
        expected: ([u8; 32], [u8; 32]),
    ) -> Result<(
        FleetTransitionRecoveryIntent,
        FleetTransitionRecoveryReplacement,
        Record,
    )> {
        let intent = self
            .read_managed_recovery_intent(directory, operation, principal)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let (plan_digest, seed_digest) = expected;
        let plan = self
            .read_recovery_publication_plan(directory, &intent, plan_digest)
            .await?;
        let (record, _) = self
            .inner
            .read::<Record>(&seed_quorum_key(intent.operation()), KIND, LIMIT)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if record.plan != plan_digest
            || record.receipt != plan.receipt_digest()
            || record.ranges.len() != plan.births().len()
            || hash(&encode(KIND, &record, LIMIT)?) != seed_digest
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok((intent, plan, record))
    }

    async fn verify_managed_seed_quorums(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        fenced: &FleetFencedTransitionRecovery,
        plan: &FleetTransitionRecoveryReplacement,
        existing: Option<&Record>,
    ) -> Result<Vec<ManagedRecoverySeedQuorum>> {
        let domain = &self.inner.domain;
        let mut qualified = BTreeMap::new();
        let mut ranges = Vec::with_capacity(plan.births().len());
        for (index, birth) in plan.births().iter().enumerate() {
            let reserved = self
                .managed_recovery_reservation_at(
                    snapshot,
                    directory,
                    fenced.request().intent(),
                    plan,
                    birth,
                )
                .await?
                .ok_or(FleetCatalogError::Unconfirmed)?;
            let boots = reserved.voter_boots();
            let selected = existing.map(|record| &record.ranges[index]);
            let quorum = domain
                .run(Box::pin(directory.observe_managed_recovery_seed_quorum(
                    birth,
                    plan.digest(),
                    boots,
                    selected,
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?;
            let mut eligible = 0;
            for (voter, boot) in birth.voters().iter().zip(boots) {
                if quorum.members().binary_search(&voter.member.get()).is_err() {
                    continue;
                }
                let current = match qualified.entry(voter.node) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(self.qualified_recovery_boot(snapshot, voter.node).await?)
                    }
                };
                // The seed is bound to durable storage. A newly qualified
                // boot can retain it only on the exact same disk and engine.
                if current
                    .as_ref()
                    .is_some_and(|current| boot.same_engine_owner(current))
                {
                    eligible += 1;
                }
            }
            if eligible <= birth.voters().len() / 2 {
                return Err(FleetCatalogError::Unconfirmed);
            }
            ranges.push(quorum);
        }
        Ok(ranges)
    }

    async fn qualified_recovery_boot(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
    ) -> Result<Option<BootBinding>> {
        let host = self.host_from_record(snapshot, node).await?;
        if host.record.isolation.is_some() || !host.state().eligibility().receive_replicas {
            return Ok(None);
        }
        match self.capacity_from(snapshot, node).await {
            Ok(capacity) if capacity.receive_replicas() => {
                Ok(Some(capacity.attestation().boot.clone()))
            }
            Ok(_) | Err(FleetCatalogError::Conflict) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

pub(super) fn publication_input<'a>(
    intent: &FleetTransitionRecoveryIntent,
    plan: &'a FleetTransitionRecoveryReplacement,
    seeds: [u8; 32],
) -> Result<ManagedRecoveryPublicationInput<'a>> {
    let transition = ProposalId::new(intent.transition())
        .map_err(|_| FleetCatalogError::Corrupt("managed recovery transition"))?;
    let barrier = plan
        .mutation()
        .expected_directory_digest()
        .ok_or(FleetCatalogError::Conflict)?;
    Ok(ManagedRecoveryPublicationInput {
        intent: intent.digest(),
        transition,
        transition_plan: intent.basis().plan,
        barrier,
        plan: plan.digest(),
        seeds,
        mutation: plan.mutation(),
        births: plan.births(),
    })
}

fn seed_quorum_key(operation: [u8; 16]) -> String {
    format!(
        "managed-transition-recovery-seed-quorums/{}",
        hex::encode(operation)
    )
}
