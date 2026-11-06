// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select the exact seed receipts used to admit a managed recovery publication.
use super::{histories::catalog_error, *};
use crate::raft::{
    catalog::{
        DirectoryPublication, ManagedRecoveryPublicationInput, ManagedRecoverySeedQuorum,
        RangeDirectory,
    },
    cell_groups::CellGroupArchivePool,
    fleet_stop::BootBinding,
};
use cellar_logic::consensus::{NodeId, ProposalId};
use serde::Deserialize;
use std::collections::BTreeMap;

const KIND: u8 = 229;
const LIMIT: usize = 64 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    plan: [u8; 32],
    receipt: [u8; 32],
    ranges: Vec<ManagedRecoverySeedQuorum>,
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
            let key = format!(
                "managed-transition-recovery-seed-quorums/{}",
                hex::encode(intent.operation())
            );
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
            let key = format!(
                "managed-transition-recovery-seed-quorums/{}",
                hex::encode(intent.operation())
            );
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
            let transition = ProposalId::new(intent.transition())
                .map_err(|_| FleetCatalogError::Corrupt("managed recovery transition"))?;
            let publication = ManagedRecoveryPublicationInput {
                intent: intent.digest(),
                transition,
                barrier: fenced.request().barrier().directory_digest(),
                plan: plan.digest(),
                seeds: selected.digest(),
                mutation: plan.mutation(),
                births: plan.births(),
            };
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
            self.require_confirmed_managed_fence(directory, fenced, archives).await?;
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
                .managed_recovery_reservation_at(snapshot, directory, fenced, plan, birth)
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
                if current.as_ref() == Some(boot) {
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
