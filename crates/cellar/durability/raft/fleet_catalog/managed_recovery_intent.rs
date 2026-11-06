// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable recovery selections that survive controller loss.
use super::{
    histories::{catalog_error, validate_request},
    managed_transitions::root_digest,
    *,
};
use crate::raft::{
    catalog::RangeDirectory,
    cell_groups::{CellGroupArchiveHold, CellGroupArchivePool},
};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{DiskLineage, GroupId, MemberId, NamespaceId, NodeId, ProposalId},
};
use serde::Deserialize;
use std::collections::BTreeSet;

const KIND: u8 = 222;
const LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagedRecoveryIntentBasis {
    pub(crate) transition: [u8; 16],
    pub(crate) plan: [u8; 32],
    pub(crate) fleet_root: [u8; 32],
    pub(crate) fleet_revision: u64,
    pub(crate) directory_root: [u8; 32],
    pub(crate) publication: Option<[u8; 32]>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lineage {
    group: [u8; 16],
    incarnation: [u8; 16],
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
    host: [u8; 16],
}

impl From<&FleetRecoveryVoterLineage> for Lineage {
    fn from(value: &FleetRecoveryVoterLineage) -> Self {
        Self {
            group: value.group.group.bytes(),
            incarnation: value.group.incarnation.bytes(),
            member: value.member.get(),
            node: value.node.bytes(),
            disk: value.disk.bytes(),
            host: value.physical_host.bytes(),
        }
    }
}

impl Lineage {
    fn decode(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
    ) -> Result<FleetRecoveryVoterLineage> {
        let invalid = |_| FleetCatalogError::Corrupt("transition recovery lineage");
        Ok(FleetRecoveryVoterLineage {
            group: GroupHistory {
                cluster,
                namespace,
                group: GroupId::new(self.group).map_err(invalid)?,
                incarnation: IncarnationId::new(self.incarnation).map_err(invalid)?,
            },
            member: MemberId::new(self.member).map_err(invalid)?,
            node: NodeId::new(self.node).map_err(invalid)?,
            disk: DiskLineage::new(self.disk).map_err(invalid)?,
            physical_host: IncarnationId::new(self.host).map_err(invalid)?,
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    catalog: [u8; 32],
    namespace: [u8; 32],
    operation: [u8; 16],
    principal: String,
    basis: ManagedRecoveryIntentBasis,
    holds: Vec<CellGroupArchiveHold>,
    lineages: Vec<Lineage>,
}

/// Exact durable input to a future fencing operation. Starting a fence
/// requires its own ownership barrier and a fresh verification of these cuts.
pub struct FleetTransitionRecoveryIntent {
    record: Record,
    digest: [u8; 32],
    lineages: Vec<FleetRecoveryVoterLineage>,
}

/// Read-back proof of the exact recovery barrier selected in the directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FleetTransitionRecoveryBarrier {
    intent_digest: [u8; 32],
    directory_digest: [u8; 32],
}

impl FleetTransitionRecoveryBarrier {
    pub fn intent_digest(&self) -> [u8; 32] {
        self.intent_digest
    }

    pub fn directory_digest(&self) -> [u8; 32] {
        self.directory_digest
    }
}

impl FleetTransitionRecoveryIntent {
    pub fn operation(&self) -> [u8; 16] {
        self.record.operation
    }

    pub fn transition(&self) -> [u8; 16] {
        self.record.basis.transition
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn archive_holds(&self) -> &[CellGroupArchiveHold] {
        &self.record.holds
    }

    pub fn required_voter_lineages(&self) -> &[FleetRecoveryVoterLineage] {
        &self.lineages
    }
}

impl Record {
    fn validate(
        self,
        fleet: &FleetCatalog,
        directory: &RangeDirectory,
        operation: ProposalId,
        principal: &str,
    ) -> Result<FleetTransitionRecoveryIntent> {
        validate_request(&FleetRequestIdentity {
            id: self.operation,
            operation: self.operation,
            principal: self.principal.clone(),
            expected_revision: self.basis.fleet_revision,
        })?;
        if self.catalog != fleet.catalog_id()?
            || self.namespace != directory.namespace().root_digest()
            || self.operation != operation.bytes()
            || self.principal != principal
        {
            return Err(FleetCatalogError::Conflict);
        }
        let basis = &self.basis;
        if basis.transition == [0; 16]
            || basis.transition == self.operation
            || [basis.plan, basis.fleet_root, basis.directory_root].contains(&[0; 32])
            || basis.publication == Some([0; 32])
            || self.holds.is_empty()
            || self.holds.len() > 64
            || self.lineages.is_empty()
            || self.lineages.len() > super::managed_recovery_inventory::MAX_RECOVERY_LINEAGES
        {
            return Err(FleetCatalogError::Invalid("transition recovery intent"));
        }
        let config = directory.namespace_config();
        let lineages: Vec<_> = self
            .lineages
            .iter()
            .map(|lineage| lineage.decode(config.cluster, config.namespace))
            .collect::<Result<_>>()?;
        if lineages.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(FleetCatalogError::Corrupt(
                "transition recovery lineage order",
            ));
        }
        let groups: BTreeSet<_> = lineages.iter().map(|lineage| lineage.group).collect();
        let mut held = BTreeSet::new();
        for hold in &self.holds {
            hold.check_reference()?;
            let group = hold.marker().route().history;
            if !groups.contains(&group) || !held.insert(group) {
                return Err(FleetCatalogError::Conflict);
            }
        }
        let digest = hash(&encode(KIND, &self, LIMIT)?);
        Ok(FleetTransitionRecoveryIntent {
            record: self,
            digest,
            lineages,
        })
    }
}

pub(crate) struct ManagedRecoveryIntentInput<'a> {
    pub(crate) operation: ProposalId,
    pub(crate) principal: &'a str,
    pub(crate) basis: &'a ManagedRecoveryIntentBasis,
    pub(crate) holds: Vec<CellGroupArchiveHold>,
    pub(crate) lineages: &'a [FleetRecoveryVoterLineage],
}

impl FleetCatalog {
    pub(crate) async fn stage_managed_transition_recovery_intent(
        &self,
        directory: &RangeDirectory,
        input: ManagedRecoveryIntentInput<'_>,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<FleetTransitionRecoveryIntent> {
        self.run(
            timeout,
            Box::pin(async {
                let record = Record {
                    catalog: self.catalog_id()?,
                    namespace: directory.namespace().root_digest(),
                    operation: input.operation.bytes(),
                    principal: input.principal.into(),
                    basis: input.basis.clone(),
                    holds: input.holds,
                    lineages: input.lineages.iter().map(Lineage::from).collect(),
                };
                let intent = record.validate(self, directory, input.operation, input.principal)?;
                let key = intent_key(input.operation);
                if let Some((stored, _)) = self.inner.read::<Record>(&key, KIND, LIMIT).await? {
                    return if stored == intent.record {
                        Ok(intent)
                    } else {
                        Err(FleetCatalogError::PayloadConflict)
                    };
                }
                let domain = &self.inner.domain;
                let view = domain
                    .run(directory.observe())
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                let current = self.inner.current().await?;
                if view.digest() != input.basis.directory_root
                    || current.revision() != input.basis.fleet_revision
                    || root_digest(&current)? != input.basis.fleet_root
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.require_recovery_intent_holds(directory, archives, &intent)
                    .await?;
                self.inner
                    .create_exact(&key, &encode(KIND, &intent.record, LIMIT)?, LIMIT)
                    .await?;
                Ok(intent)
            }),
        )
        .await
    }

    async fn require_recovery_intent_holds(
        &self,
        directory: &RangeDirectory,
        archives: &CellGroupArchivePool,
        intent: &FleetTransitionRecoveryIntent,
    ) -> Result<()> {
        let domain = &self.inner.domain;
        for hold in &intent.record.holds {
            let history = hold.marker().route().history;
            let birth = domain
                .run(directory.materialize_selected_history(history))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?;
            let registry = directory
                .open_group_archive(domain, archives, &birth)
                .await
                .map_err(super::managed_transitions::transfer_archive_error)?;
            registry.require_selected_hold(domain, hold).await?;
        }
        Ok(())
    }

    pub(crate) async fn select_managed_transition_recovery_barrier(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        principal: &str,
        expected: [u8; 32],
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<FleetTransitionRecoveryBarrier> {
        self.run(
            timeout,
            Box::pin(async {
                let (record, _) = self
                    .inner
                    .read::<Record>(&intent_key(operation), KIND, LIMIT)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                let intent = record.validate(self, directory, operation, principal)?;
                if intent.digest() != expected {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let basis = &intent.record.basis;
                let snapshot = self.inner.current().await?;
                let permit = self
                    .managed_recovery_permit_at(&snapshot, directory, basis)
                    .await?;
                let domain = &self.inner.domain;
                let selected = domain
                    .run(directory.observe_managed_recovery_barrier(expected, basis))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
                let directory_digest = if let Some(selected) = selected {
                    selected
                } else {
                    if snapshot.revision() != basis.fleet_revision
                        || root_digest(&snapshot)? != basis.fleet_root
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    self.require_recovery_intent_holds(directory, archives, &intent)
                        .await?;
                    if self.inner.current().await?.token != snapshot.token {
                        return Err(FleetCatalogError::Conflict);
                    }
                    domain
                        .run(Box::pin(directory.select_managed_recovery_barrier(
                            expected,
                            basis,
                            intent.archive_holds(),
                            &permit,
                        )))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(catalog_error)?
                };
                Ok(FleetTransitionRecoveryBarrier {
                    intent_digest: expected,
                    directory_digest,
                })
            }),
        )
        .await
    }

    pub(crate) async fn reopen_managed_transition_recovery_intent(
        &self,
        directory: &RangeDirectory,
        operation: ProposalId,
        principal: &str,
        timeout: Duration,
    ) -> Result<Option<FleetTransitionRecoveryIntent>> {
        self.run(timeout, async {
            self.inner
                .read::<Record>(&intent_key(operation), KIND, LIMIT)
                .await?
                .map(|(record, _)| record.validate(self, directory, operation, principal))
                .transpose()
        })
        .await
    }
}

fn intent_key(operation: ProposalId) -> String {
    format!(
        "managed-transition-recovery/{}",
        hex::encode(operation.bytes())
    )
}
