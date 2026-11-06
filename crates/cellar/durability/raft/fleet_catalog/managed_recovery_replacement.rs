// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable, range-preserving replacement plans under a managed recovery fence.
use super::{histories::catalog_error, *};
use crate::raft::{
    catalog::{
        CellGroupBirthRequest, CellGroupBirthSpec, DirectoryMutation, DirectoryView, RangeDirectory,
    },
    cell_groups::CellGroupArchivePool,
};
use cellar_logic::consensus::ProposalId;
use serde::Deserialize;
use std::collections::BTreeSet;

const KIND: u8 = 224;
const LIMIT: usize = 4 * 1024 * 1024;

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    fence: [u8; 32],
    receipt: [u8; 32],
    births: Vec<Vec<u8>>,
    mutation: Vec<u8>,
}

/// One durable, exact replacement per recovered range in source order. The
/// plan retains placement intent but grants no resource reservation or startup.
pub struct FleetTransitionRecoveryReplacement {
    fence: [u8; 32],
    receipt: [u8; 32],
    births: Vec<CellGroupBirthRequest>,
    mutation: DirectoryMutation,
    digest: [u8; 32],
}

impl FleetTransitionRecoveryReplacement {
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn births(&self) -> &[CellGroupBirthRequest] {
        &self.births
    }

    pub fn mutation(&self) -> &DirectoryMutation {
        &self.mutation
    }

    pub(crate) fn matches_fence(&self, fenced: &FleetFencedTransitionRecovery) -> bool {
        self.fence == fenced.request().digest() && self.receipt == fenced.receipt_digest()
    }
}

impl FleetCatalog {
    pub(crate) async fn stage_managed_recovery_replacement(
        &self,
        directory: &RangeDirectory,
        fenced: &FleetFencedTransitionRecovery,
        replacements: Vec<CellGroupBirthSpec>,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<FleetTransitionRecoveryReplacement> {
        self.run(timeout, async {
            let view = self
                .managed_replacement_view(directory, fenced, archives)
                .await?;
            if replacements.len() != fenced.archives().len() {
                return Err(FleetCatalogError::Invalid("recovery replacement count"));
            }
            let owner = self.birth_owner()?;
            let births = replacements
                .into_iter()
                .map(|spec| {
                    directory
                        .prepare_fleet_group_birth(spec, &owner)
                        .map_err(catalog_error)
                })
                .collect::<Result<Vec<_>>>()?;
            let mutation = replacement_mutation(directory, &view, fenced, &births)?;
            let record = Record {
                fence: fenced.request().digest(),
                receipt: fenced.receipt_digest(),
                births: births.iter().map(|birth| birth.encode().to_vec()).collect(),
                mutation: mutation.encode().to_vec(),
            };
            let bytes = encode(KIND, &record, LIMIT)?;
            let key = replacement_key(fenced);
            if let Some((selected, _)) = self.inner.read::<Record>(&key, KIND, LIMIT).await? {
                if selected != record {
                    return Err(FleetCatalogError::PayloadConflict);
                }
            } else {
                let domain = &self.inner.domain;
                for birth in &births {
                    domain
                        .run(directory.require_unreserved_fleet_group(birth, &owner))
                        .await
                        .map_err(|_| FleetCatalogError::Closed)?
                        .map_err(catalog_error)?;
                }
                self.inner.create_exact(&key, &bytes, LIMIT).await?;
            }
            Ok(FleetTransitionRecoveryReplacement {
                fence: record.fence,
                receipt: record.receipt,
                births,
                mutation,
                digest: hash(&bytes),
            })
        })
        .await
    }

    pub(crate) async fn reopen_managed_recovery_replacement(
        &self,
        directory: &RangeDirectory,
        fenced: &FleetFencedTransitionRecovery,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<Option<FleetTransitionRecoveryReplacement>> {
        self.run(timeout, async {
            let view = self
                .managed_replacement_view(directory, fenced, archives)
                .await?;
            let Some((record, _)) = self
                .inner
                .read::<Record>(&replacement_key(fenced), KIND, LIMIT)
                .await?
            else {
                return Ok(None);
            };
            if record.fence != fenced.request().digest()
                || record.receipt != fenced.receipt_digest()
            {
                return Err(FleetCatalogError::PayloadConflict);
            }
            let births = record
                .births
                .iter()
                .map(|bytes| directory.decode_group_birth(bytes).map_err(catalog_error))
                .collect::<Result<Vec<_>>>()?;
            let mutation = replacement_mutation(directory, &view, fenced, &births)?;
            if mutation.encode() != record.mutation {
                return Err(FleetCatalogError::Corrupt("recovery replacement mutation"));
            }
            Ok(Some(FleetTransitionRecoveryReplacement {
                fence: record.fence,
                receipt: record.receipt,
                births,
                mutation,
                digest: hash(&encode(KIND, &record, LIMIT)?),
            }))
        })
        .await
    }

    pub(super) async fn require_managed_replacement(
        &self,
        fenced: &FleetFencedTransitionRecovery,
        plan: &FleetTransitionRecoveryReplacement,
    ) -> Result<()> {
        let (record, _) = self
            .inner
            .read::<Record>(&replacement_key(fenced), KIND, LIMIT)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if !plan.matches_fence(fenced)
            || record.fence != fenced.request().digest()
            || record.receipt != fenced.receipt_digest()
            || hash(&encode(KIND, &record, LIMIT)?) != plan.digest()
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(())
    }

    async fn managed_replacement_view(
        &self,
        directory: &RangeDirectory,
        fenced: &FleetFencedTransitionRecovery,
        archives: &CellGroupArchivePool,
    ) -> Result<DirectoryView> {
        self.require_confirmed_managed_fence(directory, fenced, archives)
            .await?;
        let view = self
            .inner
            .domain
            .run(directory.observe())
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(catalog_error)?;
        if view.digest() != fenced.request().barrier().directory_digest() {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(view)
    }
}

fn replacement_mutation(
    directory: &RangeDirectory,
    view: &DirectoryView,
    fenced: &FleetFencedTransitionRecovery,
    births: &[CellGroupBirthRequest],
) -> Result<DirectoryMutation> {
    let intent = fenced.request().intent();
    if births.len() != fenced.archives().len() {
        return Err(FleetCatalogError::Invalid("recovery replacement count"));
    }
    let old = intent.required_voter_lineages();
    let old_groups: BTreeSet<_> = old.iter().map(|member| member.group.group).collect();
    let old_incarnations: BTreeSet<_> = old.iter().map(|member| member.group.incarnation).collect();
    let old_nodes: BTreeSet<_> = old.iter().map(|member| member.node).collect();
    let member_floor = old
        .iter()
        .map(|member| member.member.get())
        .chain(
            fenced
                .archives()
                .iter()
                .map(|cut| cut.recovery().membership().high_water()),
        )
        .max()
        .ok_or(FleetCatalogError::Corrupt("recovery voter inventory"))?;
    let mut groups = BTreeSet::new();
    let mut incarnations = BTreeSet::new();
    let mut creations = BTreeSet::new();
    let mut sources = Vec::with_capacity(births.len());
    let mut targets = Vec::with_capacity(births.len());
    for (source, birth) in fenced.archives().iter().zip(births) {
        let source_route = source.marker().route();
        let target = birth.route();
        if target.range != source_route.range
            || target.schema_digest != source_route.schema_digest
            || old_groups.contains(&target.history.group)
            || old_incarnations.contains(&target.history.incarnation)
            || !groups.insert(target.history.group)
            || !incarnations.insert(target.history.incarnation)
            || !creations.insert(birth.creation())
            || [intent.operation(), intent.transition()].contains(&birth.creation().bytes())
            || birth.voters().len() != 3
            || !birth.learners().is_empty()
            || birth.voters().iter().any(|member| {
                old_nodes.contains(&member.node) || member.member.get() <= member_floor
            })
        {
            return Err(FleetCatalogError::Conflict);
        }
        source
            .recovery()
            .check_recovery_cell_incarnation(target.history.incarnation)?;
        sources.push(source_route.clone());
        targets.push(target.clone());
    }
    let operation = ProposalId::new(intent.operation())
        .map_err(|_| FleetCatalogError::Corrupt("recovery replacement operation"))?;
    directory
        .prepare_replacement(view, operation, &sources, &targets)
        .map_err(catalog_error)
}

fn replacement_key(fenced: &FleetFencedTransitionRecovery) -> String {
    format!(
        "managed-transition-recovery-replacements/{}",
        hex::encode(fenced.request().intent().operation())
    )
}
