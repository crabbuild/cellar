// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::{
    CellGroupArchiveError, CellGroupArchiveExecution, CellGroupArchiveHold, CellGroupArchivePool,
    CellGroupArchiveTailPool, GroupManifest,
};
use cellar_logic::consensus::LogPosition;
use serde::{Deserialize, Serialize};

const HOLD: u8 = 209;
const SET: u8 = 210;
const HOLDS: &str = "transfer-child-archive-holds";
const SETS: &str = "transfer-child-archive-sets";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ChildArchiveSet {
    namespace: [u8; 32],
    plan: [u8; 32],
    publication: [u8; 32],
    activations: [u8; 32],
    holds: Vec<[u8; 32]>,
}

struct ChildArchiveBasis {
    publication: [u8; 32],
    activations: [u8; 32],
    installed: Vec<GroupManifest>,
    applied: Vec<(GroupHistory, LogPosition)>,
    selected: Option<[u8; 32]>,
}

impl ChildArchiveBasis {
    fn check_marker(&self, index: usize, marker: &GroupManifest) -> Result<(), CatalogError> {
        let installed = &self.installed[index];
        let (history, position) = self.applied[index];
        if marker.route() != installed.route()
            || marker.route().history != history
            || !marker.is_active()
            || !marker.preserves_transfer(installed)
            || marker
                .transfer_activation()
                .is_none_or(|(publication, _, applied)| {
                    publication != self.publication || applied != position
                })
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }
}

impl RangeDirectory {
    /// Independently replay an exact archive hold for every active child.
    /// The selected coverage protects the transfer recovery boundary. Parent
    /// holds and the topology lane remain owned until retirement settlement.
    pub async fn verify_and_record_child_archives(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        plan: &DirectoryMutation,
        children: Vec<CellGroupTransitionSource>,
    ) -> Result<TransitionJournal, CellGroupArchiveBindingError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(plan)?;
        if children.len() != plan.request.children.len() {
            return Err(CatalogError::Invalid("incomplete transfer child archives").into());
        }
        domain
            .run(Box::pin(async {
                self.namespace.require_cell_group_unmanaged().await?;
                let basis = self.bounded(self.child_archive_basis(plan)).await?;
                let mut hold_records = Vec::with_capacity(children.len());
                for (index, child) in children.iter().enumerate() {
                    if child.birth.route() != basis.installed[index].route() {
                        return Err(CatalogError::Fenced.into());
                    }
                    child.hold.check_reference()?;
                    basis.check_marker(index, child.hold.marker())?;
                    let bytes = record::encode(HOLD, &child.hold, wire::LIMIT)?;
                    hold_records.push((record::digest(&bytes), bytes));
                }
                let set = ChildArchiveSet {
                    namespace: self.namespace.root_digest,
                    plan: plan.digest(),
                    publication: basis.publication,
                    activations: basis.activations,
                    holds: hold_records.iter().map(|(digest, _)| *digest).collect(),
                };
                let bytes = record::encode(SET, &set, wire::LIMIT)?;
                let digest = record::digest(&bytes);
                if basis.selected.is_some_and(|selected| selected != digest) {
                    return Err(CatalogError::Conflict("transfer child archive set changed").into());
                }

                let mut retained = Vec::with_capacity(children.len());
                for (index, child) in children.into_iter().enumerate() {
                    let registry = self.open_group_archive(domain, pool, &child.birth).await?;
                    let verified = registry
                        .verify_hold(domain, &child.hold, tails, child.receiver)
                        .await?;
                    basis.check_marker(index, verified.marker())?;
                    retained.push((registry, child.hold));
                }
                self.bounded(async {
                    for (id, hold) in hold_records {
                        self.immutable(HOLDS, id, &hold).await?;
                    }
                    self.immutable(SETS, digest, &bytes).await
                })
                .await?;
                for (registry, hold) in retained {
                    registry.require_retained_hold(domain, &hold).await?;
                }
                self.record_verified_child_archives(
                    plan,
                    basis.publication,
                    basis.activations,
                    digest,
                )
                .await
                .map_err(Into::into)
            }))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    /// Reopen the exact selected child holds for recovery or retirement.
    /// Physical replay and registry liveness still require their own checks.
    pub async fn retirement_child_holds(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Option<Vec<CellGroupArchiveHold>>, CatalogError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(plan)?;
        self.bounded(async {
            let basis = self.child_archive_basis(plan).await?;
            let Some(digest) = basis.selected else {
                return Ok(None);
            };
            let bytes = self.read_child_archive_record(SETS, digest).await?;
            let set: ChildArchiveSet = record::decode(SET, &bytes, wire::LIMIT)?;
            if set.namespace != self.namespace.root_digest
                || set.plan != plan.digest()
                || set.publication != basis.publication
                || set.activations != basis.activations
                || set.holds.len() != basis.installed.len()
            {
                return Err(CatalogError::Corrupt("transfer child archive set binding"));
            }
            let mut holds = Vec::with_capacity(set.holds.len());
            for (index, id) in set.holds.into_iter().enumerate() {
                let bytes = self.read_child_archive_record(HOLDS, id).await?;
                let hold: CellGroupArchiveHold = record::decode(HOLD, &bytes, wire::LIMIT)?;
                hold.check_reference()
                    .map_err(|_| CatalogError::Corrupt("transfer child archive hold"))?;
                basis.check_marker(index, hold.marker())?;
                holds.push(hold);
            }
            Ok(Some(holds))
        })
        .await
    }

    async fn child_archive_basis(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<ChildArchiveBasis, CatalogError> {
        let (view, _) = self.current().await?;
        let journal = self
            .matching_transition(&view.head, plan)
            .await?
            .filter(|journal| journal.phase() == TransitionPhase::Active)
            .ok_or(CatalogError::Fenced)?;
        let publication = self
            .confirmed(&view.head, plan)
            .await?
            .filter(|publication| publication.transfer_archive().is_some())
            .ok_or(CatalogError::Fenced)?
            .journal_digest();
        let activations = journal.activated_children().ok_or(CatalogError::Fenced)?;
        let installed = self
            .installed_transfer_children(plan)
            .await?
            .ok_or(CatalogError::Fenced)?;
        let applied = self
            .transfer_activations(plan)
            .await?
            .ok_or(CatalogError::Fenced)?;
        if installed.len() != plan.request.children.len() || applied.len() != installed.len() {
            return Err(CatalogError::Corrupt("transfer child archive basis"));
        }
        Ok(ChildArchiveBasis {
            publication,
            activations,
            installed,
            applied,
            selected: journal.child_archives(),
        })
    }

    async fn read_child_archive_record(
        &self,
        category: &str,
        digest: [u8; 32],
    ) -> Result<Vec<u8>, CatalogError> {
        let bytes = self
            .read(&format!("{category}/{}", hex::encode(digest)))
            .await?
            .ok_or(CatalogError::Corrupt("missing child archive record"))?;
        if record::digest(&bytes) != digest {
            return Err(CatalogError::Corrupt("child archive record digest"));
        }
        Ok(bytes)
    }
}
