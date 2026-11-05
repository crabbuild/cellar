// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed child pre-copy from independently replayed active source holds.
use super::transition_sources::TransitionSourceCut;
use super::*;
use crate::raft::{
    cell_groups::{
        CellGroupArchiveCoverage, CellGroupArchiveExecution, CellGroupArchiveHold,
        CellGroupArchivePool, CellGroupArchiveTailPool, CellGroupRecoveryPool,
        CellGroupTransferTarget, PreparedCellGroupTransferChild, StagedCellGroupPartition,
    },
    fleet_catalog::ManagedTransitionPermit,
};
use serde::{Deserialize, Serialize};

const KIND: u8 = 202;
const PREPARATIONS: &str = "transition-preparations";

/// Existing archive and restore owners, plus exact parent holds and dedicated
/// child stores in plan order. Retain the remote holds through replay.
pub struct CellGroupTransitionPreparationInputs {
    pub archives: CellGroupArchivePool,
    pub tails: CellGroupArchiveTailPool,
    pub restore: CellGroupRecoveryPool,
    pub sources: Vec<CellGroupTransitionSource>,
    pub targets: Vec<CellGroupTransferTarget>,
}

/// Replayed active source data and the complete synced, non-serving child
/// partition. A persisted preparation describes the copy. A replacement
/// controller must reopen the holds and independently rebuild its physical pins.
pub struct PreparedManagedCellGroupTransition {
    partition: StagedCellGroupPartition,
    sources: Vec<CellGroupArchiveCoverage>,
    journal: TransitionJournal,
}

impl PreparedManagedCellGroupTransition {
    pub fn journal(&self) -> &TransitionJournal {
        &self.journal
    }

    pub fn children(&self) -> &[PreparedCellGroupTransferChild] {
        &self.partition.children
    }

    pub fn sources(&self) -> &[CellGroupArchiveCoverage] {
        &self.sources
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    namespace: [u8; 32],
    plan: [u8; 32],
    sources: [u8; 32],
    children: Vec<Child>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Child {
    root: Option<[u8; 32]>,
    cells: u32,
}

impl RangeDirectory {
    pub(crate) async fn prepare_managed_transition_data(
        &self,
        domain: &impl CellGroupArchiveExecution,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
        inputs: CellGroupTransitionPreparationInputs,
    ) -> Result<PreparedManagedCellGroupTransition, CellGroupArchiveBindingError> {
        permit.check(self, plan)?;
        let verified = Box::pin(self.verify_transition_source_set(
            domain,
            &inputs.archives,
            &inputs.tails,
            plan,
            inputs.sources,
            TransitionSourceCut::Active,
        ))
        .await?;
        if let Some(expected) = verified.journal.preparation() {
            let selected = self
                .bounded(self.read_transition_preparation(plan, expected))
                .await?;
            if selected.sources != verified.digest {
                return Err(
                    CatalogError::Conflict("transition preparation sources changed").into(),
                );
            }
        }
        let partition = Box::pin(inputs.restore.stage_partition(
            domain,
            verified.sources.clone(),
            verified.children,
            inputs.targets,
            None,
        ))
        .await?;
        let record = Preparation {
            namespace: self.namespace.root_digest,
            plan: plan.digest,
            sources: verified.digest,
            children: partition
                .children
                .iter()
                .map(|child| Child {
                    root: child.root().digest(),
                    cells: child.root().cells(),
                })
                .collect(),
        };
        let bytes = record::encode(KIND, &record, wire::LIMIT)?;
        let digest = record::digest(&bytes);
        self.bounded(self.immutable(PREPARATIONS, digest, &bytes))
            .await?;
        let journal = self
            .record_managed_preparation(plan, permit, digest)
            .await?;
        Ok(PreparedManagedCellGroupTransition {
            partition,
            sources: verified.sources,
            journal,
        })
    }

    /// Reopen the exact active-source holds referenced by a managed preparation.
    /// Replaying them into fresh child stores reestablishes physical readiness.
    pub async fn managed_preparation_holds(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Vec<CellGroupArchiveHold>, CatalogError> {
        if self.namespace_config().managed_fleet.is_none() {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        self.check_transition_plan(plan)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            if journal.phase() == TransitionPhase::Cancelled {
                return Err(CatalogError::Fenced);
            }
            let digest = journal.preparation().ok_or(CatalogError::Fenced)?;
            let record = self.read_transition_preparation(plan, digest).await?;
            self.read_transition_source_holds(
                plan,
                &journal,
                record.sources,
                TransitionSourceCut::Active,
            )
            .await
        })
        .await
    }

    async fn read_transition_preparation(
        &self,
        plan: &DirectoryMutation,
        digest: [u8; 32],
    ) -> Result<Preparation, CatalogError> {
        let bytes = self
            .read(&format!("{PREPARATIONS}/{}", hex::encode(digest)))
            .await?
            .ok_or(CatalogError::Corrupt("missing transition preparation"))?;
        if record::digest(&bytes) != digest {
            return Err(CatalogError::Corrupt("transition preparation digest"));
        }
        let decoded: Preparation = record::decode(KIND, &bytes, wire::LIMIT)?;
        if decoded.namespace != self.namespace.root_digest
            || decoded.plan != plan.digest
            || decoded.sources == [0; 32]
            || decoded.children.len() != plan.request.children.len()
            || decoded.children.iter().any(|child| {
                (child.cells == 0) != child.root.is_none() || child.root == Some([0; 32])
            })
        {
            return Err(CatalogError::Corrupt("transition preparation binding"));
        }
        Ok(decoded)
    }
}
