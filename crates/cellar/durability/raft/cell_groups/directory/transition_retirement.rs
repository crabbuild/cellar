// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
};
use crate::raft::{
    cache::GroupLog,
    cell_groups::{GroupManifest, GroupSourceRetirement},
    completion::RuntimeError,
    storage::ReplicaLog,
};
use cellar_logic::consensus::EngineInstanceId;

/// One sealed parent and the exact replacement certificate, bound to its
/// existing voter engine. A permit does not authorize releasing any holds.
pub struct CellGroupSourceRetirementPermit {
    group: GroupLog<GroupHistory>,
    engine: EngineInstanceId,
    sealed: GroupManifest,
    publication: [u8; 32],
    deadline: u64,
    entry: Vec<u8>,
}

impl CellGroupSourceRetirementPermit {
    pub(crate) fn history(&self) -> GroupHistory {
        self.group.identity().scope
    }

    pub(crate) fn check(
        &self,
        log: &ReplicaLog,
        group: &GroupLog<GroupHistory>,
    ) -> Result<(), RuntimeError> {
        if !Arc::ptr_eq(&group.owner, &self.group.owner)
            || group.identity() != self.group.identity()
            || log.storage_instance()? != self.engine
            || log.domain().mono_ms().map_err(|_| RuntimeError::Closed)? >= self.deadline
        {
            return Err(RuntimeError::StaleCompletion);
        }
        drop(group.read()?);
        Ok(())
    }

    pub(crate) fn applied<'a>(
        &self,
        marker: &'a GroupManifest,
    ) -> Result<Option<&'a GroupSourceRetirement>, RuntimeError> {
        if marker.route() != self.sealed.route() || !marker.preserves_source_seal(&self.sealed) {
            return Err(RuntimeError::StaleCompletion);
        }
        let retired = marker.source_seal().and_then(|seal| seal.retirement());
        if retired.is_some_and(|retired| retired.publication() != self.publication) {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(retired)
    }

    pub(crate) fn into_entry(self) -> Vec<u8> {
        self.entry
    }
}

impl RangeDirectory {
    /// Admit the exact source tombstone only after every child activation has
    /// been selected. The source remains sealed while the entry is pending.
    pub async fn prepare_source_retirement(
        &self,
        plan: &DirectoryMutation,
        source: GroupHistory,
        log: &ReplicaLog,
    ) -> Result<CellGroupSourceRetirementPermit, CatalogError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(plan)?;
        self.bounded(async {
            self.namespace.require_cell_group_unmanaged().await?;
            self.transfer_activations(plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let publication = self
                .confirmed_transfer_publication(plan)
                .await?
                .ok_or(CatalogError::Fenced)?
                .journal_digest();
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .filter(|journal| journal.phase() == TransitionPhase::Active)
                .ok_or(CatalogError::Fenced)?;
            let held = self
                .read_transition_source_holds(
                    plan,
                    &journal,
                    journal.sealed_sources().ok_or(CatalogError::Fenced)?,
                    super::transition_sources::TransitionSourceCut::Sealed,
                )
                .await?;
            let sealed = held
                .iter()
                .find(|hold| hold.marker().route().history == source)
                .ok_or(CatalogError::Fenced)?
                .marker();
            let route = sealed.route();
            let group = log.cell_group(&source).map_err(CatalogError::Log)?;
            let marker = group
                .group_manifest()
                .map_err(CatalogError::Log)?
                .ok_or(CatalogError::LostStableState)?;
            if marker.route() != route || !marker.preserves_source_seal(sealed) {
                return Err(CatalogError::Fenced);
            }
            super::bootstrap::check_member(&group)?;
            let engine = log.storage_instance().map_err(CatalogError::Log)?;
            let identity = group.identity();
            self.namespace
                .verify_node_storage_identity(identity.node, identity.disk, engine)
                .await?;
            let transition = TransitionId::new(plan.operation().bytes())
                .map_err(|_| CatalogError::Invalid("transfer operation"))?;
            let entry = CellGroupEntry {
                binding: GroupEntryBinding {
                    group: route.history,
                    route_epoch: route.epoch,
                    birth_digest: route.birth_digest,
                    schema_digest: route.schema_digest,
                    proposal: plan.operation(),
                },
                command: GroupCommand::Retire {
                    transition,
                    publication,
                },
            }
            .encode(route, GroupEntryLimits::default())
            .map_err(|_| CatalogError::Invalid("source retirement entry"))?;
            let deadline = log
                .domain()
                .mono_ms()
                .map_err(|_| CatalogError::Unconfirmed)?
                .checked_add(self.limits.operation_timeout_ms)
                .ok_or(CatalogError::Limit)?;
            Ok(CellGroupSourceRetirementPermit {
                group,
                engine,
                sealed: sealed.clone(),
                publication,
                deadline,
                entry,
            })
        })
        .await
    }
}
