// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
};
use crate::raft::{
    cache::GroupLog, cell_groups::GroupManifest, completion::RuntimeError, storage::ReplicaLog,
};
use cellar_logic::consensus::{EngineInstanceId, LogPosition};

/// Exact selected publication and seed, bound to one open voter engine.
pub struct CellGroupTransferActivationPermit {
    group: GroupLog<GroupHistory>,
    engine: EngineInstanceId,
    publication: [u8; 32],
    manifest: [u8; 32],
    deadline: u64,
    entry: Vec<u8>,
}

impl CellGroupTransferActivationPermit {
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

    pub(crate) fn applied_position(
        &self,
        marker: &GroupManifest,
    ) -> Result<Option<LogPosition>, RuntimeError> {
        if marker.route().history != self.history()
            || marker
                .transfer_commitment()
                .map_err(|e| RuntimeError::Invalid(e.to_string()))?
                != Some(self.manifest)
        {
            return Err(RuntimeError::StaleCompletion);
        }
        match marker.transfer_activation() {
            Some((publication, manifest, position))
                if publication == self.publication && manifest == self.manifest =>
            {
                Ok(Some(position))
            }
            Some(_) => Err(RuntimeError::StaleCompletion),
            None => Ok(None),
        }
    }

    pub(crate) fn into_entry(self) -> Vec<u8> {
        self.entry
    }
}

impl RangeDirectory {
    /// Bind the child voter and its retained seed to the selected publication.
    /// Admission requires a separate current-term barrier and committed apply.
    pub async fn prepare_transfer_activation(
        &self,
        plan: &DirectoryMutation,
        history: GroupHistory,
        log: &ReplicaLog,
    ) -> Result<CellGroupTransferActivationPermit, CatalogError> {
        self.unmanaged_birth()?;
        let bootstrap = self
            .bootstrap_pending_transfer_voter(plan, history, log)
            .await?;
        self.bounded(async {
            let publication = self
                .publication(plan)
                .await?
                .filter(|publication| publication.transfer_archive().is_some())
                .ok_or(CatalogError::Fenced)?;
            let route = bootstrap.route();
            let group = bootstrap.group().clone();
            let marker = group
                .group_manifest()
                .map_err(CatalogError::Log)?
                .ok_or(CatalogError::LostStableState)?;
            let manifest = marker
                .transfer_commitment()
                .map_err(|_| CatalogError::Corrupt("transfer seed commitment"))?
                .ok_or(CatalogError::Fenced)?;
            let transition = TransitionId::new(plan.operation().bytes())
                .map_err(|_| CatalogError::Invalid("transfer operation"))?;
            let publication = publication.journal_digest();
            let entry = CellGroupEntry {
                binding: GroupEntryBinding {
                    group: route.history,
                    route_epoch: route.epoch,
                    birth_digest: route.birth_digest,
                    schema_digest: route.schema_digest,
                    proposal: plan.operation(),
                },
                command: GroupCommand::Activate {
                    transition,
                    publication,
                    manifest,
                },
            }
            .encode(route, GroupEntryLimits::default())
            .map_err(|_| CatalogError::Invalid("transfer activation entry"))?;
            let deadline = log
                .domain()
                .mono_ms()
                .map_err(|_| CatalogError::Unconfirmed)?
                .checked_add(self.limits.operation_timeout_ms)
                .ok_or(CatalogError::Limit)?;
            Ok(CellGroupTransferActivationPermit {
                group,
                engine: log.storage_instance().map_err(CatalogError::Log)?,
                publication,
                manifest,
                deadline,
                entry,
            })
        })
        .await
    }
}
