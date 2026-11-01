// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recovered startup never creates an empty mapping or a missing storage pin.
use super::*;
use crate::raft::{
    cache::GroupLog,
    storage::{LogError, ReplicaLog},
};

pub(in crate::raft::catalog::range_directory) struct RecoveryStartup {
    pub(in crate::raft::catalog::range_directory) request: Arc<CellGroupRecoveryRequest>,
    pub(in crate::raft::catalog::range_directory) authority:
        Arc<dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>>,
}

impl RangeDirectory {
    pub(super) async fn recovered_member(
        &self,
        request: &CellGroupRecoveryRequest,
        log: &ReplicaLog,
    ) -> Result<GroupLog<GroupHistory>, CatalogError> {
        let (_, binding, digest) = self.published_recovery(request).await?;
        let selected = request
            .replacement()
            .voters()
            .iter()
            .chain(request.replacement().learners())
            .find(|member| member.node == log.limits().node)
            .ok_or(CatalogError::NotInitialMember)?;
        let receipt = self
            .recovery_seed_receipt(request, selected, digest)
            .await?;
        receipt.check_log(log)?;
        let group = log
            .cell_group(&request.replacement().route().history)
            .map_err(|error| match error {
                LogError::NotFound => CatalogError::LostStableState,
                error => CatalogError::Log(error),
            })?;
        if group.identity().member != selected.member
            || group
                .recovery_genesis()
                .map_err(CatalogError::Log)?
                .as_deref()
                != Some(&binding.genesis)
        {
            return Err(CatalogError::LostStableState);
        }
        let marker = group
            .group_manifest()
            .map_err(CatalogError::Log)?
            .ok_or(CatalogError::LostStableState)?;
        if marker.route() != request.replacement().route()
            || marker.recovery_request() != Some(request.digest())
            || marker
                .recovery_commitment()
                .map_err(|_| CatalogError::Corrupt("recovery lineage"))?
                != Some(binding.commitment)
        {
            return Err(CatalogError::LostStableState);
        }
        super::super::bootstrap::check_member(&group)?;
        Ok(group)
    }

    /// Resume only the exact physically seeded member of the selected recovery.
    /// The owner must still verify all local bytes and establish a fresh-term
    /// quorum barrier; the pending lifecycle refuses every application request.
    pub fn bootstrap_recovered_group_member<'a>(
        &'a self,
        request: &'a CellGroupRecoveryRequest,
        log: &'a ReplicaLog,
        authority: Arc<dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>>,
    ) -> futures_util::future::BoxFuture<'a, Result<CellGroupBootstrapPermit, CatalogError>> {
        Box::pin(async move {
            let request = Arc::new(self.decode_group_recovery(request.encode())?);
            self.bounded(async {
                self.verify_recovery_fence(&request, authority.as_ref())
                    .await?;
                let group = self.recovered_member(&request, log).await?;
                let birth = self.materialize_group_birth(request.replacement()).await?;
                self.verify_recovery_fence(&request, authority.as_ref())
                    .await?;
                self.published_recovery(&request).await?;
                Ok(CellGroupBootstrapPermit::recovered(
                    group,
                    birth,
                    self.clone(),
                    log.clone(),
                    RecoveryStartup { request, authority },
                ))
            })
            .await
        })
    }
}
