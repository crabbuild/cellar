// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Attach the exact retained seed under the selected managed recovery lock.
use super::*;
use crate::raft::{
    fleet_catalog::{FleetHostAuthorization, FleetRecoveryAttachRequest},
    storage::{LogError, ReplicaLog},
};

pub(in crate::raft::catalog::range_directory) struct ManagedRecoveryStartup {
    pub(in crate::raft::catalog::range_directory) request: FleetRecoveryAttachRequest,
    pub(in crate::raft::catalog::range_directory) host: FleetHostAuthorization,
    pub(in crate::raft::catalog::range_directory) deadline: u64,
}

impl RangeDirectory {
    pub(crate) fn bootstrap_managed_pending_recovery_voter<'a>(
        &'a self,
        request: &'a FleetRecoveryAttachRequest,
        history: GroupHistory,
        log: &'a ReplicaLog,
        host: &'a FleetHostAuthorization,
        deadline: u64,
    ) -> futures_util::future::BoxFuture<'a, Result<CellGroupBootstrapPermit, CatalogError>> {
        Box::pin(self.bounded(async move {
            let now = log.domain().mono_ms().map_err(|_| {
                CatalogError::Runtime(crate::raft::completion::RuntimeError::Closed)
            })?;
            let timeout = deadline
                .checked_sub(now)
                .filter(|remaining| *remaining > 0)
                .map(Duration::from_millis)
                .ok_or(CatalogError::Unconfirmed)?;
            let authority = host
                .authorize_published_recovery_voter(self, request, history, timeout)
                .await
                .map_err(CatalogError::from)?;
            let boot = self.require_managed_group_boot(log, host).await?;
            if boot != authority.current_boot {
                return Err(CatalogError::Fenced);
            }
            self.namespace.require_node_storage(log).await?;
            let voter = authority
                .birth
                .voters()
                .iter()
                .find(|voter| voter.node == log.limits().node)
                .ok_or(CatalogError::NotInitialMember)?;
            let genesis = self
                .require_published_managed_voter_seed(
                    &authority.birth,
                    request.plan_digest,
                    &authority.seeds,
                    voter.member,
                    &authority.seeded_boot,
                )
                .await?;
            let group = log.cell_group(&history).map_err(|error| match error {
                LogError::NotFound => CatalogError::LostStableState,
                error => CatalogError::Log(error),
            })?;
            let marker = group
                .group_manifest()
                .map_err(CatalogError::Log)?
                .ok_or(CatalogError::LostStableState)?;
            if group.identity().member != voter.member
                || group
                    .recovery_genesis()
                    .map_err(CatalogError::Log)?
                    .as_deref()
                    != Some(&genesis)
                || marker.route() != authority.birth.route()
                || marker.recovery_request() != Some(request.plan_digest)
                || marker
                    .recovery_commitment()
                    .map_err(|_| CatalogError::Corrupt("managed recovery lineage"))?
                    != Some(authority.seeds.genesis_commitment())
                || marker.is_active()
                || group
                    .membership()
                    .map_err(CatalogError::Log)?
                    .fleet_status()
                    .is_none_or(|status| status.binding != authority.group)
            {
                return Err(CatalogError::LostStableState);
            }
            super::super::bootstrap::check_member(&group)?;
            Ok(CellGroupBootstrapPermit::managed_pending_recovery(
                group,
                authority.birth,
                self.clone(),
                log.clone(),
                ManagedRecoveryStartup {
                    request: request.clone(),
                    host: host.clone(),
                    deadline,
                },
            ))
        }))
    }
}
