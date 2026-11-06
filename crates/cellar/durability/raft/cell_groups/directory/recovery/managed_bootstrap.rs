// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Attach the exact retained seed under the selected managed recovery lock.
use super::*;
use crate::raft::{
    cache::GroupLog,
    cell_groups::GroupManifest,
    fleet_catalog::{
        FleetHistory, FleetHistoryState, FleetHostAuthorization, FleetPublishedRecoveryVoter,
        FleetRecoveryAttachRequest,
    },
    member_state::FleetGroupBinding,
    storage::{LogError, ReplicaLog},
};
use cellar_logic::consensus::{EngineInstanceId, MemberId};

pub(in crate::raft::catalog::range_directory) struct ManagedRecoveryStartup {
    pub(in crate::raft::catalog::range_directory) request: FleetRecoveryAttachRequest,
    pub(in crate::raft::catalog::range_directory) host: FleetHostAuthorization,
    pub(in crate::raft::catalog::range_directory) deadline: u64,
}

impl RangeDirectory {
    /// A retained seed can keep its verified images on boot without starting
    /// Raft. Publication and attachment still require independent Fleet authority.
    pub(crate) async fn require_parked_managed_recovery(
        &self,
        group: &GroupLog<GroupHistory>,
        marker: &GroupManifest,
        history: &FleetHistory,
        engine: EngineInstanceId,
    ) -> Result<(), CatalogError> {
        self.bounded(async {
            let local = group.identity();
            let route = marker.route();
            let plan = marker.recovery_request().ok_or(CatalogError::Fenced)?;
            if history.state() != &FleetHistoryState::Reserved
                || !history.allows_managed_genesis()
                || history.birth_digest() != route.birth_digest
                || !history.binding().matches_scope(&local.scope)
            {
                return Err(CatalogError::Fenced);
            }
            let (view, _) = self.current().await?;
            let lock = view.head.recovery.as_ref().ok_or(CatalogError::Fenced)?;
            let publication = match &lock.phase {
                RecoveryPhase::ManagedSource { .. } if !lock.blocks_startup(self, route)? => None,
                RecoveryPhase::ManagedPublished {
                    replacement,
                    publication,
                    ..
                } if *replacement == plan && lock.blocks_startup(self, route)? => {
                    self.selected_route_in(&view, route).await?;
                    Some(*publication)
                }
                _ => return Err(CatalogError::Fenced),
            };
            let (birth, origin) = self.birth_reservation(route).await?;
            if origin != Some(plan)
                || !birth
                    .voters()
                    .iter()
                    .any(|voter| voter.node == local.node && voter.member == local.member)
            {
                return Err(CatalogError::Fenced);
            }
            let (genesis, commitment) = self
                .require_retained_managed_seed_intent(group, route, plan, history.binding(), engine)
                .await?;
            check_managed_recovery_member(
                group,
                marker,
                &genesis,
                (commitment, publication),
                local.member,
                history.binding(),
            )
        })
        .await
    }

    pub(crate) fn bootstrap_managed_pending_recovery_voter<'a>(
        &'a self,
        request: &'a FleetRecoveryAttachRequest,
        history: GroupHistory,
        log: &'a ReplicaLog,
        host: &'a FleetHostAuthorization,
        deadline: u64,
    ) -> futures_util::future::BoxFuture<'a, Result<CellGroupBootstrapPermit, CatalogError>> {
        Box::pin(self.bounded(async move {
            let (group, authority) = self
                .verify_published_managed_recovery_member(request, history, log, host, deadline)
                .await?;
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

    pub(super) async fn verify_published_managed_recovery_member(
        &self,
        request: &FleetRecoveryAttachRequest,
        history: GroupHistory,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
        deadline: u64,
    ) -> Result<(GroupLog<GroupHistory>, FleetPublishedRecoveryVoter), CatalogError> {
        let now = log
            .domain()
            .mono_ms()
            .map_err(|_| CatalogError::Runtime(crate::raft::completion::RuntimeError::Closed))?;
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
        self.namespace
            .verify_node_storage_identity(
                log.limits().node,
                log.limits().disk,
                log.storage_instance().map_err(CatalogError::Log)?,
            )
            .await?;
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
        check_managed_recovery_member(
            &group,
            &marker,
            &genesis,
            (
                authority.seeds.genesis_commitment(),
                Some(authority.publication.journal_digest()),
            ),
            voter.member,
            &authority.group,
        )?;
        Ok((group, authority))
    }
}

fn check_managed_recovery_member(
    group: &GroupLog<GroupHistory>,
    marker: &GroupManifest,
    genesis: &GroupManifest,
    expected: ([u8; 32], Option<[u8; 32]>),
    member: MemberId,
    binding: &FleetGroupBinding,
) -> Result<(), CatalogError> {
    let (commitment, publication) = expected;
    if group.identity().member != member
        || group
            .recovery_genesis()
            .map_err(CatalogError::Log)?
            .as_deref()
            != Some(genesis)
        || marker.route() != genesis.route()
        || marker.recovery_request() != genesis.recovery_request()
        || marker
            .recovery_activation()
            .is_some_and(|(observed, _, _)| Some(observed) != publication)
        || marker
            .recovery_commitment()
            .map_err(|_| CatalogError::Corrupt("managed recovery lineage"))?
            != Some(commitment)
        || group
            .membership()
            .map_err(CatalogError::Log)?
            .fleet_status()
            .is_none_or(|status| &status.binding != binding)
    {
        return Err(CatalogError::LostStableState);
    }
    super::super::bootstrap::check_member(group)
}
