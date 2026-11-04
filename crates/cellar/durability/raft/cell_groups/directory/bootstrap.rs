// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Member admission after real genesis and a permanent physical claim.

use super::*;
use crate::raft::{
    cache::GroupLog,
    catalog::DisasterFenceAuthority,
    storage::{CellGroupBootstrap, LogError, ReplicaLog},
};
use cellar_logic::consensus::MemberId;
use serde::{Deserialize, Serialize};

#[path = "joined_bootstrap.rs"]
mod joined;

/// Durable member startup permission, not quorum or application-serving authority.
/// The shared runtime must verify the complete selected image/manifest closure
/// and revalidate this permit before attachment. It must obtain current-term
/// quorum authority separately before accepting application work.
pub struct CellGroupBootstrapPermit {
    group: GroupLog<GroupHistory>,
    birth: CellGroupBirth,
    directory: RangeDirectory,
    log: ReplicaLog,
    origin: BootstrapOrigin,
}

enum BootstrapOrigin {
    Initial,
    Managed(Box<crate::raft::fleet_catalog::FleetHostAuthorization>),
    Joined,
    ManagedJoined(Box<crate::raft::fleet_catalog::FleetHostAuthorization>),
    Recovery(Box<recovery::RecoveryStartup>),
}

impl CellGroupBootstrapPermit {
    pub(in crate::raft::catalog::range_directory) fn recovered(
        group: GroupLog<GroupHistory>,
        birth: CellGroupBirth,
        directory: RangeDirectory,
        log: ReplicaLog,
        startup: recovery::RecoveryStartup,
    ) -> Self {
        Self {
            group,
            birth,
            directory,
            log,
            origin: BootstrapOrigin::Recovery(Box::new(startup)),
        }
    }

    pub(crate) fn log(&self) -> &ReplicaLog {
        &self.log
    }

    pub fn group(&self) -> &GroupLog<GroupHistory> {
        &self.group
    }

    pub fn route(&self) -> &GroupRoute {
        self.birth.route()
    }

    pub(crate) fn recovery_request(&self) -> Option<&Arc<CellGroupRecoveryRequest>> {
        match &self.origin {
            BootstrapOrigin::Recovery(startup) => Some(&startup.request),
            _ => None,
        }
    }

    pub(crate) fn recovery_authority(
        &self,
    ) -> Option<&Arc<dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>>> {
        match &self.origin {
            BootstrapOrigin::Recovery(startup) => Some(&startup.authority),
            _ => None,
        }
    }

    pub async fn revalidate(&self) -> Result<(), CatalogError> {
        match &self.origin {
            BootstrapOrigin::Initial => self
                .directory
                .bootstrap_group_member(&self.birth, &self.log)
                .await
                .map(|_| ()),
            BootstrapOrigin::Managed(host) => self
                .directory
                .bootstrap_managed_group_member(&self.birth, &self.log, host)
                .await
                .map(|_| ()),
            BootstrapOrigin::Joined => self
                .directory
                .bootstrap_joined_group_member(&self.birth, self.group.identity().member, &self.log)
                .await
                .map(|_| ()),
            BootstrapOrigin::ManagedJoined(host) => self
                .directory
                .bootstrap_managed_joined_group_member(
                    &self.birth,
                    self.group.identity().member,
                    &self.log,
                    host,
                )
                .await
                .map(|_| ()),
            BootstrapOrigin::Recovery(startup) => self
                .directory
                .bootstrap_recovered_group_member(
                    &startup.request,
                    &self.log,
                    startup.authority.clone(),
                )
                .await
                .map(|_| ()),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    namespace: [u8; 32],
    group: [u8; 16],
    incarnation: [u8; 16],
    birth: [u8; 32],
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
    engine: [u8; 16],
}

pub(super) fn check_member(group: &GroupLog<GroupHistory>) -> Result<(), CatalogError> {
    let cache = group.read().map_err(CatalogError::Log)?;
    if !crate::raft::member_state::members(&cache.conf_state)
        .contains(&group.identity().member.get())
        || !cache.membership.permits_group(group.identity())
    {
        return Err(CatalogError::Conflict("group member is no longer admitted"));
    }
    Ok(())
}

impl RangeDirectory {
    pub(crate) async fn verify_group_import_member_claim(
        &self,
        birth: &CellGroupBirth,
        member: MemberId,
        host: &crate::raft::fleet_catalog::FleetHostAuthorization,
    ) -> Result<(), CatalogError> {
        let history = birth.route().history;
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(&self.group_claim_key(history, member), wire::LIMIT)
            .await?
            .ok_or(CatalogError::Conflict("missing group import member claim"))?
            .0;
        let claim: Claim = record::decode(wire::GROUP_CLAIM, &bytes, wire::LIMIT)?;
        if claim.namespace != self.namespace.root_digest
            || claim.group != history.group.bytes()
            || claim.incarnation != history.incarnation.bytes()
            || claim.birth != birth.route().birth_digest
            || claim.member != member.get()
            || claim.node != host.node().bytes()
            || claim.disk != host.disk().bytes()
            || Some(claim.engine) != host.storage_instance().map(|id| id.bytes())
        {
            return Err(CatalogError::Conflict("group import member claim identity"));
        }
        Ok(())
    }

    fn group_claim_key(&self, history: GroupHistory, member: MemberId) -> Path {
        self.key(&format!(
            "claims/{}/{}/{:016x}",
            hex::encode(history.group.bytes()),
            hex::encode(history.incarnation.bytes()),
            member.get()
        ))
    }

    /// Initialize or resume an initial member using its exact immutable birth.
    /// Unknown outcomes retain the same history, disk and engine identities.
    /// Managed admission remains closed until rooted fleet authorization exists.
    pub async fn bootstrap_group_member(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
    ) -> Result<CellGroupBootstrapPermit, CatalogError> {
        self.unmanaged_birth()?;
        self.bounded(async {
            self.namespace.require_cell_group_unmanaged().await?;
            self.bootstrap_initial_group_member(birth, log, None).await
        })
        .await
    }

    /// Recheck the exact running node/engine boot and the rooted Live group on
    /// every startup and attachment. A saved birth or binding is insufficient.
    pub async fn bootstrap_managed_group_member(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        host: &crate::raft::fleet_catalog::FleetHostAuthorization,
    ) -> Result<CellGroupBootstrapPermit, CatalogError> {
        self.bounded(async {
            match log.cell_group(&birth.route().history) {
                Ok(_) => {
                    self.require_managed_retained_group_boot(birth, log, host)
                        .await?;
                }
                Err(LogError::NotFound) => {
                    self.require_managed_group_boot(log, host).await?;
                }
                Err(error) => return Err(CatalogError::Log(error)),
            }
            self.bootstrap_initial_group_member(birth, log, Some(host))
                .await
        })
        .await
    }

    pub(super) async fn require_managed_group_boot(
        &self,
        log: &ReplicaLog,
        host: &crate::raft::fleet_catalog::FleetHostAuthorization,
    ) -> Result<crate::raft::fleet_stop::BootBinding, CatalogError> {
        let boot = self.require_managed_boot_identity(log, host).await?;
        if log
            .lifecycle_guard()
            .map_err(CatalogError::Log)?
            .is_none_or(|guard| !guard.admits_group_start(&boot))
        {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(boot)
    }

    pub(super) async fn require_managed_retained_group_boot(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        host: &crate::raft::fleet_catalog::FleetHostAuthorization,
    ) -> Result<crate::raft::fleet_stop::BootBinding, CatalogError> {
        let boot = self.require_managed_boot_identity(log, host).await?;
        if log
            .lifecycle_guard()
            .map_err(CatalogError::Log)?
            .is_some_and(|guard| guard.admits_group_start(&boot))
        {
            return Ok(boot);
        }
        let guard = log
            .lifecycle_guard()
            .map_err(CatalogError::Log)?
            .ok_or(CatalogError::ManagedAdmissionRequired)?;
        let selector = guard
            .quiescing()
            .ok_or(CatalogError::ManagedAdmissionRequired)?
            .clone();
        let group = log
            .cell_group(&birth.route().history)
            .map_err(CatalogError::Log)?;
        host.require_retained_group_start(
            &selector,
            group.identity(),
            Duration::from_millis(self.limits.operation_timeout_ms),
        )
        .await
        .map_err(CatalogError::from)?;
        let current = log
            .lifecycle_guard()
            .map_err(CatalogError::Log)?
            .ok_or(CatalogError::Fenced)?;
        if current.boot != boot || current.quiescing() != Some(&selector) {
            return Err(CatalogError::Fenced);
        }
        Ok(boot)
    }

    async fn require_managed_boot_identity(
        &self,
        log: &ReplicaLog,
        host: &crate::raft::fleet_catalog::FleetHostAuthorization,
    ) -> Result<crate::raft::fleet_stop::BootBinding, CatalogError> {
        let owner = self
            .namespace
            .cell_group_owner()
            .await?
            .ok_or(CatalogError::ManagedAdmissionRequired)?;
        let boot = host.boot_binding().map_err(CatalogError::from)?;
        if owner.fleet != host.fleet()
            || owner.catalog != boot.catalog
            || host.cluster() != self.namespace.config.cluster
            || host.node() != log.limits().node
            || host.disk() != log.limits().disk
            || host.storage_instance() != Some(log.storage_instance().map_err(CatalogError::Log)?)
            || boot.process != log.limits().process.bytes()
            || log
                .lifecycle_guard()
                .map_err(CatalogError::Log)?
                .is_none_or(|guard| guard.boot != boot)
        {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(boot)
    }

    async fn bootstrap_initial_group_member(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        host: Option<&crate::raft::fleet_catalog::FleetHostAuthorization>,
    ) -> Result<CellGroupBootstrapPermit, CatalogError> {
        self.bounded(async {
            let authorization = if let Some(host) = host {
                let authorization = host
                    .group_authorization(
                        &birth.route().history,
                        Duration::from_millis(self.limits.operation_timeout_ms),
                    )
                    .await
                    .map_err(CatalogError::from)?;
                if !authorization.check_birth(self, birth) {
                    return Err(CatalogError::ManagedAdmissionRequired);
                }
                Some(authorization)
            } else {
                None
            };
            self.require_current_group_birth(birth).await?;
            self.require_ordinary_birth_reservation(birth.route())
                .await?;
            let selected = birth
                .voters()
                .iter()
                .chain(birth.learners())
                .find(|member| member.node == log.limits().node)
                .ok_or(CatalogError::NotInitialMember)?;
            // This permanent node pin refuses a recreated engine even before a
            // member claim has been written. Keep the existing pin protocol.
            let _node = self.namespace.require_node_storage(log).await?;
            let history = birth.route().history;
            let expected = record::encode(
                wire::GROUP_CLAIM,
                &Claim {
                    namespace: self.namespace.root_digest,
                    group: history.group.bytes(),
                    incarnation: history.incarnation.bytes(),
                    birth: birth.route().birth_digest,
                    member: selected.member.get(),
                    node: selected.node.bytes(),
                    disk: log.limits().disk.bytes(),
                    engine: log.storage_instance().map_err(CatalogError::Log)?.bytes(),
                },
                wire::LIMIT,
            )?;
            let key = self.group_claim_key(history, selected.member);
            let remote = self
                .namespace
                .inner
                .read_versioned_bounded(&key, wire::LIMIT)
                .await?;
            if remote.as_ref().is_some_and(|(bytes, _)| bytes != &expected) {
                return Err(CatalogError::Conflict("permanent group member claim"));
            }
            let local = match log.cell_group(&history) {
                Ok(group) => Some(group),
                Err(LogError::NotFound) => None,
                Err(error) => return Err(CatalogError::Log(error)),
            };
            if remote.is_some() && local.is_none() {
                return Err(CatalogError::LostStableState);
            }
            if local.is_none()
                && authorization
                    .as_ref()
                    .is_some_and(|authority| !authority.allows_managed_genesis())
            {
                return Err(CatalogError::ManagedAdmissionRequired);
            }
            // The worker creates the complete empty root with genesis and only
            // returns after actual engine sync. Existing mappings must match
            // the original route, membership and birth, even after later apply.
            let bootstrap = CellGroupBootstrap {
                route: birth.route().clone(),
                member: selected.member,
                voters: birth.voters().iter().map(|m| m.member).collect(),
                learners: birth.learners().iter().map(|m| m.member).collect(),
            };
            let paused_retained = if let Some(host) = host {
                let boot = host.boot_binding().map_err(CatalogError::from)?;
                let guard = log
                    .lifecycle_guard()
                    .map_err(CatalogError::Log)?
                    .ok_or(CatalogError::ManagedAdmissionRequired)?;
                !guard.admits_group_start(&boot)
            } else {
                false
            };
            let group = if paused_retained {
                // A returned paused boot only reattaches retained state. It
                // cannot complete genesis or create a missing permanent claim.
                if remote.is_none() {
                    return Err(CatalogError::LostStableState);
                }
                let host = host.ok_or(CatalogError::ManagedAdmissionRequired)?;
                self.require_managed_retained_group_boot(birth, log, host)
                    .await?;
                let group = local.ok_or(CatalogError::LostStableState)?;
                {
                    let cache = group.read().map_err(CatalogError::Log)?;
                    let authorization = authorization
                        .as_ref()
                        .ok_or(CatalogError::ManagedAdmissionRequired)?;
                    if group.identity().member != selected.member
                        || cache
                            .group_manifest
                            .as_ref()
                            .is_none_or(|root| root.route() != birth.route())
                        || cache
                            .membership
                            .fleet_status()
                            .is_none_or(|status| &status.binding != authorization.binding())
                        || !authorization.matches_replica(group.identity(), &cache.membership)
                    {
                        return Err(CatalogError::ManagedAdmissionRequired);
                    }
                }
                group
            } else {
                match &authorization {
                    Some(authorization) => {
                        let boot = host
                            .ok_or(CatalogError::ManagedAdmissionRequired)?
                            .boot_binding()
                            .map_err(CatalogError::from)?;
                        log.create_managed_cell_group(bootstrap, authorization, &boot)
                            .await
                    }
                    None => log.create_cell_group(bootstrap).await,
                }
                .map_err(CatalogError::Log)?
            };
            check_member(&group)?;
            let observed = if paused_retained {
                // Reobserve without a conditional create: a missing claim
                // during return must refuse, including after the earlier read.
                self.namespace
                    .inner
                    .read_versioned_bounded(&key, wire::LIMIT)
                    .await?
                    .ok_or(CatalogError::LostStableState)?
                    .0
            } else {
                self.namespace
                    .inner
                    .create_and_observe_bounded(&key, &expected, wire::LIMIT)
                    .await?
            };
            if observed != expected {
                return Err(CatalogError::Conflict("permanent group member claim"));
            }
            self.require_current_group_birth(birth).await?;
            check_member(&group)?;
            if let Some(host) = host {
                self.require_managed_retained_group_boot(birth, log, host)
                    .await?;
            }
            Ok(CellGroupBootstrapPermit {
                group,
                birth: birth.clone(),
                directory: self.clone(),
                log: log.clone(),
                origin: match host {
                    Some(host) => BootstrapOrigin::Managed(Box::new(host.clone())),
                    None => BootstrapOrigin::Initial,
                },
            })
        })
        .await
    }
}
