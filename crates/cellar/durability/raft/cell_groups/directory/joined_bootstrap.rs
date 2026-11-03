// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A staging mapping never grants startup. Only a locally fenced authenticated
//! install plus the exact permanent remote claim can resume a joined member.
use super::*;
use crate::raft::{
    fleet_catalog::{FleetHostAuthorization, FleetPermitAuthorization},
    transport::CellGroupSeedGrant,
};

impl RangeDirectory {
    pub(crate) async fn stage_group_learner(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        grant: Arc<CellGroupSeedGrant>,
    ) -> Result<GroupLog<GroupHistory>, CatalogError> {
        self.unmanaged_birth()?;
        self.bounded(async {
            self.namespace.require_cell_group_unmanaged().await?;
            self.stage_joined_group(birth, log, grant, None).await
        })
        .await
    }

    pub(crate) async fn stage_managed_group_learner(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        grant: Arc<CellGroupSeedGrant>,
        host: &FleetHostAuthorization,
    ) -> Result<GroupLog<GroupHistory>, CatalogError> {
        self.stage_joined_group(birth, log, grant, Some(host)).await
    }

    /// The same active physical reservation must still name this boot. The
    /// returned capability does not substitute for the authenticated seed grant.
    pub(crate) async fn authorize_group_learner_seed(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        target: &cellar_logic::consensus::ReplicaIdentity<GroupHistory>,
        source: &cellar_logic::consensus::ReplicaIdentity<GroupHistory>,
        host: &FleetHostAuthorization,
        deadline: u64,
    ) -> Result<FleetPermitAuthorization, CatalogError> {
        let remaining = || -> Result<Duration, CatalogError> {
            let now = log.domain().mono_ms().map_err(|_| CatalogError::Fenced)?;
            deadline
                .checked_sub(now)
                .filter(|ms| *ms > 0)
                .map(|ms| Duration::from_millis(ms.min(self.limits.operation_timeout_ms)))
                .ok_or(CatalogError::Fenced)
        };
        crate::asyncrt::timeout(remaining()?, async {
            let boot = self.require_managed_group_boot(log, host).await?;
            let authorization = host
                .admitted_learner_from(&birth.route().history, source, remaining()?)
                .await
                .map_err(CatalogError::from)?;
            let group = host
                .group_authorization_at_least(
                    &birth.route().history,
                    authorization.minimum_trust_revision(),
                    remaining()?,
                )
                .await
                .map_err(CatalogError::from)?;
            let replacement = authorization
                .permit()
                .replacement()
                .map_err(CatalogError::Log)?;
            if !group.check_birth(self, birth)
                || !authorization.matches_scope(&target.scope)
                || target.scope != birth.route().history
                || authorization.target_boot() != Some(&boot)
                || replacement.member != target.member
                || replacement.node != target.node
                || replacement.disk != target.disk
                || target.node != log.limits().node
                || target.disk != log.limits().disk
                || target.process != log.limits().process
            {
                return Err(CatalogError::ManagedAdmissionRequired);
            }
            self.require_current_group_birth(birth).await?;
            remaining()?;
            self.require_managed_group_boot(log, host).await?;
            remaining()?;
            Ok(authorization)
        })
        .await
        .map_err(|_| CatalogError::Unconfirmed)?
    }

    async fn stage_joined_group(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        grant: Arc<CellGroupSeedGrant>,
        host: Option<&FleetHostAuthorization>,
    ) -> Result<GroupLog<GroupHistory>, CatalogError> {
        self.bounded(async {
            grant.check_log(log).map_err(CatalogError::Log)?;
            if let Some(host) = host {
                let current = self
                    .authorize_group_learner_seed(
                        birth,
                        log,
                        grant.local(),
                        grant.source(),
                        host,
                        grant.deadline(),
                    )
                    .await?;
                grant
                    .check_authorization(&current)
                    .map_err(CatalogError::Log)?;
            } else if grant.managed_binding().is_some() {
                return Err(CatalogError::ManagedAdmissionRequired);
            }
            self.require_current_group_birth(birth).await?;
            let _node = self.namespace.require_node_storage(log).await?;
            let member = grant.local().member;
            let bootstrap = CellGroupBootstrap {
                route: birth.route().clone(),
                member,
                voters: birth.voters().iter().map(|m| m.member).collect(),
                learners: birth.learners().iter().map(|m| m.member).collect(),
            };
            grant
                .initial_configuration(&bootstrap)
                .map_err(CatalogError::Log)?;
            let expected = self.joined_claim(birth, member, log)?;
            let key = self.group_claim_key(birth.route().history, member);
            let remote = self
                .namespace
                .inner
                .read_versioned_bounded(&key, wire::LIMIT)
                .await?;
            if remote.as_ref().is_some_and(|(bytes, _)| bytes != &expected) {
                return Err(CatalogError::Conflict("permanent group member claim"));
            }
            match log.cell_group(&birth.route().history) {
                Err(LogError::NotFound) if remote.is_some() => {
                    return Err(CatalogError::LostStableState)
                }
                Err(LogError::NotFound) | Ok(_) => (),
                Err(error) => return Err(CatalogError::Log(error)),
            }
            // Stage before publishing the remote claim. An interrupted operation
            // can reconcile this same engine; an existing claim can never reset it.
            let group = log
                .stage_cell_group(bootstrap, grant.clone())
                .await
                .map_err(CatalogError::Log)?;
            let observed = self
                .namespace
                .inner
                .create_and_observe_bounded(&key, &expected, wire::LIMIT)
                .await?;
            if observed != expected {
                return Err(CatalogError::Conflict("permanent group member claim"));
            }
            self.require_current_group_birth(birth).await?;
            grant.check_log(log).map_err(CatalogError::Log)?;
            if let Some(host) = host {
                let current = self
                    .authorize_group_learner_seed(
                        birth,
                        log,
                        grant.local(),
                        grant.source(),
                        host,
                        grant.deadline(),
                    )
                    .await?;
                grant
                    .check_authorization(&current)
                    .map_err(CatalogError::Log)?;
            }
            Ok(group)
        })
        .await
    }

    fn joined_claim(
        &self,
        birth: &CellGroupBirth,
        member: MemberId,
        log: &ReplicaLog,
    ) -> Result<Vec<u8>, CatalogError> {
        let history = birth.route().history;
        record::encode(
            wire::GROUP_CLAIM,
            &Claim {
                namespace: self.namespace.root_digest,
                group: history.group.bytes(),
                incarnation: history.incarnation.bytes(),
                birth: birth.route().birth_digest,
                member: member.get(),
                node: log.limits().node.bytes(),
                disk: log.limits().disk.bytes(),
                engine: log.storage_instance().map_err(CatalogError::Log)?.bytes(),
            },
            wire::LIMIT,
        )
    }

    /// Resume a previously installed joined member. A staged empty mapping, an
    /// imported snapshot or a recreated disk cannot substitute for the local
    /// authenticated-install receipt and permanent engine claim. The runtime
    /// still verifies the whole physical closure before attaching this permit.
    pub async fn bootstrap_joined_group_member(
        &self,
        birth: &CellGroupBirth,
        member: MemberId,
        log: &ReplicaLog,
    ) -> Result<CellGroupBootstrapPermit, CatalogError> {
        self.unmanaged_birth()?;
        self.bounded(async {
            self.namespace.require_cell_group_unmanaged().await?;
            self.bootstrap_joined_member(birth, member, log, None).await
        })
        .await
    }

    /// A retained install receipt and permanent engine claim are mandatory even
    /// after a repair completes. A saved active permit is not restart authority.
    pub async fn bootstrap_managed_joined_group_member(
        &self,
        birth: &CellGroupBirth,
        member: MemberId,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
    ) -> Result<CellGroupBootstrapPermit, CatalogError> {
        self.bootstrap_joined_member(birth, member, log, Some(host))
            .await
    }

    async fn bootstrap_joined_member(
        &self,
        birth: &CellGroupBirth,
        member: MemberId,
        log: &ReplicaLog,
        host: Option<&FleetHostAuthorization>,
    ) -> Result<CellGroupBootstrapPermit, CatalogError> {
        self.bounded(async {
            let authorization = if let Some(host) = host {
                self.require_managed_group_boot(log, host).await?;
                let group = host
                    .group_authorization(
                        &birth.route().history,
                        Duration::from_millis(self.limits.operation_timeout_ms),
                    )
                    .await
                    .map_err(CatalogError::from)?;
                if !group.check_birth(self, birth) {
                    return Err(CatalogError::ManagedAdmissionRequired);
                }
                Some(group)
            } else {
                None
            };
            self.require_current_group_birth(birth).await?;
            let _node = self.namespace.require_node_storage(log).await?;
            let group = log
                .cell_group(&birth.route().history)
                .map_err(CatalogError::Log)?;
            let check = || -> Result<(), CatalogError> {
                let cache = group.read().map_err(CatalogError::Log)?;
                let joined = cache.membership.joined(member).map_err(CatalogError::Log)?;
                if group.identity().member != member
                    || cache
                        .group_manifest
                        .as_ref()
                        .is_none_or(|m| m.route() != birth.route())
                    || joined.is_none_or(|(binding, at)| {
                        binding.node != log.limits().node
                            || binding.disk != log.limits().disk
                            || cache.group_seed.is_none_or(|seed| seed < at.get())
                    })
                {
                    return Err(CatalogError::Conflict(
                        "joined group has no durable seed receipt",
                    ));
                }
                match (&authorization, cache.membership.fleet_status()) {
                    (Some(authorization), Some(status))
                        if &status.binding == authorization.binding()
                            && authorization
                                .matches_replica(group.identity(), &cache.membership) =>
                    {
                        ()
                    }
                    (None, None) => (),
                    _ => return Err(CatalogError::ManagedAdmissionRequired),
                }
                drop(cache);
                check_member(&group)
            };
            check()?;
            let key = self.group_claim_key(birth.route().history, member);
            let expected = self.joined_claim(birth, member, log)?;
            let remote = self
                .namespace
                .inner
                .read_versioned_bounded(&key, wire::LIMIT)
                .await?;
            if remote.is_none_or(|(bytes, _)| bytes != expected) {
                return Err(CatalogError::Conflict("permanent group member claim"));
            }
            self.require_current_group_birth(birth).await?;
            check()?;
            if let Some(host) = host {
                self.require_managed_group_boot(log, host).await?;
            }
            Ok(CellGroupBootstrapPermit {
                group,
                birth: birth.clone(),
                directory: self.clone(),
                log: log.clone(),
                origin: match host {
                    Some(host) => BootstrapOrigin::ManagedJoined(Box::new(host.clone())),
                    None => BootstrapOrigin::Joined,
                },
            })
        })
        .await
    }
}
