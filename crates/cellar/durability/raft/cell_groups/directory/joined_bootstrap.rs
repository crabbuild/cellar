// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A staging mapping never grants startup. Only a locally fenced authenticated
//! install plus the exact permanent remote claim can resume a joined member.
use super::*;
use crate::raft::transport::CellGroupSeedGrant;

impl RangeDirectory {
    pub(crate) async fn stage_group_learner(
        &self,
        birth: &CellGroupBirth,
        log: &ReplicaLog,
        grant: Arc<CellGroupSeedGrant>,
    ) -> Result<GroupLog<GroupHistory>, CatalogError> {
        self.unmanaged_birth()?;
        self.bounded(async {
            grant.check_log(log).map_err(CatalogError::Log)?;
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
            Ok(CellGroupBootstrapPermit {
                group,
                birth: birth.clone(),
                directory: self.clone(),
                log: log.clone(),
                joined: true,
            })
        })
        .await
    }
}
