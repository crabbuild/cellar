// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Initial member admission after real genesis and a permanent physical claim.

use super::*;
use crate::raft::{
    cache::GroupLog,
    storage::{CellGroupBootstrap, LogError, ReplicaLog},
};
use cellar_logic::consensus::MemberId;
use serde::{Deserialize, Serialize};

/// Initial member startup permission, not quorum or application-serving authority.
/// The shared runtime must verify the complete selected image/manifest closure
/// and revalidate this permit before attachment. It must obtain current-term
/// quorum authority separately before accepting application work.
pub struct CellGroupBootstrapPermit {
    group: GroupLog<GroupHistory>,
    birth: CellGroupBirth,
    directory: RangeDirectory,
    log: ReplicaLog,
}

impl CellGroupBootstrapPermit {
    pub(crate) fn log(&self) -> &ReplicaLog {
        &self.log
    }

    pub fn group(&self) -> &GroupLog<GroupHistory> {
        &self.group
    }

    pub fn route(&self) -> &GroupRoute {
        self.birth.route()
    }

    pub async fn revalidate(&self) -> Result<(), CatalogError> {
        self.directory
            .bootstrap_group_member(&self.birth, &self.log)
            .await
            .map(|_| ())
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

fn check_member(group: &GroupLog<GroupHistory>) -> Result<(), CatalogError> {
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
            self.require_current_group_birth(birth).await?;
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
            // The worker creates the complete empty root with genesis and only
            // returns after actual engine sync. Existing mappings must match
            // the original route, membership and birth, even after later apply.
            let group = log
                .create_cell_group(CellGroupBootstrap {
                    route: birth.route().clone(),
                    member: selected.member,
                    voters: birth.voters().iter().map(|m| m.member).collect(),
                    learners: birth.learners().iter().map(|m| m.member).collect(),
                })
                .await
                .map_err(CatalogError::Log)?;
            check_member(&group)?;
            let observed = self
                .namespace
                .inner
                .create_and_observe_bounded(&key, &expected, wire::LIMIT)
                .await?;
            if observed != expected {
                return Err(CatalogError::Conflict("permanent group member claim"));
            }
            self.require_current_group_birth(birth).await?;
            check_member(&group)?;
            Ok(CellGroupBootstrapPermit {
                group,
                birth: birth.clone(),
                directory: self.clone(),
                log: log.clone(),
            })
        })
        .await
    }
}
