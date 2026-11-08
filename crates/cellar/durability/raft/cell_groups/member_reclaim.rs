// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An obsolete replica leaves a member fence, not a whole-history tombstone.
use super::GroupManifest;
use crate::raft::{
    cache::GroupLog,
    member_state::{FleetMembershipPermit, FleetPermitPhase, RepairPhase},
    storage::{LogError, ReplicaLog},
};
use cellar_logic::{cell_group::GroupHistory, consensus::*};
use serde::{Deserialize, Serialize};
use std::sync::{atomic::Ordering, Arc};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Member {
    pub(crate) history: [[u8; 16]; 4],
    pub(crate) birth: [u8; 32],
    pub(crate) root: Option<[u8; 32]>,
    pub(crate) member: u64,
    pub(crate) high_water: u64,
    pub(crate) permit: FleetMembershipPermit,
    pub(crate) membership: u64,
    pub(crate) index: u64,
    pub(crate) term: u64,
}

impl Member {
    pub(crate) fn from_group(
        group: &GroupLog<GroupHistory>,
        permit: &FleetMembershipPermit,
    ) -> Result<Self, LogError> {
        let cache = group.read()?;
        let marker = cache
            .group_manifest
            .as_ref()
            .ok_or(LogError::WrongIdentity)?;
        let history = marker.route().history;
        let member = Self {
            history: [
                history.cluster.bytes(),
                history.namespace.bytes(),
                history.group.bytes(),
                history.incarnation.bytes(),
            ],
            birth: marker.route().birth_digest,
            root: marker.root().digest(),
            member: group.identity().member.get(),
            high_water: cache.membership.high_water(),
            permit: permit.clone(),
            membership: marker.membership_revision(),
            index: marker.position().index.get(),
            term: marker.position().term.get(),
        };
        drop(cache);
        member.check_group(group)?;
        Ok(member)
    }

    pub(crate) fn scope(&self) -> Result<GroupHistory, LogError> {
        let invalid = |_| LogError::WrongIdentity;
        let history = GroupHistory {
            cluster: ClusterId::new(self.history[0]).map_err(invalid)?,
            namespace: NamespaceId::new(self.history[1]).map_err(invalid)?,
            group: GroupId::new(self.history[2]).map_err(invalid)?,
            incarnation: IncarnationId::new(self.history[3]).map_err(invalid)?,
        };
        let permit = self.authorization()?;
        if self.birth == [0; 32]
            || self.root == Some([0; 32])
            || self.member == 0
            || self.high_water < self.member
            || self.membership == 0
            || self.index < self.membership
            || self.term == 0
            || permit
                .binding()
                .scope()
                .map_err(|_| LogError::WrongIdentity)?
                != history
            || permit.expected_revision().get() >= self.membership
            || permit
                .replacement()
                .map_err(|_| LogError::WrongIdentity)?
                .member
                .get()
                > self.high_water
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(history)
    }

    pub(crate) fn authorization(&self) -> Result<FleetMembershipPermit, LogError> {
        // Preserve the typed permit in the bounded refusal record. Embedding
        // its framed bytes as JSON integers would multiply retained metadata.
        self.permit.encode().map_err(|_| LogError::WrongIdentity)?;
        Ok(self.permit.clone())
    }

    pub(crate) fn check_marker(&self, marker: &GroupManifest) -> Result<(), LogError> {
        if marker.route().history != self.scope()?
            || marker.route().birth_digest != self.birth
            || marker.root().digest() != self.root
            || marker.membership_revision() != self.membership
            || marker.position().index.get() != self.index
            || marker.position().term.get() != self.term
            || marker.source_seal().is_some()
            || !marker.is_active()
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    pub(crate) fn check_group(&self, group: &GroupLog<GroupHistory>) -> Result<(), LogError> {
        let cache = group.read()?;
        self.check_marker(
            cache
                .group_manifest
                .as_ref()
                .ok_or(LogError::WrongIdentity)?,
        )?;
        let permit = self.authorization()?;
        let status = cache
            .membership
            .fleet_status()
            .ok_or(LogError::WrongIdentity)?;
        let repair = cache
            .membership
            .repair_status()?
            .ok_or(LogError::WrongIdentity)?;
        let conf = &cache.conf_state;
        if group.identity().member.get() != self.member
            || cache.membership.high_water() != self.high_water
            || cache.membership.revision().get() != self.membership
            || status.permit.as_ref() != Some(&permit)
            || status.consumed_sequence != permit.sequence()
            || status.phase != Some(FleetPermitPhase::Complete)
            || repair.phase != RepairPhase::Complete
            || repair.plan.obsolete.get() != self.member
            || permit.check_plan(&repair.plan).is_err()
            || conf.voters.len() != 3
            || conf.voters.contains(&self.member)
            || !conf.voters_outgoing.is_empty()
            || !conf.learners.is_empty()
            || !conf.learners_next.is_empty()
            || conf.auto_leave
            || cache.hard_state.commit < self.index
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }
}

/// The completed Fleet replacement must be verified before constructing this
/// process-local capability. Neither a serialized marker nor a peer claim owns it.
#[derive(Clone)]
pub(crate) struct Permit {
    pub(crate) member: Member,
    owner: Arc<()>,
    engine: EngineInstanceId,
    node: NodeId,
    disk: DiskLineage,
}

impl Permit {
    pub(in crate::raft) fn new(log: &ReplicaLog, member: Member) -> Result<Self, LogError> {
        member.scope()?;
        Ok(Self {
            member,
            owner: log.owner(),
            engine: log.storage_instance()?,
            node: log.limits().node,
            disk: log.limits().disk,
        })
    }

    pub(crate) fn check(
        &self,
        owner: &Arc<()>,
        engine: [u8; 16],
        node: NodeId,
        disk: DiskLineage,
    ) -> Result<(), LogError> {
        if !Arc::ptr_eq(owner, &self.owner)
            || self.engine.bytes() != engine
            || self.node != node
            || self.disk != disk
        {
            return Err(LogError::WrongIdentity);
        }
        self.member.scope().map(|_| ())
    }

    pub(crate) fn check_group(&self, group: &GroupLog<GroupHistory>) -> Result<(), LogError> {
        self.member.check_group(group)?;
        if group.consensus_claimed.load(Ordering::Acquire) {
            return Err(LogError::Overloaded);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub(crate) format: u8,
    pub(crate) member: Member,
    pub(crate) engine: [u8; 16],
    pub(crate) node: [u8; 16],
    pub(crate) disk: [u8; 16],
    pub(crate) old_key: u64,
    pub(crate) before_groups: u32,
    pub(crate) sequence: u64,
    pub(crate) complete: bool,
}

impl Record {
    pub(crate) fn validate(
        &self,
        engine: [u8; 16],
        node: NodeId,
        disk: DiskLineage,
    ) -> Result<(), LogError> {
        self.member.scope()?;
        if self.format != 1
            || self.engine != engine
            || self.node != node.bytes()
            || self.disk != disk.bytes()
            || self.before_groups == 0
            || self.old_key == 0
            || self.sequence == 0
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    pub(crate) fn permits_mapping(
        &self,
        history: GroupHistory,
        birth: [u8; 32],
        member: u64,
    ) -> bool {
        self.complete
            && self.member.scope().is_ok_and(|scope| scope == history)
            && self.member.birth == birth
            && member > self.member.high_water
    }
}

pub(crate) fn key(group: GroupId) -> Vec<u8> {
    let mut key = b"cellar-removed-member/".to_vec();
    key.extend_from_slice(&group.bytes());
    key
}

/// Page and image collection runs only after this member's synced engine fence.
pub(super) fn prepare_physical(
    applier: &super::CellGroupApplier,
    member: &Member,
) -> Result<(), crate::raft::execution::ExecutionError> {
    let storage = applier
        .storage
        .lock()
        .map_err(|_| crate::raft::execution::ExecutionError::Busy)?;
    storage
        .pages
        .retention
        .prepare_member_reclamation(&applier.group, member)
        .map_err(crate::raft::execution::storage)
}

pub(super) fn reclaim_physical(
    applier: &super::CellGroupApplier,
    member: &Member,
) -> Result<bool, crate::raft::execution::ExecutionError> {
    let mut storage = applier
        .storage
        .lock()
        .map_err(|_| crate::raft::execution::ExecutionError::Busy)?;
    storage
        .pages
        .retention
        .detach_reclaimed_member(&applier.group, member)
        .map_err(crate::raft::execution::storage)?;
    storage
        .pages
        .reconcile_retained(&[])
        .map_err(crate::raft::execution::storage)?;
    storage
        .pages
        .reclaim(256)
        .map_err(crate::raft::execution::storage)?;
    super::source_reclaim::collect_images(&storage.images)?;
    Ok(storage.pages.usage().pages == 0)
}
