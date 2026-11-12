// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Permanent local refusal after a selected, drained source history is erased.
use super::GroupManifest;
use crate::raft::{
    cache::GroupLog,
    storage::{LogError, ReplicaLog},
};
use cellar_logic::{cell_group::GroupHistory, consensus::*};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub(crate) history: [[u8; 16]; 4],
    pub(crate) birth: [u8; 32],
    pub(crate) root: Option<[u8; 32]>,
    pub(crate) member: u64,
    pub(crate) operation: [u8; 16],
    pub(crate) plan: [u8; 32],
    pub(crate) retirement: [u8; 32],
    pub(crate) publication: [u8; 32],
    pub(crate) seal_index: u64,
    pub(crate) seal_term: u64,
    pub(crate) index: u64,
    pub(crate) term: u64,
}

impl Source {
    pub(crate) fn scope(&self) -> Result<GroupHistory, LogError> {
        let invalid = |_| LogError::WrongIdentity;
        let scope = GroupHistory {
            cluster: ClusterId::new(self.history[0]).map_err(invalid)?,
            namespace: NamespaceId::new(self.history[1]).map_err(invalid)?,
            group: GroupId::new(self.history[2]).map_err(invalid)?,
            incarnation: IncarnationId::new(self.history[3]).map_err(invalid)?,
        };
        if self.root == Some([0; 32])
            || self.member == 0
            || self.operation == [0; 16]
            || [self.birth, self.plan, self.retirement, self.publication].contains(&[0; 32])
            || self.seal_index == 0
            || self.seal_term == 0
            || self.index <= self.seal_index
            || self.term < self.seal_term
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(scope)
    }

    pub(crate) fn check_marker(&self, marker: &GroupManifest) -> Result<(), LogError> {
        let seal = marker.source_seal().ok_or(LogError::WrongIdentity)?;
        let retired = seal.retirement().ok_or(LogError::WrongIdentity)?;
        if marker.route().history != self.scope()?
            || marker.route().birth_digest != self.birth
            || marker.root().digest() != self.root
            || seal.transition() != self.operation
            || seal.position().index.get() != self.seal_index
            || seal.position().term.get() != self.seal_term
            || retired.publication() != self.publication
            || retired.position().index.get() != self.index
            || retired.position().term.get() != self.term
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }
}

/// Constructed by the directory only after the rooted cleanup authority and
/// released parent holds verify. The engine owner is never supplied on the wire.
#[derive(Clone)]
pub(crate) struct Permit {
    pub(crate) source: Source,
    owner: Arc<()>,
    engine: EngineInstanceId,
    node: NodeId,
    disk: DiskLineage,
}

impl Permit {
    pub(in crate::raft) fn new(log: &ReplicaLog, source: Source) -> Result<Self, LogError> {
        source.scope()?;
        Ok(Self {
            source,
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
        self.source.scope().map(|_| ())
    }

    pub(crate) fn check_group(&self, group: &GroupLog<GroupHistory>) -> Result<(), LogError> {
        let current = group.group_manifest()?.ok_or(LogError::WrongIdentity)?;
        self.source.check_marker(&current)?;
        if group.identity().member.get() != self.source.member
            || group
                .consensus_claimed
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(LogError::Overloaded);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub(crate) format: u8,
    pub(crate) source: Source,
    pub(crate) engine: [u8; 16],
    pub(crate) node: [u8; 16],
    pub(crate) disk: [u8; 16],
    pub(crate) old_key: u64,
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
        self.source.scope()?;
        if self.format != 1
            || self.engine != engine
            || self.node != node.bytes()
            || self.disk != disk.bytes()
            || self.old_key == 0
            || self.sequence == 0
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }
}

/// Group IDs remain reserved across namespace/incarnation changes. Tombstones
/// are point-read from engine group zero, never loaded into an unbounded cache.
pub(crate) fn key(group: GroupId) -> Vec<u8> {
    let mut key = b"t:".to_vec();
    key.extend_from_slice(&group.bytes());
    key
}

/// Both operations run on the node's blocking domain. The page owner outlives
/// the engine Clean; physical pins are detached only after its synced refusal.
pub(super) fn prepare_physical(
    applier: &super::CellGroupApplier,
    source: &Source,
) -> Result<(), crate::raft::execution::ExecutionError> {
    let storage = applier
        .storage
        .lock()
        .map_err(|_| crate::raft::execution::ExecutionError::Busy)?;
    storage
        .pages
        .retention
        .prepare_source_reclamation(&applier.group, source)
        .map_err(crate::raft::execution::storage)
}

pub(super) fn reclaim_physical(
    applier: &super::CellGroupApplier,
    source: &Source,
) -> Result<bool, crate::raft::execution::ExecutionError> {
    let mut storage = applier
        .storage
        .lock()
        .map_err(|_| crate::raft::execution::ExecutionError::Busy)?;
    storage
        .pages
        .retention
        .detach_reclaimed_source(&applier.group, source)
        .map_err(crate::raft::execution::storage)?;
    storage
        .pages
        .reconcile_retained(&[])
        .map_err(crate::raft::execution::storage)?;
    storage
        .pages
        .reclaim(256)
        .map_err(crate::raft::execution::storage)?;
    collect_images(&storage.images)?;
    Ok(storage.pages.usage().files == 0)
}

pub(super) fn collect_images(
    images: &super::CellGroupApplyImages,
) -> Result<(), crate::raft::execution::ExecutionError> {
    use crate::raft::execution::storage;
    match images {
        super::CellGroupApplyImages::Materialized { images, .. } => {
            images.reclaim(256).map_err(storage)?;
        }
        super::CellGroupApplyImages::Extents { images, .. } => {
            images.reclaim(256).map_err(storage)?;
        }
    }
    Ok(())
}

pub(crate) const MAX_RECORD_BYTES: usize = 4096;

// Serialized KV, engine tables and rewrite/response overlap. Permanent refusal
// metadata stays charged to the existing node cache budget after source removal.
pub(crate) const CACHE_BYTES: usize = 4 * MAX_RECORD_BYTES;

pub(crate) const BUDGET_KEY: &[u8] = b"cellar-retired-source-budget";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Budget {
    pub(crate) format: u8,
    pub(crate) count: u32,
}

#[cfg(test)]
#[path = "source_reclaim_tests.rs"]
mod tests;
