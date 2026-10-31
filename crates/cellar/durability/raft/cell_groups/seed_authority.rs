// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh quorum authorization for a complete-group learner seed. Destination
//! authentication, permanent claims and installation are separate boundaries.

use super::{
    CellGroupCheckpoint, CellGroupCheckpointChunk, CellGroupCheckpointError,
    CellGroupCheckpointPool, GroupManifest, PreparedGroupManifest,
};
use crate::{
    asyncrt::DomainToken,
    raft::{budget::Permit, cache::GroupLog, completion::RuntimeError},
};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{LogIndex, LogPosition, ReplicaIdentity},
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub(super) struct SeedSelection {
    pub group: GroupLog<GroupHistory>,
    pub root: PreparedGroupManifest,
    pub target: ReplicaIdentity<GroupHistory>,
    pub barrier: LogPosition,
    pub leader: Arc<AtomicU64>,
}

/// Reject birth members, retired IDs, other disks, and members in either voter
/// set. A joining learner has one permanent physical binding in the applied log.
pub(super) fn check_target(
    group: &GroupLog<GroupHistory>,
    target: &ReplicaIdentity<GroupHistory>,
) -> Result<LogIndex, RuntimeError> {
    let cache = group.read()?;
    let local = group.identity();
    let id = target.member.get();
    if target.scope != local.scope
        || target.node == local.node
        || !cache.conf_state.learners.contains(&id)
        || cache.conf_state.voters.contains(&id)
        || cache.conf_state.voters_outgoing.contains(&id)
        || cache.conf_state.learners_next.contains(&id)
        || !cache.membership.permits_group(target)
    {
        return Err(RuntimeError::Invalid("group seed target".into()));
    }
    let (binding, joined_at) = cache
        .membership
        .joined(target.member)?
        .ok_or_else(|| RuntimeError::Invalid("group seed requires a joined learner".into()))?;
    if binding.node != target.node || binding.disk != target.disk {
        return Err(RuntimeError::Invalid("group seed member binding".into()));
    }
    Ok(joined_at)
}

/// One non-cloneable, deadline-bound ReadIndex result. Holding this value pins a
/// complete applied root and charges the existing node and group read budgets.
/// Capture and stream on an admitted blocking owner, retaining it through actual
/// completion even when a network waiter disconnects.
pub struct CellGroupSeedAuthority {
    selection: SeedSelection,
    deadline: u64,
    domain: DomainToken,
    _node: Permit,
    _group: Permit,
}

impl CellGroupSeedAuthority {
    pub(super) fn new(
        selection: SeedSelection,
        deadline: u64,
        domain: DomainToken,
        mut node: Permit,
        mut group: Permit,
    ) -> Result<Self, RuntimeError> {
        let bytes = selection
            .root
            .reserved_bytes()
            .checked_add(4096)
            .ok_or(RuntimeError::Overloaded)?;
        if !node.grow_to(bytes) || !group.grow_to(bytes) {
            return Err(RuntimeError::Overloaded);
        }
        let authority = Self {
            selection,
            deadline,
            domain,
            _node: node,
            _group: group,
        };
        authority.check()?;
        Ok(authority)
    }

    pub fn target(&self) -> &ReplicaIdentity<GroupHistory> {
        &self.selection.target
    }

    pub fn barrier(&self) -> LogPosition {
        self.selection.barrier
    }

    pub fn deadline(&self) -> u64 {
        self.deadline
    }

    /// Leadership, applied membership and deadline remain live constraints.
    /// This check cannot replace the fresh ReadIndex that created the value.
    pub fn check(&self) -> Result<(), RuntimeError> {
        if self.domain.mono_ms().map_err(|_| RuntimeError::Closed)? >= self.deadline {
            return Err(RuntimeError::DeadlineExceeded);
        }
        let s = &self.selection;
        let cache = s.group.read()?;
        let marker = s.root.marker();
        let local = s.group.identity();
        let term = s.barrier.term.get();
        if s.leader.load(Ordering::Acquire) != term
            || cache.hard_state.term != term
            || cache
                .group_manifest
                .as_ref()
                .is_none_or(|m| m.route() != marker.route())
            || cache.membership.revision().get() != marker.membership_revision()
            || !cache.membership.permits_group(local)
            || !cache
                .conf_state
                .voters
                .iter()
                .chain(&cache.conf_state.voters_outgoing)
                .any(|id| *id == local.member.get())
        {
            return Err(RuntimeError::NotReady);
        }
        drop(cache);
        if check_target(&s.group, &s.target)? > s.barrier.index {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }

    /// Capture the exact selected root. If apply advanced before capture, refuse
    /// and obtain a fresh authority. Ordinary writes after capture may continue;
    /// the admitted source retains its immutable cut. Membership changes revoke it.
    pub fn capture(
        self,
        pool: &CellGroupCheckpointPool,
    ) -> Result<CellGroupSeedSource, CellGroupCheckpointError> {
        self.check().map_err(CellGroupCheckpointError::Authority)?;
        let checkpoint = pool.capture(&self.selection.group, &self.selection.root)?;
        self.check().map_err(CellGroupCheckpointError::Authority)?;
        Ok(CellGroupSeedSource {
            authority: self,
            checkpoint,
        })
    }
}

/// A one-shot complete checkpoint bound to one joining process and fresh quorum
/// barrier. Neither its metadata nor a completed stream grants installation or
/// voting authority. A receiver must require an authenticated successful End.
pub struct CellGroupSeedSource {
    authority: CellGroupSeedAuthority,
    checkpoint: CellGroupCheckpoint,
}

impl CellGroupSeedSource {
    pub fn target(&self) -> &ReplicaIdentity<GroupHistory> {
        self.authority.target()
    }

    pub fn barrier(&self) -> LogPosition {
        self.authority.barrier()
    }

    pub fn marker(&self) -> &GroupManifest {
        self.checkpoint.marker()
    }

    pub fn check(&self) -> Result<(), RuntimeError> {
        self.authority.check()
    }

    pub fn snapshot(&self) -> Result<raft::prelude::Snapshot, CellGroupCheckpointError> {
        self.check().map_err(CellGroupCheckpointError::Authority)?;
        self.checkpoint.snapshot()
    }

    /// No lock is held across the sink. A revoked or expired stream cannot report
    /// success, including when the last sink call stalls beyond the deadline.
    pub fn stream(
        &mut self,
        mut sink: impl FnMut(CellGroupCheckpointChunk<'_>) -> Result<(), CellGroupCheckpointError>,
    ) -> Result<(), CellGroupCheckpointError> {
        self.check().map_err(CellGroupCheckpointError::Authority)?;
        let authority = &self.authority;
        self.checkpoint.stream(|chunk| {
            authority
                .check()
                .map_err(CellGroupCheckpointError::Authority)?;
            sink(chunk)?;
            authority
                .check()
                .map_err(CellGroupCheckpointError::Authority)
        })?;
        self.check().map_err(CellGroupCheckpointError::Authority)
    }
}
