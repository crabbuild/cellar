// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-owned safe-ReadIndex observations for whole physical groups.
//! These contain no application image and grant no mutation or release lease.
use super::GroupManifest;
use crate::raft::{budget::Permit, completion::RuntimeError, member_state::GroupMembership};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{EngineInstanceId, LogPosition, ReplicaIdentity},
};
use raft::prelude::ConfState;

// Charged to the existing node/group read banks before sending ReadIndex or
// cloning membership metadata. No admission ceiling is enlarged for this lane.
pub(super) const MEMBERSHIP_BYTES: usize = 64 * 1024;

pub(super) struct MembershipSelection {
    pub replica: ReplicaIdentity<GroupHistory>,
    pub engine: EngineInstanceId,
    pub root: GroupManifest,
    pub barrier: LogPosition,
    pub membership: GroupMembership,
    pub configuration: ConfState,
    pub status: super::CellGroupStatus,
}

/// Exact durable configuration observed behind this invocation's fresh quorum
/// barrier. Retaining this snapshot supplies no current quorum, fleet eligibility,
/// complete inventory, archive coverage or permission to reclaim physical debt.
pub struct CellGroupQuorumMembership {
    selected: MembershipSelection,
    _node: Permit,
    _group: Permit,
}

impl CellGroupQuorumMembership {
    pub(super) fn new(
        selected: MembershipSelection,
        node: Permit,
        group: Permit,
    ) -> Result<Self, RuntimeError> {
        selected.membership.validate(
            &selected.configuration,
            selected.root.position().index.get(),
        )?;
        selected
            .membership
            .validate_scope(&selected.replica.scope)?;
        if selected.root.route().history != selected.replica.scope
            || selected.root.membership_revision() != selected.membership.revision().get()
            || selected.barrier.index.get() == 0
            || selected.barrier.term.get() == 0
            || selected.root.position().index < selected.barrier.index
            || selected.root.position().term != selected.barrier.term
            || selected.status.term != selected.barrier.term
            || selected.status.applied != selected.root.position()
            || selected.status.committed < selected.root.position().index
            || selected.status.leader.is_none()
            || selected.membership.revision() > selected.barrier.index
            || !selected.membership.permits_group(&selected.replica)
        {
            return Err(RuntimeError::Invalid(
                "quorum group membership observation".into(),
            ));
        }
        Ok(Self {
            selected,
            _node: node,
            _group: group,
        })
    }

    pub fn replica(&self) -> &ReplicaIdentity<GroupHistory> {
        &self.selected.replica
    }

    pub fn engine(&self) -> EngineInstanceId {
        self.selected.engine
    }

    pub fn root(&self) -> &GroupManifest {
        &self.selected.root
    }

    pub fn barrier(&self) -> LogPosition {
        self.selected.barrier
    }

    pub fn membership(&self) -> &GroupMembership {
        &self.selected.membership
    }

    pub fn configuration(&self) -> &ConfState {
        &self.selected.configuration
    }

    /// Same-owner progress at selection. A follower observation establishes no
    /// local leader, execution, transfer or application-serving authority.
    pub fn status(&self) -> &super::CellGroupStatus {
        &self.selected.status
    }
}
