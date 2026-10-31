// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Enrollment observations bind logical names before a cell image exists.

use super::{EnrollmentRecord, PreparedGroupManifest};
use crate::application::consensus::cell_routing_key;
use crate::raft::{cache::GroupLog, completion::RuntimeError};
use cellar_logic::{cell_group::*, consensus::Term};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// A verified current-term applied enrollment observation, including on a
/// follower. It permits resolution of old producers, never new initialization.
pub struct CellEnrollmentState {
    group: GroupLog<GroupHistory>,
    route: GroupRoute,
    cell: CellHistory,
    term: Term,
    record: Option<EnrollmentRecord>,
}

impl CellEnrollmentState {
    pub(crate) fn engine_owner(&self) -> &Arc<()> {
        &self.group.owner
    }

    pub(super) fn new(
        group: GroupLog<GroupHistory>,
        current: &PreparedGroupManifest,
        cell: CellHistory,
    ) -> Result<Self, RuntimeError> {
        let route = current.marker().route();
        if !route.history.same_namespace(&cell)
            || !route
                .range
                .contains(&cell_routing_key(cell.namespace, &cell.cell))
        {
            return Err(RuntimeError::Invalid("enrollment cell route".into()));
        }
        let state = Self {
            record: current
                .pin_cell(&cell.cell)
                .map(|(leaf, _)| leaf.enrollment.clone()),
            group,
            route: route.clone(),
            cell,
            term: current.marker().position().term,
        };
        state.check()?;
        Ok(state)
    }

    pub fn cell(&self) -> &CellHistory {
        &self.cell
    }

    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn record(&self) -> Option<&EnrollmentRecord> {
        self.record.as_ref()
    }

    pub(crate) fn belongs_to_engine(&self, owner: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.group.owner, owner)
    }

    pub(crate) fn same_binding(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.group.owner, &other.group.owner)
            && self.route == other.route
            && self.cell == other.cell
    }

    pub fn check(&self) -> Result<(), RuntimeError> {
        let cache = self.group.read()?;
        let current = cache
            .group_manifest
            .as_ref()
            .ok_or(RuntimeError::NotReady)?;
        let key = cell_routing_key(self.cell.namespace, &self.cell.cell);
        if self.term.get() == 0
            || cache.hard_state.term != self.term.get()
            || current.position().term != self.term
            || current.position().index.get() > cache.hard_state.commit
            || current.route() != &self.route
            || !cache.membership.permits_group(self.group.identity())
            || cache
                .manifest_pins
                .as_ref()
                .ok_or(RuntimeError::NotReady)?
                .get(&key)
                .map(|(leaf, _)| &leaf.enrollment)
                != self.record.as_ref()
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }
}

/// Issued by the actual leader only after its current-term committed prefix is
/// applied. A node SQL admission lease must additionally serialize the name.
pub struct CellEnrollmentAuthority {
    state: CellEnrollmentState,
    leader_term: Arc<AtomicU64>,
}

impl CellEnrollmentAuthority {
    pub(super) fn new(
        state: CellEnrollmentState,
        leader_term: Arc<AtomicU64>,
    ) -> Result<Self, RuntimeError> {
        let authority = Self { state, leader_term };
        authority.check()?;
        Ok(authority)
    }

    pub fn state(&self) -> &CellEnrollmentState {
        &self.state
    }

    pub fn check(&self) -> Result<(), RuntimeError> {
        if self.leader_term.load(Ordering::Acquire) != self.state.term.get() {
            return Err(RuntimeError::NotReady);
        }
        self.state.check()?;
        let cache = self.state.group.read()?;
        if !cache
            .conf_state
            .voters
            .iter()
            .chain(&cache.conf_state.voters_outgoing)
            .any(|id| *id == self.state.group.identity().member.get())
        {
            return Err(RuntimeError::NotReady);
        }
        if self.leader_term.load(Ordering::Acquire) != self.state.term.get() {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }
}
