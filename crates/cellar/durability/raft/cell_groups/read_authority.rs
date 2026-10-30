// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One invocation's coherent cell pin, never a reusable leadership lease.

use super::{CellImagePin, CellManifestLeaf};
use crate::raft::{budget::Permit, cache::GroupLog, completion::RuntimeError};
use cellar_logic::{
    cell_group::*,
    consensus::{LogPosition, Term},
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Clone)]
pub enum CellReadConsistency {
    /// Explicitly allows a locally committed older image, including on followers.
    Stale,
    /// The dispatcher must first authenticate the version and its schema/lineage.
    AtLeast(CellVersion),
    Linearizable,
}

pub(super) struct ReadSelection {
    pub group: GroupLog<GroupHistory>,
    pub route: GroupRoute,
    pub cell: CellManifestLeaf,
    pub image: CellImagePin,
    pub applied: LogPosition,
    pub barrier: Option<LogPosition>,
    pub leader: Option<(Term, Arc<AtomicU64>)>,
}

/// Consumed by one SQL query. The image and its admission cannot be cloned out
/// of this proof. A strong selection requires a fresh correlated ReadIndex and
/// contiguous apply; unrelated cell progress never becomes a cell version.
pub struct CellReadAuthority {
    selection: ReadSelection,
    deadline: u64,
    _node: Permit,
    _group: Permit,
}

impl CellReadAuthority {
    pub(super) fn new(
        selection: ReadSelection,
        deadline: u64,
        mut node: Permit,
        mut group: Permit,
    ) -> Result<Self, RuntimeError> {
        let bytes = usize::try_from(selection.cell.geometry.length)
            .ok()
            .and_then(|n| n.checked_add(4096))
            .ok_or(RuntimeError::Overloaded)?;
        if !node.grow_to(bytes) || !group.grow_to(bytes) {
            return Err(RuntimeError::Overloaded);
        }
        Ok(Self {
            selection,
            deadline,
            _node: node,
            _group: group,
        })
    }

    pub fn cell(&self) -> &CellManifestLeaf {
        &self.selection.cell
    }

    pub fn route(&self) -> &GroupRoute {
        &self.selection.route
    }

    pub fn applied(&self) -> LogPosition {
        self.selection.applied
    }

    pub fn barrier(&self) -> Option<LogPosition> {
        self.selection.barrier
    }

    pub fn deadline(&self) -> u64 {
        self.deadline
    }

    pub(crate) fn image(&self) -> &CellImagePin {
        &self.selection.image
    }

    pub(crate) fn belongs_to_engine(&self, engine: &Arc<()>) -> bool {
        Arc::ptr_eq(engine, &self.selection.group.owner)
    }

    pub(crate) fn check(&self, now: u64) -> Result<(), RuntimeError> {
        if now >= self.deadline {
            return Err(RuntimeError::DeadlineExceeded);
        }
        let s = &self.selection;
        let cache = s.group.read()?;
        if cache
            .group_manifest
            .as_ref()
            .is_none_or(|m| m.route() != &s.route)
            || !cache.membership.permits_group(s.group.identity())
            || s.leader.as_ref().is_some_and(|(term, leader)| {
                cache.hard_state.term != term.get() || leader.load(Ordering::Acquire) != term.get()
            })
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }
}
