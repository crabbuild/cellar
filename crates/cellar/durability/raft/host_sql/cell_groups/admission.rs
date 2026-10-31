// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, NamespaceId},
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, Weak};

type LogicalCell = (ClusterId, NamespaceId, CellId);

pub(super) struct Admission {
    lease: Arc<crate::raft::storage::SqlPoolLease>,
    node: Arc<Budget>,
    limits: CellSqlLimits,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    cells: BTreeSet<LogicalCell>,
    groups: BTreeMap<GroupHistory, Weak<Budget>>,
}

pub(super) struct Bytes {
    _lease: Arc<crate::raft::storage::SqlPoolLease>,
    _node: Permit,
    _group: Permit,
}

impl Bytes {
    pub fn shrink(bytes: &mut Arc<Self>, retained: usize) -> Result<(), ExecutionError> {
        let unique =
            Arc::get_mut(bytes).ok_or(ExecutionError::Invalid("shared capture admission"))?;
        if !unique._node.shrink_to(retained) || !unique._group.shrink_to(retained) {
            return Err(ExecutionError::Invalid("retained SQL output bound"));
        }
        Ok(())
    }
}

pub(super) struct CellLease {
    owner: Arc<Admission>,
    cell: LogicalCell,
}

impl Drop for CellLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            state.cells.remove(&self.cell);
        }
    }
}

impl Admission {
    pub fn new(limits: CellSqlLimits, lease: Arc<crate::raft::storage::SqlPoolLease>) -> Arc<Self> {
        Arc::new(Self {
            lease,
            node: Budget::new(limits.node_bytes, limits.max_pending_cells),
            limits,
            state: Mutex::new(State::default()),
        })
    }

    pub fn reserve(
        self: &Arc<Self>,
        cell: &CellHistory,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<(CellLease, Arc<Bytes>), ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        let key = (cell.cluster, cell.namespace, cell.cell.clone());
        if state.cells.contains(&key) {
            return Err(ExecutionError::Busy);
        }
        let bytes = self.reserve_bytes(&mut state, group, bytes)?;
        state.cells.insert(key.clone());
        Ok((
            CellLease {
                owner: self.clone(),
                cell: key,
            },
            bytes,
        ))
    }

    pub fn reserve_read(
        &self,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<Arc<Bytes>, ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        self.reserve_bytes(&mut state, group, bytes)
    }

    fn reserve_bytes(
        &self,
        state: &mut State,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<Arc<Bytes>, ExecutionError> {
        let node = self.node.acquire(bytes).ok_or_else(overloaded)?;
        // Reclaim metadata only, never authority or pending physical ownership.
        state.groups.retain(|_, budget| budget.strong_count() != 0);
        let group_budget = state
            .groups
            .get(&group)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                Budget::new(self.limits.group_bytes, self.limits.group_pending_cells)
            });
        let group_permit = group_budget.acquire(bytes).ok_or_else(overloaded)?;
        state.groups.insert(group, Arc::downgrade(&group_budget));
        Ok(Arc::new(Bytes {
            _lease: self.lease.clone(),
            _node: node,
            _group: group_permit,
        }))
    }

    pub fn usage(&self) -> Result<(usize, usize, usize), ExecutionError> {
        let cells = self.state.lock().map_err(|_| unavailable())?.cells.len();
        let (bytes, reservations) = self.node.usage();
        Ok((cells, bytes, reservations))
    }
}
