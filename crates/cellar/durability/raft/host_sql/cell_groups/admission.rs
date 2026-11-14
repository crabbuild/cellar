// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, NamespaceId},
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, Weak};

type LogicalCell = (ClusterId, NamespaceId, CellId);

/// A received refusal from one finite admission bank, before SQL enqueue.
/// This pins only bounded admission metadata; it grants no physical credit.
pub(crate) struct SqlCapacityWait {
    bank: Weak<Budget>,
    bytes: usize,
}

impl SqlCapacityWait {
    pub(crate) async fn released(&self) {
        let Some(bank) = self.bank.upgrade() else {
            // The old bank has no owner or credit left. The next admission
            // still checks the current bank and authority under their locks.
            return;
        };
        loop {
            let mut changed = std::pin::pin!(bank.wake.notified());
            changed.as_mut().enable();
            // Register before inspecting both dimensions. Another waiter may
            // have consumed a stored notification after this refusal, without
            // consuming all the credit this request can use.
            let (bytes, items) = bank.usage();
            let (max_bytes, max_items) = bank.capacity();
            if items < max_items
                && bytes
                    .checked_add(self.bytes)
                    .is_some_and(|n| n <= max_bytes)
            {
                return;
            }
            changed.await;
        }
    }
}

pub(super) struct Failure {
    pub error: ExecutionError,
    pub wait: Option<SqlCapacityWait>,
}

impl Failure {
    fn terminal(error: ExecutionError) -> Self {
        Self { error, wait: None }
    }

    fn capacity(bank: &Arc<Budget>, bytes: usize) -> Self {
        let (max_bytes, max_items) = bank.capacity();
        Self {
            error: overloaded(),
            // A request that cannot fit by itself must refuse immediately.
            wait: (bytes <= max_bytes && max_items != 0).then(|| SqlCapacityWait {
                bank: Arc::downgrade(bank),
                bytes,
            }),
        }
    }
}

pub(super) struct Admission {
    lease: Arc<crate::raft::storage::SqlPoolLease>,
    node: Arc<Budget>,
    limits: CellSqlLimits,
    state: Mutex<State>,
    max_retired_groups: usize,
    writers: tokio::sync::watch::Sender<()>,
    performance: Arc<crate::raft::performance::Registry>,
}

#[derive(Default)]
struct State {
    cells: BTreeSet<LogicalCell>,
    groups: BTreeMap<GroupHistory, Weak<Budget>>,
    retired: BTreeSet<GroupHistory>,
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
    group: GroupHistory,
    held: bool,
}

impl CellLease {
    pub(super) fn park(&mut self) -> Result<(), ExecutionError> {
        let mut state = self.owner.state.lock().map_err(|_| unavailable())?;
        if !self.held || !state.cells.remove(&self.cell) {
            return Err(ExecutionError::Invalid("cell writer lease is not held"));
        }
        self.held = false;
        drop(state);
        self.owner.writers.send_replace(());
        Ok(())
    }

    pub(super) fn resume(&mut self) -> Result<(), ExecutionError> {
        let mut state = self.owner.state.lock().map_err(|_| unavailable())?;
        if self.held {
            return Ok(());
        }
        if state.retired.contains(&self.group) {
            return Err(unavailable());
        }
        if !state.cells.insert(self.cell.clone()) {
            return Err(ExecutionError::Busy);
        }
        self.held = true;
        Ok(())
    }
}

impl Drop for CellLease {
    fn drop(&mut self) {
        let released = self.held
            && self
                .owner
                .state
                .lock()
                .is_ok_and(|mut state| state.cells.remove(&self.cell));
        if released {
            self.owner.writers.send_replace(());
        }
    }
}

impl Admission {
    /// Local writer release need not produce another Raft apply notification.
    pub(super) fn writer_progress(&self) -> tokio::sync::watch::Receiver<()> {
        self.writers.subscribe()
    }

    pub fn limits(&self) -> CellSqlLimits {
        self.limits
    }

    pub fn new(
        limits: CellSqlLimits,
        lease: Arc<crate::raft::storage::SqlPoolLease>,
        max_retired_groups: usize,
        performance: Arc<crate::raft::performance::Registry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            lease,
            max_retired_groups,
            node: Budget::new(limits.node_bytes, limits.max_pending_cells),
            limits,
            state: Mutex::new(State::default()),
            writers: tokio::sync::watch::channel(()).0,
            performance,
        })
    }

    pub fn reserve(
        self: &Arc<Self>,
        cell: &CellHistory,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<(CellLease, Arc<Bytes>), ExecutionError> {
        self.reserve_stage(cell, group, bytes)
            .map_err(|failure| failure.error)
    }

    pub(super) fn reserve_stage(
        self: &Arc<Self>,
        cell: &CellHistory,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<(CellLease, Arc<Bytes>), Failure> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Failure::terminal(unavailable()))?;
        let key = (cell.cluster, cell.namespace, cell.cell.clone());
        if state.cells.contains(&key) {
            return Err(Failure::terminal(ExecutionError::Busy));
        }
        let bytes = self.reserve_bytes(&mut state, group, bytes)?;
        state.cells.insert(key.clone());
        Ok((
            CellLease {
                owner: self.clone(),
                cell: key,
                group,
                held: true,
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
            .map_err(|failure| failure.error)
    }

    fn reserve_bytes(
        &self,
        state: &mut State,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<Arc<Bytes>, Failure> {
        if state.retired.contains(&group) {
            return Err(Failure::terminal(ExecutionError::Refused(
                CommandFailure::Unavailable,
            )));
        }
        let node = self.node.acquire(bytes).ok_or_else(|| {
            self.performance.increment(
                crate::raft::performance::Counter::SqlNodeAdmissionRefusals,
                1,
            );
            Failure::capacity(&self.node, bytes)
        })?;
        // Reclaim metadata only, never authority or pending physical ownership.
        state.groups.retain(|_, budget| budget.strong_count() != 0);
        let group_budget = state
            .groups
            .get(&group)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                Budget::new(self.limits.group_bytes, self.limits.group_pending_cells)
            });
        let group_permit = group_budget.acquire(bytes).ok_or_else(|| {
            self.performance.increment(
                crate::raft::performance::Counter::SqlGroupAdmissionRefusals,
                1,
            );
            Failure::capacity(&group_budget, bytes)
        })?;
        state.groups.insert(group, Arc::downgrade(&group_budget));
        Ok(Arc::new(Bytes {
            _lease: self.lease.clone(),
            _node: node,
            _group: group_permit,
        }))
    }

    /// Seal this history's admission under the same lock as reservation. A
    /// retained response/producer stays charged after its caller disconnects.
    pub fn retire(&self, group: GroupHistory) -> Result<bool, ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if !state.retired.contains(&group) && state.retired.len() >= self.max_retired_groups {
            return Err(overloaded());
        }
        let newly_retired = state.retired.insert(group);
        let drained = state
            .groups
            .get(&group)
            .is_none_or(|budget| budget.strong_count() == 0);
        drop(state);
        if newly_retired {
            // Waiting invocations must recheck authority and release their
            // metadata pins; retirement does not refund physical credits.
            self.writers.send_replace(());
        }
        Ok(drained)
    }

    pub(super) fn release_reclaimed_group(
        &self,
        group: GroupHistory,
    ) -> Result<(), ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state
            .groups
            .get(&group)
            .is_some_and(|budget| budget.strong_count() != 0)
        {
            return Err(ExecutionError::Busy);
        }
        state.groups.remove(&group);
        state.retired.remove(&group);
        Ok(())
    }

    pub fn usage(&self) -> Result<(usize, usize, usize), ExecutionError> {
        let cells = self.state.lock().map_err(|_| unavailable())?.cells.len();
        let (bytes, reservations) = self.node.usage();
        Ok((cells, bytes, reservations))
    }
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "admission_tests.rs"]
mod tests;
