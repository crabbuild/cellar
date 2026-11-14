// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, NamespaceId},
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicUsize;
use std::sync::{Mutex, Weak};

type LogicalCell = (ClusterId, NamespaceId, CellId);

pub(super) struct Admission {
    lease: Arc<crate::raft::storage::SqlPoolLease>,
    node: Arc<Budget>,
    limits: CellSqlLimits,
    state: Mutex<State>,
    max_retired_groups: usize,
    writers: tokio::sync::watch::Sender<()>,
}

#[derive(Default)]
struct State {
    closed: bool,
    cells: BTreeSet<LogicalCell>,
    writers: BTreeMap<LogicalCell, Weak<WriterGate>>,
    groups: BTreeMap<GroupHistory, Weak<Budget>>,
    retired: BTreeSet<GroupHistory>,
}

struct WriterGate {
    semaphore: Arc<tokio::sync::Semaphore>,
    waiting: AtomicUsize,
}

struct Waiting(Arc<WriterGate>);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.waiting.fetch_sub(1, Ordering::AcqRel);
    }
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
    gate: Arc<WriterGate>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    waiting: Option<
        futures_util::future::BoxFuture<
            'static,
            Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError>,
        >,
    >,
}

impl CellLease {
    pub(super) fn park(&mut self) -> Result<(), ExecutionError> {
        let mut state = self.owner.state.lock().map_err(|_| unavailable())?;
        if !self.held || !state.cells.remove(&self.cell) {
            return Err(ExecutionError::Invalid("cell writer lease is not held"));
        }
        self.held = false;
        drop(state);
        self.permit.take();
        self.owner.writers.send_replace(());
        Ok(())
    }

    pub(super) fn contended(&self) -> bool {
        self.gate.waiting.load(Ordering::Acquire) != 0
    }

    pub(super) fn resume(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ExecutionError>> {
        use std::task::Poll;
        if self.held {
            return Poll::Ready(Ok(()));
        }
        {
            let state = self.owner.state.lock().map_err(|_| unavailable())?;
            if state.closed || state.retired.contains(&self.group) {
                return Poll::Ready(Err(unavailable()));
            }
        }
        if self.waiting.is_none() {
            self.gate.waiting.fetch_add(1, Ordering::AcqRel);
            let waiting = Waiting(self.gate.clone());
            self.waiting = Some(Box::pin(async move {
                let semaphore = waiting.0.semaphore.clone();
                let _waiting = waiting;
                semaphore.acquire_owned().await
            }));
        }
        let permit = match self
            .waiting
            .as_mut()
            .ok_or_else(unavailable)?
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result.map_err(|_| unavailable())?,
        };
        self.waiting = None;
        let mut state = self.owner.state.lock().map_err(|_| unavailable())?;
        if state.closed || state.retired.contains(&self.group) {
            return Poll::Ready(Err(unavailable()));
        }
        if !state.cells.insert(self.cell.clone()) {
            return Poll::Ready(Err(ExecutionError::Invalid("writer gate and lease differ")));
        }
        self.permit = Some(permit);
        self.held = true;
        Poll::Ready(Ok(()))
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
        self.waiting.take();
        self.permit.take();
        if let Ok(mut state) = self.owner.state.lock() {
            if state.writers.get(&self.cell).is_some_and(|gate| {
                gate.strong_count() == 1 && gate.ptr_eq(&Arc::downgrade(&self.gate))
            }) {
                state.writers.remove(&self.cell);
            }
        }
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
    ) -> Arc<Self> {
        Arc::new(Self {
            lease,
            max_retired_groups,
            node: Budget::new(limits.node_bytes, limits.max_pending_cells),
            limits,
            state: Mutex::new(State::default()),
            writers: tokio::sync::watch::channel(()).0,
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
        let gate = Self::writer(&mut state, &key);
        let permit = gate
            .semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => ExecutionError::Busy,
                tokio::sync::TryAcquireError::Closed => unavailable(),
            })?;
        let bytes = self.reserve_bytes(&mut state, group, bytes)?;
        state.cells.insert(key.clone());
        Ok((
            CellLease {
                owner: self.clone(),
                cell: key,
                group,
                held: true,
                gate,
                permit: Some(permit),
                waiting: None,
            },
            bytes,
        ))
    }

    fn writer(state: &mut State, cell: &LogicalCell) -> Arc<WriterGate> {
        state.writers.retain(|_, gate| gate.strong_count() != 0);
        let gate = state
            .writers
            .get(cell)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                Arc::new(WriterGate {
                    semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
                    waiting: AtomicUsize::new(0),
                })
            });
        state.writers.insert(cell.clone(), Arc::downgrade(&gate));
        gate
    }

    /// Byte/item admission bounds every queued writer before it waits. Semaphore
    /// cancellation removes its FIFO position; idle cells retain no gate.
    pub async fn reserve_queued(
        self: &Arc<Self>,
        cell: &CellHistory,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<(CellLease, Arc<Bytes>), ExecutionError> {
        let (mut lease, bytes) = {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            let bytes = self.reserve_bytes(&mut state, group, bytes)?;
            let key = (cell.cluster, cell.namespace, cell.cell.clone());
            let gate = Self::writer(&mut state, &key);
            (
                CellLease {
                    owner: self.clone(),
                    cell: key,
                    group,
                    held: false,
                    gate,
                    permit: None,
                    waiting: None,
                },
                bytes,
            )
        };
        std::future::poll_fn(|cx| lease.resume(cx)).await?;
        Ok((lease, bytes))
    }

    pub fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            for gate in state.writers.values().filter_map(Weak::upgrade) {
                gate.semaphore.close();
            }
        }
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
        if state.closed || state.retired.contains(&group) {
            return Err(ExecutionError::Refused(CommandFailure::Unavailable));
        }
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

    /// Seal this history's admission under the same lock as reservation. A
    /// retained response/producer stays charged after its caller disconnects.
    pub fn retire(&self, group: GroupHistory) -> Result<bool, ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if !state.retired.contains(&group) && state.retired.len() >= self.max_retired_groups {
            return Err(overloaded());
        }
        state.retired.insert(group);
        Ok(state
            .groups
            .get(&group)
            .is_none_or(|budget| budget.strong_count() == 0))
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
