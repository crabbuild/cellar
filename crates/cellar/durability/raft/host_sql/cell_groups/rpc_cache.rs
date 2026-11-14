// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-wide bounded native instances, without idle SQLite or image custody.

use super::*;
use crate::application::consensus::{CellRpcActivation, RaftClass};
use crate::raft::cell_groups::CellActivationFence;
use std::collections::BTreeMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const IDLE_MILLIS: u64 = 60_000;
const PRUNE_PER_TURN: usize = 8;

#[derive(Clone)]
pub(super) struct RpcActivation {
    pub activation: Option<CellRpcActivation>,
    pub permit: Option<Arc<OwnedSemaphorePermit>>,
    pub fence: CellActivationFence,
}

impl RpcActivation {
    fn shared(&self) -> bool {
        self.activation
            .as_ref()
            .is_some_and(CellRpcActivation::shared)
    }

    pub(super) fn close(mut self) -> Result<(), ExecutionError> {
        self.activation.take().map_or(Ok(()), |activation| {
            activation.close().map_err(crate::raft::execution::storage)
        })
    }
}

struct Idle {
    value: RpcActivation,
    at: u64,
    serial: u64,
}

#[derive(Default)]
struct State {
    idle: BTreeMap<CellHistory, Idle>,
    order: BTreeMap<u64, CellHistory>,
    cursor: Option<CellHistory>,
    serial: u64,
    failure: Option<String>,
    prune_after: u64,
}

pub(in crate::raft::host_sql) struct RpcCache {
    state: Mutex<State>,
    slots: Arc<Semaphore>,
    capacity: usize,
    closed: AtomicBool,
}

impl RpcCache {
    pub(in crate::raft::host_sql) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(in crate::raft::host_sql) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
            closed: AtomicBool::new(false),
        })
    }

    pub(super) fn retire(&self, value: RpcActivation) -> Result<(), ExecutionError> {
        let result = value.close();
        if let Err(error) = &result {
            self.record_failure(error)?;
        }
        result
    }

    pub(super) fn record_failure(&self, error: &ExecutionError) -> Result<(), ExecutionError> {
        self.closed.store(true, Ordering::Release);
        self.slots.close();
        self.state
            .lock()
            .map_err(|_| unavailable())?
            .failure
            .get_or_insert_with(|| error.to_string());
        Ok(())
    }

    pub(super) fn take(
        &self,
        cell: &CellHistory,
        authority: &CellExecutionAuthority,
        class: &RaftClass,
        now: u64,
    ) -> Result<RpcActivation, ExecutionError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let prior = {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            let prior = state.idle.remove(cell);
            if let Some(prior) = &prior {
                state.order.remove(&prior.serial);
            }
            prior
        };
        if let Some(prior) = prior {
            if (prior.value.shared() || now.saturating_sub(prior.at) < IDLE_MILLIS)
                && prior.value.fence.accepts(authority)
                && prior.value.activation.as_ref().is_some_and(|activation| {
                    activation.live() && class.accepts_cell_activation(activation)
                })
            {
                return Ok(prior.value);
            }
            self.retire(prior.value)?;
        }
        Ok(RpcActivation {
            activation: None,
            permit: None,
            fence: authority.activation_fence(),
        })
    }

    pub(super) fn reserve(&self) -> Result<OwnedSemaphorePermit, ExecutionError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        match self.slots.clone().try_acquire_owned() {
            Ok(slot) => return Ok(slot),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(unavailable()),
            Err(tokio::sync::TryAcquireError::NoPermits) => (),
        }
        let evicted = {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            let selected = state
                .order
                .iter()
                .find(|(_, cell)| {
                    state
                        .idle
                        .get(*cell)
                        .is_some_and(|idle| !idle.value.shared())
                })
                .map(|(serial, cell)| (*serial, cell.clone()));
            selected.and_then(|(serial, cell)| {
                state.order.remove(&serial);
                state.idle.remove(&cell)
            })
        };
        if let Some(evicted) = evicted {
            self.retire(evicted.value)?;
        }
        self.slots
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => unavailable(),
                tokio::sync::TryAcquireError::NoPermits => overloaded(),
            })
    }

    pub(super) fn put(
        &self,
        cell: CellHistory,
        value: RpcActivation,
        now: u64,
    ) -> Result<(), ExecutionError> {
        if value.activation.is_none() {
            return value.close();
        }
        if self.closed.load(Ordering::Acquire)
            || !value.fence.current()
            || value
                .activation
                .as_ref()
                .is_some_and(|activation| !activation.live())
        {
            return self.retire(value);
        }
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if self.closed.load(Ordering::Acquire) {
            drop(state);
            return self.retire(value);
        }
        if state.idle.contains_key(&cell) || value.permit.is_none() {
            drop(state);
            self.retire(value)?;
            return Err(ExecutionError::Invalid(
                "duplicate or unadmitted RPC activation",
            ));
        }
        let serial = state.serial.checked_add(1).ok_or_else(unavailable)?;
        state.serial = serial;
        state.order.insert(serial, cell.clone());
        state.idle.insert(
            cell,
            Idle {
                value,
                at: now,
                serial,
            },
        );
        Ok(())
    }

    pub(super) fn prune(&self, now: u64) -> Result<(), ExecutionError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let observations = {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            if now < state.prune_after {
                return Ok(());
            }
            state.prune_after = now.saturating_add(10);
            let entries = match &state.cursor {
                Some(cursor) => state.idle.range((Excluded(cursor), Unbounded)),
                None => state.idle.range::<CellHistory, _>(..),
            };
            let mut selected = entries
                .take(PRUNE_PER_TURN)
                .map(|(cell, idle)| (cell.clone(), idle.serial, idle.at, idle.value.fence.clone()))
                .collect::<Vec<_>>();
            if selected.is_empty() {
                selected.extend(state.idle.iter().take(PRUNE_PER_TURN).map(|(cell, idle)| {
                    (cell.clone(), idle.serial, idle.at, idle.value.fence.clone())
                }));
            }
            state.cursor = selected.last().map(|(cell, ..)| cell.clone());
            selected
        };
        for (cell, serial, at, fence) in observations {
            // Never take a group/cache lock while holding the node cache lock.
            let (shared, live) = self
                .state
                .lock()
                .map_err(|_| unavailable())?
                .idle
                .get(&cell)
                .map_or((false, false), |idle| {
                    (
                        idle.serial == serial && idle.value.shared(),
                        idle.value
                            .activation
                            .as_ref()
                            .is_some_and(CellRpcActivation::live),
                    )
                });
            if live && (shared || now.saturating_sub(at) < IDLE_MILLIS) && fence.current() {
                continue;
            }
            let expired = {
                let mut state = self.state.lock().map_err(|_| unavailable())?;
                if state
                    .idle
                    .get(&cell)
                    .is_some_and(|idle| idle.serial == serial)
                {
                    state.order.remove(&serial);
                    state.idle.remove(&cell)
                } else {
                    None
                }
            };
            if let Some(expired) = expired {
                self.retire(expired.value)?;
            }
        }
        Ok(())
    }

    pub(in crate::raft::host_sql) fn usage(&self) -> Result<(usize, usize, usize), ExecutionError> {
        let idle = self
            .state
            .lock()
            .map_err(|_| unavailable())?
            .idle
            .values()
            .filter(|idle| !idle.value.shared())
            .count();
        let retained = self
            .capacity
            .checked_sub(self.slots.available_permits())
            .ok_or_else(unavailable)?;
        Ok((idle, retained, self.capacity))
    }

    pub(super) fn close(&self) -> Result<(), ExecutionError> {
        self.closed.store(true, Ordering::Release);
        self.slots.close();
        let idle = {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            state.order.clear();
            std::mem::take(&mut state.idle)
        };
        for (_, idle) in idle {
            let _ = self.retire(idle.value);
        }
        let failure = self
            .state
            .lock()
            .map_err(|_| unavailable())?
            .failure
            .clone();
        failure.map_or(Ok(()), |error| {
            Err(crate::raft::execution::storage(anyhow::anyhow!(error)))
        })
    }
}
