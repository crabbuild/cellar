// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Active stateless callers share a local FIFO, never a persistent cell owner.

use super::CellHistory;
use crate::raft::budget::Permit;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, CommandFailure, CommandId, IncarnationId, NamespaceId},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

type Key = (ClusterId, NamespaceId, CellId);

#[derive(Default)]
struct State {
    next: Option<Successor>,
}

struct Successor {
    incarnation: IncarnationId,
    sequence: u64,
}

#[derive(Default)]
pub(super) struct Lanes {
    active: Mutex<BTreeMap<Key, Weak<tokio::sync::Mutex<State>>>>,
}

pub(super) struct Lane {
    owner: Arc<Lanes>,
    key: Key,
    // Present until Drop takes it while holding the registry lock.
    gate: Option<Arc<tokio::sync::Mutex<State>>>,
    // Each caller retains its existing byte/item admission before allocating
    // a key or joining the FIFO. No idle entry may outlive those reservations.
    _memory: Permit,
}

pub(super) struct Turn<'a> {
    key: &'a Key,
    state: tokio::sync::MutexGuard<'a, State>,
    previous: Option<Successor>,
}

impl Lanes {
    pub(super) fn enter(
        self: &Arc<Self>,
        cell: &CellHistory,
        memory: Permit,
    ) -> Result<Lane, CommandFailure> {
        let key = (cell.cluster, cell.namespace, cell.cell.clone());
        let mut active = self
            .active
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?;
        let gate = match active.get(&key).and_then(Weak::upgrade) {
            Some(gate) => gate,
            None => {
                let gate = Arc::new(tokio::sync::Mutex::new(State::default()));
                active.insert(key.clone(), Arc::downgrade(&gate));
                gate
            }
        };
        drop(active);
        Ok(Lane {
            owner: self.clone(),
            key,
            gate: Some(gate),
            _memory: memory,
        })
    }
}

impl Lane {
    pub(super) async fn lock(&self) -> Turn<'_> {
        let mut state = self
            .gate
            .as_ref()
            .expect("live SQL ingress lane")
            .lock()
            .await;
        // A cancellation or error cannot leave the inherited hint available
        // for another caller. Only a later positive receipt replenishes it.
        let previous = state.next.take();
        Turn {
            key: &self.key,
            state,
            previous,
        }
    }
}

impl Turn<'_> {
    fn matches(&self, cell: &CellHistory) -> bool {
        self.key.0 == cell.cluster && self.key.1 == cell.namespace && self.key.2 == cell.cell
    }

    /// Selection hint only. Every command still validates its destination,
    /// authority and immutable fingerprint; a stale hint needs the ordinary
    /// fresh-quorum consumed-other proof before choosing a different sequence.
    pub(super) fn take_successor(&mut self, cell: &CellHistory) -> Option<u64> {
        let previous = self.previous.take()?;
        (self.matches(cell) && previous.incarnation == cell.incarnation)
            .then_some(previous.sequence)
    }

    /// Call only after validating the exact successful command receipt. This
    /// fixed-size hint shares the already admitted active FIFO and disappears
    /// when its last caller leaves; it creates no idle cell owner or authority.
    pub(super) fn acknowledged(&mut self, cell: &CellHistory, command: CommandId) {
        self.previous = None;
        self.state.next = None;
        if self.matches(cell) && command.client == super::super::sql_ingress_identity::client() {
            self.state.next = command.sequence().checked_add(1).map(|sequence| Successor {
                incarnation: cell.incarnation,
                sequence,
            });
        }
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        // Enter and last-owner removal share this lock, so an arriving caller
        // cannot observe a replacement FIFO while an older waiter still exists.
        let mut active = self
            .owner
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(gate) = self.gate.take() {
            if Arc::strong_count(&gate) == 1 {
                active.remove(&self.key);
            }
            // Release this strong owner before unlocking the registry. If two
            // destructors both observed the other's not-yet-dropped field,
            // neither would remove the now-idle weak entry.
            drop(gate);
        }
    }
}

#[cfg(test)]
#[path = "sql_ingress_lanes_tests.rs"]
mod tests;
