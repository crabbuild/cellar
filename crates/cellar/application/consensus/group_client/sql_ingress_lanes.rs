// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Active stateless callers share a local FIFO, never a persistent cell owner.

use super::{
    sql_ingress_hints::{Hint, Hints, Target},
    CellHistory,
};
use crate::raft::budget::Permit;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, CommandFailure, CommandId, NamespaceId},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

type Key = (ClusterId, NamespaceId, CellId);

#[derive(Default)]
struct State {
    next: Option<Hint>,
}

#[derive(Default)]
pub(super) struct Lanes {
    hints: Hints,
    active: Mutex<BTreeMap<Key, Weak<tokio::sync::Mutex<State>>>>,
}

pub(super) struct Lane {
    owner: Arc<Lanes>,
    target: Target,
    key: Key,
    // Present until Drop takes it while holding the registry lock.
    gate: Option<Arc<tokio::sync::Mutex<State>>>,
    // Each caller retains its existing byte/item admission before allocating
    // a key or joining the FIFO. No idle entry may outlive those reservations.
    _memory: Permit,
}

pub(super) struct Turn<'a> {
    owner: &'a Lanes,
    target: Target,
    key: &'a Key,
    state: tokio::sync::MutexGuard<'a, State>,
    previous: Option<Hint>,
    previous_node: Option<Hint>,
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
            target: Target::new(cell),
            key,
            gate: Some(gate),
            _memory: memory,
        })
    }

    pub(super) fn close_hints(&self) {
        self.hints.close();
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
        let previous_node = self.owner.hints.take(self.target);
        Turn {
            owner: &self.owner,
            target: self.target,
            previous_node,
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
    pub(super) fn take_successor(
        &mut self,
        cell: &CellHistory,
        schema: [u8; 32],
        now_ms: u64,
    ) -> Option<u64> {
        self.previous.take()?.sequence(cell, schema, now_ms)
    }

    pub(super) fn take_node_successor(
        &mut self,
        cell: &CellHistory,
        schema: [u8; 32],
        now_ms: u64,
    ) -> Option<u64> {
        self.previous_node.take()?.sequence(cell, schema, now_ms)
    }

    /// Publish only after the original cancellation/deadline selection delivers
    /// the validated exact receipt. Keep the FIFO turn through this publication
    /// so a later caller cannot be overtaken by a delayed predecessor's hint.
    pub(super) fn acknowledged(
        &mut self,
        cell: &CellHistory,
        command: CommandId,
        schema: [u8; 32],
        until_ms: u64,
        now_ms: u64,
    ) {
        self.previous = None;
        self.previous_node = None;
        self.state.next = None;
        if self.matches(cell) && command.client == super::super::sql_ingress_identity::client() {
            let hint = Hint::new(cell, schema, command.sequence(), until_ms, now_ms);
            self.state.next = hint;
            if let Some(hint) = hint {
                self.owner.hints.remember(self.target, hint, now_ms);
            }
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

#[cfg(test)]
#[path = "sql_ingress_idle_tests.rs"]
mod idle_tests;
