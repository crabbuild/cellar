// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Active stateless callers share a local FIFO, never a persistent cell owner.

use super::CellHistory;
use crate::raft::budget::Permit;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, CommandFailure, NamespaceId},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

type Key = (ClusterId, NamespaceId, CellId);

#[derive(Default)]
pub(super) struct Lanes {
    active: Mutex<BTreeMap<Key, Weak<tokio::sync::Mutex<()>>>>,
}

pub(super) struct Lane {
    owner: Arc<Lanes>,
    key: Key,
    // Present until Drop takes it while holding the registry lock.
    gate: Option<Arc<tokio::sync::Mutex<()>>>,
    // Each caller retains its existing byte/item admission before allocating
    // a key or joining the FIFO. No idle entry may outlive those reservations.
    _memory: Permit,
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
                let gate = Arc::new(tokio::sync::Mutex::new(()));
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
    pub(super) async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.gate
            .as_ref()
            .expect("live SQL ingress lane")
            .lock()
            .await
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
