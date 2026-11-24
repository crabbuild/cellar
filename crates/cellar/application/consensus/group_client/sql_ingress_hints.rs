// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Discardable advice between independent requests, never command authority.

use super::super::ingress_cursor::Key as Scope;
use super::CellHistory;
use sha2::{Digest, Sha256};
use std::sync::Mutex;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Target([u8; 32]);

impl Target {
    pub(super) fn new(cell: &CellHistory) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"cellar-node-ingress-target-1");
        hash.update(cell.cluster.bytes());
        hash.update(cell.namespace.bytes());
        hash.update((cell.cell.as_str().len() as u64).to_be_bytes());
        hash.update(cell.cell.as_str().as_bytes());
        Self(hash.finalize().into())
    }
}

#[derive(Clone, Copy)]
pub(super) struct Hint {
    scope: Scope,
    next: u64,
    until_ms: u64,
}

impl Hint {
    pub(super) fn new(
        cell: &CellHistory,
        schema: [u8; 32],
        sequence: u64,
        until_ms: u64,
        now_ms: u64,
    ) -> Option<Self> {
        sequence
            .checked_add(1)
            .filter(|_| now_ms < until_ms)
            .map(|next| Self {
                scope: Scope::new(cell, schema),
                next,
                until_ms,
            })
    }

    pub(super) fn sequence(self, cell: &CellHistory, schema: [u8; 32], now_ms: u64) -> Option<u64> {
        (self.scope == Scope::new(cell, schema) && now_ms < self.until_ms).then_some(self.next)
    }
}

#[derive(Clone, Copy)]
struct Entry {
    target: Target,
    hint: Hint,
}

#[derive(Default)]
struct State {
    entries: [Option<Entry>; 4],
    next: usize,
    closed: bool,
}

/// Four inline scalar/hash slots per node. This owns no target strings,
/// admitted work, per-cell owner, payload, storage pin, task or authority.
/// Replacement, expiry or poisoning can only force the ordinary quorum query.
#[derive(Default)]
pub(super) struct Hints(Mutex<State>);

impl Hints {
    /// Consume under the matching active FIFO before any work. A cancelled
    /// owner cannot leave advice available for an unknown command's successor.
    pub(super) fn take(&self, target: Target) -> Option<Hint> {
        let mut state = self.0.lock().ok()?;
        if state.closed {
            return None;
        }
        state
            .entries
            .iter_mut()
            .find(|slot| slot.is_some_and(|e| e.target == target))?
            .take()
            .map(|e| e.hint)
    }

    /// Only the original successful caller may replenish, while still holding
    /// its FIFO turn. The deadline is inherited unchanged, never extended.
    pub(super) fn remember(&self, target: Target, hint: Hint, now_ms: u64) {
        let Ok(mut state) = self.0.lock() else { return };
        if state.closed {
            return;
        }
        for slot in &mut state.entries {
            if slot.is_some_and(|e| e.target == target || now_ms >= e.hint.until_ms) {
                *slot = None;
            }
        }
        let index = state
            .entries
            .iter()
            .position(Option::is_none)
            .unwrap_or(state.next);
        state.entries[index] = Some(Entry { target, hint });
        state.next = (index + 1) % state.entries.len();
    }

    pub(super) fn close(&self) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        state.entries.fill(None);
    }
}
