// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One disposable sequence candidate per stateless invocation. This retains no
//! receipt, storage pin or serving authority. Only an exact successful command
//! receipt may replenish it; every subsequent command still needs admission and
//! the target's required proof. Conflicts require the ordinary fresh quorum read.

use cellar_logic::cell_group::CellHistory;
use sha2::{Digest, Sha256};
use std::sync::Mutex;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Key([u8; 32]);

impl Key {
    pub(crate) fn new(history: &CellHistory, schema: [u8; 32]) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"cellar-stateless-ingress-cursor-1");
        hash.update(history.cluster.bytes());
        hash.update(history.namespace.bytes());
        hash.update((history.cell.as_str().len() as u64).to_be_bytes());
        hash.update(history.cell.as_str().as_bytes());
        hash.update(history.incarnation.bytes());
        // The stream spans methods, but never a different class contract.
        hash.update(schema);
        Self(hash.finalize().into())
    }
}

struct Hint {
    key: Key,
    next: u64,
    until_ms: u64,
}

/// Fixed-size control state within an admitted event, allocated lazily for its
/// first stateless Raft call. No target strings or growing map survive a call.
#[derive(Default)]
pub(crate) struct Cursor(Mutex<Option<Hint>>);

impl Cursor {
    /// Taking consumes the candidate before submission. Cancellation, refusal or
    /// an unknown result cannot restore it or advance it to a different sequence.
    pub(crate) fn take(&self, key: Key, now_ms: u64) -> Option<u64> {
        self.0
            .lock()
            .unwrap()
            .take()
            .filter(|hint| hint.key == key && now_ms < hint.until_ms)
            .map(|hint| hint.next)
    }

    /// Call only after validating the exact command receipt and minimum proof.
    /// Expiry is that call's original monotonic deadline, never a new TTL.
    pub(crate) fn remember(&self, key: Key, sequence: u64, until_ms: u64, now_ms: u64) {
        *self.0.lock().unwrap() =
            sequence
                .checked_add(1)
                .filter(|_| now_ms < until_ms)
                .map(|next| Hint {
                    key,
                    next,
                    until_ms,
                });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_logic::cell_group::CellId;
    use cellar_logic::consensus::{ClusterId, IncarnationId, NamespaceId};

    fn history() -> CellHistory {
        CellHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: CellId::new("__SQLDatabase:test").unwrap(),
            incarnation: IncarnationId::new([3; 16]).unwrap(),
        }
    }

    #[test]
    fn candidate_is_consumed_once_and_has_no_cross_invocation_lifetime() {
        let cursor = Cursor::default();
        let key = Key::new(&history(), [4; 32]);
        assert_eq!(cursor.take(key, 10), None);
        cursor.remember(key, 7, 100, 10);
        assert_eq!(Cursor::default().take(key, 20), None);
        assert_eq!(cursor.take(key, 20), Some(8));
        // A submitted call that fails/cancels supplies no new receipt. There is
        // no candidate left for that unknown call or a subsequent call to reuse.
        assert_eq!(cursor.take(key, 21), None);
        cursor.remember(key, 8, 200, 30);
        assert_eq!(cursor.take(key, 40), Some(9));
    }

    #[test]
    fn identity_schema_and_original_deadline_bound_every_candidate() {
        let cursor = Cursor::default();
        let history = history();
        let key = Key::new(&history, [4; 32]);
        let mut other = history.clone();
        other.cluster = ClusterId::new([5; 16]).unwrap();
        let mut namespace = history.clone();
        namespace.namespace = NamespaceId::new([5; 16]).unwrap();
        let mut cell = history.clone();
        cell.cell = CellId::new("__SQLDatabase:other").unwrap();
        let mut incarnation = history.clone();
        incarnation.incarnation = IncarnationId::new([5; 16]).unwrap();
        for wrong in [
            Key::new(&other, [4; 32]),
            Key::new(&namespace, [4; 32]),
            Key::new(&cell, [4; 32]),
            Key::new(&incarnation, [4; 32]),
            Key::new(&history, [5; 32]),
        ] {
            cursor.remember(key, 7, 100, 10);
            assert_eq!(cursor.take(wrong, 20), None);
            assert_eq!(cursor.take(key, 21), None);
        }
        cursor.remember(key, 7, 100, 10);
        assert_eq!(cursor.take(key, 100), None);
        cursor.remember(key, 7, 100, 100);
        assert_eq!(cursor.take(key, 101), None);
        cursor.remember(key, u64::MAX, 200, 110);
        assert_eq!(cursor.take(key, 120), None);
    }
}
