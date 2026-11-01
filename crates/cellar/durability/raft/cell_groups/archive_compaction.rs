// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local checkpoint compaction follows independently verified archive coverage.
use super::{CellGroupArchiveCoverage, GroupManifest};
use crate::raft::{cache::GroupLog, storage::LogError};
use cellar_logic::cell_group::GroupHistory;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

struct State {
    required: bool,
    covered: Option<GroupManifest>,
    admitted: usize,
    activations: usize,
}

impl State {
    fn permits(&self, marker: &GroupManifest) -> bool {
        self.activations == 0
            && (!self.required
                || self.covered.as_ref().is_some_and(|covered| {
                    covered.route() == marker.route()
                        && covered.position().index >= marker.position().index
                        && (covered.position().index != marker.position().index
                            || covered == marker)
                }))
    }
}

/// Admission already entered under the preceding policy. Archive activation
/// retries until both the owner and entered storage work have released the same
/// cut-bound lease. Storage can finish this work after new admission is closed.
pub(crate) struct ArchiveCompactionAdmission {
    gate: Arc<ArchiveCompaction>,
    cut: GroupManifest,
}

// The existing 4 KiB checkpoint metadata charge covers this lease, including
// both bounded routing-key strings and the Arc allocation's reference counts.
const _: () = assert!(
    std::mem::size_of::<ArchiveCompactionAdmission>()
        + 2 * cellar_logic::cell_group::MAX_CELL_ID_BYTES
        + 2 * std::mem::size_of::<usize>()
        <= 4096
);

impl ArchiveCompactionAdmission {
    pub(crate) fn validate(
        &self,
        gate: &Arc<ArchiveCompaction>,
        cut: &GroupManifest,
    ) -> Result<(), LogError> {
        if !Arc::ptr_eq(&self.gate, gate) || &self.cut != cut {
            return Err(LogError::WrongIdentity);
        }
        let state = self.gate.state.lock().map_err(|_| LogError::Closed)?;
        if state.admitted == 0 {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }
}

impl Drop for ArchiveCompactionAdmission {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.admitted -= 1;
        }
    }
}

/// Owned by the controller's registration, and by an entered metadata write.
/// Dropping an unentered intent restores the previous policy. Successful sync
/// makes the requirement irreversible before releasing the admission barrier.
pub(crate) struct ArchiveActivation {
    gate: Arc<ArchiveCompaction>,
    pending: AtomicBool,
    prepared: AtomicBool,
}

impl ArchiveActivation {
    pub(crate) fn drained(&self, gate: &Arc<ArchiveCompaction>) -> Result<(), LogError> {
        if !Arc::ptr_eq(&self.gate, gate) {
            return Err(LogError::WrongIdentity);
        }
        let mut state = self.gate.state.lock().map_err(|_| LogError::Closed)?;
        if !self.pending.load(Ordering::Relaxed) {
            return if state.required {
                Ok(())
            } else {
                Err(LogError::WrongIdentity)
            };
        }
        if state.activations == 0 {
            return Err(LogError::WrongIdentity);
        }
        if state.admitted != 0 {
            return Err(LogError::Overloaded);
        }
        if !self.prepared.swap(true, Ordering::Relaxed) {
            // Reset before I/O, once per intent. A delayed duplicate metadata
            // completion must not discard coverage verified in the meantime.
            state.covered = None;
        }
        Ok(())
    }

    /// Call only after selecting the durable local requirement, or rechecking
    /// an existing durable bit. No failed/uncertain write may complete intent.
    pub(crate) fn published(&self, gate: &Arc<ArchiveCompaction>) -> Result<(), LogError> {
        if !Arc::ptr_eq(&self.gate, gate) {
            return Err(LogError::WrongIdentity);
        }
        let mut state = self.gate.state.lock().map_err(|_| LogError::Closed)?;
        if !self.pending.load(Ordering::Relaxed) {
            return if state.required {
                Ok(())
            } else {
                Err(LogError::WrongIdentity)
            };
        }
        if state.activations == 0 || state.admitted != 0 || !self.prepared.load(Ordering::Relaxed) {
            return Err(LogError::WrongIdentity);
        }
        state.required = true;
        self.pending.store(false, Ordering::Relaxed);
        state.activations -= 1;
        Ok(())
    }
}

impl Drop for ArchiveActivation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            if self.pending.swap(false, Ordering::Relaxed) {
                state.activations -= 1;
            }
        }
    }
}

/// Shared by every clone of one engine-local group. The requirement survives
/// restart in the group record; the frontier never survives without rechecking
/// the actual remote hold. No replay workspace or per-cell state is retained.
pub(crate) struct ArchiveCompaction {
    state: Mutex<State>,
}

impl ArchiveCompaction {
    pub(crate) fn new(required: bool) -> Self {
        Self {
            state: Mutex::new(State {
                required,
                covered: None,
                admitted: 0,
                activations: 0,
            }),
        }
    }

    pub(crate) fn begin_activation(self: &Arc<Self>) -> Result<Arc<ArchiveActivation>, LogError> {
        let mut state = self.state.lock().map_err(|_| LogError::Closed)?;
        state.activations = state
            .activations
            .checked_add(1)
            .ok_or(LogError::Overloaded)?;
        Ok(Arc::new(ArchiveActivation {
            gate: self.clone(),
            pending: AtomicBool::new(true),
            prepared: AtomicBool::new(false),
        }))
    }

    /// Called only while the controller owns its journaled current remote hold.
    /// This historical frontier grants neither an S3 response nor serving rights.
    pub(in crate::raft::cell_groups) fn confirm(
        &self,
        group: &GroupLog<GroupHistory>,
        coverage: &CellGroupArchiveCoverage,
    ) -> Result<(), LogError> {
        let cache = group.read()?;
        let current = cache
            .group_manifest
            .as_ref()
            .ok_or(LogError::WrongIdentity)?;
        let marker = coverage.marker();
        let position = marker.position();
        if !cache.archive_required
            || marker.route() != current.route()
            || position.index > current.position().index
            || (position.index == current.position().index && marker != current)
        {
            return Err(LogError::WrongIdentity);
        }
        // An old pending intent can reconcile behind a checkpoint selected
        // before archival was enabled. Keep compaction closed until a newer
        // verified cut reaches the retained local history; do not quarantine a
        // valid historical publication or invent its missing local term.
        if position.index.get() < cache.snapshot.get_metadata().index {
            return Ok(());
        }
        if cache.term(position.index.get()) != Some(position.term.get()) {
            return Err(LogError::WrongIdentity);
        }
        let mut state = self.state.lock().map_err(|_| LogError::Closed)?;
        if !state.required {
            return Err(LogError::WrongIdentity);
        }
        if let Some(previous) = &state.covered {
            if previous.position().index > position.index {
                return Ok(());
            }
            if previous.position().index == position.index && previous != marker {
                return Err(LogError::WrongIdentity);
            }
        }
        state.covered = Some(marker.clone());
        Ok(())
    }

    pub(crate) fn permits(&self, marker: &GroupManifest) -> Result<bool, LogError> {
        let state = self.state.lock().map_err(|_| LogError::Closed)?;
        Ok(state.permits(marker))
    }

    pub(crate) fn admit(
        self: &Arc<Self>,
        marker: &GroupManifest,
    ) -> Result<Option<Arc<ArchiveCompactionAdmission>>, LogError> {
        let mut state = self.state.lock().map_err(|_| LogError::Closed)?;
        if !state.permits(marker) {
            return Ok(None);
        }
        state.admitted = state.admitted.checked_add(1).ok_or(LogError::Overloaded)?;
        Ok(Some(Arc::new(ArchiveCompactionAdmission {
            gate: self.clone(),
            cut: marker.clone(),
        })))
    }
}
