// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local checkpoint compaction follows independently verified archive coverage.
use super::{CellGroupArchiveCoverage, GroupManifest};
use crate::raft::{cache::GroupLog, storage::LogError};
use cellar_logic::cell_group::GroupHistory;
use std::sync::Mutex;

struct State {
    required: bool,
    covered: Option<GroupManifest>,
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
            }),
        }
    }

    pub(crate) fn require(&self) -> Result<(), LogError> {
        let mut state = self.state.lock().map_err(|_| LogError::Closed)?;
        state.required = true;
        state.covered = None;
        Ok(())
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
        Ok(!state.required
            || state.covered.as_ref().is_some_and(|covered| {
                covered.route() == marker.route()
                    && covered.position().index >= marker.position().index
                    && (covered.position().index != marker.position().index || covered == marker)
            }))
    }
}
