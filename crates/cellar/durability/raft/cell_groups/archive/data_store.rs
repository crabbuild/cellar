// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private physical preparation, without a Raft mapping or voter identity.
use super::super::{apply::Storage, root::ArchiveManifestOwner, *};
use crate::application::consensus::group_protocol::GroupEntryLimits;
use crate::raft::execution::ExecutionError;
use cellar_logic::cell_group::GroupRoute;
use std::sync::{Arc, Mutex};

/// Owns archive verification pages/images for one expected route. Open dedicated
/// physical directories on the owning blocking domain, separately from serving
/// stores. No ReplicaLog, member claim,
/// bootstrap permission or application callback is created or required.
#[derive(Clone)]
pub struct CellGroupArchiveStore {
    route: GroupRoute,
    pub(in crate::raft::cell_groups) storage: Arc<Mutex<Storage>>,
    pub(in crate::raft::cell_groups) owner: Arc<ArchiveManifestOwner>,
}

impl CellGroupArchiveStore {
    pub fn new(
        route: GroupRoute,
        pages: ManifestPageStore,
        images: CellGroupApplyImages,
        manifest_limits: ManifestLimits,
        entry_limits: GroupEntryLimits,
        max_effect_rows: u32,
    ) -> Result<Self, ExecutionError> {
        let state = Storage::new(
            route.clone(),
            pages,
            images,
            manifest_limits,
            entry_limits,
            max_effect_rows,
        )?;
        Ok(Self {
            route,
            storage: Arc::new(Mutex::new(state)),
            owner: Arc::new(ArchiveManifestOwner::new()),
        })
    }

    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    /// Refuse further receives/replay. Entered blocking work keeps its physical
    /// pins/admission until it exits; its execution domain owns the join.
    pub fn close(&self) {
        self.owner.close();
    }

    pub(in crate::raft::cell_groups) fn require_live(
        &self,
    ) -> Result<(), CellGroupCheckpointError> {
        self.owner
            .require_live()
            .map_err(CellGroupCheckpointError::from)
    }
}
