// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::{
    CellGroupArchiveStore, CellManifestLeaf, ManifestError, ManifestPageStore, ManifestTree,
};
use super::receive::{State, Target, WireReceiver};
use super::*;
use cellar_logic::cell_group::GroupRoute;
use cellar_ltx::stream_snapshot::ScratchPool;
use std::{path::PathBuf, sync::Mutex};

#[cfg(test)]
#[path = "receive_archive_tests.rs"]
mod tests;

impl Target for CellGroupArchiveStore {
    fn storage(&self) -> &Arc<Mutex<super::super::apply::Storage>> {
        &self.storage
    }

    fn route(&self) -> &GroupRoute {
        CellGroupArchiveStore::route(self)
    }

    fn require_live(&self) -> Result<()> {
        CellGroupArchiveStore::require_live(self)
    }

    fn check_header(&self, _header: &Header) -> Result<()> {
        self.require_live()
    }

    fn check_finish(&self, _header: &Header) -> Result<()> {
        self.require_live()
    }

    fn prepare(
        &self,
        tree: &ManifestTree,
        pages: &mut ManifestPageStore,
        header: &Header,
        image: &mut dyn FnMut(
            &CellManifestLeaf,
        ) -> std::result::Result<CellImagePin, ManifestError>,
    ) -> Result<PreparedGroupManifest> {
        Ok(tree.prepare_archive_complete(&self.owner, pages, header.root.clone(), image)?)
    }
}

impl CellGroupCheckpointPool {
    /// Reserve before opening/scanning an independently owned page inventory.
    /// Includes worst-case inventory and directory metadata in this same bank.
    pub(crate) fn prepare_archive_receive(
        &self,
        max_pages: usize,
    ) -> Result<super::PreparedArchiveReceive> {
        let bytes = max_pages
            .checked_mul(512)
            .and_then(|n| n.checked_add(16384))
            .ok_or(CellGroupCheckpointError::Limit)?;
        Ok(PreparedArchiveReceive {
            pool: self.clone(),
            reservation: State::<CellGroupArchiveStore>::reserve(self, bytes)?,
        })
    }

    /// Receive into private data stores, without a Raft mapping, surviving voter
    /// disk or applied-cut observation. Shares the node's source/receiver quota.
    pub fn receive_archive(
        &self,
        store: CellGroupArchiveStore,
        scratch: ScratchPool,
        directory: PathBuf,
    ) -> Result<CellGroupArchiveCheckpointReceiver> {
        Ok(CellGroupArchiveCheckpointReceiver(
            State::new(self, store, scratch, directory)?,
            false,
        ))
    }
}

pub(crate) struct PreparedArchiveReceive {
    pool: CellGroupCheckpointPool,
    reservation: Permit,
}

impl PreparedArchiveReceive {
    pub(crate) fn receive(
        self,
        store: CellGroupArchiveStore,
        scratch: ScratchPool,
        directory: PathBuf,
    ) -> Result<CellGroupArchiveCheckpointReceiver> {
        Ok(CellGroupArchiveCheckpointReceiver(
            State::with_reservation(&self.pool, store, scratch, directory, self.reservation)?,
            false,
        ))
    }
}

/// Physical archive receiver. It cannot produce the engine-bound checkpoint
/// capability accepted by SnapshotChange or the Raft owner.
pub struct CellGroupArchiveCheckpointReceiver(State<CellGroupArchiveStore>, bool);

impl WireReceiver for CellGroupArchiveCheckpointReceiver {
    fn wire_extent(
        &self,
        cell: &CellId,
        ordinal: u32,
        digest: [u8; 32],
        length: u64,
    ) -> Result<ExtentId> {
        self.0.wire_extent(cell, ordinal, digest, length)
    }

    fn append_chunk(&mut self, chunk: CellGroupCheckpointChunk<'_>) -> Result<()> {
        self.0.append(chunk)
    }
}

impl CellGroupArchiveCheckpointReceiver {
    /// Only private managed replay selects immutable source retention. Public
    /// receivers preserve their original full replay admission.
    pub(in crate::raft) fn for_transition_source(mut self) -> Self {
        self.1 = true;
        self
    }

    pub(in crate::raft) fn transition_source(&self) -> bool {
        self.1
    }

    pub fn append(&mut self, chunk: CellGroupCheckpointChunk<'_>) -> Result<()> {
        self.0.append(chunk)
    }

    pub fn finish(self) -> Result<PreparedCellGroupArchiveCheckpoint> {
        let received = self.0.finish()?;
        let snapshot = received.header.snapshot()?;
        Ok(PreparedCellGroupArchiveCheckpoint(Arc::new(Inner {
            store: received.target,
            root: received.root,
            header: received.header,
            snapshot,
            reservation: Some(received.reservation),
            _retained: None,
        })))
    }
}

struct Inner {
    store: CellGroupArchiveStore,
    root: PreparedGroupManifest,
    header: Header,
    snapshot: raft::prelude::Snapshot,
    reservation: Option<Permit>,
    _retained: Option<crate::raft::budget::RetainedBytes>,
}

/// Complete archive-local closure with no engine publication or serving right.
/// Clones share physical owners and admission, and have a distinct type from a
/// received Raft snapshot. Private owner tokens refuse every live engine.
#[derive(Clone)]
pub struct PreparedCellGroupArchiveCheckpoint(Arc<Inner>);

impl PreparedCellGroupArchiveCheckpoint {
    /// Only a fully joined controller publication may release the receiver
    /// operation. All bytes and immutable physical pins remain charged; public
    /// receivers and live snapshot installation keep their original admission.
    pub(in crate::raft::cell_groups) fn retain_publication(&mut self) -> Result<()> {
        let inner = Arc::get_mut(&mut self.0).ok_or(CellGroupCheckpointError::Busy)?;
        if let Some(reservation) = inner.reservation.take() {
            inner._retained = Some(reservation.retain_all());
        }
        Ok(())
    }

    pub(in crate::raft::cell_groups) fn resume_publication(&mut self) -> Result<()> {
        let inner = Arc::get_mut(&mut self.0).ok_or(CellGroupCheckpointError::Busy)?;
        if let Some(retained) = inner._retained.take() {
            match retained.try_resume() {
                Ok(reservation) => inner.reservation = Some(reservation),
                Err(retained) => {
                    inner._retained = Some(retained);
                    return Err(CellGroupCheckpointError::Busy);
                }
            }
        }
        Ok(())
    }

    pub fn marker(&self) -> &GroupManifest {
        self.0.root.marker()
    }

    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.0.root
    }

    /// Snapshot metadata only; copying it does not grant installation permission.
    pub fn snapshot(&self) -> &raft::prelude::Snapshot {
        &self.0.snapshot
    }

    pub fn store(&self) -> &CellGroupArchiveStore {
        &self.0.store
    }

    pub(crate) fn membership(&self) -> &GroupMembership {
        &self.0.header.membership
    }
}
