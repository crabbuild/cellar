// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::{
    CellGroupArchiveStore, CellManifestLeaf, ManifestError, ManifestPageStore, ManifestTree,
};
use super::receive::{State, Target, WireReceiver};
use super::*;
use cellar_logic::cell_group::GroupRoute;
use cellar_ltx::stream_snapshot::ScratchPool;
use std::{path::PathBuf, sync::Mutex};

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
    /// Receive into private data stores, without a Raft mapping, surviving voter
    /// disk or applied-cut observation. Shares the node's source/receiver quota.
    pub fn receive_archive(
        &self,
        store: CellGroupArchiveStore,
        scratch: ScratchPool,
        directory: PathBuf,
    ) -> Result<CellGroupArchiveCheckpointReceiver> {
        Ok(CellGroupArchiveCheckpointReceiver(State::new(
            self, store, scratch, directory,
        )?))
    }
}

/// Physical archive receiver. It cannot produce the engine-bound checkpoint
/// capability accepted by SnapshotChange or the Raft owner.
pub struct CellGroupArchiveCheckpointReceiver(State<CellGroupArchiveStore>);

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
            _reservation: received.reservation,
        })))
    }
}

struct Inner {
    store: CellGroupArchiveStore,
    root: PreparedGroupManifest,
    header: Header,
    snapshot: raft::prelude::Snapshot,
    _reservation: Permit,
}

/// Complete archive-local closure with no engine publication or serving right.
/// Clones share physical owners and admission, and have a distinct type from a
/// received Raft snapshot. Private owner tokens refuse every live engine.
#[derive(Clone)]
pub struct PreparedCellGroupArchiveCheckpoint(Arc<Inner>);

impl PreparedCellGroupArchiveCheckpoint {
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
