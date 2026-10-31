// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use raft::prelude::Snapshot;

struct Inner {
    source: CellGroupCheckpoint,
    snapshot: Snapshot,
}

/// A captured local cut. It retains source admission and physical pins through
/// publication; only the matching engine completion establishes a checkpoint.
#[derive(Clone)]
pub struct PreparedLocalCellGroupCheckpoint(Arc<Inner>);

impl std::fmt::Debug for PreparedLocalCellGroupCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedLocalCellGroupCheckpoint")
            .field("marker", self.marker())
            .finish()
    }
}

pub(super) fn prepare(source: CellGroupCheckpoint) -> Result<PreparedLocalCellGroupCheckpoint> {
    if source.started {
        return Err(CellGroupCheckpointError::Invalid("source already streamed"));
    }
    let snapshot = source.snapshot()?;
    Ok(PreparedLocalCellGroupCheckpoint(Arc::new(Inner {
        source,
        snapshot,
    })))
}

impl PreparedLocalCellGroupCheckpoint {
    pub fn marker(&self) -> &GroupManifest {
        self.0.source.marker()
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.0.snapshot
    }

    pub(crate) fn root(&self) -> &PreparedGroupManifest {
        &self.0.source.root
    }

    pub(crate) fn membership(&self) -> Result<GroupMembership> {
        Ok(Header::decode(&self.0.source.header)?.membership)
    }
}
