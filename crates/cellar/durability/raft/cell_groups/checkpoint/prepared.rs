// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::CellGroupApplier;
use super::*;
use raft::prelude::Snapshot;

struct Inner {
    applier: CellGroupApplier,
    root: PreparedGroupManifest,
    header: Header,
    snapshot: Snapshot,
    _reservation: Permit,
}

/// Receiver-local complete closure. Clones retain the same admission and physical
/// owners through queued engine work, including a lost publication response.
/// This capability proves storage, not that Raft has accepted the snapshot.
#[derive(Clone)]
pub struct PreparedCellGroupCheckpoint(Arc<Inner>);

impl std::fmt::Debug for PreparedCellGroupCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedCellGroupCheckpoint")
            .field("marker", self.0.root.marker())
            .finish()
    }
}

impl PreparedCellGroupCheckpoint {
    pub(super) fn new(
        applier: CellGroupApplier,
        root: PreparedGroupManifest,
        header: Header,
        reservation: Permit,
    ) -> Result<Self> {
        let snapshot = header.snapshot()?;
        Ok(Self(Arc::new(Inner {
            applier,
            root,
            header,
            snapshot,
            _reservation: reservation,
        })))
    }

    pub fn marker(&self) -> &GroupManifest {
        self.0.root.marker()
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.0.snapshot
    }

    pub fn applier(&self) -> &CellGroupApplier {
        &self.0.applier
    }

    /// Complete physical closure. Admission still requires the engine-selected
    /// marker and the current catalog permit; preparation alone cannot serve.
    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.0.root
    }

    pub(crate) fn root(&self) -> &PreparedGroupManifest {
        &self.0.root
    }

    pub(crate) fn membership(&self) -> &GroupMembership {
        &self.0.header.membership
    }
}

/// Decode persisted group snapshot metadata. This does not recover its physical
/// closure; startup must independently recover every referenced page and image.
pub(crate) fn persisted_marker(
    snapshot: &Snapshot,
    current: &GroupManifest,
    membership: &GroupMembership,
) -> std::result::Result<Option<GroupManifest>, crate::raft::storage::LogError> {
    use crate::raft::storage::LogError;
    if snapshot.data.is_empty() {
        if snapshot.get_metadata().index != 1 || snapshot.get_metadata().term != 1 {
            return Err(LogError::Corrupt(
                "group snapshot lacks complete descriptor".into(),
            ));
        }
        return Ok(None);
    }
    let header = Header::decode(&snapshot.data).map_err(|e| LogError::Corrupt(e.to_string()))?;
    let metadata = snapshot.get_metadata();
    let conf = header
        .configuration()
        .map_err(|e| LogError::Corrupt(e.to_string()))?;
    if header.root.route() != current.route()
        || header.root.position().index.get() != metadata.index
        || header.root.position().term.get() != metadata.term
        || crate::raft::manifest::encode_conf(&conf)
            .map_err(|e| LogError::Corrupt(e.to_string()))?
            != crate::raft::manifest::encode_conf(metadata.get_conf_state())
                .map_err(|e| LogError::Corrupt(e.to_string()))?
        || header.root.position().index > current.position().index
    {
        return Err(LogError::Corrupt("group snapshot metadata binding".into()));
    }
    membership.succeeds(&header.membership)?;
    Ok(Some(header.root))
}
