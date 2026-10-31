// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete group checkpoints and committed tails in immutable object storage. Upload confirmation
//! and independently recovered physical state are separate capabilities. Neither
//! authorizes S3 settlement, log truncation, membership or disaster recovery.

pub use super::checkpoint::{
    CellGroupArchiveCheckpointReceiver, PreparedCellGroupArchiveCheckpoint,
};
use super::{checkpoint::wire, CellGroupCheckpoint, CellGroupCheckpointError, GroupManifest};
use crate::{
    asyncrt::ExecutionDomain,
    raft::budget::{Budget, Permit},
};
use cellar_logic::cell_group::GroupHistory;
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};

mod checkpoint;
mod data_store;
pub use data_store::CellGroupArchiveStore;
mod collection;
mod graph;
mod objects;
mod publication;
pub use collection::{CellGroupArchiveCollectionLimits, CellGroupArchiveCollectionReport};
pub use publication::{CellGroupArchiveCoverage, PreparedCellGroupArchivePublication};
mod upload;
pub use upload::{
    CellGroupArchiveUpload, CellGroupArchiveUploadLimits, CellGroupArchiveUploadPool,
};
mod ownership;
pub use ownership::{
    CellGroupArchiveCreation, CellGroupArchiveHold, CellGroupArchiveIntent,
    CellGroupArchiveRegistry, CellGroupArchiveUploadTicket, PreparedCellGroupArchiveCreation,
};
mod replay;
mod tail;
mod tail_admission;
mod tail_codec;
pub use replay::VerifiedCellGroupArchiveRecovery;
pub use tail::{CellGroupArchiveTailReference, CellGroupArchiveUploadedTail};
pub use tail_admission::{CellGroupArchiveTailLimits, CellGroupArchiveTailPool};

const DOMAIN: &str = "cellar-cell-group-archive-checkpoint-1";
const CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub enum CellGroupArchiveError {
    Invalid(&'static str),
    Limit,
    Busy,
    Stopped,
    Timeout,
    Conflict,
    Expired,
    Unconfirmed,
    MissingRegistry,
    Checkpoint(CellGroupCheckpointError),
    Replay(crate::raft::execution::ExecutionError),
    Storage(object_store::Error),
    Worker(crate::asyncrt::TaskPanic),
}

impl fmt::Display for CellGroupArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cell-group archive: {self:?}")
    }
}

impl std::error::Error for CellGroupArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Checkpoint(e) => Some(e),
            Self::Replay(e) => Some(e),
            Self::Storage(e) => Some(e),
            Self::Worker(e) => Some(e),
            _ => None,
        }
    }
}

impl From<CellGroupCheckpointError> for CellGroupArchiveError {
    fn from(error: CellGroupCheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

type Result<T> = std::result::Result<T, CellGroupArchiveError>;

/// Share one archive pool across this node's group jobs. Clones share admission.
/// Checkpoint source/receiver pools separately account physical closures/scratch.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupArchiveLimits {
    pub max_operations: usize,
    pub max_retained_bytes: usize,
    pub max_index_bytes: usize,
    pub max_chunks: usize,
    pub max_checkpoint_bytes: u64,
    /// Deadline for a complete individual PUT or GET, including its body stream.
    pub io_timeout_ms: u64,
}

impl Default for CellGroupArchiveLimits {
    fn default() -> Self {
        Self {
            max_operations: 2,
            max_retained_bytes: 32 * 1024 * 1024,
            max_index_bytes: 1024 * 1024,
            max_chunks: 4096,
            max_checkpoint_bytes: 256 * 1024 * 1024,
            io_timeout_ms: 10_000,
        }
    }
}

impl CellGroupArchiveLimits {
    fn workspace(self) -> Result<usize> {
        if !(1..=4096).contains(&self.max_operations)
            || !(1024..=8 * 1024 * 1024).contains(&self.max_index_bytes)
            || !(1..=65_536).contains(&self.max_chunks)
            || !(64 * 1024..=1024 * 1024 * 1024).contains(&self.max_checkpoint_bytes)
            || !(1..=60_000).contains(&self.io_timeout_ms)
        {
            return Err(CellGroupArchiveError::Limit);
        }
        // Decoded records, bounded encoded index, GET/PUT windows and transport
        // copies. The physical source or receiver has its own pool reservation.
        self.max_index_bytes
            .checked_mul(6)
            .and_then(|n| n.checked_add(self.max_chunks.checked_mul(2048)?))
            .and_then(|n| n.checked_add(8 * CHUNK_BYTES))
            .ok_or(CellGroupArchiveError::Limit)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellGroupArchiveUsage {
    pub operations: usize,
    pub retained_bytes: usize,
}

/// An untrusted locator, suitable for persistence by a later archive-head owner.
/// Its digest authenticates the index bytes, not their complete recovery closure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellGroupArchiveReference {
    registry: [u8; 32],
    generation: u64,
    digest: [u8; 32],
    length: u64,
}

impl CellGroupArchiveReference {
    pub fn registry(&self) -> [u8; 32] {
        self.registry
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn object(&self) -> objects::Object {
        objects::Object {
            registry: self.registry,
            generation: self.generation,
            digest: self.digest,
            length: self.length,
        }
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn length(&self) -> u64 {
        self.length
    }
}

/// Every referenced object was downloaded, hashed and physically recovered on
/// the destination. The contained checkpoint retains receiver-local pins. This
/// is a checkpoint-only proof, without a tail, published head or remote GC lease.
pub struct VerifiedCellGroupArchiveCheckpoint {
    reference: CellGroupArchiveReference,
    checkpoint: PreparedCellGroupArchiveCheckpoint,
}

impl VerifiedCellGroupArchiveCheckpoint {
    pub fn reference(&self) -> &CellGroupArchiveReference {
        &self.reference
    }

    pub fn checkpoint(&self) -> &PreparedCellGroupArchiveCheckpoint {
        &self.checkpoint
    }

    pub fn into_checkpoint(self) -> PreparedCellGroupArchiveCheckpoint {
        self.checkpoint
    }
}

/// Group-owned archive namespace. No synthetic cell identity is involved.
#[derive(Clone)]
pub struct CellGroupArchive {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    history: GroupHistory,
    limits: CellGroupArchiveLimits,
    budget: Arc<Budget>,
    workspace: usize,
}

/// Node-wide archive admission, shared by every group binding and both transfer
/// directions. Holding a group handle consumes no operation slot or I/O buffer.
#[derive(Clone)]
pub struct CellGroupArchivePool {
    limits: CellGroupArchiveLimits,
    budget: Arc<Budget>,
    workspace: usize,
}

impl CellGroupArchivePool {
    pub fn new(limits: CellGroupArchiveLimits) -> Result<Self> {
        let workspace = limits.workspace()?;
        if limits.max_retained_bytes < workspace {
            return Err(CellGroupArchiveError::Limit);
        }
        Ok(Self {
            limits,
            budget: Budget::new(limits.max_retained_bytes, limits.max_operations),
            workspace,
        })
    }

    pub fn for_group(
        &self,
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        history: GroupHistory,
    ) -> Result<CellGroupArchive> {
        if prefix.as_ref().len() > 4096 {
            return Err(CellGroupArchiveError::Limit);
        }
        let mut binding = Sha256::new();
        binding.update(b"cellar-cell-group-archive-history-1\0");
        binding.update(history.cluster.bytes());
        binding.update(history.namespace.bytes());
        binding.update(history.group.bytes());
        binding.update(history.incarnation.bytes());
        let prefix = prefix.child(DOMAIN).child(hex::encode(binding.finalize()));
        Ok(CellGroupArchive {
            store,
            prefix,
            history,
            limits: self.limits,
            budget: self.budget.clone(),
            workspace: self.workspace,
        })
    }

    pub fn usage(&self) -> CellGroupArchiveUsage {
        let (retained_bytes, operations) = self.budget.usage();
        CellGroupArchiveUsage {
            operations,
            retained_bytes,
        }
    }
}

impl CellGroupArchive {
    /// Advisory node-wide counters shared with every binding from this pool.
    pub fn usage(&self) -> CellGroupArchiveUsage {
        let (retained_bytes, operations) = self.budget.usage();
        CellGroupArchiveUsage {
            operations,
            retained_bytes,
        }
    }

    fn admit(&self) -> Result<Arc<Permit>> {
        self.budget
            .acquire(self.workspace)
            .map(Arc::new)
            .ok_or(CellGroupArchiveError::Busy)
    }
}
