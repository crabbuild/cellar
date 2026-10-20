//! Command/query consensus contracts for opt-in Raft namespaces.
//!
//! These values describe durable identities and physical update envelopes.
//! Transport, storage and command registration are supplied by the host.

pub(crate) mod protocol;

pub use cellar_logic::consensus::{
    ClientId, CommandFailure, CommandId, CommittedVersion, Durability, GroupScope,
};
pub use cellar_logic::NamespaceMode;
pub use protocol::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate, ProtocolError};

/// Unstable host-storage interfaces for integration verification and embedding.
#[doc(hidden)]
pub mod verification {
    pub use crate::raft::cache::{
        CommittedImage, DurableApply, GroupLog, GroupWrite, SnapshotChange,
    };
    pub use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
    pub use crate::raft::storage::{GroupBootstrap, LogError, ReplicaLog, SyncReceipt};
}
