//! Command/query consensus contracts for opt-in Raft namespaces.
//!
//! These values describe durable identities and physical update envelopes.
//! Transport, storage and command registration are supplied by the host.

mod protocol;

pub use cellar_logic::consensus::{
    ClientId, CommandFailure, CommandId, CommittedVersion, Durability, GroupScope,
};
pub use cellar_logic::NamespaceMode;
pub use protocol::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate, ProtocolError};
