//! Command/query consensus contracts for opt-in Raft namespaces.
//!
//! These values describe durable identities and physical update envelopes.
//! Transport, storage and command registration are supplied by the host.

mod class;
pub(crate) mod client;
pub(crate) mod protocol;

pub use crate::raft::archive::ArchiveLimits;

pub use crate::raft::catalog::{
    BirthProposal, CatalogOptions, InitialMember, NamespaceConfig, VoterPolicy,
};
pub use crate::raft::host::{
    RaftArchiveOptions, RaftCellPlacement, RaftCellRegistration, RaftCellsConfig,
    RaftCellsServices, RaftHostOptions, RaftPeerNode,
};
pub use crate::raft::member_state::JoiningMember;
pub use crate::raft::options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions};
pub use crate::raft::transport_options::{NamespaceQueryOrigin, RaftTls, TransportOptions};
pub use crate::raft::version::{
    VersionError, VersionKeys, VersionSigningKey, VersionToken, VersionVerificationKey,
};
pub use crate::storage::{CommandInputs, CommandTransaction};
pub use class::{
    command_request_digest, CommandContext, OperationContract, OperationKind, QueryContext,
    RaftClass, RegistrationError,
};
pub use client::{
    CellStatus, CommandOptions, CommandReceipt, LearnerOptions, LearnerReceipt, QueryConsistency,
    QueryOptions, QueryResult, QueryRouting, RaftCellHandle, RaftCells, RaftCloseReport,
    StaleReadPolicy, UnfinishedCommand,
};

pub use cellar_logic::consensus::{
    ClientId, ClusterId, CommandFailure, CommandId, CommittedVersion, DiskLineage, Durability,
    GroupId, GroupScope, IncarnationId, LogIndex, LogPosition, MemberId, NamespaceId, NodeId,
    NodeIdentity, ProcessGeneration, Term,
};
pub use cellar_logic::NamespaceMode;
pub use protocol::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate, ProtocolError};

/// Unstable host-storage interfaces for integration verification and embedding.
#[doc(hidden)]
pub mod verification {
    pub use crate::raft::apply::{ImageApplier, ImageApplyError, PublishedImage};
    pub use crate::raft::archive::{
        ArchiveDependency, ArchiveError, ArchiveLimits, ArchiveRecoverySet, ArchiveVerifier,
        CellArchive, CommittedArchiveCut,
    };
    pub use crate::raft::cache::{
        CommittedImage, DurableApply, GroupLog, GroupWrite, SnapshotChange,
    };
    pub use crate::raft::catalog::{
        BirthProposal, BootstrapPermit, Catalog, CatalogError, CatalogOptions, GroupBirth,
        InitialMember, NamespaceCatalog, NamespaceConfig, VoterPolicy,
    };
    pub use crate::raft::completion::{
        AcceptedProposal, ApplyCompletion, ApplyTicket, GroupStatus, RuntimeError, RuntimeEvent,
        RuntimeEventKind, SnapshotCompletion,
    };
    pub use crate::raft::execution::{
        query_registered, AppliedExecution, CommandAttempt, CommandAuthority, CommandRequest,
        CommittedReceipt, ExecutionError, ExecutionLimits, ExecutionStatus, PreparedCommand,
        PrivateExecutor, PrivateOptions, RegisteredQueryOutput, StageOutcome, StoredReceipt,
    };
    pub use crate::raft::member_state::{GroupMembership, JoiningMember};
    pub use crate::raft::options::RaftRuntimeOptions;
    pub use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
    pub use crate::raft::runtime::{RaftRuntime, RuntimeEvents};
    pub use crate::raft::snapshot::{
        PreparedSnapshot, SnapshotCheckpoint, SnapshotDownload, SnapshotError, SnapshotLimits,
        SnapshotPackage, SnapshotReference,
    };
    pub use crate::raft::storage::{GroupBootstrap, LogError, ReplicaLog, SyncReceipt};
    pub use crate::raft::transport::{
        Delivery, GroupRoute, Lane, RaftPeer, RaftTransport, TransportError, TransportUsage,
    };
    pub use crate::raft::transport_options::{NamespaceQueryOrigin, RaftTls, TransportOptions};
}
