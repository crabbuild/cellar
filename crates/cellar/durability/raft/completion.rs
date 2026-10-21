//! Process-local effect tickets bind completions to one exact worker and entry.

use super::budget::Permit;
use super::cache::{CommittedImage, DurableApply};
use super::storage::LogError;
use ::raft::prelude::{Entry, Message, Snapshot};
use cellar_logic::consensus::{
    LogIndex, LogPosition, MemberId, OperationId, ReplicaIdentity, Term,
};
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Storage(LogError),
    Invalid(String),
    Protocol(String),
    NotLeader,
    NotReady,
    NotFound,
    AlreadyAttached,
    Overloaded,
    StaleCompletion,
    Closed,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft runtime: {self:?}")
    }
}

impl std::error::Error for RuntimeError {}

impl From<LogError> for RuntimeError {
    fn from(error: LogError) -> Self {
        Self::Storage(error)
    }
}

impl From<::raft::Error> for RuntimeError {
    fn from(error: ::raft::Error) -> Self {
        Self::Protocol(error.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct ApplyTicket {
    pub(crate) owner: Arc<()>,
    pub(crate) replica: ReplicaIdentity,
    pub(crate) operation: OperationId,
    pub(crate) position: LogPosition,
    pub(crate) digest: [u8; 32],
}

impl PartialEq for ApplyTicket {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owner, &other.owner)
            && self.replica == other.replica
            && self.operation == other.operation
            && self.position == other.position
            && self.digest == other.digest
    }
}

impl Eq for ApplyTicket {}

impl ApplyTicket {
    pub fn replica(&self) -> &ReplicaIdentity {
        &self.replica
    }

    pub fn operation(&self) -> OperationId {
        self.operation
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn entry_digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// An immutable image already verified, fsynced and renamed by the apply worker.
#[derive(Clone, Debug)]
pub struct ApplyCompletion {
    pub ticket: ApplyTicket,
    pub image: CommittedImage,
}

#[derive(Clone, Debug)]
pub struct SnapshotCompletion {
    pub ticket: ApplyTicket,
    pub applied: DurableApply,
    /// Physical producers supply this capability for the post-sync retention
    /// handoff. None is reserved for trusted mechanical engine fixtures.
    pub reference: Option<super::snapshot::SnapshotReference>,
}

/// Proposal admission is provisional; it proves neither commit nor command success.
#[derive(Clone, Debug)]
pub struct AcceptedProposal {
    pub replica: ReplicaIdentity,
    pub position: LogPosition,
    pub entry_digest: [u8; 32],
}

/// A request to Raft's current leader, never an external routing change.
/// Acceptance does not establish that the target won an election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeadershipTransfer {
    pub target: MemberId,
    pub expected_term: Term,
    pub expected_revision: LogIndex,
}

/// Leader-local replication observations. These are neither apply nor quorum
/// proofs; the owner rechecks actual progress when accepting a transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationStatus {
    pub replica: ReplicaIdentity,
    pub term: Term,
    pub membership_revision: LogIndex,
    pub committed: LogIndex,
    pub members: Vec<MemberProgress>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberProgress {
    pub member: MemberId,
    pub matched: LogIndex,
    pub voter: bool,
    pub recent_active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupStatus {
    pub membership_revision: LogIndex,
    pub member_high_water: u64,
    pub replica: ReplicaIdentity,
    pub term: Term,
    pub leader: Option<u64>,
    pub is_leader: bool,
    pub protocol_ready: bool,
    pub persisted: LogIndex,
    pub committed: LogIndex,
    pub applied: LogPosition,
    pub checkpoint: LogPosition,
    pub awaiting_ready: bool,
    pub awaiting_apply: bool,
    pub election_timeout: usize,
    /// Local monotonic observation of the current leader/source. A recent
    /// observation is a stale-read policy input, never a strong-read proof.
    pub source_observed_ms: Option<u64>,
}

impl GroupStatus {
    /// Project every protocol admission predicate into the command core. The
    /// host allocates `sequence` before requesting this status, and supplies
    /// separately verified image/archive progress to that core.
    pub fn leadership_observation(
        &self,
        sequence: u64,
    ) -> Result<cellar_logic::raft_command::LeadershipObservation, RuntimeError> {
        Ok(cellar_logic::raft_command::LeadershipObservation {
            replica: self.replica.clone(),
            sequence,
            term: self.term,
            leader: self
                .leader
                .map(MemberId::new)
                .transpose()
                .map_err(|error| RuntimeError::Invalid(error.to_string()))?,
            is_leader: self.is_leader,
            protocol_ready: self.protocol_ready,
            awaiting_ready: self.awaiting_ready,
            awaiting_apply: self.awaiting_apply,
            barrier: self.applied,
        })
    }
}

#[derive(Debug)]
pub enum RuntimeEventKind {
    Persisted {
        replica: ReplicaIdentity,
        sequence: u64,
        committed: LogIndex,
        light_commit: bool,
    },
    Message {
        replica: ReplicaIdentity,
        message: Message,
    },
    Apply {
        ticket: ApplyTicket,
        entry: Entry,
    },
    Snapshot {
        ticket: ApplyTicket,
        snapshot: Snapshot,
        source: u64,
    },
    Applied {
        ticket: ApplyTicket,
        applied: DurableApply,
    },
    SnapshotInstalled {
        ticket: ApplyTicket,
        applied: DurableApply,
    },
    /// Cancellation is durable; the image and checkpoint remain at their old
    /// prefix. Any older physical apply emits Applied before this event.
    SnapshotAborted {
        ticket: ApplyTicket,
        applied: DurableApply,
    },
    Checkpointed {
        ticket: ApplyTicket,
    },
    Failed {
        replica: ReplicaIdentity,
        error: RuntimeError,
    },
    ReadIndex {
        replica: ReplicaIdentity,
        index: LogIndex,
        context: Vec<u8>,
    },
}

/// Admission remains charged while this event is in either runtime queue.
pub struct RuntimeEvent {
    pub kind: RuntimeEventKind,
    pub(crate) _permit: std::sync::Arc<Permit>,
}

impl RuntimeEvent {
    /// A deferred apply retains the same admission until both the blocking
    /// effect and its retry owner release the immutable entry bytes.
    pub(crate) fn clone_apply(&self) -> Option<Self> {
        let RuntimeEventKind::Apply { ticket, entry } = &self.kind else {
            return None;
        };
        Some(Self {
            kind: RuntimeEventKind::Apply {
                ticket: ticket.clone(),
                entry: entry.clone(),
            },
            _permit: self._permit.clone(),
        })
    }
}

impl RuntimeEventKind {
    pub(crate) fn bytes(&self) -> usize {
        4096 + match self {
            Self::Message { message, .. } => message.compute_size() as usize,
            Self::Apply { entry, .. } => entry.compute_size() as usize,
            Self::Snapshot { snapshot, .. } => snapshot.compute_size() as usize,
            Self::ReadIndex { context, .. } => context.len(),
            Self::Applied { .. }
            | Self::Checkpointed { .. }
            | Self::SnapshotInstalled { .. }
            | Self::SnapshotAborted { .. }
            | Self::Failed { .. }
            | Self::Persisted { .. } => 1024,
        }
    }
}

pub(crate) fn entry_position(entry: &Entry) -> LogPosition {
    LogPosition {
        index: LogIndex::new(entry.index),
        term: Term::new(entry.term),
    }
}

pub(crate) fn entry_digest(entry: &Entry) -> Result<[u8; 32], RuntimeError> {
    Ok(Sha256::digest(
        entry
            .write_to_bytes()
            .map_err(|e| RuntimeError::Protocol(e.to_string()))?,
    )
    .into())
}
