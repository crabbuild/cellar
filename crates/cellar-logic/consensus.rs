// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pure contracts between the Raft adapter and the cell authority/output gates.
//!
//! Log persistence, elections, image verification and archive verification are
//! effects. This module accepts their scoped observations and never infers one
//! proof from another. In particular, a Fleet output ticket is not a log index.

use std::fmt;

macro_rules! identity {
    ($($name:ident),+ $(,)?) => {$ (
        #[doc = concat!("A nonzero, opaque ", stringify!($name), ".")]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 16]);

        impl $name {
            pub fn new(bytes: [u8; 16]) -> Result<Self, Refusal> {
                if bytes == [0; 16] {
                    return Err(Refusal::InvalidIdentity);
                }
                Ok(Self(bytes))
            }

            pub const fn bytes(self) -> [u8; 16] {
                self.0
            }
        }
    )+};
}

identity!(
    ClusterId,
    NamespaceId,
    GroupId,
    IncarnationId,
    NodeId,
    DiskLineage,
    EngineInstanceId,
    ProcessGeneration,
    ProposalId,
    ClientId
);

macro_rules! counter {
    ($($name:ident),+ $(,)?) => {$ (
        #[doc = concat!("A ", stringify!($name), "; distinct from Fleet epochs and tickets.")]
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }
    )+};
}

counter!(Term, LogIndex, ActivationGeneration, OperationId);

/// Raft reserves member zero. A lost disk joins with a newly allocated member.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberId(std::num::NonZeroU64);

impl MemberId {
    pub fn new(value: u64) -> Result<Self, Refusal> {
        std::num::NonZeroU64::new(value)
            .map(Self)
            .ok_or(Refusal::InvalidIdentity)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// One logical history. No progress comparison may cross this identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupScope {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub cell: String,
    pub group: GroupId,
    pub incarnation: IncarnationId,
}

impl GroupScope {
    pub fn validate(&self) -> Result<(), Refusal> {
        if self.cell.is_empty() || self.cell.len() > 1024 || self.cell.chars().any(char::is_control)
        {
            return Err(Refusal::InvalidIdentity);
        }
        Ok(())
    }
}

/// A physical node's disk/process identity, independent of any Raft member.
/// Identifying a node supplies no cell membership or mutation authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeIdentity {
    pub node: NodeId,
    pub disk: DiskLineage,
    pub process: ProcessGeneration,
}

/// A process operating a particular durable member disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaIdentity<S = GroupScope> {
    pub scope: S,
    pub node: NodeId,
    pub member: MemberId,
    pub disk: DiskLineage,
    pub process: ProcessGeneration,
}

impl<S> ReplicaIdentity<S> {
    pub fn node_identity(&self) -> NodeIdentity {
        NodeIdentity {
            node: self.node,
            disk: self.disk,
            process: self.process,
        }
    }
}

/// Authority is bound to a term and an in-process activation, independently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftAuthority {
    pub replica: ReplicaIdentity,
    pub term: Term,
    pub activation: ActivationGeneration,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogPosition {
    pub index: LogIndex,
    pub term: Term,
}

impl LogPosition {
    pub fn validate(self) -> Result<(), Refusal> {
        if (self.index.get() == 0) != (self.term.get() == 0) {
            return Err(Refusal::InvalidPosition);
        }
        Ok(())
    }
}

/// Positions from one member disk/process. A different group's maxima cannot
/// establish proof even when its numeric indices happen to be larger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaProgress {
    replica: ReplicaIdentity,
    persisted: LogIndex,
    committed: LogIndex,
    applied: LogIndex,
    queryable: LogIndex,
    archived: LogIndex,
}

impl ReplicaProgress {
    pub fn new(replica: ReplicaIdentity) -> Result<Self, Refusal> {
        Self::checkpoint(replica, LogIndex::new(0), LogIndex::new(0))
    }

    pub fn replica(&self) -> &ReplicaIdentity {
        &self.replica
    }

    pub fn persisted(&self) -> LogIndex {
        self.persisted
    }

    pub fn committed(&self) -> LogIndex {
        self.committed
    }

    pub fn applied(&self) -> LogIndex {
        self.applied
    }

    pub fn queryable(&self) -> LogIndex {
        self.queryable
    }

    pub fn archived(&self) -> LogIndex {
        self.archived
    }

    pub fn persist(&mut self, index: LogIndex) -> Result<(), Refusal> {
        advance(&mut self.persisted, index, None)
    }

    /// Raft may replace a persisted but uncommitted suffix with a shorter one.
    /// Its committed prefix can never be truncated by that operation.
    pub fn replace_suffix(&mut self, last: LogIndex) -> Result<(), Refusal> {
        if last < self.committed {
            return Err(Refusal::Unproven);
        }
        self.persisted = last;
        Ok(())
    }

    pub fn commit(&mut self, index: LogIndex) -> Result<(), Refusal> {
        advance(&mut self.committed, index, Some(self.persisted))
    }

    /// Ordinary apply cannot skip an entry, including configuration/no-op entries.
    pub fn apply(&mut self, index: LogIndex) -> Result<(), Refusal> {
        if index != self.applied && self.applied.get().checked_add(1) != Some(index.get()) {
            return Err(Refusal::NonContiguous);
        }
        advance(&mut self.applied, index, Some(self.committed))
    }

    /// Called only after the verified image and its engine marker are durable.
    pub fn publish(&mut self, index: LogIndex) -> Result<(), Refusal> {
        advance(&mut self.queryable, index, Some(self.applied))
    }

    /// The caller verifies a complete snapshot plus covering tail/dependencies.
    pub fn archive(&mut self, index: LogIndex) -> Result<(), Refusal> {
        advance(&mut self.archived, index, Some(self.committed))
    }

    /// Recover/install one already durable coherent checkpoint. Ordinary
    /// completion events must use the separate ordered advancement methods.
    pub fn checkpoint(
        replica: ReplicaIdentity,
        index: LogIndex,
        archived: LogIndex,
    ) -> Result<Self, Refusal> {
        replica.scope.validate()?;
        if archived > index {
            return Err(Refusal::Unproven);
        }
        Ok(Self {
            replica,
            persisted: index,
            committed: index,
            applied: index,
            queryable: index,
            archived,
        })
    }
}

fn advance(
    current: &mut LogIndex,
    next: LogIndex,
    ceiling: Option<LogIndex>,
) -> Result<(), Refusal> {
    if next < *current {
        return Err(Refusal::ProgressRegression);
    }
    if ceiling.is_some_and(|maximum| next > maximum) {
        return Err(Refusal::Unproven);
    }
    *current = next;
    Ok(())
}

impl RaftAuthority {
    /// A leader cannot execute commands until a current-term entry is committed
    /// and its local committed image is queryable through that barrier.
    pub fn admit(
        &self,
        leader: Option<MemberId>,
        barrier: LogPosition,
        progress: &ReplicaProgress,
    ) -> Result<(), Refusal> {
        self.replica.scope.validate()?;
        if self.replica != progress.replica {
            return Err(Refusal::WrongHistory);
        }
        barrier.validate()?;
        if leader != Some(self.replica.member) {
            return Err(Refusal::NotLeader);
        }
        if self.term.get() == 0
            || self.activation.get() == 0
            || barrier.term != self.term
            || barrier.index.get() == 0
            || progress.queryable < barrier.index
        {
            return Err(Refusal::LeaderNotReady);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Durability {
    Quorum,
    S3,
}

/// Client identities are authenticated by ingress and never reused in a cell
/// incarnation. Each identity submits its per-cell sequences in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandId {
    pub client: ClientId,
    sequence: std::num::NonZeroU64,
}

impl CommandId {
    pub fn new(client: ClientId, sequence: u64) -> Result<Self, Refusal> {
        Ok(Self {
            client,
            sequence: std::num::NonZeroU64::new(sequence).ok_or(Refusal::InvalidIdentity)?,
        })
    }

    pub const fn sequence(self) -> u64 {
        self.sequence.get()
    }
}

/// SDK-visible outcomes. A submitted operation that loses its waiter retains
/// its identity in Ambiguous; callers must reconcile instead of minting a new ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandFailure {
    NotLeader {
        leader: Option<MemberId>,
    },
    Unavailable,
    Overloaded,
    /// SQLite refused private execution for lack of storage. No proposal was
    /// submitted; free capacity before retrying this command identity.
    StorageFull,
    Ambiguous {
        command: CommandId,
    },
    CommandConflict,
    SequenceGap {
        next: u64,
    },
    CommandExpired,
    VersionUnavailable,
    WrongIncarnation,
    UnsupportedCapability,
    UnsupportedConsistency,
    ArchiveUnavailable {
        command: CommandId,
    },
    CorruptState,
}

impl fmt::Display for CommandFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "consensus operation failed: {self:?}")
    }
}

impl std::error::Error for CommandFailure {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandIdentity {
    pub authority: RaftAuthority,
    pub operation: OperationId,
    pub proposal: ProposalId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProofStage {
    Committed,
    Applied,
    Queryable,
    Archived,
}

/// A proof names the exact proposed bytes, not only a position sampled later.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftProof {
    pub command: CommandIdentity,
    pub entry_digest: [u8; 32],
    pub position: LogPosition,
    pub stage: ProofStage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputProof {
    Fleet { epoch: crate::Epoch, ticket: u64 },
    Raft(Box<RaftProof>),
}

impl OutputProof {
    pub fn validate_mode(&self, mode: crate::NamespaceMode) -> Result<(), Refusal> {
        match (self, mode) {
            (Self::Fleet { .. }, crate::NamespaceMode::Fleet)
            | (Self::Raft(_), crate::NamespaceMode::Raft) => Ok(()),
            _ => Err(Refusal::WrongMode),
        }
    }
}

/// Pending command output owned by the serial cell core. The adapter cannot
/// release a response by advancing a mutable watermark directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandGate {
    command: CommandIdentity,
    entry_digest: [u8; 32],
    required: Durability,
    position: Option<LogPosition>,
    stage: Option<ProofStage>,
    retired: bool,
}

impl CommandGate {
    pub fn new(
        command: CommandIdentity,
        entry_digest: [u8; 32],
        required: Durability,
    ) -> Result<Self, Refusal> {
        command.authority.replica.scope.validate()?;
        if command.operation.get() == 0
            || command.authority.term.get() == 0
            || command.authority.activation.get() == 0
        {
            return Err(Refusal::InvalidIdentity);
        }
        Ok(Self {
            command,
            entry_digest,
            required,
            position: None,
            stage: None,
            retired: false,
        })
    }

    pub fn observe(
        &mut self,
        proof: &OutputProof,
        progress: &ReplicaProgress,
    ) -> Result<(), Refusal> {
        if self.retired {
            return Err(Refusal::StaleCompletion);
        }
        if self.command.authority.replica != progress.replica {
            return Err(Refusal::WrongHistory);
        }
        let OutputProof::Raft(proof) = proof else {
            return Err(Refusal::WrongMode);
        };
        if proof.command != self.command || proof.entry_digest != self.entry_digest {
            return Err(Refusal::StaleCompletion);
        }
        proof.position.validate()?;
        if proof.position.term != self.command.authority.term
            || self
                .position
                .is_some_and(|position| position != proof.position)
        {
            return Err(Refusal::WrongHistory);
        }
        let covered = match proof.stage {
            ProofStage::Committed => progress.committed,
            ProofStage::Applied => progress.applied,
            ProofStage::Queryable => progress.queryable,
            ProofStage::Archived => progress.archived,
        };
        if covered < proof.position.index {
            return Err(Refusal::Unproven);
        }
        let next = match self.stage {
            None => ProofStage::Committed,
            Some(ProofStage::Committed) => ProofStage::Applied,
            Some(ProofStage::Applied) => ProofStage::Queryable,
            Some(ProofStage::Queryable | ProofStage::Archived) => ProofStage::Archived,
        };
        // Duplicates are harmless; reordered future stages cannot skip proof.
        if self.stage.is_none_or(|stage| proof.stage > stage) && proof.stage != next {
            return Err(Refusal::Unproven);
        }
        self.position = Some(proof.position);
        self.stage = Some(
            self.stage
                .map_or(proof.stage, |stage| stage.max(proof.stage)),
        );
        Ok(())
    }

    pub fn strengthen(&mut self, required: Durability) {
        self.required = self.required.max(required);
    }

    pub fn retire(&mut self) {
        self.retired = true;
    }

    pub fn releasable(&self) -> bool {
        !self.retired
            && self.stage.is_some_and(|stage| {
                stage
                    >= match self.required {
                        Durability::Quorum => ProofStage::Queryable,
                        Durability::S3 => ProofStage::Archived,
                    }
            })
    }

    pub fn position(&self) -> Option<LogPosition> {
        self.position
    }
}

/// An authenticated token is decoded by the host before this comparison.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedVersion {
    pub scope: GroupScope,
    pub position: LogPosition,
}

impl CommittedVersion {
    pub fn covers(&self, required: &Self) -> Result<bool, Refusal> {
        if self.scope != required.scope {
            return Err(Refusal::WrongHistory);
        }
        self.position.validate()?;
        required.position.validate()?;
        if self.position.index == required.position.index
            && self.position.term != required.position.term
        {
            return Err(Refusal::WrongHistory);
        }
        Ok(self.position.index >= required.position.index)
    }
}

/// Immutable claim created once after the initial stable member state is synced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapClaim {
    pub scope: GroupScope,
    pub member: MemberId,
    pub disk: DiskLineage,
    pub birth_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapAction {
    InitializeLocal,
    EstablishClaim,
    Resume,
}

impl BootstrapClaim {
    /// `local` is read from stable storage, never synthesized from the birth.
    pub fn reconcile(
        &self,
        local: Option<&Self>,
        remote: Option<&Self>,
    ) -> Result<BootstrapAction, Refusal> {
        self.scope.validate()?;
        if local.is_some_and(|claim| claim != self) || remote.is_some_and(|claim| claim != self) {
            return Err(Refusal::WrongHistory);
        }
        match (local, remote) {
            (None, None) => Ok(BootstrapAction::InitializeLocal),
            (Some(_), None) => Ok(BootstrapAction::EstablishClaim),
            (Some(_), Some(_)) => Ok(BootstrapAction::Resume),
            (None, Some(_)) => Err(Refusal::LostStableState),
        }
    }
}

/// Typed refusals are preserved at the adapter/SDK boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    InvalidIdentity,
    InvalidPosition,
    WrongMode,
    WrongHistory,
    StaleCompletion,
    ProgressRegression,
    NonContiguous,
    Unproven,
    NotLeader,
    LeaderNotReady,
    LostStableState,
    Overloaded,
    Closed,
}

impl fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "consensus contract refused: {self:?}")
    }
}

impl std::error::Error for Refusal {}
