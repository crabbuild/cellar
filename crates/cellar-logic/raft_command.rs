// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Serial command ownership for a Raft cell, independent of Fleet leases.
//!
//! The host retains this state and its private executor after a caller goes
//! away. Runtime observations, verified image publication and archive receipts
//! are effects; only this core decides admission and successful output. A host
//! must serialize observations here and recheck live leadership before submission
//! and output. Neither an accepted proposal nor a Fleet ticket proves success.

use crate::consensus::{
    ActivationGeneration, CommandGate, CommandId, CommandIdentity, CommittedVersion, Durability,
    LogPosition, MemberId, OperationId, OutputProof, RaftAuthority, Refusal, ReplicaIdentity,
    ReplicaProgress, Term,
};

/// Sequence numbers are allocated before requesting runtime status. Delayed
/// status replies cannot restore authority revoked by a newer observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeadershipObservation {
    pub replica: ReplicaIdentity,
    pub sequence: u64,
    pub term: Term,
    pub leader: Option<MemberId>,
    pub is_leader: bool,
    pub protocol_ready: bool,
    pub awaiting_ready: bool,
    pub awaiting_apply: bool,
    /// The runtime's durable applied head, after any required current-term
    /// startup barrier. Its image must separately be verified and published.
    pub barrier: LogPosition,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityChange {
    pub authority: Option<RaftAuthority>,
    /// Fail this operation's waiter conservatively; retain its execution owner
    /// until discard/reconciliation has actually completed.
    pub retired: Option<CommandIdentity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cancellation {
    /// Cancel/join private execution and rebuild from the committed image. The
    /// slot remains occupied until `discard_completed` confirms that work.
    DiscardPrivate,
    /// Cancel the waiter only. The producer and unresolved proposal stay owned.
    Submitted,
}

/// A one-use decision for the host's held response. The host binds its response
/// body to `identity`, rather than releasing whichever request happens to wait
/// on the cell when a delayed completion arrives.
#[derive(Debug, PartialEq, Eq)]
pub struct OutputPermit {
    identity: CommandIdentity,
    command: CommandId,
    version: CommittedVersion,
    durability: Durability,
}

impl OutputPermit {
    pub fn identity(&self) -> &CommandIdentity {
        &self.identity
    }

    pub fn command(&self) -> CommandId {
        self.command
    }

    pub fn version(&self) -> &CommittedVersion {
        &self.version
    }

    pub fn durability(&self) -> Durability {
        self.durability
    }
}

struct BoundEntry {
    position: LogPosition,
    digest: [u8; 32],
    gate: CommandGate,
}

struct Pending {
    identity: CommandIdentity,
    command: CommandId,
    required: Durability,
    recorded_minimum: Durability,
    submitted: bool,
    cancelled: bool,
    retired: bool,
    entry: Option<BoundEntry>,
    receipt_verified: bool,
    recovered: Option<CommittedVersion>,
    retirement: Option<LogPosition>,
}

/// One serial actor owns this value per resident Raft cell. It cannot be cloned
/// or reconstructed to evade an unresolved submission. Reopening an executor in
/// the same process/term requires retaining this owner; closing it requires the
/// node to join its workers before a replacement can recover the directory.
pub struct CommandState {
    replica: ReplicaIdentity,
    minimum: Durability,
    progress: ReplicaProgress,
    observation: Option<LeadershipObservation>,
    highest_term: Term,
    authority: Option<RaftAuthority>,
    last_activation_term: Term,
    activation: u64,
    operation: u64,
    pending: Option<Pending>,
    closed: bool,
}

impl CommandState {
    /// `progress` comes from the recovered, verified image and engine marker,
    /// not from directory recency or an unverified runtime watermark.
    pub fn new(progress: ReplicaProgress, minimum: Durability) -> Result<Self, Refusal> {
        progress.replica().scope.validate()?;
        Ok(Self {
            replica: progress.replica().clone(),
            minimum,
            progress,
            observation: None,
            highest_term: Term::new(0),
            authority: None,
            last_activation_term: Term::new(0),
            activation: 0,
            operation: 0,
            pending: None,
            closed: false,
        })
    }

    pub fn authority(&self) -> Option<&RaftAuthority> {
        self.authority.as_ref()
    }

    pub fn pending(&self) -> Option<&CommandIdentity> {
        self.pending.as_ref().map(|pending| &pending.identity)
    }

    /// Freeze this minimum in the command's private receipt transaction.
    /// Later waiters may strengthen the output requirement without changing
    /// the durability originally recorded with an already staged command.
    pub fn receipt_durability(&self, identity: &CommandIdentity) -> Result<Durability, Refusal> {
        self.open()?;
        self.pending
            .as_ref()
            .filter(|pending| pending.identity == *identity)
            .map(|pending| pending.recorded_minimum)
            .ok_or(Refusal::StaleCompletion)
    }

    pub fn progress(&self) -> &ReplicaProgress {
        &self.progress
    }

    /// The host has accepted one coherent progress observation for this exact
    /// replica. Applied/queryable/archive positions require their respective
    /// verified effects. A status reply alone cannot advance image readiness.
    pub fn observe_progress(&mut self, progress: ReplicaProgress) -> Result<(), Refusal> {
        self.open()?;
        if progress.replica() != &self.replica {
            return Err(Refusal::WrongHistory);
        }
        if progress.committed() < self.progress.committed()
            || progress.applied() < self.progress.applied()
            || progress.queryable() < self.progress.queryable()
            || progress.archived() < self.progress.archived()
        {
            return Err(Refusal::ProgressRegression);
        }
        // A persisted uncommitted suffix may be replaced with a shorter one.
        // ReplicaProgress itself guarantees it never falls below committed.
        self.progress = progress;
        Ok(())
    }

    pub fn observe_leadership(
        &mut self,
        observation: LeadershipObservation,
    ) -> Result<AuthorityChange, Refusal> {
        self.open()?;
        if observation.replica != self.replica {
            return Err(Refusal::WrongHistory);
        }
        observation.barrier.validate()?;
        if observation.sequence == 0
            || observation.term < self.highest_term
            || self
                .observation
                .as_ref()
                .is_some_and(|old| observation.sequence <= old.sequence)
        {
            return Err(Refusal::StaleCompletion);
        }
        if observation.barrier.term > observation.term {
            return Err(Refusal::InvalidPosition);
        }
        let revoked = self.authority.as_ref().is_some_and(|authority| {
            authority.term != observation.term
                || !observation.is_leader
                || observation.leader != Some(self.replica.member)
        });
        let retired = if revoked {
            self.retire_authority()
        } else {
            None
        };
        self.highest_term = observation.term;
        self.observation = Some(observation);
        if self.authority.is_none() && self.last_activation_term != self.highest_term {
            let candidate = RaftAuthority {
                replica: self.replica.clone(),
                term: self.highest_term,
                activation: ActivationGeneration::new(
                    self.activation.checked_add(1).ok_or(Refusal::Closed)?,
                ),
            };
            if self.ready(&candidate).is_ok() {
                self.activation = candidate.activation.get();
                self.last_activation_term = candidate.term;
                self.authority = Some(candidate);
            }
        }
        Ok(AuthorityChange {
            authority: self.authority.clone(),
            retired,
        })
    }

    pub fn admit(
        &mut self,
        authority: &RaftAuthority,
        command: CommandId,
        proposal: crate::consensus::ProposalId,
        required: Durability,
    ) -> Result<CommandIdentity, Refusal> {
        self.check_authority(authority)?;
        self.ready(authority)?;
        if self.pending.is_some() {
            return Err(Refusal::Overloaded);
        }
        let operation = self.operation.checked_add(1).ok_or(Refusal::Closed)?;
        let identity = CommandIdentity {
            authority: authority.clone(),
            operation: OperationId::new(operation),
            proposal,
        };
        self.operation = operation;
        self.pending = Some(Pending {
            identity: identity.clone(),
            command,
            required: required.max(self.minimum),
            recorded_minimum: required.max(self.minimum),
            submitted: false,
            cancelled: false,
            retired: false,
            entry: None,
            receipt_verified: false,
            recovered: None,
            retirement: None,
        });
        Ok(identity)
    }

    /// Runs before awaiting proposal admission. Even an unpolled/cancelled
    /// response future cannot establish that the runtime did not receive it.
    pub fn mark_submitted(&mut self, identity: &CommandIdentity) -> Result<(), Refusal> {
        self.check_authority(&identity.authority)?;
        self.ready(&identity.authority)?;
        let pending = self.matching(identity)?;
        if pending.submitted || pending.cancelled || pending.retired || pending.recovered.is_some()
        {
            return Err(Refusal::StaleCompletion);
        }
        pending.submitted = true;
        Ok(())
    }

    /// Bind the runtime's immutable entry receipt. Admission is provisional;
    /// ordered commit/apply/queryable proofs still have to reach the gate.
    pub fn bind_entry(
        &mut self,
        identity: &CommandIdentity,
        position: LogPosition,
        digest: [u8; 32],
    ) -> Result<(), Refusal> {
        position.validate()?;
        if position.index.get() == 0 || position.term != identity.authority.term {
            return Err(Refusal::WrongHistory);
        }
        let pending = self.matching(identity)?;
        if !pending.submitted || pending.retired {
            return Err(Refusal::StaleCompletion);
        }
        if let Some(entry) = &pending.entry {
            return if entry.position == position && entry.digest == digest {
                Ok(())
            } else {
                Err(Refusal::WrongHistory)
            };
        }
        pending.entry = Some(BoundEntry {
            position,
            digest,
            gate: CommandGate::new(identity.clone(), digest, pending.required)?,
        });
        Ok(())
    }

    pub fn observe_proof(&mut self, proof: &OutputProof) -> Result<(), Refusal> {
        self.open()?;
        let OutputProof::Raft(raft) = proof else {
            return Err(Refusal::WrongMode);
        };
        let pending = self.pending.as_mut().ok_or(Refusal::StaleCompletion)?;
        if pending.identity != raft.command || pending.retired {
            return Err(Refusal::StaleCompletion);
        }
        let entry = pending.entry.as_mut().ok_or(Refusal::Unproven)?;
        if raft.position != entry.position {
            return Err(Refusal::WrongHistory);
        }
        entry.gate.observe(proof, &self.progress)
    }

    /// The image worker/private executor verified that this command's receipt
    /// exists in the exact published proposal image. A log/apply watermark
    /// alone does not prove a business result, even if it covers the index.
    pub fn verify_receipt(
        &mut self,
        identity: &CommandIdentity,
        command: CommandId,
        position: LogPosition,
        minimum_durability: Durability,
    ) -> Result<(), Refusal> {
        let pending = self.matching(identity)?;
        let entry = pending.entry.as_ref().ok_or(Refusal::Unproven)?;
        if pending.retired || pending.command != command {
            return Err(Refusal::StaleCompletion);
        }
        if position != entry.position || minimum_durability != pending.recorded_minimum {
            return Err(Refusal::WrongHistory);
        }
        pending.receipt_verified = true;
        Ok(())
    }

    pub fn strengthen(
        &mut self,
        identity: &CommandIdentity,
        required: Durability,
    ) -> Result<(), Refusal> {
        let pending = self.matching(identity)?;
        pending.required = pending.required.max(required);
        if let Some(entry) = &mut pending.entry {
            entry.gate.strengthen(pending.required);
        }
        Ok(())
    }

    /// A retry found its matching receipt in a verified committed image. The
    /// worker must compare the operation, contract and normalized input digest
    /// before returning this observation. No new proposal or invented entry
    /// proof is needed. The returned version names that exact image, which may
    /// precede this leader's startup barrier and term.
    pub fn recover_receipt(
        &mut self,
        identity: &CommandIdentity,
        command: CommandId,
        version: CommittedVersion,
        stored_minimum: Durability,
    ) -> Result<(), Refusal> {
        self.check_authority(&identity.authority)?;
        self.ready(&identity.authority)?;
        version.position.validate()?;
        if version.scope != self.replica.scope
            || version.position.index.get() == 0
            || version.position.term > identity.authority.term
            || version.position.index > self.progress.queryable()
            || self.observation.as_ref().is_some_and(|observation| {
                version.position.index == observation.barrier.index
                    && version.position.term != observation.barrier.term
            })
        {
            return Err(Refusal::WrongHistory);
        }
        if stored_minimum < self.minimum {
            return Err(Refusal::Unproven);
        }
        let pending = self.matching(identity)?;
        if pending.submitted || pending.retired || pending.cancelled || pending.command != command {
            return Err(Refusal::StaleCompletion);
        }
        if pending
            .recovered
            .as_ref()
            .is_some_and(|old| old != &version)
        {
            return Err(Refusal::WrongHistory);
        }
        pending.required = pending.required.max(stored_minimum);
        pending.recovered = Some(version);
        pending.receipt_verified = true;
        Ok(())
    }

    pub fn finish(&mut self, identity: &CommandIdentity) -> Result<OutputPermit, Refusal> {
        self.check_authority(&identity.authority)?;
        let archived = self.progress.archived();
        let pending = self.matching(identity)?;
        if pending.retired || pending.cancelled || !pending.receipt_verified {
            return Err(Refusal::Unproven);
        }
        let version = if let Some(version) = &pending.recovered {
            if pending.required == Durability::S3 && archived < version.position.index {
                return Err(Refusal::Unproven);
            }
            version.clone()
        } else {
            let entry = pending.entry.as_ref().ok_or(Refusal::Unproven)?;
            if !entry.gate.releasable() {
                return Err(Refusal::Unproven);
            }
            CommittedVersion {
                scope: pending.identity.authority.replica.scope.clone(),
                position: entry.position,
            }
        };
        let permit = OutputPermit {
            identity: pending.identity.clone(),
            command: pending.command,
            version,
            durability: pending.required,
        };
        self.pending = None;
        Ok(permit)
    }

    pub fn cancel(&mut self, identity: &CommandIdentity) -> Result<Cancellation, Refusal> {
        let pending = self.matching(identity)?;
        if pending.submitted {
            return Ok(Cancellation::Submitted);
        }
        pending.cancelled = true;
        Ok(Cancellation::DiscardPrivate)
    }

    /// Called after the unsubmitted callback has stopped and its private
    /// mutations have been discarded/rebuilt. Cancellation itself never frees
    /// a slot while its synchronous callback can still be running.
    pub fn discard_completed(&mut self, identity: &CommandIdentity) -> Result<(), Refusal> {
        if self.matching(identity)?.submitted {
            return Err(Refusal::Unproven);
        }
        self.pending = None;
        Ok(())
    }

    /// A submitted operation may be abandoned only after a different term has
    /// established and applied its barrier and the host has joined/rebuilt the
    /// old executor from that authoritative image. The same live term cannot
    /// declare a missing response to mean the proposal did not commit.
    pub fn reconcile_completed(
        &mut self,
        identity: &CommandIdentity,
        replacement: &RaftAuthority,
    ) -> Result<(), Refusal> {
        self.check_authority(replacement)?;
        self.ready(replacement)?;
        self.matching(identity)?;
        if replacement.term <= identity.authority.term {
            return Err(Refusal::Unproven);
        }
        self.pending = None;
        Ok(())
    }

    /// A committed and published current-term barrier decides whether an old
    /// term's submission can still be in the history, including on a follower.
    /// This grants no new write authority. The host must next close/discard the
    /// old private owner against this exact durable cut.
    pub fn begin_retirement(&mut self, identity: &CommandIdentity) -> Result<LogPosition, Refusal> {
        self.open()?;
        let observation = self.observation.as_ref().ok_or(Refusal::Unproven)?;
        let barrier = observation.barrier;
        if observation.term <= identity.authority.term
            || barrier.term != observation.term
            || barrier.index.get() == 0
            || self.progress.queryable() < barrier.index
        {
            return Err(Refusal::Unproven);
        }
        let pending = self.matching(identity)?;
        if !pending.submitted || !pending.retired {
            return Err(Refusal::Unproven);
        }
        pending.retirement = Some(barrier);
        Ok(barrier)
    }

    pub fn retirement_completed(
        &mut self,
        identity: &CommandIdentity,
        barrier: LogPosition,
    ) -> Result<(), Refusal> {
        let queryable = self.progress.queryable();
        let pending = self.matching(identity)?;
        if pending.retirement != Some(barrier)
            || !pending.submitted
            || !pending.retired
            || barrier.term <= identity.authority.term
        {
            return Err(Refusal::StaleCompletion);
        }
        if queryable < barrier.index {
            return Err(Refusal::Unproven);
        }
        self.pending = None;
        Ok(())
    }

    /// Stop admission/output immediately, retaining unfinished operation
    /// identity for shutdown reporting. The host still has to join its work.
    pub fn close(&mut self) -> Option<CommandIdentity> {
        self.closed = true;
        self.retire_authority();
        self.pending().cloned()
    }

    fn open(&self) -> Result<(), Refusal> {
        if self.closed {
            Err(Refusal::Closed)
        } else {
            Ok(())
        }
    }

    fn matching(&mut self, identity: &CommandIdentity) -> Result<&mut Pending, Refusal> {
        self.open()?;
        self.pending
            .as_mut()
            .filter(|pending| pending.identity == *identity)
            .ok_or(Refusal::StaleCompletion)
    }

    fn check_authority(&self, authority: &RaftAuthority) -> Result<(), Refusal> {
        self.open()?;
        if self.authority.as_ref() != Some(authority) {
            return Err(Refusal::StaleCompletion);
        }
        Ok(())
    }

    fn ready(&self, authority: &RaftAuthority) -> Result<(), Refusal> {
        let observation = self.observation.as_ref().ok_or(Refusal::LeaderNotReady)?;
        if !observation.is_leader
            || !observation.protocol_ready
            || observation.awaiting_ready
            || observation.awaiting_apply
            || observation.term != authority.term
            || observation.barrier.index != self.progress.applied()
            || self.progress.persisted() != self.progress.committed()
            || self.progress.committed() != self.progress.applied()
            || self.progress.applied() != self.progress.queryable()
        {
            return Err(Refusal::LeaderNotReady);
        }
        authority.admit(observation.leader, observation.barrier, &self.progress)
    }

    fn retire_authority(&mut self) -> Option<CommandIdentity> {
        self.authority = None;
        let pending = self.pending.as_mut()?;
        pending.retired = true;
        if let Some(entry) = &mut pending.entry {
            entry.gate.retire();
        }
        Some(pending.identity.clone())
    }
}
