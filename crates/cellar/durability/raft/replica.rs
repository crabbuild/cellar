//! One serialized RawNode and its ordered persistence/application protocol.

use super::cache::{ConsensusStorage, DurableApply, GroupLog, GroupWrite, SnapshotChange};
use super::completion::*;
use super::options::RaftRuntimeOptions;
use super::runtime::{GroupAction, Outbox, Response};
use ::raft::prelude::{ConfChange, ConfChangeV2, Entry, EntryType, Message, MessageType};
use ::raft::{Config, LightReady, RawNode, Ready, StateRole, Storage};
use cellar_logic::consensus::{LogIndex, LogPosition, OperationId, ReplicaIdentity, Term};
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::sync::Arc;

pub(crate) struct Operations {
    owner: Arc<()>,
    next: u64,
}

impl Operations {
    pub fn new() -> Self {
        Self {
            owner: Arc::new(()),
            next: 0,
        }
    }

    pub fn next(&mut self) -> Result<OperationId, RuntimeError> {
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| RuntimeError::Protocol("operation counter exhausted".into()))?;
        Ok(OperationId::new(self.next))
    }

    fn ticket(
        &mut self,
        replica: &ReplicaIdentity,
        position: LogPosition,
        digest: [u8; 32],
    ) -> Result<ApplyTicket, RuntimeError> {
        Ok(ApplyTicket {
            owner: self.owner.clone(),
            replica: replica.clone(),
            operation: self.next()?,
            position,
            digest,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WriteTicket {
    key: ApplyTicket,
    ready: u64,
}

enum Phase {
    Idle,
    Checkpoint {
        checkpoint: Box<super::snapshot::SnapshotCheckpoint>,
        key: WriteTicket,
        synced: bool,
    },
    Ready {
        ready: Box<Ready>,
        key: WriteTicket,
        synced: bool,
        snapshot_ticket: Option<ApplyTicket>,
        snapshot_authorized: bool,
        messages_taken: bool,
    },
    Light {
        light: Box<LightReady>,
        key: Option<WriteTicket>,
        synced: bool,
    },
    Apply {
        applied: DurableApply,
        key: WriteTicket,
        synced: bool,
    },
    AbortSnapshot {
        ticket: ApplyTicket,
        key: WriteTicket,
        applied: Option<(ApplyTicket, DurableApply)>,
        synced: bool,
    },
}

pub(crate) struct Replica {
    pub log: GroupLog,
    node: RawNode<ConsensusStorage>,
    phase: Phase,
    pub write: Option<(WriteTicket, GroupWrite)>,
    outbound: VecDeque<RuntimeEventKind>,
    apply: VecDeque<Entry>,
    apply_ticket: Option<ApplyTicket>,
    finished_apply: Option<ApplyCompletion>,
    snapshot_applied: Option<DurableApply>,
    snapshot_reference: Option<super::snapshot::SnapshotReference>,
    checkpoint: Option<Box<super::snapshot::SnapshotCheckpoint>>,
    pub ready_bytes: usize,
    pub apply_bytes: usize,
    last_sync: u64,
    source_observed: Option<(u64, u64, u64)>,
    abort_snapshot: Option<ApplyTicket>,
    seed: [u8; 32],
}

impl Replica {
    pub fn new(
        log: GroupLog,
        options: &RaftRuntimeOptions,
        seed: [u8; 32],
    ) -> Result<Self, RuntimeError> {
        let conf = log.initial_state()?.conf_state;
        let members = conf
            .voters
            .iter()
            .chain(&conf.voters_outgoing)
            .chain(&conf.learners)
            .chain(&conf.learners_next)
            .collect::<std::collections::BTreeSet<_>>();
        if members.len() > options.max_members_per_group {
            return Err(RuntimeError::Overloaded);
        }
        let applied = log.applied()?;
        if log.initial_state()?.hard_state.term >= u64::MAX - 2 || log.last_index()? >= u64::MAX - 2
        {
            return Err(RuntimeError::Invalid("exhausted Raft counters".into()));
        }
        let config = Config {
            id: log.identity().member.get(),
            applied: applied.position.index.get(),
            election_tick: 20,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            read_only_option: ::raft::ReadOnlyOption::Safe,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 8,
            max_uncommitted_size: 32 * 1024 * 1024,
            max_committed_size_per_ready: 4 * 1024 * 1024,
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let node =
            RawNode::new_with_election_seed(&config, log.consensus_storage(), &logger, Some(seed))?;
        Ok(Self {
            log,
            node,
            phase: Phase::Idle,
            write: None,
            outbound: VecDeque::new(),
            apply: VecDeque::new(),
            apply_ticket: None,
            finished_apply: None,
            snapshot_applied: None,
            snapshot_reference: None,
            checkpoint: None,
            ready_bytes: 0,
            apply_bytes: 0,
            last_sync: 0,
            source_observed: None,
            abort_snapshot: None,
            seed,
        })
    }

    pub fn identity(&self) -> &ReplicaIdentity {
        self.log.identity()
    }

    pub fn idle(&self) -> bool {
        matches!(self.phase, Phase::Idle) && self.outbound.is_empty() && self.checkpoint.is_none()
    }

    pub fn has_ready(&self) -> bool {
        self.node.has_ready()
    }

    pub fn pending_reads(&self) -> usize {
        self.node.raft.read_only.pending_read_count()
    }

    pub fn status(&self) -> Result<GroupStatus, RuntimeError> {
        let cache = self.log.read()?;
        let is_leader = self.node.raft.state == StateRole::Leader;
        Ok(GroupStatus {
            replica: self.identity().clone(),
            term: Term::new(self.node.raft.term),
            leader: (self.node.raft.leader_id != 0).then_some(self.node.raft.leader_id),
            is_leader,
            protocol_ready: is_leader
                && self.idle()
                && cache.hard_state.term == self.node.raft.term
                && cache.applied.position.term.get() == self.node.raft.term,
            persisted: LogIndex::new(cache.last_index()),
            committed: LogIndex::new(cache.hard_state.commit),
            applied: cache.applied.position,
            awaiting_ready: !self.idle(),
            awaiting_apply: self.apply_ticket.is_some(),
            election_timeout: self.node.raft.randomized_election_timeout(),
            source_observed_ms: if is_leader
                && cache.applied.position.term.get() == self.node.raft.term
            {
                Some(crate::asyncrt::mono_ms())
            } else {
                self.source_observed
                    .filter(|(leader, term, _)| {
                        *leader == self.node.raft.leader_id && *term == self.node.raft.term
                    })
                    .map(|(_, _, at)| at)
            },
        })
    }

    pub fn execute(
        &mut self,
        action: GroupAction,
        options: &RaftRuntimeOptions,
    ) -> Result<Response, RuntimeError> {
        if !self.idle() {
            return Err(RuntimeError::NotReady);
        }
        match action {
            GroupAction::Checkpoint(checkpoint) => {
                if self.node.has_ready() || self.apply_ticket.is_some() || !self.apply.is_empty() {
                    return Err(RuntimeError::NotReady);
                }
                checkpoint
                    .validate(&self.log)
                    .map_err(|error| RuntimeError::Invalid(error.to_string()))?;
                checkpoint
                    .retain_pending()
                    .map_err(|error| RuntimeError::Invalid(error.to_string()))?;
                self.ready_bytes = checkpoint.bytes() + 4096;
                self.checkpoint = Some(checkpoint);
                Ok(Response::Unit)
            }
            GroupAction::Campaign => {
                self.check_counters()?;
                self.node.campaign()?;
                Ok(Response::Unit)
            }
            GroupAction::Step(message) => {
                self.validate_message(&message, options)?;
                let (from, term, kind) = (message.from, message.term, message.get_msg_type());
                self.node.step(message)?;
                if matches!(
                    kind,
                    MessageType::MsgAppend | MessageType::MsgHeartbeat | MessageType::MsgSnapshot
                ) && from == self.node.raft.leader_id
                    && term == self.node.raft.term
                {
                    self.source_observed = Some((from, term, crate::asyncrt::mono_ms()));
                }
                Ok(Response::Unit)
            }
            GroupAction::Propose { context, data } => {
                self.admit_proposal()?;
                let before = self.node.raft.raft_log.last_index();
                self.node.propose(context, data)?;
                self.accepted(before, EntryType::EntryNormal)
            }
            GroupAction::Configure { context, change } => {
                self.admit_proposal()?;
                if self.node.raft.pending_conf_index > self.node.raft.raft_log.applied {
                    return Err(RuntimeError::NotReady);
                }
                if change.changes.len() > options.max_members_per_group {
                    return Err(RuntimeError::Invalid("oversized membership change".into()));
                }
                let mut members = self
                    .node
                    .raft
                    .prs()
                    .iter()
                    .map(|(&id, _)| id)
                    .collect::<std::collections::BTreeSet<_>>();
                for item in &change.changes {
                    if item.node_id == 0 {
                        return Err(RuntimeError::Invalid("zero configuration member".into()));
                    }
                    members.insert(item.node_id);
                }
                if members.len() > options.max_members_per_group {
                    return Err(RuntimeError::Overloaded);
                }
                let before = self.node.raft.raft_log.last_index();
                self.node.propose_conf_change(context, change)?;
                self.accepted(before, EntryType::EntryConfChangeV2)
            }
            GroupAction::ReadIndex(context) => {
                self.admit_read(options)?;
                self.node.read_index(context);
                Ok(Response::Unit)
            }
            GroupAction::Unreachable(member) => {
                if !self.accepts_peer(member) {
                    return Err(RuntimeError::Invalid("unknown transport peer".into()));
                }
                self.node.report_unreachable(member);
                Ok(Response::Unit)
            }
            GroupAction::SnapshotStatus { member, success } => {
                if !self.accepts_peer(member) {
                    return Err(RuntimeError::Invalid("unknown transport peer".into()));
                }
                self.node.report_snapshot(
                    member,
                    if success {
                        ::raft::SnapshotStatus::Finish
                    } else {
                        ::raft::SnapshotStatus::Failure
                    },
                );
                Ok(Response::Unit)
            }
        }
    }

    fn admit_proposal(&self) -> Result<(), RuntimeError> {
        self.check_counters()?;
        let status = self.status()?;
        if !status.is_leader {
            return Err(RuntimeError::NotLeader);
        }
        if !status.protocol_ready {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }

    fn admit_read(&self, options: &RaftRuntimeOptions) -> Result<(), RuntimeError> {
        if self.node.raft.leader_id == 0 {
            return Err(RuntimeError::NotReady);
        }
        if self.node.raft.state == StateRole::Leader && !self.status()?.protocol_ready {
            return Err(RuntimeError::NotReady);
        }
        if self.pending_reads() >= options.max_pending_reads {
            return Err(RuntimeError::Overloaded);
        }
        Ok(())
    }

    fn accepted(&self, before: u64, expected: EntryType) -> Result<Response, RuntimeError> {
        let entry = self
            .node
            .raft
            .raft_log
            .unstable_entries()
            .last()
            .ok_or(RuntimeError::NotReady)?;
        if entry.index != before.checked_add(1).ok_or(RuntimeError::Overloaded)?
            || entry.get_entry_type() != expected
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(Response::Proposal(AcceptedProposal {
            replica: self.identity().clone(),
            position: entry_position(entry),
            entry_digest: entry_digest(entry)?,
        }))
    }

    pub fn tick(&mut self) -> Result<(), RuntimeError> {
        self.check_counters()?;
        self.node.tick();
        Ok(())
    }

    fn check_counters(&self) -> Result<(), RuntimeError> {
        if self.node.raft.term >= u64::MAX - 2
            || self.node.raft.raft_log.last_index() >= u64::MAX - 2
        {
            return Err(RuntimeError::Overloaded);
        }
        Ok(())
    }

    pub(crate) fn accepts_peer(&self, member: u64) -> bool {
        member != 0
            && member != self.identity().member.get()
            && self.node.raft.prs().get(member).is_some()
    }

    pub(crate) fn validate_message(
        &self,
        message: &Message,
        options: &RaftRuntimeOptions,
    ) -> Result<(), RuntimeError> {
        let invalid = || RuntimeError::Invalid("malformed Raft peer message".into());
        if message.to != self.identity().member.get()
            || !self.accepts_peer(message.from)
            || ::raft::raw_node::is_local_msg(message.get_msg_type())
            || message.get_msg_type() == MessageType::MsgPropose
            || [
                message.term,
                message.log_term,
                message.index,
                message.commit,
                message.reject_hint,
                message.commit_term,
                message.request_snapshot,
            ]
            .iter()
            .any(|&n| n >= u64::MAX - 2)
            || message.entries.len() > 65_536
            || message.compute_size() as usize > options.max_message_bytes
        {
            return Err(invalid());
        }
        match message.get_msg_type() {
            MessageType::MsgAppend => {
                if (message.index == 0) != (message.log_term == 0) {
                    return Err(invalid());
                }
                let mut index = message.index;
                let mut term = message.log_term;
                for entry in &message.entries {
                    index = index.checked_add(1).ok_or_else(invalid)?;
                    if entry.index != index
                        || entry.index >= u64::MAX - 2
                        || entry.term == 0
                        || entry.term < term
                        || entry.term > message.term
                        || entry.data.len().saturating_add(entry.context.len())
                            > options.max_proposal_bytes
                    {
                        return Err(invalid());
                    }
                    term = entry.term;
                }
            }
            MessageType::MsgReadIndex | MessageType::MsgReadIndexResp => {
                if message.get_msg_type() == MessageType::MsgReadIndex {
                    self.admit_read(options)?;
                }
                if message.entries.len() != 1
                    || message.entries.iter().any(|e| {
                        e.data.is_empty()
                            || e.data.len() > 4096
                            || e.index != 0
                            || e.term != 0
                            || !e.context.is_empty()
                            || e.get_entry_type() != EntryType::EntryNormal
                    })
                {
                    return Err(invalid());
                }
            }
            _ => {
                if !message.entries.is_empty() {
                    return Err(invalid());
                }
            }
        }
        if message.get_msg_type() == MessageType::MsgSnapshot || !message.get_snapshot().is_empty()
        {
            let snapshot = message.get_snapshot();
            let metadata = snapshot.get_metadata();
            if message.get_msg_type() != MessageType::MsgSnapshot
                || metadata.index == 0
                || metadata.index >= u64::MAX - 2
                || metadata.term == 0
                || metadata.term > message.term
                || snapshot.compute_size() > 64 * 1024
                || conf_member_count(metadata.get_conf_state()) > options.max_members_per_group
            {
                return Err(invalid());
            }
            super::cache::validate_conf(metadata.get_conf_state()).map_err(|_| invalid())?;
        }
        Ok(())
    }

    pub fn complete_apply(&mut self, completed: ApplyCompletion) -> Result<(), RuntimeError> {
        if self.apply_ticket.as_ref() != Some(&completed.ticket) || self.finished_apply.is_some() {
            return Err(RuntimeError::StaleCompletion);
        }
        completed.image.validate()?;
        self.finished_apply = Some(completed);
        Ok(())
    }

    pub fn complete_snapshot(&mut self, completed: SnapshotCompletion) -> Result<(), RuntimeError> {
        if self.abort_snapshot.is_some() {
            return Err(RuntimeError::StaleCompletion);
        }
        let Phase::Ready {
            ready,
            snapshot_ticket: Some(expected),
            snapshot_authorized,
            ..
        } = &self.phase
        else {
            return Err(RuntimeError::StaleCompletion);
        };
        if *snapshot_authorized
            || expected != &completed.ticket
            || self.snapshot_applied.is_some()
            || completed.applied.position != expected.position
            || completed.applied.position.index.get() != ready.snapshot().get_metadata().index
        {
            return Err(RuntimeError::StaleCompletion);
        }
        if let Some(image) = &completed.applied.image {
            image.validate()?;
        }
        if completed
            .reference
            .as_ref()
            .is_some_and(|reference| !reference.matches(&completed.ticket, &completed.applied))
        {
            return Err(RuntimeError::StaleCompletion);
        }
        if let Some(reference) = &completed.reference {
            reference
                .retain_pending()
                .map_err(|error| RuntimeError::Invalid(error.to_string()))?;
        }
        self.snapshot_reference = completed.reference;
        self.snapshot_applied = Some(completed.applied);
        Ok(())
    }

    pub fn abort_snapshot(&mut self, ticket: ApplyTicket) -> Result<(), RuntimeError> {
        if self.abort_snapshot.as_ref() == Some(&ticket) {
            return Ok(());
        }
        let Phase::Ready {
            snapshot_ticket: Some(expected),
            snapshot_authorized: false,
            synced: false,
            ..
        } = &self.phase
        else {
            return Err(RuntimeError::StaleCompletion);
        };
        if expected != &ticket
            || self.snapshot_applied.is_some()
            || self.snapshot_reference.is_some()
            || self.write.is_some()
        {
            return Err(RuntimeError::StaleCompletion);
        }
        // An older physical apply still owns its ticket. Drive joins its
        // completion before fencing term/vote and rebuilding from stable state.
        self.abort_snapshot = Some(ticket);
        Ok(())
    }

    pub fn synced(&mut self, ticket: &WriteTicket, sequence: u64) -> Result<(), RuntimeError> {
        if sequence <= self.last_sync {
            return Err(RuntimeError::StaleCompletion);
        }
        match &mut self.phase {
            Phase::Ready { key, synced, .. }
            | Phase::Apply { key, synced, .. }
            | Phase::AbortSnapshot { key, synced, .. }
                if key == ticket =>
            {
                *synced = true
            }
            Phase::Checkpoint {
                checkpoint,
                key,
                synced,
            } if key == ticket => {
                checkpoint.retain().map_err(|error| {
                    RuntimeError::Storage(super::storage::LogError::Corrupt(error.to_string()))
                })?;
                *synced = true;
            }
            Phase::Light {
                key: Some(key),
                synced,
                ..
            } if key == ticket => *synced = true,
            _ => return Err(RuntimeError::StaleCompletion),
        }
        if let Some(reference) = &self.snapshot_reference {
            reference.retain().map_err(|error| {
                RuntimeError::Storage(super::storage::LogError::Corrupt(error.to_string()))
            })?;
        }
        self.snapshot_reference = None;
        self.last_sync = sequence;
        self.outbound.push_back(RuntimeEventKind::Persisted {
            replica: self.log.identity().clone(),
            sequence,
            committed: LogIndex::new(self.log.initial_state()?.hard_state.commit),
            light_commit: matches!(self.phase, Phase::Light { .. }),
        });
        Ok(())
    }

    pub fn drive(
        &mut self,
        operations: &mut Operations,
        outbox: &mut Outbox,
        options: &RaftRuntimeOptions,
        can_collect: bool,
    ) -> Result<bool, RuntimeError> {
        // Transferring messages into the bounded outbox is the send boundary.
        // Never advance the Ready until every dependent message is transferred.
        if let Some(event) = self.outbound.pop_front() {
            return match outbox.push(event) {
                None => Ok(true),
                Some(event) => {
                    self.outbound.push_front(event);
                    Ok(false)
                }
            };
        }
        if matches!(
            &self.phase,
            Phase::Ready {
                snapshot_authorized: false,
                ..
            }
        ) {
            if let Some(ticket) = &self.abort_snapshot {
                if self.apply_ticket.is_some() && self.finished_apply.is_none() {
                    return Ok(false);
                }
                let mut hard = self.node.raft.hard_state();
                // Restoring an unstable snapshot has already raised RawNode's
                // commit index. Its bytes are uninstalled: retain the stable
                // commit while persisting the term/vote needed to refuse a
                // second vote after cancellation or a crash.
                hard.commit = self.log.initial_state()?.hard_state.commit;
                let mut write = GroupWrite::new(&self.log);
                write.hard_state = Some(hard);
                let applied = if let Some(completed) = self.finished_apply.take() {
                    if self.apply_ticket.as_ref() != Some(&completed.ticket)
                        || self.apply.front().map(entry_position)
                            != Some(completed.ticket.position())
                    {
                        return Err(RuntimeError::StaleCompletion);
                    }
                    // This capability is now consumed by the abort write.
                    // A duplicate completion cannot reacquire its effect while
                    // storage sync or the abort event is still pending.
                    self.apply_ticket = None;
                    let applied = DurableApply {
                        position: completed.ticket.position(),
                        image: Some(completed.image),
                    };
                    write.applied = Some(applied.clone());
                    Some((completed.ticket, applied))
                } else {
                    None
                };
                let key = WriteTicket {
                    key: operations.ticket(
                        self.log.identity(),
                        ticket.position(),
                        ticket.entry_digest(),
                    )?,
                    ready: 0,
                };
                self.write = Some((key.clone(), write));
                self.phase = Phase::AbortSnapshot {
                    ticket: ticket.clone(),
                    key,
                    applied,
                    synced: false,
                };
                return Ok(true);
            }
        }
        match &mut self.phase {
            Phase::Ready {
                ready,
                key,
                synced: false,
                snapshot_ticket,
                snapshot_authorized,
                ..
            } if !ready.snapshot().is_empty() && self.write.is_none() => {
                if let Some(applied) = self.snapshot_applied.take() {
                    *snapshot_authorized = true;
                    let mut write = ready_write(&self.log, ready);
                    write.snapshot = Some(SnapshotChange::Install {
                        snapshot: ready.snapshot().clone(),
                        applied,
                    });
                    self.write = Some((key.clone(), write));
                    return Ok(true);
                }
                if !*snapshot_authorized && snapshot_ticket.is_none() {
                    let snapshot = ready.snapshot();
                    let position = LogPosition {
                        index: LogIndex::new(snapshot.get_metadata().index),
                        term: Term::new(snapshot.get_metadata().term),
                    };
                    let digest = Sha256::digest(
                        snapshot
                            .write_to_bytes()
                            .map_err(|e| RuntimeError::Protocol(e.to_string()))?,
                    )
                    .into();
                    let ticket = operations.ticket(self.log.identity(), position, digest)?;
                    if outbox
                        .push(RuntimeEventKind::Snapshot {
                            ticket: ticket.clone(),
                            snapshot: snapshot.clone(),
                        })
                        .is_none()
                    {
                        *snapshot_ticket = Some(ticket);
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Phase::Ready { synced: false, .. }
            | Phase::Light { synced: false, .. }
            | Phase::Checkpoint { synced: false, .. }
            | Phase::AbortSnapshot { synced: false, .. }
            | Phase::Apply { synced: false, .. } => Ok(false),
            Phase::AbortSnapshot {
                ticket, applied, ..
            } => {
                if let Some((ticket, applied)) = applied {
                    if outbox
                        .push(RuntimeEventKind::Applied {
                            ticket: ticket.clone(),
                            applied: applied.clone(),
                        })
                        .is_some()
                    {
                        return Ok(false);
                    }
                    let Phase::AbortSnapshot { applied, .. } = &mut self.phase else {
                        unreachable!()
                    };
                    *applied = None;
                    return Ok(true);
                }
                let event = RuntimeEventKind::SnapshotAborted {
                    ticket: ticket.clone(),
                    applied: self.log.applied()?,
                };
                let mut recovered = Self::new(self.log.clone(), options, self.seed)?;
                recovered.last_sync = self.last_sync;
                if outbox.push(event).is_some() {
                    return Ok(false);
                }
                *self = recovered;
                Ok(true)
            }
            Phase::Ready {
                ready,
                messages_taken,
                snapshot_ticket,
                ..
            } => {
                if !*messages_taken {
                    *messages_taken = true;
                    if let Some(ticket) = snapshot_ticket.take() {
                        self.outbound
                            .push_back(RuntimeEventKind::SnapshotInstalled {
                                ticket,
                                applied: self.log.applied()?,
                            });
                    }
                    let mut messages = ready.take_messages();
                    messages.extend(ready.take_persisted_messages());
                    self.outbound.extend(messages.into_iter().map(|message| {
                        RuntimeEventKind::Message {
                            replica: self.log.identity().clone(),
                            message,
                        }
                    }));
                    self.outbound
                        .extend(ready.take_read_states().into_iter().map(|state| {
                            RuntimeEventKind::ReadIndex {
                                replica: self.log.identity().clone(),
                                index: LogIndex::new(state.index),
                                context: state.request_ctx.to_vec(),
                            }
                        }));
                    if !self.outbound.is_empty() {
                        return Ok(true);
                    }
                }
                self.advance_ready(operations)
            }
            Phase::Checkpoint { key, .. } => {
                if outbox
                    .push(RuntimeEventKind::Checkpointed {
                        ticket: key.key.clone(),
                    })
                    .is_some()
                {
                    return Ok(false);
                }
                self.phase = Phase::Idle;
                Ok(true)
            }
            Phase::Light { .. } => {
                let Phase::Light { mut light, .. } =
                    std::mem::replace(&mut self.phase, Phase::Idle)
                else {
                    unreachable!()
                };
                self.outbound
                    .extend(light.take_messages().into_iter().map(|message| {
                        RuntimeEventKind::Message {
                            replica: self.log.identity().clone(),
                            message,
                        }
                    }));
                self.enqueue_apply(light.take_committed_entries())?;
                if self.outbound.is_empty() {
                    self.ready_bytes = 0;
                }
                Ok(true)
            }
            Phase::Apply { applied, key, .. } => {
                let event = RuntimeEventKind::Applied {
                    ticket: key.key.clone(),
                    applied: applied.clone(),
                };
                if outbox.push(event).is_some() {
                    return Ok(false);
                }
                self.node.advance_apply_to(applied.position.index.get());
                if let Some(entry) = self.apply.pop_front() {
                    self.apply_bytes -= entry_bytes(&entry);
                }
                self.apply_ticket = None;
                self.phase = Phase::Idle;
                Ok(true)
            }
            Phase::Idle => {
                if let Some(checkpoint) = self.checkpoint.take() {
                    let ticket = operations.ticket(
                        self.log.identity(),
                        checkpoint.position(),
                        checkpoint
                            .digest()
                            .map_err(|error| RuntimeError::Invalid(error.to_string()))?,
                    )?;
                    let key = WriteTicket {
                        key: ticket,
                        ready: 0,
                    };
                    self.write = Some((key.clone(), checkpoint.write()));
                    self.phase = Phase::Checkpoint {
                        checkpoint,
                        key,
                        synced: false,
                    };
                    return Ok(true);
                }
                if self.ready_bytes != 0 {
                    self.ready_bytes = 0;
                    return Ok(true);
                }
                if self.node.has_ready()
                    && ((can_collect && self.apply_bytes < options.max_group_apply_bytes)
                        || self.node.raft.raft_log.unstable_snapshot().is_some())
                {
                    let ready = self.node.ready();
                    self.ready_bytes = ready_bytes(&ready);
                    if self.ready_bytes > options.max_ready_bytes {
                        return Err(RuntimeError::Overloaded);
                    }
                    let key = WriteTicket {
                        key: operations.ticket(
                            self.log.identity(),
                            LogPosition::default(),
                            [0; 32],
                        )?,
                        ready: ready.number(),
                    };
                    if ready.snapshot().is_empty() {
                        self.write = Some((key.clone(), ready_write(&self.log, &ready)));
                    }
                    self.phase = Phase::Ready {
                        ready: Box::new(ready),
                        key,
                        synced: false,
                        snapshot_ticket: None,
                        snapshot_authorized: false,
                        messages_taken: false,
                    };
                    return Ok(true);
                }
                let Some(entry) = self.apply.front() else {
                    return Ok(false);
                };
                let position = entry_position(entry);
                if entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty() {
                    if let Some(completed) = self.finished_apply.take() {
                        let mut write = GroupWrite::new(&self.log);
                        let applied = DurableApply {
                            position,
                            image: Some(completed.image),
                        };
                        write.applied = Some(applied.clone());
                        let key = WriteTicket {
                            key: completed.ticket,
                            ready: 0,
                        };
                        self.write = Some((key.clone(), write));
                        self.phase = Phase::Apply {
                            applied,
                            key,
                            synced: false,
                        };
                        return Ok(true);
                    }
                    if self.apply_ticket.is_none() {
                        let ticket = operations.ticket(
                            self.log.identity(),
                            position,
                            entry_digest(entry)?,
                        )?;
                        if outbox
                            .push(RuntimeEventKind::Apply {
                                ticket: ticket.clone(),
                                entry: entry.clone(),
                            })
                            .is_none()
                        {
                            self.apply_ticket = Some(ticket);
                            return Ok(true);
                        }
                    }
                    return Ok(false);
                }
                let mut write = GroupWrite::new(&self.log);
                match entry.get_entry_type() {
                    EntryType::EntryNormal => (),
                    EntryType::EntryConfChange => {
                        let change = ConfChange::parse_from_bytes(&entry.data)
                            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
                        write.conf_state = Some(self.node.apply_conf_change(&change)?);
                    }
                    EntryType::EntryConfChangeV2 => {
                        let change = ConfChangeV2::parse_from_bytes(&entry.data)
                            .map_err(|e| RuntimeError::Protocol(e.to_string()))?;
                        write.conf_state = Some(self.node.apply_conf_change(&change)?);
                    }
                }
                if write.conf_state.as_ref().is_some_and(|c| {
                    c.voters.len()
                        + c.voters_outgoing.len()
                        + c.learners.len()
                        + c.learners_next.len()
                        > options.max_members_per_group * 2
                }) {
                    return Err(RuntimeError::Overloaded);
                }
                let applied = DurableApply {
                    position,
                    image: self.log.applied()?.image,
                };
                write.applied = Some(applied.clone());
                let key = WriteTicket {
                    key: operations.ticket(self.log.identity(), position, entry_digest(entry)?)?,
                    ready: 0,
                };
                self.write = Some((key.clone(), write));
                self.phase = Phase::Apply {
                    applied,
                    key,
                    synced: false,
                };
                Ok(true)
            }
        }
    }

    fn advance_ready(&mut self, operations: &mut Operations) -> Result<bool, RuntimeError> {
        let Phase::Ready { mut ready, .. } = std::mem::replace(&mut self.phase, Phase::Idle) else {
            return Err(RuntimeError::Protocol("missing Ready".into()));
        };
        let snapshot_index =
            (!ready.snapshot().is_empty()).then_some(ready.snapshot().get_metadata().index);
        if snapshot_index.is_some() {
            self.apply.clear();
            self.apply_bytes = 0;
            self.apply_ticket = None;
            self.finished_apply = None;
        }
        self.enqueue_apply(ready.take_committed_entries())?;
        let light = self.node.advance_append(*ready);
        if let Some(index) = snapshot_index {
            self.node.advance_apply_to(index);
        }
        let key = if let Some(commit) = light.commit_index() {
            let mut write = GroupWrite::new(&self.log);
            let mut hard = self.log.initial_state()?.hard_state;
            hard.commit = commit;
            write.hard_state = Some(hard);
            let key = WriteTicket {
                key: operations.ticket(self.log.identity(), LogPosition::default(), [0; 32])?,
                ready: 0,
            };
            self.write = Some((key.clone(), write));
            Some(key)
        } else {
            None
        };
        self.ready_bytes = self.ready_bytes.saturating_add(
            light
                .messages()
                .iter()
                .map(|m| m.compute_size() as usize)
                .sum::<usize>()
                + light
                    .committed_entries()
                    .iter()
                    .map(entry_bytes)
                    .sum::<usize>(),
        );
        self.phase = Phase::Light {
            light: Box::new(light),
            synced: key.is_none(),
            key,
        };
        Ok(true)
    }

    fn enqueue_apply(&mut self, entries: Vec<Entry>) -> Result<(), RuntimeError> {
        let applied = self.log.applied()?.position.index.get();
        let committed = self.log.initial_state()?.hard_state.commit;
        let mut previous = self.apply.back().map_or(applied, |e| e.index);
        for entry in entries {
            if entry.index != previous.checked_add(1).ok_or(RuntimeError::Overloaded)?
                || entry.index > committed
            {
                return Err(RuntimeError::Protocol(
                    "unordered or unpersisted committed entries".into(),
                ));
            }
            previous = entry.index;
            self.apply_bytes = self
                .apply_bytes
                .checked_add(entry_bytes(&entry))
                .ok_or(RuntimeError::Overloaded)?;
            self.apply.push_back(entry);
        }
        Ok(())
    }
}

fn ready_write(log: &GroupLog, ready: &Ready) -> GroupWrite {
    let mut write = GroupWrite::new(log);
    write.entries = ready.entries().clone();
    write.hard_state = ready.hs().cloned();
    write
}

fn entry_bytes(entry: &Entry) -> usize {
    entry.compute_size() as usize + 128
}

fn conf_member_count(conf: &::raft::prelude::ConfState) -> usize {
    conf.voters
        .iter()
        .chain(&conf.voters_outgoing)
        .chain(&conf.learners)
        .chain(&conf.learners_next)
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

fn ready_bytes(ready: &Ready) -> usize {
    4096 + ready.entries().iter().map(entry_bytes).sum::<usize>()
        + ready
            .read_states()
            .iter()
            .map(|s| s.request_ctx.len() + 128)
            .sum::<usize>()
        + ready
            .committed_entries()
            .iter()
            .map(entry_bytes)
            .sum::<usize>()
        + ready
            .messages()
            .iter()
            .chain(ready.persisted_messages())
            .map(|m| m.compute_size() as usize + 128)
            .sum::<usize>()
        + ready.snapshot().compute_size() as usize
}
