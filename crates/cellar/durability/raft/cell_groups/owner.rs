// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One physical group owns one RawNode. The node scheduler executes the emitted
//! effects; this owner performs no image I/O and starts no per-cell tasks.

use super::{CellGroupBootstrapPermit, GroupManifest, PreparedGroupManifest};
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryLimits,
};
use crate::raft::{
    cache::{ConsensusStorage, GroupLog, GroupWrite},
    completion::{entry_digest, entry_position, RuntimeError},
    options::RaftRuntimeOptions,
    storage::ReplicaLog,
};
use ::raft::{prelude::*, Config, LightReady, RawNode, Ready, StateRole, Storage};
use cellar_logic::{
    cell_group::*,
    consensus::{LogIndex, LogPosition, OperationId, Term},
};
use protobuf::Message as _;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

struct Lease(Arc<AtomicBool>);

impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct CellGroupTicket {
    lease: Arc<Lease>,
    sequence: u64,
    position: LogPosition,
    digest: [u8; 32],
}

impl CellGroupTicket {
    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn entry_digest(&self) -> [u8; 32] {
        self.digest
    }

    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.lease, &other.lease)
            && self.sequence == other.sequence
            && self.position == other.position
            && self.digest == other.digest
    }
}

/// Physical work retains the owner lease even if its awaiting client is dropped.
/// The engine request also retains that lease until sync/publication finishes.
pub struct CellGroupPersist {
    log: ReplicaLog,
    ticket: CellGroupTicket,
    write: GroupWrite<GroupHistory>,
}

pub struct CellGroupPersisted {
    ticket: CellGroupTicket,
    sequence: u64,
}

impl CellGroupPersist {
    /// Execute through the existing node-wide persistence worker. A lost waiter
    /// cannot advance the owner; its pending work must settle or restart recover.
    pub async fn run(self) -> Result<CellGroupPersisted, RuntimeError> {
        let operation = OperationId::new(self.ticket.sequence);
        let receipt = self.log.persist_cells(operation, vec![self.write]).await?;
        Ok(CellGroupPersisted {
            ticket: self.ticket,
            sequence: receipt.sequence,
        })
    }
}

pub struct CellGroupApply {
    pub ticket: CellGroupTicket,
    pub entry: Entry,
    pub base: PreparedGroupManifest,
}

/// A ReadIndex result is a barrier to wait for, not permission to serve a cell.
/// The dispatcher must recheck owner/term and pin an applied coherent cell view.
pub enum CellGroupEffect {
    Persist(Box<CellGroupPersist>),
    Message(Message),
    Apply(Box<CellGroupApply>),
    Applied {
        ticket: CellGroupTicket,
        manifest: Box<GroupManifest>,
    },
    ReadIndex {
        term: Term,
        index: LogIndex,
        context: Vec<u8>,
    },
}

#[derive(Clone, Debug)]
pub struct CellGroupProposal {
    pub group: GroupHistory,
    pub position: LogPosition,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct CellGroupStatus {
    pub term: Term,
    pub leader: Option<u64>,
    pub applied: LogPosition,
    pub committed: LogIndex,
    pub ready: bool,
}

enum Phase {
    Ready(Box<Ready>, Term),
    Light(LightReady),
    Apply(CellGroupTicket, Box<PreparedGroupManifest>, bool),
}

pub struct CellGroupOwner {
    lease: Arc<Lease>,
    log: ReplicaLog,
    group: GroupLog<GroupHistory>,
    node: RawNode<ConsensusStorage>,
    current: PreparedGroupManifest,
    options: RaftRuntimeOptions,
    phase: Option<Phase>,
    waiting: Option<CellGroupTicket>,
    effects: VecDeque<CellGroupEffect>,
    apply: VecDeque<Entry>,
    apply_bytes: usize,
    applying: Option<CellGroupTicket>,
    next_ticket: u64,
    last_sync: u64,
    failed: Option<RuntimeError>,
}

impl CellGroupOwner {
    /// Attach only a claimed initial voter with a completely verified selected
    /// root. The catalog permit is revalidated; serving additionally waits for
    /// the current term's committed/applied Raft barrier. Managed members and
    /// incoming snapshots remain closed until their complete protocols exist.
    pub async fn start(
        permit: CellGroupBootstrapPermit,
        current: PreparedGroupManifest,
        options: RaftRuntimeOptions,
        seed: [u8; 32],
    ) -> Result<Self, RuntimeError> {
        options.validate(permit.log().limits())?;
        let group = permit.group().clone();
        group
            .consensus_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| RuntimeError::AlreadyAttached)?;
        let lease = Arc::new(Lease(group.consensus_claimed.clone()));
        permit
            .revalidate()
            .await
            .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
        let cache = group.read()?;
        if !Arc::ptr_eq(&current.engine_owner, &group.owner)
            || cache.group_manifest.as_ref() != Some(current.marker())
            || current.marker().route() != permit.route()
            || !cache.membership.permits_group(group.identity())
            || cache.conf_state.voters.len() + cache.conf_state.learners.len()
                > options.max_members_per_group
        {
            return Err(RuntimeError::Invalid(
                "cell-group startup closure/member".into(),
            ));
        }
        drop(cache);
        let config = Config {
            id: group.identity().member.get(),
            applied: current.marker().position().index.get(),
            election_tick: 20,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            read_only_option: ::raft::ReadOnlyOption::Safe,
            max_size_per_msg: options.max_message_bytes as u64,
            max_inflight_msgs: 8,
            max_uncommitted_size: options.max_group_apply_bytes as u64,
            // A valid entry has at least four encoded bytes (term/index). Its
            // retained accounting adds 128 bytes. Budget for those small
            // entries as well as payloads; raft-rs may still return one entry
            // larger than this soft byte cap to guarantee progress.
            max_committed_size_per_ready: (options.max_message_bytes / 66).max(1) as u64,
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let node = RawNode::new_with_election_seed(
            &config,
            group.consensus_storage(),
            &logger,
            Some(seed),
        )?;
        Ok(Self {
            lease,
            log: permit.log().clone(),
            group,
            node,
            current,
            options,
            phase: None,
            waiting: None,
            effects: VecDeque::new(),
            apply: VecDeque::new(),
            apply_bytes: 0,
            applying: None,
            next_ticket: 0,
            last_sync: 0,
            failed: None,
        })
    }

    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.current
    }

    pub fn status(&self) -> Result<CellGroupStatus, RuntimeError> {
        self.live()?;
        let cache = self.group.read()?;
        Ok(CellGroupStatus {
            term: Term::new(self.node.raft.term),
            leader: (self.node.raft.leader_id != 0).then_some(self.node.raft.leader_id),
            applied: self.current.marker().position(),
            committed: LogIndex::new(cache.hard_state.commit),
            ready: self.node.raft.state == StateRole::Leader
                && self.phase.is_none()
                && self.apply_bytes < self.options.max_group_apply_bytes
                && self.current.marker().position().term.get() == self.node.raft.term
                && cache.hard_state.term == self.node.raft.term
                && cache.group_manifest.as_ref() == Some(self.current.marker()),
        })
    }

    pub fn campaign(&mut self) -> Result<(), RuntimeError> {
        self.live()?;
        if self.phase.is_some() {
            return Err(RuntimeError::NotReady);
        }
        self.node.campaign()?;
        Ok(())
    }

    pub fn tick(&mut self) -> Result<(), RuntimeError> {
        self.live()?;
        // The scheduler coalesces a deferred tick until pending Ready work is
        // drained. In particular, an arbitrarily slow fsync must not accumulate
        // an unbounded vector of heartbeat/election messages inside RawNode.
        if self.phase.is_some() || self.node.has_ready() {
            return Err(RuntimeError::NotReady);
        }
        self.node.tick();
        Ok(())
    }

    pub fn step(&mut self, message: Message) -> Result<(), RuntimeError> {
        self.live()?;
        if self.phase.is_some() {
            return Err(RuntimeError::NotReady);
        }
        if self.apply_bytes >= self.options.max_group_apply_bytes {
            return Err(RuntimeError::NotReady);
        }
        self.validate_message(&message)?;
        self.node.step(message)?;
        Ok(())
    }

    /// Protocol admission only. The shared cell executor must serialize each
    /// cell's preparation, reserve physical work and retain ambiguous outcomes.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<CellGroupProposal, RuntimeError> {
        if !self.status()?.ready {
            return Err(RuntimeError::NotReady);
        }
        if data.len() > self.options.max_proposal_bytes {
            return Err(RuntimeError::Overloaded);
        }
        let entry = CellGroupEntry::decode(
            &data,
            self.current.marker().route(),
            GroupEntryLimits::default(),
        )
        .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
        if !matches!(
            entry.command,
            GroupCommand::Enroll { .. } | GroupCommand::Mutation(_)
        ) {
            return Err(RuntimeError::Invalid(
                "group administration is not attached".into(),
            ));
        }
        let before = self.node.raft.raft_log.last_index();
        self.node.propose(Vec::new(), data)?;
        let entry = self
            .node
            .raft
            .raft_log
            .unstable_entries()
            .last()
            .ok_or(RuntimeError::NotReady)?;
        if entry.index != before + 1 {
            return Err(RuntimeError::NotReady);
        }
        Ok(CellGroupProposal {
            group: self.group.identity().scope,
            position: entry_position(entry),
            digest: entry_digest(entry)?,
        })
    }

    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), RuntimeError> {
        if !self.status()?.ready {
            return Err(RuntimeError::NotReady);
        }
        if context.is_empty() || context.len() > 4096 {
            return Err(RuntimeError::Invalid("read context".into()));
        }
        if self.node.raft.read_only.pending_read_count() >= self.options.max_pending_reads {
            return Err(RuntimeError::Overloaded);
        }
        self.node.read_index(context);
        Ok(())
    }

    pub fn complete_persist(&mut self, completed: CellGroupPersisted) -> Result<(), RuntimeError> {
        self.live()?;
        if self
            .waiting
            .as_ref()
            .is_none_or(|t| !t.matches(&completed.ticket))
            || completed.sequence <= self.last_sync
        {
            return Err(RuntimeError::StaleCompletion);
        }
        self.last_sync = completed.sequence;
        self.waiting = None;
        Ok(())
    }

    pub fn complete_apply(
        &mut self,
        ticket: CellGroupTicket,
        next: PreparedGroupManifest,
    ) -> Result<(), RuntimeError> {
        self.live()?;
        if self.phase.is_some() {
            return Err(RuntimeError::NotReady);
        }
        if self.applying.as_ref().is_none_or(|t| !t.matches(&ticket))
            || next.marker().position() != ticket.position
            || next.base.as_ref() != Some(self.current.marker())
            || next.marker().route() != self.current.marker().route()
            || !Arc::ptr_eq(&next.engine_owner, &self.group.owner)
            || !Arc::ptr_eq(&next.page_owner, &self.current.page_owner)
        {
            return Err(RuntimeError::StaleCompletion);
        }
        let entry = self.apply.front().ok_or(RuntimeError::StaleCompletion)?;
        self.validate_apply(entry, &next)?;
        self.phase = Some(Phase::Apply(ticket, Box::new(next), false));
        Ok(())
    }

    /// Return one bounded effect. No Ready-dependent message or applied event is
    /// released until its exact physical persistence completion is consumed.
    pub fn poll(&mut self) -> Result<Option<CellGroupEffect>, RuntimeError> {
        self.live()?;
        let result = self.poll_inner();
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        result
    }

    fn live(&self) -> Result<(), RuntimeError> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if self.group.closed.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if self.node.raft.term >= u64::MAX - 2
            || self.node.raft.raft_log.last_index() >= u64::MAX - 2
        {
            return Err(RuntimeError::Overloaded);
        }
        Ok(())
    }

    fn ticket(
        &mut self,
        position: LogPosition,
        digest: [u8; 32],
    ) -> Result<CellGroupTicket, RuntimeError> {
        self.next_ticket = self
            .next_ticket
            .checked_add(1)
            .ok_or(RuntimeError::Overloaded)?;
        Ok(CellGroupTicket {
            lease: self.lease.clone(),
            sequence: self.next_ticket,
            position,
            digest,
        })
    }

    fn persistence(
        &mut self,
        mut write: GroupWrite<GroupHistory>,
    ) -> Result<CellGroupEffect, RuntimeError> {
        let ticket = self.ticket(LogPosition::default(), [0; 32])?;
        write.consensus_pin = Some(self.lease.clone());
        self.waiting = Some(ticket.clone());
        Ok(CellGroupEffect::Persist(Box::new(CellGroupPersist {
            log: self.log.clone(),
            ticket,
            write,
        })))
    }

    fn enqueue(&mut self, entries: Vec<Entry>) -> Result<(), RuntimeError> {
        let mut previous = self
            .apply
            .back()
            .map_or(self.current.marker().position().index.get(), |e| e.index);
        let committed = self.group.initial_state()?.hard_state.commit;
        for entry in entries {
            if entry.index != previous + 1 || entry.index > committed {
                return Err(RuntimeError::Protocol(
                    "noncontiguous/unpersisted group apply".into(),
                ));
            }
            previous = entry.index;
            self.apply_bytes = self
                .apply_bytes
                .checked_add(entry.compute_size() as usize + 128)
                .ok_or(RuntimeError::Overloaded)?;
            // Ready and its LightReady can each retain one oversize entry.
            // Collection stops at the soft allowance; these two bounded
            // in-flight batches must be drained, not treated as data failure.
            if self.apply_bytes
                > self.options.max_group_apply_bytes + 2 * self.options.max_message_bytes + 256
            {
                return Err(RuntimeError::Overloaded);
            }
            self.apply.push_back(entry);
        }
        Ok(())
    }

    fn messages(&mut self, messages: Vec<Message>) -> Result<(), RuntimeError> {
        if self.effects.len() + messages.len() > self.options.max_events {
            return Err(RuntimeError::Overloaded);
        }
        let existing: usize = self
            .effects
            .iter()
            .map(|event| match event {
                CellGroupEffect::Message(message) => message.compute_size() as usize + 128,
                CellGroupEffect::ReadIndex { context, .. } => context.len() + 128,
                _ => 4096,
            })
            .sum();
        let added: usize = messages
            .iter()
            .map(|m| m.compute_size() as usize + 128)
            .sum();
        if existing.saturating_add(added) > self.options.max_event_bytes {
            return Err(RuntimeError::Overloaded);
        }
        self.effects
            .extend(messages.into_iter().map(CellGroupEffect::Message));
        Ok(())
    }

    fn poll_inner(&mut self) -> Result<Option<CellGroupEffect>, RuntimeError> {
        loop {
            if let Some(event) = self.effects.pop_front() {
                return Ok(Some(event));
            }
            if self.waiting.is_some() {
                return Ok(None);
            }
            match self.phase.take() {
                Some(Phase::Ready(mut ready, term)) => {
                    self.messages(ready.take_messages())?;
                    self.messages(ready.take_persisted_messages())?;
                    let reads = ready.take_read_states();
                    let queued: usize = self
                        .effects
                        .iter()
                        .map(|event| match event {
                            CellGroupEffect::Message(message) => {
                                message.compute_size() as usize + 128
                            }
                            _ => 4096,
                        })
                        .sum();
                    let read_bytes: usize = reads.iter().map(|r| r.request_ctx.len() + 128).sum();
                    if self.effects.len() + reads.len() > self.options.max_events
                        || queued.saturating_add(read_bytes) > self.options.max_event_bytes
                    {
                        return Err(RuntimeError::Overloaded);
                    }
                    for read in reads {
                        self.effects.push_back(CellGroupEffect::ReadIndex {
                            term,
                            index: LogIndex::new(read.index),
                            context: read.request_ctx.to_vec(),
                        });
                    }
                    self.enqueue(ready.take_committed_entries())?;
                    let light = self.node.advance_append(*ready);
                    let commit = light.commit_index();
                    self.phase = Some(Phase::Light(light));
                    if let Some(commit) = commit {
                        let mut write = GroupWrite::new(&self.group);
                        let mut hard = self.group.initial_state()?.hard_state;
                        hard.commit = commit;
                        write.hard_state = Some(hard);
                        return self.persistence(write).map(Some);
                    }
                }
                Some(Phase::Light(mut light)) => {
                    self.messages(light.take_messages())?;
                    self.enqueue(light.take_committed_entries())?;
                }
                Some(Phase::Apply(ticket, next, submitted)) => {
                    if !submitted {
                        let mut write = GroupWrite::new(&self.group);
                        write.group_manifest = Some((*next).clone());
                        self.phase = Some(Phase::Apply(ticket, next, true));
                        return self.persistence(write).map(Some);
                    }
                    if self.group.group_manifest()?.as_ref() != Some(next.marker()) {
                        return Err(RuntimeError::StaleCompletion);
                    }
                    self.node.advance_apply_to(ticket.position.index.get());
                    let entry = self
                        .apply
                        .pop_front()
                        .ok_or(RuntimeError::StaleCompletion)?;
                    self.apply_bytes -= entry.compute_size() as usize + 128;
                    self.applying = None;
                    self.current = *next;
                    return Ok(Some(CellGroupEffect::Applied {
                        ticket,
                        manifest: Box::new(self.current.marker().clone()),
                    }));
                }
                None => {
                    if self.node.has_ready()
                        && self.apply_bytes < self.options.max_group_apply_bytes
                    {
                        let ready = self.node.ready();
                        if !ready.snapshot().is_empty() {
                            return Err(RuntimeError::Invalid(
                                "group snapshot is not attached".into(),
                            ));
                        }
                        let size: usize = ready
                            .entries()
                            .iter()
                            .chain(ready.committed_entries())
                            .map(|e| e.compute_size() as usize + 128)
                            .sum::<usize>()
                            + ready
                                .messages()
                                .iter()
                                .chain(ready.persisted_messages())
                                .map(|m| m.compute_size() as usize + 128)
                                .sum::<usize>()
                            + ready
                                .read_states()
                                .iter()
                                .map(|r| r.request_ctx.len() + 128)
                                .sum::<usize>();
                        if size > self.options.max_ready_bytes {
                            return Err(RuntimeError::Overloaded);
                        }
                        let changed = !ready.entries().is_empty() || ready.hs().is_some();
                        let mut write = GroupWrite::new(&self.group);
                        write.entries = ready.entries().clone();
                        write.hard_state = ready.hs().cloned();
                        self.phase = Some(Phase::Ready(
                            Box::new(ready),
                            Term::new(self.node.raft.term),
                        ));
                        if changed {
                            return self.persistence(write).map(Some);
                        }
                        continue;
                    }
                    if self.applying.is_some() {
                        return Ok(None);
                    }
                    let Some(entry) = self.apply.front().cloned() else {
                        return Ok(None);
                    };
                    let ticket = self.ticket(entry_position(&entry), entry_digest(&entry)?)?;
                    self.applying = Some(ticket.clone());
                    return Ok(Some(CellGroupEffect::Apply(Box::new(CellGroupApply {
                        ticket,
                        entry,
                        base: self.current.clone(),
                    }))));
                }
            }
        }
    }

    fn validate_apply(
        &self,
        entry: &Entry,
        next: &PreparedGroupManifest,
    ) -> Result<(), RuntimeError> {
        let invalid = || RuntimeError::Invalid("group apply does not match committed entry".into());
        if entry.get_entry_type() != EntryType::EntryNormal {
            return Err(invalid());
        }
        if entry.data.is_empty() {
            return if next.marker().root() == self.current.marker().root() {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        let decoded = CellGroupEntry::decode(
            &entry.data,
            self.current.marker().route(),
            GroupEntryLimits::default(),
        )
        .map_err(|_| invalid())?;
        match decoded.command {
            GroupCommand::Enroll { ref cell, .. } => {
                let old = self.current.pin_cell(&cell.cell);
                let decision = super::decide_enrollment(
                    self.current.marker().route(),
                    super::EnrollmentPhase::Active,
                    &entry.data,
                    old.as_ref().map(|(leaf, _)| &leaf.enrollment),
                    GroupEntryLimits::default(),
                );
                match decision {
                    Ok(super::EnrollmentDecision::Propose(record)) => {
                        let (leaf, _) = next.pin_cell(&cell.cell).ok_or_else(invalid)?;
                        if leaf.enrollment != record
                            || leaf.version.sequence.get() != 1
                            || leaf.version.image != record.initial()
                            || leaf.last_mutation != entry_position(entry)
                        {
                            return Err(invalid());
                        }
                    }
                    Ok(super::EnrollmentDecision::Existing(_))
                    | Err(super::EnrollmentError::Conflict) => {
                        if next.marker().root() != self.current.marker().root() {
                            return Err(invalid());
                        }
                    }
                    Err(_) => return Err(invalid()),
                }
            }
            GroupCommand::Mutation(mutation) => {
                let (old, _) = self
                    .current
                    .pin_cell(&mutation.cell.cell)
                    .ok_or_else(invalid)?;
                mutation.check_base(&old.version).map_err(|_| invalid())?;
                let (new, _) = next.pin_cell(&mutation.cell.cell).ok_or_else(invalid)?;
                if new.version.history != mutation.cell
                    || new.version.sequence != mutation.sequence
                    || new.version.image != mutation.result.into()
                    || new.enrollment != old.enrollment
                    || new.last_mutation != entry_position(entry)
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }

    fn validate_message(&self, message: &Message) -> Result<(), RuntimeError> {
        let invalid = || RuntimeError::Invalid("cell-group peer message".into());
        let kind = message.get_msg_type();
        if message.to != self.group.identity().member.get()
            || message.from == message.to
            || message.from == 0
            || self.node.raft.prs().get(message.from).is_none()
            || ::raft::raw_node::is_local_msg(kind)
            || matches!(
                kind,
                MessageType::MsgPropose | MessageType::MsgTransferLeader | MessageType::MsgSnapshot
            )
            || !message.get_snapshot().is_empty()
            || message.entries.len() > 65_536
            || message.compute_size() as usize > self.options.max_message_bytes
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
        {
            return Err(invalid());
        }
        match kind {
            MessageType::MsgAppend => {
                let mut index = message.index;
                let mut term = message.log_term;
                if (index == 0) != (term == 0) {
                    return Err(invalid());
                }
                for entry in &message.entries {
                    index = index.checked_add(1).ok_or_else(invalid)?;
                    if entry.index != index
                        || entry.index >= u64::MAX - 2
                        || entry.term == 0
                        || entry.term < term
                        || entry.term > message.term
                        || entry.get_entry_type() != EntryType::EntryNormal
                        || entry.data.len() > self.options.max_proposal_bytes
                    {
                        return Err(invalid());
                    }
                    term = entry.term;
                }
            }
            MessageType::MsgReadIndex | MessageType::MsgReadIndexResp => {
                if message.entries.len() != 1 {
                    return Err(invalid());
                }
                let entry = &message.entries[0];
                if entry.data.is_empty()
                    || entry.data.len() > 4096
                    || entry.index != 0
                    || entry.term != 0
                    || !entry.context.is_empty()
                    || entry.get_entry_type() != EntryType::EntryNormal
                {
                    return Err(invalid());
                }
                if kind == MessageType::MsgReadIndex
                    && self.node.raft.read_only.pending_read_count()
                        >= self.options.max_pending_reads
                {
                    return Err(RuntimeError::Overloaded);
                }
            }
            _ if !message.entries.is_empty() => return Err(invalid()),
            _ => (),
        }
        Ok(())
    }
}
