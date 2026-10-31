// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One physical group owns one RawNode. The node scheduler executes the emitted
//! effects; this owner performs no image I/O and starts no per-cell tasks.

use super::{
    CellGroupBootstrapPermit, GroupManifest, PreparedCellGroupCheckpoint, PreparedGroupManifest,
    PreparedLocalCellGroupCheckpoint,
};
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryLimits,
};
use crate::raft::{
    cache::{ConsensusStorage, GroupLog, GroupWrite, SnapshotChange},
    completion::{entry_digest, entry_position, RuntimeError},
    options::RaftRuntimeOptions,
    storage::ReplicaLog,
};
use ::raft::{prelude::*, Config, LightReady, RawNode, Ready, StateRole, Storage};
use cellar_logic::{
    cell_group::*,
    consensus::{LogIndex, LogPosition, OperationId, ReplicaIdentity, Term},
};
use protobuf::Message as _;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    pub(super) admission: Option<Arc<crate::raft::budget::Permit>>,
    pub(crate) preparation: Option<Arc<crate::raft::budget::Permit>>,
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
    pub(super) ticket: CellGroupTicket,
    pub(super) write: GroupWrite<GroupHistory>,
}

pub struct CellGroupPersisted {
    pub(super) ticket: CellGroupTicket,
    pub(super) sequence: u64,
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

#[derive(Clone)]
pub struct CellGroupApply {
    pub ticket: CellGroupTicket,
    pub entry: Entry,
    pub base: PreparedGroupManifest,
}

/// Snapshot admission does not establish persistence or serving authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellGroupSnapshotAdmission {
    /// Raft selected the verified snapshot; persistence is still outstanding.
    Restoring,
    /// Raft retained its log. Any resulting hard state/messages still need sync.
    Ignored,
}

pub enum CellGroupEffect {
    Persist(Box<CellGroupPersist>),
    Message(Message),
    Apply(Box<CellGroupApply>),
    Applied {
        ticket: CellGroupTicket,
        manifest: Box<GroupManifest>,
    },
    CheckpointInstalled {
        manifest: Box<GroupManifest>,
    },
    CheckpointPublished {
        manifest: Box<GroupManifest>,
    },
    /// A barrier to wait for, not permission to serve a cell. The dispatcher
    /// must recheck owner/term and pin an applied coherent cell view.
    ReadIndex {
        term: Term,
        index: LogIndex,
        context: Vec<u8>,
    },
}

impl CellGroupEffect {
    pub(super) fn bytes(&self) -> usize {
        4096 + match self {
            Self::Persist(work) => work.write.estimated_bytes().unwrap_or(usize::MAX - 4096),
            Self::Message(message) => message.compute_size() as usize,
            Self::Apply(work) => work.entry.compute_size() as usize + work.base.reserved_bytes(),
            Self::ReadIndex { context, .. } => context.len(),
            Self::Applied { .. }
            | Self::CheckpointInstalled { .. }
            | Self::CheckpointPublished { .. } => 1024,
        }
    }
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
    Checkpoint(PreparedLocalCellGroupCheckpoint, bool),
}

pub struct CellGroupOwner {
    leader_term: Arc<AtomicU64>,
    lease: Arc<Lease>,
    log: ReplicaLog,
    group: GroupLog<GroupHistory>,
    node: RawNode<ConsensusStorage>,
    current: PreparedGroupManifest,
    checkpoint: Option<PreparedCellGroupCheckpoint>,
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
    proposals: super::proposal_admission::ProposalAdmission,
    pub(super) proposal_budget: Arc<crate::raft::budget::Budget>,
    prepared_only: bool,
}

impl CellGroupOwner {
    /// Attach only a claimed initial voter with a completely verified selected
    /// root. The catalog permit is revalidated; serving additionally waits for
    /// the current term's committed/applied Raft barrier. Managed members and
    /// ordinary snapshot messages remain closed; step_checkpoint requires the
    /// exact independently verified destination closure.
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
        current
            .require_checkpoint(&group)
            .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
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
            leader_term: Arc::new(AtomicU64::new(0)),
            lease,
            log: permit.log().clone(),
            group,
            node,
            current,
            checkpoint: None,
            proposal_budget: crate::raft::budget::Budget::new(
                options.max_request_bytes,
                options.max_requests,
            ),
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
            proposals: Default::default(),
            prepared_only: false,
        })
    }

    pub(super) fn revoke_execution(&mut self) {
        self.leader_term.store(0, Ordering::Release);
        self.proposals.clear();
    }

    fn refresh_execution_fence(&mut self) {
        let term = if self.node.raft.state == StateRole::Leader
            && self.node.raft.lead_transferee.is_none()
            && self
                .node
                .raft
                .prs()
                .conf()
                .voters()
                .contains(self.node.raft.id)
        {
            self.node.raft.term
        } else {
            0
        };
        if self.leader_term.swap(term, Ordering::AcqRel) != term {
            self.proposals.clear();
        }
    }

    pub fn execution_authority(
        &self,
        cell: &CellHistory,
    ) -> Result<super::CellExecutionAuthority, RuntimeError> {
        let status = self.status()?;
        // Role refusal is distinct from a leader still awaiting durable apply.
        // Remote callers must not retry a follower as generic unavailability.
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        if !status.ready || status.applied.index != status.committed {
            return Err(RuntimeError::NotReady);
        }
        super::CellExecutionAuthority::new(
            self.group.clone(),
            &self.current,
            cell,
            status.term,
            self.leader_term.clone(),
        )
    }

    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.current
    }

    pub(super) fn select_read(
        &self,
        cell: &CellHistory,
        consistency: &super::CellReadConsistency,
        term: Option<Term>,
        barrier: Option<LogIndex>,
    ) -> Result<super::read_authority::ReadSelection, RuntimeError> {
        self.live()?;
        let status = self.status()?;
        let leader = if matches!(consistency, super::CellReadConsistency::Linearizable) {
            if self.node.raft.state != StateRole::Leader || Some(status.term) != term {
                return Err(RuntimeError::NotLeader);
            }
            let index = barrier.ok_or(RuntimeError::NotReady)?;
            if !status.ready || status.applied.index < index {
                return Err(RuntimeError::NotReady);
            }
            Some((status.term, self.leader_term.clone()))
        } else {
            None
        };
        let (leaf, image) = self
            .current
            .pin_cell(&cell.cell)
            .ok_or(RuntimeError::NotFound)?;
        if &leaf.version.history != cell {
            return Err(RuntimeError::Invalid("cell read history".into()));
        }
        if let super::CellReadConsistency::AtLeast(minimum) = consistency {
            if !leaf
                .version
                .covers(minimum)
                .map_err(|_| RuntimeError::Invalid("cell read version".into()))?
            {
                return Err(RuntimeError::NotReady);
            }
        }
        Ok(super::read_authority::ReadSelection {
            group: self.group.clone(),
            route: self.current.marker().route().clone(),
            cell: leaf.clone(),
            image,
            applied: status.applied,
            barrier: term
                .zip(barrier)
                .map(|(term, index)| LogPosition { term, index }),
            leader,
        })
    }

    pub(super) fn check_seed_target(
        &self,
        target: &ReplicaIdentity<GroupHistory>,
    ) -> Result<(), RuntimeError> {
        self.live()?;
        super::seed_authority::check_target(&self.group, target).map(|_| ())
    }

    pub(super) fn select_seed(
        &self,
        target: &ReplicaIdentity<GroupHistory>,
        term: Option<Term>,
        barrier: Option<LogIndex>,
    ) -> Result<super::seed_authority::SeedSelection, RuntimeError> {
        self.live()?;
        let status = self.status()?;
        if self.node.raft.state != raft::StateRole::Leader || Some(status.term) != term {
            return Err(RuntimeError::NotLeader);
        }
        let index = barrier.ok_or(RuntimeError::NotReady)?;
        if !status.ready
            || status.applied.index < index
            || self.current.marker().membership_revision() > index.get()
            || super::seed_authority::check_target(&self.group, target)? > index
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(super::seed_authority::SeedSelection {
            group: self.group.clone(),
            root: self.current.clone(),
            target: target.clone(),
            barrier: LogPosition {
                term: status.term,
                index,
            },
            leader: self.leader_term.clone(),
        })
    }

    pub fn enrollment_state(
        &self,
        cell: CellHistory,
    ) -> Result<super::CellEnrollmentState, RuntimeError> {
        self.live()?;
        super::CellEnrollmentState::new(self.group.clone(), &self.current, cell)
    }

    pub fn enrollment_authority(
        &self,
        cell: CellHistory,
    ) -> Result<super::CellEnrollmentAuthority, RuntimeError> {
        let status = self.status()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        if !status.ready || status.applied.index != status.committed {
            return Err(RuntimeError::NotReady);
        }
        super::CellEnrollmentAuthority::new(self.enrollment_state(cell)?, self.leader_term.clone())
    }

    pub fn reconciliation_authority(
        &self,
        cell: &CellHistory,
    ) -> Result<super::CellReconciliationAuthority, RuntimeError> {
        self.live()?;
        super::CellReconciliationAuthority::new(self.group.clone(), &self.current, cell)
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
                && self.node.raft.lead_transferee.is_none()
                && self
                    .node
                    .raft
                    .prs()
                    .conf()
                    .voters()
                    .contains(self.node.raft.id)
                && self.phase.is_none()
                && self.checkpoint.is_none()
                && self.apply_bytes < self.options.max_group_apply_bytes
                && self.current.marker().position().term.get() == self.node.raft.term
                && cache.hard_state.term == self.node.raft.term
                && cache.group_manifest.as_ref() == Some(self.current.marker()),
        })
    }

    pub fn campaign(&mut self) -> Result<(), RuntimeError> {
        self.live()?;
        if self.phase.is_some() || self.checkpoint.is_some() {
            return Err(RuntimeError::NotReady);
        }
        let result = self.node.campaign();
        self.refresh_execution_fence();
        result?;
        Ok(())
    }

    /// Admit a handoff to a caught-up incoming voter. Completion is observed
    /// through leadership status; admission alone cannot prove an election.
    pub fn transfer_leader(&mut self, member: u64) -> Result<(), RuntimeError> {
        self.live()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let status = self.status()?;
        let cache = self.group.read()?;
        if member == self.node.raft.id || !cache.conf_state.voters.contains(&member) {
            return Err(RuntimeError::Invalid(
                "leadership target is not another incoming voter".into(),
            ));
        }
        if !status.ready
            || status.applied.index != status.committed
            || !self.apply.is_empty()
            || self.applying.is_some()
            || self.node.raft.pending_conf_index > self.node.raft.raft_log.applied
            || self
                .node
                .raft
                .prs()
                .get(member)
                .is_none_or(|p| p.matched < self.node.raft.raft_log.last_index())
        {
            return Err(RuntimeError::NotReady);
        }
        drop(cache);
        self.node.transfer_leader(member);
        self.refresh_execution_fence();
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
        self.refresh_execution_fence();
        Ok(())
    }

    pub(super) fn validate_snapshot_member(&self, member: u64) -> Result<(), RuntimeError> {
        self.live()?;
        if member == 0 || self.node.raft.prs().get(member).is_none() {
            return Err(RuntimeError::Protocol("unknown snapshot member".into()));
        }
        Ok(())
    }

    pub(super) fn report_snapshot(
        &mut self,
        member: u64,
        delivered: bool,
    ) -> Result<(), RuntimeError> {
        self.validate_snapshot_member(member)?;
        if self.phase.is_some() || self.checkpoint.is_some() {
            return Err(RuntimeError::NotReady);
        }
        self.node.report_snapshot(
            member,
            if delivered {
                ::raft::SnapshotStatus::Finish
            } else {
                ::raft::SnapshotStatus::Failure
            },
        );
        Ok(())
    }

    pub(super) fn report_unreachable(&mut self, member: u64) -> Result<(), RuntimeError> {
        self.live()?;
        if self.phase.is_some() || self.checkpoint.is_some() {
            return Err(RuntimeError::NotReady);
        }
        if member == 0 || self.node.raft.prs().get(member).is_none() {
            return Err(RuntimeError::Protocol("unknown unreachable member".into()));
        }
        self.node.report_unreachable(member);
        Ok(())
    }

    /// Queue publication of the exact applied cut without changing Raft progress
    /// or the applied view. A later CheckpointPublished event proves persistence.
    pub fn publish_checkpoint(
        &mut self,
        checkpoint: PreparedLocalCellGroupCheckpoint,
    ) -> Result<(), RuntimeError> {
        self.live()?;
        if !self.idle() || self.applying.is_some() {
            return Err(RuntimeError::NotReady);
        }
        self.select_local_checkpoint(checkpoint)
    }

    /// The controller holds this exact successor ticket without starting apply.
    /// Publishing its base cannot race another applied root or snapshot install.
    /// Pending Ready work may persist a later suffix; it cannot change this cut.
    pub(super) fn checkpoint_before_apply(
        &mut self,
        ticket: &CellGroupTicket,
        checkpoint: PreparedLocalCellGroupCheckpoint,
    ) -> Result<(), RuntimeError> {
        self.live()?;
        if self.phase.is_some() || self.checkpoint.is_some() || self.waiting.is_some() {
            return Err(RuntimeError::NotReady);
        }
        if self
            .applying
            .as_ref()
            .is_none_or(|active| !active.matches(ticket))
        {
            return Err(RuntimeError::StaleCompletion);
        }
        self.select_local_checkpoint(checkpoint)
    }

    fn select_local_checkpoint(
        &mut self,
        checkpoint: PreparedLocalCellGroupCheckpoint,
    ) -> Result<(), RuntimeError> {
        let root = checkpoint.root();
        if root.marker() != self.current.marker()
            || !Arc::ptr_eq(&root.engine_owner, &self.group.owner)
            || !Arc::ptr_eq(&root.page_owner, &self.current.page_owner)
            || root.marker().position().index.get() <= 1
            || root.marker().position().index.get() >= u64::MAX - 2
            || root.marker().position().term.get() >= u64::MAX - 2
        {
            return Err(RuntimeError::Invalid("local checkpoint owner/cut".into()));
        }
        if root
            .reserved_bytes()
            .saturating_add(checkpoint.snapshot().compute_size() as usize)
            .saturating_add(4096)
            > self.options.max_ready_bytes
        {
            return Err(RuntimeError::Overloaded);
        }
        self.phase = Some(Phase::Checkpoint(checkpoint, false));
        Ok(())
    }

    /// Admit an authenticated snapshot envelope only with its exact complete
    /// receiver-local closure. This returns protocol admission, never a durable
    /// acknowledgement. Snapshot messages are emitted after the Ready fence.
    pub fn step_checkpoint(
        &mut self,
        message: Message,
        checkpoint: PreparedCellGroupCheckpoint,
    ) -> Result<CellGroupSnapshotAdmission, RuntimeError> {
        self.live()?;
        if !self.idle() || self.applying.is_some() || !self.apply.is_empty() {
            return Err(RuntimeError::NotReady);
        }
        self.validate_peer_message(&message, true)?;
        let root = checkpoint.manifest();
        if message.get_msg_type() != MessageType::MsgSnapshot
            || message.get_snapshot() != checkpoint.snapshot()
            || checkpoint.marker().position().index.get() >= u64::MAX - 2
            || checkpoint.marker().position().term.get() >= u64::MAX - 2
            || message.term < self.node.raft.term
            || message.term < checkpoint.marker().position().term.get()
            || root.base.as_ref() != Some(self.current.marker())
            || root.marker().route() != self.current.marker().route()
            || !Arc::ptr_eq(&root.engine_owner, &self.group.owner)
            || !Arc::ptr_eq(&root.page_owner, &self.current.page_owner)
            || !checkpoint.membership().permits_group(self.group.identity())
            || root.marker().position().index < self.current.marker().position().index
            || root.marker().root().cells() > self.options.max_cells_per_group
            || crate::raft::member_state::members(
                checkpoint.snapshot().get_metadata().get_conf_state(),
            )
            .len()
                > self.options.max_members_per_group
        {
            return Err(RuntimeError::Invalid(
                "checkpoint envelope/owner/base".into(),
            ));
        }
        if root
            .reserved_bytes()
            .saturating_add(message.compute_size() as usize)
            .saturating_add(4096)
            > self.options.max_ready_bytes
        {
            return Err(RuntimeError::Overloaded);
        }
        let result = self.node.step(message);
        self.refresh_execution_fence();
        result?;
        if let Some(snapshot) = self.node.snap() {
            if snapshot != checkpoint.snapshot() {
                self.failed = Some(RuntimeError::StaleCompletion);
                self.revoke_execution();
                return Err(RuntimeError::StaleCompletion);
            }
            self.checkpoint = Some(checkpoint);
            Ok(CellGroupSnapshotAdmission::Restoring)
        } else {
            Ok(CellGroupSnapshotAdmission::Ignored)
        }
    }

    pub fn step(&mut self, message: Message) -> Result<(), RuntimeError> {
        self.live()?;
        if self.phase.is_some() || self.checkpoint.is_some() {
            return Err(RuntimeError::NotReady);
        }
        if self.apply_bytes >= self.options.max_group_apply_bytes {
            return Err(RuntimeError::NotReady);
        }
        self.validate_message(&message)?;
        let result = self.node.step(message);
        self.refresh_execution_fence();
        result?;
        Ok(())
    }

    /// Propose a route/revision-bound whole-group configuration. The returned
    /// position is admission only; the persisted applied root proves completion.
    pub fn configure(
        &mut self,
        change: super::CellGroupMembershipChange,
    ) -> Result<CellGroupProposal, RuntimeError> {
        self.live()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        if !self.status()?.ready
            || !self.apply.is_empty()
            || self.applying.is_some()
            || self.node.raft.pending_conf_index > self.node.raft.raft_log.applied
        {
            return Err(RuntimeError::NotReady);
        }
        let before = self.node.raft.raft_log.last_index();
        let entry = Entry {
            entry_type: EntryType::EntryConfChangeV2,
            index: before + 1,
            term: self.node.raft.term,
            data: change
                .change
                .write_to_bytes()
                .map_err(|e| RuntimeError::Invalid(e.to_string()))?
                .into(),
            ..Default::default()
        };
        if entry.data.len() > self.options.max_proposal_bytes {
            return Err(RuntimeError::Overloaded);
        }
        let cache = self.group.read()?;
        let next = super::membership::transition(
            &entry,
            self.current.marker().route(),
            &cache.conf_state,
            &cache.membership,
        )?;
        if crate::raft::member_state::members(&next.conf).len() > self.options.max_members_per_group
        {
            return Err(RuntimeError::Overloaded);
        }
        for item in &change.change.changes {
            if item.get_change_type() == ConfChangeType::AddNode
                && !cache.conf_state.voters.contains(&item.node_id)
                && self
                    .node
                    .raft
                    .prs()
                    .get(item.node_id)
                    .is_none_or(|p| p.matched < cache.hard_state.commit)
            {
                return Err(RuntimeError::NotReady);
            }
        }
        drop(cache);
        let expected = entry.data.clone();
        if let Err(error) = self.node.propose_conf_change(Vec::new(), change.change) {
            let error = RuntimeError::from(error);
            if self.node.raft.raft_log.last_index() != before {
                self.failed = Some(error.clone());
                self.revoke_execution();
            }
            return Err(error);
        }
        let result = (|| {
            let observed = self
                .node
                .raft
                .raft_log
                .unstable_entries()
                .last()
                .ok_or_else(|| RuntimeError::Protocol("missing configuration proposal".into()))?;
            if observed.index != before + 1
                || observed.get_entry_type() != EntryType::EntryConfChangeV2
                || observed.data != expected
            {
                return Err(RuntimeError::Protocol(
                    "configuration proposal changed".into(),
                ));
            }
            Ok(CellGroupProposal {
                group: self.group.identity().scope,
                position: entry_position(observed),
                digest: entry_digest(observed)?,
            })
        })();
        if let Err(error) = &result {
            self.failed = Some(error.clone());
            self.revoke_execution();
        }
        result
    }

    /// Protocol admission only. The shared cell executor must serialize each
    /// cell's preparation, reserve physical work and retain ambiguous outcomes.
    /// The first prepared submission permanently closes this byte-only entry
    /// point for this owner, including already queued raw retries.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<CellGroupProposal, RuntimeError> {
        if self.prepared_only {
            return Err(RuntimeError::Invalid("prepared submission required".into()));
        }
        self.try_propose(data)?.ok_or(RuntimeError::Overloaded)
    }

    /// Seals a definitively refused preparation against every delayed clone.
    /// Queue admission errors never manufacture a rejection proof.
    pub fn submit(
        &mut self,
        prepared: crate::raft::execution::CellPreparedCommand,
    ) -> Result<crate::raft::execution::CellSubmissionOutcome, RuntimeError> {
        let engine = self.group.owner.clone();
        let group = self.group.identity().scope;
        if !prepared.belongs_to(&engine, group) {
            return Err(RuntimeError::Invalid("prepared submission owner".into()));
        }
        self.prepared_only = true;
        prepared.submit(&engine, group, || {
            self.live()?;
            if prepared.term().get() != self.node.raft.term {
                return Err(RuntimeError::StaleCompletion);
            }
            self.try_propose(prepared.bytes().to_vec())
        })
    }

    fn try_propose(&mut self, data: Vec<u8>) -> Result<Option<CellGroupProposal>, RuntimeError> {
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
        let admission = match self.proposals.check(
            &entry,
            &data,
            &self.current,
            self.options.max_cells_per_group,
            self.options.max_requests,
            &self.proposal_budget,
        ) {
            Ok(admission) => admission,
            Err(RuntimeError::Overloaded) => return Ok(None),
            Err(error) => return Err(error),
        };
        let (candidate, permit) = match admission {
            super::proposal_admission::Admission::Existing(accepted) => return Ok(Some(accepted)),
            super::proposal_admission::Admission::New(candidate, permit) => (candidate, permit),
        };
        let before = self.node.raft.raft_log.last_index();
        if let Err(error) = self.node.propose(Vec::new(), data) {
            let error = RuntimeError::from(error);
            if self.node.raft.raft_log.last_index() != before {
                self.failed = Some(error.clone());
                self.revoke_execution();
            }
            return Err(error);
        }
        let observed = (|| {
            let entry = self
                .node
                .raft
                .raft_log
                .unstable_entries()
                .last()
                .ok_or_else(|| RuntimeError::Protocol("missing proposed entry".into()))?;
            if entry.index != before + 1 {
                return Err(RuntimeError::Protocol("proposal append position".into()));
            }
            Ok(CellGroupProposal {
                group: self.group.identity().scope,
                position: entry_position(entry),
                digest: entry_digest(entry)?,
            })
        })();
        let accepted = match observed {
            Ok(accepted) => accepted,
            Err(error) => {
                // Append was attempted. Never turn an uncertain local result
                // into permission to append again or a rejection proof.
                self.failed = Some(error.clone());
                self.revoke_execution();
                return Err(error);
            }
        };
        self.proposals.accept(candidate, accepted.clone(), permit);
        Ok(Some(accepted))
    }

    /// Check the SQL preparation fence on the actual RawNode owner in the same
    /// turn as proposal admission. A sampled status before queueing is not enough.
    pub fn propose_at_term(
        &mut self,
        term: Term,
        data: Vec<u8>,
    ) -> Result<CellGroupProposal, RuntimeError> {
        self.live()?;
        if term.get() != self.node.raft.term {
            return Err(RuntimeError::StaleCompletion);
        }
        self.propose(data)
    }

    pub fn read_index(&mut self, context: Vec<u8>) -> Result<(), RuntimeError> {
        if super::runtime::reads::reserved_context(&context) {
            return Err(RuntimeError::Invalid("reserved cell read context".into()));
        }
        self.read_cell_index(context)
    }

    pub(super) fn read_cell_index(&mut self, context: Vec<u8>) -> Result<(), RuntimeError> {
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
        if self.phase.is_some() || self.checkpoint.is_some() {
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
        self.poll_budget(true, true)
    }

    pub(super) fn poll_budget(
        &mut self,
        collect: bool,
        emit: bool,
    ) -> Result<Option<CellGroupEffect>, RuntimeError> {
        self.live()?;
        let result = self.poll_inner(collect, emit);
        if let Err(error) = &result {
            self.failed = Some(error.clone());
            self.revoke_execution();
        }
        result
    }

    pub(super) fn idle(&self) -> bool {
        self.phase.is_none()
            && self.checkpoint.is_none()
            && self.waiting.is_none()
            && self.effects.is_empty()
            && !self.node.has_ready()
    }

    pub(super) fn waiting(&self) -> bool {
        self.waiting.is_some()
    }

    pub(super) fn control_pending(&self) -> bool {
        self.phase.is_some()
    }

    pub(super) fn has_work(&self) -> bool {
        !self.effects.is_empty()
            || self.phase.is_some()
            || self.node.has_ready()
            || (self.applying.is_none() && !self.apply.is_empty())
    }

    pub(super) fn usage(&self) -> (usize, usize, usize) {
        let entries = |entries: &[Entry]| {
            entries
                .iter()
                .map(|e| e.compute_size() as usize + 128)
                .sum::<usize>()
        };
        let messages = |messages: &[Message]| {
            messages
                .iter()
                .map(|m| m.compute_size() as usize + 128)
                .sum::<usize>()
        };
        let ready = match &self.phase {
            Some(Phase::Ready(ready, _)) => {
                entries(ready.entries())
                    + entries(ready.committed_entries())
                    + messages(ready.messages())
                    + messages(ready.persisted_messages())
                    + ready
                        .read_states()
                        .iter()
                        .map(|r| r.request_ctx.len() + 128)
                        .sum::<usize>()
            }
            Some(Phase::Light(light)) => {
                entries(light.committed_entries()) + messages(light.messages())
            }
            Some(Phase::Apply(_, next, _)) => next.reserved_bytes() + 4096,
            Some(Phase::Checkpoint(checkpoint, _)) => {
                checkpoint.root().reserved_bytes()
                    + checkpoint.snapshot().compute_size() as usize
                    + 4096
            }
            None => 0,
        };
        (
            ready
                + self.checkpoint.as_ref().map_or(0, |c| {
                    c.manifest().reserved_bytes() + c.snapshot().compute_size() as usize
                })
                + self
                    .effects
                    .iter()
                    .map(CellGroupEffect::bytes)
                    .sum::<usize>(),
            self.apply_bytes,
            self.node.raft.read_only.pending_read_count(),
        )
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
            admission: None,
            preparation: None,
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

    fn poll_inner(
        &mut self,
        collect: bool,
        emit: bool,
    ) -> Result<Option<CellGroupEffect>, RuntimeError> {
        loop {
            if self.waiting.is_some() {
                return Ok(if emit { self.effects.pop_front() } else { None });
            }
            match self.phase.take() {
                Some(Phase::Ready(mut ready, term)) => {
                    let installed = if !ready.snapshot().is_empty() {
                        let checkpoint = self
                            .checkpoint
                            .take()
                            .ok_or(RuntimeError::StaleCompletion)?;
                        if checkpoint.snapshot() != ready.snapshot()
                            || self.group.group_manifest()?.as_ref() != Some(checkpoint.marker())
                            || self.group.checkpoint_manifest()?.as_ref()
                                != Some(checkpoint.marker())
                        {
                            return Err(RuntimeError::StaleCompletion);
                        }
                        checkpoint
                            .manifest()
                            .require_checkpoint(&self.group)
                            .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
                        self.current = checkpoint.manifest().clone();
                        Some(self.current.marker().position().index.get())
                    } else {
                        None
                    };
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
                    if let Some(index) = installed {
                        self.node.advance_apply_to(index);
                        self.proposals.clear();
                        if self.effects.len() >= self.options.max_events
                            || self
                                .effects
                                .iter()
                                .map(CellGroupEffect::bytes)
                                .sum::<usize>()
                                .saturating_add(5120)
                                > self.options.max_event_bytes
                        {
                            return Err(RuntimeError::Overloaded);
                        }
                        self.effects
                            .push_back(CellGroupEffect::CheckpointInstalled {
                                manifest: Box::new(self.current.marker().clone()),
                            });
                    }
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
                Some(Phase::Checkpoint(checkpoint, submitted)) => {
                    if !submitted {
                        let mut write = GroupWrite::new(&self.group);
                        write.snapshot =
                            Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
                        self.phase = Some(Phase::Checkpoint(checkpoint, true));
                        return self.persistence(write).map(Some);
                    }
                    let cache = self.group.read()?;
                    if cache.snapshot != *checkpoint.snapshot()
                        || cache.checkpoint_manifest.as_ref() != Some(checkpoint.marker())
                        || cache.group_manifest.as_ref() != Some(self.current.marker())
                    {
                        return Err(RuntimeError::StaleCompletion);
                    }
                    drop(cache);
                    self.current
                        .require_checkpoint(&self.group)
                        .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
                    return Ok(Some(CellGroupEffect::CheckpointPublished {
                        manifest: Box::new(checkpoint.marker().clone()),
                    }));
                }
                Some(Phase::Apply(mut ticket, next, submitted)) => {
                    if !submitted {
                        let mut write = GroupWrite::new(&self.group);
                        write.group_manifest = Some((*next).clone());
                        let entry = self.apply.front().ok_or(RuntimeError::StaleCompletion)?;
                        if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
                            let cache = self.group.read()?;
                            let transition = super::membership::transition(
                                entry,
                                self.current.marker().route(),
                                &cache.conf_state,
                                &cache.membership,
                            )?;
                            write.conf_state = Some(transition.conf);
                            write.membership = Some(transition.membership);
                        }
                        self.phase = Some(Phase::Apply(ticket, next, true));
                        return self.persistence(write).map(Some);
                    }
                    if self.group.group_manifest()?.as_ref() != Some(next.marker()) {
                        return Err(RuntimeError::StaleCompletion);
                    }
                    let entry = self.apply.front().ok_or(RuntimeError::StaleCompletion)?;
                    if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
                        let change =
                            super::membership::decode(entry, self.current.marker().route())?;
                        let conf = self.node.apply_conf_change(&change)?;
                        // RawNode retains its historical insertion order while
                        // pure replay restores the persisted configuration first.
                        // Compare validated sets, including joint/auto-leave
                        // state, rather than hash iteration order after a move.
                        let durable = self.group.initial_state()?.conf_state;
                        if crate::raft::manifest::encode_conf(&conf)
                            .map_err(|e| RuntimeError::Invalid(e.to_string()))?
                            != crate::raft::manifest::encode_conf(&durable)
                                .map_err(|e| RuntimeError::Invalid(e.to_string()))?
                        {
                            return Err(RuntimeError::StaleCompletion);
                        }
                        self.refresh_execution_fence();
                    }
                    let last = self.node.raft.raft_log.last_index();
                    self.node.advance_apply_to(ticket.position.index.get());
                    if self.node.raft.raft_log.last_index() > last {
                        // Raft appended an automatic joint-leave entry. Drive
                        // replication without waiting for a later write/tick;
                        // Ready still fences messages behind its log sync.
                        self.node.raft.bcast_append();
                    }
                    let entry = self
                        .apply
                        .pop_front()
                        .ok_or(RuntimeError::StaleCompletion)?;
                    self.apply_bytes -= entry.compute_size() as usize + 128;
                    self.applying = None;
                    self.current = *next;
                    self.proposals.applied(ticket.position);
                    // Physical preparation and publication have settled. Its
                    // reservation must not deadlock admission of Applied.
                    ticket.admission = None;
                    return Ok(Some(CellGroupEffect::Applied {
                        ticket,
                        manifest: Box::new(self.current.marker().clone()),
                    }));
                }
                None => {
                    // Complete already-persisted protocol phases even when
                    // output admission is full. Otherwise held Apply events
                    // can prevent the completion which releases their quota.
                    if emit {
                        if let Some(event) = self.effects.pop_front() {
                            return Ok(Some(event));
                        }
                    } else {
                        return Ok(None);
                    }
                    if collect
                        && self.node.has_ready()
                        && self.apply_bytes < self.options.max_group_apply_bytes
                    {
                        let ready = self.node.ready();
                        if !ready.snapshot().is_empty()
                            && self
                                .checkpoint
                                .as_ref()
                                .is_none_or(|c| c.snapshot() != ready.snapshot())
                        {
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
                                .sum::<usize>()
                            + ready.snapshot().compute_size() as usize
                            + self
                                .checkpoint
                                .as_ref()
                                .map_or(0, |c| c.manifest().reserved_bytes());
                        if size > self.options.max_ready_bytes {
                            return Err(RuntimeError::Overloaded);
                        }
                        let changed = !ready.entries().is_empty()
                            || ready.hs().is_some()
                            || !ready.snapshot().is_empty();
                        let mut write = GroupWrite::new(&self.group);
                        write.entries = ready.entries().clone();
                        write.hard_state = ready.hs().cloned();
                        if !ready.snapshot().is_empty() {
                            write.snapshot = Some(SnapshotChange::CellGroup(
                                self.checkpoint
                                    .as_ref()
                                    .ok_or(RuntimeError::StaleCompletion)?
                                    .clone(),
                            ));
                        }
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
        if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
            let cache = self.group.read()?;
            super::membership::transition(
                entry,
                self.current.marker().route(),
                &cache.conf_state,
                &cache.membership,
            )?;
            return if next.marker().root() == self.current.marker().root()
                && next.marker().membership_revision() == entry.index
            {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        if entry.get_entry_type() != EntryType::EntryNormal
            || next.marker().membership_revision() != self.current.marker().membership_revision()
        {
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
        let witness = cellar_logic::cell_group::CommitWitness {
            group: self.current.marker().route().history,
            position: entry_position(entry),
            proposal: decoded.binding.proposal,
        };
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
                            || leaf.witness != witness
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
                    || new.witness != witness
                {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }

    pub(super) fn validate_message(&self, message: &Message) -> Result<(), RuntimeError> {
        self.validate_peer_message(message, false)
    }

    fn validate_peer_message(
        &self,
        message: &Message,
        checkpoint: bool,
    ) -> Result<(), RuntimeError> {
        let invalid = || RuntimeError::Invalid("cell-group peer message".into());
        let kind = message.get_msg_type();
        if message.to != self.group.identity().member.get()
            || message.from == message.to
            || message.from == 0
            || self.node.raft.prs().get(message.from).is_none()
            || ::raft::raw_node::is_local_msg(kind)
            || matches!(
                kind,
                MessageType::MsgPropose | MessageType::MsgTransferLeader
            )
            || (!checkpoint
                && (kind == MessageType::MsgSnapshot || !message.get_snapshot().is_empty()))
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
                        || !matches!(
                            entry.get_entry_type(),
                            EntryType::EntryNormal | EntryType::EntryConfChangeV2
                        )
                        || entry.data.len() > self.options.max_proposal_bytes
                    {
                        return Err(invalid());
                    }
                    if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
                        super::membership::decode(entry, self.current.marker().route())?;
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
                    || super::runtime::reads::reserved_context(&entry.data)
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

impl Drop for CellGroupOwner {
    fn drop(&mut self) {
        self.leader_term.store(0, Ordering::Release);
    }
}
