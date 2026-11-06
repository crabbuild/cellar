// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One physical group owns one RawNode. The node scheduler executes the emitted
//! effects; this owner performs no image I/O and starts no per-cell tasks.

use super::{
    hibernate::{
        LocalParkChange, LocalParkRecord, ParkAcknowledgement, ParkPreparation, ParkPrepareRequest,
        MAX_PARK_NEGOTIATION_MS,
    },
    CellGroupBootstrapPermit, GroupManifest, GroupParkCut, ParkCertificate,
    PreparedCellGroupCheckpoint, PreparedGroupManifest, PreparedLocalCellGroupCheckpoint,
};
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
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
    cell_group_placement::WriteHeat,
    consensus::{LogIndex, LogPosition, MemberId, OperationId, ReplicaIdentity, Term},
};
use protobuf::Message as _;
use rand::RngCore as _;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Weak,
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

/// A historical witness that a pending child committed and durably applied
/// a fresh-term barrier. Only its Raft owner can construct this proof.
#[derive(Clone)]
pub struct TransferChildQuorum {
    marker: GroupManifest,
    leader: super::InstalledTransferVoter,
}

impl TransferChildQuorum {
    pub(crate) fn into_parts(self) -> (GroupManifest, super::InstalledTransferVoter) {
        (self.marker, self.leader)
    }
}

/// Admission is distinct from a seal committed and selected by this owner.
pub enum CellGroupSealOutcome {
    Accepted(CellGroupProposal),
    Applied(super::GroupSourceSeal),
}

/// The exact retirement is either proposed or durably applied.
pub enum CellGroupSourceRetirement {
    Accepted(CellGroupProposal),
    Applied(Box<super::CellGroupSourceRetirementAck>),
}

/// Admission is distinct from activation committed and selected by this owner.
pub enum CellGroupRecoveryActivation {
    Accepted(CellGroupProposal),
    Applied(Box<super::CellGroupRecoveryActivationAck>),
}

/// An admitted activation proposal or its durable applied position.
pub enum CellGroupTransferActivation {
    Accepted(CellGroupProposal),
    Applied(Box<super::CellGroupTransferActivationAck>),
}

enum Phase {
    Ready(Box<Ready>, Term),
    Light(LightReady),
    Apply(CellGroupTicket, Box<PreparedGroupManifest>, bool),
    Checkpoint(
        PreparedLocalCellGroupCheckpoint,
        bool,
        Arc<super::archive_compaction::ArchiveCompactionAdmission>,
    ),
}

struct ConfigurationFence {
    index: u64,
    announced: bool,
}

/// Local evidence that the leader synced its exact certificate selection.
pub(crate) struct SelectedParkCertificate {
    certificate: Box<ParkCertificate>,
    sync_sequence: u64,
}

impl SelectedParkCertificate {
    pub(crate) fn certificate(&self) -> &ParkCertificate {
        &self.certificate
    }

    pub(crate) fn sync_sequence(&self) -> u64 {
        self.sync_sequence
    }
}

struct PendingPark {
    request: ParkPrepareRequest,
    preparation: ParkPreparation,
    abort: Option<LogPosition>,
}

struct PendingApply {
    entry: Entry,
    queued_at_ms: u64,
}

pub struct CellGroupOwner {
    log_admission: super::log_admission::LogAdmission,
    leader_term: Arc<AtomicU64>,
    source_epoch: Arc<AtomicU64>,
    source_identity: (u64, u64),
    source_observed: Option<(u64, u64, u64)>,
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
    deliveries: Vec<Weak<crate::raft::budget::Permit>>,
    configuration_fence: Option<ConfigurationFence>,
    apply: VecDeque<PendingApply>,
    workload_owner: [u8; 16],
    admitted_writes: u64,
    applied_writes: u64,
    write_heat: WriteHeat,
    apply_bytes: usize,
    applying: Option<CellGroupTicket>,
    next_ticket: u64,
    last_sync: u64,
    failed: Option<RuntimeError>,
    proposals: super::proposal_admission::ProposalAdmission,
    pub(super) proposal_budget: Arc<crate::raft::budget::Budget>,
    prepared_only: bool,
    activation_proposal: Option<Box<(Term, CellGroupProposal)>>,
    retirement_proposal: Option<Box<(Term, CellGroupProposal)>>,
    pending_seal: Option<Box<(Term, [u8; 32], CellGroupProposal)>>,
    pending_park: Option<Box<PendingPark>>,
    parked: Option<ParkGeneration>,
    waking_after: Option<LogIndex>,
}

impl CellGroupOwner {
    /// Attach a durably claimed member with a completely verified selected
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
        Box::pin(permit.revalidate())
            .await
            .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
        Box::pin(permit.log().close_retained_park(&group)).await?;
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
        let waking_after = cache
            .local_park
            .as_ref()
            .and_then(LocalParkRecord::wake_floor)
            .filter(|floor| {
                current.marker().position().index <= *floor
                    || current.marker().position().term.get() != cache.hard_state.term
            });
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
        let mut workload_owner = [0; 16];
        crate::asyncrt::rng("cell-group-workload-owner").fill_bytes(&mut workload_owner);
        Ok(Self {
            log_admission: Default::default(),
            leader_term: Arc::new(AtomicU64::new(0)),
            source_epoch: Arc::new(AtomicU64::new(1)),
            source_identity: (0, 0),
            source_observed: None,
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
            deliveries: Vec::new(),
            configuration_fence: None,
            apply: VecDeque::new(),
            workload_owner,
            admitted_writes: 0,
            applied_writes: 0,
            write_heat: WriteHeat::default(),
            apply_bytes: 0,
            applying: None,
            next_ticket: 0,
            last_sync: 0,
            failed: None,
            proposals: Default::default(),
            prepared_only: false,
            activation_proposal: None,
            retirement_proposal: None,
            pending_seal: None,
            pending_park: None,
            parked: None,
            waking_after,
        })
    }

    fn finish_log_admission(
        &mut self,
        next: super::log_admission::Prospective,
    ) -> Result<(), RuntimeError> {
        if let Err(error) = self.log_admission.finish(&self.group, &self.node, next) {
            self.failed = Some(error.clone());
            self.revoke_execution();
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn revoke_execution(&mut self) {
        self.leader_term.store(0, Ordering::Release);
        self.source_epoch.store(0, Ordering::Release);
        self.proposals.clear();
        self.activation_proposal = None;
        self.retirement_proposal = None;
        self.pending_seal = None;
        self.pending_park = None;
    }

    fn advance_source_epoch(&self) {
        if self
            .source_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                (epoch != 0).then(|| epoch.checked_add(1)).flatten()
            })
            .is_err()
        {
            // Exhaustion or revocation never rearms an old read capability.
            self.source_epoch.store(0, Ordering::Release);
        }
    }

    fn refresh_execution_fence(&mut self) {
        let source = (self.node.raft.term, self.node.raft.leader_id);
        if source != self.source_identity {
            self.source_identity = source;
            self.source_observed = None;
            self.advance_source_epoch();
        }
        if self.pending_seal.as_ref().is_some_and(|pending| {
            pending.0.get() != self.node.raft.term || self.node.raft.state != StateRole::Leader
        }) {
            self.pending_seal = None;
        }
        if self.pending_park.as_ref().is_some_and(|pending| {
            pending.request.term.get() != self.node.raft.term
                || self.node.raft.state != StateRole::Leader
        }) {
            self.pending_park = None;
        }
        let at = self.current.marker().position();
        if self
            .waking_after
            .is_some_and(|floor| at.index > floor && at.term.get() == self.node.raft.term)
        {
            self.waking_after = None;
        }
        let term = if !self.park_fenced()
            && self.pending_seal.is_none()
            && self.current.marker().source_seal().is_none()
            && self.node.raft.state == StateRole::Leader
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
            self.activation_proposal = None;
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

    pub(crate) fn next_effect(
        &self,
        now: u64,
        after: Option<&CellId>,
    ) -> Result<Option<super::CellExecutionAuthority>, RuntimeError> {
        let status = self.status()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        if !status.ready || status.applied.index != status.committed {
            return Err(RuntimeError::NotReady);
        }
        let Some((leaf, _pin)) = self.current.next_due(now, after) else {
            return Ok(None);
        };
        self.execution_authority(&leaf.version.history).map(Some)
    }

    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.current
    }

    pub(super) fn require_application_active(&self) -> Result<(), RuntimeError> {
        self.live()?;
        if self.park_fenced() || self.pending_seal.is_some() || !self.current.marker().is_active() {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }

    pub(super) fn capture_checkpoint(
        &self,
        pool: &super::CellGroupCheckpointPool,
    ) -> Result<super::CellGroupCheckpoint, super::CellGroupCheckpointError> {
        self.live()
            .map_err(super::CellGroupCheckpointError::Authority)?;
        pool.capture_owner(&self.group, &self.current)
    }

    pub(super) fn select_read(
        &self,
        cell: &CellHistory,
        consistency: &super::CellReadConsistency,
        term: Option<Term>,
        barrier: Option<LogIndex>,
    ) -> Result<super::read_authority::ReadSelection, RuntimeError> {
        self.require_application_active()?;
        let status = self.status()?;
        let mut source = None;
        let mut source_deadline = None;
        let leader = if matches!(consistency, super::CellReadConsistency::Linearizable) {
            if term.is_none() {
                return Err(RuntimeError::NotReady);
            }
            if Some(status.term) != term {
                return Err(RuntimeError::NotLeader);
            }
            let index = barrier.ok_or(RuntimeError::NotReady)?;
            if status.applied.index < index {
                return Err(RuntimeError::NotReady);
            }
            if self.node.raft.state == StateRole::Leader {
                if !status.ready {
                    return Err(RuntimeError::NotReady);
                }
                Some((status.term, self.leader_term.clone()))
            } else if self.node.raft.state == StateRole::Follower && self.membership_read_ready()? {
                source = Some(self.read_source_epoch(status.term)?);
                None
            } else {
                return Err(RuntimeError::NotLeader);
            }
        } else {
            None
        };
        if let super::CellReadConsistency::RecentSource { max_age_ms } = consistency {
            if !(1..=3_600_000).contains(max_age_ms) {
                return Err(RuntimeError::Invalid("stale source age".into()));
            }
            let now = self
                .log
                .domain()
                .mono_ms()
                .map_err(|_| RuntimeError::Closed)?;
            let observed = if self.node.raft.state == StateRole::Leader
                && self.current.marker().position().term == status.term
            {
                Some(now)
            } else {
                self.source_observed
                    .filter(|(member, term, _)| {
                        *member == self.node.raft.leader_id && *term == status.term.get()
                    })
                    .map(|(_, _, at)| at)
            };
            source_deadline = Some(
                crate::raft::query::recent_source_deadline(observed, now, *max_age_ms)
                    .ok_or(RuntimeError::NotReady)?,
            );
            source = Some(self.read_source_epoch(status.term)?);
        }
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
            source,
            source_deadline,
        })
    }

    fn read_source_epoch(&self, term: Term) -> Result<(Term, u64, Arc<AtomicU64>), RuntimeError> {
        let epoch = self.source_epoch.load(Ordering::Acquire);
        if epoch == 0 || self.group.read()?.hard_state.term != term.get() {
            return Err(RuntimeError::NotReady);
        }
        Ok((term, epoch, self.source_epoch.clone()))
    }

    pub(super) fn check_seed_target(
        &self,
        target: &ReplicaIdentity<GroupHistory>,
    ) -> Result<(), RuntimeError> {
        self.live()?;
        super::seed_authority::check_target(&self.group, target).map(|_| ())
    }

    pub(super) fn select_membership(
        &self,
        minimum: LogIndex,
        term: Option<Term>,
        barrier: Option<LogIndex>,
    ) -> Result<super::membership_observation::MembershipSelection, RuntimeError> {
        self.require_application_active()?;
        let status = self.status()?;
        if Some(status.term) != term {
            return Err(RuntimeError::NotLeader);
        }
        let index = barrier.ok_or(RuntimeError::NotReady)?;
        if !self.membership_read_ready()?
            || status.applied.index < index
            || status.applied.index < minimum
        {
            return Err(RuntimeError::NotReady);
        }
        let cache = self.group.read()?;
        let root = self.current.marker();
        if cache.group_manifest.as_ref() != Some(root)
            || root.membership_revision() != cache.membership.revision().get()
            || cache.membership.revision() > index
        {
            return Err(RuntimeError::NotReady);
        }
        cache
            .membership
            .validate(&cache.conf_state, root.position().index.get())?;
        cache.membership.validate_scope(&root.route().history)?;
        let bytes = cache
            .membership
            .reserved_bytes()
            .checked_add(cache.conf_state.compute_size() as usize)
            .and_then(|n| n.checked_add(serde_json::to_vec(root).ok()?.len()))
            .and_then(|n| n.checked_add(4096))
            .ok_or(RuntimeError::Overloaded)?;
        if bytes > super::membership_observation::MEMBERSHIP_BYTES {
            return Err(RuntimeError::Overloaded);
        }
        Ok(super::membership_observation::MembershipSelection {
            replica: self.group.identity().clone(),
            engine: self.log.storage_instance()?,
            root: root.clone(),
            barrier: LogPosition {
                term: status.term,
                index,
            },
            membership: cache.membership.clone(),
            configuration: cache.conf_state.clone(),
            status,
        })
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

    /// Count even a leader awaiting its current-term apply barrier. Headroom
    /// must never mistake a non-serving leader for an unused leadership slot.
    pub(super) fn local_leader(&self) -> Result<bool, RuntimeError> {
        self.live()?;
        Ok(self.node.raft.state == StateRole::Leader)
    }

    pub fn status(&self) -> Result<CellGroupStatus, RuntimeError> {
        self.live()?;
        let cache = self.group.read()?;
        Ok(self.status_at(&cache))
    }

    fn status_at(&self, cache: &crate::raft::cache::CachedGroup) -> CellGroupStatus {
        CellGroupStatus {
            term: Term::new(self.node.raft.term),
            leader: (self.node.raft.leader_id != 0).then_some(self.node.raft.leader_id),
            applied: self.current.marker().position(),
            committed: LogIndex::new(cache.hard_state.commit),
            ready: !self.park_fenced()
                && self.pending_seal.is_none()
                && self.current.marker().is_active()
                && self.protocol_ready(cache),
        }
    }

    pub(super) fn observe(&self) -> Result<super::CellGroupObservation, RuntimeError> {
        use crate::application::consensus::RaftReplicaRole;
        self.live()?;
        let cache = self.group.read()?;
        let replica = self.group.identity().clone();
        let member = replica.member.get();
        let role = if cache.conf_state.voters.contains(&member)
            || cache.conf_state.voters_outgoing.contains(&member)
        {
            if self.node.raft.state == StateRole::Leader {
                RaftReplicaRole::Leader
            } else {
                RaftReplicaRole::Voter
            }
        } else if cache.conf_state.learners.contains(&member)
            || cache.conf_state.learners_next.contains(&member)
        {
            RaftReplicaRole::Learner
        } else {
            RaftReplicaRole::Removed
        };
        let checkpoint = cache.snapshot.get_metadata();
        Ok(super::CellGroupObservation {
            replica,
            status: self.status_at(&cache),
            role,
            membership_revision: cache.membership.revision(),
            persisted: LogIndex::new(cache.last_index()),
            checkpoint: LogPosition {
                term: Term::new(checkpoint.term),
                index: LogIndex::new(checkpoint.index),
            },
            retained_entries: cache.entries.len(),
            cells: self.current.marker().root().cells(),
            archive_required: cache.archive_required,
        })
    }

    pub(super) fn observe_partition(
        &self,
    ) -> Result<super::CellGroupPartitionObservation, RuntimeError> {
        self.live()?;
        let cache = self.group.read()?;
        let marker = self.current.marker();
        if cache.group_manifest.as_ref() != Some(marker) {
            return Err(RuntimeError::NotReady);
        }
        let pins = &self.current.images;
        let sampled_at_ms = self
            .log
            .domain()
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?;
        let cells = marker.root().cells();
        let geometry = |(rank, logical_bytes)| {
            Some(super::CellGroupPartitionCell {
                key: pins.key_at_rank(rank)?.clone(),
                next: pins.key_at_rank(rank + 1).cloned(),
                has_predecessor: rank > 0,
                logical_bytes,
            })
        };
        let membership = MembershipRevision::new(cache.membership.revision().get())
            .map_err(|_| RuntimeError::NotReady)?;
        let status = self.status_at(&cache);
        let hot_writer = self
            .write_heat
            .observed(status.term, membership, sampled_at_ms)
            .and_then(|(started_at_ms, cell, admitted_writes)| {
                let key = crate::application::consensus::cell_routing_key(
                    marker.route().history.namespace,
                    cell,
                );
                let cell = geometry(pins.image_rank(&key)?)?;
                Some(super::CellGroupPartitionWriter {
                    cell,
                    started_at_ms,
                    admitted_writes,
                })
            });
        Ok(super::CellGroupPartitionObservation {
            route: marker.route().clone(),
            membership,
            status,
            stable_leader: marker.is_active()
                && !self.park_fenced()
                && self.pending_seal.is_none()
                && self.configuration_fence.is_none()
                && cache.conf_state.voters_outgoing.is_empty()
                && self.node.raft.pending_conf_index <= marker.position().index.get()
                && self.selected_leader(&cache),
            workload: super::CellGroupWorkload {
                owner: self.workload_owner,
                sampled_at_ms,
                admitted_writes: self.admitted_writes,
                applied_writes: self.applied_writes,
                queue_age_ms: self.apply.front().map_or(0, |pending| {
                    sampled_at_ms.saturating_sub(pending.queued_at_ms)
                }),
            },
            cells,
            logical_bytes: pins.logical_bytes(),
            last_mutation: marker.root().last_mutation(),
            balanced_pivot: if cells > 1 {
                pins.key_at_rank(cells as usize / 2).cloned()
            } else {
                None
            },
            largest_cell: pins.largest_image_rank().and_then(geometry),
            hot_writer,
            archive_lag_entries: None,
        })
    }

    /// Observe the real leader's progress tracker. This is advisory; transfer
    /// admission rechecks the same owner against the caller's term and revision.
    pub fn replication_status(
        &self,
    ) -> Result<crate::raft::completion::ReplicationStatus<GroupHistory>, RuntimeError> {
        use crate::raft::completion::{MemberProgress, ReplicationStatus};
        self.live()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let cache = self.group.read()?;
        let mut members = self
            .node
            .raft
            .prs()
            .iter()
            .map(|(&id, progress)| {
                Ok(MemberProgress {
                    member: MemberId::new(id).map_err(|e| RuntimeError::Protocol(e.to_string()))?,
                    matched: LogIndex::new(progress.matched),
                    voter: cache.conf_state.voters.contains(&id)
                        || cache.conf_state.voters_outgoing.contains(&id),
                    recent_active: progress.recent_active,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        members.sort_by_key(|member| member.member);
        Ok(ReplicationStatus {
            replica: self.group.identity().clone(),
            term: Term::new(self.node.raft.term),
            membership_revision: cache.membership.revision(),
            committed: LogIndex::new(self.node.raft.raft_log.committed),
            members,
        })
    }

    fn protocol_ready(&self, cache: &crate::raft::cache::CachedGroup) -> bool {
        self.phase.is_none()
            && self.checkpoint.is_none()
            && self.apply_bytes < self.options.max_group_apply_bytes
            && self.selected_leader(cache)
    }

    fn selected_leader(&self, cache: &crate::raft::cache::CachedGroup) -> bool {
        self.node.raft.state == StateRole::Leader
            && self.node.raft.lead_transferee.is_none()
            && self
                .node
                .raft
                .prs()
                .conf()
                .voters()
                .contains(self.node.raft.id)
            && self.current.marker().position().term.get() == self.node.raft.term
            && cache.hard_state.term == self.node.raft.term
            && cache.group_manifest.as_ref() == Some(self.current.marker())
    }

    pub(super) fn pending_transfer_quorum(&self) -> Result<TransferChildQuorum, RuntimeError> {
        self.live()?;
        let marker = self.current.marker();
        let cache = self.group.read()?;
        if marker.transfer_origin().is_none()
            || marker.transfer_activation().is_some()
            || marker.membership_revision() != 1
            || cache.membership.revision().get() != 1
            || !cache.conf_state.voters_outgoing.is_empty()
            || !cache.conf_state.learners_next.is_empty()
            || cache.conf_state.auto_leave
        {
            return Err(RuntimeError::NotReady);
        }
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let position = marker.position();
        if !self.protocol_ready(&cache)
            || position.term.get() <= 1
            || position.index.get() <= 1
            || position.index.get() != cache.hard_state.commit
            || !self.apply.is_empty()
            || self.applying.is_some()
        {
            return Err(RuntimeError::NotReady);
        }
        let local = self.group.identity();
        Ok(TransferChildQuorum {
            marker: marker.clone(),
            leader: super::InstalledTransferVoter {
                member: local.member,
                node: local.node,
                disk: local.disk,
                engine: self.log.storage_instance()?,
            },
        })
    }

    /// Admit only the selected recovery's exact activation. Retries in the same
    /// term share one bounded proposal; a new leader must apply its fresh-term
    /// barrier before it can decide whether an inherited activation survived.
    pub fn activate_recovery(
        &mut self,
        permit: super::CellGroupRecoveryActivationPermit,
    ) -> Result<CellGroupRecoveryActivation, RuntimeError> {
        self.live()?;
        permit.check(&self.log, &self.group)?;
        if permit.matches(self.current.marker())? {
            self.activation_proposal = None;
            return Ok(CellGroupRecoveryActivation::Applied(Box::new(
                permit.acknowledgement(&self.group, self.current.marker())?,
            )));
        }
        self.propose_activation(|| permit.entry())
            .map(CellGroupRecoveryActivation::Accepted)
    }

    pub fn activate_transfer(
        &mut self,
        permit: super::CellGroupTransferActivationPermit,
    ) -> Result<CellGroupTransferActivation, RuntimeError> {
        self.live()?;
        permit.check(&self.log, &self.group)?;
        let marker = self.current.marker();
        if let Some(acknowledgement) = permit.acknowledge(&self.group, marker)? {
            self.activation_proposal = None;
            return Ok(CellGroupTransferActivation::Applied(Box::new(
                acknowledgement,
            )));
        }
        self.propose_activation(|| Ok(permit.into_entry()))
            .map(CellGroupTransferActivation::Accepted)
    }

    fn propose_activation(
        &mut self,
        entry: impl FnOnce() -> Result<Vec<u8>, RuntimeError>,
    ) -> Result<CellGroupProposal, RuntimeError> {
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        if let Some(pending) = &self.activation_proposal {
            if pending.0.get() == self.node.raft.term {
                return Ok(pending.1.clone());
            }
        }
        self.require_control_prefix()?;
        let proposal = self.append_control(entry()?, "group activation")?;
        self.activation_proposal =
            Some(Box::new((Term::new(self.node.raft.term), proposal.clone())));
        Ok(proposal)
    }

    pub fn retire_source(
        &mut self,
        permit: super::CellGroupSourceRetirementPermit,
    ) -> Result<CellGroupSourceRetirement, RuntimeError> {
        self.live()?;
        permit.check(&self.log, &self.group)?;
        let marker = self.current.marker();
        if let Some(retirement) = permit.acknowledge(&self.group, marker)? {
            self.retirement_proposal = None;
            return Ok(CellGroupSourceRetirement::Applied(Box::new(retirement)));
        }
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let term = Term::new(self.node.raft.term);
        if let Some(pending) = &self.retirement_proposal {
            if pending.0 == term {
                return Ok(CellGroupSourceRetirement::Accepted(pending.1.clone()));
            }
        }
        self.require_control_prefix()?;
        let proposal = self.append_control(permit.into_entry(), "source retirement")?;
        self.retirement_proposal = Some(Box::new((term, proposal.clone())));
        Ok(CellGroupSourceRetirement::Accepted(proposal))
    }

    fn require_control_prefix(&self) -> Result<(), RuntimeError> {
        let cache = self.group.read()?;
        if self.park_fenced()
            || !self.protocol_ready(&cache)
            || self.current.marker().position().index.get() != cache.hard_state.commit
            || !self.apply.is_empty()
            || self.applying.is_some()
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }

    /// Drain the selected prefix before appending the irreversible source cut.
    /// Only the rooted transition lane can supply a permit for this entry.
    pub fn seal_source(
        &mut self,
        permit: super::CellGroupSealPermit,
    ) -> Result<CellGroupSealOutcome, RuntimeError> {
        self.live()?;
        permit.check(&self.log, &self.group)?;
        let marker = self.current.marker();
        if let Some(seal) = marker.source_seal() {
            let cache = self.group.read()?;
            if !permit.matches(seal)
                || cache.group_manifest.as_ref() != Some(marker)
                || cache.hard_state.commit < seal.position().index.get()
            {
                return Err(RuntimeError::StaleCompletion);
            }
            return Ok(CellGroupSealOutcome::Applied(seal.clone()));
        }
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let term = Term::new(self.node.raft.term);
        let payload = permit.payload();
        if let Some(pending) = &self.pending_seal {
            if pending.0 == term && pending.1 == payload {
                return Ok(CellGroupSealOutcome::Accepted(pending.2.clone()));
            }
            return Err(RuntimeError::StaleCompletion);
        }
        let cache = self.group.read()?;
        let index = marker.position().index.get();
        if self.park_fenced()
            || !marker.is_active()
            || !self.protocol_ready(&cache)
            || index != cache.hard_state.commit
            || index != self.node.raft.raft_log.last_index()
            || !cache.conf_state.voters_outgoing.is_empty()
            || !cache.conf_state.learners_next.is_empty()
            || !self.apply.is_empty()
            || self.applying.is_some()
        {
            return Err(RuntimeError::NotReady);
        }
        drop(cache);
        let proposal = self.append_control(permit.into_entry(), "source seal")?;
        self.pending_seal = Some(Box::new((term, payload, proposal.clone())));
        self.leader_term.store(0, Ordering::Release);
        self.advance_source_epoch();
        Ok(CellGroupSealOutcome::Accepted(proposal))
    }

    pub(super) fn prepare_park(
        &mut self,
        request: ParkPrepareRequest,
    ) -> Result<ParkPreparation, RuntimeError> {
        self.live()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let now = self
            .log
            .domain()
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?;
        if request.deadline_ms <= now {
            return Err(RuntimeError::DeadlineExceeded);
        }
        let remaining = request.deadline_ms - now;
        if remaining > MAX_PARK_NEGOTIATION_MS {
            return Err(RuntimeError::Invalid("park negotiation deadline".into()));
        }
        if let Some(pending) = &self.pending_park {
            let same = ParkPrepareRequest {
                deadline_ms: request.deadline_ms,
                ..pending.request
            };
            return if same == request
                && pending.abort.is_none()
                && now < pending.request.deadline_ms
            {
                Ok(pending.preparation.clone())
            } else {
                Err(RuntimeError::NotReady)
            };
        }
        let cache = self.group.read()?;
        let marker = self.current.marker();
        let at = marker.position();
        let conf = &cache.conf_state;
        let next_due_unix_millis = marker.root().next_due_unix_millis();
        let wall = u64::try_from(crate::asyncrt::wall_ms()).map_err(|_| RuntimeError::NotReady)?;
        if request.term.get() != self.node.raft.term
            || request.membership.get() != cache.membership.revision().get()
            || request.expected_applied != at
        {
            return Err(RuntimeError::StaleCompletion);
        }
        if !self.idle()
            || !self.status_at(&cache).ready
            || cache.hard_state.commit != at.index.get()
            || self.node.raft.raft_log.last_index() != at.index.get()
            || self.node.raft.pending_conf_index > at.index.get()
            || self.configuration_fence.is_some()
            || !conf.voters_outgoing.is_empty()
            || !conf.learners.is_empty()
            || !conf.learners_next.is_empty()
            || conf.auto_leave
            || self.applying.is_some()
            || !self.apply.is_empty()
            || self.node.raft.read_only.pending_read_count() != 0
            // Execution, enrollment and claimed-effect handles pin this fence.
            || Arc::strong_count(&self.leader_term) != 1
            || next_due_unix_millis.is_some_and(|due| due <= wall.saturating_add(remaining))
        {
            return Err(RuntimeError::NotReady);
        }
        let route = marker.route();
        let index = at
            .index
            .get()
            .checked_add(1)
            .ok_or(RuntimeError::Overloaded)?;
        let entry = CellGroupEntry {
            binding: GroupEntryBinding {
                group: route.history,
                route_epoch: route.epoch,
                birth_digest: route.birth_digest,
                schema_digest: route.schema_digest,
                proposal: request.proposal,
            },
            command: GroupCommand::PreparePark {
                generation: ParkGeneration::new(index)
                    .map_err(|error| RuntimeError::Invalid(error.to_string()))?,
                term: request.term,
                membership: request.membership,
            },
        }
        .encode(route, GroupEntryLimits::default())
        .map_err(|error| RuntimeError::Invalid(error.to_string()))?;
        drop(cache);
        let proposal = self.append_control(entry, "park preparation")?;
        let preparation = ParkPreparation {
            proposal,
            next_due_unix_millis,
            deadline_ms: request.deadline_ms,
        };
        self.pending_park = Some(Box::new(PendingPark {
            request,
            preparation: preparation.clone(),
            abort: None,
        }));
        // A same-term abort can mint fresh capabilities. Any old handle must
        // remain fenced even if the leader keeps its term.
        self.leader_term.store(0, Ordering::Release);
        self.leader_term = Arc::new(AtomicU64::new(0));
        self.source_observed = None;
        self.advance_source_epoch();
        Ok(preparation)
    }

    fn expire_park_preparation(&mut self) -> Result<(), RuntimeError> {
        let Some(pending) = self.pending_park.as_ref() else {
            return Ok(());
        };
        let now = self
            .log
            .domain()
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?;
        if pending.abort.is_none() && now >= pending.request.deadline_ms {
            // Advance the log before re-enabling admission. Persistence closes
            // local acknowledgements, and apply proves fresh quorum progress.
            match self.append_control(Vec::new(), "park preparation timeout") {
                Ok(proposal) => {
                    if let Some(pending) = self.pending_park.as_mut() {
                        pending.abort = Some(proposal.position);
                    }
                }
                // A full log must still tick and let check-quorum run.
                Err(RuntimeError::Overloaded) => (),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn append_control(
        &mut self,
        data: Vec<u8>,
        kind: &str,
    ) -> Result<CellGroupProposal, RuntimeError> {
        if data.len() > self.options.max_proposal_bytes {
            return Err(RuntimeError::Overloaded);
        }
        let before = self.node.raft.raft_log.last_index();
        let expected = Entry {
            index: before + 1,
            term: self.node.raft.term,
            data: data.clone().into(),
            ..Default::default()
        };
        let admission =
            self.log_admission
                .prepare(&self.group, std::slice::from_ref(&expected), None)?;
        let result = self.node.propose(Vec::new(), data);
        self.finish_log_admission(admission)?;
        if let Err(error) = result {
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
                .ok_or_else(|| RuntimeError::Protocol(format!("missing {kind}")))?;
            if entry != &expected {
                return Err(RuntimeError::Protocol(format!("changed {kind}")));
            }
            Ok(CellGroupProposal {
                group: self.group.identity().scope,
                position: entry_position(entry),
                digest: entry_digest(entry)?,
            })
        })();
        let proposal = match observed {
            Ok(proposal) => proposal,
            Err(error) => {
                self.failed = Some(error.clone());
                self.revoke_execution();
                return Err(error);
            }
        };
        Ok(proposal)
    }

    pub fn campaign(&mut self) -> Result<(), RuntimeError> {
        self.live()?;
        if self.pending_park.is_some()
            || self.parked.is_some()
            || self.phase.is_some()
            || self.checkpoint.is_some()
        {
            return Err(RuntimeError::NotReady);
        }
        let admission = self
            .log_admission
            .election(&self.group, &self.node, Default::default())?;
        let result = self.node.campaign();
        self.refresh_execution_fence();
        self.finish_log_admission(admission)?;
        result?;
        Ok(())
    }

    /// Admit a handoff to a caught-up incoming voter. Completion is observed
    /// through leadership status; admission alone cannot prove an election.
    pub fn transfer_leader(&mut self, member: u64) -> Result<(), RuntimeError> {
        let progress = self.replication_status()?;
        self.transfer_leadership(crate::raft::completion::LeadershipTransfer {
            target: MemberId::new(member).map_err(|e| RuntimeError::Invalid(e.to_string()))?,
            expected_term: progress.term,
            expected_revision: progress.membership_revision,
        })
    }

    /// Recheck whole-group matched progress immediately before restart
    /// handoff/observation. The runtime separately checks the synced stop gate.
    pub(crate) fn restart_leadership(
        &mut self,
        check: crate::raft::completion::RestartLeadershipCheck,
    ) -> Result<(), RuntimeError> {
        let progress = self.replication_status()?;
        let cache = self.group.read()?;
        let transfer = crate::raft::restart_leadership::check_progress(
            &progress,
            &cache.conf_state,
            &check,
            self.node.raft.pending_conf_index > self.node.raft.raft_log.applied,
            self.node.raft.lead_transferee.is_some(),
        )?;
        drop(cache);
        if let Some(transfer) = transfer {
            self.transfer_leadership(transfer)?;
        }
        Ok(())
    }

    pub fn transfer_leadership(
        &mut self,
        transfer: crate::raft::completion::LeadershipTransfer,
    ) -> Result<(), RuntimeError> {
        self.live()?;
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let status = self.status()?;
        let cache = self.group.read()?;
        let member = transfer.target.get();
        if transfer.expected_term != status.term
            || transfer.expected_revision != cache.membership.revision()
            || member == self.node.raft.id
            || !cache.conf_state.voters.contains(&member)
        {
            return Err(RuntimeError::Invalid(
                "leadership target is not another incoming voter".into(),
            ));
        }
        if !status.ready
            || !cache.conf_state.voters_outgoing.is_empty()
            || self.node.raft.lead_transferee.is_some()
            || cache.membership.repair_status()?.is_some_and(|repair| {
                matches!(
                    repair.phase,
                    crate::raft::member_state::RepairPhase::Seeding
                        | crate::raft::member_state::RepairPhase::Joint
                )
            })
            || status.applied.index != status.committed
            || !self.apply.is_empty()
            || self.applying.is_some()
            || self.node.raft.pending_conf_index > self.node.raft.raft_log.applied
            || self.node.raft.prs().get(member).is_none_or(|p| {
                !p.recent_active || p.matched < self.node.raft.raft_log.last_index()
            })
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
        if self.parked.is_some() {
            return Ok(());
        }
        // The scheduler coalesces a deferred tick until pending Ready work is
        // drained. In particular, an arbitrarily slow fsync must not accumulate
        // an unbounded vector of heartbeat/election messages inside RawNode.
        if self.phase.is_some() || self.node.has_ready() {
            return Err(RuntimeError::NotReady);
        }
        self.expire_park_preparation()?;
        let admission = if self.node.raft.state == StateRole::Leader {
            Default::default()
        } else {
            self.log_admission
                .election(&self.group, &self.node, Default::default())?
        };
        self.node.tick();
        self.refresh_execution_fence();
        self.finish_log_admission(admission)?;
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
        let admission = self
            .group
            .archive_compaction
            .admit(root.marker())?
            .ok_or(RuntimeError::Overloaded)?;
        self.phase = Some(Phase::Checkpoint(checkpoint, false, admission));
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
        self.finish_log_admission(Default::default())?;
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
        let entries = if message.get_msg_type() == MessageType::MsgAppend {
            message.entries.as_slice()
        } else {
            &[]
        };
        let mut admission = self.log_admission.prepare(&self.group, entries, None)?;
        if self.node.raft.state != StateRole::Leader
            && matches!(
                message.get_msg_type(),
                MessageType::MsgRequestVoteResponse
                    | MessageType::MsgRequestPreVoteResponse
                    | MessageType::MsgTimeoutNow
            )
        {
            admission = self
                .log_admission
                .election(&self.group, &self.node, admission)?;
        }
        let (from, term, kind) = (message.from, message.term, message.get_msg_type());
        let result = self.node.step(message);
        self.refresh_execution_fence();
        self.finish_log_admission(admission)?;
        result?;
        if matches!(kind, MessageType::MsgAppend | MessageType::MsgHeartbeat)
            && from == self.node.raft.leader_id
            && term == self.node.raft.term
        {
            self.source_observed = Some((
                from,
                term,
                self.log
                    .domain()
                    .mono_ms()
                    .map_err(|_| RuntimeError::Closed)?,
            ));
        }
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
        let admission =
            self.log_admission
                .prepare(&self.group, std::slice::from_ref(&entry), None)?;
        let admission = self.log_admission.configuration(
            &self.group,
            admission,
            change.change.enter_joint() == Some(true),
        )?;
        let result = self.node.propose_conf_change(Vec::new(), change.change);
        self.finish_log_admission(admission)?;
        if let Err(error) = result {
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
        self.try_propose(data, None)?
            .ok_or(RuntimeError::Overloaded)
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
            self.try_propose(prepared.bytes().to_vec(), Some(prepared.log_reservation()?))
        })
    }

    fn try_propose(
        &mut self,
        data: Vec<u8>,
        reservation: Option<&Arc<super::log_retention::LogReservation>>,
    ) -> Result<Option<CellGroupProposal>, RuntimeError> {
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
        let entry = Entry {
            index: before + 1,
            term: self.node.raft.term,
            data: data.clone().into(),
            ..Default::default()
        };
        let admission =
            match self
                .log_admission
                .prepare(&self.group, std::slice::from_ref(&entry), reservation)
            {
                Ok(admission) => admission,
                Err(RuntimeError::Overloaded) => return Ok(None),
                Err(error) => return Err(error),
            };
        let result = self.node.propose(Vec::new(), data);
        self.finish_log_admission(admission)?;
        if let Err(error) = result {
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
        // Track only newly appended mutations. A duplicate, rejected
        // preparation or enrollment does not contribute to per-cell heat.
        if let (Some(cell), Ok(now_ms), Ok(membership)) = (
            candidate.mutation_cell(),
            self.log.domain().mono_ms(),
            MembershipRevision::new(self.current.marker().membership_revision()),
        ) {
            self.write_heat
                .record(accepted.position.term, membership, now_ms, cell);
        }
        self.proposals.accept(candidate, accepted.clone(), permit);
        self.admitted_writes = self.admitted_writes.saturating_add(1);
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

    /// Business reads have their own authenticated forwarding context; seed
    /// contexts remain leader-local and cannot enter this path.
    pub(super) fn read_forwarded_cell_index(
        &mut self,
        context: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        if super::runtime::reads::cell_origin(&context) != Some(self.membership_read_origin()) {
            return Err(RuntimeError::Invalid("cell read origin".into()));
        }
        if !self.membership_read_ready()?
            || (self.node.raft.state == StateRole::Leader && !self.status()?.ready)
        {
            return Err(RuntimeError::NotReady);
        }
        if self.node.raft.read_only.pending_read_count() >= self.options.max_pending_reads {
            return Err(RuntimeError::Overloaded);
        }
        self.node.read_index(context);
        Ok(())
    }

    pub(super) fn membership_read_origin(&self) -> u64 {
        self.group.identity().member.get()
    }

    /// Safe forwarding requires an active coherent member and known leader.
    /// This does not change leader-only mutation or seed readiness.
    pub(super) fn membership_read_ready(&self) -> Result<bool, RuntimeError> {
        self.require_application_active()?;
        let cache = self.group.read()?;
        let member = self.group.identity().member.get();
        let conf = &cache.conf_state;
        // Retained history still binds its old identity. A replica removed
        // from the durable configuration cannot complete a quorum read.
        if !conf.voters.contains(&member)
            && !conf.voters_outgoing.contains(&member)
            && !conf.learners.contains(&member)
            && !conf.learners_next.contains(&member)
        {
            return Err(RuntimeError::NotFound);
        }
        // Leaders drop ReadIndex before committing their first entry in the
        // current term. Local durable apply proves that point on followers too.
        Ok(matches!(
            self.node.raft.state,
            StateRole::Leader | StateRole::Follower
        ) && self.node.raft.leader_id != 0
            && self.node.raft.term > 0
            && self.phase.is_none()
            && self.checkpoint.is_none()
            && cache.hard_state.term == self.node.raft.term
            && self.current.marker().position().term.get() == self.node.raft.term
            && cache.group_manifest.as_ref() == Some(self.current.marker())
            && cache.membership.permits_group(self.group.identity()))
    }

    pub(super) fn read_membership_index(&mut self, context: Vec<u8>) -> Result<(), RuntimeError> {
        if super::runtime::reads::membership_origin(&context) != Some(self.membership_read_origin())
        {
            return Err(RuntimeError::Invalid("membership read origin".into()));
        }
        if !self.membership_read_ready()? {
            return Err(RuntimeError::NotReady);
        }
        if self.node.raft.read_only.pending_read_count() >= self.options.max_pending_reads {
            return Err(RuntimeError::Overloaded);
        }
        self.node.read_index(context);
        Ok(())
    }

    fn park_fenced(&self) -> bool {
        self.pending_park.is_some() || self.parked.is_some() || self.waking_after.is_some()
    }

    pub(super) fn check_park_wake(&self, generation: ParkGeneration) -> Result<(), RuntimeError> {
        self.live()?;
        let marker = self.current.marker();
        if !marker.is_active()
            || self.parked.is_some_and(|current| current != generation)
            || marker
                .park_cut()
                .is_some_and(|cut| cut.generation() > generation.get())
            || self.pending_park.as_ref().is_some_and(|pending| {
                pending.preparation.proposal.position.index.get() > generation.get()
            })
        {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(())
    }

    pub(super) fn is_parked(&self) -> bool {
        self.parked.is_some()
    }

    pub(super) fn parked_term(&self) -> Option<u64> {
        self.parked.map(|_| self.node.raft.term)
    }

    pub(super) fn parked_effect_deadline(&self) -> Option<u64> {
        self.parked?;
        self.current.marker().root().next_due_unix_millis()
    }

    pub(super) fn begin_park(
        &mut self,
        certificate: &ParkCertificate,
    ) -> Result<(Box<CellGroupPersist>, usize), RuntimeError> {
        if self.parked.is_none() {
            self.require_park_acknowledgement(certificate.leader()?, certificate.cut())?;
            if Arc::strong_count(&self.leader_term) != 1
                || self
                    .deliveries
                    .iter()
                    .any(|delivery| delivery.strong_count() != 0)
            {
                return Err(RuntimeError::NotReady);
            }
        } else if !self.idle() {
            return Err(RuntimeError::NotReady);
        }
        let voter = self.group.identity();
        self.group
            .read()?
            .admit_park(certificate, voter.member, voter.node_identity())?;
        self.persist_park_change(LocalParkChange::Park(Box::new(certificate.clone())))
    }

    pub(super) fn finish_park(
        &mut self,
        certificate: &ParkCertificate,
    ) -> Result<(), RuntimeError> {
        let cache = self.group.read()?;
        if !cache
            .local_park
            .as_ref()
            .is_some_and(|park| park.parked() && park.certificate() == Some(certificate))
        {
            return Err(RuntimeError::StaleCompletion);
        }
        drop(cache);
        let generation = ParkGeneration::new(certificate.cut().generation())
            .map_err(|_| RuntimeError::StaleCompletion)?;
        self.parked = Some(generation);
        self.pending_park = None;
        self.leader_term.store(0, Ordering::Release);
        self.leader_term = Arc::new(AtomicU64::new(0));
        self.source_observed = None;
        self.advance_source_epoch();
        Ok(())
    }

    pub(super) fn begin_wake(&mut self) -> Result<(Box<CellGroupPersist>, usize), RuntimeError> {
        self.live()?;
        if !self.idle() {
            return Err(RuntimeError::NotReady);
        }
        let generation = self.parked.ok_or(RuntimeError::NotReady)?;
        self.persist_park_change(LocalParkChange::Close(generation))
    }

    pub(super) fn finish_wake(
        &mut self,
        generation: ParkGeneration,
        campaign: bool,
    ) -> Result<(), RuntimeError> {
        let floor = LogIndex::new(generation.get());
        if self.parked != Some(generation)
            || self
                .group
                .read()?
                .local_park
                .as_ref()
                .and_then(LocalParkRecord::wake_floor)
                != Some(floor)
        {
            return Err(RuntimeError::StaleCompletion);
        }
        self.waking_after = Some(floor);
        self.parked = None;
        // Clear soft leadership without changing the retained term or vote.
        // The next election, or a live leader's later entry, supplies authority.
        self.node.raft.become_follower(self.node.raft.term, 0);
        self.refresh_execution_fence();
        if campaign {
            match self.campaign() {
                Ok(()) | Err(RuntimeError::NotReady | RuntimeError::Overloaded) => (),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn require_park_acknowledgement(
        &self,
        leader: MemberId,
        cut: &GroupParkCut,
    ) -> Result<(), RuntimeError> {
        self.live()?;
        let position = cut.position();
        if self.node.raft.state == StateRole::Leader {
            let now = self
                .log
                .domain()
                .mono_ms()
                .map_err(|_| RuntimeError::Closed)?;
            if self.pending_park.as_ref().is_none_or(|pending| {
                pending.preparation.proposal.position != position
                    || pending.abort.is_some()
                    || now >= pending.request.deadline_ms
            }) {
                return Err(RuntimeError::NotReady);
            }
        }
        if !self.idle()
            || self.parked.is_some()
            || self.waking_after.is_some()
            || self.pending_seal.is_some()
            || self.node.raft.leader_id != leader.get()
            || self.node.raft.term != position.term.get()
            || self.node.raft.raft_log.last_index() != position.index.get()
            || self.node.raft.raft_log.committed != position.index.get()
            || self.node.raft.raft_log.applied != position.index.get()
            || self.current.marker().park_cut() != Some(cut)
            || self.applying.is_some()
            || !self.apply.is_empty()
            || self.node.raft.read_only.pending_read_count() != 0
        {
            return Err(RuntimeError::NotReady);
        }
        let cache = self.group.read()?;
        let voter = self.group.identity();
        if cache.group_manifest.as_ref() != Some(self.current.marker())
            || !cache.conf_state.voters.contains(&leader.get())
        {
            return Err(RuntimeError::NotReady);
        }
        cache.admit_park_acknowledgement(cut, voter.member, voter.process)?;
        Ok(())
    }

    pub(super) fn begin_park_acknowledgement(
        &mut self,
        leader: MemberId,
        cut: &GroupParkCut,
    ) -> Result<(Box<CellGroupPersist>, usize), RuntimeError> {
        self.require_park_acknowledgement(leader, cut)?;
        self.persist_park_change(LocalParkChange::Acknowledge(cut.clone()))
    }

    pub(super) fn begin_park_certificate(
        &mut self,
        certificate: &ParkCertificate,
    ) -> Result<(Box<CellGroupPersist>, usize), RuntimeError> {
        self.require_park_certificate(certificate)?;
        self.persist_park_change(LocalParkChange::Certify(Box::new(certificate.clone())))
    }

    fn persist_park_change(
        &mut self,
        change: LocalParkChange,
    ) -> Result<(Box<CellGroupPersist>, usize), RuntimeError> {
        let mut write = GroupWrite::new(&self.group);
        write.local_park = Some(change);
        let bytes = write.estimated_bytes()?;
        let work = self.prepare_persistence(write)?;
        Ok((work, bytes))
    }

    fn require_park_certificate(&self, certificate: &ParkCertificate) -> Result<(), RuntimeError> {
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        self.require_park_acknowledgement(certificate.leader()?, certificate.cut())?;
        let voter = self.group.identity();
        self.group.read()?.admit_park_certificate(
            certificate,
            voter.member,
            voter.node_identity(),
        )?;
        Ok(())
    }

    pub(super) fn finish_park_certificate(
        &self,
        certificate: &ParkCertificate,
    ) -> Result<SelectedParkCertificate, RuntimeError> {
        if self.is_parked() {
            self.live()?;
            if self.last_sync == 0 {
                return Err(RuntimeError::StaleCompletion);
            }
        } else {
            self.require_park_certificate(certificate)?;
        }
        if self
            .group
            .read()?
            .local_park
            .as_ref()
            .and_then(|park| park.certificate())
            != Some(certificate)
        {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(SelectedParkCertificate {
            certificate: Box::new(certificate.clone()),
            sync_sequence: self.last_sync,
        })
    }

    pub(super) fn finish_park_acknowledgement(
        &self,
        leader: MemberId,
        cut: GroupParkCut,
    ) -> Result<ParkAcknowledgement, RuntimeError> {
        self.require_park_acknowledgement(leader, &cut)?;
        let voter = self.group.identity().clone();
        let cache = self.group.read()?;
        if !cache
            .local_park
            .as_ref()
            .is_some_and(|park| park.matches_acknowledgement(&cut, voter.process))
        {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(ParkAcknowledgement {
            voter,
            leader,
            cut,
            sync_sequence: self.last_sync,
        })
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
        let pending = self.apply.front().ok_or(RuntimeError::StaleCompletion)?;
        self.validate_apply(&pending.entry, &next)?;
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
            Some(Phase::Checkpoint(checkpoint, _, _)) => {
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
        write: GroupWrite<GroupHistory>,
    ) -> Result<CellGroupEffect, RuntimeError> {
        self.prepare_persistence(write)
            .map(CellGroupEffect::Persist)
    }

    fn prepare_persistence(
        &mut self,
        mut write: GroupWrite<GroupHistory>,
    ) -> Result<Box<CellGroupPersist>, RuntimeError> {
        let ticket = self.ticket(LogPosition::default(), [0; 32])?;
        write.log_reservations = self.log_admission.persistence(&write.entries)?;
        write.consensus_pin = Some(self.lease.clone());
        self.waiting = Some(ticket.clone());
        Ok(Box::new(CellGroupPersist {
            log: self.log.clone(),
            ticket,
            write,
        }))
    }

    fn enqueue(&mut self, entries: Vec<Entry>) -> Result<(), RuntimeError> {
        if entries.is_empty() {
            return Ok(());
        }
        let queued_at_ms = self
            .log
            .domain()
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?;
        let mut previous = self
            .apply
            .back()
            .map_or(self.current.marker().position().index.get(), |pending| {
                pending.entry.index
            });
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
            self.apply.push_back(PendingApply {
                entry,
                queued_at_ms,
            });
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

    /// Register the exact output admission before exposing a Message. A physical
    /// sender retains it independently of its caller's response waiter.
    pub(super) fn track_delivery(&mut self, permit: &Arc<crate::raft::budget::Permit>) {
        self.deliveries
            .retain(|delivery| delivery.strong_count() != 0);
        self.deliveries.push(Arc::downgrade(permit));
    }

    fn announce_configuration(&mut self, index: u64) -> Result<(), RuntimeError> {
        if self
            .configuration_fence
            .as_ref()
            .is_some_and(|f| f.index == index && f.announced)
        {
            return Ok(());
        }
        self.configuration_fence = Some(ConfigurationFence {
            index,
            announced: true,
        });
        if self.node.raft.state != StateRole::Leader {
            return Ok(());
        }
        let hard = self.group.initial_state()?.hard_state;
        if hard.term != self.node.raft.term || hard.vote != self.node.raft.vote {
            return Ok(());
        }
        let entry = self
            .apply
            .front()
            .map(|pending| &pending.entry)
            .filter(|e| e.index == index)
            .ok_or_else(|| RuntimeError::Protocol("missing configuration apply".into()))?;
        let messages = self
            .node
            .raft
            .prs()
            .iter()
            .filter(|(member, _)| **member != self.node.raft.id)
            .map(|(&member, _)| Message {
                msg_type: MessageType::MsgAppend,
                from: self.node.raft.id,
                to: member,
                term: self.node.raft.term,
                index,
                log_term: entry.term,
                commit: index,
                ..Default::default()
            })
            .collect();
        // Paused peers may miss the normal commit broadcast. Announce this exact
        // committed index/term before removing their old route. Acceptance is
        // advisory; the catalog still requires actual physical closure.
        self.messages(messages)
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
                        if self.waking_after.is_some() {
                            self.refresh_execution_fence();
                        }
                        self.configuration_fence = None;
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
                    self.finish_log_admission(Default::default())?;
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
                Some(Phase::Checkpoint(checkpoint, submitted, admission)) => {
                    if !submitted {
                        let mut write = GroupWrite::new(&self.group);
                        write.snapshot =
                            Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
                        write.checkpoint_admission = Some(admission.clone());
                        self.phase = Some(Phase::Checkpoint(checkpoint, true, admission));
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
                        let entry = &self
                            .apply
                            .front()
                            .ok_or(RuntimeError::StaleCompletion)?
                            .entry;
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
                    let entry = &self
                        .apply
                        .front()
                        .ok_or(RuntimeError::StaleCompletion)?
                        .entry;
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
                        self.configuration_fence = None;
                    }
                    let last = self.node.raft.raft_log.last_index();
                    let automatic = self.node.raft.state == StateRole::Leader
                        && *self.node.raft.prs().conf().auto_leave()
                        && self.node.raft.raft_log.applied <= self.node.raft.pending_conf_index
                        && ticket.position.index.get() >= self.node.raft.pending_conf_index;
                    let admission = self.log_admission.applying(automatic)?;
                    self.node.advance_apply_to(ticket.position.index.get());
                    self.finish_log_admission(admission)?;
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
                    self.apply_bytes -= entry.entry.compute_size() as usize + 128;
                    self.applying = None;
                    // A fresh installed leaf has this exact local Raft
                    // position. Metadata entries and snapshots do not.
                    if next.marker().root().last_mutation() == Some(ticket.position) {
                        self.applied_writes = self.applied_writes.saturating_add(1);
                    }
                    self.current = *next;
                    if self
                        .pending_park
                        .as_ref()
                        .is_some_and(|pending| pending.abort == Some(ticket.position))
                    {
                        self.pending_park = None;
                        self.refresh_execution_fence();
                    } else if self.waking_after.is_some() {
                        self.refresh_execution_fence();
                    }
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
                    // Include one already-pending Ready, then freeze the finite
                    // send boundary. Later heartbeat work cannot starve Apply.
                    // An admitted snapshot keeps its existing priority.
                    let configuration_index = self
                        .apply
                        .front()
                        .map(|pending| &pending.entry)
                        .filter(|e| e.get_entry_type() == EntryType::EntryConfChangeV2)
                        .filter(|_| self.node.raft.raft_log.unstable_snapshot().is_none())
                        .map(|e| e.index);
                    let fenced = self
                        .configuration_fence
                        .as_ref()
                        .is_some_and(|f| configuration_index == Some(f.index));
                    if !fenced
                        && collect
                        && self.node.has_ready()
                        && self.apply_bytes < self.options.max_group_apply_bytes
                    {
                        if let Some(index) = configuration_index {
                            self.configuration_fence = Some(ConfigurationFence {
                                index,
                                announced: false,
                            });
                        }
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
                    if let Some(index) = configuration_index {
                        if self
                            .configuration_fence
                            .as_ref()
                            .is_none_or(|f| f.index != index)
                        {
                            self.configuration_fence = Some(ConfigurationFence {
                                index,
                                announced: false,
                            });
                        }
                        self.deliveries
                            .retain(|delivery| delivery.strong_count() != 0);
                        if !self.deliveries.is_empty() {
                            return Ok(None);
                        }
                        self.announce_configuration(index)?;
                        if !self.effects.is_empty() {
                            continue;
                        }
                    }
                    let Some(entry) = self.apply.front().map(|pending| pending.entry.clone())
                    else {
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
        if !next.marker().preserves_transfer(self.current.marker()) {
            return Err(invalid());
        }
        let same_transfer =
            next.marker().transfer_origin() == self.current.marker().transfer_origin();
        let same_recovery = next.marker().recovery == self.current.marker().recovery;
        let same_seal = next.marker().source_seal() == self.current.marker().source_seal();
        let same_park = next.marker().park_cut() == self.current.marker().park_cut();
        if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
            if !self.current.marker().is_active()
                || !same_recovery
                || !same_seal
                || !same_transfer
                || !same_park
            {
                return Err(invalid());
            }
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
            return if next.marker().root() == self.current.marker().root()
                && same_recovery
                && same_seal
                && same_transfer
                && same_park
            {
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
        let retirement = matches!(&decoded.command, GroupCommand::Retire { .. });
        if !matches!(&decoded.command, GroupCommand::PreparePark { .. }) && !same_park {
            return Err(invalid());
        }
        if !matches!(&decoded.command, GroupCommand::Activate { .. })
            && ((!self.current.marker().is_active() && !retirement)
                || !same_recovery
                || !same_transfer)
        {
            return Err(invalid());
        }
        if !matches!(&decoded.command, GroupCommand::Seal { .. }) && !retirement && !same_seal {
            return Err(invalid());
        }
        let witness = cellar_logic::cell_group::CommitWitness {
            group: self.current.marker().route().history,
            position: entry_position(entry),
            proposal: decoded.binding.proposal,
        };
        match decoded.command {
            GroupCommand::PreparePark {
                generation,
                term,
                membership,
            } => {
                let mut expected = self.current.marker().clone();
                expected.position = entry_position(entry);
                expected
                    .prepare_park(generation, term, membership)
                    .map_err(|_| invalid())?;
                if next.marker() != &expected {
                    return Err(invalid());
                }
            }
            GroupCommand::Activate {
                transition,
                publication,
                manifest,
            } => {
                if decoded.binding.proposal.bytes() != transition.bytes() {
                    return Err(invalid());
                }
                let mut expected = self.current.marker().clone();
                expected.position = entry_position(entry);
                expected
                    .activate(transition, publication, manifest)
                    .map_err(|_| invalid())?;
                if next.marker() != &expected {
                    return Err(invalid());
                }
            }
            GroupCommand::Seal {
                transition,
                authorization,
                ..
            } => {
                if decoded.binding.proposal.bytes() != transition.bytes() {
                    return Err(invalid());
                }
                use sha2::{Digest, Sha256};
                let mut expected = self.current.marker().clone();
                expected.position = entry_position(entry);
                expected
                    .seal_source(
                        transition,
                        authorization,
                        Sha256::digest(&entry.data).into(),
                    )
                    .map_err(|_| invalid())?;
                if next.marker() != &expected {
                    return Err(invalid());
                }
            }
            GroupCommand::Retire {
                transition,
                publication,
            } => {
                if decoded.binding.proposal.bytes() != transition.bytes() {
                    return Err(invalid());
                }
                let mut expected = self.current.marker().clone();
                expected.position = entry_position(entry);
                expected
                    .retire_source(transition, publication)
                    .map_err(|_| invalid())?;
                if next.marker() != &expected {
                    return Err(invalid());
                }
            }
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
                    || entry.index != 0
                    || entry.term != 0
                    || !entry.context.is_empty()
                    || entry.get_entry_type() != EntryType::EntryNormal
                {
                    return Err(invalid());
                }
                if super::runtime::reads::reserved_context(&entry.data) {
                    let origin = super::runtime::reads::membership_origin(&entry.data)
                        .or_else(|| super::runtime::reads::cell_origin(&entry.data))
                        .ok_or_else(invalid)?;
                    // Seed/private execution contexts remain local. Forwarded
                    // cell/metadata requests bind their authenticated origin;
                    // replies come only from this follower's current leader.
                    if (kind == MessageType::MsgReadIndex && origin != message.from)
                        || (kind == MessageType::MsgReadIndexResp
                            && (origin != self.group.identity().member.get()
                                || message.from != self.node.raft.leader_id))
                    {
                        return Err(invalid());
                    }
                }
                if kind == MessageType::MsgReadIndex {
                    if !self.is_parked() {
                        self.require_application_active()?;
                    }
                    if self.node.raft.read_only.pending_read_count()
                        >= self.options.max_pending_reads
                    {
                        return Err(RuntimeError::Overloaded);
                    }
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
        self.source_epoch.store(0, Ordering::Release);
    }
}
