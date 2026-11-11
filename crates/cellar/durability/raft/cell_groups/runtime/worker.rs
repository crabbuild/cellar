// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fair group turns, node-wide persistence batches and joined shutdown.

use super::{park_deadlines::ParkDeadlines, *};
use crate::raft::storage::{LogError, SyncReceipt};
use cellar_logic::consensus::OperationId;
use futures_util::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const PARK_WAKE_BATCH: usize = 32;

fn wakes_parked_peer(message: &::raft::prelude::Message, term: u64) -> bool {
    use ::raft::prelude::MessageType::*;
    message.term > term
        || matches!(
            message.get_msg_type(),
            MsgAppend
                | MsgHeartbeat
                | MsgRequestVote
                | MsgRequestPreVote
                | MsgReadIndex
                | MsgTimeoutNow
        )
}

fn wakes_parked_group(command: &Command, term: u64) -> bool {
    match command {
        Command::Step(message) | Command::AdmitStep(message) => wakes_parked_peer(message, term),
        Command::SnapshotStatus(..) | Command::Unreachable(_) => false,
        _ => true,
    }
}

struct Group {
    owner: Box<CellGroupOwner>,
    queued: bool,
    retiring: bool,
    deferred: VecDeque<Request>,
    // Requests through the last cell read present when collection began.
    // Later arrivals cannot extend this prefix.
    read_collection_remaining: usize,
    completion: Option<Request>,
    park_write: Option<Request>,
    // One status per current peer, retaining node-wide hint item/byte admission.
    snapshot_hints: BTreeMap<u64, (bool, Permit)>,
    inbound: Arc<Budget>,
    tick: bool,
    wake_due: bool,
    write: Option<Box<CellGroupPersist>>,
    write_bytes: usize,
    pending: Option<Result<CellGroupEffect, RuntimeError>>,
    settled: Option<CellGroupEffect>,
    failed: Option<RuntimeError>,
    usage: (usize, usize, usize),
}

impl Group {
    fn pop_deferred(&mut self) -> Option<Request> {
        let request = self.deferred.pop_front()?;
        self.read_collection_remaining = self.read_collection_remaining.saturating_sub(1);
        Some(request)
    }

    fn collect_queued_reads(&mut self) {
        self.read_collection_remaining = self
            .deferred
            .iter()
            .rposition(|request| {
                matches!(&request.command, Command::Read(read) if read.kind.cell_linearizable())
            })
            .map_or(0, |position| position + 1);
    }

    fn wake(&mut self, history: GroupHistory, order: &mut VecDeque<GroupHistory>) {
        if !self.queued && (!self.owner.waiting() || self.failed.is_some()) {
            self.queued = true;
            order.push_back(history);
        }
    }

    fn usage(&self) -> (usize, usize, usize) {
        let (ready, apply, reads) = self.owner.usage();
        let pending = self
            .pending
            .as_ref()
            .map_or(0, |e| e.as_ref().map_or(4096, CellGroupEffect::bytes));
        (
            ready
                .saturating_add(pending)
                .saturating_add(self.write_bytes)
                .saturating_add(self.settled.as_ref().map_or(0, CellGroupEffect::bytes)),
            apply,
            reads,
        )
    }

    fn fail(&mut self, error: RuntimeError) {
        self.owner.revoke_execution();
        self.failed = Some(error.clone());
        self.pending = Some(Err(error.clone()));
        self.settled = None;
        self.write = None;
        self.write_bytes = 0;
        self.tick = false;
        self.wake_due = false;
        self.snapshot_hints.clear();
        self.read_collection_remaining = 0;
        for request in self.deferred.drain(..) {
            reply(request, Err(error.clone()));
        }
        if let Some(request) = self.park_write.take() {
            reply(request, Err(error.clone()));
        }
        if let Some(request) = self.completion.take() {
            reply(request, Err(error));
        }
    }
}

fn reply(request: Request, result: Result<Response, RuntimeError>) {
    let Request {
        reply,
        command,
        _permit,
        inbound,
        ..
    } = request;
    drop(command);
    drop(_permit);
    drop(inbound);
    let _ = reply.send(result);
}

struct Job {
    history: GroupHistory,
    work: Box<CellGroupPersist>,
    bytes: usize,
}

type Batch = (
    Vec<Job>,
    OperationId,
    Result<SyncReceipt<GroupHistory>, LogError>,
);

pub(super) struct Worker {
    log: ReplicaLog,
    options: RaftRuntimeOptions,
    domain: crate::asyncrt::DomainToken,
    groups: BTreeMap<GroupHistory, Group>,
    ticking: BTreeSet<GroupHistory>,
    park_deadlines: ParkDeadlines,
    // Every physical roster insertion changes the scan token. Future detach
    // paths must also advance it, so an ABA roster cannot validate a sample.
    roster_revision: u64,
    order: VecDeque<GroupHistory>,
    writes: VecDeque<GroupHistory>,
    events: mpsc::Sender<CellGroupRuntimeEvent>,
    outbox: VecDeque<CellGroupRuntimeEvent>,
    output: Arc<Budget>,
    proposals: Arc<Budget>,
    inflight: Option<BoxFuture<'static, Batch>>,
    inflight_bytes: usize,
    sequence: u64,
    retry_at: u64,
    ready: usize,
    apply: usize,
    reads: usize,
    cell_reads: reads::Reads,
}

impl Worker {
    pub(super) fn new(
        log: ReplicaLog,
        options: RaftRuntimeOptions,
        domain: crate::asyncrt::DomainToken,
        events: mpsc::Sender<CellGroupRuntimeEvent>,
    ) -> Self {
        let performance = log.performance();
        Self {
            log,
            output: Budget::new(options.max_event_bytes, options.max_events),
            proposals: Budget::new(options.max_request_bytes, options.max_requests),
            cell_reads: reads::Reads::new(&options, performance),
            options,
            domain,
            groups: BTreeMap::new(),
            ticking: BTreeSet::new(),
            park_deadlines: ParkDeadlines::default(),
            roster_revision: 0,
            order: VecDeque::new(),
            writes: VecDeque::new(),
            events,
            outbox: VecDeque::new(),
            inflight: None,
            inflight_bytes: 0,
            sequence: 0,
            retry_at: 0,
            ready: 0,
            apply: 0,
            reads: 0,
        }
    }

    fn refresh(&mut self, history: GroupHistory) {
        let group = self
            .groups
            .get_mut(&history)
            .expect("scheduled group exists");
        if group.failed.is_some() {
            self.ticking.remove(&history);
            self.park_deadlines.remove(&history);
        }
        let usage = group.usage();
        self.ready = self.ready - group.usage.0 + usage.0;
        self.apply = self.apply - group.usage.1 + usage.1;
        self.reads = self.reads - group.usage.2 + usage.2;
        group.usage = usage;
    }

    fn leadership_batch(
        &self,
        after: Option<GroupHistory>,
    ) -> Result<LeadershipBatch, RuntimeError> {
        leadership::capture(&self.groups, self.roster_revision, after, |group| {
            if let Some(error) = &group.failed {
                return Err(error.clone());
            }
            group.owner.local_leader()
        })
    }

    fn dispatch(&mut self, mut request: Request) {
        if matches!(request.command, Command::Tick) {
            self.tick();
            reply(request, Ok(Response::Unit));
            return;
        }
        if let Command::LeadershipBatch(after) = &request.command {
            let result = self.leadership_batch(*after).map(Response::LeadershipBatch);
            reply(request, result);
            return;
        }
        let Some(history) = request.group else {
            reply(request, Err(RuntimeError::NotFound));
            return;
        };
        if matches!(request.command, Command::Attach(_)) {
            if self.groups.contains_key(&history) {
                reply(request, Err(RuntimeError::AlreadyAttached));
                return;
            }
            if self.groups.len() >= self.options.max_groups {
                reply(request, Err(RuntimeError::Overloaded));
                return;
            }
            let Some(revision) = self.roster_revision.checked_add(1) else {
                reply(
                    request,
                    Err(RuntimeError::Invalid(
                        "runtime roster revision overflow".into(),
                    )),
                );
                return;
            };
            let Request {
                command: Command::Attach(mut owner),
                reply: answer,
                _permit,
                ..
            } = request
            else {
                unreachable!()
            };
            // attach constructs a fresh follower; it has no accepted proposals.
            // All owners retain acceptance against the same node-wide budget.
            owner.proposal_budget = self.proposals.clone();
            let mut group = Group {
                owner,
                queued: false,
                retiring: false,
                deferred: VecDeque::new(),
                read_collection_remaining: 0,
                completion: None,
                park_write: None,
                snapshot_hints: BTreeMap::new(),
                inbound: Budget::new(
                    (self.options.max_request_bytes / 4).max(self.options.max_message_bytes + 4096),
                    (self.options.max_requests / 8).clamp(1, 32),
                ),
                tick: false,
                wake_due: false,
                write: None,
                write_bytes: 0,
                pending: None,
                settled: None,
                failed: None,
                usage: (0, 0, 0),
            };
            group.wake(history, &mut self.order);
            self.groups.insert(history, group);
            self.ticking.insert(history);
            self.roster_revision = revision;
            self.refresh(history);
            drop(_permit);
            let _ = answer.send(Ok(Response::Unit));
            return;
        }
        if let Command::QuiesceRetired(source) = &request.command {
            let result = source.check(&self.log).and_then(|()| self.retire(history));
            reply(request, result.map(|()| Response::Unit));
            return;
        }
        let Some(group) = self.groups.get_mut(&history) else {
            reply(request, Err(RuntimeError::NotFound));
            return;
        };
        if matches!(request.command, Command::Quarantine) {
            if group.failed.is_none() {
                group.fail(RuntimeError::GroupQuarantined);
                group.wake(history, &mut self.order);
            }
            reply(request, Ok(Response::Unit));
            self.refresh(history);
            return;
        }
        if let Some(error) = &group.failed {
            reply(request, Err(error.clone()));
            return;
        }
        if group.retiring
            && !matches!(
                request.command,
                Command::Apply(..)
                    | Command::CheckpointBeforeApply(..)
                    | Command::Status
                    | Command::Observe
            )
        {
            reply(request, Err(RuntimeError::NotReady));
            return;
        }
        // Prepare is a park-only operation. A retry may race certificate
        // delivery and must never enter the deferred work that wakes a voter.
        if matches!(request.command, Command::PreparePark(_))
            && (group.owner.is_parked() || group.park_write.is_some())
        {
            reply(request, Err(RuntimeError::NotReady));
            return;
        }
        let wake = match &request.command {
            Command::FencePark { generation, .. } => reads::MembershipRead::Wake(*generation),
            Command::Read(read) => read.kind.membership_mode(),
            _ => reads::MembershipRead::Ordinary,
        };
        let check = match wake {
            reads::MembershipRead::Wake(generation) => group.owner.check_park_wake(generation),
            reads::MembershipRead::ParkAdmission
                if group.owner.is_parked() || group.park_write.is_some() =>
            {
                Err(RuntimeError::NotReady)
            }
            _ => Ok(()),
        };
        if let Err(error) = check {
            reply(request, Err(error));
            return;
        }
        if let Command::CertifyPark(certificate) = &request.command {
            if group.owner.is_parked() {
                let selected = group
                    .owner
                    .finish_park_certificate(certificate)
                    .map(Response::ParkCertified);
                reply(request, selected);
                return;
            }
        }
        if matches!(
            &request.command,
            Command::AcknowledgePark { .. } | Command::CertifyPark(_) | Command::EnterPark(_)
        ) {
            // A metadata write can share the physical batch, but must never
            // overtake an already admitted Raft turn or application completion.
            if group.tick
                || !group.deferred.is_empty()
                || group.completion.is_some()
                || !group.snapshot_hints.is_empty()
                || group.pending.is_some()
                || group.settled.is_some()
                || group.write.is_some()
                || self.cell_reads.busy(history)
            {
                reply(request, Err(RuntimeError::NotReady));
                return;
            }
            let preparation = match &request.command {
                Command::AcknowledgePark { leader, cut } => {
                    group.owner.begin_park_acknowledgement(*leader, cut)
                }
                Command::CertifyPark(certificate) => {
                    group.owner.begin_park_certificate(certificate)
                }
                Command::EnterPark(certificate) => group.owner.begin_park(certificate),
                _ => Err(RuntimeError::Protocol("park write queue".into())),
            };
            match preparation {
                Ok((work, bytes)) => {
                    group.write = Some(work);
                    group.write_bytes = bytes;
                    group.park_write = Some(request);
                    self.writes.push_back(history);
                    self.refresh(history);
                }
                Err(error) => reply(request, Err(error)),
            }
            return;
        }
        if matches!(request.command, Command::Status) {
            reply(request, group.owner.status().map(Response::Status));
            return;
        }
        if let Command::RoutingStatus { advice } = request.command {
            reply(
                request,
                group
                    .owner
                    .routing_status(advice)
                    .map(Response::RoutingStatus),
            );
            return;
        }
        if matches!(request.command, Command::PendingTransferQuorum) {
            reply(
                request,
                group
                    .owner
                    .pending_transfer_quorum()
                    .map(|proof| Response::PendingTransferQuorum(Box::new(proof))),
            );
            return;
        }
        if matches!(request.command, Command::Observe) {
            let queued_linearizable_reads = group
                .deferred
                .iter()
                .filter(|request| {
                    matches!(&request.command, Command::Read(read) if read.kind.cell_linearizable())
                })
                .count();
            reply(
                request,
                group
                    .owner
                    .observe(queued_linearizable_reads)
                    .map(|value| Response::Observation(Box::new(value))),
            );
            return;
        }
        if matches!(request.command, Command::ObservePartition) {
            reply(
                request,
                group
                    .owner
                    .observe_partition()
                    .map(|value| Response::PartitionObservation(Box::new(value))),
            );
            return;
        }
        if matches!(request.command, Command::ReplicationStatus) {
            reply(
                request,
                group
                    .owner
                    .replication_status()
                    .map(Response::ReplicationStatus),
            );
            return;
        }
        if let Command::CaptureCheckpoint(pool) = &request.command {
            let source = group.owner.capture_checkpoint(pool);
            reply(request, Ok(Response::CheckpointSource(Box::new(source))));
            return;
        }
        if let Some(term) = group.owner.parked_term() {
            match &request.command {
                Command::Step(message) | Command::AdmitStep(message)
                    if !wakes_parked_peer(message, term) =>
                {
                    // A delayed reply to the last active term carries no new
                    // protocol work. It must not undo completed parking.
                    let result = group
                        .owner
                        .validate_message(message)
                        .map(|()| Response::Unit);
                    reply(request, result);
                    return;
                }
                Command::SnapshotStatus(..) | Command::Unreachable(_) => {
                    reply(request, Ok(Response::Unit));
                    return;
                }
                _ => (),
            }
        }
        if let Command::SnapshotStatus(member, delivered) = request.command {
            if let Err(error) = group.owner.validate_snapshot_member(member) {
                reply(request, Err(error));
                return;
            }
            let Request {
                reply: answer,
                _permit,
                ..
            } = request;
            group.snapshot_hints.insert(member, (delivered, _permit));
            group.wake(history, &mut self.order);
            let _ = answer.send(Ok(Response::Unit));
            return;
        }
        let admission_only = match &request.command {
            Command::AdmitStep(message) => {
                if let Err(error) = group.owner.validate_message(message) {
                    reply(request, Err(error));
                    return;
                }
                true
            }
            Command::Unreachable(_) => true,
            _ => false,
        };
        if admission_only {
            let Some(inbound) = group.inbound.acquire(request.bytes) else {
                reply(request, Err(RuntimeError::Overloaded));
                return;
            };
            let Request {
                command,
                reply: answer,
                bytes,
                _permit,
                ..
            } = request;
            let command = match command {
                Command::AdmitStep(message) => Command::Step(message),
                hint => hint,
            };
            let (reply, _) = oneshot::channel();
            group.deferred.push_back(Request {
                group: Some(history),
                command,
                reply,
                bytes,
                _permit,
                inbound: Some(inbound),
            });
            group.wake(history, &mut self.order);
            let _ = answer.send(Ok(Response::Unit));
            return;
        }
        if matches!(
            request.command,
            Command::Apply(..) | Command::CheckpointBeforeApply(..)
        ) {
            if group.owner.is_parked() {
                reply(request, Err(RuntimeError::StaleCompletion));
                return;
            }
            if group.completion.is_some() {
                reply(request, Err(RuntimeError::Overloaded));
                return;
            }
            group.completion = Some(request);
        } else {
            let Some(permit) = group.inbound.acquire(request.bytes) else {
                reply(request, Err(RuntimeError::Overloaded));
                return;
            };
            request.inbound = Some(permit);
            group.deferred.push_back(request);
        }
        group.wake(history, &mut self.order);
    }

    fn retire(&mut self, history: GroupHistory) -> Result<(), RuntimeError> {
        let Some(group) = self.groups.get_mut(&history) else {
            return Ok(());
        };
        if let Some(error) = &group.failed {
            return Err(error.clone());
        }
        group.retiring = true;
        group.owner.revoke_execution();
        group.tick = false;
        group.wake_due = false;
        group.snapshot_hints.clear();
        self.ticking.remove(&history);
        self.park_deadlines.remove(&history);
        while let Some(request) = group.pop_deferred() {
            reply(request, Err(RuntimeError::NotReady));
        }
        if group.write.is_some()
            || group.pending.is_some()
            || group.settled.is_some()
            || group.completion.is_some()
            || group.park_write.is_some()
            || self.cell_reads.busy(history)
            || !group.owner.retirement_idle()
        {
            group.wake(history, &mut self.order);
            return Err(RuntimeError::NotReady);
        }
        let revision = self
            .roster_revision
            .checked_add(1)
            .ok_or(RuntimeError::Overloaded)?;
        let group = self.groups.remove(&history).ok_or(RuntimeError::NotFound)?;
        self.ready -= group.usage.0;
        self.apply -= group.usage.1;
        self.reads -= group.usage.2;
        self.order.retain(|queued| *queued != history);
        self.writes.retain(|queued| *queued != history);
        self.outbox.retain(|event| event.group != history);
        self.roster_revision = revision;
        drop(group);
        Ok(())
    }

    fn tick(&mut self) {
        use crate::raft::performance::Counter;
        let performance = self.log.performance();
        performance.increment(Counter::CellGroupTickRounds, 1);
        for history in &self.ticking {
            if let Some(group) = self.groups.get_mut(history) {
                performance.increment(Counter::CellGroupTicksRequested, 1);
                if group.tick {
                    performance.increment(Counter::CellGroupTicksCoalesced, 1);
                }
                group.tick = true;
                group.wake(*history, &mut self.order);
            }
        }
    }

    fn schedule_park_wakes(&mut self) -> Result<Option<u64>, RuntimeError> {
        if self.park_deadlines.next().is_none() {
            return Ok(None);
        }
        let now = self.domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
        let wall = crate::asyncrt::wall_ms().max(0) as u64;
        for _ in 0..PARK_WAKE_BATCH {
            let Some(history) = self.park_deadlines.pop_due(wall) else {
                break;
            };
            if let Some(group) = self.groups.get_mut(&history) {
                if group.failed.is_none() && group.owner.is_parked() {
                    group.wake_due = true;
                    group.wake(history, &mut self.order);
                }
            }
        }
        // Convert the next wall deadline at each scheduler turn. The periodic
        // monotonic timer also resamples the wall clock after a clock change.
        Ok(self
            .park_deadlines
            .next()
            .map(|deadline| now.saturating_add(deadline.saturating_sub(wall))))
    }

    fn progress(&mut self) -> Result<bool, RuntimeError> {
        let mut made = false;
        for _ in 0..self.order.len() {
            let Some(history) = self.order.pop_front() else {
                break;
            };
            let collect = self.ready.saturating_add(self.inflight_bytes)
                < self.options.max_ready_bytes
                && self.apply < self.options.max_apply_bytes;
            let group = self
                .groups
                .get_mut(&history)
                .ok_or(RuntimeError::NotFound)?;
            group.queued = false;
            if let Some(term) = group.owner.parked_term().filter(|_| group.failed.is_none()) {
                // Responses can arrive during the park fsync, before dispatch
                // knows the voter is parked. They carry no new work.
                group.snapshot_hints.clear();
                while group
                    .deferred
                    .front()
                    .is_some_and(|request| !wakes_parked_group(&request.command, term))
                {
                    let Some(request) = group.pop_deferred() else {
                        break;
                    };
                    reply(request, Ok(Response::Unit));
                    made = true;
                }
                if !group.owner.waiting()
                    && (group.wake_due
                        || !group.deferred.is_empty()
                        || !group.snapshot_hints.is_empty())
                {
                    match group.owner.begin_wake() {
                        Ok((work, bytes)) => {
                            self.park_deadlines.remove(&history);
                            group.write = Some(work);
                            group.write_bytes = bytes;
                            self.writes.push_back(history);
                            made = true;
                        }
                        Err(error) => group.fail(error),
                    }
                }
            } else if group.failed.is_none() {
                if let Some(request) = group.completion.take() {
                    let result = match &request.command {
                        Command::Apply(ticket, next) => {
                            group.owner.complete_apply(ticket.clone(), (**next).clone())
                        }
                        Command::CheckpointBeforeApply(ticket, checkpoint) => group
                            .owner
                            .checkpoint_before_apply(ticket, checkpoint.clone()),
                        _ => return Err(RuntimeError::Protocol("completion queue".into())),
                    };
                    if matches!(result, Err(RuntimeError::NotReady)) {
                        group.completion = Some(request);
                    } else {
                        reply(request, result.map(|()| Response::Unit));
                        made = true;
                    }
                }
                // Hints never wait behind ordinary admission or consume an
                // apply-completion slot. Their protocol mutation still waits
                // until the exact in-flight Ready/persistence phase has settled.
                if group.owner.idle() {
                    if let Some((member, (delivered, _permit))) = group.snapshot_hints.pop_first() {
                        // A committed membership change may have removed this
                        // peer while the hint was queued; it then has no progress.
                        if group.owner.validate_snapshot_member(member).is_ok() {
                            group.owner.report_snapshot(member, delivered)?;
                        }
                        made = true;
                    }
                }
                if collect && group.owner.idle() && group.pending.is_none() {
                    let collecting_reads = group.read_collection_remaining != 0;
                    if group.tick {
                        match group.owner.tick() {
                            Ok(()) => {
                                group.tick = false;
                                made = true;
                            }
                            Err(RuntimeError::NotReady) => (),
                            Err(RuntimeError::Overloaded) => {
                                group.tick = false;
                                made = true;
                            }
                            Err(error) => group.fail(error),
                        }
                    } else if let Some(request) = group.pop_deferred() {
                        let Request {
                            command,
                            reply: answer,
                            _permit,
                            inbound,
                            ..
                        } = request;
                        let mut answer = Some(answer);
                        let new_read = matches!(command, Command::ReadIndex(_))
                            || matches!(&command, Command::Read(request) if request.kind.linearizable())
                            || matches!(&command, Command::Step(m) if m.get_msg_type()==::raft::prelude::MessageType::MsgReadIndex);
                        let result = if new_read && self.reads >= self.options.max_pending_reads {
                            Err(RuntimeError::Overloaded)
                        } else {
                            match command {
                                Command::Read(request) => {
                                    let coalescible = request.kind.cell_linearizable();
                                    let now =
                                        self.domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
                                    self.cell_reads.start(
                                        history,
                                        &mut group.owner,
                                        *request,
                                        answer.take().expect("read reply"),
                                        now,
                                    );
                                    if coalescible {
                                        if !collecting_reads {
                                            group.collect_queued_reads();
                                        }
                                        // Preserve queue order and the existing
                                        // 32-item group envelope. Adjacent reads
                                        // can be collected in the same turn.
                                        for _ in 1..32 {
                                            if !group.deferred.front().is_some_and(|request| {
                                                matches!(&request.command, Command::Read(read) if read.kind.cell_linearizable())
                                            }) {
                                                break;
                                            }
                                            let Request {
                                                command: Command::Read(read),
                                                reply,
                                                ..
                                            } = group.pop_deferred().expect("queued cohort read")
                                            else {
                                                unreachable!("checked cohort read");
                                            };
                                            self.cell_reads.start(
                                                history,
                                                &mut group.owner,
                                                *read,
                                                reply,
                                                now,
                                            );
                                        }
                                    }
                                    Ok(Response::Unit)
                                }
                                Command::Execution(cell) => group
                                    .owner
                                    .execution_authority(&cell)
                                    .map(|authority| Response::Execution(Box::new(authority))),
                                Command::NextEffect { now, after } => group
                                    .owner
                                    .next_effect(now, after.as_ref())
                                    .map(|authority| Response::NextEffect(authority.map(Box::new))),
                                Command::Enrollment(cell) => group
                                    .owner
                                    .enrollment_authority(cell)
                                    .map(|authority| Response::Enrollment(Box::new(authority))),
                                Command::EnrollmentState(cell) => group
                                    .owner
                                    .enrollment_state(cell)
                                    .map(|state| Response::EnrollmentState(Box::new(state))),
                                Command::Reconciliation(cell) => group
                                    .owner
                                    .reconciliation_authority(&cell)
                                    .map(|authority| Response::Reconciliation(Box::new(authority))),
                                Command::ActivateRecovery(permit) => group
                                    .owner
                                    .activate_recovery(*permit)
                                    .map(|result| Response::RecoveryActivation(Box::new(result))),
                                Command::ActivateTransfer(permit) => group
                                    .owner
                                    .activate_transfer(*permit)
                                    .map(Response::TransferActivation),
                                Command::RetireSource(permit) => group
                                    .owner
                                    .retire_source(*permit)
                                    .map(Response::SourceRetirement),
                                Command::PreparePark(request) => {
                                    if self.cell_reads.busy(history) || group.completion.is_some() {
                                        Err(RuntimeError::NotReady)
                                    } else {
                                        group
                                            .owner
                                            .prepare_park(request)
                                            .map(Response::ParkPrepared)
                                    }
                                }
                                Command::FencePark {
                                    generation,
                                    term,
                                    deadline,
                                } => group
                                    .owner
                                    .fence_managed_park(generation, term, deadline)
                                    .map(|()| Response::Unit),
                                Command::SealSource(permit) => group
                                    .owner
                                    .seal_source(*permit)
                                    .map(|result| Response::SourceSeal(Box::new(result))),
                                Command::Configure(change) => {
                                    group.owner.configure(change).map(Response::Proposal)
                                }
                                Command::TransferLeader(member) => {
                                    group.owner.transfer_leader(member).map(|()| Response::Unit)
                                }
                                Command::TransferLeadership(transfer) => group
                                    .owner
                                    .transfer_leadership(transfer)
                                    .map(|()| Response::Unit),
                                Command::TransferLeadershipBefore(transfer, deadline) => self
                                    .domain
                                    .mono_ms()
                                    .map_err(|_| RuntimeError::Closed)
                                    .and_then(|now| {
                                        if now >= deadline {
                                            return Err(RuntimeError::DeadlineExceeded);
                                        }
                                        group.owner.transfer_leadership(transfer)
                                    })
                                    .map(|()| Response::Unit),
                                Command::RestartLeadership(check) => self
                                    .domain
                                    .mono_ms()
                                    .map_err(|_| RuntimeError::Closed)
                                    .and_then(|now| {
                                        crate::raft::restart_leadership::check_gate(
                                            &self.log, now, &check,
                                        )
                                    })
                                    .and_then(|()| group.owner.restart_leadership(*check))
                                    .map(|()| Response::Unit),
                                Command::Campaign => {
                                    group.owner.campaign().map(|()| Response::Unit)
                                }
                                Command::PublishCheckpoint(checkpoint) => group
                                    .owner
                                    .publish_checkpoint(checkpoint)
                                    .map(|()| Response::Unit),
                                Command::Checkpoint(message, checkpoint) => group
                                    .owner
                                    .step_checkpoint(message, checkpoint)
                                    .map(Response::Checkpoint),
                                Command::Step(message) => {
                                    group.owner.step(message).map(|()| Response::Unit)
                                }
                                Command::Propose(data) => {
                                    group.owner.propose(data).map(Response::Proposal)
                                }
                                Command::ProposeAtTerm(term, data) => group
                                    .owner
                                    .propose_at_term(term, data)
                                    .map(Response::Proposal),
                                Command::Submit(prepared, _retained) => {
                                    group.owner.submit(prepared).map(Response::Submission)
                                }
                                Command::Unreachable(member) => group
                                    .owner
                                    .report_unreachable(member)
                                    .map(|_| Response::Unit),
                                Command::ReadIndex(context) => {
                                    group.owner.read_index(context).map(|()| Response::Unit)
                                }
                                _ => Err(RuntimeError::Protocol("ordinary queue".into())),
                            }
                        };
                        drop(_permit);
                        drop(inbound);
                        if let Some(answer) = answer {
                            let _ = answer.send(result);
                        }
                        made = true;
                    }
                }
                if group.write.is_none()
                    && (group.pending.is_none() && group.settled.is_none()
                        || group.owner.control_pending())
                {
                    let emit = group.pending.is_none() && group.settled.is_none();
                    let control = group.owner.control_pending();
                    match group.owner.poll_budget(collect && emit, emit) {
                        Ok(Some(CellGroupEffect::Persist(work))) => {
                            group.write_bytes = work.write.estimated_bytes()?;
                            group.write = Some(work);
                            self.writes.push_back(history);
                            made = true;
                        }
                        Ok(Some(CellGroupEffect::ReadIndex {
                            term,
                            index,
                            context,
                        })) if reads::reserved_context(&context) => {
                            self.cell_reads.observe(history, term, index, &context);
                            made = true;
                        }
                        Ok(Some(effect)) => {
                            if group.pending.is_none() {
                                group.pending = Some(Ok(effect));
                            } else {
                                if !matches!(
                                    effect,
                                    CellGroupEffect::Applied { .. }
                                        | CellGroupEffect::CheckpointInstalled { .. }
                                        | CellGroupEffect::CheckpointPublished { .. }
                                ) || group.settled.is_some()
                                {
                                    return Err(RuntimeError::Protocol(
                                        "reserved physical completion output".into(),
                                    ));
                                }
                                group.settled = Some(effect);
                            }
                            made = true;
                        }
                        Ok(None) => (),
                        Err(RuntimeError::Closed) => return Err(RuntimeError::Closed),
                        Err(error) => group.fail(error),
                    }
                    made |= control && !group.owner.control_pending();
                }
            }
            if group.pending.is_none() {
                group.pending = group.settled.take().map(Ok);
            }
            if let Some(mut pending) = group.pending.take() {
                let bytes = pending.as_ref().map_or(4096, CellGroupEffect::bytes);
                if bytes > self.options.max_event_bytes {
                    group.fail(RuntimeError::Overloaded);
                } else if let Some(permit) = self.output.acquire(bytes) {
                    let permit = Arc::new(permit);
                    if let Ok(CellGroupEffect::Message(_)) = &pending {
                        group.owner.track_delivery(&permit);
                    }
                    if let Ok(CellGroupEffect::Apply(work)) = &mut pending {
                        work.ticket.admission = Some(permit.clone());
                    }
                    self.outbox.push_back(CellGroupRuntimeEvent {
                        group: history,
                        effect: pending,
                        _permit: permit,
                    });
                    made = true;
                } else {
                    group.pending = Some(pending);
                }
            }
            if group.pending.is_some()
                || group.settled.is_some()
                || (group.failed.is_none()
                    && (group.tick
                        || group.wake_due
                        || !group.snapshot_hints.is_empty()
                        || !group.deferred.is_empty()
                        || group.completion.is_some()
                        || group.owner.has_work()))
            {
                group.wake(history, &mut self.order);
            }
            self.refresh(history);
        }
        Ok(made)
    }

    fn operation(&mut self) -> Result<OperationId, RuntimeError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RuntimeError::Overloaded)?;
        Ok(OperationId::new(self.sequence))
    }

    fn start_batch(&mut self) -> Result<bool, RuntimeError> {
        if self.inflight.is_some()
            || self.domain.mono_ms().map_err(|_| RuntimeError::Closed)? < self.retry_at
        {
            return Ok(false);
        }
        let mut jobs = Vec::new();
        let mut bytes = 1024;
        for _ in 0..self.writes.len() {
            let Some(history) = self.writes.pop_front() else {
                break;
            };
            let group = self
                .groups
                .get_mut(&history)
                .ok_or(RuntimeError::NotFound)?;
            let Some(work) = group.write.take() else {
                continue;
            };
            let size = std::mem::take(&mut group.write_bytes);
            if size + 1024 > self.log.limits().max_batch_bytes {
                group.fail(RuntimeError::Overloaded);
                // The unsubmitted write is discarded; release its waiting ticket
                // only by retiring this failed owner at node shutdown.
                group.queued = true;
                self.order.push_back(history);
            } else if size + bytes <= self.log.limits().max_batch_bytes {
                bytes += size;
                jobs.push(Job {
                    history,
                    work,
                    bytes: size,
                });
            } else {
                group.write = Some(work);
                group.write_bytes = size;
                self.writes.push_front(history);
                break;
            }
            self.refresh(history);
            if jobs.len() >= self.options.max_batch_groups {
                break;
            }
        }
        if jobs.is_empty() {
            return Ok(false);
        }
        let operation = self.operation()?;
        let log = self.log.clone();
        self.inflight_bytes = bytes;
        self.inflight = Some(Box::pin(async move {
            let writes = jobs.iter().map(|job| job.work.write.clone()).collect();
            let result = log.persist_cells(operation, writes).await;
            (jobs, operation, result)
        }));
        Ok(true)
    }

    fn finish_batch(&mut self, batch: Batch) -> Result<(), RuntimeError> {
        self.inflight = None;
        self.inflight_bytes = 0;
        let (jobs, operation, result) = batch;
        match result {
            Ok(receipt) => {
                if receipt.operation != operation
                    || receipt.groups
                        != jobs
                            .iter()
                            .map(|j| j.work.write.group.identity().clone())
                            .collect::<Vec<_>>()
                {
                    return Err(RuntimeError::StaleCompletion);
                }
                for job in jobs {
                    let group = self
                        .groups
                        .get_mut(&job.history)
                        .ok_or(RuntimeError::NotFound)?;
                    // The physical batch must join even after quarantine. Its
                    // successful sync cannot restore the revoked execution fence.
                    if group.failed.is_some() {
                        continue;
                    }
                    group.owner.complete_persist(CellGroupPersisted {
                        ticket: job.work.ticket,
                        sequence: receipt.sequence,
                    })?;
                    if let Some(LocalParkChange::Close(generation)) = job.work.write.local_park {
                        let campaign = std::mem::take(&mut group.wake_due)
                            || group.deferred.front().is_some_and(|request| {
                                !matches!(
                                    request.command,
                                    Command::Step(_) | Command::Checkpoint(..)
                                )
                            });
                        group.owner.finish_wake(generation, campaign)?;
                        self.ticking.insert(job.history);
                    }
                    if let Some(request) = group.park_write.take() {
                        let result = match &request.command {
                            Command::AcknowledgePark { leader, cut } => group
                                .owner
                                .finish_park_acknowledgement(*leader, cut.clone())
                                .map(Response::ParkAcknowledgement),
                            Command::CertifyPark(certificate) => group
                                .owner
                                .finish_park_certificate(certificate)
                                .map(Response::ParkCertified),
                            Command::EnterPark(certificate) => {
                                match group.owner.finish_park(certificate) {
                                    Ok(()) => {
                                        group.tick = false;
                                        self.ticking.remove(&job.history);
                                        self.park_deadlines
                                            .set(job.history, group.owner.parked_effect_deadline());
                                        Ok(Response::Unit)
                                    }
                                    Err(error) => {
                                        group.fail(error.clone());
                                        Err(error)
                                    }
                                }
                            }
                            _ => Err(RuntimeError::Protocol("park write queue".into())),
                        };
                        reply(request, result);
                    }
                    group.wake(job.history, &mut self.order);
                    self.refresh(job.history);
                }
            }
            Err(LogError::Overloaded) => {
                self.retry_at = self
                    .domain
                    .mono_ms()
                    .map_err(|_| RuntimeError::Closed)?
                    .saturating_add(10);
                for job in jobs.into_iter().rev() {
                    let group = self
                        .groups
                        .get_mut(&job.history)
                        .ok_or(RuntimeError::NotFound)?;
                    if group.failed.is_some() {
                        continue;
                    }
                    group.write = Some(job.work);
                    group.write_bytes = job.bytes;
                    self.writes.push_front(job.history);
                    self.refresh(job.history);
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn flush(&mut self) -> bool {
        let mut made = false;
        while let Some(event) = self.outbox.pop_front() {
            match self.events.try_send(event) {
                Ok(()) => made = true,
                Err(error) => {
                    self.outbox.push_front(error.into_inner());
                    break;
                }
            }
        }
        made
    }

    pub(super) async fn run(
        &mut self,
        mut requests: mailbox::Receiver<Request>,
        closing: &AtomicBool,
        stop: &Notify,
    ) -> Result<(), RuntimeError> {
        let mut tick_at = self
            .domain
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?
            .saturating_add(self.options.tick_interval_ms.unwrap_or(1000));
        loop {
            if closing.load(Ordering::Acquire) {
                return Ok(());
            }
            let wake_at = self.schedule_park_wakes()?;
            let mut made = self.progress()? | self.start_batch()? | self.flush();
            let now = self.domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
            made |= self.cell_reads.progress(
                now,
                &self.domain,
                |history, request, term, barrier, context, retry, cohort| {
                    let group = self
                        .groups
                        .get_mut(&history)
                        .ok_or(RuntimeError::NotFound)?;
                    if let Some(error) = &group.failed {
                        return Err(error.clone());
                    }
                    if let Some(expected) = cohort {
                        let status = group.owner.status()?;
                        if expected.history != history
                            || expected.term != status.term
                            || expected.configuration
                                != group.owner.manifest().marker().membership_revision()
                        {
                            return Err(RuntimeError::NotLeader);
                        }
                    }
                    match &request.kind {
                        reads::ReadKind::Cell(cell, consistency) => {
                            if matches!(consistency, CellReadConsistency::Linearizable)
                                && (term.is_none() || retry)
                            {
                                let status = group.owner.status()?;
                                if term.is_some_and(|observed| observed != status.term) {
                                    return Err(RuntimeError::NotLeader);
                                }
                                if term.is_none() && group.read_collection_remaining != 0 {
                                    return Err(RuntimeError::NotReady);
                                }
                                // Raft already retries a leader's heartbeat.
                                // A follower must resend a lost forwarded request.
                                if term.is_none()
                                    || status.leader != Some(group.owner.membership_read_origin())
                                {
                                    if term.is_none()
                                        && self.reads >= self.options.max_pending_reads
                                    {
                                        return Err(RuntimeError::Overloaded);
                                    }
                                    group.owner.read_forwarded_cell_index(
                                        context.to_vec(),
                                        term.is_none(),
                                    )?;
                                    // Cohorts initiate after the ordinary group
                                    // turn. Account the actual RawNode entry
                                    // before another group's cohort can enter.
                                    let reads = group.owner.usage().2;
                                    self.reads = self.reads - group.usage.2 + reads;
                                    group.usage.2 = reads;
                                    group.wake(history, &mut self.order);
                                    *term = Some(status.term);
                                }
                            }
                            group
                                .owner
                                .select_read(cell, consistency, *term, *barrier)
                                .map(reads::Selection::Cell)
                        }
                        reads::ReadKind::Seed(target) => group
                            .owner
                            .select_seed(target, *term, *barrier)
                            .map(reads::Selection::Seed),
                        reads::ReadKind::Membership { minimum, mode } => {
                            if let reads::MembershipRead::Wake(generation) = mode {
                                group.owner.check_park_wake(*generation)?;
                            }
                            let status = group.owner.status()?;
                            if term.is_some_and(|observed| observed != status.term) {
                                return Err(RuntimeError::NotLeader);
                            }
                            if status.applied.index < *minimum
                                || !group.owner.membership_read_ready()?
                            {
                                return Err(RuntimeError::NotReady);
                            }
                            if barrier.is_some_and(|index| {
                                group.owner.manifest().marker().membership_revision() > index.get()
                            }) {
                                // A configuration overtook this read's barrier.
                                // Re-arm within the same invocation, reservation
                                // and original deadline; never spin a new call.
                                *term = None;
                                *barrier = None;
                            }
                            if term.is_none()
                                || retry
                                    && status.leader != Some(group.owner.membership_read_origin())
                            {
                                group.owner.read_membership_index(context.to_vec())?;
                                *term = Some(status.term);
                                return Err(RuntimeError::NotReady);
                            }
                            group
                                .owner
                                .select_membership(*minimum, *term, *barrier)
                                .map(reads::Selection::Membership)
                        }
                    }
                },
            );
            let deadline = if self.retry_at > now {
                tick_at.min(self.retry_at)
            } else {
                tick_at
            };
            let deadline = self
                .cell_reads
                .deadline()
                .map_or(deadline, |read| deadline.min(read));
            let deadline = wake_at.map_or(deadline, |wake| deadline.min(wake));
            let timer = self
                .domain
                .sleep_until(deadline)
                .map_err(|_| RuntimeError::Closed)?;
            let events = self.events.clone();
            let output = self.output.clone();
            let io = async {
                match self.inflight.as_mut() {
                    Some(io) => io.await,
                    None => std::future::pending().await,
                }
            };
            crate::asyncrt::select! {
                request = requests.recv() => {
                    let Some(request) = request else { return Ok(()); }; self.dispatch(request);
                    for _ in 1..64 { let Ok(request) = requests.try_recv() else { break; }; self.dispatch(request); }
                }
                _ = stop.notified() => return Ok(()),
                result = io => self.finish_batch(result)?,
                _ = timer => { let now=self.domain.mono_ms().map_err(|_| RuntimeError::Closed)?; if now>=tick_at { if self.options.tick_interval_ms.is_some() { self.tick(); } tick_at=now.saturating_add(self.options.tick_interval_ms.unwrap_or(1000)); } }
                _ = events.closed() => return Ok(()),
                capacity = events.reserve(), if !self.outbox.is_empty() => { if capacity.is_err() { return Ok(()); } }
                _ = output.wake.notified() => (),
                _ = std::future::ready(()), if made => (),
            }
        }
    }

    pub(super) async fn finish(&mut self) -> Result<(), RuntimeError> {
        for group in self.groups.values_mut() {
            if let Some(request) = group.park_write.take() {
                reply(request, Err(RuntimeError::Closed));
            }
        }
        if let Some(io) = self.inflight.take() {
            let batch = io.await;
            batch.2.as_ref().map_err(Clone::clone)?;
            self.finish_batch(batch)?;
        }
        let operation = self.operation()?;
        self.log.persist_cells(operation, Vec::new()).await?;
        Ok(())
    }
}
