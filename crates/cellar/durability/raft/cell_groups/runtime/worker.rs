// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fair group turns, node-wide persistence batches and joined shutdown.

use super::*;
use crate::raft::storage::{LogError, SyncReceipt};
use cellar_logic::consensus::OperationId;
use futures_util::future::BoxFuture;
use std::collections::{BTreeMap, VecDeque};

struct Group {
    owner: Box<CellGroupOwner>,
    queued: bool,
    deferred: VecDeque<Request>,
    completion: Option<Request>,
    inbound: Arc<Budget>,
    tick: bool,
    write: Option<Box<CellGroupPersist>>,
    write_bytes: usize,
    pending: Option<Result<CellGroupEffect, RuntimeError>>,
    settled: Option<CellGroupEffect>,
    failed: Option<RuntimeError>,
    usage: (usize, usize, usize),
}

impl Group {
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
        for request in self.deferred.drain(..) {
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
        Self {
            log,
            output: Budget::new(options.max_event_bytes, options.max_events),
            proposals: Budget::new(options.max_request_bytes, options.max_requests),
            cell_reads: reads::Reads::new(&options),
            options,
            domain,
            groups: BTreeMap::new(),
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
        let usage = group.usage();
        self.ready = self.ready - group.usage.0 + usage.0;
        self.apply = self.apply - group.usage.1 + usage.1;
        self.reads = self.reads - group.usage.2 + usage.2;
        group.usage = usage;
    }

    fn dispatch(&mut self, mut request: Request) {
        if matches!(request.command, Command::Tick) {
            self.tick();
            reply(request, Ok(Response::Unit));
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
                deferred: VecDeque::new(),
                completion: None,
                inbound: Budget::new(
                    (self.options.max_request_bytes / 4).max(self.options.max_message_bytes + 4096),
                    (self.options.max_requests / 8).clamp(1, 32),
                ),
                tick: false,
                write: None,
                write_bytes: 0,
                pending: None,
                settled: None,
                failed: None,
                usage: (0, 0, 0),
            };
            group.wake(history, &mut self.order);
            self.groups.insert(history, group);
            self.refresh(history);
            drop(_permit);
            let _ = answer.send(Ok(Response::Unit));
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
        if matches!(request.command, Command::Status) {
            reply(request, group.owner.status().map(Response::Status));
            return;
        }
        if let Command::AdmitStep(message) = &request.command {
            if let Err(error) = group.owner.validate_message(message) {
                reply(request, Err(error));
                return;
            }
            let Some(inbound) = group.inbound.acquire(request.bytes) else {
                reply(request, Err(RuntimeError::Overloaded));
                return;
            };
            let Request {
                command: Command::AdmitStep(message),
                reply: answer,
                bytes,
                _permit,
                ..
            } = request
            else {
                unreachable!()
            };
            let (reply, _) = oneshot::channel();
            group.deferred.push_back(Request {
                group: Some(history),
                command: Command::Step(message),
                reply,
                bytes,
                _permit,
                inbound: Some(inbound),
            });
            group.wake(history, &mut self.order);
            let _ = answer.send(Ok(Response::Unit));
            return;
        }
        if matches!(request.command, Command::Apply(..)) {
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

    fn tick(&mut self) {
        for (history, group) in &mut self.groups {
            if group.failed.is_none() {
                group.tick = true;
                group.wake(*history, &mut self.order);
            }
        }
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
            if group.failed.is_none() {
                if let Some(request) = group.completion.take() {
                    let Command::Apply(ticket, next) = &request.command else {
                        return Err(RuntimeError::Protocol("completion queue".into()));
                    };
                    let result = group.owner.complete_apply(ticket.clone(), (**next).clone());
                    if matches!(result, Err(RuntimeError::NotReady)) {
                        group.completion = Some(request);
                    } else {
                        reply(request, result.map(|()| Response::Unit));
                        made = true;
                    }
                }
                if collect && group.owner.idle() && group.pending.is_none() {
                    if group.tick {
                        match group.owner.tick() {
                            Ok(()) => {
                                group.tick = false;
                                made = true;
                            }
                            Err(RuntimeError::NotReady) => (),
                            Err(error) => group.fail(error),
                        }
                    } else if let Some(request) = group.deferred.pop_front() {
                        let Request {
                            command,
                            reply: answer,
                            _permit,
                            inbound,
                            ..
                        } = request;
                        let mut answer = Some(answer);
                        let new_read = matches!(command, Command::ReadIndex(_))
                            || matches!(&command, Command::Read(request) if matches!(request.consistency, CellReadConsistency::Linearizable))
                            || matches!(&command, Command::Step(m) if m.get_msg_type()==::raft::prelude::MessageType::MsgReadIndex);
                        let result = if new_read && self.reads >= self.options.max_pending_reads {
                            Err(RuntimeError::Overloaded)
                        } else {
                            match command {
                                Command::Read(request) => {
                                    let now =
                                        self.domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
                                    self.cell_reads.start(
                                        history,
                                        &mut group.owner,
                                        *request,
                                        answer.take().expect("read reply"),
                                        now,
                                    );
                                    Ok(Response::Unit)
                                }
                                Command::Execution(cell) => group
                                    .owner
                                    .execution_authority(&cell)
                                    .map(|authority| Response::Execution(Box::new(authority))),
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
                                Command::Campaign => {
                                    group.owner.campaign().map(|()| Response::Unit)
                                }
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
            let mut made = self.progress()? | self.start_batch()? | self.flush();
            let now = self.domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
            made |= self
                .cell_reads
                .progress(now, |history, request, term, barrier| {
                    let group = self.groups.get(&history).ok_or(RuntimeError::NotFound)?;
                    if let Some(error) = &group.failed {
                        return Err(error.clone());
                    }
                    group
                        .owner
                        .select_read(&request.cell, &request.consistency, term, barrier)
                });
            let deadline = if self.retry_at > now {
                tick_at.min(self.retry_at)
            } else {
                tick_at
            };
            let deadline = self
                .cell_reads
                .deadline()
                .map_or(deadline, |read| deadline.min(read));
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
