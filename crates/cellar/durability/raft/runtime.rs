//! One domain-owned consensus worker multiplexes all groups and async log work.

use super::budget::{Budget, Permit};
use super::cache::GroupWrite;
use super::completion::*;
use super::options::RaftRuntimeOptions;
use super::replica::{Operations, Replica, WriteTicket};
use super::storage::{LogError, ReplicaLog, SyncReceipt};
use ::raft::prelude::{ConfChangeV2, Message};
use cellar_logic::consensus::GroupScope;
use futures_util::future::BoxFuture;
use protobuf::Message as _;
use rand::RngCore as _;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

pub(crate) enum GroupAction {
    Campaign,
    Step(Message),
    Propose {
        context: Vec<u8>,
        data: Vec<u8>,
    },
    Configure {
        context: Vec<u8>,
        change: ConfChangeV2,
    },
    ReadIndex(Vec<u8>),
    Unreachable(u64),
    SnapshotStatus {
        member: u64,
        success: bool,
    },
}

pub(crate) enum Response {
    Unit,
    Proposal(AcceptedProposal),
    Status(GroupStatus),
}

type Reply = oneshot::Sender<Result<Response, RuntimeError>>;

enum Command {
    Group {
        scope: GroupScope,
        action: GroupAction,
    },
    Status(GroupScope),
    Attach(GroupScope),
    Apply(ApplyCompletion),
    Snapshot(SnapshotCompletion),
    Tick,
    Close,
}

struct Request {
    command: Command,
    reply: Reply,
    _permit: Option<Permit>,
}

#[derive(Clone)]
pub struct RaftRuntime {
    inner: Arc<RuntimeHandle>,
}

struct RuntimeHandle {
    log: ReplicaLog,
    sender: mpsc::Sender<Request>,
    domain: crate::asyncrt::DomainToken,
    budget: Arc<Budget>,
    options: RaftRuntimeOptions,
    closed: Arc<AtomicBool>,
    closing: Arc<AtomicBool>,
    transport_claimed: AtomicBool,
    terminal: Arc<Mutex<Option<Result<(), RuntimeError>>>>,
}

pub struct RuntimeEvents {
    receiver: mpsc::Receiver<RuntimeEvent>,
}

pub(crate) struct TransportClaim(Arc<RuntimeHandle>);

impl Drop for TransportClaim {
    fn drop(&mut self) {
        self.0.transport_claimed.store(false, Ordering::Release);
    }
}

impl RuntimeEvents {
    pub async fn recv(&mut self) -> Option<RuntimeEvent> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<RuntimeEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

impl RaftRuntime {
    pub(crate) fn claim_transport(&self) -> Result<TransportClaim, RuntimeError> {
        self.admit()?;
        self.inner
            .transport_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| RuntimeError::AlreadyAttached)?;
        Ok(TransportClaim(self.inner.clone()))
    }

    pub(crate) fn log(&self) -> &ReplicaLog {
        &self.inner.log
    }

    pub async fn start(
        log: ReplicaLog,
        options: RaftRuntimeOptions,
    ) -> Result<(Self, RuntimeEvents), RuntimeError> {
        Self::start_with_existing(log, options, true).await
    }

    /// Do not instantiate any recovered RawNodes before the catalog/member
    /// authority has been reconciled. Explicit verified attachment follows.
    pub async fn start_parked(
        log: ReplicaLog,
        options: RaftRuntimeOptions,
    ) -> Result<(Self, RuntimeEvents), RuntimeError> {
        Self::start_with_existing(log, options, false).await
    }

    async fn start_with_existing(
        log: ReplicaLog,
        options: RaftRuntimeOptions,
        attach_existing: bool,
    ) -> Result<(Self, RuntimeEvents), RuntimeError> {
        options.validate(log.limits())?;
        let lease = log.claim_runtime()?;
        let domain = log.domain();
        let (sender, requests) = mpsc::channel(options.max_requests);
        let (events, receiver) = mpsc::channel(options.max_events);
        let (ready, wait) = oneshot::channel();
        let closed = Arc::new(AtomicBool::new(false));
        let closing = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(Mutex::new(None));
        let handle = Self {
            inner: Arc::new(RuntimeHandle {
                log: log.clone(),
                sender,
                domain: domain.clone(),
                budget: Budget::new(options.max_request_bytes, options.max_requests),
                options: options.clone(),
                closed: closed.clone(),
                closing: closing.clone(),
                transport_claimed: AtomicBool::new(false),
                terminal: terminal.clone(),
            }),
        };
        let worker_domain = domain.clone();
        domain
            .spawn_detached("raft-consensus", async move {
                struct ClosedOnDrop(Arc<AtomicBool>);

                impl Drop for ClosedOnDrop {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::Release);
                    }
                }
                let _closed = ClosedOnDrop(closed);
                let mut worker =
                    match Worker::new(log, options, events, worker_domain, attach_existing) {
                        Ok(worker) => worker,
                        Err(error) => {
                            lease.release();
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                if ready.send(Ok(())).is_err() {
                    lease.release();
                    return;
                }
                let (result, close_reply) = worker.run(requests).await;
                closing.store(true, Ordering::Release);
                // A normal restart cannot race a write submitted by the old worker.
                let fence = worker.finish().await;
                let result = result.and(fence);
                if result.is_ok() {
                    lease.release();
                }
                if let Ok(mut terminal) = terminal.lock() {
                    *terminal = Some(result.clone());
                }
                if let Some(reply) = close_reply {
                    let _ = reply.send(result.map(|()| Response::Unit));
                }
            })
            .map_err(|_| RuntimeError::Closed)?;
        wait.await.map_err(|_| RuntimeError::Closed)??;
        handle.admit()?;
        Ok((handle, RuntimeEvents { receiver }))
    }

    pub(crate) fn admit(&self) -> Result<(), RuntimeError> {
        if self.inner.closed.load(Ordering::Acquire)
            || self.inner.closing.load(Ordering::Acquire)
            || self.inner.domain.mono_ms().is_err()
        {
            return Err(RuntimeError::Closed);
        }
        Ok(())
    }

    async fn call(&self, bytes: usize, command: Command) -> Result<Response, RuntimeError> {
        self.admit()?;
        match &command {
            Command::Group { scope, .. } | Command::Status(scope) | Command::Attach(scope) => scope
                .validate()
                .map_err(|e| RuntimeError::Invalid(e.to_string()))?,
            _ => (),
        }
        let permit = self
            .inner
            .budget
            .acquire(bytes)
            .ok_or(RuntimeError::Overloaded)?;
        let (reply, wait) = oneshot::channel();
        self.inner
            .sender
            .try_send(Request {
                command,
                reply,
                _permit: Some(permit),
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
            })?;
        let response = wait.await.map_err(|_| RuntimeError::Closed)?;
        self.inner
            .domain
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?;
        response
    }

    async fn group(
        &self,
        scope: &GroupScope,
        action: GroupAction,
        bytes: usize,
    ) -> Result<Response, RuntimeError> {
        scope
            .validate()
            .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
        self.call(
            bytes + 4096,
            Command::Group {
                scope: scope.clone(),
                action,
            },
        )
        .await
    }

    pub async fn campaign(&self, scope: &GroupScope) -> Result<(), RuntimeError> {
        self.group(scope, GroupAction::Campaign, 0)
            .await
            .map(|_| ())
    }

    pub async fn step(&self, scope: &GroupScope, message: Message) -> Result<(), RuntimeError> {
        let bytes = message.compute_size() as usize;
        if bytes > self.inner.options.max_message_bytes {
            return Err(RuntimeError::Overloaded);
        }
        self.group(scope, GroupAction::Step(message), bytes)
            .await
            .map(|_| ())
    }

    pub async fn propose(
        &self,
        scope: &GroupScope,
        context: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<AcceptedProposal, RuntimeError> {
        let bytes = context
            .len()
            .checked_add(data.len())
            .ok_or(RuntimeError::Overloaded)?;
        if data.is_empty() || bytes > self.inner.options.max_proposal_bytes {
            return Err(RuntimeError::Invalid("invalid proposal size".into()));
        }
        match self
            .group(scope, GroupAction::Propose { context, data }, bytes)
            .await?
        {
            Response::Proposal(p) => Ok(p),
            _ => Err(RuntimeError::Protocol("proposal reply".into())),
        }
    }

    pub async fn configure(
        &self,
        scope: &GroupScope,
        context: Vec<u8>,
        change: ConfChangeV2,
    ) -> Result<AcceptedProposal, RuntimeError> {
        let bytes = context
            .len()
            .checked_add(change.compute_size() as usize)
            .ok_or(RuntimeError::Overloaded)?;
        if bytes > 64 * 1024 {
            return Err(RuntimeError::Overloaded);
        }
        match self
            .group(scope, GroupAction::Configure { context, change }, bytes)
            .await?
        {
            Response::Proposal(p) => Ok(p),
            _ => Err(RuntimeError::Protocol("configuration reply".into())),
        }
    }

    pub async fn read_index(
        &self,
        scope: &GroupScope,
        context: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        if context.is_empty() || context.len() > 4096 {
            return Err(RuntimeError::Invalid("read context size".into()));
        }
        let bytes = context.len();
        self.group(scope, GroupAction::ReadIndex(context), bytes)
            .await
            .map(|_| ())
    }

    pub async fn report_unreachable(
        &self,
        scope: &GroupScope,
        member: u64,
    ) -> Result<(), RuntimeError> {
        self.group(scope, GroupAction::Unreachable(member), 0)
            .await
            .map(|_| ())
    }

    pub async fn report_snapshot(
        &self,
        scope: &GroupScope,
        member: u64,
        success: bool,
    ) -> Result<(), RuntimeError> {
        self.group(scope, GroupAction::SnapshotStatus { member, success }, 0)
            .await
            .map(|_| ())
    }

    pub async fn complete_apply(&self, completed: ApplyCompletion) -> Result<(), RuntimeError> {
        self.call(4096, Command::Apply(completed)).await.map(|_| ())
    }

    pub async fn complete_snapshot(
        &self,
        completed: SnapshotCompletion,
    ) -> Result<(), RuntimeError> {
        self.call(4096, Command::Snapshot(completed))
            .await
            .map(|_| ())
    }

    pub async fn status(&self, scope: &GroupScope) -> Result<GroupStatus, RuntimeError> {
        match self.call(4096, Command::Status(scope.clone())).await? {
            Response::Status(s) => Ok(s),
            _ => Err(RuntimeError::Protocol("status reply".into())),
        }
    }

    pub async fn attach(&self, scope: &GroupScope) -> Result<(), RuntimeError> {
        self.call(4096, Command::Attach(scope.clone()))
            .await
            .map(|_| ())
    }

    pub async fn tick(&self) -> Result<(), RuntimeError> {
        self.call(1024, Command::Tick).await.map(|_| ())
    }

    /// Stop consensus and fence all submitted storage work; not a fleet drain.
    pub async fn close(&self) -> Result<(), RuntimeError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return self
                .inner
                .terminal
                .lock()
                .map_err(|_| RuntimeError::Closed)?
                .clone()
                .unwrap_or(Err(RuntimeError::Closed));
        }
        if self.inner.closing.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::Closed);
        }
        let (reply, wait) = oneshot::channel();
        let mut request = Request {
            command: Command::Close,
            reply,
            _permit: None,
        };
        loop {
            match self.inner.sender.try_send(request) {
                Ok(()) => break,
                Err(mpsc::error::TrySendError::Closed(_)) => return Err(RuntimeError::Closed),
                Err(mpsc::error::TrySendError::Full(unsent)) => {
                    request = unsent;
                    let now = self
                        .inner
                        .domain
                        .mono_ms()
                        .map_err(|_| RuntimeError::Closed)?;
                    self.inner
                        .domain
                        .sleep_until(now.saturating_add(1))
                        .map_err(|_| RuntimeError::Closed)?
                        .await;
                }
            }
        }
        wait.await.map_err(|_| RuntimeError::Closed)?.map(|_| ())
    }
}

pub(crate) struct Outbox {
    events: VecDeque<RuntimeEvent>,
    budget: Arc<Budget>,
}

impl Outbox {
    /// Return the unsent event when admission is exhausted.
    pub fn push(&mut self, kind: RuntimeEventKind) -> Option<RuntimeEventKind> {
        let Some(permit) = self.budget.acquire(kind.bytes()) else {
            return Some(kind);
        };
        self.events.push_back(RuntimeEvent {
            kind,
            _permit: permit,
        });
        None
    }

    fn flush(&mut self, sender: &mpsc::Sender<RuntimeEvent>) -> bool {
        let mut made = false;
        while let Some(event) = self.events.pop_front() {
            match sender.try_send(event) {
                Ok(()) => made = true,
                Err(error) => {
                    self.events.push_front(error.into_inner());
                    break;
                }
            }
        }
        made
    }
}

struct Group {
    replica: Replica,
    deferred: VecDeque<Request>,
    tick: bool,
    failed: Option<RuntimeError>,
    report_failure: bool,
}

struct Job {
    scope: GroupScope,
    ticket: WriteTicket,
    write: GroupWrite,
}

type BatchResult = (
    Vec<Job>,
    Result<SyncReceipt, LogError>,
    cellar_logic::consensus::OperationId,
);

struct Worker {
    log: ReplicaLog,
    domain: crate::asyncrt::DomainToken,
    options: RaftRuntimeOptions,
    groups: BTreeMap<GroupScope, Group>,
    order: VecDeque<GroupScope>,
    operations: Operations,
    outbox: Outbox,
    events: mpsc::Sender<RuntimeEvent>,
    inflight: Option<BoxFuture<'static, BatchResult>>,
    seed: [u8; 32],
    retry_at: u64,
}

impl Worker {
    fn new(
        log: ReplicaLog,
        options: RaftRuntimeOptions,
        events: mpsc::Sender<RuntimeEvent>,
        domain: crate::asyncrt::DomainToken,
        attach_existing: bool,
    ) -> Result<Self, RuntimeError> {
        let mut seed = [0; 32];
        match options.election_seed {
            Some(value) => seed = value,
            None => crate::asyncrt::rng("raft-elections").fill_bytes(&mut seed),
        }
        let outbox = Outbox {
            events: VecDeque::new(),
            budget: Budget::new(options.max_event_bytes, options.max_events),
        };
        let mut worker = Self {
            log,
            domain,
            options,
            groups: BTreeMap::new(),
            order: VecDeque::new(),
            operations: Operations::new(),
            outbox,
            events,
            inflight: None,
            seed,
            retry_at: 0,
        };
        if attach_existing {
            for group in worker.log.groups()? {
                worker.attach(group.identity().scope.clone())?;
            }
        }
        Ok(worker)
    }

    fn attach(&mut self, scope: GroupScope) -> Result<(), RuntimeError> {
        if self.groups.contains_key(&scope) {
            return Err(RuntimeError::AlreadyAttached);
        }
        if self.groups.len() >= self.options.max_groups {
            return Err(RuntimeError::Overloaded);
        }
        let log = self.log.group(&scope)?;
        let mut digest = Sha256::new();
        digest.update(self.seed);
        digest.update(scope.cluster.bytes());
        digest.update(scope.namespace.bytes());
        digest.update(scope.group.bytes());
        digest.update(scope.incarnation.bytes());
        digest.update(log.identity().member.get().to_be_bytes());
        digest.update(log.identity().node.bytes());
        let replica = Replica::new(log, &self.options, digest.finalize().into())?;
        self.groups.insert(
            scope.clone(),
            Group {
                replica,
                deferred: VecDeque::new(),
                tick: false,
                failed: None,
                report_failure: false,
            },
        );
        self.order.push_back(scope);
        Ok(())
    }

    fn fail(group: &mut Group, error: RuntimeError) {
        group.failed = Some(error.clone());
        group.report_failure = true;
        for request in group.deferred.drain(..) {
            let _ = request.reply.send(Err(error.clone()));
        }
    }

    fn dispatch(&mut self, request: Request) -> Option<Reply> {
        if matches!(request.command, Command::Close) {
            return Some(request.reply);
        }
        if let Command::Group { scope, .. } = &request.command {
            match self.groups.get_mut(scope) {
                Some(group) if group.failed.is_none() => group.deferred.push_back(request),
                Some(group) => {
                    let _ = request
                        .reply
                        .send(Err(group.failed.clone().unwrap_or(RuntimeError::Closed)));
                }
                None => {
                    let _ = request.reply.send(Err(RuntimeError::NotFound));
                }
            }
            return None;
        }
        let result = match request.command {
            Command::Status(scope) => self
                .groups
                .get(&scope)
                .ok_or(RuntimeError::NotFound)
                .and_then(|g| match &g.failed {
                    Some(error) => Err(error.clone()),
                    None => g.replica.status().map(Response::Status),
                }),
            Command::Attach(scope) => self.attach(scope).map(|()| Response::Unit),
            Command::Apply(completed) => self
                .groups
                .get_mut(&completed.ticket.replica().scope)
                .ok_or(RuntimeError::NotFound)
                .and_then(|g| match &g.failed {
                    Some(error) => Err(error.clone()),
                    None => g.replica.complete_apply(completed),
                })
                .map(|()| Response::Unit),
            Command::Snapshot(completed) => self
                .groups
                .get_mut(&completed.ticket.replica().scope)
                .ok_or(RuntimeError::NotFound)
                .and_then(|g| match &g.failed {
                    Some(error) => Err(error.clone()),
                    None => g.replica.complete_snapshot(completed),
                })
                .map(|()| Response::Unit),
            Command::Tick => {
                for group in self.groups.values_mut() {
                    group.tick = true;
                }
                Ok(Response::Unit)
            }
            Command::Group { .. } | Command::Close => {
                Err(RuntimeError::Protocol("dispatch state".into()))
            }
        };
        let _ = request.reply.send(result);
        None
    }

    fn progress(&mut self) -> Result<bool, RuntimeError> {
        let mut made = false;
        let mut ready_bytes = self
            .groups
            .values()
            .map(|g| g.replica.ready_bytes)
            .sum::<usize>();
        let mut apply_bytes = self
            .groups
            .values()
            .map(|g| g.replica.apply_bytes)
            .sum::<usize>();
        let mut pending_reads = self
            .groups
            .values()
            .map(|g| g.replica.pending_reads())
            .sum::<usize>();
        for _ in 0..self.order.len() {
            let Some(scope) = self.order.pop_front() else {
                break;
            };
            let group = self
                .groups
                .get_mut(&scope)
                .ok_or_else(|| RuntimeError::Protocol("scheduler identity".into()))?;
            if let Some(error) = &group.failed {
                if group.report_failure
                    && self
                        .outbox
                        .push(RuntimeEventKind::Failed {
                            replica: group.replica.identity().clone(),
                            error: error.clone(),
                        })
                        .is_none()
                {
                    group.report_failure = false;
                    made = true;
                }
                self.order.push_back(scope);
                continue;
            }
            let old_reads = group.replica.pending_reads();
            let old_ready = group.replica.ready_bytes;
            let old_apply = group.replica.apply_bytes;
            if group.replica.idle()
                && !group.replica.has_ready()
                && ready_bytes < self.options.max_ready_bytes
                && apply_bytes < self.options.max_apply_bytes
            {
                if group.tick {
                    if let Err(error) = group.replica.tick() {
                        Self::fail(group, error);
                    }
                    group.tick = false;
                    made = true;
                } else if let Some(request) = group.deferred.pop_front() {
                    let Command::Group { action, .. } = request.command else {
                        return Err(RuntimeError::Protocol("group queue".into()));
                    };
                    let new_read = matches!(&action, GroupAction::ReadIndex(_))
                        || matches!(&action,GroupAction::Step(m) if m.get_msg_type()==::raft::prelude::MessageType::MsgReadIndex);
                    let result = if new_read && pending_reads >= self.options.max_pending_reads {
                        Err(RuntimeError::Overloaded)
                    } else {
                        group.replica.execute(action, &self.options)
                    };
                    let _ = request.reply.send(result);
                    made = true;
                }
            }
            if group.failed.is_some() {
                self.order.push_back(scope);
                continue;
            }
            match group.replica.drive(
                &mut self.operations,
                &mut self.outbox,
                &self.options,
                ready_bytes < self.options.max_ready_bytes
                    && apply_bytes < self.options.max_apply_bytes,
            ) {
                Ok(progress) => made |= progress,
                Err(error) => Self::fail(group, error),
            }
            pending_reads = pending_reads - old_reads + group.replica.pending_reads();
            ready_bytes = ready_bytes - old_ready + group.replica.ready_bytes;
            apply_bytes = apply_bytes - old_apply + group.replica.apply_bytes;
            self.order.push_back(scope);
        }
        self.order.rotate_left(usize::from(!self.order.is_empty()));
        Ok(made)
    }

    fn start_batch(&mut self) -> Result<bool, RuntimeError> {
        if self.inflight.is_some()
            || self.domain.mono_ms().map_err(|_| RuntimeError::Closed)? < self.retry_at
        {
            return Ok(false);
        }
        let mut bytes = 1024;
        let mut jobs = Vec::new();
        for _ in 0..self.order.len() {
            let Some(scope) = self.order.pop_front() else {
                break;
            };
            let group = self.groups.get_mut(&scope).ok_or(RuntimeError::NotFound)?;
            if group.failed.is_none() {
                if let Some((_, write)) = &group.replica.write {
                    let size = write.estimated_bytes()?;
                    if size + 1024 > self.log.limits().max_batch_bytes {
                        Self::fail(group, RuntimeError::Overloaded);
                    } else if bytes + size <= self.log.limits().max_batch_bytes {
                        let (ticket, write) =
                            group.replica.write.take().ok_or(RuntimeError::NotReady)?;
                        bytes += size;
                        jobs.push(Job {
                            scope: scope.clone(),
                            ticket,
                            write,
                        });
                    }
                }
            }
            self.order.push_back(scope);
            if jobs.len() >= self.options.max_batch_groups {
                break;
            }
        }
        if jobs.is_empty() {
            return Ok(false);
        }
        let operation = self.operations.next()?;
        let log = self.log.clone();
        self.inflight = Some(Box::pin(async move {
            let writes = jobs.iter().map(|job| job.write.clone()).collect();
            let result = log.persist(operation, writes).await;
            (jobs, result, operation)
        }));
        Ok(true)
    }

    fn finish_batch(&mut self, completed: BatchResult) -> Result<(), RuntimeError> {
        self.inflight = None;
        let (jobs, result, operation) = completed;
        match result {
            Ok(receipt) => {
                if receipt.operation != operation
                    || receipt.groups
                        != jobs
                            .iter()
                            .map(|job| job.write.group.identity().clone())
                            .collect::<Vec<_>>()
                {
                    return Err(RuntimeError::StaleCompletion);
                }
                for job in jobs {
                    self.groups
                        .get_mut(&job.scope)
                        .ok_or(RuntimeError::NotFound)?
                        .replica
                        .synced(&job.ticket, receipt.sequence)?;
                }
            }
            Err(LogError::Overloaded) => {
                self.retry_at = self
                    .domain
                    .mono_ms()
                    .map_err(|_| RuntimeError::Closed)?
                    .saturating_add(10);
                for job in jobs {
                    self.groups
                        .get_mut(&job.scope)
                        .ok_or(RuntimeError::NotFound)?
                        .replica
                        .write = Some((job.ticket, job.write));
                }
            }
            Err(error @ (LogError::Invalid(_) | LogError::WrongIdentity)) => {
                for job in jobs {
                    if let Some(group) = self.groups.get_mut(&job.scope) {
                        Self::fail(group, error.clone().into());
                    }
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    async fn run(
        &mut self,
        mut requests: mpsc::Receiver<Request>,
    ) -> (Result<(), RuntimeError>, Option<Reply>) {
        let mut tick_at = self
            .domain
            .mono_ms()
            .unwrap_or(0)
            .saturating_add(self.options.tick_interval_ms.unwrap_or(1000));
        loop {
            let made = match self
                .progress()
                .and_then(|made| self.start_batch().map(|started| made | started))
            {
                Ok(made) => made,
                Err(error) => return (Err(error), None),
            } | self.outbox.flush(&self.events);
            let now = match self.domain.mono_ms() {
                Ok(now) => now,
                Err(_) => return (Err(RuntimeError::Closed), None),
            };
            let deadline = if self.retry_at > now {
                tick_at.min(self.retry_at)
            } else {
                tick_at
            };
            let timer = match self.domain.sleep_until(deadline) {
                Ok(timer) => timer,
                Err(_) => return (Err(RuntimeError::Closed), None),
            };
            let sender = self.events.clone();
            let budget = self.outbox.budget.clone();
            let io = async {
                match self.inflight.as_mut() {
                    Some(io) => io.await,
                    None => std::future::pending().await,
                }
            };
            crate::asyncrt::select! {
                request=requests.recv()=> { match request { Some(request)=>if let Some(reply)=self.dispatch(request) { return (Ok(()),Some(reply)); },None=>return (Ok(()),None) } }
                result=io=> { if let Err(error)=self.finish_batch(result) { return (Err(error),None); } }
                _=timer=> { let now=self.domain.mono_ms().unwrap_or(0); if now>=tick_at { if self.options.tick_interval_ms.is_some() { for group in self.groups.values_mut() { group.tick=true; } } tick_at=now.saturating_add(self.options.tick_interval_ms.unwrap_or(1000)); } }
                _=sender.closed()=>return (Ok(()),None),
                capacity=sender.reserve(),if !self.outbox.events.is_empty()=> { if capacity.is_err() { return (Ok(()),None); } }
                _=budget.wake.notified()=>{},
                _=std::future::ready(()),if made=>{},
            }
        }
    }

    async fn finish(&mut self) -> Result<(), RuntimeError> {
        if let Some(io) = self.inflight.take() {
            let (_, result, _) = io.await;
            result?;
        }
        let operation = self.operations.next()?;
        self.log.persist(operation, Vec::new()).await?;
        Ok(())
    }
}
