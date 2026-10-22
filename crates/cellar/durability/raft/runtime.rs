//! One domain-owned consensus worker multiplexes all groups and async log work.

use super::budget::{Budget, Permit};
use super::cache::GroupWrite;
use super::completion::*;
use super::mailbox;
use super::options::RaftRuntimeOptions;
use super::replica::{Operations, Replica, WriteTicket};
use super::snapshot::SnapshotCheckpoint;
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
use tokio::sync::{mpsc, oneshot, Notify};

#[cfg(test)]
#[path = "runtime_scheduler_tests.rs"]
mod scheduler_tests;

pub(crate) enum GroupAction {
    Campaign,
    ReplicationStatus,
    TransferLeadership(LeadershipTransfer),
    Checkpoint(Box<SnapshotCheckpoint>),
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
    Replication(ReplicationStatus),
}

type Reply = oneshot::Sender<Result<Response, RuntimeError>>;

// Physical work must be able to settle even when deferred peer/client requests
// fill ordinary admission. These small completions contain markers, not images.
const COMPLETION_SLOTS: usize = 8;

// Snapshot references now carry a bounded committed member ledger in addition
// to their image/ticket metadata. Reserve every physical completion slot for it.
const SNAPSHOT_COMPLETION_BYTES: usize = 16 * 1024;
const COMPLETION_BYTES: usize = COMPLETION_SLOTS * SNAPSHOT_COMPLETION_BYTES;
const HINT_SLOTS: usize = 256;
const HINT_BYTES: usize = HINT_SLOTS * 4096;

enum Command {
    Group {
        scope: GroupScope,
        action: GroupAction,
    },
    AdmitStep {
        scope: GroupScope,
        message: Message,
    },
    Hint {
        scope: GroupScope,
        member: u64,
        snapshot: Option<bool>,
    },
    Status(GroupScope),
    Attach(GroupScope),
    Apply(ApplyCompletion),
    Snapshot(Box<SnapshotCompletion>),
    AbortSnapshot(ApplyTicket),
    Checkpoint(Box<SnapshotCheckpoint>),
    Tick,
}

struct Request {
    command: Command,
    reply: Reply,
    _permit: Option<Permit>,
    _inbound: Option<Permit>,
}

#[derive(Clone)]
pub struct RaftRuntime {
    inner: Arc<RuntimeHandle>,
}

struct RuntimeHandle {
    log: ReplicaLog,
    sender: mailbox::Sender<Request>,
    domain: crate::asyncrt::DomainToken,
    budget: Arc<Budget>,
    completions: Arc<Budget>,
    hints: Arc<Budget>,
    options: RaftRuntimeOptions,
    closed: Arc<AtomicBool>,
    closing: Arc<AtomicBool>,
    transport_claimed: AtomicBool,
    terminal: Arc<Terminal>,
    stop: Arc<Notify>,
}

// Close runtime admission before the mailbox joins receiver teardown.
struct Requests {
    receiver: mailbox::Receiver<Request>,
    closing: Arc<AtomicBool>,
}

impl Drop for Requests {
    fn drop(&mut self) {
        self.closing.store(true, Ordering::Release);
    }
}

struct Terminal {
    result: Mutex<Option<Result<(), RuntimeError>>>,
    changed: Notify,
}

impl Terminal {
    fn complete(&self, result: Result<(), RuntimeError>) {
        let mut terminal = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if terminal.is_none() {
            *terminal = Some(result);
        }
        drop(terminal);
        self.changed.notify_waiters();
    }

    async fn wait(&self) -> Result<(), RuntimeError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let result = self
                .result
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(result) = result {
                return result;
            }
            changed.await;
        }
    }
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
        let (sender, requests) =
            mailbox::channel(options.max_requests + COMPLETION_SLOTS + HINT_SLOTS);
        let (events, receiver) = mpsc::channel(options.max_events);
        let (ready, wait) = oneshot::channel();
        let closed = Arc::new(AtomicBool::new(false));
        let closing = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(Terminal {
            result: Mutex::new(None),
            changed: Notify::new(),
        });
        let stop = Arc::new(Notify::new());
        let requests = Requests {
            receiver: requests,
            closing: closing.clone(),
        };
        let handle = Self {
            inner: Arc::new(RuntimeHandle {
                log: log.clone(),
                sender,
                domain: domain.clone(),
                budget: Budget::new(options.max_request_bytes, options.max_requests),
                completions: Budget::new(COMPLETION_BYTES, COMPLETION_SLOTS),
                hints: Budget::new(HINT_BYTES, HINT_SLOTS),
                options: options.clone(),
                closed: closed.clone(),
                closing: closing.clone(),
                transport_claimed: AtomicBool::new(false),
                terminal: terminal.clone(),
                stop: stop.clone(),
            }),
        };
        let worker_domain = domain.clone();
        domain
            .spawn_detached("raft-consensus", async move {
                struct ClosedOnDrop(Arc<AtomicBool>, Arc<Terminal>);

                impl Drop for ClosedOnDrop {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::Release);
                        self.1.complete(Err(RuntimeError::Closed));
                    }
                }
                let _closed = ClosedOnDrop(closed, terminal.clone());
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
                let result = worker.run(requests, &closing, &stop).await;
                closing.store(true, Ordering::Release);
                // A normal restart cannot race a write submitted by the old worker.
                let fence = worker.finish().await;
                let result = result.and(fence);
                // Pending replies and permits belong to the worker, including
                // deferred group actions. Completion means they have all dropped.
                drop(worker);
                if result.is_ok() {
                    lease.release();
                }
                terminal.complete(result);
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
            Command::Group { scope, .. }
            | Command::AdmitStep { scope, .. }
            | Command::Hint { scope, .. }
            | Command::Status(scope)
            | Command::Attach(scope) => scope
                .validate()
                .map_err(|e| RuntimeError::Invalid(e.to_string()))?,
            _ => (),
        }
        let budget = if matches!(
            &command,
            Command::Apply(_) | Command::Snapshot(_) | Command::AbortSnapshot(_)
        ) {
            &self.inner.completions
        } else if matches!(&command, Command::Hint { .. }) {
            &self.inner.hints
        } else {
            &self.inner.budget
        };
        let permit = budget.acquire(bytes).ok_or(RuntimeError::Overloaded)?;
        let (reply, wait) = oneshot::channel();
        self.inner
            .sender
            .try_send(Request {
                command,
                reply,
                _permit: Some(permit),
                _inbound: None,
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

    /// Admit authenticated peer traffic without waiting behind this group's
    /// Ready/install work. Both node and group reservations remain charged until
    /// execution or discard. This is no protocol, persistence or install proof.
    pub(crate) async fn admit_step(
        &self,
        scope: &GroupScope,
        message: Message,
    ) -> Result<(), RuntimeError> {
        let bytes = message.compute_size() as usize;
        if bytes > self.inner.options.max_message_bytes {
            return Err(RuntimeError::Overloaded);
        }
        self.call(
            bytes + 4096,
            Command::AdmitStep {
                scope: scope.clone(),
                message,
            },
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn admit_unreachable(
        &self,
        scope: &GroupScope,
        member: u64,
    ) -> Result<(), RuntimeError> {
        self.admit_hint(scope, member, None).await
    }

    pub(crate) async fn admit_snapshot_status(
        &self,
        scope: &GroupScope,
        member: u64,
        success: bool,
    ) -> Result<(), RuntimeError> {
        self.admit_hint(scope, member, Some(success)).await
    }

    async fn admit_hint(
        &self,
        scope: &GroupScope,
        member: u64,
        snapshot: Option<bool>,
    ) -> Result<(), RuntimeError> {
        loop {
            let result = self
                .call(
                    4096,
                    Command::Hint {
                        scope: scope.clone(),
                        member,
                        snapshot,
                    },
                )
                .await;
            if !matches!(result, Err(RuntimeError::Overloaded)) {
                return result.map(|_| ());
            }
            // The transport owns cancellation/deadline. Retry queue contention,
            // never a group's Ready/install work, on its execution-domain clock.
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

    /// Bind `change.context` with the group's observed membership before this
    /// call. The separate `context` argument remains proposal correlation data.
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
        self.call(
            SNAPSHOT_COMPLETION_BYTES,
            Command::Snapshot(Box::new(completed)),
        )
        .await
        .map(|_| ())
    }

    /// Cancel before install authorization. Admission does not prove completion:
    /// `SnapshotAborted` follows required term/vote and older apply persistence.
    /// The older physical apply keeps its existing ticket until it completes.
    /// After `complete_snapshot` is accepted, cancellation is refused; a stale
    /// completion error does not prove install failure. Join its final outcome.
    pub async fn abort_snapshot(&self, ticket: ApplyTicket) -> Result<(), RuntimeError> {
        self.call(4096, Command::AbortSnapshot(ticket))
            .await
            .map(|_| ())
    }

    /// Admit a verified local checkpoint into the serialized persistence path.
    /// Success is admission only; `Checkpointed` proves that sync and retained
    /// reference handoff have completed. No encoded image enters this queue.
    pub async fn checkpoint(&self, checkpoint: SnapshotCheckpoint) -> Result<(), RuntimeError> {
        let bytes = checkpoint
            .bytes()
            .checked_add(4096)
            .ok_or(RuntimeError::Overloaded)?;
        if bytes > 68 * 1024 {
            return Err(RuntimeError::Overloaded);
        }
        self.call(bytes, Command::Checkpoint(Box::new(checkpoint)))
            .await
            .map(|_| ())
    }

    pub async fn status(&self, scope: &GroupScope) -> Result<GroupStatus, RuntimeError> {
        match self.call(4096, Command::Status(scope.clone())).await? {
            Response::Status(s) => Ok(s),
            _ => Err(RuntimeError::Protocol("status reply".into())),
        }
    }

    pub async fn replication_status(
        &self,
        scope: &GroupScope,
    ) -> Result<ReplicationStatus, RuntimeError> {
        match self
            .group(scope, GroupAction::ReplicationStatus, 4096)
            .await?
        {
            Response::Replication(status) => Ok(status),
            _ => Err(RuntimeError::Protocol("replication status reply".into())),
        }
    }

    /// Request transfer only from the expected current leader/configuration to
    /// a caught-up voter. A lost response does not prove no transfer occurred.
    pub async fn transfer_leadership(
        &self,
        scope: &GroupScope,
        transfer: LeadershipTransfer,
    ) -> Result<(), RuntimeError> {
        self.group(scope, GroupAction::TransferLeadership(transfer), 4096)
            .await
            .map(|_| ())
    }

    pub async fn attach(&self, scope: &GroupScope) -> Result<(), RuntimeError> {
        self.call(4096, Command::Attach(scope.clone()))
            .await
            .map(|_| ())
    }

    pub async fn tick(&self) -> Result<(), RuntimeError> {
        self.call(1024, Command::Tick).await.map(|_| ())
    }

    /// Stop consensus, fence submitted storage work and join the worker.
    /// Concurrent/repeated waiters share the terminal result; cancelling a
    /// waiter never abandons shutdown. This is not a fleet drain.
    pub async fn close(&self) -> Result<(), RuntimeError> {
        self.inner.closing.store(true, Ordering::Release);
        self.inner.stop.notify_one();
        self.inner.terminal.wait().await
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
            _permit: std::sync::Arc::new(permit),
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
    scheduled: bool,
    write_scheduled: bool,
    deferred: VecDeque<Request>,
    inbound: Arc<Budget>,
    hints: BTreeMap<u64, PeerHint>,
    tick: bool,
    failed: Option<RuntimeError>,
    report_failure: bool,
}

#[derive(Default)]
struct PeerHint {
    unreachable: bool,
    snapshot: Option<bool>,
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
    // One slot per group with a pending durable write. Consensus traffic must
    // not reorder persistence waiters or repeatedly scan idle/applying groups.
    writes: VecDeque<GroupScope>,
    ready_bytes: usize,
    apply_bytes: usize,
    pending_reads: usize,
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
            writes: VecDeque::new(),
            ready_bytes: 0,
            apply_bytes: 0,
            pending_reads: 0,
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
                scheduled: true,
                write_scheduled: false,
                deferred: VecDeque::new(),
                inbound: Budget::new(
                    (self.options.max_request_bytes / 4).max(self.options.max_message_bytes + 4096),
                    (self.options.max_requests / 8).clamp(1, 32),
                ),
                hints: BTreeMap::new(),
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
        group.hints.clear();
        for request in group.deferred.drain(..) {
            let _ = request.reply.send(Err(error.clone()));
        }
    }

    fn dispatch(&mut self, request: Request) {
        // Status is observational. Other scoped requests may reactivate a
        // parked replica, but never enqueue a second slot for the same group.
        let scope = match &request.command {
            Command::Group { scope, .. }
            | Command::AdmitStep { scope, .. }
            | Command::Hint { scope, .. } => Some(scope),
            Command::Apply(completed) => Some(&completed.ticket.replica().scope),
            Command::Snapshot(completed) => Some(&completed.ticket.replica().scope),
            Command::AbortSnapshot(ticket) => Some(&ticket.replica().scope),
            Command::Checkpoint(checkpoint) => Some(checkpoint.scope()),
            _ => None,
        };
        if let Some(scope) = scope {
            if let Some(group) = self.groups.get_mut(scope) {
                if !group.scheduled {
                    group.scheduled = true;
                    self.order.push_back(scope.clone());
                }
            }
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
            return;
        }
        let result = match request.command {
            Command::Hint {
                scope,
                member,
                snapshot,
            } => self
                .groups
                .get_mut(&scope)
                .ok_or(RuntimeError::NotFound)
                .and_then(|group| {
                    if let Some(error) = &group.failed {
                        return Err(error.clone());
                    }
                    if !group.replica.accepts_peer(member) {
                        return Err(RuntimeError::Invalid("unknown transport peer".into()));
                    }
                    let replica = &group.replica;
                    group
                        .hints
                        .retain(|member, _| replica.accepts_peer(*member));
                    // One coalesced record per current member, independent of
                    // deferred body capacity. Preserve failure until execution;
                    // the latest ordered snapshot result replaces an older one.
                    let hint = group.hints.entry(member).or_default();
                    if let Some(success) = snapshot {
                        hint.snapshot = Some(success);
                    } else {
                        hint.unreachable = true;
                    }
                    Ok(Response::Unit)
                }),
            Command::AdmitStep { scope, message } => self
                .groups
                .get_mut(&scope)
                .ok_or(RuntimeError::NotFound)
                .and_then(|group| {
                    if let Some(error) = &group.failed {
                        return Err(error.clone());
                    }
                    group.replica.validate_message(&message, &self.options)?;
                    let bytes = message.compute_size() as usize;
                    let inbound = group
                        .inbound
                        .acquire(bytes + 4096)
                        .ok_or(RuntimeError::Overloaded)?;
                    // Execution revalidates against the then-current Raft
                    // membership. Its result is not a frame admission reply.
                    let (reply, _wait) = oneshot::channel();
                    group.deferred.push_back(Request {
                        command: Command::Group {
                            scope,
                            action: GroupAction::Step(message),
                        },
                        reply,
                        _permit: request._permit,
                        _inbound: Some(inbound),
                    });
                    Ok(Response::Unit)
                }),
            Command::Checkpoint(checkpoint) => {
                if checkpoint.bytes() + 4096
                    > self
                        .options
                        .max_ready_bytes
                        .saturating_sub(self.ready_bytes)
                {
                    Err(RuntimeError::Overloaded)
                } else {
                    self.groups
                        .get_mut(checkpoint.scope())
                        .ok_or(RuntimeError::NotFound)
                        .and_then(|group| {
                            let before = group.replica.ready_bytes;
                            let result = match &group.failed {
                                Some(error) => Err(error.clone()),
                                None => group
                                    .replica
                                    .execute(GroupAction::Checkpoint(checkpoint), &self.options),
                            };
                            self.ready_bytes =
                                self.ready_bytes - before + group.replica.ready_bytes;
                            result
                        })
                }
            }
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
                    None => g.replica.complete_snapshot(*completed),
                })
                .map(|()| Response::Unit),
            Command::AbortSnapshot(ticket) => self
                .groups
                .get_mut(&ticket.replica().scope)
                .ok_or(RuntimeError::NotFound)
                .and_then(|group| match &group.failed {
                    Some(error) => Err(error.clone()),
                    None => group.replica.abort_snapshot(ticket),
                })
                .map(|()| Response::Unit),
            Command::Tick => {
                self.tick();
                Ok(Response::Unit)
            }
            Command::Group { .. } => Err(RuntimeError::Protocol("dispatch state".into())),
        };
        let _ = request.reply.send(result);
    }

    fn tick(&mut self) {
        for (scope, group) in &mut self.groups {
            group.tick = true;
            if !group.scheduled && group.failed.is_none() {
                group.scheduled = true;
                self.order.push_back(scope.clone());
            }
        }
    }

    fn progress(&mut self) -> Result<bool, RuntimeError> {
        let mut made = false;
        let mut ready_bytes = self.ready_bytes;
        let mut apply_bytes = self.apply_bytes;
        let mut pending_reads = self.pending_reads;
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
                group.scheduled = group.report_failure;
                if group.scheduled {
                    self.order.push_back(scope);
                }
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
                } else if let Some((member, hint)) = group.hints.pop_first() {
                    if hint.unreachable {
                        let _ = group
                            .replica
                            .execute(GroupAction::Unreachable(member), &self.options);
                    }
                    if let Some(success) = hint.snapshot {
                        let _ = group.replica.execute(
                            GroupAction::SnapshotStatus { member, success },
                            &self.options,
                        );
                    }
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
            if group.failed.is_none() && group.replica.write.is_some() && !group.write_scheduled {
                group.write_scheduled = true;
                self.writes.push_back(scope.clone());
            }
            group.scheduled = group.report_failure
                || (group.failed.is_none()
                    && (group.tick
                        || !group.hints.is_empty()
                        || !group.deferred.is_empty()
                        || group.replica.has_work()));
            if group.scheduled {
                self.order.push_back(scope);
            }
        }
        self.ready_bytes = ready_bytes;
        self.apply_bytes = apply_bytes;
        self.pending_reads = pending_reads;
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
        while let Some(scope) = self.writes.pop_front() {
            let group = self.groups.get_mut(&scope).ok_or(RuntimeError::NotFound)?;
            group.write_scheduled = false;
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
                    } else {
                        // Keep the first waiter for the next batch. Skipping a
                        // large write lets a stream of small writes bypass it
                        // indefinitely, despite round-robin consensus turns.
                        group.write_scheduled = true;
                        self.writes.push_front(scope);
                        break;
                    }
                }
            }
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
                for job in jobs.into_iter().rev() {
                    let group = self
                        .groups
                        .get_mut(&job.scope)
                        .ok_or(RuntimeError::NotFound)?;
                    group.replica.write = Some((job.ticket, job.write));
                    group.write_scheduled = true;
                    self.writes.push_front(job.scope);
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
        mut requests: Requests,
        closing: &AtomicBool,
        stop: &Notify,
    ) -> Result<(), RuntimeError> {
        let mut tick_at = self
            .domain
            .mono_ms()
            .unwrap_or(0)
            .saturating_add(self.options.tick_interval_ms.unwrap_or(1000));
        loop {
            if closing.load(Ordering::Acquire) {
                return Ok(());
            }
            let made = self
                .progress()
                .and_then(|made| self.start_batch().map(|started| made | started))?
                | self.outbox.flush(&self.events);
            let now = match self.domain.mono_ms() {
                Ok(now) => now,
                Err(_) => return Err(RuntimeError::Closed),
            };
            let deadline = if self.retry_at > now {
                tick_at.min(self.retry_at)
            } else {
                tick_at
            };
            let timer = match self.domain.sleep_until(deadline) {
                Ok(timer) => timer,
                Err(_) => return Err(RuntimeError::Closed),
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
                request=requests.receiver.recv()=> { match request { Some(request)=>self.dispatch(request),None=>return Ok(()) } }
                _=stop.notified()=>return Ok(()),
                result=io=> { self.finish_batch(result)?; }
                _=timer=> { let now=self.domain.mono_ms().unwrap_or(0); if now>=tick_at { if self.options.tick_interval_ms.is_some() { self.tick(); } tick_at=now.saturating_add(self.options.tick_interval_ms.unwrap_or(1000)); } }
                _=sender.closed()=>return Ok(()),
                capacity=sender.reserve(),if !self.outbox.events.is_empty()=> { if capacity.is_err() { return Ok(()); } }
                _=budget.wake.notified()=>{},
                _=std::future::ready(()),if made=>{},
            }
        }
    }

    async fn finish(&mut self) -> Result<(), RuntimeError> {
        if let Some(io) = self.inflight.take() {
            let completed = io.await;
            completed.1.as_ref().map_err(Clone::clone)?;
            // Closing must settle verified image references just as the normal
            // completion path does, even when no event consumer remains.
            self.finish_batch(completed)?;
        }
        let operation = self.operations.next()?;
        self.log.persist(operation, Vec::new()).await?;
        Ok(())
    }
}
