// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One execution-domain worker multiplexes physical cell-group owners per node.

pub(super) mod reads;
mod worker;

use super::*;
use crate::raft::{
    budget::{Budget, Permit},
    completion::RuntimeError,
    mailbox,
    options::RaftRuntimeOptions,
    runtime::Terminal,
    storage::ReplicaLog,
};
use ::raft::prelude::Message;
use cellar_logic::cell_group::GroupHistory;
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{mpsc, oneshot, Notify};

const COMPLETION_SLOTS: usize = 8;

pub struct CellGroupRuntimeEvent {
    group: GroupHistory,
    effect: Result<CellGroupEffect, RuntimeError>,
    _permit: Arc<Permit>,
}

impl CellGroupRuntimeEvent {
    pub fn group(&self) -> GroupHistory {
        self.group
    }

    pub fn effect(&self) -> Result<&CellGroupEffect, &RuntimeError> {
        self.effect.as_ref()
    }

    /// The cloned ticket retains event admission through physical preparation
    /// and publication, even if the receiving task drops this event.
    pub fn retain_apply(&self) -> Option<CellGroupApply> {
        match &self.effect {
            Ok(CellGroupEffect::Apply(work)) => Some((**work).clone()),
            _ => None,
        }
    }
}

pub struct CellGroupRuntimeEvents {
    receiver: mpsc::Receiver<CellGroupRuntimeEvent>,
    owner: std::sync::Weak<Handle>,
}

impl CellGroupRuntimeEvents {
    pub(crate) fn belongs_to(&self, runtime: &CellGroupRuntime) -> bool {
        self.owner.ptr_eq(&Arc::downgrade(&runtime.inner))
    }

    pub async fn recv(&mut self) -> Option<CellGroupRuntimeEvent> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<CellGroupRuntimeEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

#[derive(Clone)]
pub struct CellGroupRuntime {
    inner: Arc<Handle>,
}

struct Handle {
    log: ReplicaLog,
    domain: crate::asyncrt::DomainToken,
    options: RaftRuntimeOptions,
    requests: mailbox::Sender<Request>,
    budget: Arc<Budget>,
    completions: Arc<Budget>,
    read_pins: Arc<Budget>,
    transport_claimed: AtomicBool,
    closing: Arc<AtomicBool>,
    terminal: Arc<Terminal>,
    stop: Arc<Notify>,
}

enum Command {
    Attach(Box<CellGroupOwner>),
    Quarantine,
    Campaign,
    Execution(cellar_logic::cell_group::CellHistory),
    Enrollment(cellar_logic::cell_group::CellHistory),
    EnrollmentState(cellar_logic::cell_group::CellHistory),
    Reconciliation(cellar_logic::cell_group::CellHistory),
    Step(Message),
    Checkpoint(Message, PreparedCellGroupCheckpoint),
    AdmitStep(Message),
    Propose(Vec<u8>),
    ProposeAtTerm(cellar_logic::consensus::Term, Vec<u8>),
    Submit(
        crate::raft::execution::CellPreparedCommand,
        Option<Arc<dyn Send + Sync>>,
    ),
    ReadIndex(Vec<u8>),
    Unreachable(u64),
    Read(Box<reads::ReadRequest>),
    Apply(CellGroupTicket, Box<PreparedGroupManifest>),
    Status,
    Tick,
}

enum Response {
    Unit,
    Checkpoint(CellGroupSnapshotAdmission),
    Proposal(CellGroupProposal),
    Submission(crate::raft::execution::CellSubmissionOutcome),
    Execution(Box<CellExecutionAuthority>),
    Enrollment(Box<CellEnrollmentAuthority>),
    EnrollmentState(Box<CellEnrollmentState>),
    Reconciliation(Box<CellReconciliationAuthority>),
    Status(CellGroupStatus),
    Read(Box<CellReadAuthority>),
}

struct Request {
    group: Option<GroupHistory>,
    command: Command,
    reply: oneshot::Sender<Result<Response, RuntimeError>>,
    bytes: usize,
    _permit: Permit,
    inbound: Option<Permit>,
}

pub(crate) struct TransportClaim(Arc<Handle>);

impl Drop for TransportClaim {
    fn drop(&mut self) {
        self.0.transport_claimed.store(false, Ordering::Release);
    }
}

impl CellGroupRuntime {
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn log(&self) -> &ReplicaLog {
        &self.inner.log
    }

    pub(crate) fn claim_transport(&self) -> Result<TransportClaim, RuntimeError> {
        self.admit()?;
        self.inner
            .transport_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| RuntimeError::AlreadyAttached)?;
        Ok(TransportClaim(self.inner.clone()))
    }

    pub(crate) fn uses_domain(&self, domain: &crate::asyncrt::DomainToken) -> bool {
        self.inner.domain.same_owner(domain)
    }

    pub async fn submit(
        &self,
        prepared: crate::raft::execution::CellPreparedCommand,
    ) -> Result<crate::raft::execution::CellSubmissionOutcome, RuntimeError> {
        self.submit_retained(prepared, None).await
    }

    pub(crate) async fn submit_retained(
        &self,
        prepared: crate::raft::execution::CellPreparedCommand,
        retained: Option<Arc<dyn Send + Sync>>,
    ) -> Result<crate::raft::execution::CellSubmissionOutcome, RuntimeError> {
        let bytes = prepared.bytes().len();
        if bytes > self.inner.options.max_proposal_bytes {
            return Err(RuntimeError::Overloaded);
        }
        let group = prepared.group();
        match self
            .call(
                Some(group),
                Command::Submit(prepared, retained),
                bytes + 4096,
            )
            .await?
        {
            Response::Submission(outcome) => Ok(outcome),
            _ => Err(RuntimeError::Protocol("prepared submission reply".into())),
        }
    }

    /// Start parked: no recovered group is attached without its verified root
    /// and catalog/member permit. The engine admits only one node runtime.
    pub async fn start(
        log: ReplicaLog,
        options: RaftRuntimeOptions,
    ) -> Result<(Self, CellGroupRuntimeEvents), RuntimeError> {
        options.validate(log.limits())?;
        // Reject an engine containing the superseded application family.
        log.cell_groups()?;
        let lease = log.claim_runtime()?;
        let domain = log.domain();
        let (sender, requests) = mailbox::channel(options.max_requests + COMPLETION_SLOTS);
        let (events, receiver) = mpsc::channel(options.max_events);
        let closing = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(Terminal::new());
        let stop = Arc::new(Notify::new());
        let (ready, wait) = oneshot::channel();
        let handle = Self {
            inner: Arc::new(Handle {
                log: log.clone(),
                domain: domain.clone(),
                budget: Budget::new(options.max_request_bytes, options.max_requests),
                completions: Budget::new(options.max_ready_bytes, COMPLETION_SLOTS),
                read_pins: Budget::new(options.max_read_pin_bytes, options.max_pending_reads),
                options: options.clone(),
                requests: sender,
                transport_claimed: AtomicBool::new(false),
                closing: closing.clone(),
                terminal: terminal.clone(),
                stop: stop.clone(),
            }),
        };
        let worker_domain = domain.clone();
        domain
            .spawn_detached("cell-group-consensus", async move {
                struct Closed(Arc<AtomicBool>, Arc<Terminal>);

                impl Drop for Closed {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::Release);
                        self.1.complete(Err(RuntimeError::Closed));
                    }
                }
                let _closed = Closed(closing.clone(), terminal.clone());
                let mut worker = worker::Worker::new(log, options, worker_domain, events);
                if ready.send(()).is_err() {
                    lease.release();
                    return;
                }
                let result = worker.run(requests, &closing, &stop).await;
                closing.store(true, Ordering::Release);
                let fence = worker.finish().await;
                let result = result.and(fence);
                drop(worker);
                if result.is_ok() {
                    lease.release();
                }
                terminal.complete(result);
            })
            .map_err(|_| RuntimeError::Closed)?;
        wait.await.map_err(|_| RuntimeError::Closed)?;
        handle.admit()?;
        let owner = Arc::downgrade(&handle.inner);
        Ok((handle, CellGroupRuntimeEvents { receiver, owner }))
    }

    pub(crate) fn admit(&self) -> Result<(), RuntimeError> {
        if self.inner.closing.load(Ordering::Acquire) || self.inner.domain.mono_ms().is_err() {
            return Err(RuntimeError::Closed);
        }
        Ok(())
    }

    async fn call(
        &self,
        group: Option<GroupHistory>,
        command: Command,
        bytes: usize,
    ) -> Result<Response, RuntimeError> {
        self.admit()?;
        let completion = matches!(command, Command::Apply(..) | Command::Quarantine);
        let budget = if completion {
            &self.inner.completions
        } else {
            &self.inner.budget
        };
        if bytes > budget.capacity().0 {
            return Err(RuntimeError::Overloaded);
        }
        let permit = loop {
            let wake = budget.wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            self.admit()?;
            if let Some(permit) = budget.acquire(bytes) {
                break permit;
            }
            if !completion {
                return Err(RuntimeError::Overloaded);
            }
            crate::asyncrt::select! {
                _ = wake => (),
                _ = self.inner.terminal.wait() => return Err(RuntimeError::Closed),
            }
        };
        self.send_reserved(group, command, bytes, permit).await
    }

    async fn send_reserved(
        &self,
        group: Option<GroupHistory>,
        command: Command,
        bytes: usize,
        permit: Permit,
    ) -> Result<Response, RuntimeError> {
        self.admit()?;
        let (reply, wait) = oneshot::channel();
        self.inner
            .requests
            .try_send(Request {
                group,
                command,
                reply,
                bytes,
                _permit: permit,
                inbound: None,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
            })?;
        let result = wait.await.map_err(|_| RuntimeError::Closed)?;
        self.admit()?;
        result
    }

    /// Revoke this group's authority without retiring its physical owners. This
    /// control request bypasses the waiting apply/ordinary queues and shares the
    /// reserved completion lane. Only joined node shutdown releases the claim.
    pub(crate) async fn quarantine(&self, group: GroupHistory) -> Result<(), RuntimeError> {
        match self.call(Some(group), Command::Quarantine, 4096).await? {
            Response::Unit => Ok(()),
            _ => Err(RuntimeError::Protocol("quarantine reply".into())),
        }
    }

    pub async fn attach(
        &self,
        permit: CellGroupBootstrapPermit,
        current: PreparedGroupManifest,
    ) -> Result<(), RuntimeError> {
        self.admit()?;
        // Reserve before catalog revalidation and RawNode construction, keeping
        // concurrent attachment work inside the node's ordinary admission cap.
        let bytes = current.reserved_bytes() + 4096;
        let admission = self
            .inner
            .budget
            .acquire(bytes)
            .ok_or(RuntimeError::Overloaded)?;
        let history = permit.route().history;
        let local = self.inner.log.cell_group(&history)?;
        if !Arc::ptr_eq(&local.owner, &permit.group().owner) {
            return Err(RuntimeError::StaleCompletion);
        }
        let mut seed = Sha256::new();
        let election_seed = self
            .inner
            .domain
            .run(async {
                self.inner.options.election_seed.unwrap_or_else(|| {
                    use rand::RngCore as _;
                    let mut seed = [0; 32];
                    crate::asyncrt::rng("cell-group-election").fill_bytes(&mut seed);
                    seed
                })
            })
            .await
            .map_err(|_| RuntimeError::Closed)?;
        seed.update(election_seed);
        seed.update(history.group.bytes());
        seed.update(history.incarnation.bytes());
        seed.update(local.identity().member.get().to_be_bytes());
        seed.update(local.identity().node.bytes());
        let owner = self
            .inner
            .domain
            .run(CellGroupOwner::start(
                permit,
                current,
                self.inner.options.clone(),
                seed.finalize().into(),
            ))
            .await
            .map_err(|_| RuntimeError::Closed)??;
        self.send_reserved(
            Some(history),
            Command::Attach(Box::new(owner)),
            bytes,
            admission,
        )
        .await
        .map(|_| ())
    }

    /// Select one coherent image for this invocation. The deadline uses the
    /// owning execution domain's monotonic clock and includes queue/barrier wait.
    /// Returned pins retain separate node/group read admission until dropped.
    pub async fn read_authority(
        &self,
        history: GroupHistory,
        cell: cellar_logic::cell_group::CellHistory,
        consistency: CellReadConsistency,
        deadline: u64,
    ) -> Result<CellReadAuthority, RuntimeError> {
        self.admit()?;
        if self
            .inner
            .domain
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?
            >= deadline
        {
            return Err(RuntimeError::DeadlineExceeded);
        }
        let reservation = self
            .inner
            .read_pins
            .acquire(4096)
            .ok_or(RuntimeError::Overloaded)?;
        let request = reads::ReadRequest {
            cell,
            consistency,
            deadline,
            reservation,
        };
        let timer = self
            .inner
            .domain
            .sleep_until(deadline)
            .map_err(|_| RuntimeError::Closed)?;
        crate::asyncrt::select! {
            result = self.call(Some(history), Command::Read(Box::new(request)), 4096) => {
                match result? {
                    Response::Read(authority) => {
                        authority.check(self.inner.domain.mono_ms().map_err(|_| RuntimeError::Closed)?)?;
                        Ok(*authority)
                    }
                    _ => Err(RuntimeError::Protocol("cell read authority reply".into())),
                }
            }
            _ = timer => Err(RuntimeError::DeadlineExceeded),
        }
    }

    pub async fn execution_authority(
        &self,
        history: GroupHistory,
        cell: cellar_logic::cell_group::CellHistory,
    ) -> Result<CellExecutionAuthority, RuntimeError> {
        let bytes = cell.cell.as_str().len() + 4096;
        match self
            .call(Some(history), Command::Execution(cell), bytes)
            .await?
        {
            Response::Execution(authority) => Ok(*authority),
            _ => Err(RuntimeError::Protocol("execution authority reply".into())),
        }
    }

    pub async fn reconciliation_authority(
        &self,
        history: GroupHistory,
        cell: cellar_logic::cell_group::CellHistory,
    ) -> Result<CellReconciliationAuthority, RuntimeError> {
        let bytes = cell.cell.as_str().len() + 4096;
        match self
            .call(Some(history), Command::Reconciliation(cell), bytes)
            .await?
        {
            Response::Reconciliation(authority) => Ok(*authority),
            _ => Err(RuntimeError::Protocol(
                "reconciliation authority reply".into(),
            )),
        }
    }

    pub async fn enrollment_authority(
        &self,
        history: GroupHistory,
        cell: cellar_logic::cell_group::CellHistory,
    ) -> Result<CellEnrollmentAuthority, RuntimeError> {
        let bytes = cell.cell.as_str().len() + 4096;
        match self
            .call(Some(history), Command::Enrollment(cell), bytes)
            .await?
        {
            Response::Enrollment(authority) => Ok(*authority),
            _ => Err(RuntimeError::Protocol("enrollment authority reply".into())),
        }
    }

    pub async fn enrollment_state(
        &self,
        history: GroupHistory,
        cell: cellar_logic::cell_group::CellHistory,
    ) -> Result<CellEnrollmentState, RuntimeError> {
        let bytes = cell.cell.as_str().len() + 4096;
        match self
            .call(Some(history), Command::EnrollmentState(cell), bytes)
            .await?
        {
            Response::EnrollmentState(state) => Ok(*state),
            _ => Err(RuntimeError::Protocol("enrollment state reply".into())),
        }
    }

    pub async fn campaign(&self, history: GroupHistory) -> Result<(), RuntimeError> {
        self.call(Some(history), Command::Campaign, 4096)
            .await
            .map(|_| ())
    }

    pub async fn step(&self, history: GroupHistory, message: Message) -> Result<(), RuntimeError> {
        let bytes = message.compute_size() as usize;
        if bytes > self.inner.options.max_message_bytes {
            return Err(RuntimeError::Overloaded);
        }
        self.call(Some(history), Command::Step(message), bytes + 4096)
            .await
            .map(|_| ())
    }

    /// Queue a verified snapshot on this group owner. Admission is not a
    /// persistence receipt; only its post-fence Raft response acknowledges it.
    pub async fn step_checkpoint(
        &self,
        history: GroupHistory,
        message: Message,
        checkpoint: PreparedCellGroupCheckpoint,
    ) -> Result<CellGroupSnapshotAdmission, RuntimeError> {
        let size = message.compute_size() as usize;
        if size > self.inner.options.max_message_bytes {
            return Err(RuntimeError::Overloaded);
        }
        let bytes = checkpoint
            .manifest()
            .reserved_bytes()
            .saturating_add(size)
            .saturating_add(4096);
        match self
            .call(
                Some(history),
                Command::Checkpoint(message, checkpoint),
                bytes,
            )
            .await?
        {
            Response::Checkpoint(outcome) => Ok(outcome),
            _ => Err(RuntimeError::Protocol("checkpoint admission reply".into())),
        }
    }

    pub async fn propose(
        &self,
        history: GroupHistory,
        data: Vec<u8>,
    ) -> Result<CellGroupProposal, RuntimeError> {
        let bytes = data.len();
        if bytes > self.inner.options.max_proposal_bytes {
            return Err(RuntimeError::Overloaded);
        }
        match self
            .call(Some(history), Command::Propose(data), bytes + 4096)
            .await?
        {
            Response::Proposal(proposal) => Ok(proposal),
            _ => Err(RuntimeError::Protocol("proposal reply".into())),
        }
    }

    pub async fn propose_at_term(
        &self,
        history: GroupHistory,
        term: cellar_logic::consensus::Term,
        data: Vec<u8>,
    ) -> Result<CellGroupProposal, RuntimeError> {
        let bytes = data.len();
        if bytes > self.inner.options.max_proposal_bytes {
            return Err(RuntimeError::Overloaded);
        }
        match self
            .call(
                Some(history),
                Command::ProposeAtTerm(term, data),
                bytes + 4096,
            )
            .await?
        {
            Response::Proposal(proposal) => Ok(proposal),
            _ => Err(RuntimeError::Protocol("fenced proposal reply".into())),
        }
    }

    /// Queue an authenticated peer frame without waiting for protocol work.
    /// Node/group admission remains owned until execution or discard. Success
    /// is neither a Raft acknowledgement nor persistence/application proof.
    pub async fn admit_step(
        &self,
        history: GroupHistory,
        message: Message,
    ) -> Result<(), RuntimeError> {
        let bytes = message.compute_size() as usize;
        if bytes > self.inner.options.max_message_bytes {
            return Err(RuntimeError::Overloaded);
        }
        self.call(Some(history), Command::AdmitStep(message), bytes + 4096)
            .await
            .map(|_| ())
    }

    pub(crate) async fn report_unreachable(
        &self,
        history: GroupHistory,
        member: u64,
    ) -> Result<(), RuntimeError> {
        self.call(Some(history), Command::Unreachable(member), 4096)
            .await
            .map(|_| ())
    }

    pub async fn read_index(
        &self,
        history: GroupHistory,
        context: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        let bytes = context.len();
        if bytes > 4096 {
            return Err(RuntimeError::Overloaded);
        }
        self.call(Some(history), Command::ReadIndex(context), bytes + 4096)
            .await
            .map(|_| ())
    }

    pub async fn complete_apply(
        &self,
        history: GroupHistory,
        work: CellGroupApply,
        next: PreparedGroupManifest,
    ) -> Result<(), RuntimeError> {
        let bytes = next.reserved_bytes() + 4096;
        self.call(
            Some(history),
            Command::Apply(work.ticket, Box::new(next)),
            bytes,
        )
        .await
        .map(|_| ())
    }

    pub async fn status(&self, history: GroupHistory) -> Result<CellGroupStatus, RuntimeError> {
        match self.call(Some(history), Command::Status, 4096).await? {
            Response::Status(status) => Ok(status),
            _ => Err(RuntimeError::Protocol("status reply".into())),
        }
    }

    pub async fn tick(&self) -> Result<(), RuntimeError> {
        self.call(None, Command::Tick, 4096).await.map(|_| ())
    }

    /// Close admission, join submitted persistence and release runtime ownership
    /// only after a final engine fence. Cancelling this waiter does not cancel closure.
    pub async fn close(&self) -> Result<(), RuntimeError> {
        self.inner.closing.store(true, Ordering::Release);
        self.inner.stop.notify_one();
        self.inner.terminal.wait().await
    }
}
