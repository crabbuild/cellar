// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::preparation::Preparation;
use super::rpc_cache::RpcActivation;
use super::*;
use std::collections::{BTreeMap, VecDeque};

#[path = "cleanup.rs"]
mod cleanup;
use cleanup::Retiring;

#[path = "opening.rs"]
mod opening;
#[path = "suspended_rpc.rs"]
mod rpc;
#[path = "stage.rs"]
mod stage;

type VerificationCallback =
    Box<dyn FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>> + Send>;

pub(super) enum Callback {
    Verification(VerificationCallback),
    Registered {
        class: Arc<crate::application::consensus::RaftClass>,
        input: Box<[u8]>,
        admission: CommandAdmission,
        commits: Option<Box<RpcCommitContext>>,
    },
}

pub(super) struct Stage {
    pub options: CellSqlOptions,
    pub authority: CellExecutionAuthority,
    pub attempt: CommandAttempt,
    pub mode: CellSqlStageMode,
    pub callback: Callback,
    pub lease: CellLease,
    pub bytes: Arc<Bytes>,
}

// The request's authority is consumed exactly once when SQL opens. Its callback
// and original admission remain retained independently through rejected cleanup.
struct Capture {
    options: CellSqlOptions,
    attempt: CommandAttempt,
    mode: CellSqlStageMode,
    callback: Callback,
    lease: CellLease,
    bytes: Arc<Bytes>,
}

impl Stage {
    fn is_cell_rpc(&self) -> bool {
        matches!(
            &self.callback,
            Callback::Registered { class, .. }
                if class.is_cell_rpc(&self.attempt.request.operation)
        )
    }
}

type Initializer = Box<dyn FnOnce(&CommandTransaction<'_>) -> anyhow::Result<()> + Send>;

pub(super) struct Initialize {
    pub options: CellSqlOptions,
    pub authority: CellEnrollmentAuthority,
    pub attempt: CellEnrollmentAttempt,
    pub callback: Initializer,
    pub lease: CellLease,
    pub bytes: Arc<Bytes>,
}

pub(super) enum Action {
    Stage(Box<Stage>),
    Initialize(Box<Initialize>),
    ResolveEnrollment(ProposalId, Box<CellEnrollmentState>),
    Resume(ProposalId),
    Submit(ProposalId),
    Finish(Box<CellExecutionAuthority>),
    Reconcile(ProposalId, Box<CellReconciliationAuthority>),
    Reject(Box<CellSqlRejection>),
    Query(Box<super::query::Query>),
}

impl Action {
    pub(super) fn progress_priority(&self) -> bool {
        !matches!(self, Self::Stage(_) | Self::Initialize(_) | Self::Query(_))
    }
}

pub(in crate::raft::host_sql) struct Request {
    pub cell: CellHistory,
    pub(super) queued: crate::raft::performance::Timer,
    pub(super) action: Action,
    pub(super) reply: oneshot::Sender<Result<CellSqlOutcome, ExecutionError>>,
    pub(super) _slot: WorkerReservation,
}

impl Request {
    pub(in crate::raft::host_sql) fn progress_priority(&self) -> bool {
        self.action.progress_priority()
    }
}

struct Pending {
    producer: Producer,
    prepared: CellSqlPrepared,
    submitted: bool,
    _lease: CellLease,
    activation: Option<RpcActivation>,
}

struct StageRetention {
    lease: CellLease,
    bytes: Arc<Bytes>,
    max_result: usize,
}

struct Preparing {
    // Joined before the executor, writer lease and bytes also on unwinding.
    task: Preparation,
    executor: CellPrivateExecutor,
    cell: CellHistory,
    mode: CellSqlStageMode,
    reply: oneshot::Sender<SqlReply>,
    retention: StageRetention,
    activation: Option<RpcActivation>,
    waker: std::task::Waker,
}

enum Producer {
    Command(Box<CellPrivateExecutor>),
    Enrollment(Box<CellPrivateEnrollment>),
}

impl Producer {
    fn mark_submitted(&mut self, proposal: ProposalId) -> Result<(), ExecutionError> {
        match self {
            Self::Command(executor) => executor.mark_submitted(proposal),
            Self::Enrollment(executor) => executor.mark_submitted(proposal),
        }
    }

    fn command(&mut self) -> Result<&mut CellPrivateExecutor, ExecutionError> {
        match self {
            Self::Command(executor) => Ok(executor),
            Self::Enrollment(_) => Err(ExecutionError::Invalid("command producer required")),
        }
    }

    fn close(self) -> Result<(), ExecutionError> {
        match self {
            Self::Command(executor) => (*executor).close(),
            Self::Enrollment(executor) => (*executor).close(),
        }
    }
}

pub(in crate::raft::host_sql) struct Cells {
    opening: VecDeque<opening::Opening>,
    workspace_lane: Arc<tokio::sync::Semaphore>,
    pending: BTreeMap<CellHistory, Pending>,
    suspended: VecDeque<rpc::Suspended>,
    preparing: VecDeque<Preparing>,
    retiring: VecDeque<Retiring>,
    activations: Arc<RpcCache>,
    wake: Arc<crate::raft::host_sql::WorkerWake>,
    performance: Arc<crate::raft::performance::Registry>,
    root_apply_limits: Option<cellar_ltx::apply::ApplyLimits>,
}

impl Cells {
    pub(in crate::raft::host_sql) fn new(
        activations: Arc<RpcCache>,
        wake: Arc<crate::raft::host_sql::WorkerWake>,
        performance: Arc<crate::raft::performance::Registry>,
        root_apply_limits: Option<cellar_ltx::apply::ApplyLimits>,
    ) -> Self {
        Self {
            opening: VecDeque::new(),
            workspace_lane: Arc::new(tokio::sync::Semaphore::new(1)),
            pending: BTreeMap::new(),
            suspended: VecDeque::new(),
            preparing: VecDeque::new(),
            retiring: VecDeque::new(),
            activations,
            wake,
            performance,
            root_apply_limits,
        }
    }

    pub fn execute(
        &mut self,
        request: Request,
        max: usize,
        domain: &crate::asyncrt::DomainToken,
        stopped: &AtomicBool,
        performance: &Arc<crate::raft::performance::Registry>,
    ) {
        let Request {
            cell,
            action,
            reply,
            _slot,
            queued,
        } = request;
        let operation = match &action {
            Action::Stage(_) | Action::Initialize(_) => {
                crate::raft::performance::Operation::SqlStage
            }
            Action::Finish(_)
            | Action::Reconcile(..)
            | Action::ResolveEnrollment(..)
            | Action::Reject(_) => crate::raft::performance::Operation::SqlPublish,
            Action::Query(_) => crate::raft::performance::Operation::SqlQuery,
            _ => crate::raft::performance::Operation::SqlOther,
        };
        let publication = match &action {
            Action::Finish(_) => Some("finish"),
            Action::Reconcile(..) => Some("reconcile"),
            Action::ResolveEnrollment(..) => Some("resolve-enrollment"),
            Action::Reject(_) => Some("reject"),
            _ => None,
        };
        let timer = performance.timer(operation);
        let result = match domain.mono_ms() {
            Ok(now) if !stopped.load(Ordering::Acquire) => {
                queued.finish(true);
                match action {
                    // The public waiter can expire while unrelated physical
                    // work holds this worker. Keep the original admission
                    // fence through dequeue, before opening a private writer.
                    Action::Stage(stage) if stage.mode.expired(now) => Err(
                        crate::raft::execution::authority_failure(RuntimeError::DeadlineExceeded),
                    ),
                    action @ (Action::Stage(_) | Action::Initialize(_)) => {
                        self.start_opening(
                            cell,
                            action,
                            opening::Completion {
                                reply,
                                slot: _slot,
                                timer,
                            },
                            max,
                            domain,
                            now,
                        );
                        return;
                    }
                    action @ (Action::Finish(_) | Action::Reconcile(..)) => {
                        self.start_settlement(
                            cell,
                            action,
                            cleanup::Completion {
                                reply,
                                slot: _slot,
                                timer,
                                at: now,
                            },
                            domain,
                        );
                        return;
                    }
                    Action::Query(query) => {
                        // Retirement requires every same-cell private frame to
                        // join. Its discovery must not mask runnable callbacks.
                        let binding_ready = !self.opening.iter().any(|frame| frame.cell == cell)
                            && !self.suspended.iter().any(|rpc| rpc.belongs_to(&cell))
                            && !self
                                .preparing
                                .iter()
                                .any(|preparation| preparation.cell == cell)
                            && !self.retiring.iter().any(|frame| frame.cell == cell);
                        super::query::run(*query, domain, binding_ready)
                    }
                    action => self.run(cell, action),
                }
            }
            _ => {
                queued.finish(false);
                Err(unavailable())
            }
        };
        // A lost receiver drops only the delivery. Pending owns both the
        // private writer and per-cell reservation until exact settlement.
        if let (Some(action), Err(error)) = (publication, &result) {
            tracing::trace!(target: "cellar::raft::sql_publish", action, ?error,
                "private publication attempt remains unresolved");
        }
        timer.finish(
            result.is_ok()
                && !matches!(
                    &result,
                    Ok(CellSqlOutcome::ReconciliationPending
                        | CellSqlOutcome::ReconciliationRetained)
                ),
        );
        let _ = reply.send(result);
    }

    fn run(&mut self, cell: CellHistory, action: Action) -> Result<CellSqlOutcome, ExecutionError> {
        match action {
            Action::Query(_) => Err(ExecutionError::Invalid("query worker dispatch")),
            Action::Stage(_) => Err(ExecutionError::Invalid("stage worker dispatch")),
            Action::Initialize(_) => Err(ExecutionError::Invalid("initialization worker dispatch")),
            Action::ResolveEnrollment(creation, state) => {
                let pending = self
                    .pending
                    .get_mut(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                let Producer::Enrollment(executor) = &mut pending.producer else {
                    return Err(ExecutionError::Invalid("enrollment producer required"));
                };
                let outcome = executor.resolve(creation, *state)?;
                let pending = self
                    .pending
                    .remove(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                pending.producer.close()?;
                if let Some(activation) = pending.activation {
                    activation.close()?;
                }
                let outcome = match outcome {
                    CellEnrollmentResolution::Committed(record) => {
                        CellSqlEnrollmentResolution::Committed(Box::new(CellSqlEnrollment {
                            record: *record,
                            _retention: OutputRetention::Sql {
                                _bytes: pending.prepared._bytes,
                            },
                            s3: None,
                        }))
                    }
                    CellEnrollmentResolution::NotCommitted => {
                        CellSqlEnrollmentResolution::NotCommitted
                    }
                    CellEnrollmentResolution::Conflict => CellSqlEnrollmentResolution::Conflict,
                };
                Ok(CellSqlOutcome::EnrollmentResolved(outcome))
            }
            Action::Resume(proposal) => {
                let pending = self.pending.get(&cell).ok_or(ExecutionError::Uncertain)?;
                if pending.prepared.proposal() != proposal {
                    return Err(ExecutionError::Invalid("cell producer proposal"));
                }
                Ok(CellSqlOutcome::Prepared(pending.prepared.clone()))
            }
            Action::Reject(rejection) => {
                let pending = self.pending.get(&cell).ok_or(ExecutionError::Uncertain)?;
                if !pending
                    .prepared
                    .prepared
                    .matches_rejection(&rejection.proof)
                {
                    return Err(ExecutionError::Invalid("cell rejection preparation"));
                }
                let pending = self
                    .pending
                    .remove(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                // The proof permanently seals every submission clone. Close
                // physical SQL ownership before dropping logical-name admission.
                pending.producer.close()?;
                if let Some(activation) = pending.activation {
                    activation.close()?;
                }
                Ok(CellSqlOutcome::Rejected)
            }
            Action::Submit(proposal) => {
                let pending = self
                    .pending
                    .get_mut(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                if pending.prepared.proposal() != proposal {
                    return Err(ExecutionError::Invalid("cell producer proposal"));
                }
                if !pending.submitted {
                    pending.producer.mark_submitted(proposal)?;
                    pending.submitted = true;
                }
                Ok(CellSqlOutcome::Submitted)
            }
            Action::Finish(_) | Action::Reconcile(..) => {
                Err(ExecutionError::Invalid("settlement worker dispatch"))
            }
        }
    }

    fn check_capacity(&self, cell: &CellHistory, max: usize) -> Result<(), ExecutionError> {
        if self.opening.iter().any(|frame| &frame.cell == cell)
            || self.pending.contains_key(cell)
            || self.preparing.iter().any(|p| &p.cell == cell)
            || self.retiring.iter().any(|frame| &frame.cell == cell)
        {
            return Err(ExecutionError::Busy);
        }
        if self.opening.len()
            + self.pending.len()
            + self.suspended.len()
            + self.preparing.len()
            + self.retiring.len()
            >= max
        {
            return Err(overloaded());
        }
        Ok(())
    }

    fn publish_stage(
        &mut self,
        cell: CellHistory,
        executor: CellPrivateExecutor,
        result: Result<CellStageOutcome, ExecutionError>,
        retention: StageRetention,
        activation: Option<RpcActivation>,
        now: u64,
    ) -> SqlReply {
        let StageRetention {
            lease,
            mut bytes,
            max_result,
        } = retention;
        match result {
            Ok(CellStageOutcome::Prepared(prepared)) => {
                let shrink = prepared
                    .bytes()
                    .len()
                    .checked_add(max_result)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(overloaded)
                    .and_then(|retained| Bytes::shrink(&mut bytes, retained));
                if let Err(error) = shrink {
                    executor.close()?;
                    if let Some(activation) = activation {
                        activation.close()?;
                    }
                    return Err(error);
                }
                let prepared = CellSqlPrepared {
                    prepared,
                    _bytes: bytes,
                };
                self.pending.insert(
                    cell,
                    Pending {
                        producer: Producer::Command(Box::new(executor)),
                        prepared: prepared.clone(),
                        submitted: false,
                        _lease: lease,
                        activation,
                    },
                );
                Ok(CellSqlOutcome::Prepared(prepared))
            }
            Ok(CellStageOutcome::Duplicate(receipt)) => {
                executor.close()?;
                if let Some(activation) = activation {
                    self.activations.put(cell, activation, now)?;
                }
                let retained = receipt
                    .receipt
                    .result()
                    .len()
                    .checked_add(4096)
                    .ok_or_else(overloaded)?;
                Bytes::shrink(&mut bytes, retained)?;
                Ok(CellSqlOutcome::Receipt(Box::new(CellSqlReceipt {
                    receipt: *receipt,
                    _retention: OutputRetention::Sql { _bytes: bytes },
                })))
            }
            Err(error) => {
                executor.close()?;
                if let Some(activation) = activation {
                    activation.close()?;
                }
                Err(error)
            }
        }
    }

    pub fn close(&mut self, failed: bool) -> Result<(), ExecutionError> {
        let closure = if failed {
            rpc::Closure::Failed
        } else {
            rpc::Closure::Graceful
        };
        let mut result = Ok(());
        for opening in std::mem::take(&mut self.opening) {
            if let Err(error) = opening.close(closure) {
                result = Err(error);
            }
        }
        for retiring in std::mem::take(&mut self.retiring) {
            if let Err(error) = retiring.close(&self.activations, closure) {
                result = Err(error);
            }
        }
        for preparing in std::mem::take(&mut self.preparing) {
            if let Err(error) = preparing.close(closure) {
                result = Err(error);
            }
        }
        for suspended in std::mem::take(&mut self.suspended) {
            if let Err(error) = suspended.close(closure) {
                result = Err(error);
            }
        }
        for (_, pending) in std::mem::take(&mut self.pending) {
            if let Err(error) = pending.producer.close() {
                result = Err(error);
            }
            if let Some(activation) = pending.activation {
                if let Err(error) = activation.close() {
                    result = Err(error);
                }
            }
        }
        let cached = self.activations.close();
        result.and(cached)
    }
}
