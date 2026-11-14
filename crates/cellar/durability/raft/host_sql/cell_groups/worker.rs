// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::rpc_cache::RpcActivation;
use super::*;
use std::collections::{BTreeMap, VecDeque};

#[path = "suspended_rpc.rs"]
mod rpc;

type VerificationCallback =
    Box<dyn FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>> + Send>;

pub(super) enum Callback {
    Verification(VerificationCallback),
    Registered {
        class: Arc<crate::application::consensus::RaftClass>,
        input: Box<[u8]>,
        system: bool,
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
    pub(super) _slot: tokio::sync::OwnedSemaphorePermit,
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
    pending: BTreeMap<CellHistory, Pending>,
    suspended: VecDeque<rpc::Suspended>,
    activations: Arc<RpcCache>,
    wake: Arc<crate::raft::host_sql::WorkerWake>,
    performance: Arc<crate::raft::performance::Registry>,
}

impl Cells {
    pub(in crate::raft::host_sql) fn new(
        activations: Arc<RpcCache>,
        wake: Arc<crate::raft::host_sql::WorkerWake>,
        performance: Arc<crate::raft::performance::Registry>,
    ) -> Self {
        Self {
            pending: BTreeMap::new(),
            suspended: VecDeque::new(),
            activations,
            wake,
            performance,
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
                    Action::Stage(stage) if stage.is_cell_rpc() => {
                        timer.finish(self.start_rpc(cell, *stage, reply, max, domain, now));
                        return;
                    }
                    Action::Query(query) => super::query::run(*query, domain),
                    action => self.run(cell, action, max, now),
                }
            }
            _ => {
                queued.finish(false);
                Err(unavailable())
            }
        };
        // A lost receiver drops only the delivery. Pending owns both the
        // private writer and per-cell reservation until exact settlement.
        timer.finish(result.is_ok());
        let _ = reply.send(result);
    }

    fn run(
        &mut self,
        cell: CellHistory,
        action: Action,
        max: usize,
        now: u64,
    ) -> Result<CellSqlOutcome, ExecutionError> {
        match action {
            Action::Query(_) => Err(ExecutionError::Invalid("query worker dispatch")),
            Action::Stage(stage) => {
                self.check_capacity(&cell, max)?;
                // A parked RPC has released its writer lease, but still owns a
                // continuation that may later complete the original sequence.
                // Seal only after every same-cell frame has physically joined.
                if matches!(
                    stage.attempt.request.operation.as_str(),
                    crate::raft::execution::binding_seal::OPERATION
                        | crate::storage::raft_binding_recovery::CLAIM
                        | crate::storage::raft_binding_recovery::SETTLE
                ) && self.suspended.iter().any(|rpc| rpc.belongs_to(&cell))
                {
                    return Err(ExecutionError::Busy);
                }
                let Stage {
                    options,
                    authority,
                    attempt,
                    mode,
                    callback,
                    lease,
                    bytes,
                } = *stage;
                if authority.cell().version.history != cell {
                    return Err(ExecutionError::Invalid("SQL cell history"));
                }
                let allow_new = mode.allows_new();
                let max_result = options.limits().max_result_bytes;
                let mut executor = options.open(authority, &self.performance)?;
                let result = match callback {
                    Callback::Verification(callback) => {
                        executor.stage(attempt, allow_new, callback)
                    }
                    Callback::Registered {
                        class,
                        input,
                        system,
                        ..
                    } => {
                        if system {
                            executor.stage_system_registered(attempt, &class, &input, allow_new)
                        } else {
                            executor.stage_registered(attempt, &class, &input, allow_new)
                        }
                    }
                };
                self.publish_stage(
                    cell,
                    executor,
                    result,
                    StageRetention {
                        lease,
                        bytes,
                        max_result,
                    },
                    None,
                    now,
                )
            }
            Action::Initialize(request) => {
                self.check_capacity(&cell, max)?;
                let Initialize {
                    options,
                    authority,
                    attempt,
                    callback,
                    lease,
                    mut bytes,
                } = *request;
                if authority.state().cell() != &cell {
                    return Err(ExecutionError::Invalid("initialization cell history"));
                }
                authority
                    .check()
                    .map_err(crate::raft::execution::authority_failure)?;
                if let Some(record) = authority.state().record() {
                    if record.cell() != &cell || record.creation() != attempt.creation {
                        return Err(ExecutionError::Refused(CommandFailure::CommandConflict));
                    }
                    return Ok(CellSqlOutcome::Enrollment(Box::new(CellSqlEnrollment {
                        record: record.clone(),
                        _retention: OutputRetention::Sql { _bytes: bytes },
                        s3: None,
                    })));
                }
                let mut executor = options.open_enrollment(authority)?;
                let prepared = match executor.prepare(attempt, callback) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        executor.close()?;
                        return Err(error);
                    }
                };
                let retained = prepared
                    .bytes()
                    .len()
                    .checked_add(4096)
                    .ok_or_else(overloaded)?;
                Bytes::shrink(&mut bytes, retained)?;
                let prepared = CellSqlPrepared {
                    prepared,
                    _bytes: bytes,
                };
                self.pending.insert(
                    cell,
                    Pending {
                        producer: Producer::Enrollment(Box::new(executor)),
                        prepared: prepared.clone(),
                        submitted: false,
                        _lease: lease,
                        activation: None,
                    },
                );
                Ok(CellSqlOutcome::Prepared(prepared))
            }
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
            Action::Finish(authority) => {
                let fence = authority.activation_fence();
                let pending = self
                    .pending
                    .get_mut(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                let receipt = pending.producer.command()?.finish(*authority)?;
                let pending = self
                    .pending
                    .remove(&cell)
                    .ok_or(ExecutionError::Invalid("cell producer changed"))?;
                pending.producer.close()?;
                if let Some(mut activation) = pending.activation {
                    activation.fence = fence;
                    self.activations.put(cell, activation, now)?;
                }
                Ok(CellSqlOutcome::Receipt(Box::new(CellSqlReceipt {
                    receipt,
                    _retention: OutputRetention::Sql {
                        _bytes: pending.prepared._bytes,
                    },
                })))
            }
            Action::Reconcile(proposal, authority) => {
                let pending = self
                    .pending
                    .get_mut(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                let expected = pending.producer.command()?.prepared_final_version();
                let resolution = pending
                    .producer
                    .command()?
                    .reconcile(proposal, *authority)?;
                let pending = self
                    .pending
                    .remove(&cell)
                    .ok_or(ExecutionError::Invalid("cell producer changed"))?;
                pending.producer.close()?;
                if let Some(mut activation) = pending.activation {
                    // Normal public settlement also uses reconciliation. A
                    // committed final image can retain native state while the
                    // original leader fence survives; a later image or term
                    // cannot establish that this instance matches the cell.
                    let fence = match &resolution {
                        CellReconciliation::Committed(receipt)
                            if expected.as_ref() == Some(&receipt.version) =>
                        {
                            activation.fence.advanced(&receipt.version)
                        }
                        _ => None,
                    };
                    if let Some(fence) = fence {
                        activation.fence = fence;
                        self.activations.put(cell, activation, now)?;
                    } else {
                        activation.close()?;
                    }
                }
                let resolution = match resolution {
                    CellReconciliation::Committed(receipt) => {
                        CellSqlReconciliation::Committed(Box::new(CellSqlReceipt {
                            receipt: *receipt,
                            _retention: OutputRetention::Sql {
                                _bytes: pending.prepared._bytes,
                            },
                        }))
                    }
                    CellReconciliation::NotCommitted => CellSqlReconciliation::NotCommitted,
                    CellReconciliation::Refused(failure) => CellSqlReconciliation::Refused(failure),
                };
                Ok(CellSqlOutcome::Reconciled(resolution))
            }
        }
    }

    fn check_capacity(&self, cell: &CellHistory, max: usize) -> Result<(), ExecutionError> {
        if self.pending.contains_key(cell) {
            return Err(ExecutionError::Busy);
        }
        if self.pending.len() + self.suspended.len() >= max {
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

    pub fn close(&mut self) -> Result<(), ExecutionError> {
        let mut result = Ok(());
        for suspended in std::mem::take(&mut self.suspended) {
            if let Err(error) = suspended.close() {
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
