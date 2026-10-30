// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::collections::BTreeMap;

type Callback = Box<dyn FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>> + Send>;

pub(super) struct Stage {
    pub options: CellSqlOptions,
    pub authority: CellExecutionAuthority,
    pub attempt: CommandAttempt,
    pub allow_new: bool,
    pub callback: Callback,
    pub lease: CellLease,
    pub bytes: Arc<Bytes>,
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
}

impl Action {
    pub(super) fn progress_priority(&self) -> bool {
        !matches!(self, Self::Stage(_) | Self::Initialize(_))
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

#[derive(Default)]
pub(in crate::raft::host_sql) struct Cells {
    pending: BTreeMap<CellHistory, Pending>,
}

impl Cells {
    pub fn len(&self) -> usize {
        self.pending.len()
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
            Action::Finish(_) | Action::Reconcile(..) | Action::ResolveEnrollment(..) => {
                crate::raft::performance::Operation::SqlPublish
            }
            _ => crate::raft::performance::Operation::SqlOther,
        };
        let timer = performance.timer(operation);
        let result = if stopped.load(Ordering::Acquire) || domain.mono_ms().is_err() {
            queued.finish(false);
            Err(unavailable())
        } else {
            queued.finish(true);
            self.run(cell, action, max)
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
    ) -> Result<CellSqlOutcome, ExecutionError> {
        match action {
            Action::Stage(stage) => {
                if self.pending.contains_key(&cell) {
                    return Err(ExecutionError::Busy);
                }
                if self.pending.len() >= max {
                    return Err(overloaded());
                }
                let Stage {
                    options,
                    authority,
                    attempt,
                    allow_new,
                    callback,
                    lease,
                    mut bytes,
                } = *stage;
                if authority.cell().version.history != cell {
                    return Err(ExecutionError::Invalid("SQL cell history"));
                }
                let max_result = options.limits().max_result_bytes;
                let mut executor = options.open(authority)?;
                let result = executor.stage(attempt, allow_new, callback);
                match result {
                    Ok(CellStageOutcome::Prepared(prepared)) => {
                        let retained = prepared
                            .bytes()
                            .len()
                            .checked_add(max_result)
                            .and_then(|n| n.checked_add(4096))
                            .ok_or_else(overloaded)?;
                        Bytes::shrink(&mut bytes, retained)?;
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
                            },
                        );
                        Ok(CellSqlOutcome::Prepared(prepared))
                    }
                    Ok(CellStageOutcome::Duplicate(receipt)) => {
                        executor.close()?;
                        let retained = receipt
                            .receipt
                            .result()
                            .len()
                            .checked_add(4096)
                            .ok_or_else(overloaded)?;
                        Bytes::shrink(&mut bytes, retained)?;
                        Ok(CellSqlOutcome::Receipt(Box::new(CellSqlReceipt {
                            receipt: *receipt,
                            _bytes: bytes,
                        })))
                    }
                    Err(error) => {
                        executor.close()?;
                        Err(error)
                    }
                }
            }
            Action::Initialize(request) => {
                if self.pending.contains_key(&cell) {
                    return Err(ExecutionError::Busy);
                }
                if self.pending.len() >= max {
                    return Err(overloaded());
                }
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
                authority.check().map_err(crate::raft::execution::authority_failure)?;
                if let Some(record) = authority.state().record() {
                    if record.cell() != &cell || record.creation() != attempt.creation {
                        return Err(ExecutionError::Refused(CommandFailure::CommandConflict));
                    }
                    return Ok(CellSqlOutcome::Enrollment(Box::new(CellSqlEnrollment {
                        record: record.clone(),
                        _bytes: bytes,
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
                let outcome = match outcome {
                    CellEnrollmentResolution::Committed(record) => {
                        CellSqlEnrollmentResolution::Committed(Box::new(CellSqlEnrollment {
                            record: *record,
                            _bytes: pending.prepared._bytes,
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
                Ok(CellSqlOutcome::Receipt(Box::new(CellSqlReceipt {
                    receipt,
                    _bytes: pending.prepared._bytes,
                })))
            }
            Action::Reconcile(proposal, authority) => {
                let pending = self
                    .pending
                    .get_mut(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                let resolution = pending
                    .producer
                    .command()?
                    .reconcile(proposal, *authority)?;
                let pending = self
                    .pending
                    .remove(&cell)
                    .ok_or(ExecutionError::Invalid("cell producer changed"))?;
                pending.producer.close()?;
                let resolution = match resolution {
                    CellReconciliation::Committed(receipt) => {
                        CellSqlReconciliation::Committed(Box::new(CellSqlReceipt {
                            receipt: *receipt,
                            _bytes: pending.prepared._bytes,
                        }))
                    }
                    CellReconciliation::NotCommitted => CellSqlReconciliation::NotCommitted,
                    CellReconciliation::Refused(failure) => CellSqlReconciliation::Refused(failure),
                };
                Ok(CellSqlOutcome::Reconciled(resolution))
            }
        }
    }

    pub fn close(&mut self) -> Result<(), ExecutionError> {
        let mut result = Ok(());
        for (_, pending) in std::mem::take(&mut self.pending) {
            if let Err(error) = pending.producer.close() {
                result = Err(error);
            }
        }
        result
    }
}
