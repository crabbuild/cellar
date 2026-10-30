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

pub(super) enum Action {
    Stage(Box<Stage>),
    Resume(ProposalId),
    Submit(ProposalId),
    Finish(Box<CellExecutionAuthority>),
    Reconcile(ProposalId, Box<CellReconciliationAuthority>),
}

impl Action {
    pub(super) fn progress_priority(&self) -> bool {
        !matches!(self, Self::Stage(_))
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
    executor: CellPrivateExecutor,
    prepared: CellSqlPrepared,
    submitted: bool,
    _lease: CellLease,
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
            Action::Stage(_) => crate::raft::performance::Operation::SqlStage,
            Action::Finish(_) | Action::Reconcile(..) => {
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
                                executor,
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
                    pending.executor.mark_submitted(proposal)?;
                    pending.submitted = true;
                }
                Ok(CellSqlOutcome::Submitted)
            }
            Action::Finish(authority) => {
                let pending = self
                    .pending
                    .get_mut(&cell)
                    .ok_or(ExecutionError::Uncertain)?;
                let receipt = pending.executor.finish(*authority)?;
                let pending = self
                    .pending
                    .remove(&cell)
                    .ok_or(ExecutionError::Invalid("cell producer changed"))?;
                pending.executor.close()?;
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
                let resolution = pending.executor.reconcile(proposal, *authority)?;
                let pending = self
                    .pending
                    .remove(&cell)
                    .ok_or(ExecutionError::Invalid("cell producer changed"))?;
                pending.executor.close()?;
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
            if let Err(error) = pending.executor.close() {
                result = Err(error);
            }
        }
        result
    }
}
