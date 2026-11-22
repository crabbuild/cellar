// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep resolved producer custody on SQL while physical cleanup progresses.

use super::super::cleanup_task::CleanupTask;
use super::*;
use crate::raft::cell_groups::CellActivationFence;
use crate::raft::execution::CellPrivateRetirement;
use cellar_logic::cell_group::CellVersion;
use std::task::{Context, Poll, Waker};

enum Settlement {
    Finished {
        receipt: Box<CellCommittedReceipt>,
        fence: Box<CellActivationFence>,
    },
    Reconciled {
        resolution: CellReconciliation,
        expected: Option<CellVersion>,
    },
}

impl Settlement {
    fn action(&self) -> &'static str {
        match self {
            Self::Finished { .. } => "finish",
            Self::Reconciled { .. } => "reconcile",
        }
    }

    fn complete(
        self,
        cell: CellHistory,
        prepared: CellSqlPrepared,
        activation: Option<RpcActivation>,
        cache: &RpcCache,
        at: u64,
    ) -> SqlReply {
        match self {
            Self::Finished { receipt, fence } => {
                if let Some(mut activation) = activation {
                    activation.fence = *fence;
                    activation.committed(&receipt.receipt);
                    cache.put(cell, activation, at)?;
                }
                Ok(CellSqlOutcome::Receipt(Box::new(CellSqlReceipt {
                    receipt: *receipt,
                    _retention: OutputRetention::Sql {
                        _bytes: prepared._bytes,
                    },
                })))
            }
            Self::Reconciled {
                resolution,
                expected,
            } => {
                if let Some(mut activation) = activation {
                    // A later image/term cannot show this native instance still
                    // matches. Cache admission rechecks its current live fence.
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
                        if let CellReconciliation::Committed(receipt) = &resolution {
                            activation.committed(&receipt.receipt);
                        }
                        cache.put(cell, activation, at)?;
                    } else {
                        activation.close()?;
                    }
                }
                let resolution = match resolution {
                    CellReconciliation::Committed(receipt) => {
                        CellSqlReconciliation::Committed(Box::new(CellSqlReceipt {
                            receipt: *receipt,
                            _retention: OutputRetention::Sql {
                                _bytes: prepared._bytes,
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
}

// The one-entry queue is reusable after dequeue; physical users still retain
// the shared progress lane, as ApplyTasks does, and the original producer bank.
pub(super) enum Reservation {
    Ordinary {
        _permit: tokio::sync::OwnedSemaphorePermit,
    },
    Progress {
        _lane: Arc<tokio::sync::OwnedSemaphorePermit>,
    },
}

impl From<WorkerReservation> for Reservation {
    fn from(slot: WorkerReservation) -> Self {
        match slot {
            WorkerReservation::Ordinary { _permit } => Self::Ordinary { _permit },
            WorkerReservation::Progress { _lane, _queue } => {
                drop(_queue);
                Self::Progress { _lane }
            }
        }
    }
}

pub(super) struct Retiring {
    // Join also on unwinding, before activation, lease, bytes or lane drop.
    task: CleanupTask,
    pub(super) cell: CellHistory,
    prepared: CellSqlPrepared,
    activation: Option<RpcActivation>,
    settlement: Settlement,
    reply: oneshot::Sender<SqlReply>,
    timer: crate::raft::performance::Timer,
    at: u64,
    waker: Waker,
    _lease: CellLease,
    _slot: Reservation,
}

impl Retiring {
    fn finish(
        mut self,
        cache: &RpcCache,
        cleaned: Result<(), ExecutionError>,
    ) -> Result<(), ExecutionError> {
        let action = self.settlement.action();
        let result = match cleaned {
            Ok(()) => self.settlement.complete(
                self.cell,
                self.prepared,
                self.activation.take(),
                cache,
                self.at,
            ),
            Err(error) => {
                if let Some(activation) = self.activation.take() {
                    if let Err(close) = activation.close() {
                        tracing::error!(%close, "failed to close activation after private cleanup failure");
                    }
                }
                Err(error)
            }
        };
        if let Err(error) = &result {
            tracing::trace!(target: "cellar::raft::sql_publish", action, ?error,
                "private publication cleanup failed");
        }
        // Deliver the original typed failure to its waiter. Shutdown also
        // reports failures even when that waiter has already disappeared.
        let closed = result.as_ref().map(|_| ()).map_err(|error| {
            crate::raft::execution::storage(anyhow::anyhow!("private cleanup completion: {error}"))
        });
        self.timer.finish(result.is_ok());
        let _ = self.reply.send(result);
        closed
    }

    pub(super) fn close(mut self, cache: &RpcCache) -> Result<(), ExecutionError> {
        let result = self.task.join(&self.waker);
        self.finish(cache, result)
    }
}

enum Prepared {
    Pending,
    Retained,
    Cleanup(Box<(Pending, Settlement, CellPrivateRetirement)>),
}

pub(super) struct Completion {
    pub(super) reply: oneshot::Sender<SqlReply>,
    pub(super) slot: WorkerReservation,
    pub(super) timer: crate::raft::performance::Timer,
    pub(super) at: u64,
}

impl Cells {
    fn prepare_settlement(
        &mut self,
        cell: &CellHistory,
        action: Action,
    ) -> Result<Prepared, ExecutionError> {
        if self.retiring.iter().any(|frame| &frame.cell == cell) {
            return Err(ExecutionError::Busy);
        }
        let pending = self
            .pending
            .get_mut(cell)
            .ok_or(ExecutionError::Uncertain)?;
        let (settlement, retirement) = match action {
            Action::Finish(authority) => {
                let fence = Box::new(authority.activation_fence());
                let (receipt, retirement) =
                    pending.producer.command()?.finish_for_cleanup(*authority)?;
                (
                    Settlement::Finished {
                        receipt: Box::new(receipt),
                        fence,
                    },
                    retirement,
                )
            }
            Action::Reconcile(proposal, authority) => {
                let expected = pending.producer.command()?.prepared_final_version();
                let (resolution, retirement) = match pending
                    .producer
                    .command()?
                    .reconcile_for_cleanup(proposal, *authority)
                {
                    Ok(Some(resolution)) => resolution,
                    Ok(None) => return Ok(Prepared::Pending),
                    Err(ExecutionError::Uncertain) if pending.prepared.proposal() == proposal => {
                        return Ok(Prepared::Retained)
                    }
                    Err(error) => return Err(error),
                };
                (
                    Settlement::Reconciled {
                        resolution,
                        expected,
                    },
                    retirement,
                )
            }
            _ => return Err(ExecutionError::Invalid("private cleanup dispatch")),
        };
        let pending = self
            .pending
            .remove(cell)
            .ok_or(ExecutionError::Invalid("cell producer changed"))?;
        Ok(Prepared::Cleanup(Box::new((
            pending, settlement, retirement,
        ))))
    }

    pub(super) fn start_settlement(
        &mut self,
        cell: CellHistory,
        action: Action,
        completion: Completion,
        domain: &crate::asyncrt::DomainToken,
    ) {
        let Completion {
            reply,
            slot,
            timer,
            at,
        } = completion;
        let result = match self.prepare_settlement(&cell, action) {
            Ok(Prepared::Pending) => Ok(CellSqlOutcome::ReconciliationPending),
            Ok(Prepared::Retained) => Ok(CellSqlOutcome::ReconciliationRetained),
            Ok(Prepared::Cleanup(work)) => {
                let (pending, settlement, retirement) = *work;
                let Pending {
                    producer,
                    prepared,
                    _lease,
                    activation,
                    ..
                } = pending;
                let cleanup = match producer {
                    Producer::Command(executor) => (*executor).close_writer(retirement),
                    Producer::Enrollment(executor) => (*executor)
                        .close()
                        .and(Err(ExecutionError::Invalid("command cleanup required"))),
                };
                match cleanup {
                    Ok(cleanup) => {
                        self.retiring.push_back(Retiring {
                            task: CleanupTask::new(cleanup, domain),
                            cell,
                            prepared,
                            activation,
                            settlement,
                            reply,
                            timer,
                            at,
                            waker: Waker::from(Arc::new(crate::raft::host_sql::ApplyWake(
                                self.wake.clone(),
                            ))),
                            _lease,
                            _slot: slot.into(),
                        });
                        return;
                    }
                    Err(error) => {
                        if let Some(activation) = activation {
                            if let Err(close) = activation.close() {
                                tracing::error!(%close, "failed to close activation after writer close failure");
                            }
                        }
                        Err(error)
                    }
                }
            }
            Err(error) => Err(error),
        };
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

    pub(super) fn poll_retiring(&mut self) -> bool {
        for _ in 0..self.retiring.len() {
            let Some(mut frame) = self.retiring.pop_front() else {
                break;
            };
            let result = match frame.task.poll(&mut Context::from_waker(&frame.waker)) {
                Poll::Pending => {
                    self.retiring.push_back(frame);
                    continue;
                }
                Poll::Ready(result) => result,
            };
            let _ = frame.finish(&self.activations, result);
            return true;
        }
        false
    }
}
