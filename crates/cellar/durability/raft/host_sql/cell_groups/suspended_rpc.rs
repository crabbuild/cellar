// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep suspended RPCs on their existing worker without occupying its queue.

use super::*;
use crate::raft::execution::{CellRpcStart, CellRpcTurn};
use std::task::{Context, Poll, Wake, Waker};

struct Ready(AtomicBool);

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) struct Suspended {
    cell: CellHistory,
    turn: Box<CellRpcTurn>,
    executor: CellPrivateExecutor,
    mode: CellSqlStageMode,
    ready: Arc<Ready>,
    reply: oneshot::Sender<SqlReply>,
    retention: StageRetention,
}

impl Suspended {
    pub(super) fn close(mut self) -> Result<(), ExecutionError> {
        let rolled_back = self.executor.abort_rpc(*self.turn);
        let closed = self.executor.close();
        let _ = self.reply.send(Err(unavailable()));
        rolled_back.and(closed)
    }

    fn check(&self, now: u64) -> Result<(), ExecutionError> {
        check_waiter(self.mode, &self.reply, now)?;
        if !self.executor.rpc_term_current() {
            return Err(unavailable());
        }
        Ok(())
    }
}

fn check_waiter(
    mode: CellSqlStageMode,
    reply: &oneshot::Sender<SqlReply>,
    now: u64,
) -> Result<(), ExecutionError> {
    if mode.expired(now) {
        return Err(crate::raft::execution::authority_failure(
            RuntimeError::DeadlineExceeded,
        ));
    }
    if reply.is_closed() {
        return Err(unavailable());
    }
    Ok(())
}

impl Cells {
    pub(super) fn start_rpc(
        &mut self,
        cell: CellHistory,
        stage: Stage,
        reply: oneshot::Sender<SqlReply>,
        max: usize,
        domain: &crate::asyncrt::DomainToken,
    ) -> bool {
        let started = (|| {
            self.check_capacity(&cell, max)?;
            let Stage {
                options,
                authority,
                attempt,
                mode,
                callback,
                lease,
                bytes,
            } = stage;
            let Callback::Registered {
                class,
                input,
                system: false,
            } = callback
            else {
                return Err(ExecutionError::Invalid("cell RPC callback"));
            };
            if authority.cell().version.history != cell {
                return Err(ExecutionError::Invalid("SQL cell history"));
            }
            check_waiter(mode, &reply, domain.mono_ms().map_err(|_| unavailable())?)?;
            let max_result = options.limits().max_result_bytes;
            let mut executor = options.open(authority)?;
            let start = domain
                .mono_ms()
                .map_err(|_| unavailable())
                .and_then(|now| check_waiter(mode, &reply, now))
                .and_then(|()| executor.start_rpc(attempt, &class, &input, mode.allows_new()));
            Ok((
                executor,
                start,
                mode,
                StageRetention {
                    lease,
                    bytes,
                    max_result,
                },
            ))
        })();
        let result = match started {
            Ok((executor, start, mode, retention)) => {
                let result = match start {
                    Ok(CellRpcStart::Open(turn)) => {
                        self.suspended.push_back(Suspended {
                            cell,
                            turn,
                            executor,
                            mode,
                            ready: Arc::new(Ready(AtomicBool::new(true))),
                            reply,
                            retention,
                        });
                        return true;
                    }
                    Ok(CellRpcStart::Duplicate(receipt)) => {
                        Ok(CellStageOutcome::Duplicate(receipt))
                    }
                    Err(error) => Err(error),
                };
                self.publish_stage(cell, executor, result, retention)
            }
            Err(error) => Err(error),
        };
        let success = result.is_ok();
        let _ = reply.send(result);
        success
    }

    pub(in crate::raft::host_sql) fn poll_suspended(
        &mut self,
        domain: &crate::asyncrt::DomainToken,
        stopped: &AtomicBool,
        performance: &Arc<crate::raft::performance::Registry>,
    ) -> bool {
        if self.suspended.is_empty() {
            return false;
        }
        let Ok(now) = domain.mono_ms() else {
            return false;
        };
        // Serve one ready continuation per worker turn. Rotate past unready
        // ones so a self-waking handler cannot monopolize the worker.
        for _ in 0..self.suspended.len() {
            let Some(mut rpc) = self.suspended.pop_front() else {
                break;
            };
            let admitted = rpc.check(now);
            if admitted.is_ok() && !rpc.ready.0.swap(false, Ordering::AcqRel) {
                self.suspended.push_back(rpc);
                continue;
            }
            let timer = performance.timer(crate::raft::performance::Operation::SqlStage);
            let outcome = match admitted.and_then(|()| rpc.executor.check_rpc()) {
                Ok(()) => rpc
                    .turn
                    .poll(&mut Context::from_waker(&Waker::from(rpc.ready.clone()))),
                Err(error) => Poll::Ready(Err(error.into())),
            };
            let Poll::Ready(result) = outcome else {
                self.suspended.push_back(rpc);
                timer.finish(true);
                return true;
            };
            let result = result.and_then(|result| {
                if stopped.load(Ordering::Acquire) {
                    return Err(unavailable().into());
                }
                rpc.check(domain.mono_ms().map_err(|_| unavailable())?)?;
                Ok(result)
            });
            let staged = rpc.executor.finish_rpc(*rpc.turn, result);
            let result = self.publish_stage(rpc.cell, rpc.executor, staged, rpc.retention);
            timer.finish(result.is_ok());
            let _ = rpc.reply.send(result);
            return true;
        }
        false
    }
}
