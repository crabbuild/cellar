// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep suspended RPCs on their existing worker without occupying its queue.

use super::super::rpc_commit::{wait_checkpoint, CheckpointProof};
use super::*;
use crate::raft::execution::{CellRpcDispatch, CellRpcEnvironment, CellRpcStart, CellRpcTurn};
use futures_util::future::BoxFuture;
use std::task::{Context, Poll, Wake, Waker};

#[path = "rpc_resume.rs"]
mod resume;

struct Ready(AtomicBool, Arc<crate::raft::host_sql::WorkerWake>);

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
        self.1.wake();
    }
}

pub(super) struct Suspended {
    cell: CellHistory,
    turn: Box<CellRpcTurn>,
    executor: Option<CellPrivateExecutor>,
    options: CellSqlOptions,
    class: Arc<crate::application::consensus::RaftClass>,
    resuming: Option<BoxFuture<'static, Result<CellExecutionAuthority, ExecutionError>>>,
    mode: CellSqlStageMode,
    ready: Arc<Ready>,
    reply: oneshot::Sender<SqlReply>,
    retention: StageRetention,
    commits: Option<RpcCommitContext>,
    checkpoint: Option<Checkpoint>,
    activation: Option<RpcActivation>,
}

struct Checkpoint {
    reply: oneshot::Sender<Result<(), String>>,
    prepared: crate::raft::execution::CellPreparedCommand,
    expected: cellar_logic::cell_group::CellVersion,
    phase: CheckpointPhase,
}

enum CheckpointPhase {
    Applying(BoxFuture<'static, Result<CheckpointProof, ExecutionError>>),
    Archiving(BoxFuture<'static, Result<(), ExecutionError>>),
}

impl Suspended {
    pub(super) fn belongs_to(&self, cell: &CellHistory) -> bool {
        &self.cell == cell
    }

    pub(super) fn close(mut self) -> Result<(), ExecutionError> {
        let rolled_back = match self.executor.take() {
            Some(mut executor) => {
                let aborted = executor.abort_rpc(*self.turn);
                aborted.and(executor.close())
            }
            None => self.turn.close_parked(),
        };
        let retired = self.activation.take().map_or(Ok(()), RpcActivation::close);
        let _ = self.reply.send(Err(unavailable()));
        rolled_back.and(retired)
    }

    fn applying(
        &self,
        checkpoint: &Checkpoint,
        domain: &crate::asyncrt::DomainToken,
    ) -> Result<CheckpointPhase, ExecutionError> {
        let commits = self.commits.as_ref().ok_or_else(unavailable)?;
        Ok(CheckpointPhase::Applying(Box::pin(wait_checkpoint(
            commits.runtime.clone(),
            domain.clone(),
            checkpoint.prepared.clone(),
            checkpoint.expected.clone(),
            self.retention.bytes.clone(),
            Some(commits.deadline),
        ))))
    }

    fn begin_checkpoint(
        &mut self,
        reply: oneshot::Sender<Result<(), String>>,
        domain: &crate::asyncrt::DomainToken,
    ) -> Result<(), ExecutionError> {
        let commits = self.commits.as_ref().ok_or(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability,
        ))?;
        let executor = self.executor.as_mut().ok_or_else(unavailable)?;
        let prepared = executor.stage_rpc_checkpoint(&mut self.turn)?;
        let expected = executor.rpc_checkpoint_version()?;
        executor.mark_submitted(prepared.proposal())?;
        let phase = CheckpointPhase::Applying(Box::pin(wait_checkpoint(
            commits.runtime.clone(),
            domain.clone(),
            prepared.clone(),
            expected.clone(),
            self.retention.bytes.clone(),
            Some(commits.deadline),
        )));
        self.checkpoint = Some(Checkpoint {
            reply,
            prepared,
            expected,
            phase,
        });
        self.ready.0.store(true, Ordering::Release);
        Ok(())
    }

    fn poll_checkpoint(
        &mut self,
        cx: &mut Context<'_>,
        domain: &crate::asyncrt::DomainToken,
    ) -> Poll<Result<(), ExecutionError>> {
        let mut checkpoint = self.checkpoint.take().expect("owned RPC checkpoint");
        let result = match &mut checkpoint.phase {
            CheckpointPhase::Applying(future) => match future.as_mut().poll(cx) {
                Poll::Pending => {
                    self.checkpoint = Some(checkpoint);
                    return Poll::Pending;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(CheckpointProof::Rejected)) => {
                    return Poll::Ready(Err(unavailable()))
                }
                Poll::Ready(Ok(CheckpointProof::Reconcile(proof))) => {
                    match self
                        .executor
                        .as_mut()
                        .ok_or_else(unavailable)?
                        .reconcile(checkpoint.prepared.proposal(), *proof)
                    {
                        Ok(_) => return Poll::Ready(Err(unavailable())),
                        Err(error) => Err(error),
                    }
                }
                Poll::Ready(Ok(CheckpointProof::Current(execution, proof))) => {
                    match self
                        .executor
                        .as_mut()
                        .ok_or_else(unavailable)?
                        .finish_rpc_checkpoint(*execution, *proof)
                    {
                        Ok(receipt) => {
                            self.check(domain.mono_ms().map_err(|_| unavailable())?)?;
                            if receipt.receipt.minimum_durability()
                                == cellar_logic::consensus::Durability::S3
                            {
                                let commits =
                                    self.commits.as_ref().ok_or_else(unavailable)?.clone();
                                let archive = commits.archive.ok_or(ExecutionError::Refused(
                                    CommandFailure::UnsupportedCapability,
                                ))?;
                                let receipt = Box::new(CellSqlReceipt {
                                    receipt,
                                    _retention: OutputRetention::Sql {
                                        _bytes: self.retention.bytes.clone(),
                                    },
                                });
                                let domain = domain.clone();
                                checkpoint.phase =
                                    CheckpointPhase::Archiving(Box::pin(async move {
                                        let settled = archive
                                            .command(
                                                &commits.runtime,
                                                commits.route,
                                                receipt,
                                                commits.limits,
                                                commits.deadline,
                                            )
                                            .await
                                            .map_err(crate::raft::execution::storage)?;
                                        settled
                                            .proof
                                            .require_current(&domain, commits.deadline)
                                            .await
                                            .map_err(crate::raft::execution::storage)?;
                                        Ok(())
                                    }));
                                self.checkpoint = Some(checkpoint);
                                self.ready.0.store(true, Ordering::Release);
                                return Poll::Pending;
                            }
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                }
            },
            CheckpointPhase::Archiving(future) => match future.as_mut().poll(cx) {
                Poll::Pending => {
                    self.checkpoint = Some(checkpoint);
                    return Poll::Pending;
                }
                Poll::Ready(result) => result,
            },
        };
        if let Err(error) = result {
            if matches!(
                error,
                ExecutionError::Uncertain | ExecutionError::Refused(CommandFailure::Unavailable)
            ) && self
                .executor
                .as_ref()
                .is_some_and(|executor| executor.rpc_checkpoint_version().is_ok())
            {
                checkpoint.phase = self.applying(&checkpoint, domain)?;
                self.checkpoint = Some(checkpoint);
                self.ready.0.store(true, Ordering::Release);
                return Poll::Pending;
            }
            return Poll::Ready(Err(error));
        }
        self.check(domain.mono_ms().map_err(|_| unavailable())?)?;
        self.executor
            .as_mut()
            .ok_or_else(unavailable)?
            .continue_rpc_checkpoint(&mut self.turn)?;
        let _ = checkpoint.reply.send(Ok(()));
        self.ready.0.store(true, Ordering::Release);
        Poll::Ready(Ok(()))
    }

    fn check(&self, now: u64) -> Result<(), ExecutionError> {
        check_waiter(self.mode, &self.reply, now)?;
        if !self.executor.as_ref().map_or_else(
            || {
                self.activation
                    .as_ref()
                    .is_some_and(|activation| activation.fence.term_current())
            },
            CellPrivateExecutor::rpc_term_current,
        ) || self
            .activation
            .as_ref()
            .and_then(|activation| activation.activation.as_ref())
            .is_some_and(|activation| !activation.live())
        {
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
        now: u64,
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
                system,
                commits,
            } = callback
            else {
                return Err(ExecutionError::Invalid("cell RPC callback"));
            };
            if authority.cell().version.history != cell {
                return Err(ExecutionError::Invalid("SQL cell history"));
            }
            check_waiter(mode, &reply, domain.mono_ms().map_err(|_| unavailable())?)?;
            let commits = commits.map(|context| *context);
            let max_result = options.limits().max_result_bytes;
            let mut activation = self.activations.take(&cell, &authority, &class, now)?;
            let mut executor = options.clone().open(authority)?;
            let permit = &mut activation.permit;
            let mut admitted = false;
            let mut reserve = || {
                if permit.is_none() {
                    *permit = Some(Arc::new(self.activations.reserve()?));
                }
                admitted = true;
                Ok(())
            };
            let start = domain
                .mono_ms()
                .map_err(|_| unavailable())
                .and_then(|now| check_waiter(mode, &reply, now))
                .and_then(|()| {
                    executor.start_rpc(
                        attempt,
                        &class,
                        &input,
                        mode.allows_new(),
                        commits.is_some(),
                        CellRpcEnvironment {
                            system,
                            activation: &mut activation.activation,
                            reserve: &mut reserve,
                            dispatch: commits.as_ref().map(|commits| CellRpcDispatch {
                                domain: domain.clone(),
                                deadline: commits.deadline,
                                bindings: commits.bindings.clone(),
                            }),
                        },
                    )
                });
            Ok((
                executor,
                start,
                mode,
                StageRetention {
                    lease,
                    bytes,
                    max_result,
                },
                commits,
                activation,
                admitted,
                options,
                class,
            ))
        })();
        let result = match started {
            Ok((
                executor,
                start,
                mode,
                retention,
                commits,
                activation,
                admitted,
                options,
                class,
            )) => {
                let result = match start {
                    Ok(CellRpcStart::Open(turn)) => {
                        self.suspended.push_back(Suspended {
                            cell,
                            turn,
                            executor: Some(executor),
                            options,
                            class,
                            resuming: None,
                            mode,
                            ready: Arc::new(Ready(AtomicBool::new(true), self.wake.clone())),
                            reply,
                            retention,
                            commits,
                            checkpoint: None,
                            activation: Some(activation),
                        });
                        return true;
                    }
                    Ok(CellRpcStart::Duplicate(receipt)) => {
                        Ok(CellStageOutcome::Duplicate(receipt))
                    }
                    Err(error) if !admitted => {
                        // Receipt/input admission has not entered the native
                        // instance. A refused retry must not discard healthy
                        // local state or rerun its constructor on the next call.
                        let result = executor
                            .close()
                            .and_then(|()| self.activations.put(cell, activation, now))
                            .and(Err(error));
                        let _ = reply.send(result);
                        return false;
                    }
                    Err(error) => Err(error),
                };
                self.publish_stage(cell, executor, result, retention, Some(activation), now)
            }
            Err(error) => Err(error),
        };
        let success = result.is_ok();
        let _ = reply.send(result);
        success
    }

    fn finish_suspended(
        &mut self,
        mut rpc: Suspended,
        result: anyhow::Result<Vec<u8>>,
        now: u64,
    ) -> bool {
        let outcome = match rpc.executor.take() {
            Some(mut executor) => {
                let failed_callback = result.as_ref().err().is_some_and(|error| {
                    error.is::<crate::application::consensus::native_work::Failure>()
                });
                let staged = executor.finish_rpc(*rpc.turn, result);
                let staged = if failed_callback {
                    // The retry receipt is a successful framework mutation,
                    // but the failed application's memory is never reusable.
                    let retired = rpc
                        .activation
                        .take()
                        .map_or(Ok(()), |activation| self.activations.retire(activation));
                    staged.and_then(|outcome| retired.map(|()| outcome))
                } else {
                    staged
                };
                self.publish_stage(
                    rpc.cell,
                    executor,
                    staged,
                    rpc.retention,
                    rpc.activation,
                    now,
                )
            }
            None => {
                let closed = rpc.turn.close_parked();
                let retired = rpc.activation.take().map_or(Ok(()), RpcActivation::close);
                let result = match result {
                    Err(error) => Err(error
                        .downcast::<ExecutionError>()
                        .unwrap_or_else(crate::raft::execution::storage)),
                    Ok(_) => Err(ExecutionError::Invalid("parked RPC produced output")),
                };
                let closed = closed.and(retired);
                let remembered = closed
                    .as_ref()
                    .err()
                    .map_or(Ok(()), |error| self.activations.record_failure(error));
                closed.and(remembered).and(result)
            }
        };
        let success = outcome.is_ok();
        let _ = rpc.reply.send(outcome);
        success
    }

    pub(in crate::raft::host_sql) fn poll_suspended(
        &mut self,
        domain: &crate::asyncrt::DomainToken,
        stopped: &AtomicBool,
        performance: &Arc<crate::raft::performance::Registry>,
    ) -> bool {
        let Ok(now) = domain.mono_ms() else {
            return false;
        };
        let _ = self.activations.prune(now);
        for _ in 0..self.suspended.len() {
            let Some(mut rpc) = self.suspended.pop_front() else {
                break;
            };
            // A submitted producer outlives deadline/term loss until exact
            // reconciliation; a parked invocation owns no submitted producer.
            let admitted = if rpc.checkpoint.is_some() {
                Ok(())
            } else {
                rpc.check(now)
            };
            if admitted.is_ok() && !rpc.ready.0.swap(false, Ordering::AcqRel) {
                self.suspended.push_back(rpc);
                continue;
            }
            let timer = performance.timer(crate::raft::performance::Operation::SqlStage);
            let waker = Waker::from(rpc.ready.clone());
            let mut cx = Context::from_waker(&waker);
            if let Err(error) = admitted {
                timer.finish(self.finish_suspended(rpc, Err(error.into()), now));
                return true;
            }
            if rpc.checkpoint.is_some() {
                match rpc.poll_checkpoint(&mut cx, domain) {
                    Poll::Pending | Poll::Ready(Ok(())) => {
                        self.suspended.push_back(rpc);
                        timer.finish(true);
                    }
                    Poll::Ready(Err(error)) => {
                        timer.finish(self.finish_suspended(rpc, Err(error.into()), now));
                    }
                }
                return true;
            }
            if rpc.executor.is_none() && rpc.turn.can_poll_parked_io() {
                match rpc.turn.poll_parked_io(&mut cx) {
                    Ok(true) => {
                        self.suspended.push_back(rpc);
                        timer.finish(true);
                        return true;
                    }
                    Ok(false) => (),
                    Err(error) => {
                        timer.finish(self.finish_suspended(rpc, Err(error.into()), now));
                        return true;
                    }
                }
            }
            if rpc.executor.is_none() {
                match rpc.poll_resume(&self.activations, &mut cx, domain, now) {
                    Poll::Pending => {
                        self.suspended.push_back(rpc);
                        timer.finish(true);
                        return false;
                    }
                    Poll::Ready(Err(error)) => {
                        timer.finish(self.finish_suspended(rpc, Err(error.into()), now));
                        return true;
                    }
                    Poll::Ready(Ok(())) => (),
                }
            }
            let outcome = match rpc
                .executor
                .as_ref()
                .ok_or_else(unavailable)
                .and_then(CellPrivateExecutor::check_rpc)
            {
                Ok(()) => rpc.turn.poll(&mut cx),
                Err(error) => Poll::Ready(Err(error.into())),
            };
            let Poll::Ready(result) = outcome else {
                let progress = if let Some(reply) = rpc.turn.take_checkpoint() {
                    rpc.begin_checkpoint(reply, domain)
                } else if !rpc.ready.0.load(Ordering::Acquire) {
                    rpc.try_park(&self.activations, now)
                } else {
                    Ok(())
                };
                if let Err(error) = progress {
                    timer.finish(self.finish_suspended(rpc, Err(error.into()), now));
                } else {
                    self.suspended.push_back(rpc);
                    timer.finish(true);
                }
                return true;
            };
            let result = result.and_then(|result| {
                if stopped.load(Ordering::Acquire) {
                    return Err(unavailable().into());
                }
                rpc.check(domain.mono_ms().map_err(|_| unavailable())?)?;
                Ok(result)
            });
            timer.finish(self.finish_suspended(rpc, result, now));
            return true;
        }
        false
    }
}
