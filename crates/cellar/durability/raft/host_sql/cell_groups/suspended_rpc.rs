// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep suspended RPCs on their existing worker without occupying its queue.

use super::super::checkpoint_task::CheckpointTask;
use super::super::rpc_commit::{wait_checkpoint, CheckpointProof};
use super::*;
use crate::raft::execution::{CellRpcDispatch, CellRpcEnvironment, CellRpcStart, CellRpcTurn};
use futures_util::future::BoxFuture;
use std::task::{Context, Poll, Wake, Waker};

#[path = "rpc_resume.rs"]
mod resume;

#[cfg(test)]
#[path = "suspended_rpc_tests.rs"]
mod tests;

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

#[derive(Clone)]
struct RpcReply(Arc<Mutex<Option<oneshot::Sender<SqlReply>>>>);

impl RpcReply {
    fn new(reply: oneshot::Sender<SqlReply>) -> Self {
        Self(Arc::new(Mutex::new(Some(reply))))
    }

    fn is_closed(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(oneshot::Sender::is_closed)
    }

    fn take(&self) -> Option<oneshot::Sender<SqlReply>> {
        self.0.lock().unwrap().take()
    }

    fn send(self, result: SqlReply) {
        if let Some(reply) = self.take() {
            let _ = reply.send(result);
        }
    }
}

pub(super) struct Suspended {
    // Physical preparation joins before any callback/lease field drops.
    checkpoint: Option<Checkpoint>,
    cell: CellHistory,
    turn: Box<CellRpcTurn>,
    executor: Option<CellPrivateExecutor>,
    options: CellSqlOptions,
    root_apply_limits: Option<cellar_ltx::apply::ApplyLimits>,
    class: Arc<crate::application::consensus::RaftClass>,
    resuming: Option<BoxFuture<'static, Result<CellExecutionAuthority, ExecutionError>>>,
    mode: CellSqlStageMode,
    ready: Arc<Ready>,
    reply: RpcReply,
    retention: StageRetention,
    commits: Option<RpcCommitContext>,
    activation: Option<RpcActivation>,
}

enum Checkpoint {
    Preparing {
        reply: oneshot::Sender<Result<(), String>>,
        task: CheckpointTask,
    },
    Submitted(Box<SubmittedCheckpoint>),
}

struct SubmittedCheckpoint {
    reply: oneshot::Sender<Result<(), String>>,
    prepared: crate::raft::execution::CellPreparedCommand,
    expected: cellar_logic::cell_group::CellVersion,
    phase: CheckpointPhase,
}

enum CheckpointPhase {
    Applying(BoxFuture<'static, Result<CheckpointProof, ExecutionError>>),
    Archiving(
        BoxFuture<
            'static,
            Result<Option<Arc<crate::raft::cell_groups::S3ReadSourceProof>>, ExecutionError>,
        >,
    ),
}

// Keep an immediately ready outcome for normal proof/reply-bridge processing.
// Polling the completed submission again would violate its future contract.
fn prime_submission(
    future: &mut BoxFuture<'static, Result<CheckpointProof, ExecutionError>>,
    cx: &mut Context<'_>,
) {
    if let Poll::Ready(result) = future.as_mut().poll(cx) {
        *future = Box::pin(std::future::ready(result));
    }
}

impl Suspended {
    pub(super) fn belongs_to(&self, cell: &CellHistory) -> bool {
        &self.cell == cell
    }

    pub(super) fn close(mut self) -> Result<(), ExecutionError> {
        let joined = match &mut self.checkpoint {
            Some(Checkpoint::Preparing { task, .. }) => {
                let returned = task.join(&Waker::from(self.ready.clone()));
                let adopted = returned.producer.map_or(Ok(()), |producer| {
                    self.executor
                        .as_mut()
                        .ok_or_else(unavailable)?
                        .adopt_checkpoint(producer)
                });
                adopted.and(returned.outcome.map(|_| ()))
            }
            _ => Ok(()),
        };
        let rolled_back = match self.executor.take() {
            Some(mut executor) => {
                let aborted = executor.abort_rpc(*self.turn);
                aborted.and(executor.close())
            }
            None => self.turn.close_parked(),
        };
        let retired = self.activation.take().map_or(Ok(()), RpcActivation::close);
        self.reply.send(Err(unavailable()));
        joined.and(rolled_back).and(retired)
    }

    fn retry_checkpoint(
        &self,
        prepared: &crate::raft::execution::CellPreparedCommand,
        expected: &cellar_logic::cell_group::CellVersion,
        domain: &crate::asyncrt::DomainToken,
        cx: &mut Context<'_>,
    ) -> Result<CheckpointPhase, ExecutionError> {
        let commits = self.commits.as_ref().ok_or_else(unavailable)?;
        let fence = self
            .executor
            .as_ref()
            .ok_or_else(unavailable)?
            .rpc_activation_fence();
        let mut future: BoxFuture<'static, Result<CheckpointProof, ExecutionError>> =
            Box::pin(wait_checkpoint(
                commits.runtime.clone(),
                domain.clone(),
                prepared.clone(),
                expected.clone(),
                fence,
                self.retention.bytes.clone(),
                Some(commits.deadline),
            ));
        prime_submission(&mut future, cx);
        Ok(CheckpointPhase::Applying(future))
    }

    fn begin_checkpoint(
        &mut self,
        reply: oneshot::Sender<Result<(), String>>,
        domain: &crate::asyncrt::DomainToken,
        stopped: Arc<AtomicBool>,
    ) -> Result<(), ExecutionError> {
        let commits = self.commits.as_ref().ok_or(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability,
        ))?;
        let executor = self.executor.as_mut().ok_or_else(unavailable)?;
        let (captured, grant) = executor.stage_rpc_checkpoint(&mut self.turn)?;
        let fence = executor.rpc_activation_fence();
        tracing::debug!(target: "cellar::raft::checkpoint", phase = "captured",
            history = ?self.cell, at_ms = ?domain.mono_ms().ok());
        let waiter = self.reply.clone();
        let mode = self.mode;
        let activation = self.activation.as_ref().and_then(|a| a.activation.clone());
        let clock = domain.clone();
        self.checkpoint = Some(Checkpoint::Preparing {
            reply,
            task: CheckpointTask::new(
                captured,
                grant,
                domain.clone(),
                commits.clone(),
                fence,
                self.retention.bytes.clone(),
                move || {
                    check_waiter(
                        mode,
                        waiter.is_closed(),
                        clock.mono_ms().map_err(|_| unavailable())?,
                    )?;
                    if stopped.load(Ordering::Acquire)
                        || activation.as_ref().is_some_and(|a| !a.live())
                    {
                        return Err(unavailable());
                    }
                    Ok(())
                },
            )?,
        });
        self.ready.0.store(true, Ordering::Release);
        Ok(())
    }

    fn poll_checkpoint(
        &mut self,
        cx: &mut Context<'_>,
        domain: &crate::asyncrt::DomainToken,
    ) -> Poll<Result<(), ExecutionError>> {
        let mut checkpoint = match self.checkpoint.take().expect("owned RPC checkpoint") {
            Checkpoint::Submitted(checkpoint) => *checkpoint,
            Checkpoint::Preparing { reply, mut task } => {
                let returned = match task.poll(cx) {
                    Poll::Pending => {
                        self.checkpoint = Some(Checkpoint::Preparing { reply, task });
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => result,
                };
                let producer = returned.producer;
                let prepared = producer.as_ref().map(|p| p.command().clone());
                let expected = producer.as_ref().map(|p| p.expected().clone());
                // Even rejection/error returns producer custody before the
                // original error path can close or reconcile the invocation.
                if let Some(producer) = producer {
                    self.executor
                        .as_mut()
                        .ok_or_else(unavailable)?
                        .adopt_checkpoint(producer)?;
                }
                if !returned.outcome? {
                    return Poll::Ready(Err(unavailable()));
                }
                let prepared = prepared.ok_or_else(unavailable)?;
                let expected = expected.ok_or_else(unavailable)?;
                let proof = returned.proof.ok_or_else(unavailable)?;
                SubmittedCheckpoint {
                    reply,
                    prepared,
                    expected,
                    phase: CheckpointPhase::Applying(Box::pin(std::future::ready(Ok(proof)))),
                }
            }
        };
        let result = match &mut checkpoint.phase {
            CheckpointPhase::Applying(future) => match future.as_mut().poll(cx) {
                Poll::Pending => {
                    self.checkpoint = Some(Checkpoint::Submitted(Box::new(checkpoint)));
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
                            tracing::debug!(target: "cellar::raft::checkpoint", phase = "applied",
                                history = ?self.cell, sequence = checkpoint.expected.sequence.get(),
                                at_ms = ?domain.mono_ms().ok());
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
                                checkpoint.phase = CheckpointPhase::Archiving(Box::pin(
                                    async move {
                                        let settled = archive
                                            .checkpoint(
                                                &commits.runtime,
                                                commits.route,
                                                receipt,
                                                commits.limits,
                                                commits.deadline,
                                            )
                                            .await
                                            .map_err(crate::raft::execution::storage)?;
                                        commits
                                            .runtime
                                            .log()
                                            .performance()
                                            .measure(
                                                crate::raft::performance::Operation::CellCommandS3Freshness,
                                                settled.proof.require_current(
                                                    &domain,
                                                    commits.deadline,
                                                ),
                                            )
                                            .await
                                            .map_err(crate::raft::execution::storage)?;
                                        Ok(Some(settled.into_read_source()))
                                    },
                                ));
                                self.checkpoint = Some(Checkpoint::Submitted(Box::new(checkpoint)));
                                self.ready.0.store(true, Ordering::Release);
                                return Poll::Pending;
                            }
                            Ok(None)
                        }
                        Err(error) => Err(error),
                    }
                }
            },
            CheckpointPhase::Archiving(future) => match future.as_mut().poll(cx) {
                Poll::Pending => {
                    self.checkpoint = Some(Checkpoint::Submitted(Box::new(checkpoint)));
                    return Poll::Pending;
                }
                Poll::Ready(result) => result,
            },
        };
        let source = match result {
            Ok(source) => source,
            Err(error) => {
                if matches!(
                    error,
                    ExecutionError::Uncertain
                        | ExecutionError::Refused(CommandFailure::Unavailable)
                ) && self
                    .executor
                    .as_ref()
                    .is_some_and(|executor| executor.rpc_checkpoint_version().is_ok())
                {
                    checkpoint.phase = self.retry_checkpoint(
                        &checkpoint.prepared,
                        &checkpoint.expected,
                        domain,
                        cx,
                    )?;
                    self.checkpoint = Some(Checkpoint::Submitted(Box::new(checkpoint)));
                    self.ready.0.store(true, Ordering::Release);
                    return Poll::Pending;
                }
                return Poll::Ready(Err(error));
            }
        };
        self.check(domain.mono_ms().map_err(|_| unavailable())?)?;
        self.executor
            .as_mut()
            .ok_or_else(unavailable)?
            .continue_rpc_checkpoint(&mut self.turn)?;
        if let Some(source) = source {
            self.executor
                .as_ref()
                .ok_or_else(unavailable)?
                .bind_rpc_s3_read_source(&self.turn, source)?;
        }
        tracing::debug!(target: "cellar::raft::checkpoint", phase = "continued",
            history = ?self.cell, sequence = checkpoint.expected.sequence.get(),
            at_ms = ?domain.mono_ms().ok());
        let _ = checkpoint.reply.send(Ok(()));
        self.ready.0.store(true, Ordering::Release);
        Poll::Ready(Ok(()))
    }

    fn check_poll(
        &self,
        domain: &crate::asyncrt::DomainToken,
        stopped: &AtomicBool,
    ) -> Result<(), ExecutionError> {
        if stopped.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        self.check(domain.mono_ms().map_err(|_| unavailable())?)?;
        self.executor.as_ref().ok_or_else(unavailable)?.check_rpc()
    }

    fn check(&self, now: u64) -> Result<(), ExecutionError> {
        check_waiter(self.mode, self.reply.is_closed(), now)?;
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

fn check_waiter(mode: CellSqlStageMode, closed: bool, now: u64) -> Result<(), ExecutionError> {
    if mode.expired(now) {
        return Err(crate::raft::execution::authority_failure(
            RuntimeError::DeadlineExceeded,
        ));
    }
    if closed {
        return Err(unavailable());
    }
    Ok(())
}

impl Preparing {
    pub(super) fn close(mut self) -> Result<(), ExecutionError> {
        let joined = self.task.join(&self.waker);
        let closed = self.executor.close();
        let retired = self.activation.take().map_or(Ok(()), RpcActivation::close);
        let _ = self.reply.send(Err(unavailable()));
        joined.and(closed).and(retired)
    }
}

impl Cells {
    fn poll_preparing(&mut self, stopped: &AtomicBool, now: u64) -> bool {
        for _ in 0..self.preparing.len() {
            let Some(mut preparation) = self.preparing.pop_front() else {
                break;
            };
            let mut cx = Context::from_waker(&preparation.waker);
            let image = match preparation
                .task
                .poll(&mut cx, stopped.load(Ordering::Acquire))
            {
                Poll::Pending => {
                    self.preparing.push_back(preparation);
                    continue;
                }
                Poll::Ready(result) => result,
            };
            let staged = image.and_then(|image| {
                let check = check_waiter(preparation.mode, preparation.reply.is_closed(), now)
                    .and_then(|()| {
                        if stopped.load(Ordering::Acquire)
                            || preparation
                                .activation
                                .as_ref()
                                .and_then(|a| a.activation.as_ref())
                                .is_some_and(|activation| !activation.live())
                        {
                            Err(unavailable())
                        } else {
                            Ok(())
                        }
                    });
                let image = image.checked(check)?;
                preparation.executor.complete_preparation(image)
            });
            let result = self.publish_stage(
                preparation.cell,
                preparation.executor,
                staged,
                preparation.retention,
                preparation.activation,
                now,
            );
            let _ = preparation.reply.send(result);
            return true;
        }
        false
    }

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
                admission,
                commits,
            } = callback
            else {
                return Err(ExecutionError::Invalid("cell RPC callback"));
            };
            if authority.cell().version.history != cell {
                return Err(ExecutionError::Invalid("SQL cell history"));
            }
            check_waiter(
                mode,
                reply.is_closed(),
                domain.mono_ms().map_err(|_| unavailable())?,
            )?;
            let commits = commits.map(|context| *context);
            let max_result = options.limits().max_result_bytes;
            let mut activation = self.activations.take(&cell, &authority, &class, now)?;
            if commits.is_some() && activation.conflicts_with(&attempt.request) {
                // `take` matched the exact current image and live leader fence.
                // This rejects only a known conflicting input; it supplies no
                // output or absence proof and does not authorize a new sequence.
                let refusal = (|| {
                    authority
                        .check()
                        .map_err(crate::raft::execution::authority_failure)?;
                    if authority.route().schema_digest != options.application_digest() {
                        return Err(ExecutionError::Invalid("cell execution schema"));
                    }
                    crate::raft::execution::validate_admitted_registered_command(
                        options.limits(),
                        options.application_digest(),
                        &cell,
                        &attempt.request,
                        &class,
                        &input,
                        admission,
                    )?;
                    authority
                        .check()
                        .map_err(crate::raft::execution::authority_failure)?;
                    Err(ExecutionError::Refused(CommandFailure::CommandConflict))
                })();
                return self
                    .activations
                    .put(cell.clone(), activation, now)
                    .and(refusal);
            }
            let mut executor =
                options
                    .clone()
                    .open(authority, &self.performance, self.root_apply_limits)?;
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
                .and_then(|now| check_waiter(mode, reply.is_closed(), now))
                .and_then(|()| {
                    executor.start_rpc(
                        attempt,
                        &class,
                        &input,
                        mode.allows_new(),
                        commits.is_some(),
                        CellRpcEnvironment {
                            admission,
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
                            root_apply_limits: self.root_apply_limits,
                            class,
                            resuming: None,
                            mode,
                            ready: Arc::new(Ready(AtomicBool::new(true), self.wake.clone())),
                            reply: RpcReply::new(reply),
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
        domain: &crate::asyncrt::DomainToken,
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
                match staged {
                    Ok(captured) => {
                        self.preparing.push_back(Preparing {
                            task: Preparation::new(captured, domain.clone(), rpc.mode),
                            executor,
                            cell: rpc.cell,
                            mode: rpc.mode,
                            reply: rpc.reply.take().expect("owned RPC reply"),
                            retention: rpc.retention,
                            activation: rpc.activation,
                            waker: Waker::from(rpc.ready),
                        });
                        return true;
                    }
                    Err(error) => self.publish_stage(
                        rpc.cell,
                        executor,
                        Err(error),
                        rpc.retention,
                        rpc.activation,
                        now,
                    ),
                }
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
        rpc.reply.send(outcome);
        success
    }

    pub(in crate::raft::host_sql) fn poll_suspended(
        &mut self,
        domain: &crate::asyncrt::DomainToken,
        stopped: &Arc<AtomicBool>,
        performance: &Arc<crate::raft::performance::Registry>,
    ) -> bool {
        if self.poll_retiring() {
            return true;
        }
        let Ok(now) = domain.mono_ms() else {
            return false;
        };
        if self.poll_preparing(stopped, now) {
            return true;
        }
        let _ = self.activations.prune(now);
        for _ in 0..self.suspended.len() {
            let Some(mut rpc) = self.suspended.pop_front() else {
                break;
            };
            // A submitted producer outlives deadline/term loss until exact
            // reconciliation; a parked invocation owns no submitted producer.
            let mut admitted = if rpc.checkpoint.is_some() {
                Ok(())
            } else {
                rpc.check(now)
            };
            let mut yield_io = false;
            if admitted.is_ok()
                && rpc.executor.is_some()
                && rpc.checkpoint.is_none()
                && rpc.retention.lease.contended()
                && rpc.turn.waiting_on_owned_io()
            {
                match rpc.turn.can_park() {
                    Ok(ready) => yield_io = ready,
                    Err(error) => admitted = Err(error),
                }
            }
            if admitted.is_ok() && !rpc.ready.0.swap(false, Ordering::AcqRel) && !yield_io {
                self.suspended.push_back(rpc);
                continue;
            }
            let timer = performance.timer(crate::raft::performance::Operation::SqlStage);
            let waker = Waker::from(rpc.ready.clone());
            let mut cx = Context::from_waker(&waker);
            if let Err(error) = admitted {
                timer.finish(self.finish_suspended(rpc, Err(error.into()), now, domain));
                return true;
            }
            if yield_io {
                // The SQL owner observes contention even if remote owned I/O
                // has not woken. Yield before polling application continuation.
                match rpc.try_park(&self.activations, now) {
                    Ok(()) => {
                        self.suspended.push_back(rpc);
                        timer.finish(true);
                    }
                    Err(error) => {
                        timer.finish(self.finish_suspended(rpc, Err(error.into()), now, domain));
                    }
                }
                return true;
            }
            if rpc.checkpoint.is_some() {
                match rpc.poll_checkpoint(&mut cx, domain) {
                    Poll::Pending => {
                        self.suspended.push_back(rpc);
                        timer.finish(true);
                        return true;
                    }
                    Poll::Ready(Err(error)) => {
                        timer.finish(self.finish_suspended(rpc, Err(error.into()), now, domain));
                        return true;
                    }
                    Poll::Ready(Ok(()))
                        if rpc.retention.lease.contended() && rpc.turn.waiting_on_owned_io() =>
                    {
                        // Keep the existing source-query handoff before its VM
                        // resumes when another writer is already waiting.
                        self.suspended.push_back(rpc);
                        timer.finish(true);
                        return true;
                    }
                    Poll::Ready(Ok(())) => {
                        // The completed proof permits one dependent poll in
                        // this worker turn. Refresh the same checks that the
                        // next turn would have performed after queued I/O.
                        if let Err(error) = rpc.check_poll(domain, stopped) {
                            timer.finish(self.finish_suspended(
                                rpc,
                                Err(error.into()),
                                now,
                                domain,
                            ));
                            return true;
                        }
                        // Consume the checkpoint's ready notification before
                        // polling. A new wake during that poll stays pending.
                        let _ = rpc.ready.0.swap(false, Ordering::AcqRel);
                        // Settle the framework's checkpoint reply bridge before
                        // the handler observes it. Pending application I/O still
                        // takes the ordinary checkpoint/park path below.
                        if let Err(error) = rpc.turn.poll_checkpoint_work(&mut cx) {
                            timer.finish(self.finish_suspended(rpc, Err(error), now, domain));
                            return true;
                        }
                        if rpc.retention.lease.contended() {
                            // Keep the existing FIFO handoff at a proven root.
                            // A reply made ready above stays ready across park.
                            if let Err(error) = rpc.try_park(&self.activations, now) {
                                timer.finish(self.finish_suspended(
                                    rpc,
                                    Err(error.into()),
                                    now,
                                    domain,
                                ));
                                return true;
                            }
                            if rpc.executor.is_none() {
                                self.suspended.push_back(rpc);
                                timer.finish(true);
                                return true;
                            }
                        }
                    }
                }
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
                        timer.finish(self.finish_suspended(rpc, Err(error.into()), now, domain));
                        return true;
                    }
                }
            }
            if rpc.executor.is_none() {
                match rpc.poll_resume(&self.activations, &mut cx, domain, now, performance) {
                    Poll::Pending => {
                        self.suspended.push_back(rpc);
                        timer.finish(true);
                        // Waiting for this cell's writer is not a completed
                        // work quantum. Another callback may already be ready
                        // without a new queue entry or wake. Finish this bounded
                        // pass before letting the owner serve queued work/idle.
                        continue;
                    }
                    Poll::Ready(Err(error)) => {
                        timer.finish(self.finish_suspended(rpc, Err(error.into()), now, domain));
                        return true;
                    }
                    Poll::Ready(Ok(())) => (),
                }
            }
            let outcome = match rpc.check_poll(domain, stopped) {
                Ok(()) => rpc.turn.poll(&mut cx),
                Err(error) => Poll::Ready(Err(error.into())),
            };
            let Poll::Ready(result) = outcome else {
                let progress = if let Some(reply) = rpc.turn.take_checkpoint() {
                    // Start the owned submission before queued SQL occupies this
                    // worker; process proofs in the normal continuation turn.
                    rpc.begin_checkpoint(reply, domain, stopped.clone())
                } else if !rpc.ready.0.load(Ordering::Acquire) || rpc.retention.lease.contended() {
                    rpc.try_park(&self.activations, now)
                } else {
                    Ok(())
                };
                if let Err(error) = progress {
                    timer.finish(self.finish_suspended(rpc, Err(error.into()), now, domain));
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
            timer.finish(self.finish_suspended(rpc, result, now, domain));
            return true;
        }
        false
    }
}
