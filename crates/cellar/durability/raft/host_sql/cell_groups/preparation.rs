// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded immutable preparation per admitted SQL producer. Entered I/O is
//! joined before its producer, cell lease or byte reservation can be released.

use super::*;
use crate::raft::execution::{CellPrivatePreparation, CellPrivatePrepared};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

pub(super) struct Preparation {
    task: Option<crate::asyncrt::TaskHandle<Result<CellPrivatePrepared, ExecutionError>>>,
    input: Option<Arc<CellPrivatePreparation>>,
    retry: Option<crate::asyncrt::Sleep>,
    domain: crate::asyncrt::DomainToken,
    deadline: u64,
}

fn busy(error: &ExecutionError) -> bool {
    matches!(error, ExecutionError::Storage(cause)
        if matches!(cause.downcast_ref::<cellar_ltx::internal::StoreError>(),
            Some(cellar_ltx::internal::StoreError::Busy)))
}

// A joined host task failure is not a returned storage refusal. Its owner lost
// the physical operation's completion and must close before admission resumes.
pub(super) fn task_failed(error: &ExecutionError) -> bool {
    matches!(error, ExecutionError::Storage(cause) if cause.is::<crate::asyncrt::TaskPanic>())
}

impl Preparation {
    pub fn new(
        input: CellPrivatePreparation,
        domain: crate::asyncrt::DomainToken,
        mode: CellSqlStageMode,
    ) -> Self {
        Self {
            task: None,
            input: Some(Arc::new(input)),
            retry: None,
            domain,
            deadline: match mode {
                CellSqlStageMode::InvokeBefore(at) => at,
                _ => u64::MAX,
            },
        }
    }

    pub fn poll(
        &mut self,
        cx: &mut Context<'_>,
        stopped: bool,
    ) -> Poll<Result<CellPrivatePrepared, ExecutionError>> {
        loop {
            if let Some(task) = &mut self.task {
                let joined = match Pin::new(task).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(joined) => joined,
                };
                self.task = None;
                let result = joined.map_err(crate::raft::execution::storage)?;
                match result {
                    Err(error) if busy(&error) && !stopped => {
                        let now = self.domain.mono_ms().map_err(|_| unavailable())?;
                        if now >= self.deadline {
                            return Poll::Ready(Err(error));
                        }
                        // Only physical inventory contention retries here. SQL,
                        // callbacks, command IDs and captured bytes never replay.
                        self.retry = Some(
                            self.domain
                                .sleep_until(now.saturating_add(5).min(self.deadline))
                                .map_err(|_| unavailable())?,
                        );
                    }
                    result => {
                        self.input.take();
                        return Poll::Ready(result);
                    }
                }
            }
            if stopped {
                return Poll::Ready(Err(unavailable()));
            }
            if self.domain.mono_ms().map_err(|_| unavailable())? >= self.deadline {
                return Poll::Ready(Err(crate::raft::execution::authority_failure(
                    RuntimeError::DeadlineExceeded,
                )));
            }
            if let Some(retry) = &mut self.retry {
                if retry.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.retry = None;
            }
            let input = self.input.as_ref().ok_or_else(unavailable)?.clone();
            self.task = Some(
                self.domain
                    .blocking(move || input.prepare())
                    .map_err(|_| unavailable())?,
            );
        }
    }

    pub fn join(&mut self, waker: &Waker) -> Result<(), ExecutionError> {
        let mut cx = Context::from_waker(waker);
        loop {
            match self.poll_join(&mut cx) {
                Poll::Pending => std::thread::park(),
                Poll::Ready(result) => return result,
            }
        }
    }

    pub(super) fn poll_join(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ExecutionError>> {
        self.retry = None;
        self.input.take();
        if let Some(task) = &mut self.task {
            let joined = match Pin::new(task).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(joined) => joined,
            };
            self.task = None;
            return Poll::Ready(match joined.map_err(crate::raft::execution::storage)? {
                Ok(prepared) => prepared.discard(),
                Err(error) if busy(&error) => Ok(()),
                Err(error) => Err(error),
            });
        }
        Poll::Ready(Ok(()))
    }
}

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

impl Drop for Preparation {
    fn drop(&mut self) {
        // Also join if an owning SQL turn unwinds. The enclosing frame declares
        // preparation before the executor/lease, preserving physical custody.
        let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
        if let Err(error) = self.join(&waker) {
            tracing::error!(%error, "failed to join private image preparation");
        }
    }
}
