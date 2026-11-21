// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable checkpoint preparation and proposal admission do not need a SQL
//! turn. Cancellation returns physical work and producer custody to that owner.

use super::preparation::Preparation;
use super::rpc_commit::submit_checkpoint;
use super::*;
use crate::raft::execution::{
    CellCheckpointGrant, CellCheckpointPrepared, CellPrivatePreparation, CellPrivatePrepared,
};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

struct Work {
    preparation: Preparation,
    producer: Option<CellCheckpointPrepared>,
    completing: Option<crate::asyncrt::TaskHandle<Result<CellCheckpointPrepared, ExecutionError>>>,
    completion_input: Arc<Mutex<Option<(CellPrivatePrepared, CellCheckpointGrant)>>>,
}

// Constructed before spawning: even an unpolled or rejected future returns its
// entered I/O. Dropping this guard on an async executor never joins blocking I/O.
struct ReturnToOwner<T> {
    work: Option<T>,
    slot: Arc<Mutex<Option<T>>>,
}

impl<T> Drop for ReturnToOwner<T> {
    fn drop(&mut self) {
        *self.slot.lock().unwrap() = self.work.take();
    }
}

pub(super) struct CheckpointTask {
    task: Option<crate::asyncrt::TaskHandle<Result<bool, ExecutionError>>>,
    returned: Arc<Mutex<Option<Box<Work>>>>,
    work: Option<Box<Work>>,
    outcome: Option<Result<bool, ExecutionError>>,
}

pub(super) struct CheckpointReturn {
    pub producer: Option<CellCheckpointPrepared>,
    pub outcome: Result<bool, ExecutionError>,
}

impl CheckpointTask {
    pub(super) fn new(
        captured: CellPrivatePreparation,
        grant: CellCheckpointGrant,
        domain: crate::asyncrt::DomainToken,
        commits: RpcCommitContext,
        retained: Arc<Bytes>,
        check: impl Fn() -> Result<(), ExecutionError> + Send + Sync + 'static,
    ) -> Result<Self, ExecutionError> {
        let returned = Arc::new(Mutex::new(None));
        let mut guard = ReturnToOwner {
            work: Some(Box::new(Work {
                preparation: Preparation::new(
                    captured,
                    domain.clone(),
                    CellSqlStageMode::InvokeBefore(commits.deadline),
                ),
                producer: None,
                completing: None,
                completion_input: Arc::new(Mutex::new(None)),
            })),
            slot: returned.clone(),
        };
        let mut owner = Self {
            task: None,
            returned,
            work: None,
            outcome: None,
        };
        let token = domain.clone();
        let check = Arc::new(check);
        let task =
            domain.spawn_detached("cell checkpoint preparation and submission", async move {
                let work = guard.work.as_mut().expect("checkpoint work");
                check()?;
                let image = poll_fn(|cx| work.preparation.poll(cx, false)).await?;
                // Authority checking/retirement remains in the owning blocking
                // domain too. A queued closure borrows custody from this slot, so
                // rejection before entry cannot drop an image on the async thread.
                *work.completion_input.lock().unwrap() = Some((image, grant));
                let input = work.completion_input.clone();
                let checking = check.clone();
                work.completing = Some(
                    token
                        .blocking(move || {
                            let (image, grant) =
                                input.lock().unwrap().take().ok_or_else(unavailable)?;
                            grant.complete(image.checked(checking())?)
                        })
                        .map_err(|_| unavailable())?,
                );
                let result = work
                    .completing
                    .as_mut()
                    .expect("checkpoint completion")
                    .await;
                work.completing = None;
                work.producer = Some(result.map_err(crate::raft::execution::storage)??);
                let command = work
                    .producer
                    .as_ref()
                    .expect("checkpoint producer")
                    .command()
                    .clone();
                submit_checkpoint(
                    &commits.runtime,
                    &token,
                    &command,
                    retained,
                    Some(commits.deadline),
                    || check(),
                )
                .await
            });
        // A rejected spawn drops its future/return guard synchronously. The
        // owner exists already so physical custody is joined on that path too.
        owner.task = Some(task.map_err(|_| unavailable())?);
        Ok(owner)
    }

    fn poll_result(&mut self, cx: &mut Context<'_>, closing: bool) -> Poll<CheckpointReturn> {
        if let Some(task) = &mut self.task {
            let result = match Pin::new(&mut *task).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };
            self.outcome = Some(match result {
                Ok(result) => result,
                Err(_) if closing && task.was_cancelled() => Ok(false),
                Err(error) => Err(crate::raft::execution::storage(error)),
            });
            self.task = None;
            self.work = self.returned.lock().unwrap().take();
        }
        if let Some(work) = &mut self.work {
            match work.preparation.poll_join(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => self.outcome = Some(Err(error)),
                Poll::Ready(Ok(())) => (),
            }
            if let Some(task) = &mut work.completing {
                let result = match Pin::new(task).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result,
                };
                work.completing = None;
                match result
                    .map_err(crate::raft::execution::storage)
                    .and_then(|result| result)
                {
                    Ok(producer) => work.producer = Some(producer),
                    Err(error) => self.outcome = Some(Err(error)),
                }
            }
            // Only the SQL owner can drain unentered completion input. The
            // entered task above has joined before any preview is retired.
            let unentered = work.completion_input.lock().unwrap().take();
            if let Some((image, _grant)) = unentered {
                if let Err(error) = image.discard() {
                    self.outcome = Some(Err(error));
                }
            }
        }
        Poll::Ready(CheckpointReturn {
            producer: self.work.take().and_then(|work| work.producer),
            outcome: self.outcome.take().unwrap_or_else(|| Err(unavailable())),
        })
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<CheckpointReturn> {
        self.poll_result(cx, false)
    }

    pub(super) fn join(&mut self, waker: &Waker) -> CheckpointReturn {
        if let Some(task) = &self.task {
            task.abort();
        }
        // A rejected spawn can have returned custody without a task handle.
        if self.task.is_none() && self.work.is_none() {
            self.work = self.returned.lock().unwrap().take();
        }
        let mut cx = Context::from_waker(waker);
        loop {
            match self.poll_result(&mut cx, true) {
                Poll::Pending => std::thread::park(),
                Poll::Ready(result) => return result,
            }
        }
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

impl Drop for CheckpointTask {
    fn drop(&mut self) {
        if self.task.is_some() || self.work.is_some() || self.returned.lock().unwrap().is_some() {
            let result = self.join(&Waker::from(Arc::new(ThreadWake(std::thread::current()))));
            if let Err(error) = result.outcome {
                tracing::error!(%error, "failed to join checkpoint submission");
            }
        }
    }
}

#[cfg(test)]
#[path = "checkpoint_task_tests.rs"]
mod tests;
