// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Prepare a rooted workspace without occupying its SQL owner. The original
//! request/continuation retains all admission until this physical owner joins.

use super::cleanup_task::CleanupTask;
use super::*;
use crate::raft::execution::CellPrivateCleanup;
use futures_util::future::BoxFuture;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

pub(super) struct SqlOpening {
    options: CellSqlOptions,
    cell: CellHistory,
    domain: crate::asyncrt::DomainToken,
    lane: Arc<Semaphore>,
    waiting: Option<BoxFuture<'static, Result<OwnedSemaphorePermit, AcquireError>>>,
    // One lane per existing SQL worker preserves its prior maximum of one
    // workspace preparation. It remains owned through adoption or cleanup.
    permit: Option<OwnedSemaphorePermit>,
    task: Option<crate::asyncrt::TaskHandle<Result<(), ExecutionError>>>,
    returned: Arc<Mutex<Option<RootPrivateOptions>>>,
    physical_failure: bool,
    entered: Arc<AtomicBool>,
    prepared: Option<RootPrivateOptions>,
    cleanup: Option<CleanupTask>,
    denied: Option<ExecutionError>,
    finished: bool,
}

impl SqlOpening {
    pub(super) fn new(
        options: &CellSqlOptions,
        cell: &CellHistory,
        domain: &crate::asyncrt::DomainToken,
        lane: Arc<Semaphore>,
    ) -> Self {
        Self {
            options: options.clone(),
            cell: cell.clone(),
            domain: domain.clone(),
            lane,
            waiting: None,
            permit: None,
            task: None,
            returned: Arc::new(Mutex::new(None)),
            physical_failure: false,
            entered: Arc::new(AtomicBool::new(false)),
            prepared: None,
            cleanup: None,
            denied: None,
            finished: false,
        }
    }

    // The two callers open a command/enrollment on their SQL thread. Success
    // must adopt the workspace before this owner's extra pin can be released.
    pub(super) fn poll_with<T>(
        &mut self,
        cx: &mut Context<'_>,
        admission: Result<(), ExecutionError>,
        open: impl FnOnce(Option<RootPrivateOptions>) -> Result<T, ExecutionError>,
    ) -> Poll<Result<T, ExecutionError>> {
        if self.finished {
            return Poll::Ready(Err(ExecutionError::Invalid("consumed SQL opening")));
        }
        if self.denied.is_none() {
            self.denied = admission.err();
        }
        let mut open = Some(open);
        loop {
            if let Some(cleanup) = &mut self.cleanup {
                let result = match cleanup.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result,
                };
                self.cleanup = None;
                self.finished = true;
                return Poll::Ready(match result {
                    Err(error) => {
                        self.physical_failure = true;
                        Err(error)
                    }
                    Ok(()) => Err(self.denied.take().unwrap_or_else(unavailable)),
                });
            }
            if let Some(task) = &mut self.task {
                let joined = match Pin::new(task).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(joined) => joined,
                };
                self.task = None;
                // The blocking task returns no workspace value. Its last pin
                // is in this owner's slot even if task delivery is cancelled.
                self.prepared = self.returned.lock().unwrap().take();
                if let Err(error) = joined
                    .map_err(crate::raft::execution::storage)
                    .and_then(|r| r)
                {
                    if self.entered.load(Ordering::Acquire) {
                        self.physical_failure = !matches!(
                            error,
                            ExecutionError::Refused(_)
                                | ExecutionError::Busy
                                | ExecutionError::Invalid(_)
                        );
                        self.denied = Some(error);
                    } else {
                        // Rejected/cancelled before entry: no physical operation
                        // began. Keep an earlier deadline/authority refusal.
                        self.denied.get_or_insert_with(unavailable);
                    }
                }
            }
            if self.denied.is_some() {
                if let Some(root) = self.prepared.take() {
                    self.cleanup = Some(CleanupTask::new(
                        CellPrivateCleanup::unopened_workspace(root),
                        &self.domain,
                    ));
                    continue;
                }
                self.waiting = None;
                self.finished = true;
                return Poll::Ready(Err(self.denied.take().unwrap_or_else(unavailable)));
            }
            let rooted = matches!(self.options, CellSqlOptions::Extents(_));
            if rooted && self.prepared.is_none() {
                if self.permit.is_none() {
                    if self.waiting.is_none() {
                        let lane = self.lane.clone();
                        self.waiting = Some(Box::pin(async move { lane.acquire_owned().await }));
                    }
                    let permit = match self
                        .waiting
                        .as_mut()
                        .expect("workspace lane wait")
                        .as_mut()
                        .poll(cx)
                    {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(permit)) => permit,
                        Poll::Ready(Err(_)) => {
                            self.finished = true;
                            return Poll::Ready(Err(unavailable()));
                        }
                    };
                    self.waiting = None;
                    self.permit = Some(permit);
                }
                let CellSqlOptions::Extents(options) = &self.options else {
                    return Poll::Ready(Err(ExecutionError::Invalid("workspace preparation kind")));
                };
                let options = options.clone();
                let cell = self.cell.clone();
                let returned = self.returned.clone();
                let entered = self.entered.clone();
                match self.domain.blocking(move || {
                    entered.store(true, Ordering::Release);
                    let root = options.prepare(&cell)?;
                    *returned.lock().unwrap() = Some(root);
                    Ok(())
                }) {
                    Ok(task) => self.task = Some(task),
                    Err(_) => {
                        self.finished = true;
                        return Poll::Ready(Err(unavailable()));
                    }
                }
                continue;
            }
            let result =
                open.take().expect("one SQL open per preparation")(self.prepared.as_ref().cloned());
            match result {
                Ok(opened) => {
                    // The returned SQL owner now pins this same workspace.
                    self.prepared = None;
                    self.finished = true;
                    return Poll::Ready(Ok(opened));
                }
                Err(error) => {
                    self.denied = Some(error);
                    // Retaining the original pin across `open` prevents a
                    // rejected clone from doing final cleanup on SQL.
                }
            }
        }
    }

    pub(super) fn close(&mut self, waker: &Waker) -> Result<(), ExecutionError> {
        if self.finished {
            return Ok(());
        }
        let mut cx = Context::from_waker(waker);
        loop {
            match self.poll_with::<()>(&mut cx, Err(unavailable()), |_| Err(unavailable())) {
                Poll::Pending => std::thread::park(),
                Poll::Ready(Err(error)) => {
                    return if self.physical_failure {
                        Err(error)
                    } else {
                        Ok(())
                    }
                }
                Poll::Ready(Ok(())) => return Ok(()),
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

impl Drop for SqlOpening {
    fn drop(&mut self) {
        let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
        if let Err(error) = self.close(&waker) {
            tracing::error!(%error, "failed to join SQL workspace preparation");
        }
    }
}

#[cfg(test)]
#[path = "workspace_opening_tests.rs"]
mod tests;
