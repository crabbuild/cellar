// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Join physical cleanup before returning any of its SQL owner's custody.

use super::*;
use crate::raft::execution::CellPrivateCleanup;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};

pub(super) struct CleanupTask {
    task: Option<crate::asyncrt::TaskHandle<Result<(), ExecutionError>>>,
    input: Arc<Mutex<Option<CellPrivateCleanup>>>,
}

impl CleanupTask {
    pub(super) fn new(input: CellPrivateCleanup, domain: &crate::asyncrt::DomainToken) -> Self {
        // The owner exists before spawning. A rejected/unentered closure owns
        // only a clone of this slot, never its last physical workspace pin.
        let mut owner = Self {
            task: None,
            input: Arc::new(Mutex::new(Some(input))),
        };
        let input = owner.input.clone();
        owner.task = domain
            .blocking(move || {
                let input = input.lock().unwrap().take().ok_or_else(unavailable)?;
                input.finish()
            })
            .ok();
        owner
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ExecutionError>> {
        let joined = if let Some(task) = &mut self.task {
            let joined = match Pin::new(task).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(joined) => joined,
            };
            self.task = None;
            joined
                .map_err(crate::raft::execution::storage)
                .and_then(|result| result)
        } else {
            Ok(())
        };
        let unentered = self.input.lock().unwrap().take();
        Poll::Ready(match unentered {
            // Admission refusal/cancellation never entered physical cleanup.
            // Finish it on the original blocking owner, as before the handoff.
            Some(input) => input.finish(),
            None => joined,
        })
    }

    pub(super) fn join(&mut self, waker: &Waker) -> Result<(), ExecutionError> {
        let mut cx = Context::from_waker(waker);
        loop {
            match self.poll(&mut cx) {
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

impl Drop for CleanupTask {
    fn drop(&mut self) {
        let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
        if let Err(error) = self.join(&waker) {
            tracing::error!(%error, "failed to join private cleanup");
        }
    }
}

#[cfg(test)]
#[path = "cleanup_task_tests.rs"]
mod tests;
