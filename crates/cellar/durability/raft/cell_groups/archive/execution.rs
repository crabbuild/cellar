// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Scheduling capability without ownership of execution-domain shutdown.
use crate::asyncrt::{DomainClosed, DomainToken, ExecutionDomain, TaskHandle};
use std::future::Future;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

mod sealed {
    pub trait Sealed {}

    impl Sealed for super::ExecutionDomain {}

    impl Sealed for super::DomainToken {}

    impl Sealed for super::ControllerExecution {}
}

#[derive(Default)]
struct Physical {
    active: AtomicUsize,
    finished: Notify,
}

struct Entered(Arc<Physical>);

impl Drop for Entered {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.finished.notify_waiters();
    }
}

/// One controller's physical joins, including closures whose async waiter has
/// already failed or disappeared. Does not join unrelated execution-domain work.
#[derive(Clone)]
pub(in crate::raft::cell_groups) struct ControllerExecution {
    domain: DomainToken,
    physical: Arc<Physical>,
}

impl ControllerExecution {
    pub(in crate::raft::cell_groups) fn new(domain: DomainToken) -> Self {
        Self {
            domain,
            physical: Arc::new(Physical::default()),
        }
    }

    pub(in crate::raft::cell_groups) async fn join(&self) {
        loop {
            let finished = self.physical.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if self.physical.active.load(Ordering::Acquire) == 0 {
                return;
            }
            finished.await;
        }
    }
}

impl CellGroupArchiveExecution for ControllerExecution {
    fn run<F>(&self, future: F) -> impl Future<Output = Result<F::Output, DomainClosed>> + Send
    where
        F: Future + Send,
        F::Output: Send,
    {
        self.domain.run(future)
    }

    fn blocking<T, F>(&self, operation: F) -> Result<TaskHandle<T>, DomainClosed>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.physical.active.fetch_add(1, Ordering::AcqRel);
        let entered = Entered(self.physical.clone());
        self.domain.blocking(move || {
            let _entered = entered;
            operation()
        })
    }
}

/// Archive I/O runs on its owning execution domain. Sealed implementations let
/// node controllers use their captured token without acquiring shutdown rights.
pub trait CellGroupArchiveExecution: sealed::Sealed + Send + Sync {
    fn run<F>(&self, future: F) -> impl Future<Output = Result<F::Output, DomainClosed>> + Send
    where
        F: Future + Send,
        F::Output: Send;

    fn blocking<T, F>(&self, operation: F) -> Result<TaskHandle<T>, DomainClosed>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static;
}

impl CellGroupArchiveExecution for ExecutionDomain {
    fn run<F>(&self, future: F) -> impl Future<Output = Result<F::Output, DomainClosed>> + Send
    where
        F: Future + Send,
        F::Output: Send,
    {
        ExecutionDomain::run(self, future)
    }

    fn blocking<T, F>(&self, operation: F) -> Result<TaskHandle<T>, DomainClosed>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        ExecutionDomain::blocking(self, operation)
    }
}

impl CellGroupArchiveExecution for DomainToken {
    fn run<F>(&self, future: F) -> impl Future<Output = Result<F::Output, DomainClosed>> + Send
    where
        F: Future + Send,
        F::Output: Send,
    {
        DomainToken::run(self, future)
    }

    fn blocking<T, F>(&self, operation: F) -> Result<TaskHandle<T>, DomainClosed>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        DomainToken::blocking(self, operation)
    }
}

#[cfg(test)]
#[path = "execution_tests.rs"]
mod tests;
