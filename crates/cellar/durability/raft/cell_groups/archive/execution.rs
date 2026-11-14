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

pub(crate) struct ControllerWork(Arc<Physical>);

impl Drop for ControllerWork {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.finished.notify_waiters();
    }
}

/// One controller's admitted async work and physical joins, including closures
/// whose async waiter has disappeared. Never joins unrelated domain work.
#[derive(Clone)]
pub(crate) struct ControllerExecution {
    domain: DomainToken,
    physical: Arc<Physical>,
}

impl ControllerExecution {
    pub(crate) fn new(domain: DomainToken) -> Self {
        Self {
            domain,
            physical: Arc::new(Physical::default()),
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.physical.active.load(Ordering::Acquire) == 0
    }

    /// Retain async ownership before returning to the controller's admission
    /// loop. Physical children retain their own work after cancellation.
    pub(crate) fn retain(&self) -> ControllerWork {
        self.physical.active.fetch_add(1, Ordering::AcqRel);
        ControllerWork(self.physical.clone())
    }

    pub(crate) async fn join(&self) {
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
        let entered = self.retain();
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
