// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-owned SDK dispatch after an independently proven source root.

use super::*;
use crate::asyncrt::DomainToken;
use crate::raft::cell_groups::CellActivationFence;
use std::{future::Future, task::Poll, time::Duration};

#[derive(Clone)]
pub(crate) struct RaftDispatchContext {
    pub domain: DomainToken,
    pub deadline: u64,
    pub result_limit: usize,
    pub fence: CellActivationFence,
    pub binding_source: Option<crate::application::cell_bindings::Source>,
}

#[derive(Clone)]
pub(crate) struct RaftDispatch {
    authority: RaftStorageAuthority,
    context: RaftDispatchContext,
    target: Option<(DomainToken, u64)>,
}

/// Only the owned checkpoint task can create this permit, after exact apply
/// and the invocation's required archive proof. It owns no SQLite/image pin.
pub(crate) struct RaftDispatchPermit(RaftDispatch);

pub(crate) struct DispatchCall<T> {
    pub future: HandlerFuture<T>,
    pub started: Arc<AtomicBool>,
}

impl RaftDispatchPermit {
    pub(crate) fn check(&self) -> Result<()> {
        self.0.remaining().map(|_| ())
    }
}

impl RaftDispatch {
    /// Metadata from this exact executing history; no dispatch or publication
    /// authority is granted and no transaction is suspended by reading it.
    pub(crate) fn source_history(&self) -> Result<consensus::CellHistory> {
        self.remaining()?;
        Ok(self
            .context
            .binding_source
            .as_ref()
            .ok_or(consensus::CommandFailure::UnsupportedCapability)?
            .history
            .clone())
    }

    pub(crate) fn result_limit(&self) -> Result<usize> {
        self.remaining()?;
        Ok(self.context.result_limit)
    }

    pub(crate) fn binding_source(&self) -> Result<crate::application::cell_bindings::Source> {
        self.remaining()?;
        let resources = &self.authority.resources;
        anyhow::ensure!(
            resources.transaction.lock().unwrap().is_none()
                && resources.open_transaction.lock().unwrap().is_none(),
            consensus::CommandFailure::UnsupportedCapability
        );
        let mut source = self
            .context
            .binding_source
            .clone()
            .ok_or_else(|| anyhow!(consensus::CommandFailure::UnsupportedCapability))?;
        let now = u64::try_from(crate::asyncrt::wall_ms())?;
        let remaining = u64::try_from(self.remaining()?.as_millis())?;
        source.deadline_unix_ms = Some(
            now.checked_add(remaining)
                .ok_or(consensus::CommandFailure::Overloaded)?,
        );
        Ok(source)
    }

    pub(crate) fn binding_hint(
        &self,
        route: &crate::application::cell_bindings::Route,
    ) -> Result<Option<consensus::CellHistory>> {
        self.remaining()?;
        Ok(self
            .context
            .fence
            .history_hint(route.cluster, route.namespace, &route.cell))
    }

    pub(crate) fn limit_source(self, timeout: Duration) -> Result<Self> {
        let domain = self.context.domain.clone();
        self.limit(domain, timeout)
    }

    pub(crate) fn remaining_timeout(&self) -> Result<Duration> {
        self.remaining()
    }

    pub(crate) fn check_current(&self) -> Result<()> {
        self.remaining().map(|_| ())
    }

    pub(crate) async fn wait_admission(&self) -> Result<()> {
        let delay = self.remaining()?.min(Duration::from_millis(10));
        let now = self
            .context
            .domain
            .mono_ms()
            .map_err(|_| consensus::RuntimeError::Closed)?;
        let until = now
            .checked_add(u64::try_from(delay.as_millis())?)
            .ok_or(consensus::CommandFailure::Overloaded)?;
        self.context
            .domain
            .sleep_until(until)
            .map_err(|_| consensus::RuntimeError::Closed)?
            .await;
        self.remaining().map(|_| ())
    }

    pub(crate) fn new(authority: RaftStorageAuthority, context: RaftDispatchContext) -> Self {
        Self {
            authority,
            context,
            target: None,
        }
    }

    pub(crate) fn limit(mut self, domain: DomainToken, timeout: Duration) -> Result<Self> {
        let started = domain
            .mono_ms()
            .map_err(|_| consensus::RuntimeError::Closed)?;
        let deadline = crate::raft::cell_groups::CellRoutingLimits {
            timeout,
            max_attempts: 4,
        }
        .deadline_at(started)
        .ok_or_else(|| {
            anyhow!(consensus::RuntimeError::Invalid(
                "SDK dispatch deadline".into()
            ))
        })?;
        self.target = Some((domain, deadline));
        self.remaining()?;
        Ok(self)
    }

    fn remaining(&self) -> Result<Duration> {
        crate::storage::require_raft_dispatch_storage(&self.authority)?;
        anyhow::ensure!(
            self.context.fence.continuation_current(),
            consensus::CommandFailure::Unavailable
        );
        let now = self
            .context
            .domain
            .mono_ms()
            .map_err(|_| consensus::RuntimeError::Closed)?;
        let mut remaining = self
            .context
            .deadline
            .checked_sub(now)
            .filter(|n| *n != 0)
            .ok_or_else(|| anyhow!(consensus::RuntimeError::DeadlineExceeded))?;
        if let Some((domain, deadline)) = &self.target {
            let now = domain
                .mono_ms()
                .map_err(|_| consensus::RuntimeError::Closed)?;
            remaining = remaining.min(
                deadline
                    .checked_sub(now)
                    .filter(|n| *n != 0)
                    .ok_or_else(|| anyhow!(consensus::RuntimeError::DeadlineExceeded))?,
            );
        }
        Ok(Duration::from_millis(remaining))
    }

    pub(crate) fn schedule<T, F, Fut>(self, action: F) -> DispatchCall<T>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        match self.try_schedule(action) {
            Ok(call) => call,
            Err(error) => DispatchCall {
                started: Arc::new(AtomicBool::new(false)),
                future: Box::pin(async move { Err(error) }),
            },
        }
    }

    pub(crate) fn try_schedule<T, F, Fut>(self, action: F) -> Result<DispatchCall<T>>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.try_schedule_inner(action, false)
    }

    /// Payload adapters execute only owned byte I/O after source proof. They
    /// can progress while their writer is parked; they cannot access SQLite or
    /// poll the application continuation until that writer resumes.
    pub(crate) fn schedule_io<T, F, Fut>(self, action: F) -> DispatchCall<T>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        match self.try_schedule_inner(action, true) {
            Ok(call) => call,
            Err(error) => DispatchCall {
                started: Arc::new(AtomicBool::new(false)),
                future: Box::pin(async move { Err(error) }),
            },
        }
    }

    fn try_schedule_inner<T, F, Fut>(self, action: F, parked_io: bool) -> Result<DispatchCall<T>>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let started = Arc::new(AtomicBool::new(false));
        let submitted = started.clone();
        let prepared = (|| -> Result<_> {
            self.remaining()?;
            let resources = &self.authority.resources;
            anyhow::ensure!(
                resources.transaction.lock().unwrap().is_none()
                    && resources.open_transaction.lock().unwrap().is_none(),
                consensus::CommandFailure::UnsupportedCapability
            );
            let (slot, _) = resources.transaction_queue.reserve();
            let authority = self.authority.clone();
            let (send, receive) = tokio::sync::oneshot::channel();
            let external = Arc::new(AtomicBool::new(false));
            let waiting = external.clone();
            let task = Box::pin(async move {
                let proof = async {
                    slot.admission.clone().await.map_err(anyhow::Error::msg)?;
                    self.remaining()?;
                    crate::storage::commit_raft_work(&self.authority).await
                }
                .await;
                drop(slot);
                let result = async {
                    proof?;
                    let timeout = self.remaining()?;
                    let permit = RaftDispatchPermit(self);
                    waiting.store(true, Ordering::Release);
                    // Admission and every response poll use the original
                    // invocation, term, route and both original deadlines.
                    let mut action = Some(action);
                    let mut future = None;
                    futures_util::future::poll_fn(move |cx| {
                        crate::application::consensus::framework_dispatch(&permit, || {
                            if future.is_none() {
                                submitted.store(true, Ordering::Release);
                                future = Some(Box::pin(action.take().expect("one SDK dispatch")(
                                    timeout,
                                )));
                            }
                            future
                                .as_mut()
                                .expect("owned SDK dispatch")
                                .as_mut()
                                .poll(cx)
                        })
                        .map_or_else(|error| Poll::Ready(Err(error)), |result| result.map(Ok))
                    })
                    .await
                }
                .await;
                let _ = send.send(result);
                Ok(())
            });
            crate::storage::schedule_raft_dispatch_work(&authority, task, external, parked_io)?;
            Ok(receive)
        })();
        let receive = prepared?;
        Ok(DispatchCall {
            started,
            future: Box::pin(async move {
                receive
                    .await
                    .map_err(|_| anyhow!(consensus::RuntimeError::Closed))?
            }),
        })
    }
}
