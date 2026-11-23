// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-owned SDK dispatch after an independently proven source root.

use super::*;
use crate::asyncrt::DomainToken;
use crate::raft::cell_groups::CellActivationFence;
use std::{future::Future, task::Poll, time::Duration};

#[path = "dispatch_reply.rs"]
mod reply;

#[derive(Clone)]
pub(crate) struct RaftDispatchContext {
    pub domain: DomainToken,
    pub deadline: u64,
    pub result_limit: usize,
    /// The private producer still enforces the complete encoded envelope bound.
    pub checkpoint_limit: usize,
    pub fence: CellActivationFence,
    pub binding_source: Option<crate::application::cell_bindings::Source>,
}

impl RaftDispatchContext {
    /// Capture the original wall deadline once for all captures and checkpoints
    /// of this invocation. Re-converting a remaining monotonic duration later
    /// can renew expiry through clock changes or millisecond sample skew.
    pub(crate) fn pin_source_deadline(mut self) -> Result<Self> {
        if let Some(source) = &mut self.binding_source {
            // Sample wall time first so time between the two samples cannot
            // extend the original monotonic budget.
            let wall = u64::try_from(crate::asyncrt::wall_ms())?;
            let now = self
                .domain
                .mono_ms()
                .map_err(|_| consensus::RuntimeError::Closed)?;
            let remaining = self
                .deadline
                .checked_sub(now)
                .filter(|n| *n != 0)
                .ok_or(consensus::RuntimeError::DeadlineExceeded)?;
            let deadline = wall
                .checked_add(remaining)
                .ok_or(consensus::CommandFailure::Overloaded)?;
            source.deadline_unix_ms = Some(
                source
                    .deadline_unix_ms
                    .map_or(deadline, |old| old.min(deadline)),
            );
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub(crate) struct RaftDispatch {
    authority: RaftStorageAuthority,
    context: RaftDispatchContext,
    target: Option<(DomainToken, u64, u64)>,
}

/// Original invocation authority for owned I/O and queued response delivery.
/// The I/O task separately proves exact apply and required archival before
/// entering the target. This permit owns no SQLite/image pin.
pub(crate) struct RaftDispatchPermit(RaftDispatch);

pub(crate) struct DispatchCall<T> {
    pub future: HandlerFuture<T>,
    pub started: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
enum SourceProof {
    Checkpoint,
    Read,
    Effect,
}

/// Only framework-owned work can switch between pure external I/O and source
/// SQLite mutations. Requesting the writer disables parked polling before any
/// mutation, and wakes the same SQL owner to resume it.
pub(crate) struct RaftDispatchIo {
    authority: RaftStorageAuthority,
    parked_io: Arc<AtomicBool>,
}

impl RaftDispatchIo {
    pub(crate) fn begin_io(&self) -> Result<()> {
        crate::storage::require_raft_dispatch_storage(&self.authority)?;
        self.parked_io.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) async fn resume_writer(&self) -> Result<()> {
        self.parked_io.store(false, Ordering::Release);
        futures_util::future::poll_fn(|cx| {
            match crate::storage::raft_dispatch_has_writer(&self.authority) {
                Ok(true) => Poll::Ready(Ok(())),
                Ok(false) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(error) => Poll::Ready(Err(error)),
            }
        })
        .await
    }
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

    pub(crate) fn checkpoint_limit(&self) -> Result<usize> {
        self.remaining()?;
        Ok(self.context.checkpoint_limit)
    }

    pub(crate) fn result_limit(&self) -> Result<usize> {
        self.remaining()?;
        Ok(self.context.result_limit)
    }

    /// Read-only capability metadata is available inside a savepoint. This
    /// neither submits a child nor grants permission to suspend its writer.
    pub(crate) fn event_wire_limit(
        &self,
        binding: &super::super::event_stream::Binding,
    ) -> Result<usize> {
        self.remaining()?;
        let source = self
            .context
            .binding_source
            .as_ref()
            .ok_or(consensus::CommandFailure::UnsupportedCapability)?;
        let scope = super::super::event_stream::cell_scope(&binding.resource)?;
        let authority = consensus::native_event_rpc::Authority::Event(
            super::super::event_stream::Authority::producer(binding, source.history.cell.as_str())?,
        );
        let route = source.host.event_route(
            &source.history,
            source.configuration,
            &scope,
            "__streamAppend",
            &authority,
        )?;
        Ok(route
            .max_input_bytes
            .min(route.max_result_bytes.saturating_sub(4096)))
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
        let original = source
            .deadline_unix_ms
            .ok_or(consensus::CommandFailure::Unavailable)?;
        source.deadline_unix_ms = Some(
            self.target
                .as_ref()
                .map_or(original, |(_, _, target)| original.min(*target)),
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
        let wall = u64::try_from(crate::asyncrt::wall_ms())?;
        let started = domain
            .mono_ms()
            .map_err(|_| consensus::RuntimeError::Closed)?;
        // A second limit can only narrow the first; it cannot restart its
        // timeout or replace a shorter deadline from another domain.
        let timeout = timeout.min(self.remaining()?);
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
        let wall_deadline = wall
            .checked_add(u64::try_from(timeout.as_millis())?)
            .ok_or(consensus::CommandFailure::Overloaded)?;
        let wall_deadline = self
            .target
            .as_ref()
            .map_or(wall_deadline, |(_, _, old)| wall_deadline.min(*old));
        self.target = Some((domain, deadline, wall_deadline));
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
        if let Some((domain, deadline, _)) = &self.target {
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
        self.try_schedule_inner(
            action,
            Arc::new(AtomicBool::new(false)),
            SourceProof::Checkpoint,
        )
    }

    /// The binding adapter alone owns both its remote I/O and private intent
    /// updates. Every source mutation explicitly resumes SQLite; the source
    /// checkpoint proof still precedes the first external poll.
    pub(crate) fn try_schedule_binding<T, F, Fut>(self, action: F) -> Result<DispatchCall<T>>
    where
        T: Send + 'static,
        F: FnOnce(RaftDispatchIo) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let parked_io = Arc::new(AtomicBool::new(true));
        let io = RaftDispatchIo {
            authority: self.authority.clone(),
            parked_io: parked_io.clone(),
        };
        self.try_schedule_inner(move |_| action(io), parked_io, SourceProof::Checkpoint)
    }

    /// Target calls and payload adapters execute owned I/O after source proof.
    /// They can progress while their writer is parked; they cannot access SQLite or
    /// poll the application continuation until that writer resumes.
    pub(crate) fn schedule_io<T, F, Fut>(self, action: F) -> DispatchCall<T>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.schedule_owned_io(action, SourceProof::Checkpoint)
    }

    /// Read-only target queries and provider GET bodies may reuse an unchanged
    /// source proof. This never authorizes a provider or target mutation.
    pub(crate) fn schedule_read<T, F, Fut>(self, action: F) -> DispatchCall<T>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.schedule_owned_io(action, SourceProof::Read)
    }

    /// Provider effects may reuse only this exact invocation's unchanged,
    /// proven unfinished receipt. Entry/read authority alone cannot admit them.
    pub(crate) fn schedule_effect<T, F, Fut>(self, action: F) -> DispatchCall<T>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.schedule_owned_io(action, SourceProof::Effect)
    }

    fn schedule_owned_io<T, F, Fut>(self, action: F, proof: SourceProof) -> DispatchCall<T>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        match self.try_schedule_inner(action, Arc::new(AtomicBool::new(true)), proof) {
            Ok(call) => call,
            Err(error) => DispatchCall {
                started: Arc::new(AtomicBool::new(false)),
                future: Box::pin(async move { Err(error) }),
            },
        }
    }

    fn try_schedule_inner<T, F, Fut>(
        self,
        action: F,
        parked_io: Arc<AtomicBool>,
        proof: SourceProof,
    ) -> Result<DispatchCall<T>>
    where
        T: Send + 'static,
        F: FnOnce(Duration) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let read_only = matches!(proof, SourceProof::Read);
        let started = Arc::new(AtomicBool::new(false));
        let submitted = started.clone();
        let response_permit = RaftDispatchPermit(self.clone());
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
                if read_only {
                    tracing::debug!(target: "cellar::raft::resource_read", phase = "source-queued",
                        source = ?self.context.binding_source.as_ref().map(|s| &s.history.cell),
                        at_ms = crate::asyncrt::mono_ms());
                }
                let proof = async {
                    slot.admission.clone().await.map_err(anyhow::Error::msg)?;
                    self.remaining()?;
                    if read_only {
                        tracing::debug!(target: "cellar::raft::resource_read", phase = "source-proving",
                            source = ?self.context.binding_source.as_ref().map(|s| &s.history.cell),
                            at_ms = crate::asyncrt::mono_ms());
                    }
                    match proof {
                        SourceProof::Read => crate::storage::confirm_raft_read_work(&self.authority).await,
                        SourceProof::Effect => crate::storage::confirm_raft_checkpoint_work(&self.authority).await,
                        SourceProof::Checkpoint => crate::storage::commit_raft_work(&self.authority).await,
                    }
                }
                .await;
                drop(slot);
                let result = async {
                    proof?;
                    let timeout = self.remaining()?;
                    if read_only {
                        tracing::debug!(target: "cellar::raft::resource_read", phase = "source-proven",
                            source = ?self.context.binding_source.as_ref().map(|s| &s.history.cell),
                            at_ms = crate::asyncrt::mono_ms());
                    }
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
            crate::storage::schedule_raft_dispatch_work(
                &authority, task, external, parked_io, read_only,
            )?;
            Ok(receive)
        })();
        let receive = prepared?;
        Ok(DispatchCall {
            started,
            future: Box::pin(reply::receive(
                async move {
                    receive
                        .await
                        .map_err(|_| anyhow!(consensus::RuntimeError::Closed))?
                },
                move || response_permit.check(),
            )),
        })
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
