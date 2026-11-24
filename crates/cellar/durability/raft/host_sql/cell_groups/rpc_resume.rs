// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Between proven roots, retain an invocation without retaining its writer.

use super::super::super::workspace_opening::SqlOpening;
use super::*;

pub(super) struct ResumeOpening {
    task: SqlOpening,
    authority: Option<CellExecutionAuthority>,
}

impl ResumeOpening {
    pub(super) fn close(&mut self, waker: &Waker) -> Result<(), ExecutionError> {
        self.task.close(waker)
    }
}

impl Suspended {
    pub(super) fn try_park(&mut self, cache: &RpcCache, now: u64) -> Result<(), ExecutionError> {
        if self.commits.is_none() || !self.turn.can_park()? {
            return Ok(());
        }
        // The source root is proven and only host-owned I/O can be polled.
        // Keep its admitted connection while uncontended, including bindings.
        // Foreground queued or synchronously refused writers mark contention,
        // so a target-write dependency can force parking without an I/O wake.
        // Resource cleanup instead declines busy admission: it cannot progress
        // through a held lifecycle guard and must not force this owner to park.
        // Resume still revalidates the original history/term and receipt.
        if !self.retention.lease.contended() && self.turn.waiting_on_owned_io() {
            return Ok(());
        }
        let mut executor = self.executor.take().ok_or_else(unavailable)?;
        let fence = executor.rpc_activation_fence();
        if let Err(error) = executor.park_rpc(&mut self.turn) {
            self.executor = Some(executor);
            return Err(error);
        }
        // A read VM retains its source image and physical reservation, but no
        // writer admission. New callbacks open a separate bounded image slot.
        self.turn.retain_parked_image(executor)?;
        let activation = self.activation.as_mut().ok_or_else(unavailable)?;
        activation.fence = fence;
        cache.put(self.cell.clone(), activation.clone(), now)?;
        self.retention.lease.park()?;
        Ok(())
    }

    pub(super) fn poll_resume(
        &mut self,
        cache: &RpcCache,
        cx: &mut Context<'_>,
        domain: &crate::asyncrt::DomainToken,
        performance: &crate::raft::performance::Registry,
        lane: Arc<tokio::sync::Semaphore>,
        stopped: &AtomicBool,
    ) -> Poll<Result<(), ExecutionError>> {
        if self.opening.is_none() {
            let now = domain.mono_ms().map_err(|_| unavailable())?;
            if self.resuming.is_none() {
                match self.retention.lease.resume(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => result?,
                }
                let commits = self.commits.as_ref().ok_or_else(unavailable)?.clone();
                let cell = self.cell.clone();
                let fence = self
                    .activation
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .fence
                    .clone();
                let domain = domain.clone();
                self.resuming = Some(Box::pin(async move {
                    commits.continuation_authority(&domain, &cell, &fence).await
                }));
            }
            let authority = match self
                .resuming
                .as_mut()
                .ok_or_else(unavailable)?
                .as_mut()
                .poll(cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            };
            self.resuming = None;
            let prior = self
                .activation
                .as_ref()
                .and_then(|value| value.activation.as_ref())
                .ok_or_else(unavailable)?
                .resources();
            let activation = cache.take(&self.cell, &authority, &self.class, now)?;
            if !activation
                .activation
                .as_ref()
                .is_some_and(|value| Arc::ptr_eq(&value.resources(), &prior))
            {
                cache.put(self.cell.clone(), activation, now)?;
                return Poll::Ready(Err(unavailable()));
            }
            self.activation = Some(activation);
            let task = SqlOpening::new(&self.options, &self.cell, domain, lane);
            let task = match self.mode {
                CellSqlStageMode::InvokeBefore(deadline) => task.wait_for_capacity_before(deadline),
                _ => task,
            };
            self.opening = Some(ResumeOpening {
                task,
                authority: Some(authority),
            });
        }
        let admission = if stopped.load(Ordering::Acquire) {
            Err(unavailable())
        } else {
            domain
                .mono_ms()
                .map_err(|_| unavailable())
                .and_then(|now| self.check(now))
        };
        let opening = self.opening.as_mut().ok_or_else(unavailable)?;
        let opened = opening.task.poll_with(cx, admission, |root| {
            self.options.clone().open(
                root,
                opening.authority.take().ok_or_else(unavailable)?,
                self.root_apply_limits,
            )
        });
        let executor = match opened {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => {
                if matches!(
                    result,
                    Err(ExecutionError::Refused(CommandFailure::Overloaded))
                ) {
                    performance
                        .increment(crate::raft::performance::Counter::SqlPrivateOpenRefusals, 1);
                }
                result?
            }
        };
        self.opening = None;
        self.executor = Some(executor);
        // Recheck the original unfinished receipt in the latest image. Neither
        // another client's commit nor a new leader authorizes handler replay.
        self.executor
            .as_mut()
            .ok_or_else(unavailable)?
            .resume_rpc(&mut self.turn)?;
        Poll::Ready(Ok(()))
    }
}
