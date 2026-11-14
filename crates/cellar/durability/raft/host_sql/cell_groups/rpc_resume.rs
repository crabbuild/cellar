// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Between proven roots, retain an invocation without retaining its writer.

use super::*;

impl Suspended {
    pub(super) fn try_park(&mut self, cache: &RpcCache, now: u64) -> Result<(), ExecutionError> {
        if self.commits.is_none() || !self.turn.can_park()? {
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
        now: u64,
    ) -> Poll<Result<(), ExecutionError>> {
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
                loop {
                    if !fence.term_current() {
                        return Err(unavailable());
                    }
                    let now = domain.mono_ms().map_err(|_| unavailable())?;
                    if now >= commits.deadline {
                        return Err(crate::raft::execution::authority_failure(
                            RuntimeError::DeadlineExceeded,
                        ));
                    }
                    match commits
                        .runtime
                        .execution_authority(commits.route.history, cell.clone())
                        .await
                    {
                        Ok(authority) if fence.accepts_continuation(&authority) => {
                            return Ok(authority)
                        }
                        Ok(_) => return Err(unavailable()),
                        Err(RuntimeError::NotReady | RuntimeError::Overloaded) => {
                            domain
                                .sleep_until(now.saturating_add(10))
                                .map_err(|_| unavailable())?
                                .await;
                        }
                        Err(error) => return Err(crate::raft::execution::authority_failure(error)),
                    }
                }
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
        let executor = self.options.clone().open(authority)?;
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
