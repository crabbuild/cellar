// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Between proven roots, retain an invocation without retaining its writer.

use super::*;

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
