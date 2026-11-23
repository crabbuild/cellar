// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded physical apply progresses independently of the owning SQL worker.

use super::*;
use crate::raft::cell_groups::{
    CellGroupApplier, CellGroupApply, CellGroupRuntime, GroupManifest, PreparedGroupManifest,
};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, Stream};
use sha2::{Digest, Sha256};
use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

type ApplyResult = Result<CellGroupPreparedApply, ExecutionError>;

pub(in crate::raft::host_sql) struct ApplyRequest {
    pub group: GroupHistory,
    applier: CellGroupApplier,
    work: CellGroupApply,
    cache: Arc<super::apply_cache::ApplyCache>,
    cached: Option<crate::raft::cell_groups::PrefetchedApply>,
    prefetch_admission: Arc<Budget>,
    prefetch_retention: Arc<std::sync::OnceLock<Arc<crate::raft::budget::Permit>>>,
    business_admission: Arc<Admission>,
    reply: oneshot::Sender<ApplyResult>,
    _slot: Arc<tokio::sync::OwnedSemaphorePermit>,
    _queued_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    queued: crate::raft::performance::Timer,
}

/// Physical apply retains its independent byte/item admission until joined and
/// delivered. A shared completion-lane lease survives physical work. Dequeue
/// releases only its one-entry queue permit, so unrelated groups can enter the
/// existing apply bank while earlier I/O is blocked. Task metadata is bounded too.
pub(in crate::raft::host_sql) struct ApplyTasks {
    tasks: FuturesUnordered<BoxFuture<'static, Result<(), crate::asyncrt::TaskPanic>>>,
    capacity: usize,
    waker: Waker,
}

impl ApplyTasks {
    pub fn new(waker: Waker, capacity: usize) -> Self {
        Self {
            tasks: FuturesUnordered::new(),
            capacity,
            waker,
        }
    }

    fn poll_one(&mut self) -> Poll<Option<Result<(), ExecutionError>>> {
        let mut context = Context::from_waker(&self.waker);
        Pin::new(&mut self.tasks)
            .poll_next(&mut context)
            .map(|value| {
                value.map(|result| {
                    result.map_err(|error| ExecutionError::Storage(anyhow::Error::new(error)))
                })
            })
    }

    pub fn poll(&mut self) -> Result<(), ExecutionError> {
        while let Poll::Ready(Some(result)) = self.poll_one() {
            result?;
        }
        Ok(())
    }

    pub fn start(
        &mut self,
        mut request: ApplyRequest,
        domain: &crate::asyncrt::DomainToken,
        stopped: Arc<AtomicBool>,
        performance: Arc<crate::raft::performance::Registry>,
    ) -> Result<(), ExecutionError> {
        self.poll()?;
        // The node bank bounds both physical work and join custody. Keep a
        // fixed handle bound too, without accumulating escaped task metadata.
        while self.tasks.len() >= self.capacity {
            match self.poll_one() {
                Poll::Ready(Some(result)) => result?,
                Poll::Ready(None) => break,
                Poll::Pending => std::thread::park(),
            }
        }
        let admission = request
            .work
            .ticket
            .preparation
            .clone()
            .ok_or(ExecutionError::Invalid("unadmitted physical group apply"))?;
        let lane = request._slot.clone();
        let cached = request
            .cached
            .as_ref()
            .map(|value| value.reservation.clone());
        let prefetch = request.prefetch_retention.clone();
        request._queued_slot.take();
        let owner = domain.clone();
        let physical = domain
            .blocking(move || request.execute(&owner, &stopped, &performance))
            .map_err(|_| unavailable())?;
        self.tasks.push(Box::pin(async move {
            // Keep ownership through the actual task join. Deliver only after
            // releasing join custody, including a failed preparation's physical
            // reservations, before sending its result.
            let retention = (admission, lane, cached, prefetch);
            let joined = physical.await;
            drop(retention);
            let (reply, result) = joined?;
            let _ = reply.send(result);
            Ok(())
        }));
        Ok(())
    }

    pub fn join(&mut self) -> Result<(), ExecutionError> {
        let mut failure = None;
        loop {
            match self.poll_one() {
                Poll::Ready(Some(Err(error))) => {
                    failure.get_or_insert(error);
                }
                Poll::Ready(Some(Ok(()))) => (),
                Poll::Ready(None) => return failure.map_or(Ok(()), Err),
                // A wake before parking leaves an unpark token. Neither close
                // nor a failed sibling task can abandon entered physical work.
                Poll::Pending => std::thread::park(),
            }
        }
    }
}

impl Drop for ApplyTasks {
    fn drop(&mut self) {
        // SQL-owner unwinding must also join file I/O before releasing locks.
        if let Err(error) = self.join() {
            tracing::error!(%error, "failed to join physical group apply");
        }
    }
}

/// Retains physical preparation and its admission through delivery, retries and
/// owner completion. Successful submission is not a durable apply acknowledgement.
#[derive(Clone)]
pub struct CellGroupPreparedApply {
    work: CellGroupApply,
    prepared: PreparedGroupManifest,
}

impl CellGroupPreparedApply {
    pub fn manifest(&self) -> &GroupManifest {
        self.prepared.marker()
    }

    pub async fn complete(self, runtime: &CellGroupRuntime) -> Result<(), RuntimeError> {
        runtime
            .complete_apply(
                self.prepared.marker().route().history,
                self.work,
                self.prepared,
            )
            .await
    }
}

impl CellGroupSqlPool {
    /// Runs image and manifest I/O in bounded domain work owned by the fixed
    /// SQL workers, without occupying their callback lane. A cancelled
    /// waiter does not cancel physical work or release its ticket reservations.
    pub async fn prepare_apply(
        &self,
        applier: &CellGroupApplier,
        mut work: CellGroupApply,
    ) -> Result<CellGroupPreparedApply, ExecutionError> {
        self.admit()?;
        if !applier.belongs_to_engine(&self.inner.engine_owner) {
            return Err(ExecutionError::Invalid("apply pool engine"));
        }
        let bytes = applier.reservation_bytes(&work)?;
        let mut cached = self.inner.apply_cache.take(applier.history())?;
        let reservation = match self.inner.apply_admission.acquire(bytes) {
            Some(permit) => permit,
            None => {
                // Discard joined immutable prefetches before refusing original
                // apply work. This is admission only, never a mutation retry.
                let count = usize::from(cached.take().is_some()) + self.inner.apply_cache.clear();
                self.inner.performance.increment(
                    crate::raft::performance::Counter::CellApplyPrefetchEvictions,
                    count as u64,
                );
                self.inner
                    .apply_admission
                    .acquire(bytes)
                    .ok_or_else(overloaded)?
            }
        };
        let reservation = Arc::new(reservation);
        // This fixed-size anchor is retained by ApplyTasks outside the
        // physical closure. Any late permit survives through the actual join.
        let prefetch_retention = Arc::new(std::sync::OnceLock::new());
        work.ticket.preparation = Some(reservation.clone());
        let group = applier.history();
        let mut hash = Sha256::new();
        for id in [
            group.cluster.bytes(),
            group.namespace.bytes(),
            group.group.bytes(),
            group.incarnation.bytes(),
        ] {
            hash.update(id);
        }
        let hash: [u8; 32] = hash.finalize().into();
        let index = u64::from_le_bytes(
            hash[..8]
                .try_into()
                .map_err(|_| ExecutionError::Invalid("apply worker key"))?,
        ) % self.inner.workers.len() as u64;
        let worker = &self.inner.workers[index as usize];
        worker.domain.mono_ms().map_err(|_| unavailable())?;
        let (slot, queued_slot) = worker
            .completion_slots
            .acquire_queued(|| Ok(reservation))
            .await?;
        self.admit()?;
        let (reply, wait) = oneshot::channel();
        worker
            .sender
            .try_send(Envelope::GroupApply(Box::new(ApplyRequest {
                group,
                applier: applier.clone(),
                work,
                cache: self.inner.apply_cache.clone(),
                cached,
                prefetch_admission: self.inner.apply_admission.clone(),
                prefetch_retention,
                business_admission: self.inner.admission.clone(),
                reply,
                _slot: slot,
                _queued_slot: Some(queued_slot),
                queued: worker
                    .performance
                    .timer(crate::raft::performance::Operation::SqlQueue),
            })))
            .map_err(|_| unavailable())?;
        wait.await.map_err(|_| unavailable())?
    }
}

impl ApplyRequest {
    pub fn execute(
        self,
        domain: &crate::asyncrt::DomainToken,
        stopped: &AtomicBool,
        performance: &Arc<crate::raft::performance::Registry>,
    ) -> (oneshot::Sender<ApplyResult>, ApplyResult) {
        let Self {
            applier,
            work,
            cache,
            cached,
            prefetch_admission,
            prefetch_retention,
            business_admission,
            reply,
            _slot,
            queued,
            ..
        } = self;
        let timer = performance.timer(crate::raft::performance::Operation::SqlPrepare);
        let result = if stopped.load(Ordering::Acquire) || domain.mono_ms().is_err() {
            Err(unavailable())
        } else {
            queued.finish(true);
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                applier.prepare(&work, performance, cached, |bytes| {
                    if stopped.load(Ordering::Acquire)
                        || domain.mono_ms().is_err()
                        || !business_admission.may_prefetch(work.base.marker().route().history)?
                    {
                        return Ok(None);
                    }
                    let Some(permit) = prefetch_admission.acquire(bytes) else {
                        performance.increment(
                            crate::raft::performance::Counter::CellApplyPrefetchAdmissionRefusals,
                            1,
                        );
                        return Ok(None);
                    };
                    let permit = Arc::new(permit);
                    prefetch_retention.set(permit.clone()).map_err(|_| {
                        ExecutionError::Invalid("late prefetch admission already anchored")
                    })?;
                    Ok(Some(permit))
                })
            }))
            .unwrap_or_else(|panic| {
                Err(crate::raft::execution::storage(
                    crate::native_worker::panic_error(panic),
                ))
            })
            .and_then(|prepared| {
                if stopped.load(Ordering::Acquire) || domain.mono_ms().is_err() {
                    return Err(unavailable());
                }
                let crate::raft::cell_groups::ApplyPreparation {
                    first,
                    next,
                    reused,
                } = prepared;
                if let Some(next) = next {
                    cache.insert(work.base.marker().route().history, next, || {
                        business_admission.may_prefetch(work.base.marker().route().history)
                    })?;
                    performance
                        .increment(crate::raft::performance::Counter::CellApplyPreparedPairs, 1);
                }
                if reused {
                    performance
                        .increment(crate::raft::performance::Counter::CellApplyPrefetchHits, 1);
                }
                Ok(CellGroupPreparedApply {
                    work,
                    prepared: first,
                })
            })
        };
        timer.finish(result.is_ok());
        (reply, result)
    }
}
