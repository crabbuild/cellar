// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Apply uses the existing workers' reserved progress lane, independently of SQL producers.

use super::*;
use crate::raft::cell_groups::{
    CellGroupApplier, CellGroupApply, CellGroupRuntime, GroupManifest, PreparedGroupManifest,
};
use sha2::{Digest, Sha256};

pub(in crate::raft::host_sql) struct ApplyRequest {
    pub group: GroupHistory,
    applier: CellGroupApplier,
    work: CellGroupApply,
    reply: oneshot::Sender<Result<CellGroupPreparedApply, ExecutionError>>,
    _slot: tokio::sync::OwnedSemaphorePermit,
    queued: crate::raft::performance::Timer,
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
    /// Runs image and manifest I/O on the existing fixed workers. A cancelled
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
        let reservation = Arc::new(
            self.inner
                .apply_admission
                .acquire(bytes)
                .ok_or_else(overloaded)?,
        );
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
        let queued = worker
            .performance
            .timer(crate::raft::performance::Operation::SqlQueue);
        let slot = progress::reserve(&worker.completion_slots, || Ok(reservation)).await?;
        self.admit()?;
        let (reply, wait) = oneshot::channel();
        worker
            .sender
            .try_send(Envelope::GroupApply(Box::new(ApplyRequest {
                group,
                applier: applier.clone(),
                work,
                reply,
                _slot: slot,
                queued,
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
    ) {
        let Self {
            applier,
            work,
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
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| applier.prepare(&work)))
                .unwrap_or_else(|panic| {
                    Err(crate::raft::execution::storage(
                        crate::native_worker::panic_error(panic),
                    ))
                })
                .and_then(|prepared| {
                    if stopped.load(Ordering::Acquire) || domain.mono_ms().is_err() {
                        return Err(unavailable());
                    }
                    Ok(CellGroupPreparedApply { work, prepared })
                })
        };
        timer.finish(result.is_ok());
        let _ = reply.send(result);
    }
}
