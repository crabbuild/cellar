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
        let reservation = self
            .inner
            .apply_admission
            .acquire(bytes)
            .ok_or_else(overloaded)?;
        work.ticket.preparation = Some(Arc::new(reservation));
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
        let slot = worker
            .completion_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| overloaded())?;
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
            })))
            .map_err(|_| unavailable())?;
        wait.await.map_err(|_| unavailable())?
    }
}

impl ApplyRequest {
    pub fn execute(self, domain: &crate::asyncrt::DomainToken, stopped: &AtomicBool) {
        let Self {
            applier,
            work,
            reply,
            _slot,
            ..
        } = self;
        let result = if stopped.load(Ordering::Acquire) || domain.mono_ms().is_err() {
            Err(unavailable())
        } else {
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
        let _ = reply.send(result);
    }
}
