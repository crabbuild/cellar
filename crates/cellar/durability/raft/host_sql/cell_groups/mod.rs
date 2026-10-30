// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell-history admission and commands on the existing fixed node SQL workers.

mod admission;
mod worker;

use super::{Budget, Budgets, Envelope, Permit, Pool, ResourceObserver, Worker};
use crate::raft::cell_groups::{
    cell_image_history_digest, CellEnrollmentAuthority, CellEnrollmentState,
    CellExecutionAuthority, CellReconciliationAuthority, EnrollmentRecord,
};
use crate::raft::completion::RuntimeError;
use crate::raft::execution::{
    CellCommittedReceipt, CellEnrollmentAttempt, CellEnrollmentResolution, CellPreparedCommand,
    CellPrivateEnrollment, CellPrivateExecutor, CellReconciliation, CellStageOutcome,
    CommandAttempt, ExecutionError, ExecutionLimits, PrivateOptions, RootPrivateOptions,
};
use crate::raft::runtime::Terminal;
use crate::raft::storage::ReplicaLog;
use crate::storage::CommandTransaction;
use admission::{Admission, Bytes, CellLease};
use cellar_logic::cell_group::{CellHistory, GroupHistory};
use cellar_logic::consensus::{CommandFailure, ProposalId};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{oneshot, Notify};
pub(super) use worker::{Cells, Request};

#[derive(Clone, Copy)]
pub struct CellSqlLimits {
    pub workers: usize,
    pub queue_per_worker: usize,
    pub max_pending_cells: usize,
    pub group_pending_cells: usize,
    pub node_bytes: usize,
    pub group_bytes: usize,
}

impl CellSqlLimits {
    fn validate(self) -> Result<(), ExecutionError> {
        if !(1..=256).contains(&self.workers)
            || !(1..=4096).contains(&self.queue_per_worker)
            || !(1..=65_536).contains(&self.max_pending_cells)
            || self.group_pending_cells == 0
            || self.group_pending_cells > self.max_pending_cells
            || self.group_bytes == 0
            || self.group_bytes > self.node_bytes
        {
            return Err(ExecutionError::Invalid("cell SQL pool limits"));
        }
        Ok(())
    }
}

/// Extent configuration without a live private workspace. The admitted worker
/// creates and destroys that physical owner; rejected/queued requests do no I/O.
#[derive(Clone)]
pub struct CellSqlRootOptions {
    pub host: cellar_ltx::LtxHost,
    pub sqlite_vfs: Option<String>,
    pub images: cellar_ltx::internal::NodeExtentStore,
    pub query_services: cellar_ltx::generation::ReadServices,
    pub application_digest: [u8; 32],
    pub limits: ExecutionLimits,
    pub cache_directory: std::path::PathBuf,
    pub cache_pool: cellar_ltx::stream_snapshot::ScratchPool,
    pub cache: cellar_ltx::internal::CacheLimits,
}

#[derive(Clone)]
pub enum CellSqlOptions {
    Materialized(PrivateOptions),
    Extents(CellSqlRootOptions),
}

impl CellSqlOptions {
    fn limits(&self) -> ExecutionLimits {
        match self {
            Self::Materialized(o) => o.limits,
            Self::Extents(o) => o.limits,
        }
    }

    fn application_digest(&self) -> [u8; 32] {
        match self {
            Self::Materialized(o) => o.application_digest,
            Self::Extents(o) => o.application_digest,
        }
    }

    fn open_enrollment(
        self,
        authority: CellEnrollmentAuthority,
    ) -> Result<CellPrivateEnrollment, ExecutionError> {
        match self {
            Self::Materialized(o) => CellPrivateEnrollment::open(o, authority),
            Self::Extents(o) => {
                CellPrivateEnrollment::open_rooted(o.prepare(authority.state().cell())?, authority)
            }
        }
    }

    fn open(
        self,
        authority: CellExecutionAuthority,
    ) -> Result<CellPrivateExecutor, ExecutionError> {
        match self {
            Self::Materialized(o) => CellPrivateExecutor::open(o, authority),
            Self::Extents(o) => CellPrivateExecutor::open_rooted(
                o.prepare(&authority.cell().version.history)?,
                authority,
            ),
        }
    }
}

impl CellSqlRootOptions {
    fn prepare(self, cell: &CellHistory) -> Result<RootPrivateOptions, ExecutionError> {
        let workspace = self
            .images
            .create_workspace(
                cell_image_history_digest(cell),
                RootPrivateOptions::workspace_bounds(self.limits)?,
            )
            .map_err(crate::raft::execution::storage)?;
        Ok(RootPrivateOptions {
            workspace,
            host: self.host,
            sqlite_vfs: self.sqlite_vfs,
            images: self.images,
            query_services: self.query_services,
            application_digest: self.application_digest,
            limits: self.limits,
            cache_directory: self.cache_directory,
            cache_pool: self.cache_pool,
            cache: self.cache,
        })
    }
}

pub struct CellSqlEnrollment {
    record: EnrollmentRecord,
    _bytes: Arc<Bytes>,
}

impl CellSqlEnrollment {
    pub fn record(&self) -> &EnrollmentRecord {
        &self.record
    }
}

pub enum CellSqlEnrollmentResolution {
    Committed(Box<CellSqlEnrollment>),
    NotCommitted,
    Conflict,
}

/// Clones retain both node and group admission through delivery and submission.
#[derive(Clone)]
pub struct CellSqlPrepared {
    prepared: CellPreparedCommand,
    _bytes: Arc<Bytes>,
}

impl CellSqlPrepared {
    pub fn term(&self) -> cellar_logic::consensus::Term {
        self.prepared.term()
    }

    pub fn bytes(&self) -> &[u8] {
        self.prepared.bytes()
    }

    pub fn proposal(&self) -> ProposalId {
        self.prepared.proposal()
    }
}

pub struct CellSqlReceipt {
    receipt: CellCommittedReceipt,
    _bytes: Arc<Bytes>,
}

impl CellSqlReceipt {
    pub fn receipt(&self) -> &CellCommittedReceipt {
        &self.receipt
    }
}

pub enum CellSqlOutcome {
    Prepared(CellSqlPrepared),
    Receipt(Box<CellSqlReceipt>),
    Submitted,
    Reconciled(CellSqlReconciliation),
    Enrollment(Box<CellSqlEnrollment>),
    EnrollmentResolved(CellSqlEnrollmentResolution),
}

pub enum CellSqlReconciliation {
    Committed(Box<CellSqlReceipt>),
    NotCommitted,
    Refused(CommandFailure),
}

pub struct CellSqlUsage {
    pub active_cells: usize,
    pub retained_bytes: usize,
    pub retained_operations: usize,
    pub running_workers: usize,
    pub worker_capacity: usize,
    pub queued_or_running: usize,
    pub completion_reservations: usize,
}

#[derive(Clone)]
pub struct CellGroupSqlPool {
    inner: Arc<Handle>,
}

struct Handle {
    engine_owner: Arc<()>,
    workers: Vec<Worker>,
    resources: ResourceObserver,
    admission: Arc<Admission>,
    closing: Arc<AtomicBool>,
    stop: Arc<Notify>,
    terminal: Arc<Terminal>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.closing.store(true, Ordering::Release);
        self.stop.notify_one();
    }
}

fn unavailable() -> ExecutionError {
    ExecutionError::Refused(CommandFailure::Unavailable)
}

fn overloaded() -> ExecutionError {
    ExecutionError::Refused(CommandFailure::Overloaded)
}

impl CellGroupSqlPool {
    pub fn start(log: &ReplicaLog, limits: CellSqlLimits) -> Result<Self, ExecutionError> {
        limits.validate()?;
        log.cell_groups().map_err(crate::raft::execution::storage)?;
        let lease = log
            .claim_sql_pool()
            .map_err(crate::raft::execution::storage)?;
        let domain = log.domain();
        let mut pool = Pool::start(
            domain.clone(),
            limits.workers,
            limits.queue_per_worker,
            limits.max_pending_cells,
            Budgets {
                business: Budget::new(limits.node_bytes, limits.max_pending_cells),
                archive_capture: Budget::new(1, 1),
                query_pins: Budget::new(1, 1),
            },
            log.performance(),
        )?;
        let workers = (0..limits.workers)
            .map(|n| pool.worker(n))
            .collect::<Result<Vec<_>, _>>()?;
        let resources = pool.resource_observer();
        let closing = pool.stopped.clone();
        let stop = Arc::new(Notify::new());
        let terminal = Arc::new(Terminal::new());
        let inner = Arc::new(Handle {
            engine_owner: log.owner(),
            workers,
            resources,
            admission: Admission::new(limits, lease.clone()),
            closing: closing.clone(),
            stop: stop.clone(),
            terminal: terminal.clone(),
        });
        let cleanup_domain = domain.clone();
        // Cleanup owns the join handles, so cancelling a close waiter cannot
        // release physical workers. Domain shutdown also joins this cleanup.
        domain.spawn_cleanup(async move {
            struct Complete(Arc<Terminal>);

            impl Drop for Complete {
                fn drop(&mut self) { self.0.complete(Err(RuntimeError::Closed)); }
            }
            let _complete = Complete(terminal.clone());
            let _physical_lease = lease;
            crate::asyncrt::select! { _ = stop.notified() => (), _ = cleanup_domain.wait_for_work() => () }
            closing.store(true, Ordering::Release);
            let result = pool.join().await.map_err(|e| RuntimeError::Protocol(e.to_string()));
            drop(pool);
            drop(_physical_lease);
            terminal.complete(result);
        }).map_err(|_| unavailable())?;
        Ok(Self { inner })
    }

    pub fn usage(&self) -> Result<CellSqlUsage, ExecutionError> {
        let (active_cells, retained_bytes, retained_operations) = self.inner.admission.usage()?;
        let resources = self.inner.resources.capture().ok_or_else(unavailable)?;
        Ok(CellSqlUsage {
            active_cells,
            retained_bytes,
            retained_operations,
            running_workers: resources.running_workers,
            worker_capacity: resources.worker_capacity,
            queued_or_running: resources.work_reservations,
            completion_reservations: self
                .inner
                .workers
                .iter()
                .map(|w| 1 - w.completion_slots.available_permits())
                .sum(),
        })
    }

    fn admit(&self) -> Result<(), ExecutionError> {
        if self.inner.closing.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        Ok(())
    }

    async fn call(
        &self,
        cell: CellHistory,
        action: worker::Action,
    ) -> Result<CellSqlOutcome, ExecutionError> {
        self.admit()?;
        let hash = cell_image_history_digest(&cell);
        let index = u64::from_le_bytes(
            hash[..8]
                .try_into()
                .map_err(|_| ExecutionError::Invalid("cell worker key"))?,
        ) % self.inner.workers.len() as u64;
        let worker = &self.inner.workers[index as usize];
        worker.domain.mono_ms().map_err(|_| unavailable())?;
        let slots = if action.progress_priority() {
            &worker.completion_slots
        } else {
            &worker.slots
        };
        let slot = slots
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => unavailable(),
                tokio::sync::TryAcquireError::NoPermits => overloaded(),
            })?;
        self.admit()?;
        let (reply, wait) = oneshot::channel();
        worker
            .sender
            .try_send(Envelope::Shared(Request {
                cell,
                action,
                reply,
                _slot: slot,
                queued: worker
                    .performance
                    .timer(crate::raft::performance::Operation::SqlQueue),
            }))
            .map_err(|_| unavailable())?;
        wait.await.map_err(|_| unavailable())?
    }

    /// The owning worker retains the callback and admission even if this waiter
    /// is cancelled. Caller-provided callbacks are the verification boundary;
    /// the production host must dispatch registered operations here.
    pub async fn stage(
        &self,
        options: CellSqlOptions,
        authority: CellExecutionAuthority,
        attempt: CommandAttempt,
        allow_new: bool,
        callback: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>> + Send + 'static,
    ) -> Result<CellSqlOutcome, ExecutionError> {
        self.admit()?;
        if !authority.belongs_to_engine(&self.inner.engine_owner) {
            return Err(ExecutionError::Invalid("cell SQL engine owner"));
        }
        if attempt.request.operation.is_empty() || attempt.request.operation.len() > 128 {
            return Err(ExecutionError::Invalid("command operation"));
        }
        let limits = options.limits();
        limits.validate()?;
        // Bound simultaneous encoded capture, entry, preview verification and
        // result buffers before queueing or opening SQLite. Disk/workspace
        // reservation is separately enforced by the chosen image owner.
        let bytes = limits
            .envelope
            .max_payload_bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(limits.max_result_bytes))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(overloaded)?;
        let cell = authority.cell().version.history.clone();
        let (lease, bytes) =
            self.inner
                .admission
                .reserve(&cell, authority.route().history, bytes)?;
        self.call(
            cell,
            worker::Action::Stage(Box::new(worker::Stage {
                options,
                authority,
                attempt,
                allow_new,
                callback: Box::new(callback),
                lease,
                bytes,
            })),
        )
        .await
    }

    /// Initialize one logical cell on the existing blocking pool. The creation
    /// ID is immutable retry identity; a committed retry returns its original
    /// enrollment record without opening SQLite or rerunning the callback.
    pub async fn initialize(
        &self,
        options: CellSqlOptions,
        authority: CellEnrollmentAuthority,
        attempt: CellEnrollmentAttempt,
        initialize: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<()> + Send + 'static,
    ) -> Result<CellSqlOutcome, ExecutionError> {
        self.admit()?;
        if !authority
            .state()
            .belongs_to_engine(&self.inner.engine_owner)
        {
            return Err(ExecutionError::Invalid("cell SQL engine owner"));
        }
        authority.check().map_err(crate::raft::execution::authority_failure)?;
        if options.application_digest() != authority.state().route().schema_digest {
            return Err(ExecutionError::Invalid("enrollment schema"));
        }
        let limits = options.limits();
        limits.validate()?;
        let bytes = if authority.state().record().is_some() {
            4096
        } else {
            limits
                .envelope
                .max_payload_bytes
                .checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(overloaded)?
        };
        let cell = authority.state().cell().clone();
        let (lease, bytes) =
            self.inner
                .admission
                .reserve(&cell, authority.state().route().history, bytes)?;
        self.call(
            cell,
            worker::Action::Initialize(Box::new(worker::Initialize {
                options,
                authority,
                attempt,
                callback: Box::new(initialize),
                lease,
                bytes,
            })),
        )
        .await
    }

    pub async fn resolve_enrollment(
        &self,
        creation: ProposalId,
        state: CellEnrollmentState,
    ) -> Result<CellSqlEnrollmentResolution, ExecutionError> {
        if !state.belongs_to_engine(&self.inner.engine_owner) {
            return Err(ExecutionError::Invalid("cell SQL engine owner"));
        }
        let cell = state.cell().clone();
        match self
            .call(
                cell,
                worker::Action::ResolveEnrollment(creation, Box::new(state)),
            )
            .await?
        {
            CellSqlOutcome::EnrollmentResolved(resolution) => Ok(resolution),
            _ => Err(ExecutionError::Invalid("enrollment resolution response")),
        }
    }

    /// Recover a lost stage response without another callback or producer.
    pub async fn resume(
        &self,
        cell: CellHistory,
        proposal: ProposalId,
    ) -> Result<CellSqlPrepared, ExecutionError> {
        match self.call(cell, worker::Action::Resume(proposal)).await? {
            CellSqlOutcome::Prepared(prepared) => Ok(prepared),
            _ => Err(ExecutionError::Invalid("cell SQL resume response")),
        }
    }

    pub async fn mark_submitted(
        &self,
        cell: CellHistory,
        proposal: ProposalId,
    ) -> Result<(), ExecutionError> {
        match self.call(cell, worker::Action::Submit(proposal)).await? {
            CellSqlOutcome::Submitted => Ok(()),
            _ => Err(ExecutionError::Invalid("cell SQL submit response")),
        }
    }

    pub async fn finish(
        &self,
        authority: CellExecutionAuthority,
    ) -> Result<CellSqlReceipt, ExecutionError> {
        if !authority.belongs_to_engine(&self.inner.engine_owner) {
            return Err(ExecutionError::Invalid("cell SQL engine owner"));
        }
        let cell = authority.cell().version.history.clone();
        match self
            .call(cell, worker::Action::Finish(Box::new(authority)))
            .await?
        {
            CellSqlOutcome::Receipt(receipt) => Ok(*receipt),
            _ => Err(ExecutionError::Invalid("cell SQL finish response")),
        }
    }

    /// Release a producer only after receipt resolution or a newer applied
    /// term excludes its proposal. Cancellation leaves resolution on the worker;
    /// a lost response must be reconciled against receipts, never inferred failed.
    pub async fn reconcile(
        &self,
        proposal: ProposalId,
        authority: CellReconciliationAuthority,
    ) -> Result<CellSqlReconciliation, ExecutionError> {
        if !authority.belongs_to_engine(&self.inner.engine_owner) {
            return Err(ExecutionError::Invalid("cell SQL engine owner"));
        }
        let cell = authority.cell().version.history.clone();
        match self
            .call(
                cell,
                worker::Action::Reconcile(proposal, Box::new(authority)),
            )
            .await?
        {
            CellSqlOutcome::Reconciled(resolution) => Ok(resolution),
            _ => Err(ExecutionError::Invalid("cell SQL reconciliation response")),
        }
    }

    pub async fn close(&self) -> Result<(), ExecutionError> {
        self.inner.closing.store(true, Ordering::Release);
        self.inner.stop.notify_one();
        self.inner
            .terminal
            .wait()
            .await
            .map_err(crate::raft::execution::storage)
    }
}
