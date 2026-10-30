// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owns a fixed roster's consensus events, transport dispatch and physical apply.
//! Admitted command ownership survives response cancellation and expiry.

mod calls;
mod commands;
mod enrollments;
mod requests;
pub use enrollments::{CellEnrollmentAdmissionError, CellEnrollmentCall};
mod worker;
pub use commands::{CellCommandAdmissionError, CellCommandCall};

use super::{CellGroupApplier, CellGroupRuntime, CellGroupRuntimeEvents, CellGroupSqlPool};
use crate::raft::{
    completion::RuntimeError,
    execution::ExecutionError,
    transport::{RaftTransport, TransportError},
};
use futures_util::FutureExt;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug)]
pub struct CellGroupControllerLimits {
    pub max_groups: usize,
    pub max_parallel_apply: usize,
    /// One node timer backs off transient SQL admission, never one timer per cell.
    pub retry_interval_ms: u64,
    /// Shared by commands and enrollments, including queued work, resolution
    /// and unconsumed responses.
    pub max_commands: usize,
    /// Invocation/input/options retention. SQL capture, apply and result bytes
    /// retain the SQL pool's independent admission as well.
    pub max_command_bytes: usize,
}

#[derive(Clone, Debug)]
pub enum CellGroupControllerError {
    Invalid(&'static str),
    Runtime(RuntimeError),
    Apply(Arc<ExecutionError>),
    Transport(TransportError),
    Command(Arc<super::CellCommandError>),
    Enrollment(Arc<super::CellEnrollmentError>),
    Closed,
}

impl std::fmt::Display for CellGroupControllerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cell-group controller: {self:?}")
    }
}

impl std::error::Error for CellGroupControllerError {}

impl From<RuntimeError> for CellGroupControllerError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CellGroupControllerUsage {
    pub groups: usize,
    pub active_apply: usize,
    pub pending_apply: usize,
    pub applied: u64,
    pub apply_retries: u64,
    /// Immediate queue/route refusals. The transport tracks later I/O failures.
    pub send_refusals: u64,
    pub closing: bool,
    pub retained_commands: usize,
    pub command_bytes: usize,
}

struct Shared {
    groups: BTreeSet<cellar_logic::cell_group::GroupHistory>,
    limits: CellGroupControllerLimits,
    domain: crate::asyncrt::DomainToken,
    commands: Arc<crate::raft::budget::Budget>,
    requests: tokio::sync::mpsc::Sender<requests::Request>,
    active_apply: AtomicUsize,
    pending_apply: AtomicUsize,
    applied: AtomicU64,
    apply_retries: AtomicU64,
    send_refusals: AtomicU64,
    closing: AtomicBool,
    stop: Notify,
    terminal: Mutex<Option<Result<(), CellGroupControllerError>>>,
    finished: Notify,
}

impl Shared {
    fn stop(&self) {
        self.closing.store(true, Ordering::Release);
        self.stop.notify_one();
    }

    fn complete(&self, result: Result<(), CellGroupControllerError>) {
        let mut terminal = self.terminal.lock().unwrap_or_else(|e| e.into_inner());
        if terminal.is_none() {
            *terminal = Some(result);
        }
        drop(terminal);
        self.finished.notify_waiters();
    }

    async fn wait(&self) -> Result<(), CellGroupControllerError> {
        loop {
            let finished = self.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            let result = self
                .terminal
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(result) = result {
                return result;
            }
            finished.await;
        }
    }
}

struct Handle(Arc<Shared>);

impl Drop for Handle {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// One worker per node, with a bounded fixed roster and no cell-specific tasks.
/// Dropping the final public handle requests joined shutdown. An explicit close
/// is required to observe its result. Runtime/transport/pool clones remain
/// usable during operation; closing this owner closes all three services.
#[derive(Clone)]
pub struct CellGroupController {
    handle: Arc<Handle>,
}

impl CellGroupController {
    /// Consume the exclusive event receiver after validating exact engine and
    /// runtime ownership. Every listed group must already be attached. Later
    /// attachment requires a future roster API; an unknown event fails closed.
    pub async fn start(
        runtime: CellGroupRuntime,
        events: CellGroupRuntimeEvents,
        transport: RaftTransport,
        sql: CellGroupSqlPool,
        appliers: Vec<CellGroupApplier>,
        limits: CellGroupControllerLimits,
    ) -> Result<Self, CellGroupControllerError> {
        if limits.max_groups == 0
            || limits.max_groups > runtime.log().limits().max_groups
            || limits.max_parallel_apply == 0
            || limits.max_parallel_apply > limits.max_groups.min(256)
            || !(1..=60_000).contains(&limits.retry_interval_ms)
            || appliers.is_empty()
            || appliers.len() > limits.max_groups
            || !(1..=65_536).contains(&limits.max_commands)
            || limits.max_command_bytes < 8192
        {
            return Err(CellGroupControllerError::Invalid(
                "controller limits/roster",
            ));
        }
        if !events.belongs_to(&runtime)
            || !transport.uses_shared_runtime(&runtime)
            || !sql.belongs_to_engine(&runtime.log().owner())
        {
            return Err(CellGroupControllerError::Invalid("controller owner"));
        }
        let mut roster = BTreeMap::new();
        for applier in appliers {
            if !applier.belongs_to_engine(&runtime.log().owner())
                || roster.insert(applier.history(), applier).is_some()
            {
                return Err(CellGroupControllerError::Invalid("controller applier"));
            }
        }
        for history in roster.keys() {
            runtime.status(*history).await?;
        }
        runtime.admit()?;
        let domain = runtime.log().domain();
        let (requests, receiver) = tokio::sync::mpsc::channel(limits.max_commands);
        let shared = Arc::new(Shared {
            groups: roster.keys().copied().collect(),
            limits,
            domain: domain.clone(),
            commands: commands::budget(limits),
            requests,
            active_apply: AtomicUsize::new(0),
            pending_apply: AtomicUsize::new(0),
            applied: AtomicU64::new(0),
            apply_retries: AtomicU64::new(0),
            send_refusals: AtomicU64::new(0),
            closing: AtomicBool::new(false),
            stop: Notify::new(),
            terminal: Mutex::new(None),
            finished: Notify::new(),
        });
        let handle = Self {
            handle: Arc::new(Handle(shared.clone())),
        };
        let worker_domain = domain.clone();
        // Infrastructure cleanup is not cancelled with application tasks. The
        // worker retains tickets/appliers until all physical owners have joined.
        domain
            .spawn_cleanup(async move {
                let mut worker = worker::Worker::new(
                    runtime,
                    events,
                    transport,
                    sql,
                    roster,
                    receiver,
                    shared.clone(),
                );
                let result = std::panic::AssertUnwindSafe(worker.run(&worker_domain))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(CellGroupControllerError::Runtime(
                        RuntimeError::Protocol("controller worker panicked".into()),
                    )));
                shared.stop();
                let joined = worker.finish().await;
                drop(worker);
                shared.active_apply.store(0, Ordering::Release);
                shared.pending_apply.store(0, Ordering::Release);
                shared.complete(result.and(joined));
            })
            .map_err(|_| CellGroupControllerError::Closed)?;
        Ok(handle)
    }

    pub fn usage(&self) -> CellGroupControllerUsage {
        let shared = &self.handle.0;
        let (command_bytes, retained_commands) = shared.commands.usage();
        CellGroupControllerUsage {
            groups: shared.groups.len(),
            command_bytes,
            retained_commands,
            active_apply: shared.active_apply.load(Ordering::Acquire),
            pending_apply: shared.pending_apply.load(Ordering::Acquire),
            applied: shared.applied.load(Ordering::Relaxed),
            apply_retries: shared.apply_retries.load(Ordering::Relaxed),
            send_refusals: shared.send_refusals.load(Ordering::Relaxed),
            closing: shared.closing.load(Ordering::Acquire),
        }
    }

    /// Wait for terminal failure or joined shutdown without requesting a stop.
    pub async fn wait(&self) -> Result<(), CellGroupControllerError> {
        self.handle.0.wait().await
    }

    /// Cancelling this waiter does not cancel transport, persistence or SQL joins.
    pub async fn close(&self) -> Result<(), CellGroupControllerError> {
        self.handle.0.stop();
        self.wait().await
    }
}
