// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owns registered groups' consensus events, transport dispatch and physical apply.
//! Admitted command ownership survives response cancellation and expiry.

mod admission;
mod archive;
pub(crate) use archive::settlement::{
    Client as S3Client, LookupPrepared as S3LookupPrepared, OutputProof as S3OutputProof,
};
pub(crate) use archive::Provisioning as CellGroupArchiveProvisioning;
pub(crate) use archive::{ArchiveSeedClient, ArchiveSeedDescriptor};
mod calls;
pub use archive::{
    CellGroupArchiveCall, CellGroupArchiveControlError, CellGroupArchiveRegistration,
    CellGroupArchiveStatus,
};
mod checkpoints;
pub use checkpoints::CellGroupCheckpointPolicy;
mod commands;
mod queries;
mod query_routes;
pub use queries::CellQueryCall;
mod effects;
mod enrollments;
pub use effects::CellGroupEffectRegistration;
mod failure;
mod provision;
mod registration;
pub use provision::{
    CellGroupProvisionCall, CellGroupProvisionLimits, CellGroupProvisionNamespace,
    CellGroupProvisioning,
};
mod requests;
pub use enrollments::{CellEnrollmentAdmissionError, CellEnrollmentCall};
pub(crate) use registration::GroupRoster;
pub use registration::{
    CellGroupRegistration, CellGroupRegistrationCall, CellGroupRegistrationError,
};
mod worker;
pub use commands::{CellCommandAdmissionError, CellCommandCall};

use super::{CellGroupApplier, CellGroupRuntime, CellGroupRuntimeEvents, CellGroupSqlPool};
use crate::raft::{
    completion::RuntimeError,
    execution::ExecutionError,
    transport::{RaftTransport, TransportError},
};
use futures_util::FutureExt;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex, OnceLock,
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug)]
pub struct CellGroupControllerLimits {
    pub max_groups: usize,
    pub max_parallel_apply: usize,
    /// One node timer backs off transient SQL admission, never one timer per cell.
    pub retry_interval_ms: u64,
    /// Shared by commands, queries, enrollments and registrations, including queued work, resolution
    /// and unconsumed responses.
    pub max_commands: usize,
    /// Invocation/input/options retention. The independent S3 output stage has
    /// the same byte ceiling; configure their combined envelope as twice this
    /// value. SQL capture/apply/results retain separate SQL pool admission.
    pub max_command_bytes: usize,
    /// Automatic compaction at an apply boundary. Requires the transport's
    /// shared checkpoint pool. None leaves local publication to its caller.
    pub checkpoints: Option<CellGroupCheckpointPolicy>,
}

#[derive(Clone, Debug)]
pub enum CellGroupControllerError {
    Invalid(&'static str),
    Runtime(RuntimeError),
    Apply(Arc<ExecutionError>),
    Archive(Arc<CellGroupArchiveControlError>),
    Checkpoint(Arc<super::CellGroupCheckpointError>),
    Transport(TransportError),
    Catalog(Arc<crate::raft::catalog::CatalogError>),
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

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct CellGroupControllerUsage {
    pub ordinary: super::CellGroupAdmissionUsage,
    pub archive_settlement: super::CellGroupAdmissionUsage,
    pub groups: usize,
    pub pending_registrations: usize,
    /// Retained failed groups still consume the registration capacity.
    pub quarantined_groups: usize,
    pub active_apply: usize,
    pub pending_apply: usize,
    pub applied: u64,
    pub active_checkpoints: usize,
    pub checkpoints_published: u64,
    pub checkpoint_retries: u64,
    pub apply_retries: u64,
    /// Immediate queue/route refusals. The transport tracks later I/O failures.
    pub send_refusals: u64,
    pub closing: bool,
    /// Ordinary node calls, including queries, enrollments and registrations.
    pub retained_commands: usize,
    pub command_bytes: usize,
    pub retained_preparations: usize,
    pub preparation_bytes: usize,
    /// Immutable namespace/directory metadata charged to provisioning's total.
    pub provision_configuration_bytes: usize,
    pub provision_waiters: usize,
    /// Independent archive-settlement admission, bounded by max_commands and
    /// max_command_bytes. This lane cannot recursively consume command slots.
    pub retained_s3: usize,
    pub s3_bytes: usize,
    /// At most one effect turn runs on the node. SQL and command admission are
    /// shared with ordinary operations; configured metadata has its own permit.
    pub active_effects: usize,
    pub effect_turns: u64,
    pub effect_retries: u64,
    /// Includes response handoff and physical retirement of seed uploads.
    pub active_archive_seeds: usize,
    /// One node-owned streaming journal sweep; never supplies serving authority.
    pub active_archive_seed_cleanup: usize,
    pub archive_seed_cleanup_errors: usize,
}

type HostResourceRelease = Box<dyn FnOnce() -> Result<(), CellGroupControllerError> + Send>;

struct Shared {
    admission_configuration: Mutex<()>,
    public: OnceLock<admission::Public>,
    // One-shot physical release is opaque to break the ownership type graph:
    // bindings contain weak archive clients back to this same shared owner.
    host_resources: Mutex<Option<HostResourceRelease>>,
    runtime: CellGroupRuntime,
    sql: crate::raft::host_sql::cell_groups::WeakCellGroupSqlPool,
    groups: Arc<GroupRoster>,
    limits: CellGroupControllerLimits,
    domain: crate::asyncrt::DomainToken,
    commands: Arc<crate::raft::budget::Budget>,
    provisioning: Mutex<Option<Arc<provision::Config>>>,
    effects: Mutex<Option<Arc<effects::Config>>>,
    active_effects: AtomicUsize,
    effect_turns: AtomicU64,
    effect_retries: AtomicU64,
    requests: tokio::sync::mpsc::Sender<requests::Request>,
    archives: Mutex<BTreeMap<cellar_logic::cell_group::GroupHistory, CellGroupArchiveStatus>>,
    archive_discovery: Mutex<std::collections::BTreeSet<cellar_logic::cell_group::GroupHistory>>,
    archive_seeds: Arc<archive::seed::Jobs>,
    s3: Arc<crate::raft::budget::Budget>,
    active_apply: AtomicUsize,
    pending_apply: AtomicUsize,
    applied: AtomicU64,
    checkpoints: Option<checkpoints::Config>,
    active_checkpoints: AtomicUsize,
    checkpoints_published: AtomicU64,
    checkpoint_retries: AtomicU64,
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

/// One worker per node, with a bounded dynamic roster and no cell-specific tasks.
/// Dropping the final public handle requests joined shutdown. An explicit close
/// is required to observe its result. Runtime/transport/pool clones remain
/// usable during operation; closing this owner closes all three services.
#[derive(Clone)]
pub struct CellGroupController {
    handle: Arc<Handle>,
}

impl CellGroupController {
    pub(crate) fn limits(&self) -> CellGroupControllerLimits {
        self.handle.0.limits
    }

    pub(crate) fn belongs_to(&self, runtime: &CellGroupRuntime) -> bool {
        self.handle.0.runtime.same_owner(runtime)
    }

    pub(in crate::raft::cell_groups) fn own_host_resources(
        &self,
        resources: Arc<super::host::HostResources>,
    ) -> Result<(), RuntimeError> {
        let mut slot = self
            .handle
            .0
            .host_resources
            .lock()
            .map_err(|_| RuntimeError::Closed)?;
        if self.handle.0.closing.load(Ordering::Acquire) {
            return Err(RuntimeError::Closed);
        }
        if slot.is_some() {
            return Err(RuntimeError::AlreadyAttached);
        }
        *slot = Some(Box::new(move || resources.release()));
        Ok(())
    }

    /// Consume the exclusive event receiver after validating exact engine and
    /// runtime ownership. Initial groups must already be attached. Later groups
    /// enter through register() or provision(); unknown events still fail closed.
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
        let checkpoints = checkpoints::Config::new(limits, &transport)?;
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
            admission_configuration: Mutex::new(()),
            public: OnceLock::new(),
            host_resources: Mutex::new(None),
            runtime: runtime.clone(),
            sql: sql.downgrade(),
            groups: GroupRoster::new(roster.keys().copied(), limits.max_groups),
            limits,
            domain: domain.clone(),
            commands: commands::budget(limits),
            provisioning: Mutex::new(None),
            effects: Mutex::new(None),
            active_effects: AtomicUsize::new(0),
            effect_turns: AtomicU64::new(0),
            effect_retries: AtomicU64::new(0),
            requests,
            archives: Mutex::new(BTreeMap::new()),
            archive_discovery: Mutex::new(std::collections::BTreeSet::new()),
            archive_seeds: Arc::new(archive::seed::Jobs::new(limits.max_groups)),
            s3: commands::budget(limits),
            active_apply: AtomicUsize::new(0),
            pending_apply: AtomicUsize::new(0),
            applied: AtomicU64::new(0),
            checkpoints,
            active_checkpoints: AtomicUsize::new(0),
            checkpoints_published: AtomicU64::new(0),
            checkpoint_retries: AtomicU64::new(0),
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
        let mut worker = worker::Worker::new(
            runtime,
            events,
            transport,
            sql,
            roster,
            receiver,
            shared.clone(),
        )?;
        domain
            .spawn_cleanup(async move {
                let result = std::panic::AssertUnwindSafe(worker.run(&worker_domain))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(CellGroupControllerError::Runtime(
                        RuntimeError::Protocol("controller worker panicked".into()),
                    )));
                shared.stop();
                let joined = worker.finish().await;
                drop(worker);
                shared
                    .provisioning
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                shared
                    .effects
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                shared.active_effects.store(0, Ordering::Release);
                shared.active_checkpoints.store(0, Ordering::Release);
                shared.active_apply.store(0, Ordering::Release);
                shared.pending_apply.store(0, Ordering::Release);
                // Resource release belongs to this joined owner, never the
                // lifetime of an async close waiter or a retained cell handle.
                let released = match shared.host_resources.lock() {
                    Ok(mut slot) => slot.take().map_or(Ok(()), |release| release()),
                    Err(error) => {
                        if let Some(release) = error.into_inner().take() {
                            let _ = release();
                        }
                        Err(CellGroupControllerError::Invalid(
                            "host resource owner poisoned",
                        ))
                    }
                };
                shared.complete(result.and(joined).and(released));
            })
            .map_err(|_| CellGroupControllerError::Closed)?;
        Ok(handle)
    }

    /// Original failure retained for a quarantined group. Recovery requires a
    /// joined restart; registration cannot replace the failed owner in place.
    pub fn group_failure(
        &self,
        group: cellar_logic::cell_group::GroupHistory,
    ) -> Result<Option<CellGroupControllerError>, RuntimeError> {
        self.handle.0.groups.failure(&group)
    }

    pub fn usage(&self) -> CellGroupControllerUsage {
        let shared = &self.handle.0;
        let (command_bytes, retained_commands) = shared.commands.usage();
        let (s3_bytes, retained_s3) = shared.s3.usage();
        let (active_archive_seeds, active_archive_seed_cleanup, archive_seed_cleanup_errors) =
            shared.archive_seeds.usage();
        let (
            preparation_bytes,
            retained_preparations,
            provision_waiters,
            provision_configuration_bytes,
        ) = shared
            .provisioning
            .lock()
            .ok()
            .and_then(|slot| {
                slot.as_ref().map(|config| {
                    let (bytes, items) = config.preparations.usage();
                    (
                        bytes,
                        items,
                        config.waiters.usage().1,
                        config.configuration_bytes,
                    )
                })
            })
            .unwrap_or_default();
        CellGroupControllerUsage {
            ordinary: super::CellGroupAdmissionUsage::read(&shared.commands),
            archive_settlement: super::CellGroupAdmissionUsage::read(&shared.s3),
            active_effects: shared.active_effects.load(Ordering::Acquire),
            effect_turns: shared.effect_turns.load(Ordering::Relaxed),
            effect_retries: shared.effect_retries.load(Ordering::Relaxed),
            preparation_bytes,
            provision_configuration_bytes,
            retained_preparations,
            provision_waiters,
            groups: shared.groups.len(),
            pending_registrations: shared.groups.pending(),
            quarantined_groups: shared.groups.quarantined(),
            command_bytes,
            retained_commands,
            s3_bytes,
            retained_s3,
            active_apply: shared.active_apply.load(Ordering::Acquire),
            pending_apply: shared.pending_apply.load(Ordering::Acquire),
            applied: shared.applied.load(Ordering::Relaxed),
            active_checkpoints: shared.active_checkpoints.load(Ordering::Acquire),
            checkpoints_published: shared.checkpoints_published.load(Ordering::Relaxed),
            checkpoint_retries: shared.checkpoint_retries.load(Ordering::Relaxed),
            apply_retries: shared.apply_retries.load(Ordering::Relaxed),
            send_refusals: shared.send_refusals.load(Ordering::Relaxed),
            closing: shared.closing.load(Ordering::Acquire),
            active_archive_seeds,
            active_archive_seed_cleanup,
            archive_seed_cleanup_errors,
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

impl CellGroupController {
    pub(crate) async fn lookup_receipt(
        &self,
        mut invocation: super::CellReceiptInvocation,
        options: super::CellSqlOptions,
    ) -> Result<super::CellReceiptResponse, super::CellDispatchError> {
        let shared = &self.handle.0;
        let work = {
            let mut registration = shared
                .registration()
                .map_err(super::CellDispatchError::Runtime)?;
            registration.work.take()
        };
        let _work = work;
        invocation.begin().await?;
        let attempt = invocation
            .attempt()
            .ok_or(super::CellDispatchError::Runtime(RuntimeError::NotFound))?;
        if !shared
            .groups
            .contains(&attempt.route().history)
            .map_err(super::CellDispatchError::Runtime)?
        {
            return Err(super::CellDispatchError::Runtime(RuntimeError::NotFound));
        }
        let sql = shared
            .sql
            .upgrade()
            .ok_or(super::CellDispatchError::Runtime(RuntimeError::Closed))?;
        invocation.execute(&shared.runtime, &sql, options).await
    }
}
