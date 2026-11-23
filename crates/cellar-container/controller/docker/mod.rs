// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently owned, bounded Docker execution. Application commands execute
//! inside an isolated container; its root is a host-owned idle scaffold, never
//! an application entrypoint. Every higher claim stops the entire old process
//! tree before admission, including detached children. No mutation is replayed.
mod engine;
mod journal;
#[cfg(test)]
#[path = "receipt_tests.rs"]
mod receipt_tests;
mod work;
use super::{Reply, Request};
use crate::Limits;
use anyhow::{ensure, Context as _, Result};
use cellar::{
    asyncrt::ExecutionDomain,
    sdk::{consensus::CellHistory, containers::*, HandlerFuture},
};
use engine::Engine;
use journal::{Journal, PhysicalOperation, Slot};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{Mutex as AsyncMutex, Notify, Semaphore};

pub(super) const PROVIDER: &str = "docker-exec";
pub(super) const OPERATIONS: &[crate::Operation] = &[
    crate::Operation::Start,
    crate::Operation::Destroy,
    crate::Operation::Reconcile,
    crate::Operation::Running,
    crate::Operation::Monitor,
    crate::Operation::Signal,
    crate::Operation::ExecOutput,
    crate::Operation::ReadFile,
    crate::Operation::WriteFile,
    crate::Operation::StatFile,
    crate::Operation::ListFiles,
];

pub(super) fn validate_parameters(
    operation: crate::Operation,
    input: &serde_json::Value,
) -> Result<()> {
    ensure!(
        OPERATIONS.contains(&operation),
        cellar::sdk::consensus::CommandFailure::UnsupportedCapability
    );
    if operation == crate::Operation::Start {
        let options = &input["options"];
        ensure!(
            options["entrypoint"].is_null() && options["enable_internet"].as_bool() != Some(true),
            "Docker exec profile uses a host-owned idle root and isolated networking"
        );
        if let Some(labels) = options["labels"].as_array() {
            for label in labels {
                ensure!(
                    !label[0].as_str().unwrap_or_default().starts_with("cellar."),
                    "reserved Docker execution label"
                );
            }
        }
    }
    if operation == crate::Operation::ExecOutput {
        ensure!(
            input["options"]["pty"].is_null(),
            cellar::sdk::consensus::CommandFailure::UnsupportedCapability
        );
    }
    Ok(())
}

/// Immutable host policy. The image must be preinstalled by digest and contain
/// `/bin/sleep`. Its fixed idle root is infrastructure, not an application
/// callback. Custom root entrypoints, Internet access and host mounts refuse.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DockerOptions {
    pub directory: PathBuf,
    pub socket: PathBuf,
    pub binding: String,
    pub owner: String,
    pub image: String,
    pub memory_bytes: u64,
    pub nano_cpus: u64,
    pub pids: u32,
    pub max_resources: usize,
    pub max_operations: usize,
    pub max_inflight: usize,
    pub max_body_bytes: usize,
    pub max_operation_ms: u64,
}

impl DockerOptions {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.directory.is_absolute() && self.socket.is_absolute(),
            "Docker storage/socket paths must be absolute"
        );
        ensure!(
            !self.binding.is_empty()
                && self.binding.len() <= 128
                && self
                    .binding
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid Docker binding"
        );
        ensure!(
            self.owner.len() == 32 && self.owner.bytes().all(|b| b.is_ascii_hexdigit()),
            "Docker owner requires a unique 128-bit hex identity"
        );
        let image = self
            .image
            .strip_prefix("sha256:")
            .context("Docker backend requires an immutable local image ID")?;
        ensure!(
            image.len() == 64 && image.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid Docker image digest"
        );
        ensure!(
            (16 * 1024 * 1024..=16 * 1024 * 1024 * 1024).contains(&self.memory_bytes)
                && (1_000_000..=16_000_000_000).contains(&self.nano_cpus)
                && (8..=4096).contains(&self.pids)
                && (1..=128).contains(&self.max_resources)
                && (1..=4096).contains(&self.max_operations)
                && (2..=128).contains(&self.max_inflight)
                && (1..=64 * 1024).contains(&self.max_body_bytes)
                && (1..=60_000).contains(&self.max_operation_ms),
            "invalid Docker resource bounds"
        );
        Ok(())
    }

    fn digest(&self) -> Result<[u8; 32]> {
        let mut digest = Sha256::new();
        digest.update(b"cellar-docker-execution-1");
        digest.update(serde_json::to_vec(self)?);
        Ok(digest.finalize().into())
    }

    pub fn identity(&self) -> Result<BackendIdentity> {
        self.validate()?;
        Ok(BackendIdentity {
            binding: self.binding.clone(),
            provider: PROVIDER.into(),
            configuration_digest: hex(&self.digest()?),
        })
    }
}

/// Independently verifies permanent old-host fencing. Docker process termination
/// alone cannot stop old Cellar hosts from restarting on another engine.
/// Implementations must be idempotent and fail closed on unknown outcomes.
pub trait DockerHistoryFence: Send + Sync + 'static {
    fn fence(self: Arc<Self>, old: CellHistory, successor: CellHistory) -> HandlerFuture<()>;
}

/// A controller-only Engine owner with independent durable fences and receipts.
/// Keep its private socket and journal out of application containers/voter disks.
pub struct DockerBackend {
    options: DockerOptions,
    identity: BackendIdentity,
    domain: Arc<ExecutionDomain>,
    engine: Engine,
    journal: Arc<Journal>,
    admission: AsyncMutex<()>,
    capacity: Arc<Semaphore>,
    observations: Arc<Semaphore>,
    jobs: Mutex<BTreeMap<String, BTreeMap<[u8; 16], u64>>>,
    changed: Notify,
    closed: AtomicBool,
    history_fence: Arc<dyn DockerHistoryFence>,
}

struct Job {
    backend: Arc<DockerBackend>,
    key: String,
    operation: [u8; 16],
}

impl Drop for Job {
    fn drop(&mut self) {
        if let Ok(mut jobs) = self.backend.jobs.lock() {
            if let Some(operations) = jobs.get_mut(&self.key) {
                operations.remove(&self.operation);
                if operations.is_empty() {
                    jobs.remove(&self.key);
                }
            }
        }
        self.backend.changed.notify_waiters();
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl DockerBackend {
    pub async fn create(
        domain: Arc<ExecutionDomain>,
        options: DockerOptions,
        history_fence: Arc<dyn DockerHistoryFence>,
    ) -> Result<Arc<Self>> {
        Self::initialize(domain, options, history_fence, true).await
    }

    /// Open the original independently durable store. Missing/corrupt state or
    /// another daemon/image refuses. Submitted mutations whose physical result
    /// was not durably recorded refuse reopening; operator reconciliation is
    /// required, never a fresh store or automatic mutation replay.
    pub async fn open(
        domain: Arc<ExecutionDomain>,
        options: DockerOptions,
        history_fence: Arc<dyn DockerHistoryFence>,
    ) -> Result<Arc<Self>> {
        Self::initialize(domain, options, history_fence, false).await
    }

    async fn initialize(
        domain: Arc<ExecutionDomain>,
        options: DockerOptions,
        history_fence: Arc<dyn DockerHistoryFence>,
        create: bool,
    ) -> Result<Arc<Self>> {
        let identity = options.identity()?;
        let engine = Engine::new(&options);
        let id = domain.run(engine.check()).await??;
        if create {
            ensure!(
                domain.run(engine.resources(None)).await??.is_empty()
                    && domain.run(engine.volumes()).await??.is_empty(),
                "Docker owner already has resources; restore its original backend store"
            );
        }
        let fs = domain.run(async { cellar::asyncrt::fs() }).await?;
        let policy = options.clone();
        let journal = domain
            .blocking(move || {
                if create {
                    Journal::create(policy, fs, id)
                } else {
                    Journal::open(policy, fs, &id)
                }
            })?
            .await??;
        if !create {
            journal.view(|state| {
                ensure!(
                    state
                        .slots
                        .values()
                        .all(
                            |slot| slot.operations.values().all(|op| op.outcome.is_some()
                                || matches!(
                                    op.operation.kind,
                                    ExecutionOperationKind::Inspect | ExecutionOperationKind::Read
                                ))
                        ),
                    ExecutionControllerFailure::RecoveryPending
                );
                Ok(())
            })?;
        }
        Ok(Arc::new(Self {
            capacity: Arc::new(Semaphore::new(options.max_inflight)),
            observations: Arc::new(Semaphore::new(options.max_inflight - 1)),
            options,
            identity,
            domain,
            engine,
            journal,
            admission: AsyncMutex::new(()),
            jobs: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
            closed: AtomicBool::new(false),
            history_fence,
        }))
    }

    async fn update<T: Send + 'static>(
        &self,
        update: impl FnOnce(&mut journal::State) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let journal = self.journal.clone();
        self.domain
            .blocking(move || journal.change(update))?
            .await?
    }

    async fn wait_jobs(&self, key: Option<&str>) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let done = {
                let jobs = self
                    .jobs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Docker job inventory poisoned"))?;
                key.map_or(jobs.is_empty(), |key| !jobs.contains_key(key))
            };
            if done {
                return Ok(());
            }
            changed.await;
        }
    }

    fn check_operation(
        &self,
        claim: &ExecutionClaim,
        operation: &ExecutionOperation,
    ) -> Result<()> {
        self.journal.check(claim)?;
        let deadline = self
            .jobs
            .lock()
            .map_err(|_| ExecutionControllerFailure::Unavailable)?
            .get(&journal::key(claim))
            .and_then(|jobs| jobs.get(&operation.id))
            .copied()
            .ok_or(ExecutionControllerFailure::UnknownOperation)?;
        ensure!(
            cellar::asyncrt::mono_ms() < deadline
                && u64::try_from(cellar::asyncrt::wall_ms())? < operation.expires_unix_ms,
            ExecutionControllerFailure::UnknownOperation
        );
        Ok(())
    }

    async fn record_outcome(
        &self,
        key: String,
        id: String,
        outcome: ExecutionOutcome,
    ) -> Result<()> {
        self.update(move |state| {
            state
                .slots
                .get_mut(&key)
                .context("Docker fence missing")?
                .operations
                .get_mut(&id)
                .context("Docker operation missing")?
                .outcome = Some(outcome);
            Ok(())
        })
        .await
    }

    /// A failed transport/output collector cannot establish the outcome of a
    /// submitted mutation. Keep its original operation unknown and stop the
    /// process namespace before releasing the owned job. Do not take admission:
    /// a concurrent revocation holds it while joining this very job.
    async fn quarantine_mutation(&self, key: &str) -> Result<()> {
        let pending_key = key.to_owned();
        let inactive = self
            .update(move |state| {
                let slot = state
                    .slots
                    .get_mut(&pending_key)
                    .context("Docker fence missing")?;
                slot.active = false;
                slot.transition = slot
                    .transition
                    .checked_add(1)
                    .context("Docker fence transition exhausted")?;
                Ok(())
            })
            .await;
        // Attempt physical termination even if durable storage failed. That
        // failure still refuses subsequent admission through the journal.
        let stopped = self.engine.stop_all(Some(key)).await;
        inactive?;
        stopped
    }

    /// Stop admission, stop all owned processes, join started operations and
    /// repeat termination after joins to cover a Create already in flight.
    /// Retain containers and the independent store for exact reopening.
    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        let service = self.clone();
        self.domain
            .run(async move { service.shutdown_owned().await })
            .await?
    }

    async fn shutdown_owned(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let _admission = self.admission.lock().await;
        let inactive = self
            .update(|state| {
                for slot in state.slots.values_mut() {
                    slot.active = false;
                }
                Ok(())
            })
            .await;
        // Persistence or an earlier termination failure must not bypass the
        // remaining cleanup. Joining covers entered jobs; the final stop also
        // covers a Create that completed after the first inventory. Return the
        // original failure even when the process namespace has been stopped.
        let stopped = self.engine.stop_all(None).await;
        let joined = self.wait_jobs(None).await;
        let stopped_after_join = self.engine.stop_all(None).await;
        inactive.and(stopped).and(joined).and(stopped_after_join)
    }

    async fn revoke_to(&self, successor: ExecutionClaim) -> Result<()> {
        ensure!(
            !self.closed.load(Ordering::Acquire),
            ExecutionControllerFailure::Unavailable
        );
        let _admission = self.admission.lock().await;
        let claim = successor.clone();
        let key = journal::key(&claim);
        self.update(move |state| {
            ensure!(
                !state.revoked.contains(&journal::history(&claim)?),
                ExecutionControllerFailure::RevokedHistory
            );
            let logical = journal::logical(claim.history(), claim.binding())?;
            let history = journal::history(&claim)?;
            ensure!(
                !state.recoveries.contains_key(&logical),
                ExecutionControllerFailure::RecoveryPending
            );
            ensure!(
                state
                    .incarnations
                    .get(&logical)
                    .is_none_or(|current| *current == history),
                ExecutionControllerFailure::RevokedHistory
            );
            state.incarnations.insert(logical, history);
            if let Some(slot) = state.slots.get_mut(&journal::key(&claim)) {
                ensure!(
                    claim.sequence() >= slot.high.sequence(),
                    ExecutionControllerFailure::StaleClaim
                );
                ensure!(
                    claim.sequence() != slot.high.sequence() || claim == slot.high,
                    ExecutionControllerFailure::ConflictingRequest
                );
                slot.high = claim;
                slot.active = false;
                slot.transition = slot
                    .transition
                    .checked_add(1)
                    .context("Docker fence transition exhausted")?;
            } else {
                state.slots.insert(
                    journal::key(&claim),
                    Slot {
                        high: claim,
                        active: false,
                        transition: 1,
                        operations: BTreeMap::new(),
                    },
                );
            }
            Ok(())
        })
        .await?;
        // Both scans are required: a Create which crossed its original gate can
        // finish after the first scan. Its owned job never disappears on RPC
        // cancellation, and no later step can pass the now-inactive fence.
        self.engine.stop_all(Some(&key)).await?;
        self.wait_jobs(Some(&key)).await?;
        self.engine.stop_all(Some(&key)).await?;
        self.journal.view(|state| {
            let slot = state.slots.get(&key).context("Docker fence missing")?;
            ensure!(
                slot.operations.values().all(|op| op.outcome.is_some()
                    || matches!(
                        op.operation.kind,
                        ExecutionOperationKind::Inspect | ExecutionOperationKind::Read
                    )),
                ExecutionControllerFailure::RecoveryPending
            );
            Ok(())
        })?;
        self.engine.resume_scaffold(self, &successor).await?;
        self.update(move |state| {
            let slot = state.slots.get_mut(&key).context("Docker fence missing")?;
            ensure!(
                slot.high == successor,
                ExecutionControllerFailure::StaleClaim
            );
            slot.active = true;
            Ok(())
        })
        .await
    }

    async fn submit(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> Result<ExecutionOutcome> {
        // Capture the receiver's monotonic expiry before admission or disk I/O.
        // Later wall-clock movement can shorten admission, never extend it.
        let remaining = operation
            .expires_unix_ms
            .saturating_sub(u64::try_from(cellar::asyncrt::wall_ms())?);
        ensure!(
            remaining <= self.options.max_operation_ms,
            ExecutionControllerFailure::ConflictingRequest
        );
        let deadline = cellar::asyncrt::mono_ms()
            .checked_add(remaining)
            .context("Docker operation deadline overflow")?;
        ensure!(
            ExecutionOperation::new(
                operation.id,
                operation.kind,
                &payload,
                operation.expires_unix_ms
            )? == operation,
            ExecutionControllerFailure::ConflictingRequest
        );
        let request = Request::decode(
            &claim,
            &operation,
            &payload,
            &Limits {
                max_body_bytes: self.options.max_body_bytes,
            },
        )?;
        self.engine.validate(&request)?;
        let observation = if matches!(
            operation.kind,
            ExecutionOperationKind::Inspect | ExecutionOperationKind::Read
        ) {
            Some(
                self.observations
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ExecutionControllerFailure::Busy)?,
            )
        } else {
            None
        };
        let _capacity = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| ExecutionControllerFailure::Busy)?;
        let admission = self.admission.lock().await;
        ensure!(
            !self.closed.load(Ordering::Acquire),
            ExecutionControllerFailure::Unavailable
        );
        self.journal.check(&claim)?;
        let id = hex(&operation.id);
        let key = journal::key(&claim);
        let existing = self.journal.view(|state| {
            Ok(state
                .slots
                .get(&key)
                .and_then(|slot| slot.operations.get(&id))
                .cloned())
        })?;
        if let Some(existing) = existing {
            ensure!(
                existing.claim == claim && existing.operation == operation,
                ExecutionControllerFailure::ConflictingRequest
            );
            return existing
                .outcome
                .ok_or(ExecutionControllerFailure::UnknownOperation.into());
        }
        ensure!(
            u64::try_from(cellar::asyncrt::wall_ms())? < operation.expires_unix_ms
                && cellar::asyncrt::mono_ms() < deadline,
            ExecutionControllerFailure::UnknownOperation
        );
        let stored = PhysicalOperation {
            claim: claim.clone(),
            operation: operation.clone(),
            outcome: None,
        };
        let reserve_key = key.clone();
        let reserve_id = id.clone();
        self.update(move |state| {
            state
                .slots
                .get_mut(&reserve_key)
                .context("Docker fence missing")?
                .operations
                .insert(reserve_id, stored);
            Ok(())
        })
        .await?;
        let replaced = self
            .jobs
            .lock()
            .map_err(|_| anyhow::anyhow!("Docker job inventory poisoned"))?
            .entry(key.clone())
            .or_default()
            .insert(operation.id, deadline);
        ensure!(
            replaced.is_none(),
            ExecutionControllerFailure::ConflictingRequest
        );
        let job = Job {
            backend: self.clone(),
            key: key.clone(),
            operation: operation.id,
        };
        drop(admission);
        // The job retains admission and the independent journal lock even if
        // its controller RPC disappears. Revocation joins it before successor
        // admission. An error after physical entry leaves the original unknown.
        self.domain
            .spawn(async move {
                let _capacity = _capacity;
                let _observation = observation;
                job.run(claim, operation, request, deadline).await
            })?
            .await?
    }

    async fn reconcile_operation(
        &self,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> Result<Option<ExecutionOutcome>> {
        // Serialize with physical reservation and job installation, not with
        // an Engine inventory. Absence alone cannot authorize another effect.
        let _admission = self.admission.lock().await;
        let existing = self
            .journal
            .view(|state| Ok(state.original_operation(&current, &original)?.cloned()))?;
        if let Some(existing) = existing {
            // An entered operation keeps its exact receipt, including unknown
            // outcomes. Expiry never converts physical uncertainty into refusal.
            return Ok(existing.outcome);
        }
        if u64::try_from(cellar::asyncrt::wall_ms())? < original.operation.expires_unix_ms {
            return Ok(None);
        }
        self.update(move |state| {
            // Validate again in the durable transition. A cancelled waiter can
            // release admission while its owned journal write is still joining.
            if let Some(existing) = state.original_operation(&current, &original)? {
                return Ok(existing.outcome.clone());
            }
            let outcome = ExecutionOutcome::ExpiredBeforeSubmission;
            state
                .slots
                .get_mut(&journal::key(&original.claim))
                .context("original Docker history missing")?
                .operations
                .insert(
                    hex(&original.operation.id),
                    PhysicalOperation {
                        claim: original.claim,
                        operation: original.operation,
                        outcome: Some(outcome.clone()),
                    },
                );
            // A delayed execute must encounter this exact immutable receipt.
            // Reply only after the tombstone has been synced successfully.
            Ok(Some(outcome))
        })
        .await
    }
}

impl ExecutionControllerBackend for DockerBackend {
    fn identity(&self) -> BackendIdentity {
        self.identity.clone()
    }

    fn revoke(
        self: Arc<Self>,
        _: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        _: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            let claim = successor.context("Docker revocation requires an exact successor")?;
            let service = self.clone();
            self.domain
                .run(async move { service.revoke_to(claim).await })
                .await?
        })
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            let service = self.clone();
            self.domain
                .run(async move { service.submit(claim, operation, payload).await })
                .await?
        })
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<Option<ExecutionOutcome>> {
        Box::pin(async move {
            let service = self.clone();
            self.domain
                .run(async move { service.reconcile_operation(current, original).await })
                .await?
        })
    }

    fn revoke_history(
        self: Arc<Self>,
        old: CellHistory,
        successor: CellHistory,
        _: Option<ExecutionClaim>,
        _: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        let domain = self.domain.clone();
        Box::pin(async move {
            domain
                .run(async move {
                    ensure!(
                        old.cluster == successor.cluster
                            && old.namespace == successor.namespace
                            && old.cell == successor.cell
                            && old.incarnation != successor.incarnation,
                        "invalid Docker history transition"
                    );
                    let _admission = self.admission.lock().await;
                    let revoked = journal::history_id(&old)?;
                    let logical = journal::logical(&old, &self.options.binding)?;
                    let new = journal::history_id(&successor)?;
                    let pending_logical = logical.clone();
                    let pending_new = new.clone();
                    let pending_old = revoked.clone();
                    let old_copy = old.clone();
                    self.update(move |state| {
                        ensure!(
                            state
                                .incarnations
                                .get(&pending_logical)
                                .is_some_and(
                                    |current| current == &pending_old || current == &pending_new
                                ),
                            "Docker recovery source history is unknown"
                        );
                        ensure!(
                            state
                                .recoveries
                                .get(&pending_logical)
                                .is_none_or(|target| target == &pending_new),
                            "Docker recovery successor conflicts"
                        );
                        state.recoveries.insert(pending_logical, pending_new);
                        state.revoked.insert(revoked);
                        for slot in state.slots.values_mut() {
                            if slot.high.history() == &old_copy {
                                slot.active = false;
                            }
                        }
                        Ok(())
                    })
                    .await?;
                    let keys = self.journal.view(|state| {
                        Ok(state
                            .slots
                            .iter()
                            .filter(|(_, slot)| slot.high.history() == &old)
                            .map(|(key, _)| key.clone())
                            .collect::<Vec<_>>())
                    })?;
                    for key in &keys {
                        self.engine.stop_all(Some(key)).await?;
                        self.wait_jobs(Some(key)).await?;
                        self.engine.stop_all(Some(key)).await?;
                    }
                    self.history_fence.clone().fence(old, successor).await?;
                    for key in &keys {
                        self.engine.stop_all(Some(key)).await?;
                    }
                    self.update(move |state| {
                        ensure!(
                            state.recoveries.get(&logical) == Some(&new),
                            "Docker recovery transition changed"
                        );
                        state.incarnations.insert(logical.clone(), new);
                        state.recoveries.remove(&logical);
                        Ok(())
                    })
                    .await
                })
                .await?
        })
    }
}
