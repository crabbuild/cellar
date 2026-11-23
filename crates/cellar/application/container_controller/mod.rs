// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent durable provider ownership and original-operation receipts.
//! An embedding RPC service must authenticate every caller. Claims are metadata;
//! they are not bearer credentials. Keep controller storage outside voter disks.
mod journal;
pub(crate) mod values;
use crate::application::{
    containers::{
        BackendIdentity, BindingIntent, BindingObservation, ExecutionClaim, ExecutionFenceAuthority,
    },
    HandlerFuture,
};
use crate::asyncrt::ExecutionDomain;
use anyhow::{ensure, Context as _, Result};
use cellar_logic::cell_group::CellHistory;
use journal::{History, Journal, Operation, Recovery, Slot};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::sync::{OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};
pub use values::*;

/// A host-selected controller with exclusive, independently durable storage.
/// Create initializes a new empty store without provider work. Open reasserts
/// fences and observes original operations before returning; it never initializes
/// missing state or resets a high-water mark.
pub struct ExecutionController {
    domain: Arc<ExecutionDomain>,
    journal: Arc<Journal>,
    backend: Arc<dyn ExecutionControllerBackend>,
    capacity: Arc<Semaphore>,
    observations: Arc<Semaphore>,
    gates: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    closed: AtomicBool,
}

struct Admission {
    permit: Arc<RequestPermit>,
    _cell: OwnedMutexGuard<()>,
}

struct RequestPermit {
    _capacity: OwnedSemaphorePermit,
    _observation: Option<OwnedSemaphorePermit>,
}

impl ExecutionController {
    pub async fn create(
        domain: Arc<ExecutionDomain>,
        mut options: ExecutionControllerOptions,
        backend: Arc<dyn ExecutionControllerBackend>,
        recovery_secret: [u8; 32],
    ) -> Result<Arc<Self>> {
        ensure!(
            options.controller_id == [0; 16],
            "create requires a fresh controller identity; use open to resume"
        );
        options.controller_id = domain
            .run(async {
                use rand::RngCore;
                let mut id = [0; 16];
                crate::asyncrt::rng("execution_controller_identity").fill_bytes(&mut id);
                id
            })
            .await?;
        ensure!(
            backend.identity() == options.backend,
            "controller backend configuration differs"
        );
        let fs = domain.run(async { crate::asyncrt::fs() }).await?;
        let journal = domain
            .blocking(move || Journal::create(options, fs, recovery_secret))?
            .await??;
        Ok(Self::from_journal(domain, journal, backend))
    }

    pub async fn open(
        domain: Arc<ExecutionDomain>,
        options: ExecutionControllerOptions,
        backend: Arc<dyn ExecutionControllerBackend>,
    ) -> Result<Arc<Self>> {
        ensure!(
            backend.identity() == options.backend,
            "controller backend configuration differs"
        );
        let fs = domain.run(async { crate::asyncrt::fs() }).await?;
        let journal = domain
            .blocking(move || Journal::open(options, fs))?
            .await??;
        let service = Self::from_journal(domain, journal, backend);
        let restoring = service.clone();
        service
            .domain
            .run(async move {
                crate::asyncrt::timeout(
                    Duration::from_millis(restoring.journal.options.limits.operation_timeout_ms),
                    restoring.restore(),
                )
                .await
                .map_err(|_| ExecutionControllerFailure::RecoveryPending)?
            })
            .await??;
        Ok(service)
    }

    fn from_journal(
        domain: Arc<ExecutionDomain>,
        journal: Arc<Journal>,
        backend: Arc<dyn ExecutionControllerBackend>,
    ) -> Arc<Self> {
        Arc::new(Self {
            capacity: Arc::new(Semaphore::new(journal.options.limits.max_inflight)),
            observations: Arc::new(Semaphore::new(
                journal.options.limits.max_inflight.saturating_sub(1).max(1),
            )),
            domain,
            journal,
            backend,
            gates: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        })
    }

    /// Resume configuration for this exact independently owned store. Its
    /// generated identity is covered by the authority's configuration digest.
    pub fn options(&self) -> ExecutionControllerOptions {
        self.journal.options.clone()
    }

    async fn admission(&self, key: &str) -> Result<Arc<Admission>> {
        self.request_admission(key, false).await
    }

    async fn request_admission(&self, key: &str, observation: bool) -> Result<Arc<Admission>> {
        ensure!(
            !self.closed.load(Ordering::Acquire),
            ExecutionControllerFailure::Unavailable
        );
        self.journal.check()?;
        let observation = if observation {
            Some(
                self.observations
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ExecutionControllerFailure::Busy)?,
            )
        } else {
            None
        };
        let capacity = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| ExecutionControllerFailure::Busy)?;
        self.readmit(
            key,
            Arc::new(RequestPermit {
                _capacity: capacity,
                _observation: observation,
            }),
        )
        .await
    }

    async fn readmit(&self, key: &str, permit: Arc<RequestPermit>) -> Result<Arc<Admission>> {
        let gate = {
            let mut gates = self
                .gates
                .lock()
                .map_err(|_| ExecutionControllerFailure::Unavailable)?;
            gates.retain(|_, gate| gate.strong_count() > 0);
            if let Some(gate) = gates.get(key).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(tokio::sync::Mutex::new(()));
                gates.insert(key.into(), Arc::downgrade(&gate));
                gate
            }
        };
        let cell = gate.lock_owned().await;
        ensure!(
            !self.closed.load(Ordering::Acquire),
            ExecutionControllerFailure::Unavailable
        );
        self.journal.check()?;
        Ok(Arc::new(Admission {
            permit,
            _cell: cell,
        }))
    }

    async fn io<T: Send + 'static>(
        &self,
        admission: &Arc<Admission>,
        action: impl FnOnce(&Journal) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let journal = self.journal.clone();
        let admission = admission.clone();
        self.domain
            .blocking(move || {
                // Cancellation cannot release the cell or shutdown admission while
                // an already-started write is still publishing its durable record.
                let _admission = admission;
                action(&journal)
            })?
            .await?
    }

    async fn load(&self, admission: &Arc<Admission>, key: &str) -> Result<Option<Slot>> {
        let key = key.to_owned();
        self.io(admission, move |journal| journal.load(&key)).await
    }

    async fn save(&self, admission: &Arc<Admission>, slot: &Slot) -> Result<()> {
        let slot = slot.clone();
        self.io(admission, move |journal| journal.save(&slot)).await
    }

    fn active<'a>(&self, slot: &'a mut Slot, claim: &ExecutionClaim) -> Result<&'a mut History> {
        journal::check_claim(claim, &self.journal.options)?;
        let incarnation = hex::encode(claim.history().incarnation.bytes());
        ensure!(
            slot.current == incarnation,
            ExecutionControllerFailure::RevokedHistory
        );
        ensure!(
            slot.recovery.is_none(),
            ExecutionControllerFailure::RecoveryPending
        );
        let history = slot
            .histories
            .get_mut(&incarnation)
            .context("controller history missing")?;
        ensure!(!history.revoked, ExecutionControllerFailure::RevokedHistory);
        ensure!(
            history.active.as_ref() == Some(claim) && history.high_water.as_ref() == Some(claim),
            ExecutionControllerFailure::StaleClaim
        );
        Ok(history)
    }

    async fn activate_inner(self: &Arc<Self>, claim: ExecutionClaim) -> Result<()> {
        journal::check_fence(&claim, &self.journal.options)?;
        let key = journal::logical_key(claim.history(), claim.binding());
        let admission = self.admission(&key).await?;
        let (mut slot, registering) = match self.load(&admission, &key).await? {
            Some(slot) => (slot, false),
            None => {
                // Existing generations/resources need explicit recovery and
                // revocation; a new controller cannot silently adopt them.
                ensure!(
                    claim.checkpoint().record.generation == 1
                        && claim.checkpoint().record.intent == BindingIntent::Running
                        && claim.checkpoint().record.observation == BindingObservation::Pending
                        && claim.checkpoint().record.resource_id.is_none(),
                    ExecutionControllerFailure::RevokedHistory
                );
                (Slot::initial(&claim), true)
            }
        };
        let incarnation = hex::encode(claim.history().incarnation.bytes());
        ensure!(
            slot.current == incarnation,
            ExecutionControllerFailure::RevokedHistory
        );
        ensure!(
            slot.recovery.is_none(),
            ExecutionControllerFailure::RecoveryPending
        );
        if claim.checkpoint().record.is_retired() {
            ensure!(
                slot.pending()
                    .iter()
                    .all(|pending| pending.operation.kind.is_observation()),
                ExecutionControllerFailure::UnknownOperation
            );
        }
        let history = slot
            .histories
            .get_mut(&incarnation)
            .context("controller incarnation missing")?;
        ensure!(!history.revoked, ExecutionControllerFailure::RevokedHistory);
        if let Some(high) = &history.high_water {
            ensure!(
                claim.sequence() >= high.sequence()
                    && claim.checkpoint().revision >= high.checkpoint().revision
                    && claim.checkpoint().record.generation >= high.checkpoint().record.generation,
                ExecutionControllerFailure::StaleClaim
            );
            if claim.checkpoint().revision == high.checkpoint().revision {
                ensure!(
                    claim.checkpoint() == high.checkpoint(),
                    ExecutionControllerFailure::ConflictingRequest
                );
            }
            if claim.checkpoint().record.generation == high.checkpoint().record.generation {
                if high.checkpoint().record.is_retired() {
                    ensure!(
                        claim.checkpoint().record == high.checkpoint().record,
                        ExecutionControllerFailure::ConflictingRequest
                    );
                }
                ensure!(
                    claim.checkpoint().record.parameters_digest
                        == high.checkpoint().record.parameters_digest
                        && claim.checkpoint().record.request_id
                            == high.checkpoint().record.request_id,
                    ExecutionControllerFailure::ConflictingRequest
                );
            }
            if claim.sequence() == high.sequence() {
                ensure!(
                    high == &claim,
                    ExecutionControllerFailure::ConflictingRequest
                );
                if history.active.as_ref() == Some(&claim) {
                    return Ok(());
                }
            }
        }
        let previous = history.active.clone();
        history.high_water = Some(claim.clone());
        let unresolved = slot.pending();
        let mut ready = slot.clone();
        ready
            .histories
            .get_mut(&incarnation)
            .context("controller history missing")?
            .active = Some(claim.clone());
        self.io(&admission, move |journal| journal.check_capacity(&ready))
            .await?;
        if registering {
            // Publish the initial high-water in the existing crash-safe
            // registration protocol. An empty registration followed by a
            // second slot write establishes no additional fencing authority.
            // Interrupted registration retains this exact inactive claim;
            // backend entry still waits for the registry to reach Ready.
            let initial = slot.clone();
            self.io(&admission, move |journal| journal.register(&initial))
                .await?;
        } else {
            self.save(&admission, &slot).await?;
        }
        // High-water is durable before revocation. Until the exact completion
        // is also durable, every old claim and every business submission refuses.
        let fenced = crate::asyncrt::timeout(
            Duration::from_millis(self.journal.options.limits.operation_timeout_ms),
            self.backend
                .clone()
                .revoke(previous, Some(claim.clone()), unresolved),
        )
        .await;
        ensure!(
            matches!(fenced, Ok(Ok(()))),
            ExecutionControllerFailure::RecoveryPending
        );
        slot.histories
            .get_mut(&incarnation)
            .context("controller history missing")?
            .active = Some(claim);
        self.save(&admission, &slot)
            .await
            .context(ExecutionControllerFailure::RecoveryPending)
    }

    /// Submit exactly once or return its retained outcome. Pending requests
    /// never replay. The first deadline is immutable and converted once to the
    /// controller's monotonic clock before persistence or backend work. A later
    /// request cannot extend it or authorize another submission.
    pub async fn execute(
        self: &Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> Result<ExecutionOutcome> {
        let service = self.clone();
        self.domain
            .run(async move {
                let remaining = operation
                    .expires_unix_ms
                    .saturating_sub(u64::try_from(crate::asyncrt::wall_ms())?);
                ensure!(
                    remaining <= service.journal.options.limits.operation_timeout_ms,
                    ExecutionControllerFailure::ConflictingRequest
                );
                let now = crate::asyncrt::mono_ms();
                let deadline = now
                    .checked_add(remaining)
                    .context("controller operation deadline overflow")?;
                // An already expired request can only read a receipt or persist its
                // no-submission tombstone. Give that metadata work a bounded budget;
                // the original effect deadline remains expired throughout.
                let response_deadline = if remaining == 0 {
                    now.checked_add(service.journal.options.limits.operation_timeout_ms)
                        .context("controller receipt deadline overflow")?
                } else {
                    deadline
                };
                let result = crate::asyncrt::timeout_at(
                    response_deadline,
                    service.execute_inner(claim, operation, payload, deadline),
                )
                .await
                .map_err(|_| ExecutionControllerFailure::UnknownOperation)?;
                ensure!(
                    crate::asyncrt::mono_ms() < response_deadline,
                    ExecutionControllerFailure::UnknownOperation
                );
                result
            })
            .await?
    }

    async fn execute_inner(
        self: &Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
        deadline: u64,
    ) -> Result<ExecutionOutcome> {
        ensure!(
            !claim.checkpoint().record.is_retired(),
            ExecutionControllerFailure::StaleClaim
        );
        ensure!(
            payload.len() <= self.journal.options.limits.max_payload_bytes,
            ExecutionControllerFailure::Capacity
        );
        ensure!(
            operation.matches_payload(&payload)?,
            ExecutionControllerFailure::ConflictingRequest
        );
        let key = journal::logical_key(claim.history(), claim.binding());
        let admission = self
            .request_admission(&key, operation.kind.is_observation())
            .await?;
        let mut slot = self
            .load(&admission, &key)
            .await?
            .context(ExecutionControllerFailure::StaleClaim)?;
        self.active(&mut slot, &claim)?;
        let incarnation = hex::encode(claim.history().incarnation.bytes());
        let history = slot
            .histories
            .get(&incarnation)
            .context("controller history missing")?;
        let id = hex::encode(operation.id);
        if let Some(previous) = history.operations.get(&id) {
            Self::same_operation(previous, &claim, &operation)?;
            return previous
                .outcome
                .clone()
                .ok_or_else(|| ExecutionControllerFailure::UnknownOperation.into());
        }
        // Historical operations retain their provenance. Only explicit
        // reconciliation may observe them under a fresh incarnation.
        ensure!(
            slot.histories
                .values()
                .all(|history| !history.operations.contains_key(&id)),
            ExecutionControllerFailure::ConflictingRequest
        );
        ensure!(
            history.operations.len() < self.journal.options.limits.max_operations_per_incarnation,
            ExecutionControllerFailure::Capacity
        );
        ensure!(
            operation.kind.is_observation()
                || slot
                    .histories
                    .values()
                    .flat_map(|history| history.operations.values())
                    .all(|op| op.outcome.is_some() || op.request.kind.is_observation()),
            ExecutionControllerFailure::UnknownOperation
        );
        if matches!(
            operation.kind,
            ExecutionOperationKind::Create | ExecutionOperationKind::Delete
        ) {
            ensure!(
                slot.histories
                    .values()
                    .flat_map(|history| history.operations.values())
                    .all(|op| {
                        let same_generation = op.claim.history() == claim.history()
                            && op.claim.checkpoint().record.generation
                                == claim.checkpoint().record.generation;
                        let original_create = op.claim.checkpoint().record.request_id
                            == claim.checkpoint().record.request_id;
                        op.request.kind != operation.kind
                            || !(same_generation || original_create)
                            || matches!(
                                op.outcome.as_ref(),
                                Some(
                                    ExecutionOutcome::Refused
                                        | ExecutionOutcome::ExpiredBeforeSubmission
                                )
                            )
                    }),
                ExecutionControllerFailure::ConflictingRequest
            );
        }
        self.active(&mut slot, &claim)?.operations.insert(
            id.clone(),
            Operation {
                request: operation.clone(),
                claim: claim.clone(),
                outcome: None,
                submitted: false,
            },
        );
        self.save(&admission, &slot).await?;
        if crate::asyncrt::mono_ms() >= deadline {
            self.finish(
                &admission,
                &mut slot,
                &claim,
                &claim,
                &id,
                ExecutionOutcome::ExpiredBeforeSubmission,
            )
            .await?;
            return Ok(ExecutionOutcome::ExpiredBeforeSubmission);
        }
        self.active(&mut slot, &claim)?
            .operations
            .get_mut(&id)
            .context("reserved operation missing")?
            .submitted = true;
        self.save(&admission, &slot).await?;
        if crate::asyncrt::mono_ms() >= deadline {
            self.finish(
                &admission,
                &mut slot,
                &claim,
                &claim,
                &id,
                ExecutionOutcome::ExpiredBeforeSubmission,
            )
            .await?;
            return Ok(ExecutionOutcome::ExpiredBeforeSubmission);
        }
        let observation = operation.kind.is_observation();
        let permit = admission.permit.clone();
        let admission = if observation {
            drop(admission);
            None
        } else {
            Some(admission)
        };
        let outcome = self
            .backend
            .clone()
            .execute(claim.clone(), operation, payload)
            .await
            .map_err(|_| ExecutionControllerFailure::UnknownOperation)?;
        let admission = match admission {
            Some(admission) => admission,
            None => {
                let admission = self
                    .readmit(&key, permit)
                    .await
                    .context(ExecutionControllerFailure::UnknownOperation)?;
                // Other owners/control calls may have progressed during a long
                // observation. Never publish our old copy over their state.
                slot = self
                    .load(&admission, &key)
                    .await?
                    .context(ExecutionControllerFailure::UnknownOperation)?;
                admission
            }
        };
        self.finish(&admission, &mut slot, &claim, &claim, &id, outcome.clone())
            .await
            .context(ExecutionControllerFailure::UnknownOperation)?;
        Ok(outcome)
    }

    fn same_operation(
        previous: &Operation,
        claim: &ExecutionClaim,
        request: &ExecutionOperation,
    ) -> Result<()> {
        ensure!(
            previous.request.id == request.id
                && previous.request.kind == request.kind
                && previous.request.payload_digest == request.payload_digest
                && previous.claim.history() == claim.history()
                && previous.claim.checkpoint().record.generation
                    == claim.checkpoint().record.generation
                && previous.claim.checkpoint().record.request_id
                    == claim.checkpoint().record.request_id,
            ExecutionControllerFailure::ConflictingRequest
        );
        Ok(())
    }

    async fn finish(
        &self,
        admission: &Arc<Admission>,
        slot: &mut Slot,
        claim: &ExecutionClaim,
        original: &ExecutionClaim,
        id: &str,
        outcome: ExecutionOutcome,
    ) -> Result<()> {
        if let ExecutionOutcome::Completed(bytes) = &outcome {
            ensure!(
                bytes.len() <= self.journal.options.limits.max_result_bytes,
                ExecutionControllerFailure::UnknownOperation
            );
        }
        self.active(slot, claim)?;
        ensure!(
            journal::logical_key(original.history(), original.binding()) == slot.key,
            ExecutionControllerFailure::ConflictingRequest
        );
        let origin = hex::encode(original.history().incarnation.bytes());
        let history = slot
            .histories
            .get_mut(&origin)
            .context("original controller history missing")?;
        ensure!(
            origin == slot.current || history.revoked,
            ExecutionControllerFailure::RevokedHistory
        );
        let operation = history
            .operations
            .get_mut(id)
            .context("original controller operation missing")?;
        ensure!(
            &operation.claim == original && operation.outcome.is_none(),
            ExecutionControllerFailure::ConflictingRequest
        );
        operation.outcome = Some(outcome);
        self.save(admission, slot)
            .await
            .context(ExecutionControllerFailure::UnknownOperation)
    }

    /// Reconcile the original pending request without replaying it. This is an
    /// observation under fresh invocation authority, so the original mutation's
    /// expired deadline does not prevent reading its retained outcome.
    pub async fn reconcile(
        self: &Arc<Self>,
        claim: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> Result<ExecutionOutcome> {
        let service = self.clone();
        self.domain
            .run(async move {
                crate::asyncrt::timeout(
                    Duration::from_millis(service.journal.options.limits.operation_timeout_ms),
                    async {
                        let key = journal::logical_key(claim.history(), claim.binding());
                        let admission = service.admission(&key).await?;
                        let mut slot = service
                            .load(&admission, &key)
                            .await?
                            .context(ExecutionControllerFailure::StaleClaim)?;
                        service.active(&mut slot, &claim)?;
                        ensure!(
                            journal::logical_key(
                                original.claim.history(),
                                original.claim.binding()
                            ) == key,
                            ExecutionControllerFailure::ConflictingRequest
                        );
                        let origin = hex::encode(original.claim.history().incarnation.bytes());
                        let id = hex::encode(original.operation.id);
                        let previous = slot
                            .histories
                            .get(&origin)
                            .and_then(|history| history.operations.get(&id))
                            .context(ExecutionControllerFailure::UnknownOperation)?
                            .clone();
                        ensure!(
                            previous.claim == original.claim
                                && previous.request == original.operation,
                            ExecutionControllerFailure::ConflictingRequest
                        );
                        if let Some(outcome) = previous.outcome {
                            return Ok(outcome);
                        }
                        let outcome = if previous.submitted {
                            match service
                                .backend
                                .clone()
                                .reconcile(claim.clone(), original)
                                .await
                            {
                                Ok(Some(outcome)) => outcome,
                                _ => {
                                    return Err(ExecutionControllerFailure::UnknownOperation.into())
                                }
                            }
                        } else {
                            ExecutionOutcome::Refused
                        };
                        service
                            .finish(
                                &admission,
                                &mut slot,
                                &claim,
                                &previous.claim,
                                &id,
                                outcome.clone(),
                            )
                            .await?;
                        Ok(outcome)
                    },
                )
                .await
                .map_err(|_| ExecutionControllerFailure::UnknownOperation)?
            })
            .await?
    }

    /// Read bounded pending metadata under the current claim, including old
    /// generations and permanently revoked incarnations. These original claims
    /// authorize observation only through explicit reconciliation, never replay.
    pub async fn pending(
        self: &Arc<Self>,
        claim: ExecutionClaim,
    ) -> Result<Vec<ExecutionPendingOperation>> {
        let service = self.clone();
        self.domain
            .run(async move {
                crate::asyncrt::timeout(
                    Duration::from_millis(service.journal.options.limits.operation_timeout_ms),
                    async {
                        let key = journal::logical_key(claim.history(), claim.binding());
                        let admission = service.admission(&key).await?;
                        let mut slot = service
                            .load(&admission, &key)
                            .await?
                            .context(ExecutionControllerFailure::StaleClaim)?;
                        service.active(&mut slot, &claim)?;
                        Ok(slot.pending())
                    },
                )
                .await
                .map_err(|_| ExecutionControllerFailure::Unavailable)?
            })
            .await?
    }

    /// Find the original create/delete for this binding request, including a
    /// completed receipt omitted by a restored cell image. This never submits
    /// provider work. Multiple candidates refuse rather than inventing order
    /// between incarnation-local ownership sequences.
    pub async fn lifecycle(
        self: &Arc<Self>,
        claim: ExecutionClaim,
        kind: ExecutionOperationKind,
    ) -> Result<Option<ExecutionPendingOperation>> {
        ensure!(
            matches!(
                kind,
                ExecutionOperationKind::Create | ExecutionOperationKind::Delete
            ),
            ExecutionControllerFailure::ConflictingRequest
        );
        let service = self.clone();
        self.domain
            .run(async move {
                crate::asyncrt::timeout(
                    Duration::from_millis(service.journal.options.limits.operation_timeout_ms),
                    async {
                        let key = journal::logical_key(claim.history(), claim.binding());
                        let admission = service.admission(&key).await?;
                        let mut slot = service
                            .load(&admission, &key)
                            .await?
                            .context(ExecutionControllerFailure::StaleClaim)?;
                        service.active(&mut slot, &claim)?;
                        let record = &claim.checkpoint().record;
                        let mut found = None;
                        let mut refused = None;
                        for operation in slot
                            .histories
                            .values()
                            .flat_map(|history| history.operations.values())
                        {
                            let original = &operation.claim.checkpoint().record;
                            if operation.request.kind == kind
                                && original.request_id == record.request_id
                                && original.generation == record.generation
                                && original.parameters_digest == record.parameters_digest
                            {
                                let original = ExecutionPendingOperation {
                                    claim: operation.claim.clone(),
                                    operation: operation.request.clone(),
                                };
                                if matches!(
                                    operation.outcome,
                                    Some(
                                        ExecutionOutcome::Refused
                                            | ExecutionOutcome::ExpiredBeforeSubmission
                                    )
                                ) {
                                    // No ordering between old histories is inferred:
                                    // every such candidate proves no provider effect.
                                    refused.get_or_insert(original);
                                } else {
                                    ensure!(
                                        found.is_none(),
                                        ExecutionControllerFailure::ConflictingRequest
                                    );
                                    found = Some(original);
                                }
                            }
                        }
                        Ok(found.or(refused))
                    },
                )
                .await
                .map_err(|_| ExecutionControllerFailure::Unavailable)?
            })
            .await?
    }

    /// Atomically seal an operation which has not reached controller dispatch.
    /// Recovery supplies its original ID/kind/digest with an expired deadline.
    /// If it already exists, return its exact original metadata without changing
    /// submission, deadline or result. Otherwise persist a no-submission receipt
    /// before acknowledging. A late queued copy can then never enter the backend.
    pub async fn seal_unsubmitted(
        self: &Arc<Self>,
        claim: ExecutionClaim,
        request: ExecutionOperation,
    ) -> Result<ExecutionPendingOperation> {
        ensure!(
            request.id != [0; 16] && request.expires_unix_ms > 0,
            ExecutionControllerFailure::ConflictingRequest
        );
        let service = self.clone();
        self.domain
            .run(async move {
                ensure!(
                    request.expires_unix_ms <= u64::try_from(crate::asyncrt::wall_ms())?,
                    ExecutionControllerFailure::ConflictingRequest
                );
                crate::asyncrt::timeout(
                    Duration::from_millis(service.journal.options.limits.operation_timeout_ms),
                    async {
                        let key = journal::logical_key(claim.history(), claim.binding());
                        let admission = service.admission(&key).await?;
                        let mut slot = service
                            .load(&admission, &key)
                            .await?
                            .context(ExecutionControllerFailure::StaleClaim)?;
                        service.active(&mut slot, &claim)?;
                        let id = hex::encode(request.id);
                        if let Some(previous) = slot
                            .histories
                            .values()
                            .find_map(|history| history.operations.get(&id))
                        {
                            ensure!(
                                previous.request.kind == request.kind
                                    && previous.request.payload_digest == request.payload_digest
                                    && previous.claim.checkpoint().record.request_id
                                        == claim.checkpoint().record.request_id
                                    && previous.claim.checkpoint().record.generation
                                        == claim.checkpoint().record.generation,
                                ExecutionControllerFailure::ConflictingRequest
                            );
                            return Ok(ExecutionPendingOperation {
                                claim: previous.claim.clone(),
                                operation: previous.request.clone(),
                            });
                        }
                        let history = service.active(&mut slot, &claim)?;
                        ensure!(
                            history.operations.len()
                                < service
                                    .journal
                                    .options
                                    .limits
                                    .max_operations_per_incarnation,
                            ExecutionControllerFailure::Capacity
                        );
                        history.operations.insert(
                            id,
                            Operation {
                                request: request.clone(),
                                claim: claim.clone(),
                                submitted: false,
                                outcome: Some(ExecutionOutcome::Refused),
                            },
                        );
                        service.save(&admission, &slot).await?;
                        Ok(ExecutionPendingOperation {
                            claim,
                            operation: request,
                        })
                    },
                )
                .await
                .map_err(|_| ExecutionControllerFailure::UnknownOperation)?
            })
            .await?
    }

    async fn restore(self: &Arc<Self>) -> Result<()> {
        let journal = self.journal.clone();
        let keys = self.domain.blocking(move || journal.keys())?.await??;
        for key in keys {
            let admission = self.admission(&key).await?;
            let mut slot = self
                .load(&admission, &key)
                .await?
                .context("registered controller state disappeared")?;
            if let Some(recovery) = slot.recovery.clone() {
                let old = slot
                    .histories
                    .get(&recovery.from)
                    .context("pending revocation source missing")?;
                let unresolved = slot.pending();
                let (old_history, successor_history) = recovery.histories()?;
                self.backend
                    .clone()
                    .revoke_history(
                        old_history,
                        successor_history,
                        old.active.clone(),
                        unresolved,
                    )
                    .await
                    .context(ExecutionControllerFailure::RecoveryPending)?;
                slot.histories
                    .get_mut(&recovery.from)
                    .context("revocation source missing")?
                    .active = None;
                slot.recovery = None;
                self.save(&admission, &slot).await?;
            }
            let active = slot
                .histories
                .get(&slot.current)
                .context("controller current history missing")?
                .clone();
            if let Some(successor) = active.high_water.clone() {
                // The independently owned provider may also have restarted.
                // Reassert its fence even when our durable activation is complete.
                // This performs no business operation or uncertain resubmission.
                self.backend
                    .clone()
                    .revoke(
                        active.active.clone(),
                        Some(successor.clone()),
                        slot.pending(),
                    )
                    .await
                    .context(ExecutionControllerFailure::RecoveryPending)?;
                if active.high_water != active.active {
                    slot.histories
                        .get_mut(&slot.current)
                        .context("controller current history missing")?
                        .active = Some(successor);
                    self.save(&admission, &slot).await?;
                }
            }
            let active = slot
                .histories
                .get(&slot.current)
                .context("controller current history missing")?
                .active
                .clone();
            if let Some(active) = active {
                for original in slot.pending() {
                    let origin = hex::encode(original.claim.history().incarnation.bytes());
                    let id = hex::encode(original.operation.id);
                    let submitted = slot
                        .histories
                        .get(&origin)
                        .and_then(|history| history.operations.get(&id))
                        .context("original pending controller operation missing")?
                        .submitted;
                    let observed = if submitted {
                        // An observation failure retains the original unknown
                        // obligation; it cannot become proof of no execution.
                        self.backend
                            .clone()
                            .reconcile(active.clone(), original.clone())
                            .await
                            .ok()
                            .flatten()
                    } else {
                        Some(ExecutionOutcome::Refused)
                    };
                    if let Some(outcome) = observed {
                        self.finish(
                            &admission,
                            &mut slot,
                            &active,
                            &original.claim,
                            &id,
                            outcome,
                        )
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Permanently revoke an old logical history before allowing a successor.
    /// The recovery credential belongs to the independent operator/controller,
    /// never a voter or application journal. A partial transition refuses both
    /// histories until this same authorized call completes its revocation.
    pub async fn recover_history(
        self: &Arc<Self>,
        old: CellHistory,
        successor: CellHistory,
        recovery_secret: [u8; 32],
    ) -> Result<()> {
        let service = self.clone();
        self.domain
            .run(async move {
                crate::asyncrt::timeout(
                    Duration::from_millis(service.journal.options.limits.operation_timeout_ms),
                    async {
                        let binding = service.journal.options.backend.binding.clone();
                        let key = journal::logical_key(&old, &binding);
                        ensure!(
                            key == journal::logical_key(&successor, &binding)
                                && old.incarnation != successor.incarnation,
                            ExecutionControllerFailure::ConflictingRequest
                        );
                        let admission = service.admission(&key).await?;
                        service
                            .io(&admission, move |journal| {
                                journal.authorize_recovery(&recovery_secret)
                            })
                            .await?;
                        let mut slot = service
                            .load(&admission, &key)
                            .await?
                            .context(ExecutionControllerFailure::RevokedHistory)?;
                        let from = hex::encode(old.incarnation.bytes());
                        let to = hex::encode(successor.incarnation.bytes());
                        let transition = Recovery {
                            from: from.clone(),
                            to: to.clone(),
                            cluster: old.cluster.bytes(),
                            namespace: old.namespace.bytes(),
                            cell: old.cell.as_str().into(),
                        };
                        if slot.current == to && slot.recovery.is_none() {
                            ensure!(
                                slot.histories
                                    .get(&from)
                                    .is_some_and(|history| history.revoked),
                                ExecutionControllerFailure::RevokedHistory
                            );
                            return Ok(());
                        }
                        if let Some(pending) = &slot.recovery {
                            ensure!(
                                pending == &transition,
                                ExecutionControllerFailure::ConflictingRequest
                            );
                        } else {
                            ensure!(
                                slot.current == from && !slot.histories.contains_key(&to),
                                ExecutionControllerFailure::RevokedHistory
                            );
                            ensure!(
                                slot.histories.len()
                                    < service.journal.options.limits.max_incarnations_per_cell,
                                ExecutionControllerFailure::Capacity
                            );
                            slot.histories
                                .get_mut(&from)
                                .context("recovery source missing")?
                                .revoked = true;
                            slot.histories.insert(
                                to.clone(),
                                History {
                                    high_water: None,
                                    active: None,
                                    revoked: false,
                                    imported: true,
                                    operations: Default::default(),
                                },
                            );
                            slot.current = to;
                            slot.recovery = Some(transition);
                            service.save(&admission, &slot).await?;
                        }
                        let prior = slot
                            .histories
                            .get(&from)
                            .context("revoked controller history missing")?;
                        let unresolved = slot.pending();
                        let fenced = crate::asyncrt::timeout(
                            Duration::from_millis(
                                service.journal.options.limits.operation_timeout_ms,
                            ),
                            service.backend.clone().revoke_history(
                                old,
                                successor,
                                prior.active.clone(),
                                unresolved,
                            ),
                        )
                        .await;
                        ensure!(
                            matches!(fenced, Ok(Ok(()))),
                            ExecutionControllerFailure::RecoveryPending
                        );
                        slot.histories
                            .get_mut(&from)
                            .context("revoked history missing")?
                            .active = None;
                        slot.recovery = None;
                        service
                            .save(&admission, &slot)
                            .await
                            .context(ExecutionControllerFailure::RecoveryPending)
                    },
                )
                .await
                .map_err(|_| ExecutionControllerFailure::RecoveryPending)?
            })
            .await?
    }

    /// Stop admission, join every admitted request and already-started durable
    /// file operation, then release the directory lock. The caller subsequently
    /// shuts down the controller's execution domain. Cancellation is retryable.
    pub async fn shutdown(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let all = self
            .capacity
            .clone()
            .acquire_many_owned(u32::try_from(self.journal.options.limits.max_inflight)?)
            .await?;
        self.journal.release()?;
        drop(all);
        Ok(())
    }
}

impl ExecutionFenceAuthority for ExecutionController {
    fn identity(&self) -> BackendIdentity {
        self.journal.options.fenced_backend()
    }

    fn activate(self: Arc<Self>, claim: ExecutionClaim) -> HandlerFuture<()> {
        Box::pin(async move {
            let service = self.clone();
            self.domain
                .run(async move {
                    crate::asyncrt::timeout(
                        Duration::from_millis(service.journal.options.limits.operation_timeout_ms),
                        service.activate_inner(claim),
                    )
                    .await
                    .map_err(|_| ExecutionControllerFailure::RecoveryPending)?
                })
                .await?
        })
    }
}
