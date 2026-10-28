// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded owned turn. Root attempts survive controller loss; samples do not.
use super::attempt::{Attempt, Stage};
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::fleet_catalog::deadline::Deadline;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetRestartOperatorPhase {
    Requested,
    Preparing,
    Gating,
    Settling,
    Verifying,
    Ready,
    CancelRequested,
    Cancelling,
    Cancelled,
    Stopping,
    Offline,
    Returning,
    AwaitReturned,
    VerifyingResume,
    Resuming,
    Completing,
    Complete,
}

pub struct FleetRestartOperatorProgress {
    pub admission: FleetRestartOperatorAdmission,
    pub phase: FleetRestartOperatorPhase,
    pub checked_histories: u64,
    pub total_histories: u64,
    pub inventory_revision: u64,
    pub blocked: Option<FleetRestartSettlementBlocker>,
    /// Historical assessment; guarded stop must renew actual current proof.
    pub ready: Option<FleetRestartReadyAdmission>,
    pub cancelled: Option<FleetRestartOperatorCancelled>,
    pub completed: Option<FleetRestartOperatorCompleted>,
    pub waiting: bool,
}

/// Explicitly constructed orchestration capability. The caller supplies a
/// running trusted observer and any required movement evidence. A dropped
/// controller retains Root work; a recreated driver repeats ephemeral proof.
pub struct FleetRestartOperatorDriver {
    pub(super) catalog: FleetCatalog,
    pub(super) namespaces: Vec<NamespaceCatalog>,
    pub(super) observer: RaftCells,
    pub(super) evidence: Option<Arc<dyn FleetMovementEvidence>>,
    session: Mutex<Option<Session>>,
    resume_session: Mutex<Option<lifecycle::ResumeSession>>,
    busy: AtomicBool,
}

struct Session {
    attempt: [u8; 16],
    walk: FleetRestartVerification,
    checked: u64,
}

struct Lease<'a> {
    owner: &'a FleetRestartOperatorDriver,
    session: Option<Session>,
    resume: Option<lifecycle::ResumeSession>,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.owner.session.lock() {
            *slot = self.session.take();
        }
        if let Ok(mut slot) = self.owner.resume_session.lock() {
            *slot = self.resume.take();
        }
        self.owner.busy.store(false, Ordering::Release);
    }
}

impl Stage {
    pub(super) fn phase(&self) -> FleetRestartOperatorPhase {
        match self {
            Self::Prepare => FleetRestartOperatorPhase::Preparing,
            Self::Gate | Self::Quiesce { .. } => FleetRestartOperatorPhase::Gating,
            Self::Settle => FleetRestartOperatorPhase::Settling,
            Self::Verify => FleetRestartOperatorPhase::Verifying,
            Self::AwaitStop { .. } => FleetRestartOperatorPhase::Ready,
            Self::Cancel { .. } => FleetRestartOperatorPhase::Cancelling,
            Self::ConsentStop { .. } | Self::Stopping => FleetRestartOperatorPhase::Stopping,
            Self::AwaitReturn => FleetRestartOperatorPhase::Offline,
            Self::Returning => FleetRestartOperatorPhase::Returning,
            Self::AwaitReturned => FleetRestartOperatorPhase::AwaitReturned,
            Self::VerifyResume => FleetRestartOperatorPhase::VerifyingResume,
            Self::Resuming => FleetRestartOperatorPhase::Resuming,
            Self::VerifyComplete | Self::Finishing => FleetRestartOperatorPhase::Completing,
        }
    }
}

impl FleetRestartOperatorDriver {
    pub fn new(
        catalog: FleetCatalog,
        namespaces: Vec<NamespaceCatalog>,
        observer: RaftCells,
        evidence: Option<Arc<dyn FleetMovementEvidence>>,
    ) -> Result<Self> {
        if namespaces.len() > 64 {
            return Err(FleetCatalogError::Limit);
        }
        Ok(Self {
            catalog,
            namespaces,
            observer,
            evidence,
            session: Mutex::new(None),
            resume_session: Mutex::new(None),
            busy: AtomicBool::new(false),
        })
    }

    fn lease(&self) -> Result<Lease<'_>> {
        self.busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| FleetCatalogError::Overloaded)?;
        let mut lease = Lease {
            owner: self,
            session: None,
            resume: None,
        };
        lease.session = self
            .session
            .lock()
            .map_err(|_| FleetCatalogError::Closed)?
            .take();
        lease.resume = self
            .resume_session
            .lock()
            .map_err(|_| FleetCatalogError::Closed)?
            .take();
        Ok(lease)
    }

    /// Advance at most one durable child or physical/history step under the
    /// separately admitted human grants and current SDK proof requirements.
    pub fn reconcile(
        &self,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<Option<FleetRestartOperatorProgress>>> {
        Box::pin(async move {
            let catalog = &self.catalog;
            let deadline = Deadline::new(catalog, timeout)?;
            let mut lease = self.lease()?;
            let root = catalog.read(deadline.remaining()?).await?;
            let Some(admission) = catalog
                .active_restart_operator(&root, deadline.remaining()?)
                .await?
            else {
                lease.session = None;
                lease.resume = None;
                return Ok(None);
            };
            let record = &admission.record;
            let attempt = catalog
                .run(
                    deadline.remaining()?,
                    catalog.restart_operator_attempt(&root, record),
                )
                .await?;
            let total = root.head.history_count;
            let cancellation = catalog
                .run(
                    deadline.remaining()?,
                    catalog.operator_cancel_command(&root, record),
                )
                .await?;
            let mut value = FleetRestartOperatorProgress {
                phase: attempt
                    .as_ref()
                    .map_or(FleetRestartOperatorPhase::Requested, |a| a.stage.phase()),
                checked_histories: 0,
                total_histories: total,
                inventory_revision: root.revision(),
                blocked: None,
                ready: None,
                cancelled: None,
                completed: None,
                waiting: false,
                admission,
            };
            if let Some(command) = cancellation {
                let refused = catalog
                    .run(
                        deadline.remaining()?,
                        catalog.operator_cancel_refused(&root, &value.admission.record, &command),
                    )
                    .await?;
                if !refused {
                    if let Some(attempt) = attempt.as_ref().filter(|a| a.facts.offline.is_some()) {
                        catalog
                            .run(
                                deadline.remaining()?,
                                catalog.refuse_operator_cancel(
                                    &root,
                                    &value.admission.record,
                                    attempt,
                                    &command,
                                ),
                            )
                            .await?;
                        value.phase = FleetRestartOperatorPhase::Offline;
                        return Ok(Some(value));
                    }
                    let joined = catalog
                        .run(
                            deadline.remaining()?,
                            catalog.operator_joined_stop(
                                &root,
                                &value.admission.record,
                                attempt.as_ref(),
                            ),
                        )
                        .await?;
                    if joined {
                        let attempt = attempt.as_ref().ok_or(FleetCatalogError::Conflict)?;
                        if !matches!(attempt.stage, Stage::Stopping) {
                            lifecycle::step(
                                self,
                                &value.admission.record,
                                attempt,
                                Stage::Stopping,
                                attempt.facts.clone(),
                                &deadline,
                            )
                            .await?;
                            value.phase = FleetRestartOperatorPhase::Stopping;
                            return Ok(Some(value));
                        }
                    } else {
                        lease.session = None;
                        lease.resume = None;
                        return super::cancellation::reconcile(
                            self,
                            &root,
                            value.admission,
                            attempt,
                            command,
                            &deadline,
                        )
                        .await
                        .map(Some);
                    }
                }
            }
            let Some(attempt) = attempt else {
                catalog
                    .run(
                        deadline.remaining()?,
                        catalog.claim_restart_operator_attempt(
                            &root,
                            &value.admission.record,
                            None,
                            Stage::Prepare,
                        ),
                    )
                    .await?;
                return Ok(Some(value));
            };
            if attempt.facts.stop.is_some()
                && !matches!(
                    attempt.stage,
                    Stage::Stopping
                        | Stage::AwaitReturn
                        | Stage::Returning
                        | Stage::AwaitReturned
                        | Stage::VerifyResume
                        | Stage::Resuming
                        | Stage::VerifyComplete
                        | Stage::Finishing
                )
            {
                let joined = catalog
                    .run(
                        deadline.remaining()?,
                        catalog.operator_joined_stop(
                            &root,
                            &value.admission.record,
                            Some(&attempt),
                        ),
                    )
                    .await?;
                if joined {
                    lease.session = None;
                    lifecycle::step(
                        self,
                        &value.admission.record,
                        &attempt,
                        Stage::Stopping,
                        attempt.facts.clone(),
                        &deadline,
                    )
                    .await?;
                    value.phase = FleetRestartOperatorPhase::Stopping;
                    return Ok(Some(value));
                }
            }
            if matches!(
                attempt.stage,
                Stage::ConsentStop { .. }
                    | Stage::Stopping
                    | Stage::AwaitReturn
                    | Stage::Returning
                    | Stage::AwaitReturned
                    | Stage::VerifyResume
                    | Stage::Resuming
                    | Stage::VerifyComplete
                    | Stage::Finishing
            ) {
                lease.session = None;
                return lifecycle::reconcile(
                    self,
                    &root,
                    &attempt,
                    value,
                    &mut lease.resume,
                    &deadline,
                )
                .await
                .map(Some);
            }
            if lease
                .session
                .as_ref()
                .is_some_and(|s| s.attempt != attempt.request.id)
            {
                lease.session = None;
            }
            match &attempt.stage {
                Stage::ConsentStop { .. }
                | Stage::Stopping
                | Stage::AwaitReturn
                | Stage::Returning
                | Stage::AwaitReturned
                | Stage::VerifyResume
                | Stage::Resuming
                | Stage::VerifyComplete
                | Stage::Finishing => {
                    return Err(FleetCatalogError::Corrupt("unrouted lifecycle step"))
                }
                Stage::Cancel { .. } => {
                    return Err(FleetCatalogError::Corrupt("missing operator cancellation"))
                }
                Stage::Prepare => {
                    self.prepare(&root, &value.admission.record, &attempt, &deadline)
                        .await?
                }
                Stage::Gate => {
                    self.gate(&root, &value.admission.record, &attempt, &deadline)
                        .await?
                }
                Stage::Quiesce { gate } => {
                    let request = gate_request(&value.admission.record, gate.clone())?;
                    catalog
                        .quiesce_restart(&root, &request, &self.observer, deadline.remaining()?)
                        .await?;
                    self.advance(&value.admission.record, &attempt, Stage::Settle, &deadline)
                        .await?;
                }
                Stage::Settle => {
                    match catalog
                        .reconcile_restart_work(
                            ProposalId::new(attempt.request.operation)
                                .map_err(|_| FleetCatalogError::Corrupt("operator operation"))?,
                            value.admission.record.birth_resolution,
                            &self.namespaces,
                            Some(&self.observer),
                            self.evidence.as_deref(),
                            deadline.remaining()?,
                        )
                        .await?
                    {
                        FleetRestartSettlementProgress::CatalogClear { .. } => {
                            self.advance(
                                &value.admission.record,
                                &attempt,
                                Stage::Verify,
                                &deadline,
                            )
                            .await?
                        }
                        FleetRestartSettlementProgress::Blocked(blocker) => {
                            value.blocked = Some(blocker)
                        }
                        _ => (),
                    }
                }
                Stage::Verify => {
                    self.verify(&root, &mut value, &attempt, &mut lease, &deadline)
                        .await?
                }
                Stage::AwaitStop { ready } => {
                    let request = ready_request(&value.admission.record, ready.clone())?;
                    let assessment = catalog
                        .restart_readiness(&request, deadline.remaining()?)
                        .await?
                        .ok_or(FleetCatalogError::Corrupt("missing driver readiness"))?;
                    match catalog
                        .run(deadline.remaining()?, assessment.check_current(catalog))
                        .await
                    {
                        Ok(()) => {
                            value.checked_histories = assessment.candidate().histories();
                            let grant = catalog
                                .run(
                                    deadline.remaining()?,
                                    catalog.operator_action(&root, &value.admission.record, "stop"),
                                )
                                .await?;
                            if grant.is_some() {
                                self.advance(
                                    &value.admission.record,
                                    &attempt,
                                    Stage::ConsentStop {
                                        ready: ready.clone(),
                                    },
                                    &deadline,
                                )
                                .await?;
                                value.phase = FleetRestartOperatorPhase::Stopping;
                            } else {
                                value.ready = Some(assessment);
                            }
                        }
                        Err(FleetCatalogError::Conflict) => {
                            self.advance(
                                &value.admission.record,
                                &attempt,
                                Stage::Verify,
                                &deadline,
                            )
                            .await?;
                            value.phase = FleetRestartOperatorPhase::Verifying;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            value.admission.observed = catalog.read(deadline.remaining()?).await?.revision();
            if let Some(session) = lease.session.as_ref() {
                value.checked_histories = session.checked;
            }
            Ok(Some(value))
        })
    }

    async fn advance(
        &self,
        record: &Record,
        expected: &Attempt,
        stage: Stage,
        deadline: &Deadline<'_>,
    ) -> Result<()> {
        let root = self.catalog.read(deadline.remaining()?).await?;
        self.catalog
            .run(
                deadline.remaining()?,
                self.catalog.claim_restart_operator_attempt(
                    &root,
                    record,
                    Some(expected.journal.id),
                    stage,
                ),
            )
            .await
    }

    async fn prepare(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        attempt: &Attempt,
        deadline: &Deadline<'_>,
    ) -> Result<()> {
        let found = self
            .catalog
            .run(deadline.remaining()?, async {
                let found = self
                    .catalog
                    .inner
                    .lookup(
                        root.head.tables[FleetTable::Operations.slot()],
                        &hex::encode(attempt.request.operation),
                    )
                    .await?;
                if found.is_none() {
                    let node = self
                        .catalog
                        .host_from_record(
                            root,
                            NodeId::new(record.boot.node)
                                .map_err(|_| FleetCatalogError::Corrupt("operator node"))?,
                        )
                        .await?;
                    if node.state() != FleetNodeState::Active || node.boot_binding()? != record.boot
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                Ok(found.is_some())
            })
            .await?;
        let request = FleetRestartRequest {
            identity: attempt.request.clone(),
            node: NodeId::new(record.boot.node)
                .map_err(|_| FleetCatalogError::Corrupt("operator node"))?,
            expected_process: ProcessGeneration::new(record.boot.process)
                .map_err(|_| FleetCatalogError::Corrupt("operator process"))?,
            grace_seconds: record.grace_seconds,
        };
        match self
            .catalog
            .begin_restart_maintenance(root, &request, &self.namespaces, deadline.remaining()?)
            .await
        {
            Ok(_) => {
                let latest = self.catalog.read(deadline.remaining()?).await?;
                let actual = self
                    .catalog
                    .restart_maintenance_admission(
                        &latest,
                        ProposalId::new(attempt.request.operation)
                            .map_err(|_| FleetCatalogError::Corrupt("operator operation"))?,
                        deadline.remaining()?,
                    )
                    .await?;
                if actual.identity() != &attempt.request || actual.boot()? != record.boot {
                    return Err(FleetCatalogError::Conflict);
                }
                self.advance(record, attempt, Stage::Gate, deadline).await
            }
            Err(FleetCatalogError::Conflict)
                if !found && root.revision() > attempt.request.expected_revision =>
            {
                self.advance(record, attempt, Stage::Prepare, deadline)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn gate(
        &self,
        root: &FleetSnapshot,
        record: &Record,
        attempt: &Attempt,
        deadline: &Deadline<'_>,
    ) -> Result<()> {
        let request = gate_request(record, attempt.request.clone())?;
        match self
            .catalog
            .admit_restart_gate(root, &request, deadline.remaining()?)
            .await
        {
            Ok(_) => {
                self.advance(
                    record,
                    attempt,
                    Stage::Quiesce {
                        gate: attempt.request.clone(),
                    },
                    deadline,
                )
                .await
            }
            Err(FleetCatalogError::Conflict)
                if root.revision() > attempt.request.expected_revision =>
            {
                let present = self
                    .catalog
                    .run(
                        deadline.remaining()?,
                        self.catalog.inner.lookup(
                            root.head.tables[FleetTable::Operations.slot()],
                            &super::super::gate::Gate::key(attempt.request.operation),
                        ),
                    )
                    .await?;
                if present.is_some() {
                    return Err(FleetCatalogError::Conflict);
                }
                self.advance(record, attempt, Stage::Gate, deadline).await
            }
            Err(error) => Err(error),
        }
    }

    async fn verify(
        &self,
        root: &FleetSnapshot,
        value: &mut FleetRestartOperatorProgress,
        attempt: &Attempt,
        lease: &mut Lease<'_>,
        deadline: &Deadline<'_>,
    ) -> Result<()> {
        let record = &value.admission.record;
        let request = ready_request(record, attempt.request.clone())?;
        if let Some(ready) = self
            .catalog
            .restart_readiness(&request, deadline.remaining()?)
            .await?
        {
            lease.session = None;
            self.advance(
                record,
                attempt,
                Stage::AwaitStop {
                    ready: ready.identity().clone(),
                },
                deadline,
            )
            .await?;
            value.phase = FleetRestartOperatorPhase::Ready;
            value.checked_histories = ready.candidate().histories();
            value.ready = Some(ready);
            return Ok(());
        }
        // A staged immutable request with no committed receipt may still be
        // in flight. A new coordination CAS fences its old expected Root before
        // a fresh proof attempt can be used; no unknown becomes readiness.
        let staged = self
            .catalog
            .run(
                deadline.remaining()?,
                self.catalog.inner.read::<FleetMutation>(
                    &format!("requests/{}", hex::encode(attempt.request.id)),
                    REQUEST_KIND,
                    REQUEST_LIMIT,
                ),
            )
            .await?
            .is_some();
        if staged || root.revision() != attempt.request.expected_revision {
            lease.session = None;
            self.advance(record, attempt, Stage::Verify, deadline)
                .await?;
            return Ok(());
        }
        if lease.session.is_none() {
            let walk = self
                .catalog
                .prepare_restart_verification(
                    root,
                    ProposalId::new(attempt.request.operation)
                        .map_err(|_| FleetCatalogError::Corrupt("operator operation"))?,
                    &self.namespaces,
                    &self.observer,
                    deadline.remaining()?,
                )
                .await?;
            lease.session = Some(Session {
                attempt: attempt.request.id,
                walk,
                checked: 0,
            });
        }
        let session = lease.session.as_mut().ok_or(FleetCatalogError::Closed)?;
        match session.walk.reconcile_next(deadline.remaining()?).await {
            Ok(Some(FleetRestartRuntimeProgress::Verified(_))) => {
                session.checked = session
                    .checked
                    .checked_add(1)
                    .filter(|n| *n <= root.head.history_count)
                    .ok_or(FleetCatalogError::Limit)?;
            }
            Ok(Some(FleetRestartRuntimeProgress::HandoffRequested { .. }))
            | Ok(Some(FleetRestartRuntimeProgress::Materialized { .. })) => (),
            Ok(None) => {
                let ready = session
                    .walk
                    .publish_ready(&request, deadline.remaining()?)
                    .await?;
                lease.session = None;
                self.advance(
                    record,
                    attempt,
                    Stage::AwaitStop {
                        ready: ready.identity().clone(),
                    },
                    deadline,
                )
                .await?;
                value.phase = FleetRestartOperatorPhase::Ready;
                value.checked_histories = ready.candidate().histories();
                value.ready = Some(ready);
            }
            Err(FleetCatalogError::Conflict) => {
                lease.session = None;
                self.advance(record, attempt, Stage::Verify, deadline)
                    .await?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }
}

pub(super) fn ready_request(
    record: &Record,
    identity: FleetRequestIdentity,
) -> Result<FleetRestartReadyRequest> {
    Ok(FleetRestartReadyRequest {
        identity,
        node: NodeId::new(record.boot.node)
            .map_err(|_| FleetCatalogError::Corrupt("operator node"))?,
        expected_process: ProcessGeneration::new(record.boot.process)
            .map_err(|_| FleetCatalogError::Corrupt("operator process"))?,
    })
}

fn gate_child(record: &Record) -> Result<ProposalId> {
    let mut digest = Sha256::new();
    digest.update(b"cellar-maintenance-driver-gate-1");
    digest.update(record.request.id);
    digest.update(record.boot.boot);
    let bytes: [u8; 32] = digest.finalize().into();
    ProposalId::new(
        bytes[..16]
            .try_into()
            .map_err(|_| FleetCatalogError::Limit)?,
    )
    .map_err(|_| FleetCatalogError::Limit)
}

fn gate_request(
    record: &Record,
    identity: FleetRequestIdentity,
) -> Result<FleetRestartGateRequest> {
    Ok(FleetRestartGateRequest {
        identity,
        node: NodeId::new(record.boot.node)
            .map_err(|_| FleetCatalogError::Corrupt("operator node"))?,
        expected_process: ProcessGeneration::new(record.boot.process)
            .map_err(|_| FleetCatalogError::Corrupt("operator process"))?,
        gate: gate_child(record)?,
    })
}
