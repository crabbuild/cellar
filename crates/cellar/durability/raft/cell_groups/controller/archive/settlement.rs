// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operation proofs share the archive lane; waiting never owns a SQL producer.
use super::reuse::{Admission, Eligibility};
use super::*;
use crate::raft::budget::Permit;
use crate::raft::cell_groups::{
    CellDispatchError, CellSqlEnrollment, CellSqlReceipt, CellSqlReceiptLookup,
};
use crate::raft::execution::{CommandRequest, ExecutionError, ExecutionLimits};
use cellar_logic::cell_group::GroupRoute;
use std::sync::{atomic::AtomicBool, Mutex, Weak};

#[derive(Clone)]
pub(crate) struct Client(Weak<Shared>);

pub(crate) struct OutputProof {
    _coverage: CellGroupArchiveCoverage,
    // The exclusive journal lock survives response buffering and node close.
    // A replacement owner cannot retire this hold while a consumer still uses it.
    _journal: Arc<journal::Journal>,
    _permit: Arc<Mutex<Permit>>,
    authority: Arc<job::Authority>,
}

impl OutputProof {
    pub(crate) async fn require_current(
        &self,
        domain: &crate::asyncrt::DomainToken,
        deadline: u64,
    ) -> std::result::Result<(), CellDispatchError> {
        let now = domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        if now >= deadline {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        let timer = domain
            .sleep_until(deadline)
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        let result = crate::asyncrt::select! {
            result = domain.run(self.authority.require_current()) => result.map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
            _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        if domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
            >= deadline
        {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        result.map_err(|error| CellDispatchError::Archive(Arc::new(error)))
    }
}

pub(crate) struct CommandSettled {
    pub receipt: Box<CellSqlReceipt>,
    pub proof: Box<OutputProof>,
}

pub(crate) struct EnrollmentSettled {
    pub enrollment: Box<CellSqlEnrollment>,
    pub proof: Box<OutputProof>,
}

pub(crate) struct LookupSettled {
    pub selected: Box<CellSqlReceiptLookup>,
    pub proof: Box<OutputProof>,
}

struct Lookup {
    selected: Box<CellSqlReceiptLookup>,
    request: CommandRequest,
}

/// Admitted immutable observation; no node work is queued until response.
pub(crate) struct LookupPrepared(Prepared);

impl LookupPrepared {
    pub(crate) async fn response(self) -> std::result::Result<LookupSettled, CellDispatchError> {
        let settled = self.0.response().await?;
        let Output::Lookup(lookup) = settled.output else {
            return Err(CellDispatchError::Invalid("S3 lookup output"));
        };
        Ok(LookupSettled {
            selected: lookup.selected,
            proof: settled.proof,
        })
    }
}

enum Output {
    Command(Box<CellSqlReceipt>),
    Enrollment(Box<CellSqlEnrollment>),
    Lookup(Box<Lookup>),
}

impl Output {
    fn retained_bytes(&self) -> std::result::Result<usize, CellDispatchError> {
        match self {
            Self::Command(receipt) => receipt.receipt().receipt.result().len(),
            // Creation records are bounded by the protocol to 4096 bytes. Their
            // variable identity bytes and the archive comparison stay admitted.
            Self::Enrollment(_) => 4096,
            Self::Lookup(lookup) => lookup
                .selected
                .receipt()
                .map_or(0, |row| row.result().len()),
        }
        .checked_mul(2)
        .and_then(|n| n.checked_add(8192))
        .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))
    }

    fn retain(self, permit: Arc<Mutex<Permit>>) -> Self {
        match self {
            Self::Command(receipt) => Self::Command(receipt.retain_for_archive(permit)),
            Self::Enrollment(enrollment) => Self::Enrollment(enrollment.retain_for_archive(permit)),
            Self::Lookup(lookup) => Self::Lookup(Box::new(Lookup {
                selected: lookup.selected.retain_for_archive(permit),
                request: lookup.request,
            })),
        }
    }
}

struct Settled {
    output: Output,
    proof: Box<OutputProof>,
}

pub(in crate::raft::cell_groups::controller) struct Request {
    pub route: GroupRoute,
    output: Output,
    limits: Option<ExecutionLimits>,
    deadline: u64,
    cancelled: Arc<AtomicBool>,
    reply: oneshot::Sender<std::result::Result<Settled, CellDispatchError>>,
    permit: Arc<Mutex<Permit>>,
    attempted_cut: Option<cellar_logic::consensus::LogIndex>,
    admission: Option<Admission>,
}

struct Cancel(Arc<AtomicBool>);

impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl CellGroupController {
    pub(crate) fn s3_client(
        &self,
        domain: &crate::asyncrt::DomainToken,
    ) -> std::result::Result<Client, RuntimeError> {
        if !self.handle.0.domain.same_owner(domain) {
            return Err(RuntimeError::Invalid("S3 controller domain".into()));
        }
        Ok(Client(Arc::downgrade(&self.handle.0)))
    }
}

impl Client {
    pub(crate) fn require_runtime(
        &self,
        runtime: &CellGroupRuntime,
    ) -> std::result::Result<(), CellDispatchError> {
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        if !shared.runtime.same_owner(runtime) {
            return Err(CellDispatchError::Runtime(RuntimeError::Invalid(
                "S3 runtime owner".into(),
            )));
        }
        Ok(())
    }

    pub(crate) fn require_group(
        &self,
        route: &GroupRoute,
    ) -> std::result::Result<(), CellDispatchError> {
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        let state = shared
            .archives
            .lock()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
            .get(&route.history)
            .cloned();
        let Some(state) = state else {
            // A transfer can activate before its archive binding exists. An S3
            // request still refuses until an owned, verified local publisher is
            // installed. Coalesce discovery before enqueueing to preserve the
            // ordinary command queue during a missing-binding outage.
            let configured = shared
                .provisioning
                .lock()
                .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
                .as_ref()
                .is_some_and(|config| config.archives.is_some());
            if configured
                && shared
                    .groups
                    .contains(&route.history)
                    .map_err(CellDispatchError::Runtime)?
            {
                let mut pending = shared
                    .archive_discovery
                    .lock()
                    .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
                if !pending.contains(&route.history) && pending.len() >= shared.limits.max_groups {
                    return Err(CellDispatchError::Runtime(RuntimeError::Overloaded));
                }
                if pending.insert(route.history) {
                    if let Err(error) = shared
                        .requests
                        .try_send(requests::Request::EnsureArchive(route.history))
                    {
                        pending.remove(&route.history);
                        return Err(CellDispatchError::Runtime(match error {
                            tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                RuntimeError::Overloaded
                            }
                            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                RuntimeError::Closed
                            }
                        }));
                    }
                }
            }
            return Err(CellDispatchError::Runtime(RuntimeError::NotReady));
        };
        if state.stopped {
            return Err(state.error.clone().map_or(
                CellDispatchError::Runtime(RuntimeError::Closed),
                CellDispatchError::Archive,
            ));
        }
        Ok(())
    }

    pub(crate) async fn command(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        receipt: Box<CellSqlReceipt>,
        limits: ExecutionLimits,
        deadline: u64,
    ) -> std::result::Result<CommandSettled, CellDispatchError> {
        let settled = self
            .settle(
                runtime,
                route,
                Output::Command(receipt),
                Some(limits),
                deadline,
            )
            .await?;
        let Output::Command(receipt) = settled.output else {
            return Err(CellDispatchError::Invalid("S3 command output"));
        };
        Ok(CommandSettled {
            receipt,
            proof: settled.proof,
        })
    }

    pub(crate) async fn enrollment(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        enrollment: Box<CellSqlEnrollment>,
        deadline: u64,
    ) -> std::result::Result<EnrollmentSettled, CellDispatchError> {
        let settled = self
            .settle(
                runtime,
                route,
                Output::Enrollment(enrollment),
                None,
                deadline,
            )
            .await?;
        let Output::Enrollment(enrollment) = settled.output else {
            return Err(CellDispatchError::Invalid("S3 enrollment output"));
        };
        Ok(EnrollmentSettled {
            enrollment,
            proof: settled.proof,
        })
    }

    pub(crate) fn prepare_lookup(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        selected: Box<CellSqlReceiptLookup>,
        request: CommandRequest,
        limits: ExecutionLimits,
        deadline: u64,
    ) -> std::result::Result<LookupPrepared, CellDispatchError> {
        self.prepare(
            runtime,
            route,
            Output::Lookup(Box::new(Lookup { selected, request })),
            Some(limits),
            deadline,
        )
        .map(LookupPrepared)
    }

    async fn settle(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        output: Output,
        limits: Option<ExecutionLimits>,
        deadline: u64,
    ) -> std::result::Result<Settled, CellDispatchError> {
        self.prepare(runtime, route, output, limits, deadline)?
            .response()
            .await
    }

    fn prepare(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        output: Output,
        limits: Option<ExecutionLimits>,
        deadline: u64,
    ) -> std::result::Result<Prepared, CellDispatchError> {
        self.require_runtime(runtime)?;
        self.require_group(&route)?;
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        let permit = shared
            .s3
            .acquire(output.retained_bytes()?)
            .map(|permit| Arc::new(Mutex::new(permit)))
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let output = output.retain(permit.clone());
        let (reply, receiver) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let now = shared
            .domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        if now >= deadline {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        Ok(Prepared {
            client: self.clone(),
            request: Request {
                route,
                output,
                limits,
                deadline,
                cancelled: cancelled.clone(),
                reply,
                permit,
                attempted_cut: None,
                admission: None,
            },
            receiver,
            _cancel: Cancel(cancelled),
        })
    }
}

struct Prepared {
    client: Client,
    request: Request,
    receiver: oneshot::Receiver<std::result::Result<Settled, CellDispatchError>>,
    _cancel: Cancel,
}

impl Prepared {
    async fn response(self) -> std::result::Result<Settled, CellDispatchError> {
        let Self {
            client,
            request,
            receiver,
            _cancel,
        } = self;
        let shared = client
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        let deadline = request.deadline;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        let now = shared
            .domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        if now >= deadline {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        shared
            .requests
            .try_send(requests::Request::S3(Box::new(request)))
            .map_err(|e| {
                CellDispatchError::Runtime(match e {
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
                    tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                })
            })?;
        let timer = shared
            .domain
            .sleep_until(deadline)
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        let result = crate::asyncrt::select! {
            result = receiver => result.map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
            _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        if shared
            .domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
            >= deadline
        {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        result
    }
}

impl Request {
    pub(super) fn admit(&mut self, admission: Admission) {
        self.admission = Some(admission);
    }

    pub(super) fn admission(&self) -> Result<Admission> {
        self.admission
            .ok_or(Error::Invalid("unadmitted archive request"))
    }

    pub(super) fn may_reuse(&self, index: cellar_logic::consensus::LogIndex) -> bool {
        self.attempted_cut.is_none_or(|previous| previous < index)
    }

    pub(super) fn fail(self, error: Arc<Error>) {
        let _ = self.reply.send(Err(CellDispatchError::Archive(error)));
    }

    fn expired(&self, now: u64) -> bool {
        self.cancelled.load(Ordering::Acquire) || self.reply.is_closed() || now >= self.deadline
    }
}

pub(super) fn prune(requests: &mut Vec<Request>, now: u64) {
    let mut live = Vec::new();
    for request in requests.drain(..) {
        if now >= request.deadline {
            let _ = request.reply.send(Err(CellDispatchError::Runtime(
                RuntimeError::DeadlineExceeded,
            )));
        } else if !request.expired(now) {
            live.push(request);
        }
    }
    *requests = live;
}

pub(super) fn oldest_admission(requests: &[Request]) -> Result<Admission> {
    let mut requests = requests.iter();
    let mut oldest = requests
        .next()
        .ok_or(Error::Invalid("empty archive proof batch"))?
        .admission()?;
    for request in requests {
        oldest = oldest.min(request.admission()?);
    }
    Ok(oldest)
}

pub(super) async fn prove(
    requests: Vec<Request>,
    context: &job::Context,
    registration: &CellGroupArchiveRegistration,
    coverage: &CellGroupArchiveCoverage,
    execution: &ControllerExecution,
    eligibility: Option<Eligibility>,
) -> Vec<Request> {
    let mut waiting = Vec::new();
    if let Err(error) = context.uses.track(coverage, eligibility) {
        let error = Arc::new(error);
        for request in requests {
            request.fail(error.clone());
        }
        return waiting;
    }
    for request in requests {
        let now = execution
            .run(async { crate::asyncrt::mono_ms() })
            .await
            .unwrap_or(u64::MAX);
        if request.expired(now) {
            continue;
        }
        if request.route != *coverage.marker().route() {
            request.fail(Arc::new(Error::Invalid("S3 output route")));
            continue;
        }
        match request
            .admission()
            .and_then(|admission| context.uses.allows(coverage, admission))
        {
            Ok(true) => (),
            Ok(false) => {
                waiting.push(request);
                continue;
            }
            Err(error) => {
                request.fail(Arc::new(error));
                continue;
            }
        }
        let store = registration.store.clone();
        let physical = coverage.clone();
        let read = execution.blocking(move || {
            let mut request = request;
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match &mut request.output {
                        Output::Command(receipt) => {
                            crate::raft::cell_groups::archive::receipt::command(
                                &physical,
                                &store,
                                receipt.receipt(),
                                request
                                    .limits
                                    .ok_or(CellGroupArchiveError::Invalid("S3 command limits"))?,
                                request.deadline,
                                request.cancelled.clone(),
                            )
                        }
                        Output::Enrollment(enrollment) => {
                            crate::raft::cell_groups::archive::enrollment::verify(
                                &physical,
                                &store,
                                enrollment.record(),
                            )
                        }
                        Output::Lookup(lookup) => {
                            let original = lookup
                                .selected
                                .receipt()
                                .map_or(0, |row| row.result().len());
                            crate::raft::cell_groups::archive::receipt::lookup(
                                &physical,
                                &store,
                                &mut lookup.selected,
                                crate::raft::cell_groups::archive::receipt::LookupRead {
                                    request: &lookup.request,
                                    limits: request.limits.ok_or(
                                        CellGroupArchiveError::Invalid("S3 lookup limits"),
                                    )?,
                                    deadline: request.deadline,
                                    cancelled: request.cancelled.clone(),
                                },
                                |length| {
                                    let bytes = length
                                        .max(original)
                                        .checked_mul(2)
                                        .and_then(|n| n.checked_add(8192))
                                        .ok_or(ExecutionError::Refused(
                                            cellar_logic::consensus::CommandFailure::Overloaded,
                                        ))?;
                                    let mut permit = request.permit.lock().map_err(|_| {
                                        ExecutionError::Invalid("S3 result admission")
                                    })?;
                                    if !permit.grow_at_least(bytes) {
                                        return Err(ExecutionError::Refused(
                                            cellar_logic::consensus::CommandFailure::Overloaded,
                                        ));
                                    }
                                    Ok(())
                                },
                            )
                        }
                    }
                }))
                .unwrap_or_else(|panic| {
                    Err(CellGroupArchiveError::Replay(
                        crate::raft::execution::storage(crate::native_worker::panic_error(panic)),
                    ))
                });
            (request, result)
        });
        // A refused submission drops the closure and its owned request. Entered
        // closures retain both until the controller-specific physical join.
        let Ok(read) = read else {
            continue;
        };
        let Ok((mut request, result)) = read.await else {
            continue;
        };
        let now = execution
            .run(async { crate::asyncrt::mono_ms() })
            .await
            .unwrap_or(u64::MAX);
        if request.expired(now) {
            continue;
        }
        match result {
            Ok(false) => {
                request.attempted_cut = Some(coverage.marker().position().index);
                waiting.push(request);
            }
            Err(CellGroupArchiveError::Busy) => waiting.push(request),
            Err(CellGroupArchiveError::Replay(error))
                if matches!(&request.output, Output::Lookup(_)) =>
            {
                let _ = request.reply.send(Err(CellDispatchError::Execution(error)));
            }
            Err(error) => request.fail(Arc::new(Error::Archive(error))),
            Ok(true) => {
                if let Err(error) = context.authority.require_current().await {
                    request.fail(Arc::new(error));
                    continue;
                }
                if let Err(error) = context.uses.prove_output(coverage) {
                    request.fail(Arc::new(error));
                    continue;
                }
                let Request {
                    output,
                    reply,
                    permit,
                    ..
                } = request;
                let _ = reply.send(Ok(Settled {
                    output,
                    proof: Box::new(OutputProof {
                        _coverage: coverage.clone(),
                        _journal: context.journal.clone(),
                        _permit: permit,
                        authority: context.authority.clone(),
                    }),
                }));
            }
        }
    }
    waiting
}
