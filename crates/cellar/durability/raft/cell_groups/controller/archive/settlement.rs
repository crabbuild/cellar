// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operation proofs share the archive lane; waiting never owns a SQL producer.
use super::reuse::{Admission, Eligibility};
use super::*;
use crate::raft::budget::Permit;
use crate::raft::cell_groups::{
    CellDispatchError, CellSqlEnrollment, CellSqlQueryResult, CellSqlReceipt, CellSqlReceiptLookup,
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
    tails: CellGroupArchiveTailPool,
    checkpoints: CellGroupCheckpointPool,
}

impl OutputProof {
    pub(crate) async fn require_current(
        &self,
        domain: &crate::asyncrt::DomainToken,
        deadline: u64,
    ) -> std::result::Result<(), CellDispatchError> {
        require_current(&self.authority, domain, deadline).await
    }
}

pub(super) async fn require_current(
    authority: &Arc<job::Authority>,
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
        result = domain.run(authority.require_current()) => result.map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
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

pub(crate) struct CommandSettled {
    pub receipt: Box<CellSqlReceipt>,
    pub proof: Box<OutputProof>,
}

impl CommandSettled {
    /// Transfer only verified source custody; the result and S3 request permit
    /// are released. Physical verification stays charged to its existing bank.
    pub(crate) fn into_read_source(self) -> Arc<super::source_read::SourceReadProof> {
        let version = self.receipt.receipt().version.clone();
        let OutputProof {
            _coverage,
            _journal,
            _permit,
            authority,
            tails,
            checkpoints,
        } = *self.proof;
        drop(self.receipt);
        drop(_permit);
        super::source_read::SourceReadProof::new(
            version,
            _coverage,
            _journal,
            authority,
            &tails,
            &checkpoints,
        )
    }
}

pub(crate) struct EnrollmentSettled {
    pub enrollment: Box<CellSqlEnrollment>,
    pub proof: Box<OutputProof>,
}

pub(crate) struct LookupSettled {
    pub selected: Box<CellSqlReceiptLookup>,
    pub proof: Box<OutputProof>,
}

pub(crate) struct QuerySettled {
    pub selected: Box<CellSqlQueryResult>,
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
    Query(Box<CellSqlQueryResult>),
}

impl Output {
    fn retained_bytes(&self) -> std::result::Result<usize, CellDispatchError> {
        match self {
            Self::Query(selected) => selected.result().len(),
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
            // SQL and its selected-image pin have joined. Keep the immutable
            // result charged to the independently acquired archive reservation,
            // so an S3 wait cannot consume discovery's SQL operation slot.
            Self::Query(selected) => Self::Query(selected.retain_for_archive(permit)),
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
    /// Coalesce background cuts while one admitted native S3 callback creates
    /// its dependent checkpoints. This hint owns no proof, timer or I/O work.
    pub(crate) fn invocation(
        &self,
        runtime: &CellGroupRuntime,
        route: &GroupRoute,
        deadline: u64,
        entered: cellar_logic::consensus::LogIndex,
    ) -> std::result::Result<super::foreground::Invocation, CellDispatchError> {
        self.require_runtime(runtime)?;
        self.require_group(route)?;
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        let now = shared
            .domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        shared
            .foregrounds
            .admit(route.history, deadline, now, entered)
            .map_err(CellDispatchError::Runtime)
    }

    pub(crate) async fn query(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        selected: Box<CellSqlQueryResult>,
        deadline: u64,
    ) -> std::result::Result<QuerySettled, CellDispatchError> {
        let settled = self
            .settle(runtime, route, Output::Query(selected), None, deadline)
            .await?;
        let Output::Query(selected) = settled.output else {
            return Err(CellDispatchError::Invalid("S3 query output"));
        };
        Ok(QuerySettled {
            selected,
            proof: settled.proof,
        })
    }

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
        let settled = runtime
            .log()
            .performance()
            .measure(
                crate::raft::performance::Operation::CellCommandS3Coverage,
                self.settle(
                    runtime,
                    route,
                    Output::Command(receipt),
                    Some(limits),
                    deadline,
                ),
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

    /// A committed intermediate checkpoint belongs to an admitted invocation.
    /// Retain its original SQL bytes while awaiting the existing S3 bank; a
    /// temporary refusal must not abort/replay its native continuation. Ordinary
    /// public output preparation keeps its fail-fast admission contract.
    pub(crate) async fn checkpoint(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        receipt: Box<CellSqlReceipt>,
        limits: ExecutionLimits,
        deadline: u64,
    ) -> std::result::Result<CommandSettled, CellDispatchError> {
        self.require_runtime(runtime)?;
        self.require_group(&route)?;
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        let output = Output::Command(receipt);
        let bytes = output.retained_bytes()?;
        let (max_bytes, max_items) = shared.s3.capacity();
        if bytes > max_bytes || max_items == 0 {
            return Err(CellDispatchError::Runtime(RuntimeError::Overloaded));
        }
        let timer = shared
            .domain
            .sleep_until(deadline)
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        tokio::pin!(timer);
        let permit = loop {
            let changed = shared.s3.wake.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
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
            if let Some(permit) = shared.s3.acquire(bytes) {
                break permit;
            }
            crate::asyncrt::select_biased! { "original checkpoint S3 capacity deadline";
                _ = &mut timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
                _ = changed => (),
            }
        };
        self.require_group(&route)?;
        let settled = self
            .prepare_admitted(route, output, Some(limits), deadline, permit)?
            .response_checkpoint()
            .await?;
        let Output::Command(receipt) = settled.output else {
            return Err(CellDispatchError::Invalid("S3 checkpoint output"));
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
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        self.prepare_admitted(route, output, limits, deadline, permit)
    }

    fn prepare_admitted(
        &self,
        route: GroupRoute,
        output: Output,
        limits: Option<ExecutionLimits>,
        deadline: u64,
        permit: Permit,
    ) -> std::result::Result<Prepared, CellDispatchError> {
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        let permit = Arc::new(Mutex::new(permit));
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
        self.respond(false).await
    }

    async fn response_checkpoint(self) -> std::result::Result<Settled, CellDispatchError> {
        self.respond(true).await
    }

    async fn respond(self, checkpoint: bool) -> std::result::Result<Settled, CellDispatchError> {
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
        let timer = shared
            .domain
            .sleep_until(deadline)
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        tokio::pin!(timer);
        if checkpoint {
            // Each waiter already owns one S3 permit and its original invocation
            // admission. Queue backpressure allocates no additional request and
            // consumes neither a new deadline nor the worker's stop notification.
            let queued = shared.requests.reserve();
            tokio::pin!(queued);
            let slot = loop {
                let changed = shared.s3.wake.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if shared.closing.load(Ordering::Acquire) {
                    return Err(CellDispatchError::Runtime(RuntimeError::Closed));
                }
                crate::asyncrt::select_biased! { "original checkpoint S3 enqueue deadline";
                    _ = &mut timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
                    result = &mut queued => break result.map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
                    _ = changed => (),
                }
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
            slot.send(requests::Request::S3(Box::new(request)));
        } else {
            shared
                .requests
                .try_send(requests::Request::S3(Box::new(request)))
                .map_err(|e| {
                    CellDispatchError::Runtime(match e {
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
                        tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                    })
                })?;
        }
        let result = crate::asyncrt::select! {
            result = receiver => result.map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
            _ = &mut timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
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

    fn minimum_index(&self) -> Option<cellar_logic::consensus::LogIndex> {
        match &self.output {
            Output::Command(receipt) => Some(receipt.receipt().position.index),
            Output::Query(selected) => {
                let witness = &selected.observation().witness;
                (witness.group == self.route.history).then_some(witness.position.index)
            }
            Output::Lookup(lookup) => {
                let witness = &lookup.selected.observation().witness;
                (witness.group == self.route.history).then_some(witness.position.index)
            }
            Output::Enrollment(_) => None,
        }
    }

    pub(super) fn may_reuse(&self, index: cellar_logic::consensus::LogIndex) -> bool {
        self.attempted_cut.is_none_or(|previous| previous < index)
            && self.minimum_index().is_none_or(|minimum| minimum <= index)
    }

    pub(super) fn fail(self, error: Arc<Error>) {
        let _ = self.reply.send(Err(CellDispatchError::Archive(error)));
    }

    fn expired(&self, now: u64) -> bool {
        self.cancelled.load(Ordering::Acquire) || self.reply.is_closed() || now >= self.deadline
    }
}

/// Only a negative bound: a prefix below every waiting command or selected
/// query or receipt image cannot prove any of them. This does not settle an
/// output: the physical verifier still checks its exact version, witness and history.
/// A foreign-group witness and enrollment keep the ordinary full-verification
/// path. A prefix already rejected by a waiter cannot qualify as its next cut.
pub(super) fn required_prefix(requests: &[Request]) -> Option<cellar_logic::consensus::LogIndex> {
    let mut minimum = None;
    for request in requests {
        let mut required = request.minimum_index()?;
        if let Some(attempted) = request.attempted_cut {
            required = required.max(cellar_logic::consensus::LogIndex::new(
                attempted.get().checked_add(1)?,
            ));
        }
        minimum = Some(
            minimum.map_or(required, |previous: cellar_logic::consensus::LogIndex| {
                previous.min(required)
            }),
        );
    }
    minimum
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
        if !request.may_reuse(coverage.marker().position().index) {
            waiting.push(request);
            continue;
        }
        let store = registration.store.clone();
        let physical = coverage.clone();
        let read = execution.blocking(move || {
            let mut request = request;
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match &mut request.output {
                        Output::Query(selected) => {
                            crate::raft::cell_groups::archive::query::verify(
                                &physical,
                                &store,
                                selected.observation(),
                            )
                        }
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
                        tails: registration.tails.clone(),
                        checkpoints: registration.checkpoints.clone(),
                    }),
                }));
            }
        }
    }
    waiting
}
