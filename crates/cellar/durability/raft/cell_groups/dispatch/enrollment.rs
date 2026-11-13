// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Routed creation identity and controller-owned initialization, never a cell task.
#[path = "enrollment_remote.rs"]
mod remote;

use super::*;
use crate::raft::execution::CellEnrollmentAttempt;
use crate::raft::transport::CellRemoteEnrollmentResponse;
use crate::storage::CommandInputs;
use cellar_logic::consensus::{CommandFailure, Durability};
use cellar_ltx::generation::ImageId;
use tokio::sync::watch;

#[derive(Clone, Copy)]
pub struct CellEnrollmentRequest {
    pub creation: ProposalId,
    pub durability: Durability,
}

/// Committed creation metadata. A remote result retains its transport proof
/// and cannot stand in for a locally inspected SQL or archive result.
pub struct CellEnrollmentResponse(EnrollmentOutput);

enum EnrollmentOutput {
    Local(Box<CellSqlEnrollment>),
    Remote(Box<CellRemoteEnrollmentResponse>),
}

impl CellEnrollmentResponse {
    pub fn record(&self) -> &EnrollmentRecord {
        match &self.0 {
            EnrollmentOutput::Local(response) => response.record(),
            EnrollmentOutput::Remote(response) => response.record(),
        }
    }

    pub fn durability(&self) -> Durability {
        match &self.0 {
            EnrollmentOutput::Local(response) => response.durability(),
            EnrollmentOutput::Remote(response) => response.durability(),
        }
    }

    pub fn local_enrollment(&self) -> Option<&CellSqlEnrollment> {
        match &self.0 {
            EnrollmentOutput::Local(response) => Some(response),
            EnrollmentOutput::Remote(_) => None,
        }
    }

    pub(crate) fn into_local(self) -> Option<CellSqlEnrollment> {
        match self.0 {
            EnrollmentOutput::Local(response) => Some(*response),
            EnrollmentOutput::Remote(_) => None,
        }
    }
}

#[derive(Debug)]
pub struct CellEnrollmentError {
    pub creation: ProposalId,
    pub ambiguous: bool,
    pub cause: Box<CellDispatchError>,
}

impl fmt::Display for CellEnrollmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cell enrollment: {} (ambiguous={})",
            self.cause, self.ambiguous
        )
    }
}

impl std::error::Error for CellEnrollmentError {}

struct Origin {
    runtime: CellGroupRuntime,
    sql: CellGroupSqlPool,
    route: GroupRoute,
    prepared: Option<CellSqlPrepared>,
}

/// Captured once before discovery and transferred into the node controller.
/// The registered class initializer is shared; no per-request closure is retained.
pub struct CellEnrollmentInvocation {
    dispatcher: CellGroupDispatcher,
    cell: CellHistory,
    creation: ProposalId,
    durability: Durability,
    preview: ImageId,
    inputs: CommandInputs,
    session: CellRoutingSession,
    started: bool,
    route_remote: bool,
    attempt: Option<CellRouteAttempt>,
    dispatched: bool,
    remote_route: Option<super::RemoteMutationRoute>,
    ambiguous: bool,
    origin: Option<Origin>,
    _admission: Permit,
}

enum EnrollmentInput {
    Local,
    Originating,
    Received(CommandInputs, ImageId),
}

struct EnrollmentCapture {
    input: EnrollmentInput,
    started: u64,
    admission: Permit,
}

impl CellGroupDispatcher {
    pub fn enroll(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> impl std::future::Future<Output = Result<CellEnrollmentInvocation, CellEnrollmentError>>
           + Send
           + 'static {
        self.prepare_enrollment(cell, request, limits, cached, EnrollmentInput::Local)
    }

    pub(crate) fn enroll_routed(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
    ) -> impl std::future::Future<Output = Result<CellEnrollmentInvocation, CellEnrollmentError>>
           + Send
           + 'static {
        self.prepare_enrollment(cell, request, limits, None, EnrollmentInput::Originating)
    }

    pub(crate) fn enroll_with_inputs(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        captured: Option<(CommandInputs, ImageId)>,
    ) -> impl std::future::Future<Output = Result<CellEnrollmentInvocation, CellEnrollmentError>>
           + Send
           + 'static {
        let input = captured.map_or(EnrollmentInput::Local, |(inputs, preview)| {
            EnrollmentInput::Received(inputs, preview)
        });
        self.prepare_enrollment(cell, request, limits, cached, input)
    }

    fn prepare_enrollment(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        input: EnrollmentInput,
    ) -> impl std::future::Future<Output = Result<CellEnrollmentInvocation, CellEnrollmentError>>
           + Send
           + 'static {
        let prepared = self.capture_enrollment(cell, request, limits, cached, input);
        async move { prepared?.await }
    }

    pub(crate) fn capture_routed_enrollment(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
    ) -> Result<
        impl std::future::Future<Output = Result<CellEnrollmentInvocation, CellEnrollmentError>>
            + Send
            + 'static,
        CellEnrollmentError,
    > {
        self.capture_enrollment(cell, request, limits, None, EnrollmentInput::Originating)
    }

    fn capture_enrollment(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        input: EnrollmentInput,
    ) -> Result<
        impl std::future::Future<Output = Result<CellEnrollmentInvocation, CellEnrollmentError>>
            + Send
            + 'static,
        CellEnrollmentError,
    > {
        let failure = |cause| CellEnrollmentError {
            creation: request.creation,
            ambiguous: false,
            cause: Box::new(cause),
        };
        let started = self.domain.mono_ms().map_err(|_| failure(closed()))?;
        if limits.deadline_at(started).is_none() {
            return Err(failure(CellDispatchError::Invalid("enrollment deadline")));
        }
        self.class
            .require_initializer()
            .map_err(|error| failure(CellDispatchError::Registration(error)))?;
        let admission = self
            .budget
            .acquire(8192)
            .ok_or_else(|| failure(CellDispatchError::Runtime(RuntimeError::Overloaded)))?;
        let dispatcher = self.clone();
        let capture = EnrollmentCapture {
            input,
            started,
            admission,
        };
        Ok(async move {
            dispatcher
                .enroll_captured(cell, request, limits, cached, capture)
                .await
        })
    }

    async fn enroll_captured(
        &self,
        cell: CellHistory,
        mut request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        capture: EnrollmentCapture,
    ) -> Result<CellEnrollmentInvocation, CellEnrollmentError> {
        let EnrollmentCapture {
            input,
            started,
            admission,
        } = capture;
        let error = |cause| CellEnrollmentError {
            creation: request.creation,
            ambiguous: false,
            cause: Box::new(cause),
        };

        request.durability = request
            .durability
            .max(self.directory.namespace_config().minimum_durability);
        let route_remote = matches!(input, EnrollmentInput::Originating);
        if request.durability == Durability::S3 && self.s3.is_none() && !route_remote {
            return Err(error(CellDispatchError::Execution(
                ExecutionError::Refused(CommandFailure::UnsupportedCapability),
            )));
        }
        self.class
            .require_initializer()
            .map_err(|e| error(CellDispatchError::Registration(e)))?;
        let (inputs, preview) = if let EnrollmentInput::Received(inputs, preview) = input {
            (inputs, preview)
        } else {
            let (inputs, preview) = self
                .domain
                .run(async {
                    use rand::RngCore as _;
                    let mut rng = crate::asyncrt::rng("cell-group-enrollment-inputs");
                    let mut random = [0; 32];
                    let mut preview = [0; 16];
                    rng.fill_bytes(&mut random);
                    rng.fill_bytes(&mut preview);
                    (
                        CommandInputs {
                            now_unix_millis: crate::asyncrt::wall_ms().max(0) as u64,
                            random,
                        },
                        ImageId::new(preview),
                    )
                })
                .await
                .map_err(|_| error(closed()))?;
            let preview =
                preview.map_err(|_| error(CellDispatchError::Invalid("enrollment preview")))?;
            (inputs, preview)
        };
        let mut hash = Sha256::new();
        hash.update(b"cellar-cell-enrollment-invocation-1\0");
        hash.update(cell_image_history_digest(&cell));
        hash.update(request.creation.bytes());
        hash.update(self.class.schema_digest());
        hash.update(inputs.now_unix_millis.to_be_bytes());
        hash.update(inputs.random);
        hash.update(limits.timeout.as_nanos().to_be_bytes());
        hash.update([
            limits.max_attempts,
            match request.durability {
                Durability::Quorum => 1,
                Durability::S3 => 2,
            },
        ]);
        let session = self
            .directory
            .routing_session_in(
                self.domain.clone(),
                CellRouteRequest {
                    cell: cell.clone(),
                    operation: CellRouteOperation::Enrollment(request.creation),
                    fingerprint: hash.finalize().into(),
                },
                limits,
                cached,
                started,
            )
            .map_err(|e| error(CellDispatchError::Routing(e)))?
            .using_cache(self.routes.clone());
        Ok(CellEnrollmentInvocation {
            dispatcher: self.clone(),
            cell,
            creation: request.creation,
            durability: request.durability,
            preview,
            inputs,
            session,
            started: false,
            route_remote,
            attempt: None,
            dispatched: false,
            remote_route: None,
            ambiguous: false,
            origin: None,
            _admission: admission,
        })
    }
}

impl CellEnrollmentInvocation {
    fn error(&self, cause: CellDispatchError) -> CellEnrollmentError {
        CellEnrollmentError {
            creation: self.creation,
            ambiguous: self.ambiguous,
            cause: Box::new(cause),
        }
    }

    pub(crate) fn controller_admission(
        &self,
        domain: &DomainToken,
    ) -> Result<(ProposalId, u64), CellEnrollmentError> {
        if self.started || self.origin.is_some() || !self.dispatcher.domain.same_owner(domain) {
            return Err(self.error(CellDispatchError::Invalid(
                "controller requires fresh local enrollment",
            )));
        }
        let deadline = self.session.deadline_ms();
        if domain.mono_ms().map_err(|_| self.error(closed()))? >= deadline {
            return Err(self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)));
        }
        Ok((self.creation, deadline))
    }

    pub(crate) fn unresolved_group(&self) -> Option<cellar_logic::cell_group::GroupHistory> {
        self.origin.as_ref().map(|origin| origin.route.history)
    }

    pub(crate) fn unresolved(&self) -> bool {
        self.origin.is_some()
    }

    pub(crate) async fn run_owned(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        roster: &crate::raft::cell_groups::controller::GroupRoster,
        progress: &mut watch::Receiver<()>,
        routing: MutationRouting<'_>,
    ) -> Result<CellEnrollmentResponse, CellEnrollmentError> {
        let transport = routing.transport;
        self.started = true;
        let mut attempt = self.begin_owned().await.map_err(|e| self.error(e))?;
        if self.route_remote && self.remote_route.is_none() {
            let member = loop {
                let selected = Box::pin(super::mutation_route::mutation_peer(
                    runtime,
                    transport,
                    &self.session,
                    &attempt,
                    &self.dispatcher.keys,
                    &self.dispatcher.domain,
                ))
                .await;
                match selected {
                    Ok(member) => break member,
                    Err(cause)
                        if matches!(
                            cause.refusal(),
                            Some(CommandFailure::NotLeader { .. } | CommandFailure::Overloaded)
                        ) && usize::from(attempt.ordinal())
                            < self.session.maximum_attempts() =>
                    {
                        // No creation was sent. Retain the same placement, inputs,
                        // creation identity and original attempt/deadline bank.
                        routing
                            .backoff(
                                &self.dispatcher.domain,
                                self.session.deadline_ms(),
                                self.session.maximum_attempts() - usize::from(attempt.ordinal()),
                            )
                            .await
                            .map_err(|cause| self.error(cause))?;
                        attempt = self
                            .session
                            .retry(&attempt)
                            .await
                            .map_err(|error| self.error(CellDispatchError::Routing(error)))?;
                    }
                    Err(CellDispatchError::Runtime(
                        RuntimeError::NotReady | RuntimeError::NotFound,
                    )) if usize::from(attempt.ordinal()) < self.session.maximum_attempts() => {
                        // Discovery has not entered an initializer or sent a
                        // creation. Refresh only this route, preserving captured
                        // inputs, identity, deadline and the shared attempt bank.
                        attempt = self
                            .session
                            .refresh(&attempt)
                            .await
                            .map_err(|error| self.error(CellDispatchError::Routing(error)))?;
                    }
                    Err(error) => return Err(self.error(error)),
                }
            };
            if let Some(member) = member {
                self.attempt = Some(attempt);
                let response = self.execute_remote(transport, member).await?;
                return Ok(CellEnrollmentResponse(EnrollmentOutput::Remote(Box::new(
                    response,
                ))));
            }
        }
        self.run_at(runtime, sql, options, roster, progress, attempt)
            .await
            .map(|response| CellEnrollmentResponse(EnrollmentOutput::Local(Box::new(response))))
            .map_err(|e| self.error(e))
    }

    async fn begin_owned(&mut self) -> Result<CellRouteAttempt, CellDispatchError> {
        let attempt = if self.remote_route.is_some() {
            self.session.begin_current().await
        } else {
            self.session.begin().await
        }
        .map_err(CellDispatchError::Routing)?;
        if self
            .remote_route
            .as_ref()
            .is_some_and(|expected| !expected.matches(attempt.route()))
        {
            return Err(CellDispatchError::Invalid(
                "remote enrollment destination route",
            ));
        }
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        Ok(attempt)
    }

    async fn run_at(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        roster: &crate::raft::cell_groups::controller::GroupRoster,
        progress: &mut watch::Receiver<()>,
        attempt: CellRouteAttempt,
    ) -> Result<CellSqlEnrollment, CellDispatchError> {
        if !roster
            .contains(&attempt.route().history)
            .map_err(CellDispatchError::Runtime)?
        {
            return Err(CellDispatchError::Runtime(RuntimeError::NotFound));
        }
        match self
            .initialize_when_ready(runtime, sql, options, progress, &attempt)
            .await?
        {
            CellSqlOutcome::Enrollment(enrolled) => return self.committed(*enrolled).await,
            CellSqlOutcome::Prepared(prepared) => self.origin_mut()?.prepared = Some(prepared),
            _ => {
                return Err(CellDispatchError::Invalid(
                    "enrollment preparation response",
                ))
            }
        }
        loop {
            if self.dispatcher.domain.mono_ms().map_err(|_| closed())? >= self.session.deadline_ms()
            {
                let prepared = self.prepared()?;
                if let Some(proof) = prepared
                    .cancel_before_submission()
                    .map_err(CellDispatchError::Runtime)?
                {
                    self.reject(proof, progress).await?;
                    return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
                }
                break;
            }
            let prepared = self.prepared()?;
            if let Some(proof) = prepared.rejection().map_err(CellDispatchError::Runtime)? {
                self.reject(proof, progress).await?;
                return Err(CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::Overloaded,
                )));
            }
            let origin = self.origin_ref()?;
            let submitted = async {
                origin
                    .sql
                    .mark_submitted(self.cell.clone(), self.creation)
                    .await
                    .map_err(CellDispatchError::Execution)?;
                prepared
                    .submit(&origin.runtime)
                    .await
                    .map_err(CellDispatchError::Runtime)
            }
            .await;
            match submitted {
                Ok(CellSqlSubmission::Accepted(_)) => break,
                Ok(CellSqlSubmission::Rejected(proof)) => {
                    self.reject(proof, progress).await?;
                    return Err(CellDispatchError::Execution(ExecutionError::Refused(
                        CommandFailure::Overloaded,
                    )));
                }
                Err(CellDispatchError::Runtime(
                    RuntimeError::NotLeader | RuntimeError::StaleCompletion,
                )) => break,
                Err(error) if transient(&error) => {
                    progress.changed().await.map_err(|_| closed())?
                }
                Err(error) => {
                    if let Some(proof) = prepared
                        .cancel_before_submission()
                        .map_err(CellDispatchError::Runtime)?
                    {
                        self.reject(proof, progress).await?;
                    }
                    return Err(error);
                }
            }
        }
        loop {
            let origin = self.origin_ref()?;
            let resolved = async {
                let state = origin
                    .runtime
                    .enrollment_state(origin.route.history, self.cell.clone())
                    .await
                    .map_err(CellDispatchError::Runtime)?;
                if state.route() != &origin.route {
                    return Err(CellDispatchError::Invalid("enrollment resolution route"));
                }
                origin
                    .sql
                    .resolve_enrollment(self.creation, state)
                    .await
                    .map_err(CellDispatchError::Execution)
            }
            .await;
            match resolved {
                Ok(CellSqlEnrollmentResolution::Committed(enrolled)) => {
                    return self.committed(*enrolled).await
                }
                Ok(result) => {
                    self.origin = None;
                    return Err(CellDispatchError::Execution(ExecutionError::Refused(
                        match result {
                            CellSqlEnrollmentResolution::Conflict => {
                                CommandFailure::CommandConflict
                            }
                            _ => CommandFailure::Unavailable,
                        },
                    )));
                }
                Err(error)
                    if transient(&error)
                        || matches!(
                            error,
                            CellDispatchError::Execution(ExecutionError::Uncertain)
                        ) => {}
                Err(error) => return Err(error),
            }
            progress.changed().await.map_err(|_| closed())?;
        }
    }

    /// Only witnessed pre-enqueue refusals may wait for a logical writer or
    /// fresh applied authority. Entered initializers and lost replies retain
    /// their existing producer ownership and reconciliation; they never retry.
    async fn initialize_when_ready(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        progress: &mut watch::Receiver<()>,
        attempt: &CellRouteAttempt,
    ) -> Result<CellSqlOutcome, CellDispatchError> {
        let mut observed_cut = None;
        let mut noted = false;
        loop {
            self.session
                .validate_attempt(attempt)
                .map_err(CellDispatchError::Routing)?;
            progress.borrow_and_update();
            let mut before_enqueue = false;
            let staged = self
                .initialize_at(runtime, sql, options.clone(), attempt, &mut before_enqueue)
                .await;
            match staged {
                Err(CellDispatchError::Execution(
                    ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded),
                )) if before_enqueue && self.origin.is_none() => {
                    if !noted {
                        tracing::debug!(target: "cellar::raft::enrollment", phase = "writer-wait", creation = ?self.creation);
                        noted = true;
                    }
                }
                Err(error)
                    if self.origin.is_none()
                        && (matches!(
                            error,
                            CellDispatchError::Runtime(RuntimeError::NotReady)
                        ) || (before_enqueue
                            && matches!(
                                error,
                                CellDispatchError::Execution(ExecutionError::Refused(
                                    CommandFailure::Unavailable
                                ))
                            ))) =>
                {
                    let status = runtime
                        .status(attempt.route().history)
                        .await
                        .map_err(CellDispatchError::Runtime)?;
                    let cut = (
                        status.term.get(),
                        status.applied.index.get(),
                        status.committed.get(),
                    );
                    if status.applied.index >= status.committed {
                        // A completed apply may race authority selection. Retry
                        // each newly observed ready cut once; stable fences refuse.
                        if status.ready && observed_cut != Some(cut) {
                            observed_cut = Some(cut);
                            continue;
                        }
                        return Err(error);
                    }
                    observed_cut = Some(cut);
                }
                result => return result,
            }
            let timer = self
                .dispatcher
                .domain
                .sleep_until(self.session.deadline_ms())
                .map_err(|_| closed())?;
            crate::asyncrt::select! {
                result = progress.changed() => result.map_err(|_| closed())?,
                _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
            }
        }
    }

    async fn initialize_at(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        attempt: &CellRouteAttempt,
        before_enqueue: &mut bool,
    ) -> Result<CellSqlOutcome, CellDispatchError> {
        let authority = runtime
            .enrollment_authority(attempt.route().history, self.cell.clone())
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.state().route() != attempt.route() {
            return Err(CellDispatchError::Invalid("enrollment destination route"));
        }
        if self.durability == Durability::S3 {
            let owner = self
                .dispatcher
                .s3
                .as_ref()
                .ok_or(CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::UnsupportedCapability,
                )))?;
            owner.require_runtime(runtime)?;
            owner.require_group(attempt.route())?;
        }
        self.session
            .mark_submitted(attempt)
            .map_err(CellDispatchError::Routing)?;
        self.ambiguous = true;
        self.origin = Some(Origin {
            runtime: runtime.clone(),
            sql: sql.clone(),
            route: attempt.route().clone(),
            prepared: None,
        });
        let class = self.dispatcher.class.clone();
        let cell = self.cell.clone();
        sql.initialize_owned(
            options,
            authority,
            CellEnrollmentAttempt {
                creation: self.creation,
                preview: self.preview,
                inputs: self.inputs,
            },
            move |tx| class.invoke_initializer(&cell, tx),
        )
        .await
        .map_err(|failure| {
            *before_enqueue = failure.before_enqueue;
            if !failure.unresolved {
                self.origin = None;
            }
            CellDispatchError::Execution(failure.error)
        })
    }

    fn origin_ref(&self) -> Result<&Origin, CellDispatchError> {
        self.origin
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing enrollment origin"))
    }

    fn origin_mut(&mut self) -> Result<&mut Origin, CellDispatchError> {
        self.origin
            .as_mut()
            .ok_or(CellDispatchError::Invalid("missing enrollment origin"))
    }

    fn prepared(&self) -> Result<CellSqlPrepared, CellDispatchError> {
        self.origin_ref()?
            .prepared
            .clone()
            .ok_or(CellDispatchError::Invalid("missing enrollment preparation"))
    }

    async fn committed(
        &mut self,
        enrolled: CellSqlEnrollment,
    ) -> Result<CellSqlEnrollment, CellDispatchError> {
        let record = enrolled.record();
        if record.cell() != &self.cell
            || record.creation() != self.creation
            || record.schema() != self.dispatcher.class.schema_digest()
        {
            return Err(CellDispatchError::Invalid("enrollment receipt binding"));
        }
        // SQL is resolved. Archive failures and deadlines must not quarantine
        // this group or keep the initializer's producer/preparation admitted.
        let mut origin = self
            .origin
            .take()
            .ok_or(CellDispatchError::Invalid("resolved enrollment origin"))?;
        if self.durability == Durability::Quorum {
            return Ok(enrolled);
        }
        origin.prepared = None;
        let owner = self
            .dispatcher
            .s3
            .as_ref()
            .ok_or(CellDispatchError::Invalid("S3 enrollment owner"))?;
        let settled = owner
            .enrollment(
                &origin.runtime,
                origin.route.clone(),
                Box::new(enrolled),
                self.session.deadline_ms(),
            )
            .await?;
        let authority = origin
            .runtime
            .enrollment_authority(origin.route.history, self.cell.clone())
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.state().route() != &origin.route
            || authority.state().record() != Some(settled.enrollment.record())
        {
            return Err(CellDispatchError::Invalid(
                "S3 enrollment current creation/route",
            ));
        }
        settled
            .proof
            .require_current(&self.dispatcher.domain, self.session.deadline_ms())
            .await?;
        if self.dispatcher.domain.mono_ms().map_err(|_| closed())? >= self.session.deadline_ms() {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        authority.check().map_err(CellDispatchError::Runtime)?;
        Ok((*settled.enrollment).with_archive_proof(settled.proof))
    }

    async fn reject(
        &mut self,
        proof: CellSqlRejection,
        progress: &mut watch::Receiver<()>,
    ) -> Result<(), CellDispatchError> {
        loop {
            match self
                .origin_ref()?
                .sql
                .reject(self.cell.clone(), proof.clone())
                .await
            {
                Ok(()) | Err(ExecutionError::Uncertain) => {
                    self.origin = None;
                    return Ok(());
                }
                Err(ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded)) => {
                    progress.changed().await.map_err(|_| closed())?;
                }
                Err(error) => return Err(CellDispatchError::Execution(error)),
            }
        }
    }
}

fn transient(error: &CellDispatchError) -> bool {
    matches!(
        error,
        CellDispatchError::Runtime(
            RuntimeError::NotReady
                | RuntimeError::Overloaded
                | RuntimeError::NotLeader
                | RuntimeError::StaleCompletion
        ) | CellDispatchError::Execution(
            ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded)
        )
    )
}
