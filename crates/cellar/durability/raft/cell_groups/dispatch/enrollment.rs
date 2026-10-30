// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Routed creation identity and controller-owned initialization, never a cell task.
#[path = "enrollment_remote.rs"]
mod remote;

use super::*;
use crate::raft::execution::CellEnrollmentAttempt;
use crate::storage::CommandInputs;
use cellar_logic::consensus::{CommandFailure, Durability};
use cellar_ltx::generation::ImageId;
use std::collections::BTreeSet;
use tokio::sync::watch;

#[derive(Clone, Copy)]
pub struct CellEnrollmentRequest {
    pub creation: ProposalId,
    pub durability: Durability,
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
    preview: ImageId,
    inputs: CommandInputs,
    session: CellRoutingSession,
    started: bool,
    attempt: Option<CellRouteAttempt>,
    dispatched: bool,
    remote_route: Option<super::RemoteMutationRoute>,
    ambiguous: bool,
    origin: Option<Origin>,
    _admission: Permit,
}

impl CellGroupDispatcher {
    pub async fn enroll(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> Result<CellEnrollmentInvocation, CellEnrollmentError> {
        self.enroll_with_inputs(cell, request, limits, cached, None)
            .await
    }

    pub(crate) async fn enroll_with_inputs(
        &self,
        cell: CellHistory,
        request: CellEnrollmentRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        captured: Option<(CommandInputs, ImageId)>,
    ) -> Result<CellEnrollmentInvocation, CellEnrollmentError> {
        let error = |cause| CellEnrollmentError {
            creation: request.creation,
            ambiguous: false,
            cause: Box::new(cause),
        };
        let started = self.domain.mono_ms().map_err(|_| error(closed()))?;
        if request.durability != Durability::Quorum
            || self.directory.namespace_config().minimum_durability != Durability::Quorum
        {
            return Err(error(CellDispatchError::Execution(
                ExecutionError::Refused(CommandFailure::UnsupportedCapability),
            )));
        }
        self.class
            .require_initializer()
            .map_err(|e| error(CellDispatchError::Registration(e)))?;
        let admission = self
            .budget
            .acquire(8192)
            .ok_or_else(|| error(CellDispatchError::Runtime(RuntimeError::Overloaded)))?;
        let (inputs, preview) = if let Some(captured) = captured {
            captured
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
        hash.update([limits.max_attempts, 1]);
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
            preview,
            inputs,
            session,
            started: false,
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

    pub(crate) fn unresolved(&self) -> bool {
        self.origin.is_some()
    }

    pub(crate) async fn run_owned(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        roster: &BTreeSet<cellar_logic::cell_group::GroupHistory>,
        progress: &mut watch::Receiver<()>,
    ) -> Result<CellSqlEnrollment, CellEnrollmentError> {
        self.started = true;
        self.run_at(runtime, sql, options, roster, progress)
            .await
            .map_err(|e| self.error(e))
    }

    async fn run_at(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        roster: &BTreeSet<cellar_logic::cell_group::GroupHistory>,
        progress: &mut watch::Receiver<()>,
    ) -> Result<CellSqlEnrollment, CellDispatchError> {
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
        if !roster.contains(&attempt.route().history) {
            return Err(CellDispatchError::Runtime(RuntimeError::NotFound));
        }
        let authority = runtime
            .enrollment_authority(attempt.route().history, self.cell.clone())
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.state().route() != attempt.route() {
            return Err(CellDispatchError::Invalid("enrollment destination route"));
        }
        self.session
            .mark_submitted(&attempt)
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
        let staged = sql
            .initialize_owned(
                options,
                authority,
                CellEnrollmentAttempt {
                    creation: self.creation,
                    preview: self.preview,
                    inputs: self.inputs,
                },
                move |tx| class.invoke_initializer(&cell, tx),
            )
            .await;
        match staged {
            Ok(CellSqlOutcome::Enrollment(enrolled)) => return self.committed(*enrolled),
            Ok(CellSqlOutcome::Prepared(prepared)) => self.origin_mut()?.prepared = Some(prepared),
            Ok(_) => {
                return Err(CellDispatchError::Invalid(
                    "enrollment preparation response",
                ))
            }
            Err(failure) => {
                if !failure.unresolved {
                    self.origin = None;
                }
                return Err(CellDispatchError::Execution(failure.error));
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
                    return self.committed(*enrolled)
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

    fn committed(
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
        self.origin = None;
        Ok(enrolled)
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
