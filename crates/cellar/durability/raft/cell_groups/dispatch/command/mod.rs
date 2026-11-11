// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Routed commands retain their originating producer until exact resolution.
//! The node controller drives publication; acceptance is only provisional.
mod output;
mod owned;
mod remote;
use output::PendingOutput;

use super::*;
use crate::application::consensus::command_request_digest;
use crate::raft::{
    cell_groups::CellSqlStageMode,
    execution::{CommandAttempt, CommandRequest},
};
use crate::storage::CommandInputs;
use cellar_logic::consensus::{CommandFailure, CommandId, Durability};
use cellar_ltx::generation::ImageId;

#[derive(Clone)]
pub struct CellCommandRequest {
    pub id: CommandId,
    pub contract: OperationContract,
    pub input: Vec<u8>,
    pub durability: Durability,
    pub retry_until_ms: u64,
}

#[derive(Debug)]
pub struct CellCommandError {
    pub command: CommandId,
    /// Conservatively sticky after SQL entry. Definitive resolution is returned
    /// separately; an error is never a non-commit proof. Retry the same identity.
    pub ambiguous: bool,
    pub cause: Box<CellDispatchError>,
}

impl fmt::Display for CellCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cell command {:?} (ambiguous={}): {}",
            self.command, self.ambiguous, self.cause
        )
    }
}

impl std::error::Error for CellCommandError {}

pub enum CellCommandPreparation {
    Prepared,
    Committed(CellCommandResponse),
}

pub enum CellCommandSubmission {
    Accepted(CellGroupProposal),
    /// An exact shared preparation was irreversibly sealed before append.
    Rejected,
}

pub enum CellCommandSettlement {
    Committed(CellCommandResponse),
    NotCommitted,
    Refused(CommandFailure),
}

pub struct CellCommandResponse {
    output: CommandOutput,
}

enum CommandOutput {
    Local {
        receipt: Box<CellSqlReceipt>,
        token: CellVersionToken,
        s3: Option<Box<super::super::controller::S3OutputProof>>,
    },
    Remote(Box<crate::raft::transport::CellRemoteCommandResponse>),
}

impl CellCommandResponse {
    /// A remote acknowledgement carries authenticated output metadata, never a
    /// locally inspected SQL image or a local archive-settlement proof.
    pub fn local_receipt(&self) -> Option<&crate::raft::execution::CellCommittedReceipt> {
        match &self.output {
            CommandOutput::Local { receipt, .. } => Some(receipt.receipt()),
            CommandOutput::Remote(_) => None,
        }
    }

    pub fn result(&self) -> &[u8] {
        match &self.output {
            CommandOutput::Local { receipt, .. } => receipt.receipt().receipt.result(),
            CommandOutput::Remote(response) => response.result(),
        }
    }

    pub fn observation(&self) -> crate::raft::version::CellVersionObservation {
        match &self.output {
            CommandOutput::Local { receipt, .. } => receipt.receipt().observation(),
            CommandOutput::Remote(response) => response.observation().clone(),
        }
    }

    pub fn token(&self) -> &CellVersionToken {
        match &self.output {
            CommandOutput::Local { token, .. } => token,
            CommandOutput::Remote(response) => response.token(),
        }
    }

    pub fn durability(&self) -> Durability {
        match &self.output {
            CommandOutput::Local { s3: Some(_), .. } => Durability::S3,
            CommandOutput::Local { s3: None, .. } => Durability::Quorum,
            CommandOutput::Remote(response) => response.durability(),
        }
    }
}

struct Origin {
    runtime: CellGroupRuntime,
    sql: CellGroupSqlPool,
    options: CellSqlOptions,
    route: GroupRoute,
    proposal: ProposalId,
    preview: ImageId,
    prepared: Option<CellSqlPrepared>,
    submitted: bool,
}

/// The controller retains this non-cloneable invocation across waiter cancellation.
/// Dropping it never releases the SQL worker's unresolved physical producer.
pub struct CellCommandInvocation {
    dispatcher: CellGroupDispatcher,
    system: bool,
    cell: CellHistory,
    request: CellCommandRequest,
    inputs: CommandInputs,
    session: CellRoutingSession,
    attempt: Option<CellRouteAttempt>,
    dispatched: bool,
    ambiguous: bool,
    origin: Option<Origin>,
    output: Option<PendingOutput>,
    remote_route: Option<super::RemoteMutationRoute>,
    _admission: Permit,
}

/// Synchronous request capture for the node queue. No SQL or routing I/O has
/// begun; dropping this preparation releases only its invocation reservation.
pub(crate) struct PendingCellCommand {
    dispatcher: CellGroupDispatcher,
    cell: CellHistory,
    request: CellCommandRequest,
    limits: CellRoutingLimits,
    cached: Option<ResolvedCellRoute>,
    captured: Option<CommandInputs>,
    system: bool,
    started: u64,
    deadline: u64,
    admission: Permit,
}

impl CellGroupDispatcher {
    /// Capture invocation admission and the original deadline immediately.
    /// Polling constructs routing state; it never restarts the timeout.
    pub fn command(
        &self,
        cell: CellHistory,
        request: CellCommandRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> impl std::future::Future<Output = Result<CellCommandInvocation, CellCommandError>>
           + Send
           + 'static {
        self.command_with_inputs(cell, request, limits, cached, None)
    }

    pub(crate) fn command_with_inputs(
        &self,
        cell: CellHistory,
        request: CellCommandRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        captured: Option<CommandInputs>,
    ) -> impl std::future::Future<Output = Result<CellCommandInvocation, CellCommandError>>
           + Send
           + 'static {
        let pending = self.prepare_command(cell, request, limits, cached, captured, false);
        async move { pending?.finish().await }
    }

    pub(crate) async fn system_command(
        &self,
        cell: CellHistory,
        request: CellCommandRequest,
        limits: CellRoutingLimits,
    ) -> Result<CellCommandInvocation, CellCommandError> {
        self.prepare_command(cell, request, limits, None, None, true)?
            .finish()
            .await
    }

    pub(crate) fn prepare_command(
        &self,
        cell: CellHistory,
        mut request: CellCommandRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        captured: Option<CommandInputs>,
        system: bool,
    ) -> Result<PendingCellCommand, CellCommandError> {
        let error = |cause| CellCommandError {
            command: request.id,
            ambiguous: false,
            cause: Box::new(cause),
        };
        let started = self.domain.mono_ms().map_err(|_| error(closed()))?;
        let deadline = limits.deadline_at(started).ok_or_else(|| {
            error(CellDispatchError::Routing(CellRoutingError {
                reason: CellRoutingFailure::InvalidRequest,
                ambiguous_command: None,
            }))
        })?;
        request.durability = request
            .durability
            .max(self.directory.namespace_config().minimum_durability);
        if request.durability == Durability::S3 && self.s3.is_none() {
            return Err(error(CellDispatchError::Execution(
                ExecutionError::Refused(CommandFailure::UnsupportedCapability),
            )));
        }
        if request.contract.operation.len() > 128
            || request.input.len() > self.limits.max_input_bytes
        {
            return Err(error(CellDispatchError::Runtime(RuntimeError::Overloaded)));
        }
        if request.contract.kind != OperationKind::Command {
            return Err(error(CellDispatchError::Registration(
                RegistrationError::KindMismatch,
            )));
        }
        if system {
            self.class.validate_system_operation(&request.contract)
        } else {
            self.class.validate_operation(&request.contract)
        }
        .map_err(|e| error(CellDispatchError::Registration(e)))?;
        if system
            && request.id.client
                != crate::raft::effects::system_client(&cell).map_err(|failure| {
                    error(CellDispatchError::Execution(ExecutionError::Refused(
                        failure,
                    )))
                })?
        {
            return Err(error(CellDispatchError::Execution(
                ExecutionError::Refused(CommandFailure::UnsupportedCapability),
            )));
        }
        let size = request
            .input
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| error(CellDispatchError::Runtime(RuntimeError::Overloaded)))?;
        let admission = self
            .budget
            .acquire(size)
            .ok_or_else(|| error(CellDispatchError::Runtime(RuntimeError::Overloaded)))?;
        request.input = request.input.into_boxed_slice().into_vec();
        request.contract.operation = request.contract.operation.into_boxed_str().into_string();
        Ok(PendingCellCommand {
            dispatcher: self.clone(),
            cell,
            request,
            limits,
            cached,
            captured,
            system,
            started,
            deadline,
            admission,
        })
    }
}

impl PendingCellCommand {
    pub(crate) fn controller_admission(
        &self,
        domain: &DomainToken,
    ) -> Result<(CommandId, u64, usize), CellCommandError> {
        let error = |cause| CellCommandError {
            command: self.request.id,
            ambiguous: false,
            cause: Box::new(cause),
        };
        if !self.dispatcher.uses_domain(domain) {
            return Err(error(CellDispatchError::Invalid(
                "command dispatcher domain",
            )));
        }
        if domain.mono_ms().map_err(|_| error(closed()))? >= self.deadline {
            return Err(error(CellDispatchError::Runtime(
                RuntimeError::DeadlineExceeded,
            )));
        }
        let bytes = self
            .request
            .input
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| error(CellDispatchError::Runtime(RuntimeError::Overloaded)))?;
        Ok((self.request.id, self.deadline, bytes))
    }

    pub(crate) async fn finish(self) -> Result<CellCommandInvocation, CellCommandError> {
        let Self {
            dispatcher,
            cell,
            request,
            limits,
            cached,
            captured,
            system,
            started,
            deadline: _,
            admission,
        } = self;
        let error = |cause| CellCommandError {
            command: request.id,
            ambiguous: false,
            cause: Box::new(cause),
        };
        let inputs = if let Some(inputs) = captured {
            inputs
        } else {
            dispatcher
                .domain
                .run(async {
                    use rand::RngCore as _;
                    let mut random = [0; 32];
                    crate::asyncrt::rng("cell-group-command-inputs").fill_bytes(&mut random);
                    CommandInputs {
                        now_unix_millis: crate::asyncrt::wall_ms().max(0) as u64,
                        random,
                    }
                })
                .await
                .map_err(|_| error(closed()))?
        };
        let session = dispatcher
            .directory
            .routing_session_in(
                dispatcher.domain.clone(),
                CellRouteRequest {
                    cell: cell.clone(),
                    operation: CellRouteOperation::Command(request.id),
                    fingerprint: fingerprint(&cell, &request, inputs, limits),
                },
                limits,
                cached,
                started,
            )
            .map_err(|e| error(CellDispatchError::Routing(e)))?
            .using_cache(dispatcher.routes.clone());
        Ok(CellCommandInvocation {
            dispatcher,
            system,
            cell,
            request,
            inputs,
            session,
            attempt: None,
            dispatched: false,
            ambiguous: false,
            origin: None,
            output: None,
            remote_route: None,
            _admission: admission,
        })
    }
}

impl CellCommandInvocation {
    fn error(&self, cause: CellDispatchError) -> CellCommandError {
        CellCommandError {
            command: self.request.id,
            ambiguous: self.ambiguous,
            cause: Box::new(cause),
        }
    }

    fn checked<T>(
        &self,
        result: Result<T, CellDispatchError>,
        deadline: u64,
    ) -> Result<T, CellCommandError> {
        let output = result.map_err(|e| self.error(e))?;
        if self
            .dispatcher
            .domain
            .mono_ms()
            .map_err(|_| self.error(closed()))?
            >= deadline
        {
            return Err(self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)));
        }
        Ok(output)
    }

    pub fn attempt(&self) -> Option<&CellRouteAttempt> {
        self.attempt.as_ref()
    }

    pub async fn begin(&mut self) -> Result<(), CellCommandError> {
        self.dispatched = true;
        let result = if self.remote_route.is_some() {
            self.session.begin_current().await
        } else {
            self.session.begin().await
        }
        .map_err(CellDispatchError::Routing);
        let attempt = result.map_err(|e| self.error(e))?;
        if self
            .remote_route
            .as_ref()
            .is_some_and(|expected| !expected.matches(attempt.route()))
        {
            return Err(self.error(CellDispatchError::Invalid(
                "remote command destination route",
            )));
        }
        self.attempt = Some(attempt);
        self.dispatched = false;
        Ok(())
    }

    pub async fn retry(&mut self) -> Result<(), CellCommandError> {
        let previous = self.current()?.clone();
        self.dispatched = true;
        let result = self
            .session
            .retry(&previous)
            .await
            .map_err(CellDispatchError::Routing);
        self.attempt = Some(result.map_err(|e| self.error(e))?);
        self.dispatched = false;
        Ok(())
    }

    pub async fn redirect(&mut self, hint: &GroupRoute) -> Result<(), CellCommandError> {
        let previous = self.current()?.clone();
        self.dispatched = true;
        let result = self
            .session
            .redirect(&previous, hint)
            .await
            .map_err(CellDispatchError::Routing);
        self.attempt = Some(result.map_err(|e| self.error(e))?);
        self.dispatched = false;
        Ok(())
    }

    fn current(&self) -> Result<&CellRouteAttempt, CellCommandError> {
        self.attempt
            .as_ref()
            .ok_or_else(|| self.error(CellDispatchError::Invalid("missing route")))
    }

    fn command_attempt(&self, origin: &Origin) -> CommandAttempt {
        CommandAttempt {
            request: CommandRequest {
                id: self.request.id,
                operation: self.request.contract.operation.clone(),
                contract_digest: self.request.contract.digest,
                request_digest: command_request_digest(&self.request.input),
                durability: self.request.durability,
                retry_until_ms: self.request.retry_until_ms,
            },
            proposal: origin.proposal,
            preview: origin.preview,
            inputs: self.inputs,
        }
    }

    /// Retain the origin before entering SQL. A cancelled or lost preparation
    /// response resumes that producer, never a second destination's callback.
    pub async fn prepare(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
    ) -> Result<CellCommandPreparation, CellCommandError> {
        let attempt = self.current()?.clone();
        self.session
            .validate_attempt(&attempt)
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        if self.dispatched {
            return Err(self.error(CellDispatchError::Invalid("consumed route attempt")));
        }
        self.dispatched = true;
        if self.origin.is_some() || self.output.is_some() {
            return self.resume().await;
        }
        let domain = self.dispatcher.domain.clone();
        if !runtime.uses_domain(&domain) || !sql.uses_domain(&domain) {
            return Err(self.error(CellDispatchError::Invalid("foreign execution domain")));
        }
        let timer = domain
            .sleep_until(attempt.deadline_ms())
            .map_err(|_| self.error(closed()))?;
        let result = crate::asyncrt::select! {
            result = self.prepare_at(&attempt, runtime, sql, options) => result,
            _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        self.checked(result, attempt.deadline_ms())
    }

    async fn prepare_at(
        &mut self,
        attempt: &CellRouteAttempt,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
    ) -> Result<CellCommandPreparation, CellDispatchError> {
        let authority = runtime
            .execution_authority(attempt.route().history, self.cell.clone())
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.route() != attempt.route() {
            return Err(CellDispatchError::Invalid("destination route binding"));
        }
        if self.request.durability == Durability::S3 {
            self.dispatcher
                .s3
                .as_ref()
                .ok_or(CellDispatchError::Invalid("S3 command owner"))?
                .require_runtime(runtime)?;
            self.dispatcher
                .s3
                .as_ref()
                .ok_or(CellDispatchError::Invalid("S3 command owner"))?
                .require_group(attempt.route())?;
        }
        let (proposal, preview) = self
            .dispatcher
            .domain
            .run(async {
                use rand::RngCore as _;
                let mut proposal = [0; 16];
                let mut preview = [0; 16];
                let mut rng = crate::asyncrt::rng("cell-group-command-preparation");
                rng.fill_bytes(&mut proposal);
                rng.fill_bytes(&mut preview);
                (ProposalId::new(proposal), ImageId::new(preview))
            })
            .await
            .map_err(|_| closed())?;
        let proposal = proposal.map_err(|_| CellDispatchError::Invalid("proposal ID"))?;
        let preview = preview.map_err(|_| CellDispatchError::Invalid("preview ID"))?;
        self.session
            .mark_submitted(attempt)
            .map_err(CellDispatchError::Routing)?;
        self.ambiguous = true;
        self.origin = Some(Origin {
            runtime: runtime.clone(),
            sql: sql.clone(),
            options: options.clone(),
            route: attempt.route().clone(),
            proposal,
            preview,
            prepared: None,
            submitted: false,
        });
        let command = self.command_attempt(
            self.origin
                .as_ref()
                .ok_or(CellDispatchError::Invalid("missing origin"))?,
        );
        let outcome = if self.system {
            sql.stage_system_owned(
                options,
                authority,
                command,
                self.dispatcher.class.clone(),
                self.request.input.clone(),
                CellSqlStageMode::InvokeBefore(attempt.deadline_ms()),
            )
            .await
        } else {
            sql.stage_registered_owned(
                options.clone(),
                authority,
                command,
                self.dispatcher.class.clone(),
                self.request.input.clone(),
                CellSqlStageMode::InvokeBefore(attempt.deadline_ms()),
                self.dispatcher
                    .class
                    .is_cell_rpc(&self.request.contract.operation)
                    .then(|| crate::raft::host_sql::cell_groups::RpcCommitContext {
                        runtime: runtime.clone(),
                        archive: self.dispatcher.s3.clone(),
                        route: attempt.route().clone(),
                        limits: options.limits(),
                        deadline: attempt.deadline_ms(),
                    }),
            )
            .await
        };
        match outcome {
            Ok(outcome) => self.preparation(outcome).await,
            Err(failure) => {
                // Admission refusals and received Stage errors completed without
                // installing this producer. Lost replies retain the exact origin.
                if !failure.unresolved {
                    self.origin = None;
                }
                Err(CellDispatchError::Execution(failure.error))
            }
        }
    }

    async fn preparation(
        &mut self,
        outcome: CellSqlOutcome,
    ) -> Result<CellCommandPreparation, CellDispatchError> {
        match outcome {
            CellSqlOutcome::Prepared(prepared) => {
                self.origin
                    .as_mut()
                    .ok_or(CellDispatchError::Invalid("missing origin"))?
                    .prepared = Some(prepared);
                Ok(CellCommandPreparation::Prepared)
            }
            CellSqlOutcome::Receipt(receipt) => {
                let response = self.response(receipt).await?;
                Ok(CellCommandPreparation::Committed(response))
            }
            _ => Err(CellDispatchError::Invalid("command preparation response")),
        }
    }

    pub async fn resume(&mut self) -> Result<CellCommandPreparation, CellCommandError> {
        let deadline = self.current()?.deadline_ms();
        let domain = self.dispatcher.domain.clone();
        if domain.mono_ms().map_err(|_| self.error(closed()))? >= deadline {
            return Err(self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)));
        }
        let timer = domain
            .sleep_until(deadline)
            .map_err(|_| self.error(closed()))?;
        let result = crate::asyncrt::select! {
            result = self.resume_at() => result,
            _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        self.checked(result, deadline)
    }

    async fn resume_at(&mut self) -> Result<CellCommandPreparation, CellDispatchError> {
        if self.output.is_some() {
            return self
                .finish_output()
                .await
                .map(CellCommandPreparation::Committed);
        }
        let origin = self
            .origin
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing origin"))?;
        match origin.sql.resume(self.cell.clone(), origin.proposal).await {
            Ok(prepared) => self.preparation(CellSqlOutcome::Prepared(prepared)).await,
            Err(ExecutionError::Uncertain) => self.lookup().await,
            Err(error) => Err(CellDispatchError::Execution(error)),
        }
    }

    // A lost duplicate/settlement response can leave no pending producer. The
    // registered path with allow_new=false may return its receipt, never execute.
    async fn lookup(&mut self) -> Result<CellCommandPreparation, CellDispatchError> {
        let origin = self
            .origin
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing origin"))?;
        let authority = origin
            .runtime
            .execution_authority(origin.route.history, self.cell.clone())
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.route() != &origin.route {
            return Err(CellDispatchError::Invalid("origin route binding"));
        }
        let outcome = if self.system {
            origin
                .sql
                .stage_system_owned(
                    origin.options.clone(),
                    authority,
                    self.command_attempt(origin),
                    self.dispatcher.class.clone(),
                    self.request.input.clone(),
                    CellSqlStageMode::Lookup,
                )
                .await
        } else {
            origin
                .sql
                .stage_registered_owned(
                    origin.options.clone(),
                    authority,
                    self.command_attempt(origin),
                    self.dispatcher.class.clone(),
                    self.request.input.clone(),
                    CellSqlStageMode::Lookup,
                    None,
                )
                .await
        }
        .map_err(|failure| CellDispatchError::Execution(failure.error))?;
        self.preparation(outcome).await
    }

    pub async fn submit(&mut self) -> Result<CellCommandSubmission, CellCommandError> {
        let deadline = self.current()?.deadline_ms();
        let domain = self.dispatcher.domain.clone();
        if domain.mono_ms().map_err(|_| self.error(closed()))? >= deadline {
            return Err(self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)));
        }
        let timer = domain
            .sleep_until(deadline)
            .map_err(|_| self.error(closed()))?;
        let result = crate::asyncrt::select! {
            result = self.submit_at() => result,
            _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        self.checked(result, deadline)
    }

    async fn submit_at(&mut self) -> Result<CellCommandSubmission, CellDispatchError> {
        let origin = self
            .origin
            .as_mut()
            .ok_or(CellDispatchError::Invalid("missing origin"))?;
        let prepared = origin
            .prepared
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing preparation"))?;
        if let Some(proof) = prepared.rejection().map_err(CellDispatchError::Runtime)? {
            return self.reject(proof).await;
        }
        origin.submitted = true;
        origin
            .sql
            .mark_submitted(self.cell.clone(), origin.proposal)
            .await
            .map_err(CellDispatchError::Execution)?;
        match prepared
            .submit(&origin.runtime)
            .await
            .map_err(CellDispatchError::Runtime)?
        {
            CellSqlSubmission::Accepted(accepted) => Ok(CellCommandSubmission::Accepted(accepted)),
            CellSqlSubmission::Rejected(proof) => self.reject(proof).await,
        }
    }

    async fn reject(
        &mut self,
        proof: CellSqlRejection,
    ) -> Result<CellCommandSubmission, CellDispatchError> {
        let origin = self
            .origin
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing origin"))?;
        match origin.sql.reject(self.cell.clone(), proof).await {
            // The retained seal proves this exact preparation can never append;
            // Uncertain here means its worker cleanup has already removed it.
            Ok(()) | Err(ExecutionError::Uncertain) => (),
            Err(error) => return Err(CellDispatchError::Execution(error)),
        }
        self.origin = None;
        Ok(CellCommandSubmission::Rejected)
    }

    /// Call after durable apply progress. Same-term absence remains uncertain;
    /// only the existing newer-term barrier can establish NotCommitted.
    pub async fn settle(&mut self) -> Result<CellCommandSettlement, CellCommandError> {
        let deadline = self.current()?.deadline_ms();
        let domain = self.dispatcher.domain.clone();
        if domain.mono_ms().map_err(|_| self.error(closed()))? >= deadline {
            return Err(self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)));
        }
        let timer = domain
            .sleep_until(deadline)
            .map_err(|_| self.error(closed()))?;
        let result = crate::asyncrt::select! {
            result = self.settle_at() => result,
            _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        self.checked(result, deadline)
    }

    async fn settle_at(&mut self) -> Result<CellCommandSettlement, CellDispatchError> {
        if self.output.is_some() {
            return self
                .finish_output()
                .await
                .map(CellCommandSettlement::Committed);
        }
        let origin = self
            .origin
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing origin"))?;
        if !origin.submitted {
            return Err(CellDispatchError::Invalid("command not submitted"));
        }
        let authority = origin
            .runtime
            .reconciliation_authority(origin.route.history, self.cell.clone())
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.route() != &origin.route {
            return Err(CellDispatchError::Invalid("origin route binding"));
        }
        match origin.sql.reconcile(origin.proposal, authority).await {
            Ok(CellSqlReconciliation::Committed(receipt)) => {
                let response = self.response(receipt).await?;
                Ok(CellCommandSettlement::Committed(response))
            }
            Ok(CellSqlReconciliation::NotCommitted) => {
                self.origin = None;
                Ok(CellCommandSettlement::NotCommitted)
            }
            Ok(CellSqlReconciliation::Refused(failure)) => {
                self.origin = None;
                Ok(CellCommandSettlement::Refused(failure))
            }
            Err(ExecutionError::Uncertain) => {
                match origin.sql.resume(self.cell.clone(), origin.proposal).await {
                    Ok(_) => return Err(CellDispatchError::Execution(ExecutionError::Uncertain)),
                    Err(ExecutionError::Uncertain) => (),
                    Err(error) => return Err(CellDispatchError::Execution(error)),
                }
                match self.lookup().await? {
                    CellCommandPreparation::Committed(receipt) => {
                        Ok(CellCommandSettlement::Committed(receipt))
                    }
                    CellCommandPreparation::Prepared => {
                        Err(CellDispatchError::Invalid("lookup executed a command"))
                    }
                }
            }
            Err(error) => Err(CellDispatchError::Execution(error)),
        }
    }
}

fn fingerprint(
    cell: &CellHistory,
    request: &CellCommandRequest,
    inputs: CommandInputs,
    limits: CellRoutingLimits,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-cell-command-invocation-1\0");
    hash.update(cell_image_history_digest(cell));
    hash.update(request.id.client.bytes());
    hash.update(request.id.sequence().to_be_bytes());
    hash.update((request.contract.operation.len() as u32).to_be_bytes());
    hash.update(request.contract.operation.as_bytes());
    hash.update(request.contract.digest);
    hash.update(command_request_digest(&request.input));
    hash.update([match request.durability {
        Durability::Quorum => 1,
        Durability::S3 => 2,
    }]);
    hash.update(request.retry_until_ms.to_be_bytes());
    hash.update(inputs.now_unix_millis.to_be_bytes());
    hash.update(inputs.random);
    hash.update(limits.timeout.as_nanos().to_be_bytes());
    hash.update([limits.max_attempts]);
    hash.finalize().into()
}
