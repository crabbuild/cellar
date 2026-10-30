// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Routed commands retain their originating producer until exact resolution.
//! The node controller drives publication; acceptance is only provisional.
mod owned;

use super::*;
use crate::application::consensus::command_request_digest;
use crate::raft::execution::{CommandAttempt, CommandRequest};
use crate::storage::CommandInputs;
use cellar_logic::consensus::{CommandFailure, CommandId, Durability};
use cellar_ltx::generation::ImageId;

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
    receipt: Box<CellSqlReceipt>,
    token: CellVersionToken,
}

impl CellCommandResponse {
    pub fn receipt(&self) -> &crate::raft::execution::CellCommittedReceipt {
        self.receipt.receipt()
    }

    pub fn token(&self) -> &CellVersionToken {
        &self.token
    }

    pub fn durability(&self) -> Durability {
        Durability::Quorum
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
    cell: CellHistory,
    request: CellCommandRequest,
    inputs: CommandInputs,
    session: CellRoutingSession,
    attempt: Option<CellRouteAttempt>,
    dispatched: bool,
    ambiguous: bool,
    origin: Option<Origin>,
    _admission: Permit,
}

impl CellGroupDispatcher {
    pub async fn command(
        &self,
        cell: CellHistory,
        mut request: CellCommandRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> Result<CellCommandInvocation, CellCommandError> {
        let error = |cause| CellCommandError {
            command: request.id,
            ambiguous: false,
            cause: Box::new(cause),
        };
        let started = self.domain.mono_ms().map_err(|_| error(closed()))?;
        // Shared-group archival coverage is not integrated yet. Never silently
        // lower either the request or the immutable namespace minimum.
        if request.durability != Durability::Quorum
            || self.directory.namespace_config().minimum_durability != Durability::Quorum
        {
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
        self.class
            .validate_operation(&request.contract)
            .map_err(|e| error(CellDispatchError::Registration(e)))?;
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
        let inputs = self
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
            .map_err(|_| error(closed()))?;
        let session = self
            .directory
            .routing_session_in(
                self.domain.clone(),
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
            .using_cache(self.routes.clone());
        Ok(CellCommandInvocation {
            dispatcher: self.clone(),
            cell,
            request,
            inputs,
            session,
            attempt: None,
            dispatched: false,
            ambiguous: false,
            origin: None,
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
        let result = self
            .session
            .begin()
            .await
            .map_err(CellDispatchError::Routing);
        self.attempt = Some(result.map_err(|e| self.error(e))?);
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
        if self.origin.is_some() {
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
        let outcome = sql
            .stage_registered_owned(
                options,
                authority,
                command,
                self.dispatcher.class.clone(),
                self.request.input.clone(),
                true,
            )
            .await;
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
                self.origin = None;
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
        let outcome = origin
            .sql
            .stage_registered(
                origin.options.clone(),
                authority,
                self.command_attempt(origin),
                self.dispatcher.class.clone(),
                self.request.input.clone(),
                false,
            )
            .await
            .map_err(CellDispatchError::Execution)?;
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
                self.origin = None;
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

    async fn response(
        &self,
        receipt: Box<CellSqlReceipt>,
    ) -> Result<CellCommandResponse, CellDispatchError> {
        if receipt.receipt().receipt.command() != self.request.id
            || receipt.receipt().version.history != self.cell
        {
            return Err(CellDispatchError::Invalid("command receipt binding"));
        }
        if receipt.receipt().receipt.minimum_durability() != Durability::Quorum {
            return Err(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::UnsupportedCapability,
            )));
        }
        let now = self
            .dispatcher
            .domain
            .run(async { crate::asyncrt::wall_ms().max(0) as u64 })
            .await
            .map_err(|_| closed())?;
        let token = self
            .dispatcher
            .keys
            .issue_cell(
                &receipt.receipt().observation(),
                self.dispatcher.class.schema_digest(),
                now,
            )
            .map_err(CellDispatchError::Version)?;
        Ok(CellCommandResponse { receipt, token })
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
    hash.update([1]); // Quorum is the only supported request/minimum until CG-06.
    hash.update(request.retry_until_ms.to_be_bytes());
    hash.update(inputs.now_unix_millis.to_be_bytes());
    hash.update(inputs.random);
    hash.update(limits.timeout.as_nanos().to_be_bytes());
    hash.update([limits.max_attempts]);
    hash.finalize().into()
}
