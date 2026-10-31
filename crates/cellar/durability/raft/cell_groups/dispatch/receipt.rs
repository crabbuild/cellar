// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receipt-only dispatch never stages SQL or turns absence into non-commit.
use super::*;
use crate::application::consensus::command_request_digest;
use crate::raft::execution::{CommandRequest, StoredReceipt};
use cellar_logic::consensus::{CommandFailure, CommandId, Durability};

#[derive(Clone)]
pub struct CellReceiptRequest {
    pub id: CommandId,
    pub contract: OperationContract,
    pub input: Vec<u8>,
    pub durability: Durability,
}

pub struct CellReceiptInvocation {
    dispatcher: CellGroupDispatcher,
    cell: CellHistory,
    request: CellReceiptRequest,
    session: CellRoutingSession,
    attempt: Option<CellRouteAttempt>,
    dispatched: bool,
    remote_route: Option<RemoteMutationRoute>,
    _admission: Permit,
}

pub struct CellReceiptResponse {
    command: CommandId,
    selected: Box<CellSqlReceiptLookup>,
    token: CellVersionToken,
    s3: Option<Box<super::super::controller::S3OutputProof>>,
}

/// One coherent quorum selection, bound to its original operation and deadline.
/// No result escapes before `response` satisfies the required durability. S3
/// admission takes SQL retention here; dropping this phase releases admission.
pub struct CellReceiptSelection<'a> {
    invocation: &'a mut CellReceiptInvocation,
    runtime: CellGroupRuntime,
    attempt: CellRouteAttempt,
    selected: Selected,
}

enum Selected {
    Quorum(Box<CellSqlReceiptLookup>),
    S3(Box<super::super::controller::S3LookupPrepared>),
}

impl CellReceiptSelection<'_> {
    pub async fn response(self) -> Result<CellReceiptResponse, CellDispatchError> {
        self.invocation
            .response(&self.runtime, self.attempt, self.selected)
            .await
    }
}

impl CellReceiptResponse {
    /// Achieved durability of this observation, including an ambiguous absence.
    pub fn durability(&self) -> Durability {
        if self.s3.is_some() {
            Durability::S3
        } else {
            Durability::Quorum
        }
    }

    pub fn command(&self) -> CommandId {
        self.command
    }

    /// None is ambiguous, even after a quorum or S3 read: an unresolved or delayed
    /// original command may still commit. This response releases no producer.
    pub fn receipt(&self) -> Option<&StoredReceipt> {
        self.selected.receipt()
    }

    pub fn observation(&self) -> &CellVersionObservation {
        self.selected.observation()
    }

    pub fn token(&self) -> &CellVersionToken {
        &self.token
    }
}

impl CellGroupDispatcher {
    pub fn lookup_receipt(
        &self,
        cell: CellHistory,
        mut request: CellReceiptRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> Result<CellReceiptInvocation, CellDispatchError> {
        request.durability = request
            .durability
            .max(self.directory.namespace_config().minimum_durability);
        if request.durability == Durability::S3 && self.s3.is_none() {
            return Err(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::UnsupportedCapability,
            )));
        }
        if request.contract.kind != OperationKind::Command {
            return Err(CellDispatchError::Registration(
                RegistrationError::KindMismatch,
            ));
        }
        self.class
            .validate_operation(&request.contract)
            .map_err(CellDispatchError::Registration)?;
        if request.contract.operation.len() > 128
            || request.input.len() > self.limits.max_input_bytes
        {
            return Err(CellDispatchError::Runtime(RuntimeError::Overloaded));
        }
        let bytes = request
            .input
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let permit = self
            .budget
            .acquire(bytes)
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        request.contract.operation = request.contract.operation.into_boxed_str().into_string();
        request.input = request.input.into_boxed_slice().into_vec();
        let started = self.domain.mono_ms().map_err(|_| closed())?;
        let mut hash = Sha256::new();
        hash.update(b"cellar-cell-receipt-invocation-1\0");
        hash.update(cell_image_history_digest(&cell));
        hash.update(request.id.client.bytes());
        hash.update(request.id.sequence().to_be_bytes());
        hash.update((request.contract.operation.len() as u32).to_be_bytes());
        hash.update(request.contract.operation.as_bytes());
        hash.update(request.contract.digest);
        hash.update(command_request_digest(&request.input));
        hash.update([
            match request.durability {
                Durability::Quorum => 1,
                Durability::S3 => 2,
            },
            limits.max_attempts,
        ]);
        hash.update(limits.timeout.as_nanos().to_be_bytes());
        let session = self
            .directory
            .routing_session_in(
                self.domain.clone(),
                CellRouteRequest {
                    cell: cell.clone(),
                    operation: CellRouteOperation::Receipt(request.id),
                    fingerprint: hash.finalize().into(),
                },
                limits,
                cached,
                started,
            )
            .map_err(CellDispatchError::Routing)?
            .using_cache(self.routes.clone());
        Ok(CellReceiptInvocation {
            dispatcher: self.clone(),
            cell,
            request,
            session,
            attempt: None,
            dispatched: false,
            remote_route: None,
            _admission: permit,
        })
    }
}

impl CellReceiptInvocation {
    pub fn attempt(&self) -> Option<&CellRouteAttempt> {
        self.attempt.as_ref()
    }

    pub(crate) fn bind_remote_route(&mut self, route: RemoteMutationRoute) {
        self.remote_route = Some(route);
    }

    pub async fn begin(&mut self) -> Result<(), CellDispatchError> {
        self.dispatched = true;
        let attempt = if self.remote_route.is_some() {
            self.session.begin_current().await
        } else {
            self.session.begin().await
        }
        .map_err(CellDispatchError::Routing)?;
        if self
            .remote_route
            .as_ref()
            .is_some_and(|r| !r.matches(attempt.route()))
        {
            return Err(CellDispatchError::Invalid("receipt destination route"));
        }
        self.attempt = Some(attempt);
        self.dispatched = false;
        Ok(())
    }

    pub async fn retry(&mut self) -> Result<(), CellDispatchError> {
        let previous = self.current()?.clone();
        self.dispatched = true;
        self.attempt = Some(
            self.session
                .retry(&previous)
                .await
                .map_err(CellDispatchError::Routing)?,
        );
        self.dispatched = false;
        Ok(())
    }

    pub async fn redirect(&mut self, hint: &GroupRoute) -> Result<(), CellDispatchError> {
        let previous = self.current()?.clone();
        self.dispatched = true;
        self.attempt = Some(
            self.session
                .redirect(&previous, hint)
                .await
                .map_err(CellDispatchError::Routing)?,
        );
        self.dispatched = false;
        Ok(())
    }

    fn current(&self) -> Result<&CellRouteAttempt, CellDispatchError> {
        self.attempt
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing receipt route"))
    }

    pub async fn execute(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
    ) -> Result<CellReceiptResponse, CellDispatchError> {
        self.select(runtime, sql, options).await?.response().await
    }

    /// Select one immutable quorum image under the original attempt/deadline.
    /// S3 selection transfers result retention before returning this opaque phase;
    /// only its response can expose the required-durability result.
    pub async fn select(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
    ) -> Result<CellReceiptSelection<'_>, CellDispatchError> {
        let attempt = self.current()?.clone();
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        if self.dispatched {
            return Err(CellDispatchError::Invalid("consumed receipt attempt"));
        }
        self.dispatched = true;
        if !runtime.uses_domain(&self.dispatcher.domain)
            || !sql.uses_domain(&self.dispatcher.domain)
        {
            return Err(CellDispatchError::Invalid("receipt execution domain"));
        }
        if self.request.durability == Durability::S3 {
            if self.remote_route.is_some() {
                return Err(CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::UnsupportedCapability,
                )));
            }
            let owner = self
                .dispatcher
                .s3
                .as_ref()
                .ok_or(CellDispatchError::Invalid("S3 receipt owner"))?;
            owner.require_runtime(runtime)?;
            owner.require_group(attempt.route())?;
        }
        let limits = match &options {
            CellSqlOptions::Materialized(o) => o.limits,
            CellSqlOptions::Extents(o) => o.limits,
        };
        let request = CommandRequest {
            id: self.request.id,
            operation: self.request.contract.operation.clone(),
            contract_digest: self.request.contract.digest,
            request_digest: command_request_digest(&self.request.input),
            durability: self.request.durability,
            retry_until_ms: 0,
        };
        let authority = runtime
            .read_authority(
                attempt.route().history,
                self.cell.clone(),
                CellReadConsistency::Linearizable,
                attempt.deadline_ms(),
            )
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.route() != attempt.route() || authority.cell().version.history != self.cell {
            return Err(CellDispatchError::Invalid("receipt destination route"));
        }
        let selected = sql
            .lookup_receipt_registered(
                options,
                authority,
                self.dispatcher.class.clone(),
                request.clone(),
                self.request.input.clone(),
            )
            .await
            .map_err(CellDispatchError::Execution)?;
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        let required = self.request.durability.max(
            selected
                .receipt()
                .map_or(Durability::Quorum, |row| row.minimum_durability()),
        );
        let selected = if required == Durability::S3 {
            if self.remote_route.is_some() {
                return Err(CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::UnsupportedCapability,
                )));
            }
            let owner = self
                .dispatcher
                .s3
                .as_ref()
                .ok_or(CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::UnsupportedCapability,
                )))?;
            Selected::S3(Box::new(owner.prepare_lookup(
                runtime,
                attempt.route().clone(),
                Box::new(selected),
                request,
                limits,
                attempt.deadline_ms(),
            )?))
        } else {
            Selected::Quorum(Box::new(selected))
        };
        Ok(CellReceiptSelection {
            invocation: self,
            runtime: runtime.clone(),
            attempt,
            selected,
        })
    }

    async fn response(
        &mut self,
        runtime: &CellGroupRuntime,
        attempt: CellRouteAttempt,
        selected: Selected,
    ) -> Result<CellReceiptResponse, CellDispatchError> {
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        let (selected, s3, authority) = match selected {
            Selected::Quorum(selected) => (selected, None, None),
            Selected::S3(prepared) => {
                let settled = prepared.response().await?;
                settled
                    .proof
                    .require_current(&self.dispatcher.domain, attempt.deadline_ms())
                    .await?;
                // Use a fresh read authority, whose leader fence tolerates later
                // cell writes, rather than retaining a private writer's base.
                let authority = runtime
                    .read_authority(
                        attempt.route().history,
                        self.cell.clone(),
                        CellReadConsistency::Linearizable,
                        attempt.deadline_ms(),
                    )
                    .await
                    .map_err(CellDispatchError::Runtime)?;
                if authority.route() != attempt.route()
                    || authority.cell().version.history != self.cell
                {
                    return Err(CellDispatchError::Invalid(
                        "S3 receipt current route/history",
                    ));
                }
                if authority.cell().version.sequence
                    < settled.selected.observation().version.sequence
                {
                    return Err(CellDispatchError::Runtime(RuntimeError::NotReady));
                }
                if authority.cell().version == settled.selected.observation().version
                    && authority.cell().witness != settled.selected.observation().witness
                {
                    return Err(CellDispatchError::Invalid("S3 receipt current decision"));
                }
                (settled.selected, Some(settled.proof), Some(authority))
            }
        };
        let now = self
            .dispatcher
            .domain
            .run(async { crate::asyncrt::wall_ms().max(0) as u64 })
            .await
            .map_err(|_| closed())?;
        let token = self
            .dispatcher
            .keys
            .issue_cell(selected.observation(), attempt.route().schema_digest, now)
            .map_err(CellDispatchError::Version)?;
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        if let Some(authority) = authority {
            authority
                .check(self.dispatcher.domain.mono_ms().map_err(|_| closed())?)
                .map_err(CellDispatchError::Runtime)?;
        }
        Ok(CellReceiptResponse {
            command: self.request.id,
            selected,
            token,
            s3,
        })
    }

    pub async fn execute_remote(
        &mut self,
        transport: &crate::raft::transport::RaftTransport,
        member: cellar_logic::consensus::MemberId,
    ) -> Result<crate::raft::transport::CellRemoteReceiptResponse, CellDispatchError> {
        let attempt = self.current()?.clone();
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        if self.dispatched || self.remote_route.is_some() {
            return Err(CellDispatchError::Invalid("consumed receipt attempt"));
        }
        if self.request.durability == Durability::S3 {
            return Err(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::UnsupportedCapability,
            )));
        }
        self.dispatched = true;
        if !transport.uses_command_domain(&self.dispatcher.domain) {
            return Err(CellDispatchError::Invalid("receipt transport domain"));
        }
        let timeout_ms = attempt
            .deadline_ms()
            .checked_sub(self.dispatcher.domain.mono_ms().map_err(|_| closed())?)
            .filter(|n| *n > 0)
            .ok_or(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))?;
        let call = crate::raft::query_wire::receipt::CellReceiptCall {
            request: self.request.clone(),
            timeout_ms,
            route_epoch: attempt.route().epoch.get(),
            birth_digest: attempt.route().birth_digest,
        };
        let response = self
            .dispatcher
            .domain
            .run(transport.lookup_cell_receipt(
                attempt.route(),
                &self.cell,
                member,
                call,
                self.dispatcher.keys.clone(),
                attempt.deadline_ms(),
            ))
            .await
            .map_err(|_| closed())?
            .map_err(|error| match error {
                crate::raft::transport::TransportError::Command(failure) => {
                    CellDispatchError::Execution(ExecutionError::Refused(failure))
                }
                other => CellDispatchError::Transport(other),
            })?;
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        Ok(response)
    }
}
