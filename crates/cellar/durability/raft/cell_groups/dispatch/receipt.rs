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
    selected: CellSqlReceiptLookup,
    token: CellVersionToken,
}

impl CellReceiptResponse {
    pub fn command(&self) -> CommandId {
        self.command
    }

    /// None is ambiguous, even after a quorum read: an unresolved or delayed
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
        if request.durability != Durability::Quorum
            || self.directory.namespace_config().minimum_durability != Durability::Quorum
        {
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
        hash.update([1, limits.max_attempts]);
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
                CommandRequest {
                    id: self.request.id,
                    operation: self.request.contract.operation.clone(),
                    contract_digest: self.request.contract.digest,
                    request_digest: command_request_digest(&self.request.input),
                    durability: self.request.durability,
                    retry_until_ms: 0,
                },
                self.request.input.clone(),
            )
            .await
            .map_err(CellDispatchError::Execution)?;
        if selected
            .receipt()
            .is_some_and(|r| r.minimum_durability() != Durability::Quorum)
        {
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
            .issue_cell(selected.observation(), attempt.route().schema_digest, now)
            .map_err(CellDispatchError::Version)?;
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        Ok(CellReceiptResponse {
            command: self.request.id,
            selected,
            token,
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
