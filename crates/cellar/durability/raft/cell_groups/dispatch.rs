// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Catalog-bound invocations over the existing consensus and SQL owners.
//! Routing observations never replace destination authority or ingress grants.

use super::*;
use crate::application::consensus::{
    OperationContract, OperationKind, RaftClass, RegistrationError,
};
use crate::asyncrt::{DomainToken, ExecutionDomain};
use crate::raft::budget::{Budget, Permit};
use crate::raft::completion::RuntimeError;
use crate::raft::execution::ExecutionError;
use crate::raft::version::{CellVersionObservation, CellVersionToken, VersionError, VersionKeys};
use cellar_logic::cell_group::{CellHistory, GroupRoute};
use cellar_logic::consensus::ProposalId;
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};

#[derive(Clone, Copy)]
pub struct CellDispatchLimits {
    pub max_invocations: usize,
    pub retained_bytes: usize,
    pub max_input_bytes: usize,
}

#[derive(Clone, Debug)]
pub enum CellQueryConsistency {
    Stale,
    AtLeast(CellVersionToken),
    Linearizable,
}

pub struct CellQueryRequest {
    pub invocation: ProposalId,
    pub contract: OperationContract,
    pub input: Vec<u8>,
    pub consistency: CellQueryConsistency,
}

#[derive(Debug)]
pub enum CellDispatchError {
    Invalid(&'static str),
    Routing(CellRoutingError),
    Registration(RegistrationError),
    Runtime(RuntimeError),
    Execution(ExecutionError),
    Version(VersionError),
}

impl fmt::Display for CellDispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cell dispatch: {self:?}")
    }
}

impl std::error::Error for CellDispatchError {}

/// One authorized namespace/class registration. Clones share invocation admission;
/// no cell owns a worker, timer loop or independent admission bank.
#[derive(Clone)]
pub struct CellGroupDispatcher {
    directory: RangeDirectory,
    class: Arc<RaftClass>,
    keys: VersionKeys,
    domain: DomainToken,
    limits: CellDispatchLimits,
    budget: Arc<Budget>,
}

impl CellGroupDispatcher {
    pub fn new(
        domain: &ExecutionDomain,
        directory: RangeDirectory,
        class: Arc<RaftClass>,
        keys: VersionKeys,
        limits: CellDispatchLimits,
    ) -> Result<Self, CellDispatchError> {
        if class.schema_digest() != directory.namespace_config().schema_digest {
            return Err(CellDispatchError::Registration(
                RegistrationError::ContractMismatch,
            ));
        }
        if !(1..=65_536).contains(&limits.max_invocations)
            || limits.max_input_bytes == 0
            || limits
                .max_input_bytes
                .checked_mul(2)
                .and_then(|n| n.checked_add(8192))
                .is_none_or(|n| n > limits.retained_bytes)
        {
            return Err(CellDispatchError::Invalid("invocation limits"));
        }
        let domain = domain.services().domain_token().ok_or_else(closed)?;
        Ok(Self {
            directory,
            class,
            keys,
            domain,
            limits,
            budget: Budget::new(limits.retained_bytes, limits.max_invocations),
        })
    }

    pub fn usage(&self) -> (usize, usize) {
        self.budget.usage()
    }

    /// Freeze the complete request before discovery. Input copies for a physical
    /// attempt are included in this reservation; SQL additionally retains its
    /// own admission if the invocation waiter or router is dropped.
    pub fn query(
        &self,
        cell: CellHistory,
        mut request: CellQueryRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> Result<CellQueryInvocation, CellDispatchError> {
        let started_at = self.domain.mono_ms().map_err(|_| closed())?;
        if request.contract.operation.len() > 128
            || request.input.len() > self.limits.max_input_bytes
        {
            return Err(CellDispatchError::Runtime(RuntimeError::Overloaded));
        }
        if request.contract.kind != OperationKind::Query {
            return Err(CellDispatchError::Registration(
                RegistrationError::KindMismatch,
            ));
        }
        self.class
            .validate_operation(&request.contract)
            .map_err(CellDispatchError::Registration)?;
        let bytes = request
            .input
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let admission = self
            .budget
            .acquire(bytes)
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        request.input = request.input.into_boxed_slice().into_vec();
        request.contract.operation = request.contract.operation.into_boxed_str().into_string();
        let session = self
            .directory
            .routing_session_in(
                self.domain.clone(),
                CellRouteRequest {
                    cell: cell.clone(),
                    operation: CellRouteOperation::Query(request.invocation),
                    fingerprint: fingerprint(&cell, &request, limits),
                },
                limits,
                cached,
                started_at,
            )
            .map_err(CellDispatchError::Routing)?;
        Ok(CellQueryInvocation {
            dispatcher: self.clone(),
            request,
            session,
            attempt: None,
            dispatched: false,
            _admission: admission,
        })
    }
}

/// A non-cloneable invocation. Each ticket is consumed before entering async
/// destination work; retries and redirects share one original deadline/budget.
pub struct CellQueryInvocation {
    dispatcher: CellGroupDispatcher,
    request: CellQueryRequest,
    session: CellRoutingSession,
    attempt: Option<CellRouteAttempt>,
    dispatched: bool,
    _admission: Permit,
}

impl CellQueryInvocation {
    pub fn attempt(&self) -> Option<&CellRouteAttempt> {
        self.attempt.as_ref()
    }

    pub async fn begin(&mut self) -> Result<(), CellDispatchError> {
        self.dispatched = true;
        let attempt = self
            .session
            .begin()
            .await
            .map_err(CellDispatchError::Routing)?;
        self.attempt = Some(attempt);
        self.dispatched = false;
        Ok(())
    }

    pub async fn retry(&mut self) -> Result<(), CellDispatchError> {
        let previous = self
            .attempt
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing route"))?;
        self.dispatched = true;
        let attempt = self
            .session
            .retry(previous)
            .await
            .map_err(CellDispatchError::Routing)?;
        self.attempt = Some(attempt);
        self.dispatched = false;
        Ok(())
    }

    pub async fn redirect(&mut self, hint: &GroupRoute) -> Result<(), CellDispatchError> {
        let previous = self
            .attempt
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing route"))?;
        self.dispatched = true;
        let attempt = self
            .session
            .redirect(previous, hint)
            .await
            .map_err(CellDispatchError::Routing)?;
        self.attempt = Some(attempt);
        self.dispatched = false;
        Ok(())
    }

    /// Execute on a local destination with the same captured clock owner. Remote
    /// transports must preserve the remaining deadline through their own protocol;
    /// monotonic timestamps from unrelated hosts are never interchangeable.
    pub async fn execute(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
    ) -> Result<CellQueryResponse, CellDispatchError> {
        let attempt = self
            .attempt
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing route"))?;
        self.session
            .validate_attempt(attempt)
            .map_err(CellDispatchError::Routing)?;
        if self.dispatched {
            return Err(CellDispatchError::Invalid("consumed route attempt"));
        }
        self.dispatched = true;
        let domain = &self.dispatcher.domain;
        if !runtime.uses_domain(domain) || !sql.uses_domain(domain) {
            return Err(CellDispatchError::Invalid("foreign execution domain"));
        }
        let timer = domain
            .sleep_until(attempt.deadline_ms())
            .map_err(|_| closed())?;
        crate::asyncrt::select! {
            result = self.execute_at(attempt, runtime, sql, options) => {
                let result = result?;
                self.session.validate_attempt(attempt).map_err(CellDispatchError::Routing)?;
                Ok(result)
            }
            _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        }
    }

    async fn execute_at(
        &self,
        attempt: &CellRouteAttempt,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
    ) -> Result<CellQueryResponse, CellDispatchError> {
        let cell = &attempt.request().cell;
        let schema = self.dispatcher.class.schema_digest();
        let consistency = match &self.request.consistency {
            CellQueryConsistency::Stale => CellReadConsistency::Stale,
            CellQueryConsistency::Linearizable => CellReadConsistency::Linearizable,
            CellQueryConsistency::AtLeast(token) => {
                let observation = self
                    .dispatcher
                    .keys
                    .verify_cell(token, cell, schema, self.now().await?)
                    .map_err(CellDispatchError::Version)?;
                CellReadConsistency::AtLeast(observation.version)
            }
        };
        let authority = runtime
            .read_authority(
                attempt.route().history,
                cell.clone(),
                consistency,
                attempt.deadline_ms(),
            )
            .await
            .map_err(CellDispatchError::Runtime)?;
        if authority.route() != attempt.route() || authority.cell().version.history != *cell {
            return Err(CellDispatchError::Invalid("destination route binding"));
        }
        let selected = sql
            .query_registered(
                options,
                authority,
                self.dispatcher.class.clone(),
                self.request.contract.clone(),
                self.request.input.clone(),
            )
            .await
            .map_err(CellDispatchError::Execution)?;
        let token = self
            .dispatcher
            .keys
            .issue_cell(selected.observation(), schema, self.now().await?)
            .map_err(CellDispatchError::Version)?;
        Ok(CellQueryResponse { selected, token })
    }

    async fn now(&self) -> Result<u64, CellDispatchError> {
        self.dispatcher
            .domain
            .run(async { crate::asyncrt::wall_ms().max(0) as u64 })
            .await
            .map_err(|_| closed())
    }
}

pub struct CellQueryResponse {
    selected: CellSqlQueryResult,
    token: CellVersionToken,
}

impl CellQueryResponse {
    pub fn result(&self) -> &[u8] {
        self.selected.result()
    }

    pub fn observation(&self) -> &CellVersionObservation {
        self.selected.observation()
    }

    pub fn token(&self) -> &CellVersionToken {
        &self.token
    }
}

fn closed() -> CellDispatchError {
    CellDispatchError::Runtime(RuntimeError::Closed)
}

fn fingerprint(
    cell: &CellHistory,
    request: &CellQueryRequest,
    limits: CellRoutingLimits,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-cell-query-invocation-1\0");
    hash.update(cell_image_history_digest(cell));
    hash.update(limits.timeout.as_nanos().to_be_bytes());
    hash.update([limits.max_attempts]);
    hash.update(request.invocation.bytes());
    hash.update((request.contract.operation.len() as u32).to_be_bytes());
    hash.update(request.contract.operation.as_bytes());
    hash.update(request.contract.digest);
    hash.update((request.input.len() as u64).to_be_bytes());
    hash.update(&request.input);
    match &request.consistency {
        CellQueryConsistency::Stale => hash.update([0]),
        CellQueryConsistency::Linearizable => hash.update([1]),
        CellQueryConsistency::AtLeast(token) => {
            hash.update([2]);
            hash.update(token.as_str());
        }
    }
    hash.finalize().into()
}
