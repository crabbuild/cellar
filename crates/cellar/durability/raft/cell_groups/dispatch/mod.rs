// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Catalog-bound invocations over the existing consensus and SQL owners.
//! Routing observations never replace destination authority or ingress grants.

mod binding_recovery;
pub(in crate::raft::cell_groups) mod mutation_route;
pub(crate) use mutation_route::MutationRouting;
mod query_routing;
mod receipt;
pub use query_routing::CellQueryRouting;
pub use receipt::{
    CellReceiptInvocation, CellReceiptRequest, CellReceiptResponse, CellReceiptSelection,
};

mod enrollment;
pub use enrollment::{
    CellEnrollmentError, CellEnrollmentInvocation, CellEnrollmentRequest, CellEnrollmentResponse,
};

mod command;
pub(crate) use command::PendingCellCommand;
pub use command::{
    CellCommandError, CellCommandInvocation, CellCommandPreparation, CellCommandRequest,
    CellCommandResponse, CellCommandSettlement, CellCommandSubmission,
};

use super::*;
use crate::application::consensus::{
    OperationContract, OperationKind, QueryAdmission, RaftClass, RegistrationError,
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
    /// Maximum retained group-range hints shared by this registration's clones.
    pub max_cached_routes: usize,
    /// Conservative reserved hint capacity; at least 8 KiB, at most 64 MiB.
    pub route_cache_bytes: usize,
    pub max_invocations: usize,
    pub retained_bytes: usize,
    pub max_input_bytes: usize,
}

#[derive(Clone, Debug)]
pub enum CellQueryConsistency {
    Stale,
    RecentSource {
        max_age_ms: u64,
    },
    AtLeast(CellVersionToken),
    Linearizable,
    /// A fresh quorum read strictly after an authenticated observed cell version.
    LinearizableAfter(CellVersionToken),
}

impl CellQueryConsistency {
    pub(crate) fn is_linearizable(&self) -> bool {
        matches!(self, Self::Linearizable | Self::LinearizableAfter(_))
    }
}

#[derive(Clone)]
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
    Transport(crate::raft::transport::TransportError),
    Archive(Arc<CellGroupArchiveControlError>),
}

impl CellDispatchError {
    /// A typed application or readiness refusal from local execution or an
    /// authenticated remote reply. This does not resolve a command error's ambiguity: callers
    /// must inspect `CellCommandError::ambiguous` and preserve its command ID.
    pub fn refusal(&self) -> Option<&cellar_logic::consensus::CommandFailure> {
        match self {
            Self::Runtime(RuntimeError::NotReady) => {
                Some(&cellar_logic::consensus::CommandFailure::Unavailable)
            }
            Self::Execution(ExecutionError::Refused(failure))
            | Self::Transport(
                crate::raft::transport::TransportError::Query(failure)
                | crate::raft::transport::TransportError::Command(failure)
                | crate::raft::transport::TransportError::CommandRejected(failure),
            ) => Some(failure),
            _ => None,
        }
    }
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
    routes: Arc<crate::raft::catalog::RouteCache>,
    s3: Option<super::controller::S3Client>,
    pub(crate) bindings: Arc<std::sync::OnceLock<Arc<dyn crate::application::cell_bindings::Host>>>,
}

impl CellGroupDispatcher {
    pub(crate) fn input_limit(&self) -> usize {
        self.limits.max_input_bytes
    }

    pub(in crate::raft::cell_groups) fn directory(&self) -> RangeDirectory {
        self.directory.clone()
    }

    pub(crate) fn host_binding(
        &self,
    ) -> Result<
        (
            crate::application::consensus::NamespaceConfig,
            Arc<RaftClass>,
            usize,
        ),
        CellDispatchError,
    > {
        // Reserve the configured invocation/cache envelopes, rather than only
        // their current usage, when admitting a node namespace registration.
        let bytes = self
            .limits
            .retained_bytes
            .checked_add(self.limits.route_cache_bytes)
            .and_then(|n| n.checked_add(self.directory.cache_capacity_bytes()))
            .and_then(|n| n.checked_add(self.class.event_enrollment_bytes()))
            .and_then(|n| n.checked_add(8192))
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        Ok((
            self.directory.namespace_config().clone(),
            self.class.clone(),
            bytes,
        ))
    }

    pub(crate) fn uses_domain(&self, domain: &DomainToken) -> bool {
        self.domain.same_owner(domain)
    }

    pub(crate) fn namespace(
        &self,
    ) -> (
        cellar_logic::consensus::ClusterId,
        cellar_logic::consensus::NamespaceId,
    ) {
        let config = self.directory.namespace_config();
        (config.cluster, config.namespace)
    }

    pub fn new(
        domain: &ExecutionDomain,
        directory: RangeDirectory,
        class: Arc<RaftClass>,
        keys: VersionKeys,
        limits: CellDispatchLimits,
    ) -> Result<Self, CellDispatchError> {
        let domain = domain.services().domain_token().ok_or_else(closed)?;
        Self::from_domain(domain, directory, class, keys, limits)
    }

    pub(crate) fn from_domain(
        domain: DomainToken,
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
        let budget = Budget::new(limits.retained_bytes, limits.max_invocations);
        let routes = Arc::new(
            crate::raft::catalog::RouteCache::new(
                directory.clone(),
                limits.max_cached_routes,
                limits.route_cache_bytes,
                budget.clone(),
                limits.max_input_bytes * 2 + 8192,
            )
            .map_err(|_| CellDispatchError::Invalid("route cache limits"))?,
        );
        Ok(Self {
            routes,
            directory,
            class,
            keys,
            domain,
            limits,
            budget,
            s3: None,
            bindings: Arc::default(),
        })
    }

    /// Bind local command, enrollment and receipt S3 settlement to the node archive owner.
    /// The destination group must already have an archive registration. Remote
    /// response settlement has a separate gate.
    pub fn with_s3(mut self, controller: &CellGroupController) -> Result<Self, CellDispatchError> {
        self.s3 = Some(
            controller
                .s3_client(&self.domain)
                .map_err(CellDispatchError::Runtime)?,
        );
        Ok(self)
    }

    pub(crate) fn effect_binding(
        &self,
        domain: &DomainToken,
    ) -> Result<
        (
            crate::application::consensus::NamespaceConfig,
            Arc<RaftClass>,
        ),
        CellDispatchError,
    > {
        let required_input = if self.class.outbox_definition().is_some() {
            32 * 1024
        } else {
            1024
        };
        if !self.domain.same_owner(domain)
            || !self.class.has_system_commands()
            || self.limits.max_input_bytes < required_input
        {
            return Err(CellDispatchError::Invalid(
                "effect dispatcher domain/class/input capacity",
            ));
        }
        Ok((
            self.directory.namespace_config().clone(),
            self.class.clone(),
        ))
    }

    /// Retained namespace range hints, never per-cell execution or authority.
    pub fn route_cache_usage(&self) -> Result<CellRouteCacheUsage, CellDispatchError> {
        self.routes.usage().map_err(catalog_failure)
    }

    /// Reuse existing bounded route metadata and a local applied manifest.
    /// Cold, remote-only or changed routes fall back to ordinary discovery.
    pub(in crate::raft::cell_groups) fn applied_history_hint(
        &self,
        log: &crate::raft::storage::ReplicaLog,
        cell: &cellar_logic::cell_group::CellId,
    ) -> Result<Option<CellHistory>, CellDispatchError> {
        if !self.uses_domain(&log.domain()) {
            return Err(CellDispatchError::Invalid("binding hint engine domain"));
        }
        let config = self.directory.namespace_config();
        let candidate = CellHistory {
            cluster: config.cluster,
            namespace: config.namespace,
            cell: cell.clone(),
            // Only a range-cache key; never used for enrollment or dispatch.
            incarnation: cellar_logic::consensus::IncarnationId::new([1; 16])
                .map_err(|_| CellDispatchError::Invalid("binding hint identity"))?,
        };
        let Some((route, _, _)) = self.routes.get(&candidate, 0).map_err(catalog_failure)? else {
            return Ok(None);
        };
        let group = match log.cell_group(&route.history) {
            Ok(group) => group,
            Err(crate::raft::storage::LogError::NotFound) => return Ok(None),
            Err(error) => return Err(CellDispatchError::Runtime(error.into())),
        };
        group
            .applied_history_hint(&route, cell, self.class.schema_digest())
            .map_err(|error| CellDispatchError::Runtime(error.into()))
    }

    /// Observe the current directory and invalidate verified changed intervals.
    /// Missing or over-budget proofs select full invalidation. Retained entries
    /// keep their original verified revisions rather than inheriting this head.
    /// This is host-driven metadata refresh, not a serving lease or a timer task.
    pub async fn refresh_routes(&self) -> Result<u64, CellDispatchError> {
        self.domain
            .run(self.routes.refresh())
            .await
            .map_err(|_| closed())?
            .map_err(catalog_failure)
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
        request: CellQueryRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> Result<CellQueryInvocation, CellDispatchError> {
        self.query_routed(cell, request, limits, cached, CellQueryRouting::Local)
    }

    pub fn query_routed(
        &self,
        cell: CellHistory,
        request: CellQueryRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        routing: CellQueryRouting,
    ) -> Result<CellQueryInvocation, CellDispatchError> {
        self.query_routed_admitted(
            cell,
            request,
            limits,
            cached,
            routing,
            QueryAdmission::Public,
        )
    }

    pub(crate) fn query_routed_admitted(
        &self,
        cell: CellHistory,
        mut request: CellQueryRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        mut routing: CellQueryRouting,
        admission: QueryAdmission,
    ) -> Result<CellQueryInvocation, CellDispatchError> {
        let started_at = self.domain.mono_ms().map_err(|_| closed())?;
        routing.validate(limits)?;
        if request.contract.operation.len() > 128
            || request.input.len() > self.limits.max_input_bytes
        {
            return Err(CellDispatchError::Runtime(RuntimeError::Overloaded));
        }
        if matches!(&request.consistency, CellQueryConsistency::RecentSource { max_age_ms }
            if !(1..=3_600_000).contains(max_age_ms))
        {
            return Err(CellDispatchError::Invalid("stale source age"));
        }
        if request.contract.kind != OperationKind::Query {
            return Err(CellDispatchError::Registration(
                RegistrationError::KindMismatch,
            ));
        }
        if let QueryAdmission::Event(minimum) = admission {
            if !request.consistency.is_linearizable()
                || minimum < self.host_binding()?.0.minimum_durability
            {
                return Err(CellDispatchError::Invalid(
                    "event metadata consistency/durability",
                ));
            }
        }
        let local_registration_error = match self
            .class
            .validate_query_admission(&request.contract, admission)
        {
            Ok(()) => None,
            Err(error @ RegistrationError::UnknownOperation(_))
                if !matches!(routing, CellQueryRouting::Local) =>
            {
                // An authenticated remote replica checks its own registration
                // before admitting this exact query contract.
                Some(error)
            }
            Err(error) => return Err(CellDispatchError::Registration(error)),
        };
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
        request.input = request.input.into_boxed_slice().into_vec();
        request.contract.operation = request.contract.operation.into_boxed_str().into_string();
        let session = self
            .directory
            .routing_session_in(
                self.domain.clone(),
                CellRouteRequest {
                    cell: cell.clone(),
                    operation: CellRouteOperation::Query(request.invocation),
                    fingerprint: fingerprint(&cell, &request, limits, &routing, admission),
                },
                limits,
                cached,
                started_at,
            )
            .map_err(CellDispatchError::Routing)?
            .using_cache(self.routes.clone());
        Ok(CellQueryInvocation {
            dispatcher: self.clone(),
            request,
            admission,
            routing,
            local_registration_error,
            leader_hint: None,
            session,
            attempt: None,
            remote_route: None,
            dispatched: false,
            _admission: Arc::new(permit),
        })
    }
}

/// A non-cloneable invocation. Each ticket is consumed before entering async
/// destination work; retries and redirects share one original deadline/budget.
pub struct CellQueryInvocation {
    dispatcher: CellGroupDispatcher,
    request: CellQueryRequest,
    admission: QueryAdmission,
    routing: CellQueryRouting,
    local_registration_error: Option<RegistrationError>,
    leader_hint: Option<cellar_logic::consensus::MemberId>,
    session: CellRoutingSession,
    attempt: Option<CellRouteAttempt>,
    remote_route: Option<RemoteMutationRoute>,
    dispatched: bool,
    _admission: Arc<Permit>,
}

impl CellQueryInvocation {
    pub(crate) fn controller_admission(
        &self,
        domain: &DomainToken,
    ) -> Result<(ProposalId, u64, usize), CellDispatchError> {
        if !self.dispatcher.uses_domain(domain) || self.attempt.is_some() || self.dispatched {
            return Err(CellDispatchError::Invalid(
                "controller requires a fresh local query",
            ));
        }
        let deadline = self.session.deadline_ms();
        if domain.mono_ms().map_err(|_| closed())? >= deadline {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        let bytes = self
            .request
            .input
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        Ok((self.request.invocation, deadline, bytes))
    }

    pub(crate) fn bind_remote_route(&mut self, route: RemoteMutationRoute) {
        self.remote_route = Some(route);
    }

    pub fn attempt(&self) -> Option<&CellRouteAttempt> {
        self.attempt.as_ref()
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
            .is_some_and(|route| !route.matches(attempt.route()))
        {
            return Err(CellDispatchError::Invalid("remote query destination route"));
        }
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

    pub(crate) async fn refresh(&mut self) -> Result<(), CellDispatchError> {
        let previous = self
            .attempt
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing route"))?;
        self.dispatched = true;
        let attempt = self
            .session
            .refresh(previous)
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
        self.execute_retained(runtime, sql, options, None).await
    }

    pub(crate) async fn execute_retained(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        retention: Option<crate::raft::host_sql::cell_groups::ReadRetention>,
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
        if let Some(error) = &self.local_registration_error {
            return Err(CellDispatchError::Registration(error.clone()));
        }
        let domain = &self.dispatcher.domain;
        if !runtime.uses_domain(domain) || !sql.uses_domain(domain) {
            return Err(CellDispatchError::Invalid("foreign execution domain"));
        }
        let timer = domain
            .sleep_until(attempt.deadline_ms())
            .map_err(|_| closed())?;
        crate::asyncrt::select! {
            result = self.execute_at(attempt, runtime, sql, options, retention) => {
                let result = result?;
                if domain.mono_ms().map_err(|_| closed())? >= attempt.deadline_ms() {
                    return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
                }
                self.session.validate_attempt(attempt).map_err(CellDispatchError::Routing)?;
                Ok(result)
            }
            _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        }
    }

    /// One remote attempt over the node's existing bounded peer query lane.
    /// The response owns its transport reservation until the caller drops it.
    pub async fn execute_remote(
        &mut self,
        transport: &crate::raft::transport::RaftTransport,
        member: cellar_logic::consensus::MemberId,
    ) -> Result<crate::raft::transport::CellRemoteQueryResponse, CellDispatchError> {
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
        if !transport.uses_query_domain(&self.dispatcher.domain) {
            return Err(CellDispatchError::Invalid("foreign query domain"));
        }
        let now = self.dispatcher.domain.mono_ms().map_err(|_| closed())?;
        let timeout_ms = attempt
            .deadline_ms()
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .ok_or(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))?;
        let call = crate::raft::query_wire::cell::CellQueryCall {
            request: self.request.clone(),
            admission: self.admission,
            timeout_ms,
            route_epoch: attempt.route().epoch.get(),
            birth_digest: attempt.route().birth_digest,
        };
        let timer = self
            .dispatcher
            .domain
            .sleep_until(attempt.deadline_ms())
            .map_err(|_| closed())?;
        let response = crate::asyncrt::select! {
            response = transport.query_cell(attempt.route(), &attempt.request().cell, member, call, crate::raft::transport::CellQueryContext { keys: self.dispatcher.keys.clone(), admission: self._admission.clone() }, attempt.deadline_ms()) => response,
            _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        let response = response.map_err(|error| CellDispatchError::Transport(error.for_read()))?;
        self.leader_hint = response.leader_hint;
        let response = response.result.map_err(|failure| {
            CellDispatchError::Transport(crate::raft::transport::TransportError::Query(failure))
        })?;
        if self.dispatcher.domain.mono_ms().map_err(|_| closed())? >= attempt.deadline_ms() {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        self.session
            .validate_attempt(attempt)
            .map_err(CellDispatchError::Routing)?;
        // Authenticate again at the caller boundary, then enforce the requested
        // minimum independently from the destination's execution path.
        let observation = self
            .dispatcher
            .keys
            .verify_cell(
                response.token(),
                &attempt.request().cell,
                attempt.route().schema_digest,
                self.now().await?,
            )
            .map_err(CellDispatchError::Version)?;
        if observation != *response.observation() {
            return Err(CellDispatchError::Invalid("query observation"));
        }
        if let CellQueryConsistency::AtLeast(token)
        | CellQueryConsistency::LinearizableAfter(token) = &self.request.consistency
        {
            let minimum = self
                .dispatcher
                .keys
                .verify_cell(
                    token,
                    &attempt.request().cell,
                    attempt.route().schema_digest,
                    self.now().await?,
                )
                .map_err(CellDispatchError::Version)?;
            if !observation
                .version
                .covers(&minimum.version)
                .map_err(|_| CellDispatchError::Version(VersionError::WrongHistory))?
                || matches!(
                    self.request.consistency,
                    CellQueryConsistency::LinearizableAfter(_)
                ) && observation.version.sequence <= minimum.version.sequence
            {
                return Err(CellDispatchError::Runtime(RuntimeError::NotReady));
            }
        }
        self.session
            .validate_attempt(attempt)
            .map_err(CellDispatchError::Routing)?;
        Ok(response)
    }

    async fn execute_at(
        &self,
        attempt: &CellRouteAttempt,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        retention: Option<crate::raft::host_sql::cell_groups::ReadRetention>,
    ) -> Result<CellQueryResponse, CellDispatchError> {
        let cell = &attempt.request().cell;
        let probe = std::time::Instant::now();
        tracing::debug!(target: "cellar::raft::sql_ingress", marker="DEBUG-cg17-restart-724", phase="query-start", cell=%cell.cell.as_str(), invocation=?self.request.invocation, left=?self.dispatcher.domain.mono_ms().map(|now| attempt.deadline_ms().saturating_sub(now)));
        let schema = self.dispatcher.class.schema_digest();
        let consistency = match &self.request.consistency {
            CellQueryConsistency::Stale => CellReadConsistency::Stale,
            CellQueryConsistency::RecentSource { max_age_ms } => {
                CellReadConsistency::RecentSource {
                    max_age_ms: *max_age_ms,
                }
            }
            CellQueryConsistency::Linearizable => CellReadConsistency::Linearizable,
            CellQueryConsistency::AtLeast(token) => {
                let observation = self
                    .dispatcher
                    .keys
                    .verify_cell(token, cell, schema, self.now().await?)
                    .map_err(CellDispatchError::Version)?;
                CellReadConsistency::AtLeast(observation.version)
            }
            CellQueryConsistency::LinearizableAfter(token) => {
                let observation = self
                    .dispatcher
                    .keys
                    .verify_cell(token, cell, schema, self.now().await?)
                    .map_err(CellDispatchError::Version)?;
                CellReadConsistency::LinearizableAfter(observation.version)
            }
        };
        if consistency.is_linearizable() {
            // A parked local read must finish the same fresh-quorum wake as a
            // command before selecting a SQL image. Explicit historical modes
            // retain their separate consistency and admission contracts.
            runtime
                .routing_status(attempt.route().history, attempt.deadline_ms())
                .await
                .map_err(CellDispatchError::Runtime)?;
        }
        let authority = runtime
            .read_authority(
                attempt.route().history,
                cell.clone(),
                consistency,
                attempt.deadline_ms(),
            )
            .await
            .map_err(CellDispatchError::Runtime)?;
        tracing::debug!(target: "cellar::raft::sql_ingress", marker="DEBUG-cg17-restart-724", phase="query-authority", cell=%cell.cell.as_str(), invocation=?self.request.invocation, elapsed=probe.elapsed().as_millis());
        if authority.route() != attempt.route() || authority.cell().version.history != *cell {
            return Err(CellDispatchError::Invalid("destination route binding"));
        }
        let mut selected = sql
            .query_registered_retained(
                options,
                authority,
                crate::raft::host_sql::cell_groups::RegisteredQuery {
                    class: self.dispatcher.class.clone(),
                    contract: self.request.contract.clone(),
                    input: self.request.input.clone(),
                    admission: self.admission,
                },
                Some(crate::raft::host_sql::cell_groups::ReadRetention::new(
                    Arc::new((retention, self._admission.clone())),
                )),
            )
            .await
            .map_err(|error| {
                // The SQL waiter uses the shared execution error envelope. Its
                // exact deadline must have the same routing meaning as this
                // invocation's outer timer; ordinary storage failures stay fatal.
                if matches!(&error, ExecutionError::Storage(cause)
                    if matches!(cause.downcast_ref::<RuntimeError>(), Some(RuntimeError::DeadlineExceeded)))
                {
                    CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                } else {
                    CellDispatchError::Execution(error)
                }
            })?;
        tracing::debug!(target: "cellar::raft::sql_ingress", marker="DEBUG-cg17-restart-724", phase="query-sql", cell=%cell.cell.as_str(), invocation=?self.request.invocation, elapsed=probe.elapsed().as_millis());
        if self.admission == QueryAdmission::Event(cellar_logic::consensus::Durability::S3) {
            let client = self
                .dispatcher
                .s3
                .as_ref()
                .ok_or(CellDispatchError::Runtime(RuntimeError::NotReady))?;
            let settled = client
                .query(
                    runtime,
                    attempt.route().clone(),
                    Box::new(selected),
                    attempt.deadline_ms(),
                )
                .await?;
            tracing::debug!(target: "cellar::raft::sql_ingress", marker="DEBUG-cg17-restart-724", phase="query-s3", cell=%cell.cell.as_str(), invocation=?self.request.invocation, elapsed=probe.elapsed().as_millis());
            settled
                .proof
                .require_current(&self.dispatcher.domain, attempt.deadline_ms())
                .await?;
            tracing::debug!(target: "cellar::raft::sql_ingress", marker="DEBUG-cg17-restart-724", phase="query-current", cell=%cell.cell.as_str(), invocation=?self.request.invocation, elapsed=probe.elapsed().as_millis());
            selected = *settled.selected;
            selected.retain_archive(settled.proof);
        }
        let token = self
            .dispatcher
            .keys
            .issue_cell(selected.observation(), schema, self.now().await?)
            .map_err(CellDispatchError::Version)?;
        Ok(CellQueryResponse {
            selected: QuerySelection::Local(selected),
            token,
        })
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
    selected: QuerySelection,
    token: CellVersionToken,
}

enum QuerySelection {
    Local(CellSqlQueryResult),
    Remote(crate::raft::transport::CellRemoteQueryResponse),
}

impl CellQueryResponse {
    pub fn result(&self) -> &[u8] {
        match &self.selected {
            QuerySelection::Local(selected) => selected.result(),
            QuerySelection::Remote(selected) => selected.result(),
        }
    }

    pub fn observation(&self) -> &CellVersionObservation {
        match &self.selected {
            QuerySelection::Local(selected) => selected.observation(),
            QuerySelection::Remote(selected) => selected.observation(),
        }
    }

    pub fn token(&self) -> &CellVersionToken {
        &self.token
    }

    pub fn replica(
        &self,
    ) -> &cellar_logic::consensus::ReplicaIdentity<cellar_logic::cell_group::GroupHistory> {
        match &self.selected {
            QuerySelection::Local(selected) => selected.replica(),
            QuerySelection::Remote(selected) => selected.replica(),
        }
    }

    pub(crate) fn remote(selected: crate::raft::transport::CellRemoteQueryResponse) -> Self {
        let token = selected.token().clone();
        Self {
            selected: QuerySelection::Remote(selected),
            token,
        }
    }
}

fn closed() -> CellDispatchError {
    CellDispatchError::Runtime(RuntimeError::Closed)
}

fn fingerprint(
    cell: &CellHistory,
    request: &CellQueryRequest,
    limits: CellRoutingLimits,
    routing: &CellQueryRouting,
    admission: QueryAdmission,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-cell-query-invocation-1\0");
    hash.update([match admission {
        QueryAdmission::Public => 0,
        QueryAdmission::Event(cellar_logic::consensus::Durability::Quorum) => 1,
        QueryAdmission::Event(cellar_logic::consensus::Durability::S3) => 2,
    }]);
    hash.update(cell_image_history_digest(cell));
    hash.update(limits.timeout.as_nanos().to_be_bytes());
    hash.update([limits.max_attempts]);
    routing.fingerprint(&mut hash);
    hash.update(request.invocation.bytes());
    hash.update((request.contract.operation.len() as u32).to_be_bytes());
    hash.update(request.contract.operation.as_bytes());
    hash.update(request.contract.digest);
    hash.update((request.input.len() as u64).to_be_bytes());
    hash.update(&request.input);
    match &request.consistency {
        CellQueryConsistency::Stale => hash.update([0]),
        CellQueryConsistency::RecentSource { max_age_ms } => {
            hash.update([3]);
            hash.update(max_age_ms.to_be_bytes());
        }
        CellQueryConsistency::Linearizable => hash.update([1]),
        CellQueryConsistency::AtLeast(token) => {
            hash.update([2]);
            hash.update(token.as_str());
        }
        CellQueryConsistency::LinearizableAfter(token) => {
            hash.update([4]);
            hash.update(token.as_str());
        }
    }
    hash.finalize().into()
}

fn catalog_failure(error: crate::raft::catalog::CatalogError) -> CellDispatchError {
    CellDispatchError::Routing(CellRoutingError {
        reason: CellRoutingFailure::Catalog(error),
        ambiguous_command: None,
    })
}

/// Receiver-side destination constraint. Commands match the actual applied local
/// manifest before execution; enrollment/receipt paths retain catalog discovery.
pub(crate) struct RemoteMutationRoute {
    pub group: cellar_logic::cell_group::GroupHistory,
    pub schema: [u8; 32],
    pub epoch: u64,
    pub birth: [u8; 32],
}

impl RemoteMutationRoute {
    fn matches(&self, route: &GroupRoute) -> bool {
        self.group == route.history
            && self.schema == route.schema_digest
            && self.epoch == route.epoch.get()
            && self.birth == route.birth_digest
    }
}
