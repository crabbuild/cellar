// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Consumed immutable read tickets own work without cloning the retry bank.
use super::*;

/// The captured request and invocation admission are shared. Each physical SQL
/// or transport capture separately retains its destination's bounded admission.
/// Automatic routing owns at most one of these beside its serial fallback.
pub(in crate::raft::cell_groups) struct CellQueryExecution {
    dispatcher: CellGroupDispatcher,
    request: Arc<CellQueryRequest>,
    admission: QueryAdmission,
    local_registration_error: Option<RegistrationError>,
    attempt: CellRouteAttempt,
    leader_hint: Option<cellar_logic::consensus::MemberId>,
    dispatched: bool,
    _admission: Arc<Permit>,
}

impl CellQueryInvocation {
    pub(in crate::raft::cell_groups) fn take_execution(
        &mut self,
    ) -> Result<CellQueryExecution, CellDispatchError> {
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
        Ok(CellQueryExecution {
            dispatcher: self.dispatcher.clone(),
            request: self.request.clone(),
            admission: self.admission,
            local_registration_error: self.local_registration_error.clone(),
            attempt: attempt.clone(),
            leader_hint: None,
            dispatched: false,
            _admission: self._admission.clone(),
        })
    }

    pub(in crate::raft::cell_groups) fn validate_query_result(
        &self,
        attempt: &CellRouteAttempt,
    ) -> Result<(), CellDispatchError> {
        self.session
            .validate_query_result(attempt)
            .map_err(CellDispatchError::Routing)
    }
}

impl CellQueryExecution {
    pub(in crate::raft::cell_groups) fn attempt(&self) -> &CellRouteAttempt {
        &self.attempt
    }

    pub(super) fn leader_hint(&self) -> Option<cellar_logic::consensus::MemberId> {
        self.leader_hint
    }

    pub(in crate::raft::cell_groups) async fn execute_retained(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        retention: Option<crate::raft::host_sql::cell_groups::ReadRetention>,
    ) -> Result<CellQueryResponse, CellDispatchError> {
        self.enter()?;
        let attempt = &self.attempt;
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
                Ok(result)
            }
            _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        }
    }

    pub(in crate::raft::cell_groups) async fn execute_remote(
        &mut self,
        transport: &crate::raft::transport::RaftTransport,
        member: cellar_logic::consensus::MemberId,
    ) -> Result<crate::raft::transport::CellRemoteQueryResponse, CellDispatchError> {
        self.enter()?;
        let attempt = &self.attempt;
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
            request: (*self.request).clone(),
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
        // The transport used this invocation's immutable key configuration,
        // history and schema. Recheck the signed time bounds at the caller and
        // enforce the requested version floor independently of the destination.
        response
            .check_version_time(self.now().await?)
            .map_err(CellDispatchError::Version)?;
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
            let observed = &response.observation().version;
            if !observed
                .covers(&minimum.version)
                .map_err(|_| CellDispatchError::Version(VersionError::WrongHistory))?
                || matches!(
                    self.request.consistency,
                    CellQueryConsistency::LinearizableAfter(_)
                ) && observed.sequence <= minimum.version.sequence
            {
                return Err(CellDispatchError::Runtime(RuntimeError::NotReady));
            }
        }
        Ok(response)
    }

    fn enter(&mut self) -> Result<(), CellDispatchError> {
        if self.dispatched {
            return Err(CellDispatchError::Invalid("consumed route attempt"));
        }
        self.dispatched = true;
        Ok(())
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
        // Queue metadata uses the leader's retention policy, just
        // like its mutating RPCs. A follower's node-local limits may differ.
        // This role check does not replace the fresh quorum authority below.
        let queue_term = if matches!(
            self.admission,
            QueryAdmission::QueueOperator(_) | QueryAdmission::QueueMetrics(_)
        ) {
            let observation = runtime
                .observe(attempt.route().history)
                .await
                .map_err(CellDispatchError::Runtime)?;
            if observation.role != crate::application::consensus::RaftReplicaRole::Leader {
                return Err(CellDispatchError::Runtime(RuntimeError::NotReady));
            }
            Some(observation.status.term)
        } else {
            None
        };
        let may_wait =
            matches!(self.admission, QueryAdmission::Event(_)) && consistency.is_linearizable();
        let retention = Arc::new((retention, self._admission.clone()));
        let mut consistency = consistency;
        let mut selected = loop {
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
            let observed = authority.cell().version.clone();
            let selected = sql
                .query_registered_retained(
                    options.clone(),
                    authority,
                    crate::raft::host_sql::cell_groups::RegisteredQuery {
                        class: self.dispatcher.class.clone(),
                        contract: self.request.contract.clone(),
                        input: self.request.input.clone(),
                        admission: self.admission,
                    },
                    Some(crate::raft::host_sql::cell_groups::ReadRetention::new(
                        retention.clone(),
                    )),
                )
                .await;
            match selected {
                Err(error) => match classify_query_error(error, may_wait) {
                    QueryError::WaitForChange => {
                        // Only the private event reader can defer its selected
                        // output. SQL has joined without a result or archive hold.
                        // Park this same invocation until this exact cell advances,
                        // then obtain fresh quorum authority before inspecting it.
                        // Neither unrelated group progress nor an old token wakes
                        // it; the attempt, fence and absolute deadline are retained.
                        consistency = CellReadConsistency::LinearizableAfter(observed);
                    }
                    QueryError::Return(error) => return Err(error),
                },
                Ok(selected) => break selected,
            }
        };
        if let Some(term) = queue_term {
            let observation = runtime
                .observe(attempt.route().history)
                .await
                .map_err(CellDispatchError::Runtime)?;
            if observation.role != crate::application::consensus::RaftReplicaRole::Leader
                || observation.status.term != term
            {
                return Err(CellDispatchError::Runtime(RuntimeError::NotReady));
            }
        }
        let archived_observation = (self
            .dispatcher
            .class
            .queue_operator_read_contract()
            .as_ref()
            == Some(&self.request.contract)
            || self.dispatcher.class.kv_inline_read_contract().as_ref()
                == Some(&self.request.contract))
            && self.dispatcher.host_binding()?.0.minimum_durability
                == cellar_logic::consensus::Durability::S3;
        if self.admission.minimum() == Some(cellar_logic::consensus::Durability::S3)
            || archived_observation
        {
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
            settled
                .proof
                .require_current(&self.dispatcher.domain, attempt.deadline_ms())
                .await?;
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

enum QueryError {
    WaitForChange,
    Return(CellDispatchError),
}

fn classify_query_error(error: ExecutionError, may_wait: bool) -> QueryError {
    if let ExecutionError::Storage(cause) = &error {
        if may_wait
            && cause.is::<crate::application::consensus::native_event_query::ActivationPending>()
        {
            return QueryError::WaitForChange;
        }
        match cause.downcast_ref::<RuntimeError>() {
            Some(RuntimeError::NotReady) => {
                // A term/source epoch can change without a cell mutation.
                // Return to the existing immutable-reader retry/routing path;
                // a new read still needs fresh quorum authority and the same
                // original deadline. Fenced and storage failures remain fatal.
                return QueryError::Return(CellDispatchError::Runtime(RuntimeError::NotReady));
            }
            Some(RuntimeError::DeadlineExceeded) => {
                return QueryError::Return(CellDispatchError::Runtime(
                    RuntimeError::DeadlineExceeded,
                ));
            }
            _ => (),
        }
    }
    QueryError::Return(CellDispatchError::Execution(error))
}

#[cfg(test)]
#[path = "query_execution_tests.rs"]
mod tests;
