// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One controller invocation owns every replica attempt and its original budget.
use super::*;
use crate::application::consensus::RegistrationError;
use crate::raft::cell_groups::{
    CellDispatchError as Error, CellQueryInvocation, CellQueryResponse, CellQueryRouting,
    CellSqlOptions,
};
use crate::raft::host_sql::cell_groups::ReadRetention;
use crate::raft::storage::LogError;
use crate::raft::transport::TransportError;
use cellar_logic::consensus::{CommandFailure, MemberId};
use std::time::Duration;

struct Destination<'a> {
    runtime: &'a CellGroupRuntime,
    sql: &'a CellGroupSqlPool,
    transport: &'a RaftTransport,
    shared: &'a Shared,
}

pub(super) async fn execute(
    invocation: &mut CellQueryInvocation,
    runtime: &CellGroupRuntime,
    sql: &CellGroupSqlPool,
    transport: &RaftTransport,
    shared: &Shared,
    options: CellSqlOptions,
    permit: Arc<admission::Retention>,
) -> Result<CellQueryResponse, Error> {
    let destination = Destination {
        runtime,
        sql,
        transport,
        shared,
    };
    invocation.begin().await?;
    invocation.validate_minimum().await?;
    let (members, attempt_timeout, leader_fallback) = match invocation.routing() {
        CellQueryRouting::Local => return destination.local(invocation, options, permit).await,
        CellQueryRouting::Automatic => {
            return destination.automatic(invocation, options, permit).await;
        }
        CellQueryRouting::Replicas {
            members,
            attempt_timeout,
            leader_fallback,
        } => (members, Some(attempt_timeout), leader_fallback),
    };
    let mut last = None;
    for (n, member) in members.iter().enumerate() {
        if n > 0 {
            invocation.retry().await?;
        }
        let timeout = match attempt_timeout {
            Some(timeout) => timeout,
            None => {
                let now = shared
                    .domain
                    .mono_ms()
                    .map_err(|_| Error::Runtime(RuntimeError::Closed))?;
                let remaining = invocation.deadline_ms().saturating_sub(now);
                // Leave time for other trusted replicas after a blackholed peer.
                Duration::from_millis((remaining / (members.len() - n) as u64).max(1))
            }
        };
        invocation.limit_attempt(timeout)?;
        let result = destination
            .attempt(invocation, *member, options.clone(), permit.clone())
            .await;
        match result {
            Ok(response) => return Ok(response),
            Err(error) if retryable(&error) => last = Some(error),
            Err(error) => return Err(error),
        }
    }
    if let Some(attempt_timeout) = attempt_timeout.filter(|_| leader_fallback) {
        let history = invocation
            .attempt()
            .ok_or(Error::Invalid("missing route"))?
            .route()
            .history;
        let timer = shared
            .domain
            .sleep_until(invocation.deadline_ms())
            .map_err(|_| Error::Runtime(RuntimeError::Closed))?;
        let status = crate::asyncrt::select! {
            status = runtime.status(history) => status,
            _ = timer => return Err(Error::Runtime(RuntimeError::DeadlineExceeded)),
        };
        let hint = match status {
            Ok(status) => status
                .leader
                .and_then(|member| MemberId::new(member).ok())
                .or(invocation.leader_hint()),
            Err(RuntimeError::NotFound) => invocation.leader_hint(),
            Err(error) => return Err(Error::Runtime(error)),
        };
        if let Some(leader) = hint.filter(|leader| !members.contains(leader)) {
            invocation.retry().await?;
            invocation.limit_attempt(attempt_timeout)?;
            return destination
                .attempt(invocation, leader, options, permit)
                .await;
        }
    }
    Err(last.unwrap_or(Error::Invalid("empty query routing plan")))
}

impl Destination<'_> {
    async fn automatic(
        &self,
        invocation: &mut CellQueryInvocation,
        options: CellSqlOptions,
        permit: Arc<admission::Retention>,
    ) -> Result<CellQueryResponse, Error> {
        let mut previous = None;
        let mut next_member = 0;
        loop {
            let attempt = invocation
                .attempt()
                .ok_or(Error::Invalid("missing route"))?;
            let route = attempt.route();
            let history = route.history;
            let ordinal = usize::from(attempt.ordinal());
            if previous != Some(history) {
                next_member = 0;
                previous = Some(history);
            }
            let result = match self.runtime.log().cell_group(&history) {
                Ok(_) => {
                    self.local(invocation, options.clone(), permit.clone())
                        .await
                }
                Err(LogError::NotFound) => {
                    match self
                        .transport
                        .cell_query_router_members(route, invocation.maximum_attempts())
                    {
                        Ok(members) if !members.is_empty() => {
                            let member = members[next_member % members.len()];
                            next_member += 1;
                            let now = self
                                .shared
                                .domain
                                .mono_ms()
                                .map_err(|_| Error::Runtime(RuntimeError::Closed))?;
                            let remaining = invocation.deadline_ms().saturating_sub(now);
                            let tickets = invocation.maximum_attempts().saturating_sub(ordinal) + 1;
                            invocation.limit_attempt(Duration::from_millis(
                                (remaining / tickets as u64).max(1),
                            ))?;
                            self.attempt(invocation, member, options.clone(), permit.clone())
                                .await
                        }
                        Ok(_) | Err(TransportError::UnknownGroup) => {
                            Err(Error::Runtime(RuntimeError::NotFound))
                        }
                        Err(error) => Err(Error::Transport(error)),
                    }
                }
                Err(error) => Err(Error::Runtime(RuntimeError::Storage(error))),
            };
            match result {
                Ok(response) => return Ok(response),
                Err(error) if retryable(&error) && ordinal < invocation.maximum_attempts() => {
                    // A selected successor can have completely different members.
                    // Replan against a verified root, never an old member list or
                    // a new session with a replenished deadline/attempt budget.
                    invocation.refresh().await?;
                    invocation.validate_minimum().await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn attempt(
        &self,
        invocation: &mut CellQueryInvocation,
        member: MemberId,
        options: CellSqlOptions,
        permit: Arc<admission::Retention>,
    ) -> Result<CellQueryResponse, Error> {
        let history = invocation
            .attempt()
            .ok_or(Error::Invalid("missing route"))?
            .route()
            .history;
        let is_local = match self.runtime.log().cell_group(&history) {
            Ok(group) => group.identity().member == member,
            Err(LogError::NotFound) => false,
            Err(error) => return Err(Error::Runtime(RuntimeError::Storage(error))),
        };
        if is_local {
            self.local(invocation, options, permit).await
        } else {
            invocation
                .execute_remote(self.transport, member)
                .await
                .map(CellQueryResponse::remote)
        }
    }

    async fn local(
        &self,
        invocation: &mut CellQueryInvocation,
        options: CellSqlOptions,
        permit: Arc<admission::Retention>,
    ) -> Result<CellQueryResponse, Error> {
        let history = invocation
            .attempt()
            .ok_or(Error::Invalid("missing route"))?
            .route()
            .history;
        if !self
            .shared
            .groups
            .contains(&history)
            .map_err(Error::Runtime)?
        {
            return Err(Error::Runtime(RuntimeError::NotFound));
        }
        invocation
            .execute_retained(
                self.runtime,
                self.sql,
                options,
                Some(ReadRetention::new(permit)),
            )
            .await
    }
}

fn unavailable(failure: &CommandFailure) -> bool {
    matches!(
        failure,
        CommandFailure::NotLeader { .. }
            | CommandFailure::Unavailable
            | CommandFailure::Overloaded
            | CommandFailure::VersionUnavailable
    )
}

pub(super) fn retryable(error: &Error) -> bool {
    match error {
        Error::Runtime(
            RuntimeError::NotLeader
            | RuntimeError::NotReady
            | RuntimeError::NotFound
            | RuntimeError::DeadlineExceeded
            | RuntimeError::Overloaded,
        )
        | Error::Registration(RegistrationError::UnknownOperation(_)) => true,
        Error::Execution(crate::raft::execution::ExecutionError::Refused(failure)) => {
            unavailable(failure)
        }
        Error::Transport(TransportError::Query(failure)) => unavailable(failure),
        Error::Transport(
            TransportError::Io(_)
            | TransportError::Timeout
            | TransportError::UnknownGroup
            | TransportError::StalePeer
            | TransportError::Overloaded,
        ) => true,
        _ => false,
    }
}
