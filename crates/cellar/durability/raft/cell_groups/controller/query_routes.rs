// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One controller invocation owns every replica attempt and its original budget.
use super::*;
use crate::raft::cell_groups::{
    CellDispatchError as Error, CellQueryInvocation, CellQueryResponse, CellQueryRouting,
    CellSqlOptions,
};
use crate::raft::host_sql::cell_groups::ReadRetention;
use crate::raft::storage::LogError;
use crate::raft::transport::TransportError;
use cellar_logic::consensus::{CommandFailure, MemberId};

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
    let CellQueryRouting::Replicas {
        members,
        attempt_timeout,
        leader_fallback,
    } = invocation.routing()
    else {
        return destination.local(invocation, options, permit).await;
    };
    let mut last = None;
    for (n, member) in members.iter().enumerate() {
        if n > 0 {
            invocation.retry().await?;
        }
        invocation.limit_attempt(attempt_timeout)?;
        let result = destination
            .attempt(invocation, *member, options.clone(), permit.clone())
            .await;
        match result {
            Ok(response) => return Ok(response),
            Err(error) if retryable(&error) => last = Some(error),
            Err(error) => return Err(error),
        }
    }
    if leader_fallback {
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

fn retryable(error: &Error) -> bool {
    match error {
        Error::Runtime(
            RuntimeError::NotLeader
            | RuntimeError::NotReady
            | RuntimeError::NotFound
            | RuntimeError::DeadlineExceeded
            | RuntimeError::Overloaded,
        ) => true,
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
