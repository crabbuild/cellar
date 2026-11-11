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
    async fn router_members(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        maximum: usize,
        deadline: u64,
        prefer_leader: bool,
    ) -> Result<Vec<MemberId>, Error> {
        let selected = || {
            self.transport
                .cell_query_candidates(route, maximum, None, None, prefer_leader)
        };
        match selected() {
            Err(TransportError::UnknownGroup) => {
                self.transport
                    .refresh_managed_router(route.history, deadline)
                    .await
                    .map_err(Error::Runtime)?;
                selected().map_err(Error::Transport)
            }
            result => result.map_err(Error::Transport),
        }
    }

    async fn local_members(
        &self,
        invocation: &CellQueryInvocation,
        route: &cellar_logic::cell_group::GroupRoute,
        local: MemberId,
        tried_locality: bool,
    ) -> Result<Vec<MemberId>, Error> {
        // Ordinary caught-up local reads need neither a leader observation nor
        // consensus work. Status is advisory and does not wake parked groups.
        let leader = if invocation.prefers_leader() || tried_locality {
            let timer = self
                .shared
                .domain
                .sleep_until(invocation.deadline_ms())
                .map_err(|_| Error::Runtime(RuntimeError::Closed))?;
            let status = crate::asyncrt::select! {
                status = self.runtime.status(route.history) => status,
                _ = timer => return Err(Error::Runtime(RuntimeError::DeadlineExceeded)),
            };
            match status {
                Ok(status) => status.leader.and_then(|member| MemberId::new(member).ok()),
                Err(RuntimeError::NotFound | RuntimeError::NotReady) => None,
                Err(error) => return Err(Error::Runtime(error)),
            }
        } else {
            None
        };
        match self.transport.cell_query_candidates(
            route,
            invocation.maximum_attempts(),
            Some(local),
            leader,
            invocation.prefers_leader(),
        ) {
            // A local proof remains available while no physical send view is
            // installed. Missing routing metadata never grants remote access.
            Err(TransportError::UnknownGroup) => Ok(vec![local]),
            result => result.map_err(Error::Transport),
        }
    }

    fn limit_automatic_attempt(&self, invocation: &mut CellQueryInvocation) -> Result<(), Error> {
        let ordinal = usize::from(
            invocation
                .attempt()
                .ok_or(Error::Invalid("missing route"))?
                .ordinal(),
        );
        let now = self
            .shared
            .domain
            .mono_ms()
            .map_err(|_| Error::Runtime(RuntimeError::Closed))?;
        let remaining = invocation.deadline_ms().saturating_sub(now);
        let tickets = invocation.maximum_attempts().saturating_sub(ordinal) + 1;
        invocation.limit_attempt(Duration::from_millis((remaining / tickets as u64).max(1)))
    }

    async fn automatic(
        &self,
        invocation: &mut CellQueryInvocation,
        options: CellSqlOptions,
        permit: Arc<admission::Retention>,
    ) -> Result<CellQueryResponse, Error> {
        let mut previous = None;
        let mut tried_members = Vec::with_capacity(invocation.maximum_attempts());
        loop {
            let attempt = invocation
                .attempt()
                .ok_or(Error::Invalid("missing route"))?;
            let route = attempt.route();
            let history = route.history;
            let ordinal = usize::from(attempt.ordinal());
            if previous != Some(history) {
                tried_members.clear();
                previous = Some(history);
            }
            // Only the active roster permits local proof acquisition. A
            // retained obsolete engine is physical debt, not a serving owner.
            let selected = match self.runtime.log().cell_group(&history) {
                Ok(group)
                    if self
                        .shared
                        .groups
                        .contains(&history)
                        .map_err(Error::Runtime)? =>
                {
                    self.local_members(
                        invocation,
                        route,
                        group.identity().member,
                        !tried_members.is_empty(),
                    )
                    .await
                    .map(|members| (members, false))
                }
                Ok(_) | Err(LogError::NotFound) => self
                    .router_members(
                        route,
                        invocation.maximum_attempts(),
                        invocation.deadline_ms(),
                        invocation.prefers_leader(),
                    )
                    .await
                    .map(|members| (members, true)),
                Err(error) => Err(Error::Runtime(RuntimeError::Storage(error))),
            };
            let mut exhausted_router = false;
            let mut cached_remaining = false;
            let result = match selected {
                Ok((members, router)) if !members.is_empty() => {
                    // Advice can reorder this snapshot between tickets. Track
                    // identities rather than an index that could skip a peer.
                    let member = members
                        .iter()
                        .copied()
                        .find(|member| !tried_members.contains(member))
                        .unwrap_or(members[0]);
                    if !tried_members.contains(&member) {
                        tried_members.push(member);
                    }
                    cached_remaining = members.iter().any(|member| !tried_members.contains(member));
                    exhausted_router = router && !cached_remaining;
                    self.limit_automatic_attempt(invocation)?;
                    self.attempt(invocation, member, options.clone(), permit.clone())
                        .await
                }
                Ok(_) => Err(Error::Runtime(RuntimeError::NotFound)),
                Err(error) => Err(error),
            };
            match result {
                Ok(response) => return Ok(response),
                Err(error) if retryable(&error) && ordinal < invocation.maximum_attempts() => {
                    if cached_remaining && replica_fallback(&error) {
                        // Lag, pressure or an unavailable first target does not
                        // by itself replace the cell range. Each next ticket
                        // retains the ID, minimum, absolute deadline and permit.
                        invocation.retry().await?;
                        invocation.validate_minimum().await?;
                        continue;
                    }
                    if exhausted_router {
                        match self
                            .transport
                            .refresh_managed_router(history, invocation.deadline_ms())
                            .await
                        {
                            Ok(()) | Err(RuntimeError::NotFound) => (),
                            Err(error) => return Err(Error::Runtime(error)),
                        }
                        tried_members.clear();
                    }
                    // Actual route/membership repair keeps the same session.
                    // A selected successor can have a wholly different roster.
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
        let attempt = invocation
            .attempt()
            .ok_or(Error::Invalid("missing route"))?;
        let route = attempt.route();
        let history = route.history;
        let (is_local, router) = match self.runtime.log().cell_group(&history) {
            Ok(group)
                if self
                    .shared
                    .groups
                    .contains(&history)
                    .map_err(Error::Runtime)? =>
            {
                (group.identity().member == member, false)
            }
            Ok(_) | Err(LogError::NotFound) => (false, true),
            Err(error) => return Err(Error::Runtime(RuntimeError::Storage(error))),
        };
        if router {
            self.transport.note_managed_router(history);
            match self.transport.cell_query_router_has_member(route, member) {
                Ok(true) => (),
                Ok(false) | Err(TransportError::UnknownGroup) => {
                    // Refresh only discovery, before transmitting this exact
                    // read ticket. Keep the member selector and attempt deadline.
                    match self
                        .transport
                        .refresh_managed_router(history, attempt.deadline_ms())
                        .await
                    {
                        Ok(()) | Err(RuntimeError::NotFound) => (),
                        Err(error) => return Err(Error::Runtime(error)),
                    }
                }
                Err(error) => return Err(Error::Transport(error)),
            }
        }
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

fn replica_fallback(error: &Error) -> bool {
    match error {
        Error::Runtime(
            RuntimeError::NotLeader
            | RuntimeError::NotReady
            | RuntimeError::DeadlineExceeded
            | RuntimeError::Overloaded,
        )
        | Error::Transport(
            TransportError::Io(_) | TransportError::Timeout | TransportError::Overloaded,
        ) => true,
        Error::Execution(crate::raft::execution::ExecutionError::Refused(failure))
        | Error::Transport(TransportError::Query(failure)) => unavailable(failure),
        _ => false,
    }
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
