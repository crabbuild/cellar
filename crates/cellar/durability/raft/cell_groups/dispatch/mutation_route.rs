// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select one destination for a mutation before submission.
use super::*;
use crate::raft::transport::{LeaderLookup, RaftTransport, TransportError};
use cellar_logic::consensus::{CommandFailure, MemberId};

/// The controller supplies its minimum retry cadence. Routing distributes the
/// original remaining window across its finite tickets and final execution.
pub(crate) struct MutationRouting<'a> {
    pub transport: &'a RaftTransport,
    pub retry_interval_ms: u64,
}

impl MutationRouting<'_> {
    pub(super) async fn backoff(
        &self,
        domain: &DomainToken,
        deadline: u64,
        remaining_attempts: usize,
    ) -> Result<(), CellDispatchError> {
        backoff(domain, deadline, self.retry_interval_ms, remaining_attempts).await
    }
}

pub(super) async fn backoff(
    domain: &DomainToken,
    deadline: u64,
    retry_interval_ms: u64,
    remaining_attempts: usize,
) -> Result<(), CellDispatchError> {
    let now = domain.mono_ms().map_err(|_| closed())?;
    if now >= deadline {
        return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
    }
    let wake = retry_at(now, deadline, retry_interval_ms, remaining_attempts);
    domain.sleep_until(wake).map_err(|_| closed())?.await;
    if domain.mono_ms().map_err(|_| closed())? >= deadline {
        return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
    }
    Ok(())
}

/// Distribute the original remaining window geometrically across the remaining
/// retries and their final execution. A small controller poll cadence alone can
/// exhaust every RPC ticket before admitted physical work has time to complete.
fn retry_at(now: u64, deadline: u64, interval_ms: u64, remaining_attempts: usize) -> u64 {
    let shares = (1u64 << remaining_attempts.saturating_add(1).min(17)) - 1;
    let delay = deadline.saturating_sub(now) / shares;
    now.saturating_add(delay.max(interval_ms).max(1))
        .min(deadline)
}

#[derive(Default)]
struct ProbeFailures {
    discovery: bool,
    no_leader: bool,
    overloaded: bool,
}

impl ProbeFailures {
    fn leader_retry(&self) -> Option<CommandFailure> {
        if self.discovery {
            None
        } else if self.no_leader {
            Some(CommandFailure::NotLeader { leader: None })
        } else if self.overloaded {
            Some(CommandFailure::Overloaded)
        } else {
            // An empty/unverified member set still needs actual discovery.
            None
        }
    }
}

pub(super) async fn mutation_peer(
    runtime: &CellGroupRuntime,
    transport: &RaftTransport,
    session: &CellRoutingSession,
    attempt: &CellRouteAttempt,
    keys: &VersionKeys,
    domain: &DomainToken,
) -> Result<Option<MemberId>, CellDispatchError> {
    let route = attempt.route();
    let status = match runtime.log().cell_group(&route.history) {
        Ok(_) => {
            runtime
                .routing_status(route.history, session.deadline_ms())
                .await
        }
        Err(crate::raft::storage::LogError::NotFound) => Err(RuntimeError::NotFound),
        Err(error) => return Err(CellDispatchError::Runtime(RuntimeError::Storage(error))),
    };
    match status {
        Ok(status) => {
            let Some(leader) = status.leader else {
                return Ok(None);
            };
            let local = runtime
                .log()
                .cell_group(&route.history)
                .map_err(|e| CellDispatchError::Runtime(RuntimeError::Storage(e)))?
                .identity()
                .member;
            if leader == local.get() {
                return Ok(None);
            }
            return MemberId::new(leader)
                .map(Some)
                .map_err(|_| CellDispatchError::Invalid("mutation leader identity"));
        }
        Err(RuntimeError::NotFound) => (),
        Err(error) => return Err(CellDispatchError::Runtime(error)),
    }

    match transport
        .cell_command_leader(route)
        .map_err(CellDispatchError::Transport)?
    {
        LeaderLookup::Known(member) => return Ok(Some(member)),
        LeaderLookup::Unverified | LeaderLookup::Membership => {
            // Only an unverified/changed configuration needs the trusted member
            // lookup. Elections with usable advice stay on the installed range.
            match transport
                .refresh_managed_router(route.history, session.deadline_ms())
                .await
            {
                Ok(()) | Err(RuntimeError::NotFound) => (),
                Err(error) => return Err(CellDispatchError::Runtime(error)),
            }
        }
        LeaderLookup::Unknown => (),
    }
    transport.note_managed_router(route.history);
    let candidates = match transport.cell_query_router_members(route, session.maximum_attempts()) {
        Ok(members) => members,
        Err(TransportError::UnknownGroup) => {
            match transport
                .refresh_managed_router(route.history, session.deadline_ms())
                .await
            {
                Ok(()) => transport
                    .cell_query_router_members(route, session.maximum_attempts())
                    .map_err(CellDispatchError::Transport)?,
                Err(RuntimeError::NotFound) => return Ok(None),
                Err(error) => return Err(CellDispatchError::Runtime(error)),
            }
        }
        Err(error) => return Err(CellDispatchError::Transport(error)),
    };
    let mut failures = ProbeFailures::default();
    for (index, member) in candidates.iter().enumerate() {
        session
            .validate_attempt(attempt)
            .map_err(CellDispatchError::Routing)?;
        let now = domain.mono_ms().map_err(|_| closed())?;
        let remaining = session.deadline_ms().saturating_sub(now);
        if remaining < 2 {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        // Keep a share of the original deadline for the mutation itself.
        let deadline = now + (remaining / (candidates.len() - index + 1) as u64).max(1);
        match transport
            .probe_cell_command_leader(
                route,
                &attempt.request().cell,
                *member,
                keys.clone(),
                deadline,
            )
            .await
        {
            Ok(true) => return Ok(Some(*member)),
            Ok(false) => failures.no_leader = true,
            Err(TransportError::Overloaded) => failures.overloaded = true,
            Err(
                TransportError::Io(_)
                | TransportError::Timeout
                | TransportError::StalePeer
                | TransportError::UnknownGroup,
            ) => failures.discovery = true,
            Err(error) => return Err(CellDispatchError::Transport(error)),
        }
        if let LeaderLookup::Known(leader) = transport
            .cell_command_leader(route)
            .map_err(CellDispatchError::Transport)?
        {
            return Ok(Some(leader));
        }
    }
    if let Some(reason) = failures.leader_retry() {
        // These are discovery observations before mutation submission. An
        // election or busy peer alone does not establish stale membership/range.
        return Err(CellDispatchError::Execution(ExecutionError::Refused(
            reason,
        )));
    }
    // Every cached candidate failed before submission. Refresh the exact
    // group's discovery so the caller's next original-session attempt can use
    // settled replacement voters. No transmitted mutation is replayed here.
    match transport
        .refresh_managed_router(route.history, session.deadline_ms())
        .await
    {
        Ok(()) | Err(RuntimeError::NotFound) => (),
        Err(error) => return Err(CellDispatchError::Runtime(error)),
    }
    Err(CellDispatchError::Runtime(RuntimeError::NotReady))
}

#[cfg(test)]
#[path = "mutation_route_tests.rs"]
mod tests;
