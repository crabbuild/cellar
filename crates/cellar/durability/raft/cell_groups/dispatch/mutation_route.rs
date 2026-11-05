// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select one destination for a mutation before submission.
use super::*;
use crate::raft::transport::{RaftTransport, TransportError};
use cellar_logic::consensus::MemberId;

pub(super) async fn mutation_peer(
    runtime: &CellGroupRuntime,
    transport: &RaftTransport,
    session: &CellRoutingSession,
    attempt: &CellRouteAttempt,
    keys: &VersionKeys,
    domain: &DomainToken,
) -> Result<Option<MemberId>, CellDispatchError> {
    let route = attempt.route();
    let timer = domain
        .sleep_until(session.deadline_ms())
        .map_err(|_| closed())?;
    let status = crate::asyncrt::select! {
        status = runtime.status(route.history) => status,
        _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
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

    let candidates = match transport.cell_query_router_members(route, session.maximum_attempts()) {
        Ok(members) => members,
        Err(TransportError::UnknownGroup) => return Ok(None),
        Err(error) => return Err(CellDispatchError::Transport(error)),
    };
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
            Ok(false) => (),
            Err(
                TransportError::Io(_)
                | TransportError::Timeout
                | TransportError::StalePeer
                | TransportError::UnknownGroup
                | TransportError::Overloaded,
            ) => (),
            Err(error) => return Err(CellDispatchError::Transport(error)),
        }
    }
    Err(CellDispatchError::Runtime(RuntimeError::NotReady))
}
