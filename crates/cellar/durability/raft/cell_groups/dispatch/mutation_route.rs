// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select one destination for a mutation before submission.
use super::*;
use crate::raft::group_host::RouterRefreshOutcome;
use crate::raft::transport::{LeaderLookup, LeaderProbeOutcome, RaftTransport, TransportError};
use cellar_logic::cell_group::GroupHistory;
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

#[derive(Debug)]
pub(in crate::raft::cell_groups) enum MutationSelectionError {
    Retry(CellDispatchError),
    StalePlacement,
    Terminal(CellDispatchError),
}

impl MutationSelectionError {
    pub(in crate::raft::cell_groups) fn into_dispatch(self) -> CellDispatchError {
        match self {
            Self::Retry(error) | Self::Terminal(error) => error,
            Self::StalePlacement => CellDispatchError::Runtime(RuntimeError::NotFound),
        }
    }
}

impl From<CellDispatchError> for MutationSelectionError {
    fn from(error: CellDispatchError) -> Self {
        Self::Terminal(error)
    }
}

fn retry(reason: CommandFailure) -> MutationSelectionError {
    MutationSelectionError::Retry(CellDispatchError::Execution(ExecutionError::Refused(
        reason,
    )))
}

async fn refresh_membership(
    transport: &RaftTransport,
    history: GroupHistory,
    deadline: u64,
) -> Result<bool, MutationSelectionError> {
    match transport.refresh_cell_router(history, deadline).await {
        Ok(RouterRefreshOutcome::Current) => Ok(true),
        Ok(RouterRefreshOutcome::StalePlacement) => Err(MutationSelectionError::StalePlacement),
        // Unmanaged hosts may lack the joined discovery service.
        Err(RuntimeError::NotFound) => Ok(false),
        Err(RuntimeError::NotReady) => Err(retry(CommandFailure::Unavailable)),
        Err(RuntimeError::Overloaded) => Err(retry(CommandFailure::Overloaded)),
        Err(error) => Err(CellDispatchError::Runtime(error).into()),
    }
}

pub(super) async fn mutation_peer(
    runtime: &CellGroupRuntime,
    transport: &RaftTransport,
    session: &CellRoutingSession,
    attempt: &CellRouteAttempt,
    keys: &VersionKeys,
    _domain: &DomainToken,
) -> Result<Option<MemberId>, MutationSelectionError> {
    let route = attempt.route();
    let status = match runtime.log().cell_group(&route.history) {
        Ok(group) => {
            let marker = group
                .group_manifest()
                .map_err(|error| CellDispatchError::Runtime(RuntimeError::Storage(error)))?;
            if marker
                .as_ref()
                .is_some_and(|marker| marker.source_seal().is_some() || marker.route() != route)
            {
                return Err(MutationSelectionError::StalePlacement);
            }
            runtime
                .routing_status(route.history, session.deadline_ms())
                .await
        }
        Err(crate::raft::storage::LogError::NotFound) => Err(RuntimeError::NotFound),
        Err(error) => return Err(CellDispatchError::Runtime(RuntimeError::Storage(error)).into()),
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
                .map_err(|_| CellDispatchError::Invalid("mutation leader identity").into());
        }
        Err(RuntimeError::NotFound) => (),
        Err(RuntimeError::NotReady) => return Err(retry(CommandFailure::Unavailable)),
        Err(RuntimeError::Overloaded) => return Err(retry(CommandFailure::Overloaded)),
        Err(error) => return Err(CellDispatchError::Runtime(error).into()),
    }

    let mut refreshed = match transport.cell_leader(route) {
        Ok(LeaderLookup::Known(member)) => return Ok(Some(member)),
        Ok(LeaderLookup::Unverified | LeaderLookup::Membership)
        | Err(TransportError::StalePeer) => {
            Some(refresh_membership(transport, route.history, session.deadline_ms()).await?)
        }
        Ok(LeaderLookup::Unknown) => None,
        Err(error) => return Err(CellDispatchError::Transport(error).into()),
    };
    transport.note_managed_router(route.history);
    let candidates = match transport.cell_query_router_members(route, session.maximum_attempts()) {
        Ok(members) => members,
        Err(TransportError::UnknownGroup) => {
            if refreshed.is_none() {
                refreshed = Some(
                    refresh_membership(transport, route.history, session.deadline_ms()).await?,
                );
            }
            if refreshed == Some(false) {
                return Ok(None);
            }
            match transport.cell_query_router_members(route, session.maximum_attempts()) {
                Ok(members) => members,
                Err(TransportError::UnknownGroup) => {
                    return Err(retry(CommandFailure::Unavailable))
                }
                Err(error) => return Err(CellDispatchError::Transport(error).into()),
            }
        }
        Err(error) => return Err(CellDispatchError::Transport(error).into()),
    };
    session
        .validate_attempt(attempt)
        .map_err(CellDispatchError::Routing)?;
    let discovered = transport
        .discover_cell_command_leader(
            route,
            &attempt.request().cell,
            keys.clone(),
            candidates.len(),
            session.deadline_ms(),
        )
        .await;
    session
        .validate_attempt(attempt)
        .map_err(CellDispatchError::Routing)?;
    let outcome = match discovered {
        Ok(outcome) => outcome,
        Err(TransportError::StalePeer | TransportError::UnknownGroup) => {
            LeaderProbeOutcome::Membership
        }
        Err(TransportError::Timeout | TransportError::Io(_)) => {
            return Err(retry(CommandFailure::Unavailable));
        }
        Err(TransportError::Overloaded) => return Err(retry(CommandFailure::Overloaded)),
        Err(error) => return Err(CellDispatchError::Transport(error).into()),
    };
    match outcome {
        LeaderProbeOutcome::Leader(member) => Ok(Some(member)),
        LeaderProbeOutcome::Retry(reason) => Err(retry(reason)),
        LeaderProbeOutcome::Membership => {
            if refreshed.is_none() {
                refresh_membership(transport, route.history, session.deadline_ms()).await?;
            }
            Err(retry(CommandFailure::Unavailable))
        }
    }
}

#[cfg(test)]
#[path = "mutation_route_tests.rs"]
mod tests;
