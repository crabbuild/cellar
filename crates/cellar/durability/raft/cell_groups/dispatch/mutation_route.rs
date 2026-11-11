// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select one destination for a mutation before submission.
use super::*;
use crate::raft::group_host::RouterRefreshOutcome;
use crate::raft::transport::{LeaderLookup, RaftTransport, TransportError};
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

#[derive(Default)]
struct ProbeFailures {
    membership: bool,
    unreachable: bool,
    no_leader: bool,
    overloaded: bool,
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

#[derive(Debug, PartialEq, Eq)]
enum ProbeRepair {
    Membership,
    Retry(CommandFailure),
}

impl ProbeFailures {
    fn observe(&mut self, error: TransportError) -> Result<(), MutationSelectionError> {
        match error {
            TransportError::StalePeer | TransportError::UnknownGroup => self.membership = true,
            TransportError::Io(_) | TransportError::Timeout => self.unreachable = true,
            TransportError::Overloaded => self.overloaded = true,
            error => return Err(CellDispatchError::Transport(error).into()),
        }
        Ok(())
    }

    fn repair(&self) -> ProbeRepair {
        if self.membership {
            ProbeRepair::Membership
        } else if self.no_leader {
            ProbeRepair::Retry(CommandFailure::NotLeader { leader: None })
        } else if self.unreachable {
            ProbeRepair::Retry(CommandFailure::Unavailable)
        } else if self.overloaded {
            ProbeRepair::Retry(CommandFailure::Overloaded)
        } else {
            // An empty/unverified member set still needs actual discovery.
            ProbeRepair::Membership
        }
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
    domain: &DomainToken,
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
    let mut failures = ProbeFailures::default();
    for (index, member) in candidates.iter().enumerate() {
        session
            .validate_attempt(attempt)
            .map_err(CellDispatchError::Routing)?;
        let now = domain.mono_ms().map_err(|_| closed())?;
        let remaining = session.deadline_ms().saturating_sub(now);
        if remaining < 2 {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded).into());
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
            Err(error) => failures.observe(error)?,
        }
        match transport.cell_leader(route) {
            Ok(LeaderLookup::Known(leader)) => return Ok(Some(leader)),
            Ok(LeaderLookup::Membership | LeaderLookup::Unverified)
            | Err(TransportError::StalePeer) => failures.membership = true,
            Ok(LeaderLookup::Unknown) => (),
            Err(error) => return Err(CellDispatchError::Transport(error).into()),
        }
    }
    match failures.repair() {
        ProbeRepair::Retry(reason) => Err(retry(reason)),
        ProbeRepair::Membership => {
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
