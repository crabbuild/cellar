// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One restart-readiness contract, evaluated by the serialized physical owner.
//! Replication observations are inputs; only the owner can admit a transfer.
use super::completion::{
    LeadershipTransfer, ReplicationStatus, RestartLeadershipCheck, RuntimeError,
};
use super::storage::ReplicaLog;
use ::raft::prelude::ConfState;

#[cfg(test)]
#[path = "restart_leadership_tests.rs"]
mod tests;

pub(crate) fn check_gate(
    log: &ReplicaLog,
    now: u64,
    check: &RestartLeadershipCheck,
) -> Result<(), RuntimeError> {
    if now >= check.deadline {
        return Err(RuntimeError::Closed);
    }
    if let Some((_, selector)) = &check.transfer {
        if log.lifecycle_guard()?.as_ref().and_then(|g| g.quiescing()) != Some(selector) {
            return Err(RuntimeError::NotReady);
        }
    }
    Ok(())
}

pub(crate) fn check_progress<S>(
    progress: &ReplicationStatus<S>,
    configuration: &ConfState,
    check: &RestartLeadershipCheck,
    pending_configuration: bool,
    transferring: bool,
) -> Result<Option<LeadershipTransfer>, RuntimeError> {
    let voters = &configuration.voters;
    if check.through.get() == 0
        || pending_configuration
        || transferring
        || progress.term != check.expected_term
        || progress.membership_revision != check.expected_revision
        || progress.committed < check.through
        || voters.len() != 3
        || !configuration.voters_outgoing.is_empty()
        || !configuration.learners.is_empty()
        || !configuration.learners_next.is_empty()
        || configuration.auto_leave
        || check.survivors[0] == check.survivors[1]
        || check
            .survivors
            .iter()
            .any(|member| !voters.contains(&member.get()))
        || check.survivors.iter().any(|member| {
            progress
                .members
                .iter()
                .find(|p| p.member == *member)
                .is_none_or(|p| {
                    !p.voter
                        || p.matched < check.through
                        || (*member != progress.replica.member && !p.recent_active)
                })
        })
    {
        return Err(RuntimeError::NotReady);
    }
    if check.all_voters
        && voters.iter().any(|id| {
            progress
                .members
                .iter()
                .find(|p| p.member.get() == *id)
                .is_none_or(|p| {
                    !p.voter
                        || p.matched < check.through
                        || (*id != progress.replica.member.get() && !p.recent_active)
                })
        })
    {
        return Err(RuntimeError::NotReady);
    }
    let local = progress.replica.member;
    match &check.transfer {
        Some((target, selector)) => {
            if check.survivors.contains(&local)
                || !check.survivors.contains(target)
                || selector.boot.node != progress.replica.node.bytes()
                || selector.boot.disk != progress.replica.disk.bytes()
                || selector.boot.process != progress.replica.process.bytes()
            {
                return Err(RuntimeError::NotReady);
            }
        }
        None if !check.survivors.contains(&local) => return Err(RuntimeError::NotReady),
        None => (),
    }
    Ok(check
        .transfer
        .as_ref()
        .map(|(target, _)| LeadershipTransfer {
            target: *target,
            expected_term: check.expected_term,
            expected_revision: check.expected_revision,
        }))
}
