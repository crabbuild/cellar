// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Deterministic startup destinations, never election or serving authority.
use ::raft::prelude::{ConfState, HardState};
use cellar_logic::consensus::{LogPosition, MemberId};

pub(super) fn candidate(
    hard: &HardState,
    configuration: &ConfState,
    applied: LogPosition,
) -> Option<MemberId> {
    if hard.term == 0 {
        return None;
    }
    let member = if hard.vote != 0 {
        hard.vote
    } else if hard.term == 1
        && hard.commit == 1
        && applied.index.get() == 1
        && applied.term.get() == 1
        && configuration.voters_outgoing.is_empty()
    {
        // Exactly cache::genesis, before any ordinary election. All members
        // choose one voter independently of configuration vector ordering.
        *configuration.voters.iter().min()?
    } else {
        return None;
    };
    if !configuration
        .voters
        .iter()
        .chain(&configuration.voters_outgoing)
        .any(|voter| *voter == member)
    {
        return None;
    }
    MemberId::new(member).ok()
}

#[cfg(test)]
#[path = "startup_tests.rs"]
mod tests;
