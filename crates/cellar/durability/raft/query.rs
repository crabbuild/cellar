// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Internal SQL minimum checks and the remaining seed barrier owner. Business
//! reads are admitted only through the shared physical-group controller.

use crate::application::consensus::client::Request;
use cellar_logic::consensus::{CommandFailure, CommittedVersion, GroupScope, LogIndex, Term};

pub(crate) type ReadId = [u8; 32];

/// The one RecentSource policy used by query admission and readiness reporting.
/// Missing/future observations, overflow and the exact expiry boundary refuse.
pub(crate) fn recent_source_deadline(observed: Option<u64>, now: u64, timeout: u64) -> Option<u64> {
    observed
        .filter(|at| *at <= now)
        .and_then(|at| at.checked_add(timeout))
        .filter(|deadline| now < *deadline)
}

#[derive(Clone)]
pub(crate) enum Minimum {
    Any,
    Version(CommittedVersion),
    ReadIndex(LogIndex),
}

impl Minimum {
    pub fn satisfied(
        &self,
        version: &CommittedVersion,
        scope: &GroupScope,
    ) -> Result<bool, CommandFailure> {
        if version.scope != *scope {
            return Err(CommandFailure::WrongIncarnation);
        }
        match self {
            Self::Any => Ok(true),
            Self::Version(minimum) => version
                .covers(minimum)
                .map_err(|_| CommandFailure::WrongIncarnation),
            Self::ReadIndex(index) => Ok(version.position.index >= *index),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    StartBarrier,
    CheckingLeader,
    Barrier,
    Image,
    Executing,
}

pub(crate) struct PendingRead {
    pub request: Request,
    pub target: cellar_logic::consensus::ReplicaIdentity,
    pub kind: crate::raft::transport::seeds::SeedKind,
    pub charge: std::sync::Arc<crate::raft::budget::Permit>,
    pub minimum: Option<LogIndex>,
    pub phase: Phase,
    pub retry_at: u64,
    pub leader: Option<(Term, Option<u64>, bool)>,
    pub checking_leader: bool,
    pub barrier_attempt: u64,
    pub barrier_retry_at: u64,
    pub leader_check: u64,
    pub archive_wait: Option<crate::raft::performance::Timer>,
}

impl PendingRead {
    pub fn seed(
        request: Request,
        target: cellar_logic::consensus::ReplicaIdentity,
        kind: crate::raft::transport::seeds::SeedKind,
        charge: std::sync::Arc<crate::raft::budget::Permit>,
    ) -> Self {
        Self {
            request,
            target,
            kind,
            charge,
            minimum: None,
            phase: Phase::StartBarrier,
            retry_at: 0,
            leader: None,
            checking_leader: false,
            barrier_attempt: 0,
            barrier_retry_at: 0,
            leader_check: 0,
            archive_wait: None,
        }
    }

    pub fn expired(&self) -> bool {
        self.request.control.cancelled()
    }
}
