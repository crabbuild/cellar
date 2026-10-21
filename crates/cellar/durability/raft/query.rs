//! Request-owned read constraints. A fresh ReadIndex result is correlated to
//! one invocation; a cached commit watermark can never replace that barrier.

use crate::application::consensus::client::Request;
use crate::application::consensus::{
    OperationContract, QueryConsistency, QueryOptions, StaleReadPolicy,
};
use cellar_logic::consensus::{CommandFailure, CommittedVersion, GroupScope, LogIndex, Term};

pub(crate) type ReadId = [u8; 32];

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
    Source,
    Probing,
    StartBarrier,
    CheckingLeader,
    Barrier,
    Image,
    Executing,
}

pub(crate) enum ReadOperation {
    Query {
        contract: OperationContract,
        input: Vec<u8>,
    },
    Seed {
        target: cellar_logic::consensus::ReplicaIdentity,
    },
}

pub(crate) struct PendingRead {
    pub request: Request,
    pub operation: ReadOperation,
    pub minimum: Option<Minimum>,
    pub phase: Phase,
    pub source_deadline: Option<u64>,
    pub retry_at: u64,
    pub leader: Option<(Term, Option<u64>, bool)>,
    pub checking_leader: bool,
    pub barrier_attempt: u64,
    pub barrier_retry_at: u64,
    pub leader_check: u64,
}

impl PendingRead {
    pub fn new(
        request: Request,
        contract: OperationContract,
        input: Vec<u8>,
        options: &QueryOptions,
        minimum: Option<CommittedVersion>,
    ) -> Self {
        Self::with_options(
            request,
            ReadOperation::Query { contract, input },
            options,
            minimum,
        )
    }

    pub fn seed(request: Request, target: cellar_logic::consensus::ReplicaIdentity) -> Self {
        Self::with_options(
            request,
            ReadOperation::Seed { target },
            &QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
            None,
        )
    }

    fn with_options(
        request: Request,
        operation: ReadOperation,
        options: &QueryOptions,
        minimum: Option<CommittedVersion>,
    ) -> Self {
        let (phase, minimum) = match options.consistency {
            QueryConsistency::Linearizable => (Phase::StartBarrier, None),
            QueryConsistency::AtLeast(_) => (Phase::Image, minimum.map(Minimum::Version)),
            QueryConsistency::Stale if options.stale_policy == StaleReadPolicy::RecentSource => {
                (Phase::Source, Some(Minimum::Any))
            }
            QueryConsistency::Stale => (Phase::Image, Some(Minimum::Any)),
        };
        Self {
            request,
            operation,
            minimum,
            phase,
            source_deadline: None,
            retry_at: 0,
            leader: None,
            checking_leader: false,
            barrier_attempt: 0,
            barrier_retry_at: 0,
            leader_check: 0,
        }
    }

    pub fn expired(&self) -> bool {
        self.request.control.cancelled()
            || self
                .source_deadline
                .is_some_and(|deadline| !self.request.control.before(deadline))
    }
}
