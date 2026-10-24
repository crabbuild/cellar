//! Managed configuration calls retain opaque catalog authorizations.
use super::*;
use crate::raft::fleet_catalog::{FleetGroupAuthorization, FleetPermitAuthorization};
use crate::raft::member_state::{FleetMembershipStatus, RepairPlan, RepairStatus, RepairStep};

pub(crate) enum FleetRequest {
    Enroll {
        authorization: FleetGroupAuthorization,
        expected: LogIndex,
    },
    Fence {
        authorization: FleetPermitAuthorization,
    },
    Begin {
        authorization: FleetPermitAuthorization,
        plan: RepairPlan,
    },
    Advance {
        authorization: FleetPermitAuthorization,
        step: RepairStep,
        expected: LogIndex,
    },
}

impl FleetRequest {
    pub(super) fn matches_scope(&self, scope: &GroupScope) -> bool {
        match self {
            Self::Enroll {
                authorization,
                expected,
            } => authorization.matches_scope(scope) && expected.get() > 0,
            Self::Fence { authorization } => authorization.matches_scope(scope),
            Self::Begin {
                authorization,
                plan,
            } => {
                authorization.matches_scope(scope)
                    && authorization.permit().check_plan(plan).is_ok()
            }
            Self::Advance {
                authorization,
                expected,
                ..
            } => authorization.matches_scope(scope) && expected.get() > 0,
        }
    }
}

impl RaftCellHandle {
    /// Observe synced permit consumption. This is not a fresh quorum proof.
    pub fn fleet_membership_status(
        &self,
    ) -> impl Future<Output = Result<Option<FleetMembershipStatus>, CommandFailure>> + Send + 'static
    {
        let call = self.call(Kind::FleetStatus, Duration::from_secs(5));
        async move {
            match call.await? {
                Response::Fleet(status) => Ok(status.map(|status| *status)),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Commit this history's fleet binding at the exact configuration revision.
    /// Once committed, ordinary membership mutation APIs refuse this group.
    pub fn enroll_fleet(
        &self,
        authorization: FleetGroupAuthorization,
        expected: LogIndex,
        timeout: Duration,
    ) -> impl Future<Output = Result<FleetMembershipStatus, CommandFailure>> + Send + 'static {
        let call = self.call(
            Kind::Fleet(Box::new(FleetRequest::Enroll {
                authorization,
                expected,
            })),
            timeout,
        );
        async move {
            match call.await? {
                Response::Fleet(Some(status)) => Ok(*status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Commit a cancellation fence for an unused sequence. Lost replies require
    /// reconciliation of the same authorization, never a new cancellation ID.
    pub fn fence_fleet_permit(
        &self,
        authorization: FleetPermitAuthorization,
        timeout: Duration,
    ) -> impl Future<Output = Result<FleetMembershipStatus, CommandFailure>> + Send + 'static {
        let call = self.call(
            Kind::Fleet(Box::new(FleetRequest::Fence { authorization })),
            timeout,
        );
        async move {
            match call.await? {
                Response::Fleet(Some(status)) => Ok(*status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Consume the next catalog permit with the committed learner change.
    /// An unavailable response can follow commitment; retry the same permit.
    pub fn begin_managed_repair(
        &self,
        authorization: FleetPermitAuthorization,
        plan: RepairPlan,
        timeout: Duration,
    ) -> impl Future<Output = Result<RepairStatus, CommandFailure>> + Send + 'static {
        let call = self.call(
            Kind::Fleet(Box::new(FleetRequest::Begin {
                authorization,
                plan,
            })),
            timeout,
        );
        async move {
            match call.await? {
                Response::Repair(Some(status)) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// Advance the exact consumed permit. Joint completion still requires both
    /// majorities, and an entered joint transition cannot be aborted.
    pub fn advance_managed_repair(
        &self,
        authorization: FleetPermitAuthorization,
        step: RepairStep,
        expected: LogIndex,
        timeout: Duration,
    ) -> impl Future<Output = Result<RepairStatus, CommandFailure>> + Send + 'static {
        let call = self.call(
            Kind::Fleet(Box::new(FleetRequest::Advance {
                authorization,
                step,
                expected,
            })),
            timeout,
        );
        async move {
            match call.await? {
                Response::Repair(Some(status)) => Ok(status),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }

    /// A managed seed still needs its existing fresh-quorum grant and verified
    /// physical initialization. The catalog authorization alone cannot seed.
    pub fn activate_managed_learner(
        &self,
        authorization: FleetPermitAuthorization,
        activation: LearnerActivation,
    ) -> impl Future<Output = Result<LearnerReceipt, CommandFailure>> + Send + 'static {
        let timeout = activation.timeout;
        let call = self.call(
            Kind::ActivateManaged {
                authorization: Box::new(authorization),
                activation,
            },
            timeout,
        );
        async move {
            match call.await? {
                Response::Learner(receipt) => Ok(receipt),
                _ => Err(CommandFailure::CorruptState),
            }
        }
    }
}
