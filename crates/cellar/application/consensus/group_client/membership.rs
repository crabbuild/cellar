// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Administrative commands address a physical group. Durable apply and a fresh
//! quorum barrier, rather than proposal admission, establish their result.
use super::*;
use crate::raft::{
    completion::RuntimeError,
    fleet_catalog::{FleetGroupAuthorization, FleetPermitAuthorization},
    member_state::FleetPermitPhase,
};
use cellar_logic::consensus::LogIndex;

/// Refused means this invocation submitted no configuration change. It does not
/// resolve an earlier lost response. Unconfirmed may follow actual commitment;
/// reconcile the same authorization instead of inventing another operation.
#[derive(Clone, Debug)]
pub enum CellGroupControlError {
    Refused(RuntimeError),
    Unconfirmed(RuntimeError),
}

impl std::fmt::Display for CellGroupControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(error) => write!(f, "group control refused: {error}"),
            Self::Unconfirmed(error) => write!(f, "group control outcome unconfirmed: {error}"),
        }
    }
}

impl std::error::Error for CellGroupControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Refused(error) | Self::Unconfirmed(error) => error,
        })
    }
}

pub(super) enum Change {
    Enroll {
        authorization: FleetGroupAuthorization,
        expected: LogIndex,
    },
    Fence(Box<FleetPermitAuthorization>),
    Repair(Box<super::repair::Change>),
}

impl Change {
    fn satisfied(&self, observed: &CellGroupQuorumMembership) -> Result<bool, RuntimeError> {
        let status = observed.membership().fleet_status();
        match self {
            Self::Enroll { authorization, .. } => {
                if authorization.history_digests()
                    != (
                        observed.root().route().birth_digest,
                        observed.root().route().schema_digest,
                    )
                    || !authorization.matches_replica(observed.replica(), observed.membership())
                {
                    return Err(RuntimeError::Invalid("fleet enrollment authority".into()));
                }
                let Some(status) = status else {
                    return Ok(false);
                };
                if &status.binding != authorization.binding() {
                    return Err(RuntimeError::Invalid(
                        "group enrolled in another fleet".into(),
                    ));
                }
                Ok(true)
            }
            Self::Fence(authorization) => {
                let status =
                    status.ok_or_else(|| RuntimeError::Invalid("group is not enrolled".into()))?;
                let permit = authorization.permit();
                if &status.binding != permit.binding() {
                    return Err(RuntimeError::Invalid("foreign group fence".into()));
                }
                if status.consumed_sequence < permit.sequence() {
                    return Ok(false);
                }
                if status.consumed_sequence == permit.sequence()
                    && status.permit.as_ref() == Some(permit)
                    && status.phase == Some(FleetPermitPhase::Fenced)
                {
                    return Ok(true);
                }
                // A later sequence or another consumed phase cannot prove that
                // this exact cancellation fence committed, nor release its debt.
                Err(RuntimeError::StaleCompletion)
            }
            Self::Repair(change) => change.satisfied(observed),
        }
    }

    fn prepare(
        &self,
        observed: &CellGroupQuorumMembership,
    ) -> Result<CellGroupMembershipChange, RuntimeError> {
        let route = observed.root().route();
        let membership = observed.membership();
        let configuration = observed.configuration();
        Ok(match self {
            Self::Enroll {
                authorization,
                expected,
            } => {
                if membership.revision() != *expected {
                    return Err(RuntimeError::StaleCompletion);
                }
                CellGroupMembershipChange::fleet_enrollment(
                    route,
                    membership,
                    configuration,
                    authorization,
                )?
            }
            Self::Fence(authorization) => CellGroupMembershipChange::fleet_fence(
                route,
                membership,
                configuration,
                authorization,
            )?,
            Self::Repair(change) => change.prepare(observed)?,
        })
    }
}

impl RaftGroupHandle {
    pub(super) fn control_deadline(&self, timeout: Duration) -> Result<u64, RuntimeError> {
        if self.owner.is_closed() {
            return Err(RuntimeError::Closed);
        }
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(RuntimeError::Invalid("group control timeout".into()));
        }
        self.owner
            .handle
            .domain
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?
            .checked_add(timeout.as_millis() as u64)
            .ok_or(RuntimeError::Overloaded)
    }

    /// A fresh quorum observation behind a durable physical apply floor. Time
    /// before polling consumes the original invocation deadline.
    pub fn quorum_membership_after(
        &self,
        minimum: LogIndex,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, RuntimeError>> + Send + 'static
    {
        let deadline = self.control_deadline(timeout);
        let owner = self.owner.clone();
        let history = self.history;
        async move {
            owner
                .handle
                .core
                .runtime
                .quorum_membership_after(history, minimum, deadline?)
                .await
        }
    }

    /// The exact rooted fleet binding must already be committed by this group.
    /// The result contains no cell alias, SQL image or physical-release lease.
    pub fn observe_fleet_membership(
        &self,
        authorization: FleetGroupAuthorization,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, RuntimeError>> + Send + 'static
    {
        let deadline = self.control_deadline(timeout);
        let owner = self.owner.clone();
        let history = self.history;
        async move {
            if !authorization.matches_scope(&history) {
                return Err(RuntimeError::Invalid("fleet observation history".into()));
            }
            let observed = owner
                .handle
                .core
                .runtime
                .quorum_membership(history, deadline?)
                .await?;
            let status = observed
                .membership()
                .fleet_status()
                .ok_or_else(|| RuntimeError::Invalid("group is not enrolled".into()))?;
            if &status.binding != authorization.binding()
                || authorization.history_digests()
                    != (
                        observed.root().route().birth_digest,
                        observed.root().route().schema_digest,
                    )
                || !authorization.matches_replica(observed.replica(), observed.membership())
            {
                return Err(RuntimeError::Invalid("fleet observation authority".into()));
            }
            Ok(observed)
        }
    }

    /// Commit this physical history's exact fleet guard. An exact retry can
    /// return a fresh observation of the already committed guard.
    pub fn enroll_fleet(
        &self,
        authorization: FleetGroupAuthorization,
        expected: LogIndex,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, CellGroupControlError>> + Send + 'static
    {
        let deadline = self.control_deadline(timeout);
        let owner = self.clone();
        async move {
            if !authorization.matches_scope(&owner.history) {
                return Err(CellGroupControlError::Refused(RuntimeError::Invalid(
                    "fleet enrollment history".into(),
                )));
            }
            owner
                .change_membership(
                    Change::Enroll {
                        authorization,
                        expected,
                    },
                    deadline.map_err(CellGroupControlError::Refused)?,
                )
                .await
        }
    }

    /// Commit the exact unused permit's cancellation fence. Admission alone
    /// never supplies a fence receipt or permits reservation release.
    pub fn fence_fleet_permit(
        &self,
        authorization: FleetPermitAuthorization,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellGroupQuorumMembership, CellGroupControlError>> + Send + 'static
    {
        let deadline = self.control_deadline(timeout);
        let owner = self.clone();
        async move {
            if !authorization.matches_scope(&owner.history) {
                return Err(CellGroupControlError::Refused(RuntimeError::Invalid(
                    "fleet fence history".into(),
                )));
            }
            owner
                .change_membership(
                    Change::Fence(Box::new(authorization)),
                    deadline.map_err(CellGroupControlError::Refused)?,
                )
                .await
        }
    }

    pub(super) async fn change_membership(
        &self,
        change: Change,
        deadline: u64,
    ) -> Result<CellGroupQuorumMembership, CellGroupControlError> {
        use CellGroupControlError::{Refused, Unconfirmed};
        let core = &self.owner.handle.core;
        let observed = core
            .runtime
            .quorum_membership(self.history, deadline)
            .await
            .map_err(Refused)?;
        if change.satisfied(&observed).map_err(Refused)? {
            return Ok(observed);
        }
        if let Change::Repair(repair) = &change {
            repair
                .check_placement(&core.transport, &observed)
                .map_err(Refused)?;
        }
        let proposal = change.prepare(&observed).map_err(Refused)?;
        drop(observed);
        // Runtime owns an accepted configuration independently of this waiter.
        // A timed-out send/response is conservatively uncertain, never aborted.
        let admitted = super::drain::within(
            &self.owner.handle.domain,
            deadline,
            core.runtime.configure(self.history, proposal),
        )
        .await
        .map_err(Unconfirmed)?;
        let observed = core
            .runtime
            .quorum_membership_after(self.history, admitted.position.index, deadline)
            .await
            .map_err(Unconfirmed)?;
        if !change.satisfied(&observed).map_err(Unconfirmed)? {
            return Err(Unconfirmed(RuntimeError::StaleCompletion));
        }
        Ok(observed)
    }
}
