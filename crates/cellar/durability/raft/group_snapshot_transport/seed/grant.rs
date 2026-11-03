// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::raft::{
    fleet_catalog::FleetPermitAuthorization,
    fleet_stop::{BootBinding, Guard},
    member_state::FleetGroupBinding,
};

pub(super) struct Managed {
    pub authorization: FleetPermitAuthorization,
    pub boot: BootBinding,
}

pub(super) fn check_boot(guard: Option<&Guard>, expected: &BootBinding) -> Result<(), LogError> {
    if guard.is_none_or(|guard| !guard.admits_group_start(expected)) {
        return Err(LogError::WrongIdentity);
    }
    Ok(())
}

/// Only a verified TLS response constructs this capability. It remains live
/// through queued physical installation, including cancelled callers.
pub(crate) struct Grant {
    pub(super) managed: Option<Managed>,
    pub(super) local: ReplicaIdentity<GroupHistory>,
    pub(super) source: ReplicaIdentity<GroupHistory>,
    pub(super) root: GroupManifest,
    pub(super) snapshot: ::raft::prelude::Snapshot,
    pub(super) deadline: u64,
    pub(super) owner: Arc<()>,
    pub(super) domain: DomainToken,
    pub(super) process: watch::Receiver<bool>,
    pub(super) stop: watch::Receiver<bool>,
    pub(super) _metadata: Permit,
    // Erase the transport graph while retaining its joined task ownership.
    pub(super) _request: Arc<dyn Send + Sync>,
}

impl Grant {
    pub(crate) fn local(&self) -> &ReplicaIdentity<GroupHistory> {
        &self.local
    }

    pub(crate) fn source(&self) -> &ReplicaIdentity<GroupHistory> {
        &self.source
    }

    pub(crate) fn snapshot(&self) -> &::raft::prelude::Snapshot {
        &self.snapshot
    }

    pub(crate) fn check<S>(
        &self,
        owner: &Arc<()>,
        history: GroupHistory,
        local: &ReplicaIdentity<S>,
    ) -> Result<(), LogError> {
        self.check_target(owner, history, local)?;
        if *self.process.borrow() {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    // Source liveness authorizes submission. After actual engine sync succeeds,
    // only the destination and original operation bounds govern local completion.
    fn check_target<S>(
        &self,
        owner: &Arc<()>,
        history: GroupHistory,
        local: &ReplicaIdentity<S>,
    ) -> Result<(), LogError> {
        if !Arc::ptr_eq(owner, &self.owner)
            || history != self.local.scope
            || local.member != self.local.member
            || local.node != self.local.node
            || local.disk != self.local.disk
            || local.process != self.local.process
            || *self.stop.borrow()
            || !self.domain.mono_ms().is_ok_and(|now| now < self.deadline)
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    pub(crate) fn check_lifecycle(&self, guard: Option<&Guard>) -> Result<(), LogError> {
        match &self.managed {
            Some(managed) => check_boot(guard, &managed.boot),
            None => Ok(()),
        }
    }

    pub(crate) fn initial_membership(
        &self,
        conf: &::raft::prelude::ConfState,
    ) -> Result<crate::raft::member_state::GroupMembership, LogError> {
        checkpoint::seed_initial_membership(&self.snapshot, conf)
            .map_err(|e| LogError::Invalid(e.to_string()))
    }

    pub(crate) fn deadline(&self) -> u64 {
        self.deadline
    }

    pub(crate) fn managed_binding(&self) -> Option<&FleetGroupBinding> {
        self.managed
            .as_ref()
            .map(|m| m.authorization.permit().binding())
    }

    pub(crate) fn check_authorization(
        &self,
        authorization: &FleetPermitAuthorization,
    ) -> Result<(), LogError> {
        if self.managed.as_ref().is_none_or(|managed| {
            managed.authorization.permit() != authorization.permit()
                || authorization.minimum_trust_revision()
                    < managed.authorization.minimum_trust_revision()
                || authorization.target_boot() != Some(&managed.boot)
        }) {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    pub(crate) fn check_log(&self, log: &ReplicaLog) -> Result<(), LogError> {
        self.check_target_log(log)?;
        if *self.process.borrow() {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    pub(super) fn check_target_log(&self, log: &ReplicaLog) -> Result<(), LogError> {
        let options = log.limits();
        self.check_target(
            &log.owner(),
            self.local.scope,
            &ReplicaIdentity {
                scope: self.local.scope,
                node: options.node,
                disk: options.disk,
                process: options.process,
                member: self.local.member,
            },
        )?;
        self.check_lifecycle(log.lifecycle_guard()?.as_ref())
    }

    pub(crate) fn initial_configuration(
        &self,
        bootstrap: &CellGroupBootstrap,
    ) -> Result<::raft::prelude::ConfState, LogError> {
        let conf = ::raft::prelude::ConfState {
            voters: bootstrap.voters.iter().map(|id| id.get()).collect(),
            learners: bootstrap.learners.iter().map(|id| id.get()).collect(),
            ..Default::default()
        };
        crate::raft::cache::validate_conf(&conf)?;
        crate::raft::member_state::GroupMembership::initial(&conf).validate(&conf, 1)?;
        if bootstrap.member != self.local.member
            || &bootstrap.route != self.root.route()
            || crate::raft::member_state::members(&conf).contains(&bootstrap.member.get())
        {
            return Err(LogError::WrongIdentity);
        }
        checkpoint::seed_profile(
            &self.snapshot,
            self.managed.as_ref().map(|m| m.authorization.permit()),
        )
        .map_err(|e| LogError::Invalid(e.to_string()))?;
        checkpoint::seed_birth(&self.snapshot, &bootstrap.route, &conf)
            .map_err(|e| LogError::Invalid(e.to_string()))?;
        Ok(conf)
    }
}

#[cfg(test)]
#[path = "grant_tests.rs"]
mod tests;
