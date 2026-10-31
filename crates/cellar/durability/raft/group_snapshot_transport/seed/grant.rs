// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;

/// Only a verified TLS response constructs this capability. It remains live
/// through queued physical installation, including cancelled callers.
pub(crate) struct Grant {
    pub(super) local: ReplicaIdentity<GroupHistory>,
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

    pub(crate) fn snapshot(&self) -> &::raft::prelude::Snapshot {
        &self.snapshot
    }

    pub(crate) fn check<S>(
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
            || *self.process.borrow()
            || *self.stop.borrow()
            || !self.domain.mono_ms().is_ok_and(|now| now < self.deadline)
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }

    pub(crate) fn check_log(&self, log: &ReplicaLog) -> Result<(), LogError> {
        let options = log.limits();
        self.check(
            &log.owner(),
            self.local.scope,
            &ReplicaIdentity {
                scope: self.local.scope,
                node: options.node,
                disk: options.disk,
                process: options.process,
                member: self.local.member,
            },
        )
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
        checkpoint::seed_birth(&self.snapshot, &bootstrap.route, &conf)
            .map_err(|e| LogError::Invalid(e.to_string()))?;
        Ok(conf)
    }
}
