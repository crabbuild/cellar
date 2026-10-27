// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Leadership movement is admitted by the same serialized owner as proposals.
use super::*;
#[cfg(test)]
#[path = "replica_leadership_tests.rs"]
mod tests;

impl Replica {
    pub(super) fn replication_status(&self) -> Result<ReplicationStatus, RuntimeError> {
        if self.node.raft.state != StateRole::Leader {
            return Err(RuntimeError::NotLeader);
        }
        let cache = self.log.read()?;
        let mut members = self
            .node
            .raft
            .prs()
            .iter()
            .map(|(&id, progress)| {
                Ok(MemberProgress {
                    member: cellar_logic::consensus::MemberId::new(id)
                        .map_err(|error| RuntimeError::Protocol(error.to_string()))?,
                    matched: LogIndex::new(progress.matched),
                    voter: cache.conf_state.voters.contains(&id)
                        || cache.conf_state.voters_outgoing.contains(&id),
                    recent_active: progress.recent_active,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        members.sort_by_key(|member| member.member);
        Ok(ReplicationStatus {
            replica: self.identity().clone(),
            term: Term::new(self.node.raft.term),
            membership_revision: cache.membership.revision(),
            committed: LogIndex::new(self.node.raft.raft_log.committed),
            members,
        })
    }

    pub(super) fn restart_leadership(
        &mut self,
        check: RestartLeadershipCheck,
    ) -> Result<(), RuntimeError> {
        let progress = self.replication_status()?;
        let cache = self.log.read()?;
        let voters = &cache.conf_state.voters;
        if check.through.get() == 0
            || self.node.raft.pending_conf_index > self.node.raft.raft_log.applied
            || self.node.raft.lead_transferee.is_some()
            || progress.term != check.expected_term
            || progress.membership_revision != check.expected_revision
            || progress.committed < check.through
            || voters.len() != 3
            || !cache.conf_state.voters_outgoing.is_empty()
            || !cache.conf_state.learners.is_empty()
            || !cache.conf_state.learners_next.is_empty()
            || cache.conf_state.auto_leave
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
                            || (*member != self.identity().member && !p.recent_active)
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
                            || (*id != self.identity().member.get() && !p.recent_active)
                    })
            })
        {
            return Err(RuntimeError::NotReady);
        }
        let local = self.identity().member;
        match &check.transfer {
            Some((target, selector)) => {
                if check.survivors.contains(&local)
                    || !check.survivors.contains(target)
                    || selector.boot.node != self.identity().node.bytes()
                    || selector.boot.disk != self.identity().disk.bytes()
                    || selector.boot.process != self.identity().process.bytes()
                {
                    return Err(RuntimeError::NotReady);
                }
            }
            None if !check.survivors.contains(&local) => return Err(RuntimeError::NotReady),
            None => (),
        }
        drop(cache);
        if let Some((target, _)) = check.transfer {
            // The existing owner also requires this destination to be caught up
            // to the latest commit, and refuses repair/joint/active transfers.
            self.transfer_leadership(LeadershipTransfer {
                target,
                expected_term: check.expected_term,
                expected_revision: check.expected_revision,
            })?;
        }
        Ok(())
    }

    pub(super) fn transfer_leadership(
        &mut self,
        transfer: LeadershipTransfer,
    ) -> Result<(), RuntimeError> {
        self.admit_proposal()?;
        let cache = self.log.read()?;
        let target = transfer.target.get();
        if transfer.expected_term.get() != self.node.raft.term
            || transfer.expected_revision != cache.membership.revision()
            || target == self.identity().member.get()
            || !cache.conf_state.voters.contains(&target)
        {
            return Err(RuntimeError::Invalid(
                "stale or invalid leadership target".into(),
            ));
        }
        // A repair owns its membership transition until leave-joint. Moving
        // leadership is optional work and must not compete with that protocol.
        if !cache.conf_state.voters_outgoing.is_empty()
            || cache.membership.repair_status()?.is_some_and(|repair| {
                matches!(
                    repair.phase,
                    super::super::member_state::RepairPhase::Seeding
                        | super::super::member_state::RepairPhase::Joint
                )
            })
            || self.node.raft.pending_conf_index > self.node.raft.raft_log.applied
            || self.node.raft.lead_transferee.is_some()
        {
            return Err(RuntimeError::NotReady);
        }
        if self.node.raft.prs().get(target).is_none_or(|progress| {
            progress.matched < self.node.raft.raft_log.committed || !progress.recent_active
        }) {
            return Err(RuntimeError::NotReady);
        }
        drop(cache);
        self.node.transfer_leader(target);
        Ok(())
    }
}
