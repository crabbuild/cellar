// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Leadership movement is admitted by the same serialized owner as proposals.
use super::*;

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
