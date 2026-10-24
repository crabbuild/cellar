// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One configuration-owned repair plan. The enclosing durable group mapping and
//! authenticated operation bind scope; a plan ID itself supplies no authority.

use super::*;
use ::raft::prelude::{ConfChangeSingle, ConfChangeTransition};
use cellar_logic::consensus::ProposalId;

/// An administrative intent scoped by the owning cell handle/log. Automatic
/// repair uses the three-voter profile; retries preserve this entire payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairPlan {
    pub id: ProposalId,
    pub policy_revision: u64,
    pub expected_revision: LogIndex,
    pub obsolete: MemberId,
    pub replacement: JoiningMember,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairPhase {
    Seeding,
    Joint,
    Complete,
    Aborted,
}

impl RepairPhase {
    pub fn active(self) -> bool {
        matches!(self, Self::Seeding | Self::Joint)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairStep {
    PromoteAndRemove,
    FinishJoint,
    AbortLearner,
}

/// Synced configuration observation scoped by its owning cell/log. It supplies
/// no S3 output proof or fresh-quorum permission to initialize a member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairStatus {
    pub plan: RepairPlan,
    pub phase: RepairPhase,
    pub started_at: LogIndex,
    pub changed_at: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    id: [u8; 16],
    policy_revision: u64,
    expected_revision: u64,
    obsolete: u64,
    replacement: Binding,
    voters: Vec<u64>,
    learners: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepairRecord {
    plan: Plan,
    phase: RepairPhase,
    started_at: u64,
    changed_at: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum RepairIntent {
    Start { plan: Plan },
    Promote { id: [u8; 16] },
    Finish { id: [u8; 16] },
    Abort { id: [u8; 16] },
}

impl RepairRecord {
    pub(super) fn reserved_bytes(&self) -> usize {
        512 + (self.plan.voters.capacity() + self.plan.learners.capacity()) * 8
    }
}

#[cfg(test)]
#[path = "member_plan_tests.rs"]
mod tests;

impl Plan {
    fn public(&self) -> Result<RepairPlan, LogError> {
        Ok(RepairPlan {
            id: ProposalId::new(self.id).map_err(|_| invalid())?,
            policy_revision: self.policy_revision,
            expected_revision: LogIndex::new(self.expected_revision),
            obsolete: MemberId::new(self.obsolete).map_err(|_| invalid())?,
            replacement: JoiningMember {
                member: MemberId::new(self.replacement.member).map_err(|_| invalid())?,
                node: NodeId::new(self.replacement.node).map_err(|_| invalid())?,
                disk: DiskLineage::new(self.replacement.disk).map_err(|_| invalid())?,
            },
        })
    }

    fn validate(&self) -> Result<(), LogError> {
        self.public()?;
        if self.policy_revision == 0
            || self.expected_revision == 0
            || self.expected_revision >= u64::MAX - 2
            || self.voters.len() != 3
            || self.learners.len() > 28
            || self.replacement.joined_at != 0
            || !self.voters.contains(&self.obsolete)
            || self.replacement.member >= u64::MAX - 2
            || self.voters.windows(2).any(|w| w[0] >= w[1])
            || self.learners.windows(2).any(|w| w[0] >= w[1])
            || self
                .voters
                .iter()
                .chain(&self.learners)
                .any(|id| *id == 0 || *id >= self.replacement.member)
            || self.voters.iter().any(|id| self.learners.contains(id))
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn configuration(&self, phase: RepairPhase) -> ConfState {
        let mut conf = ConfState {
            voters: self.voters.clone(),
            learners: self.learners.clone(),
            ..Default::default()
        };
        match phase {
            RepairPhase::Seeding => {
                conf.learners.push(self.replacement.member);
                conf.learners.sort_unstable();
            }
            RepairPhase::Joint | RepairPhase::Complete => {
                conf.voters.retain(|id| *id != self.obsolete);
                conf.voters.push(self.replacement.member);
                conf.voters.sort_unstable();
                if phase == RepairPhase::Joint {
                    conf.voters_outgoing = self.voters.clone();
                }
            }
            RepairPhase::Aborted => (),
        }
        conf
    }
}

fn canonical(conf: &ConfState) -> Result<ConfState, LogError> {
    crate::raft::cache::validate_conf(conf)?;
    let mut conf = conf.clone();
    conf.voters.sort_unstable();
    conf.learners.sort_unstable();
    conf.voters_outgoing.sort_unstable();
    conf.learners_next.sort_unstable();
    Ok(conf)
}

fn item(id: u64, kind: ConfChangeType) -> ConfChangeSingle {
    ConfChangeSingle {
        node_id: id,
        change_type: kind,
        ..Default::default()
    }
}

impl GroupMembership {
    pub fn repair_status(&self) -> Result<Option<RepairStatus>, LogError> {
        self.repair
            .as_ref()
            .map(|record| {
                Ok(RepairStatus {
                    plan: record.plan.public()?,
                    phase: record.phase,
                    started_at: LogIndex::new(record.started_at),
                    changed_at: LogIndex::new(record.changed_at),
                })
            })
            .transpose()
    }

    /// Bind one fresh learner to a complete immutable plan. The runtime still
    /// checks current leader, pending configuration and actual catch-up later.
    pub fn bind_repair_start(
        &self,
        change: &mut ConfChangeV2,
        conf: &ConfState,
        plan: &RepairPlan,
    ) -> Result<(), LogError> {
        if plan.expected_revision.get() != self.revision
            || self.repair.as_ref().is_some_and(|p| p.phase.active())
            || self
                .repair
                .as_ref()
                .is_some_and(|p| plan.policy_revision < p.plan.policy_revision)
            || plan.replacement.member.get() <= self.high_water
        {
            return Err(invalid());
        }
        let conf = canonical(conf)?;
        if !conf.voters_outgoing.is_empty() || !conf.learners_next.is_empty() || conf.auto_leave {
            return Err(invalid());
        }
        let wire = Plan {
            id: plan.id.bytes(),
            policy_revision: plan.policy_revision,
            expected_revision: plan.expected_revision.get(),
            obsolete: plan.obsolete.get(),
            replacement: Binding {
                member: plan.replacement.member.get(),
                node: plan.replacement.node.bytes(),
                disk: plan.replacement.disk.bytes(),
                joined_at: 0,
            },
            voters: conf.voters.clone(),
            learners: conf.learners.clone(),
        };
        wire.validate()?;
        if self.repair.as_ref().is_some_and(|r| r.plan.id == wire.id) {
            return Err(invalid());
        }
        *change = ConfChangeV2 {
            changes: vec![item(
                wire.replacement.member,
                ConfChangeType::AddLearnerNode,
            )]
            .into(),
            ..Default::default()
        };
        self.bind_intent(
            change,
            std::slice::from_ref(&plan.replacement),
            Some(RepairIntent::Start { plan: wire }),
        )?;
        self.admit(change, &conf)
    }

    pub fn bind_repair_step(
        &self,
        change: &mut ConfChangeV2,
        conf: &ConfState,
        id: ProposalId,
        step: RepairStep,
    ) -> Result<(), LogError> {
        let record = self.repair.as_ref().ok_or_else(invalid)?;
        if record.plan.id != id.bytes() {
            return Err(invalid());
        }
        *change = ConfChangeV2::default();
        let action = match step {
            RepairStep::PromoteAndRemove => {
                change.set_transition(ConfChangeTransition::Explicit);
                change.changes = vec![
                    item(record.plan.replacement.member, ConfChangeType::AddNode),
                    item(record.plan.obsolete, ConfChangeType::RemoveNode),
                ]
                .into();
                change.changes.sort_by_key(|c| c.node_id);
                RepairIntent::Promote { id: id.bytes() }
            }
            RepairStep::FinishJoint => RepairIntent::Finish { id: id.bytes() },
            RepairStep::AbortLearner => {
                change.changes.push(item(
                    record.plan.replacement.member,
                    ConfChangeType::RemoveNode,
                ));
                RepairIntent::Abort { id: id.bytes() }
            }
        };
        self.bind_intent(change, &[], Some(action))?;
        self.admit(change, conf)
    }

    pub(super) fn validate_repair_change(
        &self,
        intent: &Intent,
        change: &ConfChangeV2,
        conf: &ConfState,
    ) -> Result<(), LogError> {
        let conf = canonical(conf)?;
        let Some(action) = &intent.repair else {
            return if self.repair.as_ref().is_some_and(|p| p.phase.active()) {
                Err(invalid())
            } else {
                Ok(())
            };
        };
        let expected = match action {
            RepairIntent::Start { plan } => {
                plan.validate()?;
                if plan.expected_revision != self.revision
                    || plan.replacement.member <= self.high_water
                    || self.repair.as_ref().is_some_and(|r| {
                        r.phase.active()
                            || r.plan.id == plan.id
                            || r.plan.policy_revision > plan.policy_revision
                    })
                    || conf != plan.configuration(RepairPhase::Aborted)
                    || intent.joins != vec![plan.replacement.clone()]
                {
                    return Err(invalid());
                }
                vec![item(
                    plan.replacement.member,
                    ConfChangeType::AddLearnerNode,
                )]
            }
            RepairIntent::Promote { id }
            | RepairIntent::Finish { id }
            | RepairIntent::Abort { id } => {
                let record = self.repair.as_ref().ok_or_else(invalid)?;
                if record.plan.id != *id
                    || !intent.joins.is_empty()
                    || conf != record.plan.configuration(record.phase)
                {
                    return Err(invalid());
                }
                match action {
                    RepairIntent::Promote { .. } if record.phase == RepairPhase::Seeding => {
                        if change.get_transition() != ConfChangeTransition::Explicit {
                            return Err(invalid());
                        }
                        let mut changes = vec![
                            item(record.plan.replacement.member, ConfChangeType::AddNode),
                            item(record.plan.obsolete, ConfChangeType::RemoveNode),
                        ];
                        changes.sort_by_key(|c| c.node_id);
                        changes
                    }
                    RepairIntent::Finish { .. } if record.phase == RepairPhase::Joint => vec![],
                    RepairIntent::Abort { .. } if record.phase == RepairPhase::Seeding => {
                        vec![item(
                            record.plan.replacement.member,
                            ConfChangeType::RemoveNode,
                        )]
                    }
                    _ => return Err(invalid()),
                }
            }
        };
        if change.get_changes() != expected.as_slice()
            || (!matches!(action, RepairIntent::Promote { .. })
                && change.get_transition() != ConfChangeTransition::Auto)
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(super) fn apply_repair(&mut self, intent: &Intent, index: u64) -> Result<(), LogError> {
        match &intent.repair {
            None => (),
            Some(RepairIntent::Start { plan }) => {
                self.repair = Some(RepairRecord {
                    plan: plan.clone(),
                    phase: RepairPhase::Seeding,
                    started_at: index,
                    changed_at: index,
                })
            }
            Some(action) => {
                let record = self.repair.as_mut().ok_or_else(invalid)?;
                record.phase = match action {
                    RepairIntent::Promote { .. } => RepairPhase::Joint,
                    RepairIntent::Finish { .. } => RepairPhase::Complete,
                    RepairIntent::Abort { .. } => RepairPhase::Aborted,
                    _ => return Err(invalid()),
                };
                record.changed_at = index;
            }
        }
        Ok(())
    }

    pub(super) fn validate_repair(&self, conf: &ConfState, applied: u64) -> Result<(), LogError> {
        let Some(record) = &self.repair else {
            return Ok(());
        };
        record.plan.validate()?;
        if record.started_at <= record.plan.expected_revision
            || record.changed_at < record.started_at
            || record.changed_at > applied
            || record.changed_at > self.revision
            || record.plan.replacement.member > self.high_water
            || (record.phase == RepairPhase::Seeding && record.changed_at != record.started_at)
            || (record.phase != RepairPhase::Seeding && record.changed_at == record.started_at)
            || (record.phase.active() && record.changed_at != self.revision)
            || (record.changed_at == self.revision
                && canonical(conf)? != record.plan.configuration(record.phase))
        {
            return Err(invalid());
        }
        let binding = self
            .bindings
            .iter()
            .find(|b| b.member == record.plan.replacement.member);
        if record.phase == RepairPhase::Aborted {
            if binding.is_some() || members(conf).contains(&record.plan.replacement.member) {
                return Err(invalid());
            }
        } else if let Some(binding) = binding {
            if binding.node != record.plan.replacement.node
                || binding.disk != record.plan.replacement.disk
                || binding.joined_at != record.started_at
            {
                return Err(invalid());
            }
        } else if record.phase.active() {
            return Err(invalid());
        }
        Ok(())
    }

    pub(super) fn repair_succeeds(&self, old: &Self) -> Result<(), LogError> {
        let previous_revision = old.revision;
        match (&old.repair, &self.repair) {
            (None, None) => Ok(()),
            (None, Some(new)) => {
                if new.plan.expected_revision < previous_revision
                    || new.started_at <= previous_revision
                {
                    Err(invalid())
                } else {
                    Ok(())
                }
            }
            (Some(_), None) => Err(invalid()),
            (Some(old), Some(new)) if old.plan.id == new.plan.id => {
                if old.plan != new.plan
                    || old.started_at != new.started_at
                    || new.changed_at < old.changed_at
                {
                    return Err(invalid());
                }
                let allowed = match old.phase {
                    RepairPhase::Seeding => true,
                    RepairPhase::Joint => {
                        matches!(new.phase, RepairPhase::Joint | RepairPhase::Complete)
                    }
                    RepairPhase::Complete | RepairPhase::Aborted => {
                        old.phase == new.phase && old.changed_at == new.changed_at
                    }
                };
                if !allowed || (old.phase == new.phase && old.changed_at != new.changed_at) {
                    return Err(invalid());
                }
                Ok(())
            }
            (Some(old), Some(new)) => {
                if new.plan.expected_revision < previous_revision
                    || new.started_at <= old.changed_at
                    || new.plan.replacement.member <= old.plan.replacement.member
                    || new.plan.policy_revision < old.plan.policy_revision
                {
                    return Err(invalid());
                }
                Ok(())
            }
        }
    }
}
