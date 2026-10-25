// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configuration-owned physical bindings. Birth members retain their immutable
//! catalog identity; new IDs must first commit a node/disk-bound learner entry.

use super::storage::LogError;
use ::raft::prelude::{ConfChangeType, ConfChangeV2, ConfState};
use cellar_logic::consensus::{DiskLineage, LogIndex, MemberId, NodeId};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[path = "membership.rs"]
mod repairs;
use repairs::{RepairIntent, RepairRecord};
pub use repairs::{RepairPhase, RepairPlan, RepairStatus, RepairStep};

#[path = "member_fleet.rs"]
mod fleet;
pub use fleet::{
    FleetGroupBinding, FleetMembershipPermit, FleetMembershipStatus, FleetPermitPhase,
};
use fleet::{FleetIntent, FleetState};

const MAX_BYTES: usize = 16 * 1024;
const MAX_MEMBERS: usize = 32;

#[cfg(test)]
#[path = "member_state_tests.rs"]
mod tests;

/// A requested fresh learner identity. Process generations and addresses are
/// discovery values; neither can replace this committed node/disk binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoiningMember {
    pub member: MemberId,
    pub node: NodeId,
    pub disk: DiskLineage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
    joined_at: u64,
}

/// Persisted with the applied configuration, including across compaction. IDs
/// at or below high_water can never be assigned to a different member again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupMembership {
    format: u32,
    initial: Vec<u64>,
    high_water: u64,
    revision: u64,
    bindings: Vec<Binding>,
    #[serde(deserialize_with = "required_nullable")]
    repair: Option<RepairRecord>,
    #[serde(deserialize_with = "required_nullable")]
    fleet: Option<Box<FleetState>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    format: u32,
    expected_revision: u64,
    joins: Vec<Binding>,
    #[serde(deserialize_with = "required_nullable")]
    repair: Option<RepairIntent>,
    #[serde(deserialize_with = "required_nullable")]
    fleet: Option<FleetIntent>,
}

// Marker-1 records use explicit null for no plan. Serde otherwise treats a
// missing Option field as None, silently accepting an earlier record shape.
fn required_nullable<'de, T, D>(decoder: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::<T>::deserialize(decoder)
}

fn invalid() -> LogError {
    LogError::Invalid("invalid or conflicting member binding".into())
}

pub(crate) fn members(conf: &ConfState) -> BTreeSet<u64> {
    conf.voters
        .iter()
        .chain(&conf.voters_outgoing)
        .chain(&conf.learners)
        .chain(&conf.learners_next)
        .copied()
        .collect()
}

impl GroupMembership {
    pub(crate) fn initial(conf: &ConfState) -> Self {
        let initial: Vec<_> = members(conf).into_iter().collect();
        Self {
            format: 1,
            high_water: initial.last().copied().unwrap_or(0),
            initial,
            revision: 1,
            bindings: vec![],
            repair: None,
            fleet: None,
        }
    }

    pub fn revision(&self) -> LogIndex {
        LogIndex::new(self.revision)
    }

    pub fn high_water(&self) -> u64 {
        self.high_water
    }

    pub(crate) fn permits(&self, replica: &cellar_logic::consensus::ReplicaIdentity) -> bool {
        if self.validate_scope(&replica.scope).is_err() {
            return false;
        }
        self.bindings
            .iter()
            .find(|b| b.member == replica.member.get())
            .map_or_else(
                || self.initial.contains(&replica.member.get()),
                |binding| {
                    binding.node == replica.node.bytes() && binding.disk == replica.disk.bytes()
                },
            )
    }

    pub fn joined(&self, member: MemberId) -> Result<Option<(JoiningMember, LogIndex)>, LogError> {
        self.bindings
            .iter()
            .find(|b| b.member == member.get())
            .map(|b| {
                Ok((
                    JoiningMember {
                        member,
                        node: NodeId::new(b.node).map_err(|_| invalid())?,
                        disk: DiskLineage::new(b.disk).map_err(|_| invalid())?,
                    },
                    LogIndex::new(b.joined_at),
                ))
            })
            .transpose()
    }

    pub(crate) fn reserved_bytes(&self) -> usize {
        // Retirement can leave spare vector capacity until this value is
        // cloned or released. Keep that allocation charged too.
        512 + self.initial.capacity() * 24
            + self.bindings.capacity() * 384
            + self.repair.as_ref().map_or(0, RepairRecord::reserved_bytes)
            + usize::from(self.fleet.is_some()) * 8192
    }

    pub(crate) fn validate(&self, conf: &ConfState, applied: u64) -> Result<(), LogError> {
        super::cache::validate_conf(conf)?;
        let active = members(conf);
        if self.format != 1
            || self.initial.is_empty()
            || self.initial.len() > MAX_MEMBERS
            || self.bindings.len() > MAX_MEMBERS
            || active.len() > MAX_MEMBERS
            || self.initial.windows(2).any(|w| w[0] >= w[1])
            || self
                .initial
                .iter()
                .any(|id| *id == 0 || *id >= u64::MAX - 2)
            || self.high_water < self.initial.last().copied().unwrap_or(0)
            || self.high_water >= u64::MAX - 2
            || self.revision == 0
            || self.revision > applied
            || self.revision >= u64::MAX - 2
            || self.bindings.windows(2).any(|w| w[0].member >= w[1].member)
        {
            return Err(invalid());
        }
        for binding in &self.bindings {
            if binding.member <= self.initial.last().copied().unwrap_or(0)
                || binding.member > self.high_water
                || !active.contains(&binding.member)
                || binding.joined_at <= 1
                || binding.joined_at > self.revision
                || NodeId::new(binding.node).is_err()
                || DiskLineage::new(binding.disk).is_err()
            {
                return Err(invalid());
            }
        }
        if active
            .iter()
            .any(|id| !self.initial.contains(id) && !self.bindings.iter().any(|b| b.member == *id))
        {
            return Err(invalid());
        }
        self.validate_repair(conf, applied)?;
        self.validate_fleet()?;
        Ok(())
    }

    /// Bind an explicit configuration proposal to this observed revision. New
    /// members must appear exactly once as AddLearnerNode in the same change.
    pub fn bind_change(
        &self,
        change: &mut ConfChangeV2,
        joins: &[JoiningMember],
    ) -> Result<(), LogError> {
        if self.fleet.is_some() {
            return Err(invalid());
        }
        self.bind_intent(change, joins, None, None)
    }

    fn bind_intent(
        &self,
        change: &mut ConfChangeV2,
        joins: &[JoiningMember],
        repair: Option<RepairIntent>,
        fleet: Option<FleetIntent>,
    ) -> Result<(), LogError> {
        if joins.len() > MAX_MEMBERS {
            return Err(LogError::Overloaded);
        }
        let mut bindings: Vec<_> = joins
            .iter()
            .map(|j| Binding {
                member: j.member.get(),
                node: j.node.bytes(),
                disk: j.disk.bytes(),
                joined_at: 0,
            })
            .collect();
        bindings.sort_by_key(|b| b.member);
        let intent = Intent {
            format: 1,
            expected_revision: self.revision,
            joins: bindings,
            repair,
            fleet,
        };
        let bytes = serde_json::to_vec(&intent).map_err(|_| invalid())?;
        if bytes.len() > MAX_BYTES {
            return Err(LogError::Overloaded);
        }
        change.context = bytes.into();
        Ok(())
    }

    fn intent(&self, change: &ConfChangeV2) -> Result<Intent, LogError> {
        if change.context.is_empty() {
            // Raft may generate an empty automatic joint-leave entry. This
            // supplies no new identity; explicit runtime proposals require a
            // revision-bound intent before they can be appended.
            return Ok(Intent {
                format: 1,
                expected_revision: self.revision,
                joins: vec![],
                repair: None,
                fleet: None,
            });
        }
        if change.context.len() > MAX_BYTES {
            return Err(LogError::Overloaded);
        }
        let intent: Intent = serde_json::from_slice(&change.context).map_err(|_| invalid())?;
        if intent.format != 1
            || intent.expected_revision != self.revision
            || serde_json::to_vec(&intent).map_err(|_| invalid())? != change.context
            || intent.joins.len() > MAX_MEMBERS
            || intent.joins.windows(2).any(|w| w[0].member >= w[1].member)
        {
            return Err(invalid());
        }
        Ok(intent)
    }

    pub(crate) fn admit(&self, change: &ConfChangeV2, conf: &ConfState) -> Result<(), LogError> {
        if change.changes.len() > MAX_MEMBERS {
            return Err(LogError::Overloaded);
        }
        if change.get_unknown_fields().iter().next().is_some() {
            return Err(invalid());
        }
        let intent = self.intent(change)?;
        self.validate_fleet_change(&intent, change, conf)?;
        self.validate_repair_change(&intent, change, conf)?;
        let active = members(conf);
        let mut new = BTreeSet::new();
        let mut touched = BTreeSet::new();
        for item in &change.changes {
            if item.node_id == 0
                || item.node_id >= u64::MAX - 2
                || !touched.insert(item.node_id)
                || item.get_unknown_fields().iter().next().is_some()
            {
                return Err(invalid());
            }
            if !active.contains(&item.node_id) {
                if item.get_change_type() != ConfChangeType::AddLearnerNode
                    || item.node_id <= self.high_water
                {
                    return Err(invalid());
                }
                new.insert(item.node_id);
            }
        }
        if new.len() != intent.joins.len() || active.len() + new.len() > MAX_MEMBERS {
            return Err(invalid());
        }
        for binding in &intent.joins {
            if !new.contains(&binding.member)
                || binding.joined_at != 0
                || NodeId::new(binding.node).is_err()
                || DiskLineage::new(binding.disk).is_err()
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    /// Apply only at the configuration's committed ordered apply head. The
    /// resulting state must be stored atomically with ConfState and that marker.
    pub fn applied(
        &self,
        change: &ConfChangeV2,
        before: &ConfState,
        after: &ConfState,
        index: LogIndex,
    ) -> Result<Self, LogError> {
        self.admit(change, before)?;
        if index.get() <= self.revision {
            return Err(invalid());
        }
        let intent = self.intent(change)?;
        let mut next = self.clone();
        next.revision = index.get();
        for binding in &intent.joins {
            let mut binding = binding.clone();
            binding.joined_at = index.get();
            next.high_water = next.high_water.max(binding.member);
            next.bindings.push(binding);
        }
        let active = members(after);
        next.bindings.retain(|b| active.contains(&b.member));
        next.bindings.sort_by_key(|b| b.member);
        next.apply_repair(&intent, index.get())?;
        next.apply_fleet(&intent, index.get())?;
        next.validate(after, index.get())?;
        Ok(next)
    }

    pub(crate) fn succeeds(&self, old: &Self) -> Result<(), LogError> {
        if self.initial != old.initial
            || self.high_water < old.high_water
            || self.revision < old.revision
            || (self.revision == old.revision && self != old)
            || old.bindings.iter().any(|old| {
                self.bindings
                    .iter()
                    .any(|new| new.member == old.member && new != old)
            })
            || self.bindings.iter().any(|new| {
                new.member <= old.high_water
                    && !old.bindings.iter().any(|old| old.member == new.member)
            })
        {
            return Err(invalid());
        }
        self.repair_succeeds(old)?;
        self.fleet_succeeds(old)?;
        Ok(())
    }

    pub(crate) fn same_birth(&self, other: &Self) -> bool {
        self.initial == other.initial
    }
}
