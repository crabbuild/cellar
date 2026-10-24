// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded placement decisions. These values never grant membership authority;
//! the host must submit revision-bound learner/joint-consensus transitions.

use crate::consensus::{DiskLineage, GroupScope, LogIndex, MemberId, NodeId};
use std::{cmp::Ordering, collections::BTreeSet};

#[derive(Clone, Debug)]
pub struct PlacementPolicy {
    pub revision: u64,
    pub pool: String,
    pub distinct_zones: bool,
    pub distinct_regions: bool,
    pub max_sample_age_ms: u64,
    /// Durable-storage working-set reservation for this cell, including copies.
    pub required_bytes: u64,
}

impl PlacementPolicy {
    /// Validate explicit policy bounds before collecting observations.
    pub fn validate(&self) -> Result<(), PlacementError> {
        if self.revision == 0
            || !label(&self.pool)
            || self.max_sample_age_ms == 0
            || self.max_sample_age_ms > 3_600_000
            || self.required_bytes == 0
        {
            return Err(PlacementError::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeHealth {
    Healthy,
    Unavailable,
}

#[derive(Clone, Debug)]
pub struct NodeCapacity {
    pub node: NodeId,
    pub disk: DiskLineage,
    pub pool: String,
    pub region: String,
    pub zone: String,
    /// Receiver-local monotonic sampling time, in the same clock as `now_ms`.
    pub sampled_ms: u64,
    pub health: NodeHealth,
    pub compatible: bool,
    pub draining: bool,
    pub groups: u32,
    pub group_capacity: u32,
    pub bytes: u64,
    /// Durable-storage budget; group slots and host budgets constrain memory.
    pub byte_capacity: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacedMember {
    pub member: MemberId,
    pub node: NodeId,
    pub disk: DiskLineage,
}

pub struct PlacementView {
    pub scope: GroupScope,
    pub revision: LogIndex,
    pub high_water: u64,
    pub voters: Vec<PlacedMember>,
    pub learners: Vec<PlacedMember>,
    pub joint: bool,
    pub repairing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairPlacement {
    pub scope: GroupScope,
    pub policy_revision: u64,
    pub expected_revision: LogIndex,
    pub obsolete: MemberId,
    pub replacement: PlacedMember,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementError {
    InvalidPolicy,
    InvalidObservation,
    InvalidConfiguration,
    ConfigurationInTransition,
    ExhaustedMemberIds,
    InsufficientCapacity,
    InsufficientDomains,
}

impl std::fmt::Display for PlacementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft placement: {self:?}")
    }
}

impl std::error::Error for PlacementError {}

fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
}

fn fresh(node: &NodeCapacity, now: u64, policy: &PlacementPolicy) -> bool {
    node.sampled_ms <= now && now - node.sampled_ms <= policy.max_sample_age_ms
}

fn domains(nodes: &[&NodeCapacity], policy: &PlacementPolicy) -> bool {
    (!policy.distinct_regions
        || nodes
            .iter()
            .map(|n| &n.region)
            .collect::<BTreeSet<_>>()
            .len()
            == nodes.len())
        && (!policy.distinct_zones
            || nodes
                .iter()
                .map(|n| (&n.region, &n.zone))
                .collect::<BTreeSet<_>>()
                .len()
                == nodes.len())
}

// Deterministic distribution for equal load; not a cryptographic identity.
pub(crate) fn rank(scope: &GroupScope, node: NodeId) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for bytes in [
        scope.cluster.bytes(),
        scope.namespace.bytes(),
        scope.group.bytes(),
        scope.incarnation.bytes(),
        node.bytes(),
    ] {
        for byte in bytes {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    hash
}

// Both inputs passed checked admission; every projected count fits u64,
// so rational comparisons fit u128 without truncation or float error.
fn pressure(node: &NodeCapacity, bytes: u64) -> (u128, u128) {
    let groups = u128::from(node.groups) + 1;
    let group_capacity = u128::from(node.group_capacity);
    let stored = u128::from(node.bytes) + u128::from(bytes);
    let byte_capacity = u128::from(node.byte_capacity);
    if groups * byte_capacity >= stored * group_capacity {
        (groups, group_capacity)
    } else {
        (stored, byte_capacity)
    }
}

fn load(left: &NodeCapacity, right: &NodeCapacity, scope: &GroupScope, bytes: u64) -> Ordering {
    let (left_load, left_capacity) = pressure(left, bytes);
    let (right_load, right_capacity) = pressure(right, bytes);
    (left_load * right_capacity)
        .cmp(&(right_load * left_capacity))
        .then_with(|| rank(scope, right.node).cmp(&rank(scope, left.node)))
        .then_with(|| left.node.cmp(&right.node))
}

/// Choose one replacement for the three-voter profile. Only explicitly current
/// observations can trigger failure/drain repair; stale/missing samples alone
/// do not establish that a voter failed. Capacity/domain rejection changes no
/// configuration. Existing plans and joint states must be reconciled first.
pub fn select_repair(
    view: &PlacementView,
    nodes: &[NodeCapacity],
    policy: &PlacementPolicy,
    now_ms: u64,
) -> Result<Option<RepairPlacement>, PlacementError> {
    policy.validate()?;
    if nodes.is_empty() || nodes.len() > 4096 {
        return Err(PlacementError::InvalidObservation);
    }
    let mut node_ids = BTreeSet::new();
    let mut disks = BTreeSet::new();
    for node in nodes {
        if !label(&node.pool)
            || !label(&node.region)
            || !label(&node.zone)
            || node.sampled_ms > now_ms
            || !node_ids.insert(node.node)
            || !disks.insert(node.disk)
        {
            return Err(PlacementError::InvalidObservation);
        }
    }
    if view.voters.len() != 3
        || view.learners.len() > 29
        || view.revision.get() == 0
        || view.revision.get() >= u64::MAX - 2
        || view.high_water >= u64::MAX - 2
        || !crate::cell::valid_cell_scope(&view.scope.cell)
    {
        return Err(PlacementError::InvalidConfiguration);
    }
    let mut members = BTreeSet::new();
    let mut occupied = BTreeSet::new();
    for replica in view.voters.iter().chain(&view.learners) {
        if replica.member.get() > view.high_water
            || !members.insert(replica.member)
            || !occupied.insert(replica.node)
        {
            return Err(PlacementError::InvalidConfiguration);
        }
    }
    if view.joint || view.repairing {
        return Err(PlacementError::ConfigurationInTransition);
    }
    let observed = |replica: &PlacedMember| {
        nodes
            .iter()
            .find(|n| n.node == replica.node && n.disk == replica.disk && fresh(n, now_ms, policy))
    };
    let mut voters: Vec<_> = view.voters.iter().collect();
    voters.sort_by_key(|m| m.member);
    let mut obsolete = voters
        .iter()
        .copied()
        .find(|member| observed(member).is_some_and(|node| node.health == NodeHealth::Unavailable))
        .or_else(|| {
            voters.iter().copied().find(|member| {
                observed(member).is_some_and(|node| {
                    node.draining || node.pool != policy.pool || !node.compatible
                })
            })
        });
    if obsolete.is_none() {
        // Desired domain changes may also move a healthy voter. Observe the
        // complete current set before making that decision.
        let current: Option<Vec<_>> = voters.iter().map(|m| observed(m)).collect();
        if let Some(current) = current {
            if !domains(&current, policy) {
                obsolete = voters.iter().copied().rev().find(|candidate| {
                    let remaining: Vec<_> = voters
                        .iter()
                        .filter(|m| m.member != candidate.member)
                        .filter_map(|m| observed(m))
                        .collect();
                    domains(&remaining, policy)
                });
                if obsolete.is_none() {
                    return Err(PlacementError::InsufficientDomains);
                }
            }
        }
    }
    let Some(obsolete) = obsolete else {
        return Ok(None);
    };
    let remaining: Option<Vec<_>> = voters
        .iter()
        .filter(|m| m.member != obsolete.member)
        .map(|m| observed(m))
        .collect();
    let remaining = remaining.ok_or(PlacementError::InvalidObservation)?;
    if remaining
        .iter()
        .any(|node| node.health != NodeHealth::Healthy || !node.compatible)
    {
        return Err(PlacementError::InvalidObservation);
    }
    let mut capacity: Vec<_> = nodes
        .iter()
        .filter(|n| {
            !occupied.contains(&n.node)
                && n.pool == policy.pool
                && n.health == NodeHealth::Healthy
                && n.compatible
                && !n.draining
                && fresh(n, now_ms, policy)
                && n.groups < n.group_capacity
                && n.bytes
                    .checked_add(policy.required_bytes)
                    .is_some_and(|b| b <= n.byte_capacity)
        })
        .collect();
    if capacity.is_empty() {
        return Err(PlacementError::InsufficientCapacity);
    }
    capacity.retain(|candidate| {
        let mut set = remaining.clone();
        set.push(candidate);
        domains(&set, policy)
    });
    if capacity.is_empty() {
        return Err(PlacementError::InsufficientDomains);
    }
    capacity.sort_by(|left, right| load(left, right, &view.scope, policy.required_bytes));
    let target = capacity
        .first()
        .ok_or(PlacementError::InsufficientDomains)?;
    if view.high_water >= u64::MAX - 3 {
        return Err(PlacementError::ExhaustedMemberIds);
    }
    let next = view
        .high_water
        .checked_add(1)
        .ok_or(PlacementError::ExhaustedMemberIds)?;
    let member = MemberId::new(next).map_err(|_| PlacementError::ExhaustedMemberIds)?;
    Ok(Some(RepairPlacement {
        scope: view.scope.clone(),
        policy_revision: policy.revision,
        expected_revision: view.revision,
        obsolete: obsolete.member,
        replacement: PlacedMember {
            member,
            node: target.node,
            disk: target.disk,
        },
    }))
}
