// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Verify explicit topology intent before an adapter admits any movement.
//! A preview supplies witnesses, not membership authority or a health lease.

mod recovery;
mod schedule;

use super::{CapacityScope, LedgerError, LedgerSnapshot, ResourceLedger, Resources};
use crate::consensus::{GroupScope, LogIndex, NodeId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub struct TopologyNode {
    /// Complete, authenticated exact-boot input is the adapter's responsibility.
    /// Physical occupancy includes retained histories and fixed source pools.
    pub ledger: LedgerSnapshot,
    pub host: String,
    pub region: String,
    pub zone: String,
    /// The explicit final keep-set. Departing nodes cannot receive new copies.
    pub keep: bool,
    /// Current authenticated placement eligibility, independent of final intent.
    /// Kept but draining, incompatible or unqualified nodes cannot receive work.
    pub receives_replicas: bool,
}

#[derive(Clone, Debug)]
pub struct TopologyGroup {
    pub scope: GroupScope,
    /// Initial stable RF3 revision. Re-read before each actual membership permit.
    pub revision: LogIndex,
    pub voters: [NodeId; 3],
    /// Every occupied binding for this history, including retired bindings.
    pub occupied: Vec<NodeId>,
    /// Conservative retained footprint of a newly completed copy.
    pub resident: Resources,
    /// Target's incremental peak, including seed, tail, image and worker work.
    pub peak: Resources,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyTarget {
    pub group: GroupScope,
    pub voters: [NodeId; 3],
}

#[derive(Clone, Debug)]
pub struct TopologyView {
    /// Digest of the complete rooted input, not a caller's free-form request ID.
    pub revision: [u8; 32],
    pub complete: bool,
    pub nodes: Vec<TopologyNode>,
    pub groups: Vec<TopologyGroup>,
}

#[derive(Clone, Copy, Debug)]
pub struct TopologyPolicy {
    pub revision: u64,
    pub distinct_hosts: bool,
    pub distinct_zones: bool,
    pub distinct_regions: bool,
    /// Shared across staged-path search and every single-node recovery search.
    pub max_work: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaMove {
    pub group: GroupScope,
    pub source: CapacityScope,
    pub target: CapacityScope,
    /// Serial dependency: complete the preceding move's terminal proof first.
    /// Indices are local to this move list, including each recovery witness.
    pub after: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopologyPeak {
    pub scope: CapacityScope,
    /// Includes baseline physical occupancy, reserve and all retained growth.
    pub resources: Resources,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRecovery {
    pub failed: CapacityScope,
    /// RF3 restoration using fresh bindings, keeping both healthy voters.
    pub moves: Vec<ReplicaMove>,
    pub peaks: Vec<TopologyPeak>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyPreview {
    pub input_revision: [u8; 32],
    pub policy_revision: u64,
    pub assignments: Vec<TopologyTarget>,
    pub moves: Vec<ReplicaMove>,
    pub peaks: Vec<TopologyPeak>,
    /// Every kept node, including an empty spare, has a separate loss scenario.
    pub recovery: Vec<NodeRecovery>,
    pub search_work: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyError {
    InvalidPolicy,
    InvalidInput,
    Incomplete,
    Limit,
    Ledger(LedgerError),
    UnsettledMovement,
    OccupiedTarget,
    ExcludedTarget,
    FailureDomains,
    /// Exhaustive only for direct moves to the supplied final assignment.
    NoStagedPath,
    /// No direct restoration witness; does not prove all reshuffles infeasible.
    RecoveryUnproven(NodeId),
    SearchExhausted,
}

struct Model<'a> {
    view: &'a TopologyView,
    policy: TopologyPolicy,
    nodes: BTreeMap<NodeId, usize>,
    groups: Vec<&'a TopologyGroup>,
    initial: Vec<[usize; 3]>,
    desired: Vec<[usize; 3]>,
    occupied: Vec<BTreeSet<usize>>,
    base: Vec<Resources>,
}

struct Work {
    used: u64,
    limit: u64,
}

impl Work {
    fn step(&mut self) -> Result<(), TopologyError> {
        if self.used == self.limit {
            return Err(TopologyError::SearchExhausted);
        }
        self.used += 1;
        Ok(())
    }
}

fn add(left: Resources, right: Resources) -> Result<Resources, TopologyError> {
    left.checked_add(right).map_err(TopologyError::Ledger)
}

/// Check a complete proposed layout, construct a legal serial move schedule,
/// and find a full direct RF3 restoration witness for every kept-node loss.
/// Nothing is admitted unless the entire result succeeds. Search exhaustion
/// returns no partial preview. This does not generate or score final layouts.
///
/// V1 requires stable RF3 groups, reachable fresh node samples and no unsettled
/// reservations. An adapter must pin inventory/policy/boot inputs, authenticate
/// them and revalidate each real permit. Completion never reclaims source space.
pub fn preview_topology(
    view: &TopologyView,
    policy: TopologyPolicy,
    targets: &[TopologyTarget],
    now_ms: u64,
) -> Result<TopologyPreview, TopologyError> {
    let model = Model::new(view, policy, targets, now_ms)?;
    let mut work = Work {
        used: 0,
        limit: policy.max_work,
    };
    let scheduled = schedule::find(
        &model,
        &model.initial,
        &model.desired,
        &model.base,
        None,
        &mut work,
    )?
    .ok_or(TopologyError::NoStagedPath)?;
    let mut occupied = model.occupied.clone();
    for (bindings, voters) in occupied.iter_mut().zip(&model.desired) {
        bindings.extend(voters);
    }
    let mut recovery = Vec::new();
    for failed in model.nodes.values().copied() {
        let node = &view.nodes[failed];
        if node.keep {
            recovery.push(recovery::find(
                &model,
                failed,
                &occupied,
                &scheduled.retained,
                &mut work,
            )?);
        }
    }
    recovery.sort_by_key(|scenario| scenario.failed.node.node);
    Ok(TopologyPreview {
        input_revision: view.revision,
        policy_revision: policy.revision,
        assignments: model
            .groups
            .iter()
            .zip(&model.desired)
            .map(|(group, voters)| TopologyTarget {
                group: group.scope.clone(),
                voters: voters.map(|n| view.nodes[n].ledger.scope.node.node),
            })
            .collect(),
        moves: scheduled.moves,
        peaks: scheduled.peaks,
        recovery,
        search_work: work.used,
    })
}

impl<'a> Model<'a> {
    fn new(
        view: &'a TopologyView,
        policy: TopologyPolicy,
        targets: &[TopologyTarget],
        now_ms: u64,
    ) -> Result<Self, TopologyError> {
        if policy.revision == 0 || policy.max_work == 0 || policy.max_work > 50_000_000 {
            return Err(TopologyError::InvalidPolicy);
        }
        if !view.complete {
            return Err(TopologyError::Incomplete);
        }
        if view.nodes.len() > 64 || view.groups.len() > 10_000 || targets.len() > 10_000 {
            return Err(TopologyError::Limit);
        }
        if view.revision == [0; 32] || view.nodes.is_empty() || targets.len() != view.groups.len() {
            return Err(TopologyError::InvalidInput);
        }
        let first = view.nodes[0].ledger.scope;
        let mut nodes = BTreeMap::new();
        let mut disks = BTreeSet::new();
        let mut engines = BTreeSet::new();
        let mut base = Vec::new();
        for (i, node) in view.nodes.iter().enumerate() {
            let scope = node.ledger.scope;
            if scope.cluster != first.cluster
                || scope.incarnation != first.incarnation
                || scope.catalog != first.catalog
                || nodes.insert(scope.node.node, i).is_some()
                || !disks.insert(scope.node.disk)
                || !engines.insert(scope.engine)
                || [&node.host, &node.zone, &node.region]
                    .iter()
                    .any(|s| s.is_empty() || s.len() > 128 || s.chars().any(char::is_control))
            {
                return Err(TopologyError::InvalidInput);
            }
            let ledger =
                ResourceLedger::restore(node.ledger.clone()).map_err(TopologyError::Ledger)?;
            let charge = ledger.charge(now_ms).map_err(TopologyError::Ledger)?;
            if charge.active_moves != 0 {
                return Err(TopologyError::UnsettledMovement);
            }
            if !node.ledger.policy.capacity.covers(charge.total) {
                return Err(TopologyError::InvalidInput);
            }
            base.push(charge.total);
        }
        let mut target_map = BTreeMap::new();
        for target in targets {
            if target_map.insert(&target.group, target.voters).is_some() {
                return Err(TopologyError::InvalidInput);
            }
        }
        let mut model = Self {
            view,
            policy,
            nodes,
            groups: view.groups.iter().collect(),
            initial: Vec::new(),
            desired: Vec::new(),
            occupied: Vec::new(),
            base,
        };
        model.groups.sort_by_key(|group| &group.scope);
        let mut histories = BTreeSet::new();
        let mut cells = BTreeSet::new();
        let mut occupancy = vec![0_u64; view.nodes.len()];
        for group in &model.groups {
            group
                .scope
                .validate()
                .map_err(|_| TopologyError::InvalidInput)?;
            if group.scope.cluster != first.cluster
                || group.revision.get() == 0
                || group.revision.get() >= u64::MAX - 2
                || !histories.insert(group.scope.group)
                || !cells.insert((group.scope.namespace, &group.scope.cell))
                || group.resident.engine_slots != 1
                || group.resident.durable_bytes == 0
                || group.resident.memory_bytes == 0
                || !group.peak.covers(group.resident)
                || group.occupied.len() > view.nodes.len()
            {
                return Err(TopologyError::InvalidInput);
            }
            let mut occupied = BTreeSet::new();
            for node in &group.occupied {
                let n = *model.nodes.get(node).ok_or(TopologyError::InvalidInput)?;
                if !occupied.insert(n) {
                    return Err(TopologyError::InvalidInput);
                }
                occupancy[n] += 1;
            }
            let initial = model.indices(group.voters)?;
            let desired = model.indices(
                *target_map
                    .get(&group.scope)
                    .ok_or(TopologyError::InvalidInput)?,
            )?;
            if initial.iter().any(|n| !occupied.contains(n)) {
                return Err(TopologyError::Incomplete);
            }
            if !model.domains(initial) || !model.domains(desired) {
                return Err(TopologyError::FailureDomains);
            }
            for n in desired {
                if !view.nodes[n].keep {
                    return Err(TopologyError::ExcludedTarget);
                }
                if !initial.contains(&n) && !view.nodes[n].receives_replicas {
                    return Err(TopologyError::ExcludedTarget);
                }
                if !initial.contains(&n) && occupied.contains(&n) {
                    return Err(TopologyError::OccupiedTarget);
                }
            }
            model.initial.push(initial);
            model.desired.push(desired);
            model.occupied.push(occupied);
        }
        if occupancy
            .iter()
            .zip(&view.nodes)
            .any(|(count, node)| *count > node.ledger.sample.physical.engine_slots)
        {
            return Err(TopologyError::Incomplete);
        }
        Ok(model)
    }

    fn indices(&self, nodes: [NodeId; 3]) -> Result<[usize; 3], TopologyError> {
        let mut result = [0; 3];
        for (i, n) in nodes.iter().enumerate() {
            result[i] = *self.nodes.get(n).ok_or(TopologyError::InvalidInput)?;
        }
        result.sort_by_key(|n| self.view.nodes[*n].ledger.scope.node.node);
        if result[0] == result[1] || result[1] == result[2] {
            return Err(TopologyError::InvalidInput);
        }
        Ok(result)
    }

    fn domains(&self, voters: [usize; 3]) -> bool {
        for i in 0..3 {
            for j in 0..i {
                let a = &self.view.nodes[voters[i]];
                let b = &self.view.nodes[voters[j]];
                if (self.policy.distinct_hosts && a.host == b.host)
                    || (self.policy.distinct_zones && (&a.region, &a.zone) == (&b.region, &b.zone))
                    || (self.policy.distinct_regions && a.region == b.region)
                {
                    return false;
                }
            }
        }
        true
    }

    fn fits(&self, node: usize, resources: Resources) -> bool {
        self.view.nodes[node]
            .ledger
            .policy
            .capacity
            .covers(resources)
    }
}
