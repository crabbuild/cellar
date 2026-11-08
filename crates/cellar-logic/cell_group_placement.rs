// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded recommendations for partition sizing. Every result is advisory.
//! Admission belongs to the current directory, Fleet, and Raft owners.

use crate::{
    cell_group::{CellBound, CellId, CellKey, GroupRoute, MembershipRevision},
    consensus::{Durability, Term},
};

const BASIS_POINTS: u16 = 10_000;
const MAX_SAMPLE_AGE_MS: u64 = 3_600_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionLoad {
    pub cells: u32,
    pub logical_bytes: u64,
    pub writes_per_second: u64,
}

impl PartitionLoad {
    fn within(self, ceiling: Self) -> bool {
        self.cells <= ceiling.cells
            && self.logical_bytes <= ceiling.logical_bytes
            && self.writes_per_second <= ceiling.writes_per_second
    }

    fn sum(self, other: Self) -> Option<Self> {
        Some(Self {
            cells: self.cells.checked_add(other.cells)?,
            logical_bytes: self.logical_bytes.checked_add(other.logical_bytes)?,
            writes_per_second: self
                .writes_per_second
                .checked_add(other.writes_per_second)?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionLimits {
    /// A split requires a load dimension above this ceiling or sustained
    /// CPU/apply pressure. Merge ceilings must be strictly lower.
    pub split_ceiling: PartitionLoad,
    pub merge_ceiling: PartitionLoad,
    pub split_cpu_bps: u16,
    pub merge_cpu_bps: u16,
    pub max_queue_age_ms: u64,
    pub max_archive_lag_entries: u64,
    pub max_failure_domain_load_bps: u16,
    pub hot_cell_bytes: u64,
    pub hot_cell_writes_per_second: u64,
    pub split_samples: u8,
    pub merge_samples: u8,
    pub sample_interval_ms: u64,
    pub max_sample_age_ms: u64,
    pub cooldown_ms: u64,
    pub max_active_operations: u16,
}

/// A validated, explicit policy. Qualification determines deployable limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionPolicy(PartitionLimits);

impl PartitionPolicy {
    /// Explicit validated limits used by the bounded sampling owner.
    pub fn limits(self) -> PartitionLimits {
        self.0
    }

    pub fn new(limits: PartitionLimits) -> Option<Self> {
        let split = limits.split_ceiling;
        let merge = limits.merge_ceiling;
        (merge.cells < split.cells
            && merge.logical_bytes < split.logical_bytes
            && merge.writes_per_second < split.writes_per_second
            && limits.merge_cpu_bps < limits.split_cpu_bps
            && limits.split_cpu_bps <= BASIS_POINTS
            && (1..=BASIS_POINTS).contains(&limits.max_failure_domain_load_bps)
            && limits.max_queue_age_ms > 0
            && limits.hot_cell_bytes > 0
            && limits.hot_cell_writes_per_second > 0
            && limits.split_samples >= 2
            && limits.merge_samples >= 2
            && (1..=MAX_SAMPLE_AGE_MS).contains(&limits.max_sample_age_ms)
            && (1..=limits.max_sample_age_ms).contains(&limits.sample_interval_ms)
            && limits.cooldown_ms > 0
            && limits.max_active_operations > 0)
            .then_some(Self(limits))
    }
}

/// Exact observations an adapter must compare again before admitting work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionBasis {
    pub route: GroupRoute,
    pub directory_digest: [u8; 32],
    pub membership: MembershipRevision,
    pub term: Term,
    pub durability: Durability,
    /// Fingerprint of placement pool, separation and failure-domain policy.
    pub placement_class: [u8; 32],
}

/// A bounded hot-cell hint. The adapter supplies the next occupied key in
/// this range and whether there are occupied keys before the hot cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotCell {
    pub key: CellKey,
    pub next: Option<CellKey>,
    pub has_predecessor: bool,
    pub logical_bytes: u64,
    pub writes_per_second: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionSample {
    pub basis: PartitionBasis,
    /// Receiver-local monotonic time.
    pub sampled_at_ms: u64,
    /// Receiver-local lower bound from observed topology progress. The owner
    /// supplies a conservative bound when it begins tracking a new route.
    pub topology_changed_at_ms: u64,
    pub load: PartitionLoad,
    pub applies_per_second: u64,
    pub queue_age_ms: u64,
    pub archive_lag_entries: Option<u64>,
    pub leader_cpu_bps: u16,
    pub failure_domain_load_bps: u16,
    pub active_and_stable: bool,
    /// A verified occupied key with a nonempty occupied partition on each side.
    pub balanced_pivot: Option<CellKey>,
    pub hot_cell: Option<HotCell>,
    pub hot_writer: Option<HotCell>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitReason {
    GroupLoad,
    LeaderCpu,
    ApplyBacklog,
    IsolateHotCell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitReason {
    NoFreshSample,
    Incompatible,
    InTransition,
    ArchiveLag,
    DestinationPressure,
    QueuePressure,
    Cooldown,
    OperationLimit,
    Hysteresis,
    NoWholeCellPivot,
    DedicatedCell,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionDecision<'a> {
    Wait(WaitReason),
    Split {
        basis: &'a PartitionBasis,
        pivot: &'a CellKey,
        reason: SplitReason,
    },
    Merge {
        left: &'a PartitionBasis,
        right: &'a PartitionBasis,
    },
}

/// Bounded ephemeral state for one observed group. No samples are counted
/// twice. A changed basis, changed policy, or observation gap resets the window.
#[derive(Default)]
pub struct PartitionTracker {
    last: Option<PartitionSample>,
    policy: Option<PartitionPolicy>,
    hot: u8,
    quiet: u8,
}

impl PartitionTracker {
    pub fn observe(&mut self, policy: PartitionPolicy, sample: PartitionSample, now_ms: u64) {
        if !sample.valid() || !fresh(&sample, policy, now_ms) {
            *self = Self::default();
            return;
        }
        if self.policy != Some(policy)
            || self
                .last
                .as_ref()
                .is_some_and(|last| last.basis != sample.basis)
        {
            *self = Self::default();
        }
        let mut continuity = false;
        if let Some(last) = &self.last {
            let Some(elapsed) = sample.sampled_at_ms.checked_sub(last.sampled_at_ms) else {
                *self = Self::default();
                return;
            };
            if sample.topology_changed_at_ms < last.topology_changed_at_ms {
                *self = Self::default();
                return;
            }
            if elapsed < policy.0.sample_interval_ms {
                if &sample != last {
                    self.hot = 0;
                    self.quiet = 0;
                    self.last = Some(sample);
                }
                return;
            }
            continuity = elapsed <= policy.0.max_sample_age_ms
                && sample.topology_changed_at_ms == last.topology_changed_at_ms;
        }
        let stable = sample.ineligible(policy).is_none();
        let reason = sample.split_reason(policy);
        let same_split = continuity
            && self.last.as_ref().is_some_and(|last| {
                last.split_reason(policy) == reason
                    && (reason != Some(SplitReason::IsolateHotCell)
                        || sample.hot_candidate(policy).map(|hot| &hot.key)
                            == last.hot_candidate(policy).map(|hot| &hot.key))
            });
        self.hot = if stable && reason.is_some() {
            if same_split {
                self.hot.saturating_add(1)
            } else {
                1
            }
        } else {
            0
        };
        self.quiet = if stable && sample.quiet(policy) {
            if continuity {
                self.quiet.saturating_add(1)
            } else {
                1
            }
        } else {
            0
        };
        self.policy = Some(policy);
        self.last = Some(sample);
    }

    pub fn recommend_split(
        &self,
        policy: PartitionPolicy,
        now_ms: u64,
        active_operations: u16,
    ) -> PartitionDecision<'_> {
        let sample = match self.eligible(policy, now_ms, active_operations) {
            Ok(sample) => sample,
            Err(reason) => return PartitionDecision::Wait(reason),
        };
        if self.hot < policy.0.split_samples {
            return PartitionDecision::Wait(WaitReason::Hysteresis);
        }
        if sample.load.cells == 1 {
            return PartitionDecision::Wait(WaitReason::DedicatedCell);
        }
        let Some(reason) = sample.split_reason(policy) else {
            return PartitionDecision::Wait(WaitReason::Hysteresis);
        };
        let pivot = if reason == SplitReason::IsolateHotCell {
            sample.hot_candidate(policy).and_then(|hot| {
                if hot.has_predecessor {
                    Some(&hot.key)
                } else {
                    hot.next.as_ref()
                }
            })
        } else {
            sample.balanced_pivot.as_ref()
        };
        let Some(pivot) = pivot else {
            return PartitionDecision::Wait(WaitReason::NoWholeCellPivot);
        };
        PartitionDecision::Split {
            basis: &sample.basis,
            pivot,
            reason,
        }
    }

    pub fn recommend_merge<'a>(
        &'a self,
        right: &'a Self,
        policy: PartitionPolicy,
        now_ms: u64,
        active_operations: u16,
    ) -> PartitionDecision<'a> {
        let left = match self.eligible(policy, now_ms, active_operations) {
            Ok(sample) => sample,
            Err(reason) => return PartitionDecision::Wait(reason),
        };
        let other = match right.eligible(policy, now_ms, active_operations) {
            Ok(sample) => sample,
            Err(reason) => return PartitionDecision::Wait(reason),
        };
        if !left.compatible_neighbor(other) {
            return PartitionDecision::Wait(WaitReason::Incompatible);
        }
        if self.quiet < policy.0.merge_samples
            || right.quiet < policy.0.merge_samples
            || left
                .load
                .sum(other.load)
                .is_none_or(|load| !load.within(policy.0.merge_ceiling))
            || left.leader_cpu_bps.saturating_add(other.leader_cpu_bps) > policy.0.merge_cpu_bps
        {
            return PartitionDecision::Wait(WaitReason::Hysteresis);
        }
        PartitionDecision::Merge {
            left: &left.basis,
            right: &other.basis,
        }
    }

    fn eligible(
        &self,
        policy: PartitionPolicy,
        now_ms: u64,
        active_operations: u16,
    ) -> Result<&PartitionSample, WaitReason> {
        let sample = self
            .last
            .as_ref()
            .filter(|sample| self.policy == Some(policy) && fresh(sample, policy, now_ms))
            .ok_or(WaitReason::NoFreshSample)?;
        if let Some(reason) = sample.ineligible(policy) {
            return Err(reason);
        }
        if now_ms.saturating_sub(sample.topology_changed_at_ms) < policy.0.cooldown_ms {
            return Err(WaitReason::Cooldown);
        }
        if active_operations >= policy.0.max_active_operations {
            return Err(WaitReason::OperationLimit);
        }
        Ok(sample)
    }
}

fn fresh(sample: &PartitionSample, policy: PartitionPolicy, now_ms: u64) -> bool {
    now_ms
        .checked_sub(sample.sampled_at_ms)
        .is_some_and(|age| age <= policy.0.max_sample_age_ms)
}

impl PartitionSample {
    fn valid(&self) -> bool {
        let range = &self.basis.route.range;
        let is_pivot = |key: &CellKey| {
            range.contains(key) && !matches!(range.lower(), CellBound::Key(lower) if lower == key)
        };
        self.basis.directory_digest != [0; 32]
            && self.basis.placement_class != [0; 32]
            && self.basis.route.birth_digest != [0; 32]
            && self.basis.route.schema_digest != [0; 32]
            && self.basis.term.get() > 0
            && self.topology_changed_at_ms <= self.sampled_at_ms
            && self.leader_cpu_bps <= BASIS_POINTS
            && self.failure_domain_load_bps <= BASIS_POINTS
            && self
                .balanced_pivot
                .as_ref()
                .is_none_or(|pivot| self.load.cells >= 2 && is_pivot(pivot))
            && self.hot_cell.iter().chain(&self.hot_writer).all(|hot| {
                self.load.cells > 0
                    && range.contains(&hot.key)
                    && hot.logical_bytes <= self.load.logical_bytes
                    && hot.writes_per_second <= self.load.writes_per_second
                    && (!hot.has_predecessor || is_pivot(&hot.key))
                    && hot
                        .next
                        .as_ref()
                        .is_none_or(|next| next > &hot.key && is_pivot(next))
                    && self.load.cells
                        >= 1 + u32::from(hot.has_predecessor) + u32::from(hot.next.is_some())
                    && (self.load.cells == 1 || hot.has_predecessor || hot.next.is_some())
            })
    }

    fn ineligible(&self, policy: PartitionPolicy) -> Option<WaitReason> {
        if !self.active_and_stable {
            Some(WaitReason::InTransition)
        } else if self
            .archive_lag_entries
            .is_none_or(|lag| lag > policy.0.max_archive_lag_entries)
        {
            Some(WaitReason::ArchiveLag)
        } else if self.failure_domain_load_bps > policy.0.max_failure_domain_load_bps {
            Some(WaitReason::DestinationPressure)
        } else if self.queue_age_ms > policy.0.max_queue_age_ms {
            Some(WaitReason::QueuePressure)
        } else {
            None
        }
    }

    fn hot_candidate(&self, policy: PartitionPolicy) -> Option<&HotCell> {
        self.hot_cell.iter().chain(&self.hot_writer).find(|hot| {
            hot.logical_bytes > policy.0.hot_cell_bytes
                || hot.writes_per_second > policy.0.hot_cell_writes_per_second
        })
    }

    fn split_reason(&self, policy: PartitionPolicy) -> Option<SplitReason> {
        if self.hot_candidate(policy).is_some() {
            Some(SplitReason::IsolateHotCell)
        } else if !self.load.within(policy.0.split_ceiling) {
            Some(SplitReason::GroupLoad)
        } else if self.leader_cpu_bps >= policy.0.split_cpu_bps {
            Some(SplitReason::LeaderCpu)
        } else if self.queue_age_ms > 0 && self.load.writes_per_second > self.applies_per_second {
            Some(SplitReason::ApplyBacklog)
        } else {
            None
        }
    }

    fn quiet(&self, policy: PartitionPolicy) -> bool {
        self.split_reason(policy).is_none()
            && self.load.within(policy.0.merge_ceiling)
            && self.leader_cpu_bps <= policy.0.merge_cpu_bps
            && self.queue_age_ms == 0
            && self.load.writes_per_second <= self.applies_per_second
    }

    fn compatible_neighbor(&self, right: &Self) -> bool {
        let left = &self.basis;
        let right = &right.basis;
        left.directory_digest == right.directory_digest
            && left.route.history.cluster == right.route.history.cluster
            && left.route.history.namespace == right.route.history.namespace
            && left.route.history.group != right.route.history.group
            && left.route.schema_digest == right.route.schema_digest
            && left.durability == right.durability
            && left.placement_class == right.placement_class
            && left.route.range.upper() == right.route.range.lower()
    }
}

/// Fixed sampling resolution for the bounded per-owner write sketch.
pub const HOT_WRITE_WINDOW_MS: u64 = 1_000;
const HOT_WRITER_SLOTS: usize = 4;

#[derive(Default)]
pub struct WriteHeat {
    scope: Option<(Term, MembershipRevision)>,
    active: Option<HeatWindow>,
    previous: Option<(u64, CellId, u64)>,
}

struct HeatWindow {
    start_ms: u64,
    writers: [Option<(CellId, u64)>; HOT_WRITER_SLOTS],
}

impl HeatWindow {
    fn record(&mut self, cell: &CellId) {
        if let Some((_, writes)) = self
            .writers
            .iter_mut()
            .flatten()
            .find(|(key, _)| key == cell)
        {
            *writes = writes.saturating_add(1);
        } else if let Some(free) = self.writers.iter_mut().find(|slot| slot.is_none()) {
            *free = Some((cell.clone(), 1));
        } else {
            // Misra-Gries cancels one observation of every tracked key with
            // this new key. Each remaining count is a lower bound.
            for slot in &mut self.writers {
                if let Some((_, writes)) = slot {
                    *writes -= 1;
                    if *writes == 0 {
                        *slot = None;
                    }
                }
            }
        }
    }

    fn hottest(&self) -> Option<(&CellId, u64)> {
        self.writers
            .iter()
            .flatten()
            .max_by(|(left, lhs), (right, rhs)| lhs.cmp(rhs).then_with(|| right.cmp(left)))
            .map(|(cell, writes)| (cell, *writes))
    }
}

impl WriteHeat {
    /// Track at most four current names and one previous winner. An epoch
    /// change drops evidence that cannot contribute to the new rate.
    pub fn record(
        &mut self,
        term: Term,
        membership: MembershipRevision,
        now_ms: u64,
        cell: &CellId,
    ) {
        let scope = (term, membership);
        let start_ms = now_ms / HOT_WRITE_WINDOW_MS * HOT_WRITE_WINDOW_MS;
        if self.scope != Some(scope) {
            *self = Self {
                scope: Some(scope),
                ..Self::default()
            };
        }
        if self
            .active
            .as_ref()
            .is_none_or(|active| active.start_ms != start_ms)
        {
            self.previous = self.active.take().and_then(|active| {
                if active.start_ms.checked_add(HOT_WRITE_WINDOW_MS) != Some(start_ms) {
                    return None;
                }
                active
                    .hottest()
                    .map(|(cell, writes)| (active.start_ms, cell.clone(), writes))
            });
            self.active = Some(HeatWindow {
                start_ms,
                writers: Default::default(),
            });
        }
        if let Some(active) = &mut self.active {
            active.record(cell);
        }
    }

    /// Return the most recent *complete* window in the same leadership and
    /// membership. Quiet windows and old epochs never reuse a candidate.
    pub fn observed(
        &self,
        term: Term,
        membership: MembershipRevision,
        now_ms: u64,
    ) -> Option<(u64, &CellId, u64)> {
        if self.scope != Some((term, membership)) {
            return None;
        }
        let complete = (now_ms / HOT_WRITE_WINDOW_MS).checked_sub(1)? * HOT_WRITE_WINDOW_MS;
        if let Some(active) = self
            .active
            .as_ref()
            .filter(|active| active.start_ms == complete)
        {
            return active
                .hottest()
                .map(|(cell, writes)| (complete, cell, writes));
        }
        self.previous
            .as_ref()
            .filter(|(start_ms, _, _)| *start_ms == complete)
            .map(|(_, cell, writes)| (complete, cell, *writes))
    }
}

#[cfg(test)]
#[path = "cell_group_placement_tests.rs"]
mod tests;

/// Deployment policy for fresh RF3 children. Recovery slots remain unassigned
/// across the inspected pool, including while source groups retain their disks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionPlacementPolicy {
    pub pool: String,
    pub distinct_regions: bool,
    pub distinct_zones: bool,
    pub recovery_slots: u32,
}

impl PartitionPlacementPolicy {
    pub fn valid(&self) -> bool {
        !self.pool.is_empty()
            && self.pool.len() <= 64
            && self
                .pool
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            && (1..=32_768).contains(&self.recovery_slots)
    }
}

/// Advisory projection of fully charged physical and pending occupancy. The
/// adapter computes available slots with both storage and memory constraints.
#[derive(Clone, Debug)]
pub struct PartitionDestination {
    pub node: crate::consensus::NodeId,
    pub physical_host: crate::consensus::IncarnationId,
    pub region: String,
    pub zone: String,
    pub occupied: u32,
    pub capacity: u32,
    pub available: u32,
}

/// Search the bounded six-target window for one merge child or two split
/// children. Both are charged together, sources remain occupied, and a spare
/// pool is retained. Selection grants no reservation or membership permission.
pub fn select_partition_voters(
    nodes: &[PartitionDestination],
    policy: &PartitionPlacementPolicy,
    children: usize,
    physical_separation: bool,
) -> Option<Vec<[crate::consensus::NodeId; 3]>> {
    use std::collections::BTreeSet;
    if !policy.valid()
        || !(1..=2).contains(&children)
        || !(3..=6).contains(&nodes.len())
        || nodes.iter().map(|n| n.node).collect::<BTreeSet<_>>().len() != nodes.len()
        || nodes.iter().any(|n| {
            n.capacity == 0
                || n.capacity > 32_768
                || n.occupied > 32_768
                || n.available > n.capacity.saturating_sub(n.occupied)
        })
    {
        return None;
    }
    let mut triples = Vec::new();
    for a in 0..nodes.len() {
        for b in (a + 1)..nodes.len() {
            for c in (b + 1)..nodes.len() {
                let indices = [a, b, c];
                if indices.iter().any(|i| nodes[*i].available == 0)
                    || physical_separation
                        && indices
                            .iter()
                            .map(|i| nodes[*i].physical_host)
                            .collect::<BTreeSet<_>>()
                            .len()
                            != 3
                    || policy.distinct_regions
                        && indices
                            .iter()
                            .map(|i| &nodes[*i].region)
                            .collect::<BTreeSet<_>>()
                            .len()
                            != 3
                    || policy.distinct_zones
                        && indices
                            .iter()
                            .map(|i| (&nodes[*i].region, &nodes[*i].zone))
                            .collect::<BTreeSet<_>>()
                            .len()
                            != 3
                {
                    continue;
                }
                triples.push(indices);
            }
        }
    }
    let mut best = None;
    for left in &triples {
        let right: Vec<_> = if children == 1 {
            vec![None]
        } else {
            triples.iter().map(Some).collect()
        };
        for right in right {
            let mut counts = [0u32; 6];
            for i in left.iter().chain(right.into_iter().flatten()) {
                counts[*i] += 1;
            }
            if nodes
                .iter()
                .enumerate()
                .any(|(i, n)| counts[i] > n.available)
            {
                continue;
            }
            if nodes
                .iter()
                .enumerate()
                .map(|(i, n)| u64::from(n.available - counts[i]))
                .sum::<u64>()
                < u64::from(policy.recovery_slots)
            {
                continue;
            }
            let score = nodes
                .iter()
                .enumerate()
                .map(|(i, n)| u128::from(n.occupied + counts[i]) * 10_000 / u128::from(n.capacity))
                .max()?;
            let mut voters = vec![left.map(|i| nodes[i].node)];
            if let Some(right) = right {
                voters.push(right.map(|i| nodes[i].node));
            }
            for group in &mut voters {
                group.sort();
            }
            let retained_load: u128 = nodes
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    u128::from(counts[i]) * u128::from(n.occupied) * 10_000 / u128::from(n.capacity)
                })
                .sum();
            if best
                .as_ref()
                .is_none_or(|(old, load, ids)| (score, retained_load, &voters) < (*old, *load, ids))
            {
                best = Some((score, retained_load, voters));
            }
        }
    }
    best.map(|(_, _, voters)| voters)
}
