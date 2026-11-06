// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded recommendations for partition sizing. Every result is advisory.
//! Admission belongs to the current directory, Fleet, and Raft owners.

use crate::{
    cell_group::{CellBound, CellKey, GroupRoute, MembershipRevision},
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
                        || sample.hot_cell.as_ref().map(|hot| &hot.key)
                            == last.hot_cell.as_ref().map(|hot| &hot.key))
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
            sample.hot_cell.as_ref().and_then(|hot| {
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
            && self.hot_cell.as_ref().is_none_or(|hot| {
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

    fn split_reason(&self, policy: PartitionPolicy) -> Option<SplitReason> {
        if self.hot_cell.as_ref().is_some_and(|hot| {
            hot.logical_bytes > policy.0.hot_cell_bytes
                || hot.writes_per_second > policy.0.hot_cell_writes_per_second
        }) {
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

#[cfg(test)]
#[path = "cell_group_placement_tests.rs"]
mod tests;
