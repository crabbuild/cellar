//! Bounded leadership balancing. Observations select a request; only the Raft
//! owner can admit it against current term, configuration and actual progress.
use crate::consensus::{GroupScope, LogIndex, MemberId, Term};
use crate::raft_placement::{rank, NodeHealth, PlacedMember};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug)]
pub struct LeaderPolicy {
    pub max_sample_age_ms: u64,
    pub min_transfer_interval_ms: u64,
    /// Minimum utilization difference in basis points (one point = 0.01%).
    pub deadband_basis_points: u16,
}

pub struct LeaderView {
    pub scope: GroupScope,
    pub term: Term,
    pub revision: LogIndex,
    pub current: MemberId,
    pub committed: LogIndex,
    pub voters: Vec<PlacedMember>,
    pub joint: bool,
    pub repairing: bool,
    /// Receiver-local time of the last attempt or controller start. Failed
    /// attempts consume the interval too; retrying must not restart transfers.
    pub last_transfer_ms: u64,
}

#[derive(Clone, Debug)]
pub struct LeaderSample {
    pub binding: PlacedMember,
    pub sampled_ms: u64,
    pub health: NodeHealth,
    pub compatible: bool,
    pub draining: bool,
    pub leaders: u32,
    pub leader_capacity: u32,
    pub matched: LogIndex,
    pub applied: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderMove {
    pub scope: GroupScope,
    pub expected_term: Term,
    pub expected_revision: LogIndex,
    pub target: PlacedMember,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaderError {
    InvalidPolicy,
    InvalidView,
    InvalidSample,
}

impl std::fmt::Display for LeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft leadership placement: {self:?}")
    }
}

impl std::error::Error for LeaderError {}

/// Select one current voter with fresh compatible applied/caught-up state.
/// Normal balancing must strictly reduce peak leader utilization after the
/// move, without reversing the relative load of the two nodes. Draining may
/// bypass the load threshold, but never target eligibility or the cooldown.
pub fn select_leader(
    view: &LeaderView,
    samples: &[LeaderSample],
    policy: LeaderPolicy,
    now_ms: u64,
) -> Result<Option<LeaderMove>, LeaderError> {
    if !(1..=3_600_000).contains(&policy.max_sample_age_ms)
        || !(1..=3_600_000).contains(&policy.min_transfer_interval_ms)
        || policy.deadband_basis_points > 10_000
    {
        return Err(LeaderError::InvalidPolicy);
    }
    if view.scope.validate().is_err()
        || view.term.get() == 0
        || view.term.get() >= u64::MAX - 2
        || view.revision.get() == 0
        || view.revision.get() >= u64::MAX - 2
        || view.committed.get() == 0
        || view.committed.get() >= u64::MAX - 2
        || !(3..=16).contains(&view.voters.len())
        || view.last_transfer_ms > now_ms
    {
        return Err(LeaderError::InvalidView);
    }
    let mut members = BTreeSet::new();
    let mut nodes = BTreeSet::new();
    let mut disks = BTreeSet::new();
    if view.voters.iter().any(|voter| {
        !members.insert(voter.member) || !nodes.insert(voter.node) || !disks.insert(voter.disk)
    }) || !members.contains(&view.current)
    {
        return Err(LeaderError::InvalidView);
    }
    let mut observed = BTreeSet::new();
    if samples.len() > 16
        || samples.iter().any(|sample| {
            !observed.insert(sample.binding.member)
                || !view.voters.contains(&sample.binding)
                || sample.sampled_ms > now_ms
                || sample.leader_capacity == 0
                || sample.matched.get() >= u64::MAX - 2
                || sample.applied.get() >= u64::MAX - 2
        })
    {
        return Err(LeaderError::InvalidSample);
    }
    if view.joint
        || view.repairing
        || now_ms - view.last_transfer_ms < policy.min_transfer_interval_ms
    {
        return Ok(None);
    }
    let fresh = |sample: &LeaderSample| {
        now_ms - sample.sampled_ms <= policy.max_sample_age_ms
            && sample.health == NodeHealth::Healthy
    };
    let Some(current) = samples.iter().find(|s| s.binding.member == view.current) else {
        return Ok(None);
    };
    if !fresh(current) || current.leaders == 0 {
        return Ok(None);
    }
    let c = u128::from(current.leaders);
    let capacity = u128::from(current.leader_capacity);
    let candidate = samples
        .iter()
        .filter(|target| {
            if target.binding.member == view.current
                || !fresh(target)
                || !target.compatible
                || target.draining
                || target.leaders >= target.leader_capacity
                || target.matched < view.committed
                || target.applied < view.committed
            {
                return false;
            }
            if current.draining {
                return true;
            }
            let t = u128::from(target.leaders);
            let target_capacity = u128::from(target.leader_capacity);
            let difference = (c * target_capacity).checked_sub(t * capacity);
            difference.is_some_and(|difference| {
                difference > 0
                    && difference * 10_000
                        >= u128::from(policy.deadband_basis_points) * capacity * target_capacity
                    && (c - 1) * target_capacity >= (t + 1) * capacity
                    && (t + 1) * capacity < c * target_capacity
            })
        })
        .min_by(|left, right| {
            ((u128::from(left.leaders) + 1) * u128::from(right.leader_capacity))
                .cmp(&((u128::from(right.leaders) + 1) * u128::from(left.leader_capacity)))
                .then_with(|| {
                    rank(&view.scope, right.binding.node).cmp(&rank(&view.scope, left.binding.node))
                })
                .then_with(|| left.binding.node.cmp(&right.binding.node))
        });
    Ok(candidate.map(|target| LeaderMove {
        scope: view.scope.clone(),
        expected_term: view.term,
        expected_revision: view.revision,
        target: target.binding.clone(),
    }))
}

#[cfg(test)]
#[path = "raft_leadership_tests.rs"]
mod tests;
