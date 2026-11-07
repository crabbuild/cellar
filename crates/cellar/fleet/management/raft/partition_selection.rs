// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fair local candidate selection. Fresh policy proofs remain advisory.
use super::partition::RaftFleetPartitionRecommendation;
use cellar_logic::{
    cell_group::{GroupHistory, GroupRoute},
    cell_group_placement::{PartitionDecision, PartitionPolicy, PartitionTracker},
};

#[cfg(test)]
#[path = "partition_selection_tests.rs"]
mod tests;

pub(super) fn select<'a>(
    groups: impl Iterator<Item = (&'a GroupRoute, &'a PartitionTracker)> + Clone,
    policy: PartitionPolicy,
    now: u64,
    cursor: Option<GroupHistory>,
) -> Option<(GroupHistory, RaftFleetPartitionRecommendation)> {
    let rank = |history: GroupHistory| (cursor.is_some_and(|after| history <= after), history);
    let mut selected = None;
    for (route, tracker) in groups.clone() {
        if let decision @ PartitionDecision::Split { .. } = tracker.recommend_split(policy, now, 0)
        {
            if selected
                .as_ref()
                .is_none_or(|(history, _)| rank(route.history) < rank(*history))
            {
                selected = Some((
                    route.history,
                    RaftFleetPartitionRecommendation::from_decision(decision),
                ));
            }
        }
    }
    if selected.is_some() {
        return selected;
    }
    // Range order, not opaque group IDs, defines merge adjacency. Comparing
    // neighboring routes is linear after this bounded local sort; pure policy
    // independently refuses foreign scopes, gaps, stale samples or instability.
    let mut ordered: Vec<_> = groups.collect();
    ordered.sort_by(|(a, _), (b, _)| {
        (a.history.cluster, a.history.namespace, a.range.lower()).cmp(&(
            b.history.cluster,
            b.history.namespace,
            b.range.lower(),
        ))
    });
    for pair in ordered.windows(2) {
        let (left, tracker) = pair[0];
        if let decision @ PartitionDecision::Merge { .. } =
            tracker.recommend_merge(pair[1].1, policy, now, 0)
        {
            if selected
                .as_ref()
                .is_none_or(|(history, _)| rank(left.history) < rank(*history))
            {
                selected = Some((
                    left.history,
                    RaftFleetPartitionRecommendation::from_decision(decision),
                ));
            }
        }
    }
    selected
}
