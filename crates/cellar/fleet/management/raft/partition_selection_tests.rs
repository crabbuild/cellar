// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::cell_group_placement::{
    PartitionBasis, PartitionLimits, PartitionLoad, PartitionSample,
};
use cellar_logic::{
    cell_group::{CellId, CellRange, GroupHistory, MembershipRevision, RouteEpoch},
    consensus::{ClusterId, Durability, GroupId, IncarnationId, NamespaceId, Term},
};

fn policy() -> PartitionPolicy {
    PartitionPolicy::new(PartitionLimits {
        split_ceiling: PartitionLoad {
            cells: 8,
            logical_bytes: 1_000,
            writes_per_second: 80,
        },
        merge_ceiling: PartitionLoad {
            cells: 4,
            logical_bytes: 450,
            writes_per_second: 20,
        },
        split_cpu_bps: 8_000,
        merge_cpu_bps: 3_000,
        max_queue_age_ms: 100,
        max_archive_lag_entries: 4,
        max_failure_domain_load_bps: 8_500,
        hot_cell_bytes: 600,
        hot_cell_writes_per_second: 60,
        split_samples: 2,
        merge_samples: 3,
        sample_interval_ms: 10,
        max_sample_age_ms: 100,
        cooldown_ms: 50,
        max_active_operations: 1,
    })
    .unwrap()
}

// A deliberate digest collision. Full IDs still order the pivots.
fn key(value: &str) -> CellKey {
    CellKey::from_digest([7; 32], CellId::new(value).unwrap())
}

fn sample(group: u8, range: CellRange) -> PartitionSample {
    PartitionSample {
        basis: PartitionBasis {
            route: GroupRoute {
                history: GroupHistory {
                    cluster: ClusterId::new([1; 16]).unwrap(),
                    namespace: NamespaceId::new([2; 16]).unwrap(),
                    group: GroupId::new([group; 16]).unwrap(),
                    incarnation: IncarnationId::new([group; 16]).unwrap(),
                },
                range,
                epoch: RouteEpoch::new(1).unwrap(),
                birth_digest: [group; 32],
                schema_digest: [3; 32],
            },
            directory_digest: [4; 32],
            membership: MembershipRevision::new(1).unwrap(),
            term: Term::new(2),
            durability: Durability::Quorum,
            placement_class: [5; 32],
        },
        sampled_at_ms: 100,
        topology_changed_at_ms: 0,
        load: PartitionLoad {
            cells: 2,
            logical_bytes: 150,
            writes_per_second: 5,
        },
        applies_per_second: 5,
        queue_age_ms: 0,
        archive_lag_entries: Some(0),
        leader_cpu_bps: 400,
        failure_domain_load_bps: 3_500,
        active_and_stable: true,
        balanced_pivot: None,
        hot_cell: None,
        hot_writer: None,
    }
}

fn observe(tracker: &mut PartitionTracker, mut sample: PartitionSample, times: &[u64]) {
    for &at in times {
        sample.sampled_at_ms = at;
        tracker.observe(policy(), sample.clone(), at);
    }
}

#[test]
fn split_selection_rotates_across_namespaces_and_rejects_stale_samples() {
    let mut a = sample(10, CellRange::all());
    a.load.cells = 9;
    a.balanced_pivot = Some(key("middle"));
    let mut b = a.clone();
    b.basis.route.history.group = GroupId::new([20; 16]).unwrap();
    b.basis.route.history.namespace = NamespaceId::new([3; 16]).unwrap();
    let (mut first, mut second) = (PartitionTracker::default(), PartitionTracker::default());
    observe(&mut first, a.clone(), &[100, 110, 120]);
    observe(&mut second, b.clone(), &[100, 110, 120]);
    let groups = [(&a.basis.route, &first), (&b.basis.route, &second)];
    let chosen = select(groups.into_iter(), policy(), 120, None).unwrap();
    assert_eq!(chosen.0, a.basis.route.history);
    assert!(matches!(
        chosen.1,
        RaftFleetPartitionRecommendation::Split { .. }
    ));
    let next = select(groups.into_iter(), policy(), 120, Some(chosen.0)).unwrap();
    assert_eq!(next.0, b.basis.route.history);
    assert_eq!(
        select(groups.into_iter(), policy(), 120, Some(next.0))
            .unwrap()
            .0,
        chosen.0
    );
    assert!(select(groups.into_iter(), policy(), 221, None).is_none());
}

#[test]
fn merge_selection_uses_range_order_and_refuses_foreign_or_unarchived_neighbors() {
    let (left, right) = CellRange::all().split(key("middle")).unwrap();
    let a = sample(20, left);
    let b = sample(10, right);
    let (mut first, mut second) = (PartitionTracker::default(), PartitionTracker::default());
    observe(&mut first, a.clone(), &[100, 110, 120]);
    observe(&mut second, b.clone(), &[100, 110, 120]);
    let chosen = select(
        [(&b.basis.route, &second), (&a.basis.route, &first)].into_iter(),
        policy(),
        120,
        None,
    )
    .unwrap();
    assert_eq!(chosen.0, a.basis.route.history);
    assert!(
        matches!(chosen.1, RaftFleetPartitionRecommendation::Merge { left, right }
        if left.route == a.basis.route && right.route == b.basis.route)
    );
    for foreign in [false, true] {
        let mut incompatible = b.clone();
        if foreign {
            incompatible.basis.route.history.namespace = NamespaceId::new([3; 16]).unwrap();
        } else {
            incompatible.archive_lag_entries = None;
        }
        let mut blocked = PartitionTracker::default();
        observe(&mut blocked, incompatible.clone(), &[100, 110, 120]);
        assert!(select(
            [
                (&a.basis.route, &first),
                (&incompatible.basis.route, &blocked)
            ]
            .into_iter(),
            policy(),
            120,
            None
        )
        .is_none());
    }
}

#[test]
fn pressured_split_precedes_quiet_merge_without_bypassing_hysteresis() {
    let (left, right) = CellRange::all().split(key("middle")).unwrap();
    let a = sample(20, left);
    let b = sample(10, right);
    let mut hot = sample(30, CellRange::all());
    hot.basis.route.history.namespace = NamespaceId::new([3; 16]).unwrap();
    hot.load.cells = 9;
    hot.balanced_pivot = Some(key("middle"));
    let (mut first, mut second, mut third) = (
        PartitionTracker::default(),
        PartitionTracker::default(),
        PartitionTracker::default(),
    );
    observe(&mut first, a.clone(), &[100, 110, 120]);
    observe(&mut second, b.clone(), &[100, 110, 120]);
    observe(&mut third, hot.clone(), &[120]);
    let chosen = select(
        [
            (&a.basis.route, &first),
            (&b.basis.route, &second),
            (&hot.basis.route, &third),
        ]
        .into_iter(),
        policy(),
        120,
        None,
    )
    .unwrap();
    assert!(matches!(
        chosen.1,
        RaftFleetPartitionRecommendation::Merge { .. }
    ));
    observe(&mut third, hot.clone(), &[130]);
    let chosen = select(
        [
            (&a.basis.route, &first),
            (&b.basis.route, &second),
            (&hot.basis.route, &third),
        ]
        .into_iter(),
        policy(),
        130,
        None,
    )
    .unwrap();
    assert_eq!(chosen.0, hot.basis.route.history);
    assert!(matches!(
        chosen.1,
        RaftFleetPartitionRecommendation::Split { .. }
    ));
}
