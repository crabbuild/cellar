// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::{
    cell_group::{CellId, CellRange, GroupHistory, RouteEpoch},
    consensus::{ClusterId, GroupId, IncarnationId, NamespaceId},
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
    }
}

fn observe(tracker: &mut PartitionTracker, mut sample: PartitionSample, times: &[u64]) {
    for &at in times {
        sample.sampled_at_ms = at;
        tracker.observe(policy(), sample.clone(), at);
    }
}

#[test]
fn scheduler_hysteresis_and_hot_isolation() {
    let mut tracker = PartitionTracker::default();
    let mut view = sample(10, CellRange::all());
    view.load.cells = 3;
    view.load.logical_bytes = 800;
    view.hot_cell = Some(HotCell {
        key: key("middle"),
        next: Some(key("right")),
        has_predecessor: true,
        logical_bytes: 700,
        writes_per_second: 4,
    });
    observe(&mut tracker, view.clone(), &[100, 100]);
    assert_eq!(
        tracker.recommend_split(policy(), 100, 0),
        PartitionDecision::Wait(WaitReason::Hysteresis)
    );
    observe(&mut tracker, view.clone(), &[110]);
    assert_eq!(
        tracker.recommend_split(policy(), 110, 0),
        PartitionDecision::Split {
            basis: &view.basis,
            pivot: &key("middle"),
            reason: SplitReason::IsolateHotCell,
        }
    );

    // The right child inherits a recent change. Once cooled down, a second
    // whole-key split can separate the now-leftmost hot cell.
    let (_, right) = view.basis.route.range.split(key("middle")).unwrap();
    view.basis.route.range = right;
    view.basis.route.epoch = RouteEpoch::new(2).unwrap();
    view.topology_changed_at_ms = 115;
    view.load.cells = 2;
    view.hot_cell.as_mut().unwrap().has_predecessor = false;
    observe(&mut tracker, view.clone(), &[120, 130]);
    assert_eq!(
        tracker.recommend_split(policy(), 130, 0),
        PartitionDecision::Wait(WaitReason::Cooldown)
    );
    assert_eq!(
        tracker.recommend_split(policy(), 170, 0),
        PartitionDecision::Split {
            basis: &view.basis,
            pivot: &key("right"),
            reason: SplitReason::IsolateHotCell,
        }
    );
    view.load.cells = 1;
    view.hot_cell.as_mut().unwrap().next = None;
    observe(&mut tracker, view, &[180, 190]);
    assert_eq!(
        tracker.recommend_split(policy(), 190, 0),
        PartitionDecision::Wait(WaitReason::DedicatedCell)
    );
}

#[test]
fn quiet_neighbors_need_a_shared_basis_and_combined_headroom() {
    let (left, right) = CellRange::all().split(key("pivot")).unwrap();
    let l = sample(10, left);
    let r = sample(11, right);
    let (mut a, mut b) = (PartitionTracker::default(), PartitionTracker::default());
    observe(&mut a, l.clone(), &[100, 110, 120]);
    observe(&mut b, r.clone(), &[100, 110]);
    assert_eq!(
        a.recommend_merge(&b, policy(), 120, 0),
        PartitionDecision::Wait(WaitReason::Hysteresis)
    );
    observe(&mut b, r.clone(), &[120]);
    assert_eq!(
        a.recommend_merge(&b, policy(), 120, 0),
        PartitionDecision::Merge {
            left: &l.basis,
            right: &r.basis,
        }
    );
    assert_eq!(
        b.recommend_merge(&a, policy(), 120, 0),
        PartitionDecision::Wait(WaitReason::Incompatible)
    );
    let mut too_large = r.clone();
    too_large.load.logical_bytes = 350;
    observe(&mut b, too_large, &[130, 140, 150]);
    assert_eq!(
        a.recommend_merge(&b, policy(), 150, 0),
        PartitionDecision::Wait(WaitReason::Hysteresis)
    );
    for incompatible in [
        PartitionBasis {
            durability: Durability::S3,
            ..r.basis.clone()
        },
        PartitionBasis {
            placement_class: [8; 32],
            ..r.basis.clone()
        },
        PartitionBasis {
            directory_digest: [9; 32],
            ..r.basis.clone()
        },
    ] {
        let other = PartitionSample {
            basis: incompatible,
            ..r.clone()
        };
        observe(&mut b, other, &[160, 170, 180]);
        assert_eq!(
            a.recommend_merge(&b, policy(), 180, 0),
            PartitionDecision::Wait(WaitReason::Incompatible)
        );
    }
}

#[test]
fn new_term_and_unknown_archive_invalidate_an_old_recommendation() {
    let mut tracker = PartitionTracker::default();
    let mut view = sample(10, CellRange::all());
    view.load.cells = 9;
    view.balanced_pivot = Some(key("pivot"));
    observe(&mut tracker, view.clone(), &[100, 110]);
    assert!(matches!(
        tracker.recommend_split(policy(), 110, 0),
        PartitionDecision::Split {
            reason: SplitReason::GroupLoad,
            ..
        }
    ));
    assert_eq!(
        tracker.recommend_split(policy(), 110, 1),
        PartitionDecision::Wait(WaitReason::OperationLimit)
    );
    assert_eq!(
        tracker.recommend_split(policy(), 211, 0),
        PartitionDecision::Wait(WaitReason::NoFreshSample)
    );
    view.basis.term = Term::new(3);
    view.basis.membership = MembershipRevision::new(2).unwrap();
    observe(&mut tracker, view.clone(), &[120]);
    assert_eq!(
        tracker.recommend_split(policy(), 120, 0),
        PartitionDecision::Wait(WaitReason::Hysteresis)
    );
    view.archive_lag_entries = None;
    observe(&mut tracker, view.clone(), &[121]);
    assert_eq!(
        tracker.recommend_split(policy(), 121, 0),
        PartitionDecision::Wait(WaitReason::ArchiveLag)
    );
    view.archive_lag_entries = Some(0);
    view.active_and_stable = false;
    observe(&mut tracker, view, &[130]);
    assert_eq!(
        tracker.recommend_split(policy(), 130, 0),
        PartitionDecision::Wait(WaitReason::InTransition)
    );
}
