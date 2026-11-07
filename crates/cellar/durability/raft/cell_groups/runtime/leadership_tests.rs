// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn history(id: u8) -> GroupHistory {
    GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([id; 16]).unwrap(),
        incarnation: IncarnationId::new([7; 16]).unwrap(),
    }
}

fn groups() -> BTreeMap<GroupHistory, bool> {
    (1..=33)
        .map(|id| (history(id), id.is_multiple_of(7)))
        .collect()
}

#[test]
fn leadership_scan_bounds_owner_work_and_publishes_only_complete_population() {
    let groups = groups();
    let mut scan = Scan::new(10, 33);
    let mut total = 0;
    loop {
        let mut calls = 0;
        let batch = capture(&groups, 4, scan.after, |leader| {
            calls += 1;
            Ok(*leader)
        })
        .unwrap();
        assert!(calls <= BATCH_GROUPS);
        assert_eq!(batch.count, calls);
        total += calls;
        if let Some(sample) = scan.accept(batch).unwrap() {
            assert_eq!(
                (sample.groups, sample.leaders, sample.started_at_ms),
                (33, 4, 10)
            );
            break;
        }
        assert!(total < 33);
    }
    assert_eq!(total, 33);
}

#[test]
fn leadership_scan_same_count_different_roster_revision_refuses() {
    let groups = groups();
    let mut scan = Scan::new(10, 33);
    assert!(scan
        .accept(capture(&groups, 4, None, |l| Ok(*l)).unwrap())
        .unwrap()
        .is_none());
    assert!(matches!(
        scan.accept(capture(&groups, 5, scan.after, |l| Ok(*l)).unwrap()),
        Err(RuntimeError::NotReady)
    ));
}

#[test]
fn leadership_scan_failed_owner_never_emits_partial_headroom() {
    let groups = groups();
    let mut scan = Scan::new(10, 33);
    assert!(scan
        .accept(capture(&groups, 4, None, |l| Ok(*l)).unwrap())
        .unwrap()
        .is_none());
    let mut calls = 0;
    let result = capture(&groups, 4, scan.after, |_| {
        calls += 1;
        if calls == 3 {
            Err(RuntimeError::GroupQuarantined)
        } else {
            Ok(false)
        }
    });
    assert!(matches!(result, Err(RuntimeError::GroupQuarantined)));
    assert_eq!(calls, 3);
}

#[test]
fn leadership_scan_missing_suffix_and_repeated_cursor_refuse() {
    let groups = groups();
    let mut scan = Scan::new(10, 33);
    let mut batch = capture(&groups, 4, None, |l| Ok(*l)).unwrap();
    batch.next = None;
    assert!(matches!(scan.accept(batch), Err(RuntimeError::NotReady)));
    let mut scan = Scan::new(10, 33);
    let first = capture(&groups, 4, None, |l| Ok(*l)).unwrap();
    let repeated = first.next;
    assert!(scan.accept(first).unwrap().is_none());
    let mut second = capture(&groups, 4, scan.after, |l| Ok(*l)).unwrap();
    second.next = repeated;
    assert!(matches!(
        scan.accept(second),
        Err(RuntimeError::Protocol(_))
    ));
}

#[test]
fn leadership_scan_excess_counts_and_empty_continuations_refuse() {
    for (population, count, leaders, next) in [
        (34, 16, 1, Some(history(16))),
        (33, 16, 17, Some(history(16))),
        (33, 0, 0, Some(history(1))),
    ] {
        let mut scan = Scan::new(10, 33);
        assert!(matches!(
            scan.accept(LeadershipBatch {
                revision: 4,
                population,
                count,
                leaders,
                next
            }),
            Err(RuntimeError::Protocol(_))
        ));
    }
}
