use super::*;

fn identity(group: u8) -> ReplicaIdentity {
    ReplicaIdentity {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: format!("Counter:{group}"),
            group: GroupId::new([group; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        member: MemberId::new(1).unwrap(),
        node: NodeId::new([1; 16]).unwrap(),
        disk: DiskLineage::new([2; 16]).unwrap(),
        process: ProcessGeneration::new([3; 16]).unwrap(),
    }
}

fn counters(status: &GroupStatus) -> Option<RaftGroupResources> {
    let lag = u64::from(status.replica.scope.group.bytes()[0]);
    Some(RaftGroupResources {
        leaders: usize::from(status.is_leader),
        voters: usize::from(!status.is_leader),
        apply_lag_entries: lag,
        max_apply_lag_entries: lag,
        archive_lag_entries: lag * 2,
        max_archive_lag_entries: lag * 2,
        ..Default::default()
    })
}

fn sample(group: u8, leader: bool) -> (ReplicaIdentity, Result<GroupStatus, RuntimeError>) {
    let replica = identity(group);
    let status = GroupStatus {
        replica: replica.clone(),
        term: Term::new(2),
        leader: Some(if leader { 1 } else { 2 }),
        is_leader: leader,
        membership_revision: LogIndex::new(1),
        member_high_water: 3,
        protocol_ready: true,
        persisted: LogIndex::new(2),
        committed: LogIndex::new(2),
        applied: LogPosition {
            index: LogIndex::new(2),
            term: Term::new(2),
        },
        checkpoint: LogPosition {
            index: LogIndex::new(0),
            term: Term::new(0),
        },
        awaiting_ready: false,
        awaiting_apply: false,
        election_timeout: 10,
        source_observed_ms: Some(100),
    };
    (replica, Ok(status))
}

#[test]
fn partial_and_failed_scans_never_refresh_or_replace_a_complete_sample() {
    let mut scanner = Scanner::new(RaftInspectionScanOptions::default());
    let population = Population {
        cells: 2,
        groups: 2,
    };
    assert!(scanner.view(100, population, false, 8).is_none());
    assert!(scanner.begin(100, population, false));
    scanner.accept(
        Completed {
            sequence: 1,
            result: vec![sample(1, true)],
        },
        110,
        population,
        false,
        counters,
    );
    assert!(scanner.view(110, population, false, 8).is_none());
    assert!(scanner.begin(111, population, false));
    scanner.accept(
        Completed {
            sequence: 1,
            result: vec![sample(2, false)],
        },
        120,
        population,
        false,
        counters,
    );
    assert_eq!(
        scanner.view(130, population, false, 8),
        Some(LeaderLoad {
            leaders: 1,
            capacity: 8,
            sample_age_ms: 30
        })
    );
    assert!(scanner.begin(400, population, false));
    let complete = scanner.cached.as_ref().unwrap();
    assert_eq!(complete.resources.attached_groups, 2);
    assert_eq!(
        (complete.resources.leaders, complete.resources.voters),
        (1, 1)
    );
    assert_eq!(
        (
            complete.resources.apply_lag_entries,
            complete.resources.max_apply_lag_entries
        ),
        (3, 2)
    );
    assert_eq!(
        (
            complete.resources.archive_lag_entries,
            complete.resources.max_archive_lag_entries
        ),
        (6, 4)
    );
    scanner.accept(
        Completed {
            sequence: 2,
            result: vec![sample(1, false)],
        },
        410,
        population,
        false,
        counters,
    );
    assert_eq!(scanner.view(410, population, false, 8).unwrap().leaders, 1);
    scanner.accept(
        Completed {
            sequence: 2,
            result: vec![(identity(2), Err(RuntimeError::Overloaded))],
        },
        420,
        population,
        false,
        counters,
    );
    assert_eq!(
        scanner.view(450, population, false, 8),
        Some(LeaderLoad {
            leaders: 1,
            capacity: 8,
            sample_age_ms: 350
        })
    );
    assert!(scanner.view(99, population, false, 8).is_none());
    let observer = GroupResourceObserver::default();
    let cached = scanner.cached.as_ref().unwrap();
    *observer.0.lock().unwrap() = Some((cached.started, cached.resources));
    assert_eq!(observer.capture(450).unwrap().sample_age_ms, 350);
    assert_eq!(observer.capture(450).unwrap().apply_lag_entries, 3);
    assert!(observer.capture(99).is_none());
}

#[test]
fn population_change_and_old_batches_cannot_complete_a_new_scan() {
    let mut scanner = Scanner::new(RaftInspectionScanOptions::default());
    let old = Population {
        cells: 2,
        groups: 2,
    };
    let current = Population {
        cells: 3,
        groups: 3,
    };
    assert!(scanner.begin(100, old, false));
    scanner.accept(
        Completed {
            sequence: 1,
            result: vec![sample(1, true)],
        },
        110,
        old,
        false,
        counters,
    );
    assert!(!scanner.begin(120, current, false));
    assert!(scanner.view(120, current, false, 8).is_none());
    assert!(scanner.begin(400, current, false));
    scanner.busy = true;
    scanner.accept(
        Completed {
            sequence: 1,
            result: vec![sample(2, true)],
        },
        410,
        current,
        false,
        counters,
    );
    assert!(scanner.busy);
    assert!(scanner.view(410, current, false, 8).is_none());
    scanner.accept(
        Completed {
            sequence: 2,
            result: vec![sample(1, true), sample(2, false)],
        },
        420,
        current,
        false,
        counters,
    );
    assert!(scanner.view(420, current, false, 8).is_none());
    scanner.accept(
        Completed {
            sequence: 2,
            result: vec![sample(3, true)],
        },
        430,
        current,
        false,
        counters,
    );
    assert_eq!(
        scanner.view(450, current, false, 8),
        Some(LeaderLoad {
            leaders: 2,
            capacity: 8,
            sample_age_ms: 50
        })
    );
    assert!(scanner.view(450, old, false, 8).is_none());
    assert!(scanner.view(450, current, true, 8).is_none());
}

#[test]
fn aggregate_overflow_and_unavailable_contributions_preserve_old_age() {
    for unavailable in [false, true] {
        let mut scanner = Scanner::new(RaftInspectionScanOptions::default());
        let population = Population {
            cells: 2,
            groups: 3,
        };
        assert!(scanner.begin(100, population, false));
        scanner.accept(
            Completed {
                sequence: 1,
                result: vec![sample(1, true), sample(2, false)],
            },
            110,
            population,
            false,
            counters,
        );
        assert_eq!(scanner.cached.as_ref().unwrap().resources.parked_groups, 1);
        assert!(scanner.begin(400, population, false));
        scanner.accept(
            Completed {
                sequence: 2,
                result: vec![sample(1, true), sample(2, false)],
            },
            410,
            population,
            false,
            |status| {
                if unavailable {
                    return None;
                }
                Some(RaftGroupResources {
                    apply_lag_entries: if status.is_leader { u64::MAX } else { 1 },
                    ..Default::default()
                })
            },
        );
        assert!(scanner.cycle.is_none());
        assert_eq!(
            scanner.cached.as_ref().unwrap().resources.apply_lag_entries,
            3
        );
        assert_eq!(
            scanner
                .view(450, population, false, 8)
                .unwrap()
                .sample_age_ms,
            350
        );
    }
}

#[test]
fn scan_refuses_deadline_identity_order_and_cardinality_errors() {
    let population = Population {
        cells: 2,
        groups: 2,
    };
    let mut wrong = sample(2, true);
    wrong.1.as_mut().unwrap().replica.process = ProcessGeneration::new([99; 16]).unwrap();
    for (now, result) in [
        (5100, vec![sample(1, true), sample(2, true)]),
        (110, vec![sample(1, true), wrong]),
        (110, vec![sample(1, true), sample(1, true)]),
        (110, vec![sample(2, true), sample(1, true)]),
        (110, vec![sample(1, true), sample(2, true), sample(3, true)]),
        (110, vec![]),
    ] {
        let mut scanner = Scanner::new(RaftInspectionScanOptions::default());
        assert!(scanner.begin(100, population, false));
        scanner.busy = true;
        scanner.accept(
            Completed {
                sequence: 1,
                result,
            },
            now,
            population,
            false,
            counters,
        );
        assert!(!scanner.busy);
        assert!(scanner.cycle.is_none());
        assert!(scanner.view(now, population, false, 8).is_none());
    }
    let mut empty = Scanner::new(RaftInspectionScanOptions::default());
    let population = Population {
        cells: 0,
        groups: 0,
    };
    assert!(!empty.begin(100, population, false));
    assert_eq!(
        empty.view(100, population, false, 8),
        Some(LeaderLoad {
            leaders: 0,
            capacity: 8,
            sample_age_ms: 0
        })
    );
}
