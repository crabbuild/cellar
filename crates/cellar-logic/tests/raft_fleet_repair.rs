// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Required repair selection; the host owns isolation and quorum provenance.
use cellar_logic::{consensus::*, raft_fleet::*, raft_placement::PlacedMember};

fn id(n: u8) -> NodeId {
    NodeId::new([n; 16]).unwrap()
}

fn bytes(n: u64) -> Resources {
    Resources {
        engine_slots: n,
        durable_bytes: n * 10,
        memory_bytes: n * 10,
    }
}

fn fixture() -> (IsolatedRepairView, Vec<TopologyNode>, TopologyPolicy) {
    let cluster = ClusterId::new([1; 16]).unwrap();
    let member = |n: u8| PlacedMember {
        member: MemberId::new(n.into()).unwrap(),
        node: id(n),
        disk: DiskLineage::new([n + 10; 16]).unwrap(),
    };
    let group = GroupScope {
        cluster,
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:repair".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    let nodes = (2..=5)
        .map(|n: u8| {
            let scope = CapacityScope {
                cluster,
                incarnation: IncarnationId::new([9; 16]).unwrap(),
                catalog: [8; 32],
                node: NodeIdentity {
                    node: id(n),
                    disk: DiskLineage::new([n + 10; 16]).unwrap(),
                    process: ProcessGeneration::new([n + 20; 16]).unwrap(),
                },
                engine: EngineInstanceId::new([n + 30; 16]).unwrap(),
                boot: [n + 40; 16],
                profile_revision: 1,
            };
            TopologyNode {
                ledger: LedgerSnapshot {
                    scope,
                    policy: LedgerPolicy {
                        capacity: bytes(10),
                        reserve: bytes(1),
                        max_active_moves: 2,
                        max_sample_age_ms: 100,
                    },
                    sample: PhysicalSample {
                        scope,
                        sequence: 1,
                        observed_ms: 100,
                        physical: bytes(1),
                    },
                    reservations: vec![],
                    complete: true,
                },
                host: format!("host-{n}"),
                region: "region".into(),
                zone: format!("zone-{n}"),
                keep: true,
                receives_replicas: true,
            }
        })
        .collect();
    (
        IsolatedRepairView {
            scope: group,
            revision: LogIndex::new(10),
            high_water: 7,
            voters: [member(1), member(2), member(3)],
            failed: id(1),
            failed_host: "host-1".into(),
            occupied: vec![id(1), id(2), id(3)],
            peak: bytes(1),
        },
        nodes,
        TopologyPolicy {
            revision: 1,
            distinct_hosts: true,
            distinct_zones: true,
            distinct_regions: false,
            max_work: 1000,
        },
    )
}

#[test]
fn isolated_repair_preserves_survivors_and_allocates_above_high_water() {
    let (v, n, p) = fixture();
    let r = select_isolated_repair(&v, &n, p, 100).unwrap();
    assert_eq!(r.obsolete, MemberId::new(1).unwrap());
    assert_eq!(r.replacement.member, MemberId::new(8).unwrap());
    assert!([id(4), id(5)].contains(&r.replacement.node));
    assert_eq!(r.expected_revision, v.revision);
}

#[test]
fn isolated_repair_is_order_independent() {
    let (v, mut n, p) = fixture();
    let a = select_isolated_repair(&v, &n, p, 100).unwrap();
    n.reverse();
    assert_eq!(select_isolated_repair(&v, &n, p, 100).unwrap(), a);
}

#[test]
fn isolated_repair_charges_memory_and_headroom_before_ranking() {
    let (v, mut n, p) = fixture();
    n[2].ledger.sample.physical.memory_bytes = 95;
    n[3].ledger.sample.physical.durable_bytes = 95;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::RecoveryUnproven(v.failed))
    );
}

#[test]
fn isolated_repair_rejects_retired_bindings_and_excluded_targets() {
    let (mut v, mut n, p) = fixture();
    v.occupied.push(id(4));
    n[3].keep = false;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::RecoveryUnproven(v.failed))
    );
}

#[test]
fn isolated_repair_keeps_failed_physical_host_excluded_during_replacement() {
    let (v, mut n, p) = fixture();
    n[2].host = v.failed_host.clone();
    n[3].zone = n[0].zone.clone();
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::RecoveryUnproven(v.failed))
    );
}

#[test]
fn isolated_repair_requires_both_fresh_surviving_voters() {
    let (v, mut n, p) = fixture();
    n[0].ledger.sample.observed_ms = 0;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 101),
        Err(TopologyError::Ledger(LedgerError::StaleObservation))
    );
    n.remove(0);
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::Incomplete)
    );
}

#[test]
fn isolated_repair_skips_stale_spare_instead_of_inventing_free_capacity() {
    let (v, mut n, p) = fixture();
    n[2].ledger.sample.observed_ms = 0;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 101)
            .unwrap()
            .replacement
            .node,
        id(5)
    );
}

#[test]
fn isolated_repair_refuses_unsettled_and_incomplete_ledgers() {
    let (v, mut n, p) = fixture();
    n[2].ledger.complete = false;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::Ledger(LedgerError::Incomplete))
    );
    n[2].ledger.complete = true;
    let scope = n[2].ledger.scope;
    n[2].ledger.reservations.push(Reservation {
        id: ProposalId::new([6; 16]).unwrap(),
        group: v.scope.clone(),
        scope,
        peak: bytes(1),
        materialized: None,
    });
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::UnsettledMovement)
    );
}

#[test]
fn isolated_repair_refuses_wrong_scope_and_member_exhaustion() {
    let (mut v, mut n, p) = fixture();
    n[2].ledger.scope.catalog = [7; 32];
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::InvalidInput)
    );
    n[2].ledger.scope.catalog = [8; 32];
    v.high_water = u64::MAX - 3;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::Limit)
    );
}

#[test]
fn isolated_repair_ranks_projected_memory_pressure() {
    let (v, mut n, p) = fixture();
    n[2].ledger.sample.physical = Resources {
        engine_slots: 0,
        durable_bytes: 0,
        memory_bytes: 70,
    };
    n[3].ledger.sample.physical = Resources {
        engine_slots: 4,
        durable_bytes: 10,
        memory_bytes: 10,
    };
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100)
            .unwrap()
            .replacement
            .node,
        id(5)
    );
}

#[test]
fn isolated_repair_refuses_overflow_instead_of_saturating_capacity() {
    let (v, mut n, p) = fixture();
    n[2].ledger.policy.capacity.memory_bytes = u64::MAX;
    n[2].ledger.policy.reserve.memory_bytes = 0;
    n[2].ledger.sample.physical.memory_bytes = u64::MAX;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::Ledger(LedgerError::ArithmeticOverflow))
    );
}

fn unrelated(node: &TopologyNode, group: &GroupScope, id: u8) -> Reservation {
    let mut other = group.clone();
    other.cell = format!("Counter:unrelated-{id}");
    other.group = GroupId::new([id; 16]).unwrap();
    Reservation {
        id: ProposalId::new([id; 16]).unwrap(),
        group: other,
        scope: node.ledger.scope,
        peak: bytes(1),
        materialized: None,
    }
}

#[test]
fn isolated_repair_charges_unrelated_reservations_before_selection() {
    let (v, mut n, p) = fixture();
    n[3].receives_replicas = false;
    let reservation = unrelated(&n[2], &v.scope, 71);
    n[2].ledger.reservations.push(reservation);
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100)
            .unwrap()
            .replacement
            .node,
        id(4)
    );
    n[2].ledger.reservations[0].peak.durable_bytes = 80;
    n[2].ledger.reservations[0].peak.memory_bytes = 80;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::RecoveryUnproven(v.failed))
    );
}

#[test]
fn isolated_repair_respects_existing_movement_slot_limit() {
    let (v, mut n, p) = fixture();
    n[3].receives_replicas = false;
    n[2].ledger.reservations = vec![
        unrelated(&n[2], &v.scope, 71),
        unrelated(&n[2], &v.scope, 72),
    ];
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::RecoveryUnproven(v.failed))
    );
}

#[test]
fn isolated_repair_does_not_credit_old_boot_materialization() {
    let (v, mut n, p) = fixture();
    n[3].receives_replicas = false;
    n[2].ledger.sample.physical = bytes(8);
    let mut reservation = unrelated(&n[2], &v.scope, 71);
    reservation.scope.boot = [99; 16];
    reservation.scope.node.process = ProcessGeneration::new([99; 16]).unwrap();
    reservation.materialized = Some(Materialization {
        sequence: 1,
        physical: bytes(1),
    });
    n[2].ledger.reservations.push(reservation);
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::RecoveryUnproven(v.failed))
    );
    n[2].ledger.reservations[0].scope = n[2].ledger.scope;
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100)
            .unwrap()
            .replacement
            .node,
        id(4)
    );
}

#[test]
fn isolated_repair_refuses_rebound_unresolved_reservations() {
    let (v, mut n, p) = fixture();
    let mut reservation = unrelated(&n[2], &v.scope, 71);
    reservation.scope.engine = EngineInstanceId::new([99; 16]).unwrap();
    n[2].ledger.reservations.push(reservation);
    assert_eq!(
        select_isolated_repair(&v, &n, p, 100),
        Err(TopologyError::Ledger(LedgerError::InvalidReservation))
    );
}
