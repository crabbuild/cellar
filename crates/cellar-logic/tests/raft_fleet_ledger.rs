// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pure resource-accounting boundary. These tests do not qualify catalog CAS,
//! physical observations, membership execution or fleet performance.

use cellar_logic::{consensus::*, raft_fleet::*};

fn resources(slots: u64, disk: u64, memory: u64) -> Resources {
    Resources {
        engine_slots: slots,
        durable_bytes: disk,
        memory_bytes: memory,
    }
}

fn scope() -> CapacityScope {
    CapacityScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        incarnation: IncarnationId::new([2; 16]).unwrap(),
        catalog: [3; 32],
        node: NodeIdentity {
            node: NodeId::new([4; 16]).unwrap(),
            disk: DiskLineage::new([5; 16]).unwrap(),
            process: ProcessGeneration::new([6; 16]).unwrap(),
        },
        engine: EngineInstanceId::new([7; 16]).unwrap(),
        boot: [8; 16],
        profile_revision: 1,
    }
}

fn sample(sequence: u64, physical: Resources) -> PhysicalSample {
    PhysicalSample {
        scope: scope(),
        sequence,
        observed_ms: sequence * 100,
        physical,
    }
}

fn reservation(id: u8, peak: Resources) -> Reservation {
    Reservation {
        id: ProposalId::new([id; 16]).unwrap(),
        group: GroupScope {
            cluster: scope().cluster,
            namespace: NamespaceId::new([9; 16]).unwrap(),
            cell: format!("Counter:{id}"),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([10; 16]).unwrap(),
        },
        scope: scope(),
        peak,
        materialized: None,
    }
}

fn snapshot() -> LedgerSnapshot {
    LedgerSnapshot {
        scope: scope(),
        policy: LedgerPolicy {
            capacity: resources(500, 5000, 5000),
            reserve: resources(100, 100, 100),
            max_active_moves: 2,
            max_sample_age_ms: 1000,
        },
        sample: sample(1, resources(300, 3000, 1000)),
        reservations: vec![],
        complete: true,
    }
}

fn attribution(id: u8, physical: Resources) -> SampleAttribution {
    SampleAttribution {
        reservation: ProposalId::new([id; 16]).unwrap(),
        scope: scope(),
        physical,
    }
}

#[test]
fn retained_slots_and_reserve_are_charged_once() {
    // 250 serving + 50 retained + two new + 100 explicit spare = 402.
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    ledger
        .reserve(reservation(11, resources(1, 100, 50)), 100)
        .unwrap();
    ledger
        .reserve(reservation(12, resources(1, 100, 50)), 100)
        .unwrap();
    let charge = ledger.charge(100).unwrap();
    assert_eq!(charge.physical.engine_slots, 300);
    assert_eq!(charge.total, resources(402, 3300, 1200));
    assert_eq!(charge.active_moves, 2);
}

#[test]
fn local_last_slot_cannot_be_reserved_twice() {
    let mut state = snapshot();
    state.sample.physical.engine_slots = 399;
    let mut ledger = ResourceLedger::restore(state).unwrap();
    ledger
        .reserve(reservation(11, resources(1, 100, 50)), 100)
        .unwrap();
    let admitted = ledger.snapshot();
    assert_eq!(
        ledger.reserve(reservation(12, resources(1, 100, 50)), 100),
        Err(LedgerError::CapacityExceeded(ResourceKind::EngineSlots))
    );
    assert_eq!(ledger.snapshot(), admitted);
}

#[test]
fn each_resource_limit_is_independent() {
    for (physical, peak, resource) in [
        (
            resources(399, 0, 0),
            resources(2, 0, 0),
            ResourceKind::EngineSlots,
        ),
        (
            resources(0, 4899, 0),
            resources(0, 2, 0),
            ResourceKind::DurableBytes,
        ),
        (
            resources(0, 0, 4899),
            resources(0, 0, 2),
            ResourceKind::MemoryBytes,
        ),
    ] {
        let mut state = snapshot();
        state.sample.physical = physical;
        let mut ledger = ResourceLedger::restore(state).unwrap();
        assert_eq!(
            ledger.reserve(reservation(11, peak), 100),
            Err(LedgerError::CapacityExceeded(resource))
        );
        assert!(ledger.snapshot().reservations.is_empty());
    }
}

#[test]
fn materialization_replaces_only_attributed_growth() {
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    let requested = reservation(11, resources(1, 200, 100));
    ledger.reserve(requested.clone(), 100).unwrap();
    assert_eq!(
        ledger.charge(100).unwrap().total,
        resources(401, 3300, 1200)
    );
    ledger
        .observe(
            sample(2, resources(301, 3100, 1040)),
            &[attribution(11, resources(1, 100, 40))],
        )
        .unwrap();
    let charge = ledger.charge(200).unwrap();
    assert_eq!(charge.total, resources(401, 3300, 1200));
    assert_eq!(charge.unmaterialized, resources(0, 100, 60));
    assert_eq!(charge.active_moves, 1);
    // Retry cannot erase the already attributed part or its sequence barrier.
    assert!(!ledger.reserve(requested, 9999).unwrap());
    let restored = ResourceLedger::restore(ledger.snapshot()).unwrap();
    assert_eq!(restored.charge(200), ledger.charge(200));
}

#[test]
fn missing_attribution_retains_full_growth() {
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    ledger
        .reserve(reservation(11, resources(1, 100, 50)), 100)
        .unwrap();
    ledger
        .observe(sample(2, resources(301, 3100, 1050)), &[])
        .unwrap();
    assert_eq!(
        ledger.charge(200).unwrap().total,
        resources(402, 3300, 1200)
    );
}

#[test]
fn reduced_physical_reading_does_not_free_retained_state() {
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    ledger
        .observe(sample(2, resources(250, 2500, 900)), &[])
        .unwrap();
    assert_eq!(
        ledger.charge(200).unwrap().physical,
        resources(300, 3000, 1000)
    );
    assert_eq!(
        ResourceLedger::restore(ledger.snapshot())
            .unwrap()
            .charge(200),
        ledger.charge(200)
    );
}

#[test]
fn old_boot_process_and_profile_keep_full_debt() {
    for change in 0..3 {
        let mut state = snapshot();
        let mut old = reservation(11, resources(1, 200, 100));
        old.materialized = Some(Materialization {
            sequence: 50,
            physical: old.peak,
        });
        match change {
            0 => old.scope.boot = [12; 16],
            1 => old.scope.node.process = ProcessGeneration::new([12; 16]).unwrap(),
            _ => old.scope.profile_revision = 2,
        }
        state.reservations.push(old);
        let mut ledger = ResourceLedger::restore(state).unwrap();
        assert_eq!(
            ledger.charge(100).unwrap().unmaterialized,
            resources(1, 200, 100)
        );
        let original = ledger.snapshot();
        assert_eq!(
            ledger.observe(
                sample(2, resources(301, 3200, 1100)),
                &[attribution(11, resources(1, 200, 100))]
            ),
            Err(LedgerError::ScopeMismatch)
        );
        assert_eq!(ledger.snapshot(), original);
        ledger
            .reserve(reservation(12, resources(1, 1, 1)), 100)
            .unwrap();
        assert_eq!(
            ledger.reserve(reservation(13, resources(1, 1, 1)), 100),
            Err(LedgerError::MovementLimit)
        );
    }
}

#[test]
fn stale_unknown_and_future_observations_do_not_grant_capacity() {
    let mut state = snapshot();
    state.complete = false;
    assert!(matches!(
        ResourceLedger::restore(state),
        Err(LedgerError::Incomplete)
    ));
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    assert_eq!(ledger.charge(99), Err(LedgerError::InvalidObservation));
    assert_eq!(ledger.charge(1101), Err(LedgerError::StaleObservation));
    assert_eq!(
        ledger.reserve(reservation(11, resources(1, 1, 1)), 1101),
        Err(LedgerError::StaleObservation)
    );
    assert!(ledger.snapshot().reservations.is_empty());
    assert_eq!(
        ledger.observe(sample(1, resources(1, 1, 1)), &[]),
        Err(LedgerError::StaleObservation)
    );
    let mut future = sample(2, resources(1, 1, 1));
    future.scope.boot = [12; 16];
    assert_eq!(ledger.observe(future, &[]), Err(LedgerError::ScopeMismatch));
}

#[test]
fn older_sample_cannot_overwrite_materialization() {
    let mut state = snapshot();
    let mut action = reservation(11, resources(1, 100, 50));
    action.materialized = Some(Materialization {
        sequence: 2,
        physical: action.peak,
    });
    state.reservations.push(action);
    assert!(matches!(
        ResourceLedger::restore(state),
        Err(LedgerError::StaleObservation)
    ));
}

#[test]
fn invalid_attribution_never_partially_advances_ledger() {
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    ledger
        .reserve(reservation(11, resources(1, 100, 50)), 100)
        .unwrap();
    ledger
        .reserve(reservation(12, resources(1, 100, 50)), 100)
        .unwrap();
    let before = ledger.snapshot();
    let first = attribution(11, resources(1, 100, 50));
    for invalid in [
        attribution(12, resources(2, 100, 50)),
        attribution(13, resources(1, 100, 50)),
        first,
    ] {
        assert!(ledger
            .observe(sample(2, resources(302, 3200, 1100)), &[first, invalid])
            .is_err());
        assert_eq!(ledger.snapshot(), before);
    }
}

#[test]
fn materialized_credits_cannot_exceed_current_physical_sample() {
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    ledger
        .reserve(reservation(11, resources(1, 100, 50)), 100)
        .unwrap();
    ledger
        .reserve(reservation(12, resources(1, 100, 50)), 100)
        .unwrap();
    let before = ledger.snapshot();
    assert_eq!(
        ledger.observe(
            sample(2, resources(1, 100, 50)),
            &[
                attribution(11, resources(1, 100, 50)),
                attribution(12, resources(1, 100, 50))
            ]
        ),
        Err(LedgerError::UnattributedPhysical)
    );
    assert_eq!(ledger.snapshot(), before);
}

#[test]
fn cumulative_materialization_cannot_move_backwards() {
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    ledger
        .reserve(reservation(11, resources(1, 100, 50)), 100)
        .unwrap();
    ledger
        .observe(
            sample(2, resources(301, 3100, 1050)),
            &[attribution(11, resources(1, 100, 50))],
        )
        .unwrap();
    let before = ledger.snapshot();
    assert_eq!(
        ledger.observe(
            sample(3, resources(301, 3100, 1050)),
            &[attribution(11, resources(1, 99, 50))]
        ),
        Err(LedgerError::StaleMaterialization)
    );
    assert_eq!(ledger.snapshot(), before);
}

#[test]
fn duplicate_identity_and_changed_retry_cannot_reset_debt() {
    let mut state = snapshot();
    let action = reservation(11, resources(1, 100, 50));
    state.reservations = vec![action.clone(), action.clone()];
    assert!(matches!(
        ResourceLedger::restore(state),
        Err(LedgerError::DuplicateReservation)
    ));
    let mut ledger = ResourceLedger::restore(snapshot()).unwrap();
    ledger.reserve(action.clone(), 100).unwrap();
    let before = ledger.snapshot();
    let mut changed = action;
    changed.peak.durable_bytes -= 1;
    assert_eq!(
        ledger.reserve(changed, 100),
        Err(LedgerError::PayloadConflict)
    );
    assert_eq!(ledger.snapshot(), before);
}

#[test]
fn replacement_engine_and_cross_catalog_debt_refuse() {
    for change in 0..5 {
        let mut state = snapshot();
        let mut action = reservation(11, resources(1, 1, 1));
        match change {
            0 => action.scope.engine = EngineInstanceId::new([12; 16]).unwrap(),
            1 => action.scope.node.disk = DiskLineage::new([12; 16]).unwrap(),
            2 => action.scope.catalog = [12; 32],
            3 => action.scope.cluster = ClusterId::new([12; 16]).unwrap(),
            _ => action.scope.incarnation = IncarnationId::new([12; 16]).unwrap(),
        }
        state.reservations.push(action);
        assert!(matches!(
            ResourceLedger::restore(state),
            Err(LedgerError::InvalidReservation)
        ));
    }
}

#[test]
fn snapshot_over_capacity_is_visible_but_cannot_admit() {
    let mut state = snapshot();
    state.sample.physical.durable_bytes = 5001;
    let mut ledger = ResourceLedger::restore(state).unwrap();
    assert_eq!(ledger.charge(100).unwrap().total.durable_bytes, 5101);
    assert_eq!(
        ledger.reserve(reservation(11, resources(1, 0, 0)), 100),
        Err(LedgerError::CapacityExceeded(ResourceKind::DurableBytes))
    );
}

#[test]
fn overflow_cannot_saturate_into_spare_capacity() {
    let mut state = snapshot();
    state.policy.capacity.durable_bytes = u64::MAX;
    state.sample.physical.durable_bytes = u64::MAX - 100;
    let mut ledger = ResourceLedger::restore(state).unwrap();
    assert_eq!(
        ledger.reserve(reservation(11, resources(1, 1, 1)), 100),
        Err(LedgerError::ArithmeticOverflow)
    );
    assert!(ledger.snapshot().reservations.is_empty());
}

#[test]
fn malformed_inputs_and_bounded_inventory_refuse() {
    let mut state = snapshot();
    state.scope.catalog = [0; 32];
    assert!(matches!(
        ResourceLedger::restore(state),
        Err(LedgerError::InvalidScope)
    ));
    let mut state = snapshot();
    state.policy.reserve.engine_slots = 501;
    assert!(matches!(
        ResourceLedger::restore(state),
        Err(LedgerError::InvalidPolicy)
    ));
    let mut state = snapshot();
    state.sample.sequence = 0;
    assert!(matches!(
        ResourceLedger::restore(state),
        Err(LedgerError::InvalidObservation)
    ));
    let mut state = snapshot();
    state.reservations = vec![reservation(11, resources(1, 1, 1)); 4097];
    assert!(matches!(
        ResourceLedger::restore(state),
        Err(LedgerError::Limit)
    ));
}
