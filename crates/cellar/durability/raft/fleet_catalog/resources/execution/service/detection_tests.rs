// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn boot(process: u8) -> BootBinding {
    BootBinding {
        catalog: [1; 32],
        fleet: [2; 16],
        node: [3; 16],
        disk: [4; 16],
        engine: [5; 16],
        boot: [process; 16],
        process: [process; 16],
        epoch: u64::from(process),
        join_key: [7; 32],
    }
}

#[test]
fn fleet_failure_detection_only_direct_failure_counts() {
    assert_eq!(
        classify(&InspectionError::Unreachable),
        HealthObservation::DirectFailure
    );
    for e in [
        InspectionError::Overloaded,
        InspectionError::Unavailable,
        InspectionError::Refused,
        InspectionError::StalePeer,
    ] {
        assert_eq!(classify(&e), HealthObservation::Unknown);
    }
}

#[test]
fn fleet_failure_detection_boot_changes_and_root_errors_clear_history() {
    let mut d = Detection::new(FleetFailureDetectionOptions::default());
    let id = NodeId::new([3; 16]).unwrap();
    d.refresh_inventory(BTreeMap::from([(id, boot(8))]));
    let policy = d.options.policy();
    let tracker = &mut d.nodes.get_mut(&id).unwrap().tracker;
    tracker.observe(policy, 100, 0, HealthObservation::DirectFailure);
    tracker.observe(policy, 5100, 0, HealthObservation::DirectFailure);
    assert_eq!(d.view(5100).nodes[0].health, FleetNodeHealth::Unavailable);
    d.refresh_inventory(BTreeMap::from([(id, boot(9))]));
    assert_eq!(d.view(5100).nodes[0].health, FleetNodeHealth::Unknown);
    d.nodes
        .get_mut(&id)
        .unwrap()
        .tracker
        .observe(policy, 5200, 0, HealthObservation::Responsive);
    assert_eq!(d.view(5200).nodes[0].health, FleetNodeHealth::Healthy);
    d.invalidate();
    assert_eq!(d.view(5200).nodes[0].health, FleetNodeHealth::Unknown);
    d.refresh_inventory(BTreeMap::new());
    assert!(d.view(5200).nodes.is_empty());
}

#[test]
fn fleet_failure_detection_status_ages_without_worker_progress() {
    let mut d = Detection::new(FleetFailureDetectionOptions::default());
    let id = NodeId::new([3; 16]).unwrap();
    d.refresh_inventory(BTreeMap::from([(id, boot(8))]));
    d.nodes.get_mut(&id).unwrap().tracker.observe(
        d.options.policy(),
        100,
        0,
        HealthObservation::Responsive,
    );
    let view = d.view(200);
    let mut stale = view.clone();
    stale.age(Some(5101));
    assert_eq!(stale.nodes[0].health, FleetNodeHealth::Unknown);
    assert_eq!(stale.nodes[0].sample_age_ms, Some(5001));
    let mut stopped = view.clone();
    stopped.age(None);
    assert_eq!(stopped.nodes[0].health, FleetNodeHealth::Unknown);
    assert_eq!(stopped.nodes[0].sample_age_ms, None);
    let mut fresh = view;
    fresh.age(Some(300));
    assert_eq!(fresh.nodes[0].health, FleetNodeHealth::Healthy);
    assert_eq!(fresh.nodes[0].sample_age_ms, Some(200));
}

#[test]
fn fleet_failure_detection_bounds_preserve_grace_and_probe_ceiling() {
    let mut options = FleetFailureDetectionOptions::default();
    assert!(options.validate().is_ok());
    options.unavailability_grace = Duration::from_millis(4999);
    assert!(options.validate().is_err());
    options.unavailability_grace = Duration::from_secs(5);
    options.probe_timeout = Duration::from_millis(501);
    assert!(options.validate().is_err());
    options.probe_timeout = Duration::ZERO;
    assert!(options.validate().is_err());
}
