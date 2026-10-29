// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn policy() -> HealthPolicy {
    HealthPolicy::new(5000, 5000).unwrap()
}

#[test]
fn unavailable_requires_repeated_fresh_failures_across_grace() {
    let mut h = HealthTracker::default();
    h.observe(policy(), 100, 0, HealthObservation::DirectFailure);
    assert_eq!(h.state(policy(), 5100), NodeHealth::Suspect);
    h.observe(policy(), 5099, 0, HealthObservation::DirectFailure);
    assert_eq!(h.state(policy(), 5099), NodeHealth::Suspect);
    h.observe(policy(), 5100, 0, HealthObservation::DirectFailure);
    assert_eq!(h.state(policy(), 5100), NodeHealth::Unavailable);
    assert_eq!(h.consecutive_failures(policy(), 5100), 3);
    assert_eq!(h.state(policy(), 10101), NodeHealth::Unknown);
    assert_eq!(h.consecutive_failures(policy(), 10101), 0);
}

#[test]
fn unknown_and_recovery_restart_the_failure_grace() {
    for reset in [HealthObservation::Unknown, HealthObservation::Responsive] {
        let mut h = HealthTracker::default();
        h.observe(policy(), 100, 0, HealthObservation::DirectFailure);
        h.observe(policy(), 5100, 0, HealthObservation::DirectFailure);
        assert_eq!(h.state(policy(), 5100), NodeHealth::Unavailable);
        h.observe(policy(), 5200, 0, reset);
        assert_eq!(h.consecutive_failures(policy(), 5200), 0);
        h.observe(policy(), 5300, 0, HealthObservation::DirectFailure);
        assert_eq!(h.state(policy(), 5300), NodeHealth::Suspect);
        h.observe(policy(), 10299, 0, HealthObservation::DirectFailure);
        assert_eq!(h.state(policy(), 10299), NodeHealth::Suspect);
    }
}

#[test]
fn collection_gaps_and_old_samples_cannot_accumulate_failure_time() {
    let mut h = HealthTracker::default();
    h.observe(policy(), 100, 0, HealthObservation::DirectFailure);
    h.observe(policy(), 5200, 0, HealthObservation::DirectFailure);
    assert_eq!(h.state(policy(), 5200), NodeHealth::Suspect);
    assert_eq!(h.consecutive_failures(policy(), 5200), 1);
    h.observe(policy(), 11000, 5001, HealthObservation::DirectFailure);
    assert_eq!(h.state(policy(), 11000), NodeHealth::Unknown);
    h.observe(policy(), 12000, 0, HealthObservation::DirectFailure);
    assert_eq!(h.consecutive_failures(policy(), 12000), 1);
}

#[test]
fn probe_duration_and_clock_regression_do_not_shorten_grace() {
    let mut h = HealthTracker::default();
    h.observe(policy(), 500, 500, HealthObservation::DirectFailure);
    h.observe(policy(), 5000, 500, HealthObservation::DirectFailure);
    assert_eq!(h.state(policy(), 5000), NodeHealth::Suspect);
    h.observe(policy(), 5500, 500, HealthObservation::DirectFailure);
    assert_eq!(h.state(policy(), 5500), NodeHealth::Unavailable);
    h.observe(policy(), 5400, 0, HealthObservation::Responsive);
    assert_eq!(h.state(policy(), 5400), NodeHealth::Unknown);
    h.observe(policy(), 10, 11, HealthObservation::Responsive);
    assert_eq!(h.state(policy(), 10), NodeHealth::Unknown);
    assert!(HealthPolicy::new(0, 5000).is_none());
    assert!(HealthPolicy::new(5000, 0).is_none());
}
