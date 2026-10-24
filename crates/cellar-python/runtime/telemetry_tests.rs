// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn every_lifecycle_exit_has_one_terminal_disposition() {
    let metrics = Arc::new(Metrics::default());
    let origin = Instant::now();
    let queued = metrics.admit(origin);
    assert!(!metrics.report().settled());
    drop(queued);
    let mut running = metrics.admit(origin);
    running.begin();
    drop(running);
    let mut lost_reply = metrics.admit(origin);
    lost_reply.begin();
    lost_reply.complete(false);
    drop(lost_reply);
    for success in [true, false] {
        let mut delivered = metrics.admit(origin);
        delivered.begin();
        delivered.complete(success);
        assert!(!metrics.report().settled());
        delivered.delivered();
    }
    let report = metrics.report();
    assert!(report.settled());
    assert_eq!(report.admitted, 5);
    assert_eq!(report.started, 4);
    assert_eq!(report.succeeded, 1);
    assert_eq!(report.failed, 2);
    assert_eq!(report.delivered, 2);
    assert_eq!(report.abandoned_queued, 1);
    assert_eq!(report.abandoned_running, 1);
    assert_eq!(report.undelivered, 1);
    assert_eq!(report.queue_wait.samples, 4);
    assert_eq!(report.sdk_duration.samples, 4);
    assert_eq!(report.reply_wait.samples, 2);
    assert_eq!(report.round_trip.samples, 2);
    let mut inconsistent = report;
    inconsistent.delivered += 1;
    assert!(!inconsistent.settled());
}

#[test]
fn histogram_bounds_are_inclusive_and_elapsed_totals_saturate() {
    let histogram = Histogram::default();
    for ns in [0, 1_000, 1_001, 10_000_000_000, 10_000_000_001, u64::MAX] {
        histogram.observe(Duration::from_nanos(ns));
    }
    let report = histogram.report();
    assert_eq!(report.samples, 6);
    assert_eq!(report.total_ns, u64::MAX);
    assert_eq!(report.maximum_ns, u64::MAX);
    assert_eq!(report.bucket_counts, [2, 1, 0, 0, 0, 0, 0, 1, 2]);
    assert_eq!(LatencyReport::upper_bounds_ns()[0], Some(1_000));
    assert_eq!(LatencyReport::upper_bounds_ns()[8], None);
}
